# Standalone VPS DEV image

This overlay is intentionally independent of the NAS Compose stack. It embeds
the reviewed DEV source into a read-only DAG tree, uses fresh project volumes,
and keeps every runtime container off the public internet by default. All
containers stay on the `runtime` network (`internal: true`). LocalExecutor
means `scheduler` is the only container that executes DAG tasks, so it is
also the only container additionally attached to the `egress` network;
`webserver`, `init`, `app-init`, `metadata`, `application`, and the three ML
sidecars (`diarize-api`, `yamnet-api`, `whisper-api`) never leave `runtime`.
`egress` defaults to `internal: true` (no egress) whenever `EGRESS_INTERNAL`
is unset, so a stack rendered without that variable is exactly as isolated as
before. The runtime publishes no Docker ports: the webserver has a fixed
address inside the internal network, and the host relay owned by
`homeserver-config/ansible/vps-dev` exposes it on the VPS loopback only. All
business DAGs start paused and examples are disabled.

Deployment is owned by `homeserver-config/ansible/vps-dev/run.sh`; use that
entrypoint rather than a bare `docker compose up`. It creates persistent keys,
starts PostgreSQL, runs `init.py`, then starts consumers and runs `verify.py`.
The complete launch, acceptance, backup/restore, and rollback runbook is in
`homeserver-config/ansible/vps-dev/README.md`.

## Configuration sources

Compose is rendered from three `--env-file` sources supplied by Ansible:

- `runtime.env` — persistent secrets generated once per host (DB passwords,
  Fernet/webserver keys, admin password).
- `release.env` — non-secret per-release settings (image tags, subnets,
  `UI_UPSTREAM`, `EGRESS_SUBNET`, `EGRESS_INTERNAL`, `YOUTUBE_TOKENS_HOST_DIR`,
  `NAS_SYNC_HOST_DIR`, `NAS_ARCHIVE_HOST`, `NAS_ARCHIVE_PORT`,
  `NAS_ARCHIVE_USER`, `NAS_ARCHIVE_ROOT`, `NAS_ARCHIVE_MIN_AGE_DAYS`).
- `external.env` — optional external API key secrets (`OPENAI_API_KEY`,
  `YOUTUBE_API_KEY`, `REAP_API_KEY`, `PIKZELS_API_KEY`). Each falls back to
  the literal placeholder `dev-disabled-not-a-credential` when this file (or
  the individual key) is absent, so a stack without vault access behaves
  exactly as before this contract existed.

YouTube OAuth tokens live at `YOUTUBE_TOKENS_HOST_DIR` on the host, bind-mounted
read-write into the scheduler at
`/opt/airflow/data/congress_videos/youtube_tokens` (the path
`congress_videos/config/paths.py` already expects). Ansible creates and
chowns that directory to `50000:0` (the image's `airflow` uid:gid) before
`compose up`; the compose file requires the variable with no default so a
missing directory fails loudly rather than being created with the wrong
owner.

`NAS_SYNC_HOST_DIR` on the host holds `id_ed25519`, `id_ed25519.pub`, and
`known_hosts` for the `nas_archive` DAG; it is bind-mounted **read-only** into
the scheduler at `/opt/airflow/nas_sync`. This mount is required with no
default (`:?Required`), like `YOUTUBE_TOKENS_HOST_DIR`, so a missing directory
fails loudly. `NAS_ARCHIVE_HOST` defaults to an empty string, which disables
`congress_videos/nas_archive_dag.py` entirely (its `check_enabled` task
short-circuits) — a stack without a configured NAS archive target behaves
exactly as before this contract existed. When enabled, that DAG offloads
local raw/derived material for fully-completed videos (uploaded, verified,
and at least `NAS_ARCHIVE_MIN_AGE_DAYS` days old — default 14) to
`NAS_ARCHIVE_HOST:NAS_ARCHIVE_ROOT` over rsync-over-SSH, then prunes it from
local disk; `PROJECT_DATA_DIR/thumbnails/` is mirrored to the same target on
every enabled run but is never pruned locally, since its files are keyed by
the uploaded YouTube video id and can't be attributed to one source video.

`congress_videos/nas_fetch_dag.py` (`nas_fetch`) is the inverse, on-demand
recovery DAG: it pulls one or more already-archived videos' material back
from the NAS onto local disk so a downstream DAG (`speaker_turns`,
`trim_proposals`, `speaker_turn_videos`, ...) can reprocess them. It reuses
the exact same `NAS_ARCHIVE_*` settings and SSH key mount as `nas_archive` —
no additional configuration. Trigger it with:

```bash
airflow dags trigger nas_fetch --conf '{"video_id": "abc123"}'
airflow dags trigger nas_fetch --conf '{"video_ids": ["abc123", "def456"]}'
airflow dags trigger nas_fetch --conf '{"video_id": "abc123", "channel_slug": "congreso-es-tv"}'
```

`channel_slug` defaults to `DEFAULT_CHANNEL` when omitted. Each requested
video is handled independently: a failure fetching or verifying one video
aborts only that one (its `.nas_archived.json` marker is left in place) and
is recorded in the run summary; the rest still proceed. The NAS copy is never
modified or deleted by this DAG. After a successful fetch, every restored
media file's mtime is reset to the fetch time — `rsync -a` preserves the
NAS's original timestamps, so without this the video would still look old to
`nas_archive`'s local age gate — giving the video a fresh full
`NAS_ARCHIVE_MIN_AGE_DAYS` window before `nas_archive` can pick it up again.
Re-archiving afterwards is cheap: the NAS copy is unchanged, so the eventual
re-push is close to a no-op sync.

`utils/git_sync_dag.py` is excluded from DAG loading on this image: the
Dockerfile appends `git_sync_dag` to `.airflowignore` before the tree is made
read-only, since the VPS scheduler must never pull from GitHub or hold a
`GITHUB_TOKEN`.

`test_contract.py` is a standalone local YAML/static test, not a DAG import or
full application test. Run it with a Python environment containing PyYAML.
`verify.py` imports real DAGs **only inside the isolated deployed runtime**.
Do not execute that verifier directly on a workstation with production access.

The Dockerfile uses the source revision's frozen `uv.lock`, but OS packages
are not snapshotted. Preserve the built image and recorded image ID for exact
reuse.

The three ML sidecars (`whisper-api`, `yamnet-api`, `diarize-api`) run on the
same internal network with model volumes pre-seeded by Ansible: no runtime
download and no Hugging Face token exist on the VPS. `diarize-api` and
`yamnet-api` are built from `benchmarks/` at the release revision; `whisper-api`
is the third-party image pinned by digest. Everything is CPU-only.
`ml_smoke.py` runs inside the scheduler and proves each sidecar answers on
synthetic audio. Airflow's local Whisper path is intentionally absent from the
frozen image, so DEV transcription goes through `whisper-api` text-only (no
SRT), a known functional gap versus production's start-time pip install.
The scheduler bind-mounts `YOUTUBE_TOKENS_HOST_DIR` for OAuth tokens (see
"Configuration sources" above), but no real token is seeded by this
foundation. This foundation does not prove the full video pipeline works; no
runtime claim follows from the passing static contracts.

## Application database

`application` is a second, fully isolated `postgres:16-alpine` instance
(same pinned digest as `metadata`) holding the business schema — it starts
**empty**; no data is copied from anywhere. The one-shot `app-init` service
provisions it once per release: as the bootstrap superuser (`airflow`, the
same legacy role name `congress_videos/sql/grant_permissions.sql` expects on
the NAS) it creates the `development` schema and applies that idempotent
grant script, then sets the `airflow_dev` (runtime, DML-only) and
`airflow_migrations` (DDL) role passwords. It then calls the same migration
functions `utils/migrations_dag.py`'s `run_migrations` DAG uses — directly,
never through a DAG run, so `verify.py`'s zero-DAG-run assertion still holds.
`scheduler` and `webserver` only ever hold the `airflow_dev` runtime
credential; the bootstrap superuser and migration passwords never reach
their environment. `app_smoke.py` runs inside the scheduler afterward and
proves the DAG code can authenticate as `airflow_dev`, see the migrated
schema, and perform a DML round-trip (rolled back on purpose).
