# Design: test-runner-uv-docker-e2e

## Context

This design resolves the 5 open questions the spec explicitly deferred, grounded in the
actual repository state (read directly, not assumed):

- `pyproject.toml` already defines `[build-system]` (setuptools), `[project]` (name `airflow-dags`,
  `requires-python >=3.12`), pytest + coverage config with `--cov-fail-under=80`. There is **no**
  dependency declaration for dev/test today — those live in `requirements-dev.txt`.
- `requirements-dev.txt` pins: `pytest>=7.4,<8.0`, `pytest-cov>=4.1,<5.0`, `pytest-mock>=3.12,<4.0`,
  `pytest-xdist>=3.5,<4.0`, `freezegun>=1.4,<2.0`, `coverage[toml]>=7.4,<8.0`.
- DAG code lives at repo root in `congress_videos/`, `examples/`, `utils/` (all confirmed present).
- `conftest.py` (repo root) fixes `sys.path`, stubs `yt_dlp`/`pytubefix`/`whisper`, and sets test
  env vars **at import time** — this is what makes `import utils.*` / `import congress_videos.*`
  work without installing the package. This matters for the uv decision (see Q1).
- Production `docker-compose.yml` uses a **pre-built** `image: my-airflow:latest`, `LocalExecutor`,
  external Postgres (`postgres_shared`), external networks (`postgres_infra_network`,
  `whisper_network`), NAS bind mount (`/volume1/docker/airflow/...`), and git-sync init containers
  (`init-dags`/`init-perms`). DAGs are served from `/opt/airflow/dags/repo`.
- **Critical:** the heavy DAG-import dependencies (`yt-dlp`, `openai-whisper`, `pytubefix`, `openai`,
  `beautifulsoup4`, `google-api-python-client`, `Pillow`, `PyPDF2`, `webrtcvad-wheels`,
  `apache-airflow-providers-postgres`) are **NOT baked into the `Dockerfile`**. The `Dockerfile` only
  adds `ffmpeg`, `curl`, `unzip`, `nodejs`, `npm`, `git`. The Python packages are injected at
  container start through `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml`. **Resolved by the user
  (see updated Note N1):** rather than replicate that ~15-min install for parity, the e2e now uses a
  **lightweight stock Airflow image with no `_PIP_ADDITIONAL_REQUIREMENTS`** — this directly shapes Q3
  (short timeout) and introduces one mechanical consequence tracked in **Note N7**.
- A third compose file exists: `docker-compose.prod.yml` (same shape as `docker-compose.yml`, branch
  `main`). See **Note N2** for its interaction with the `docker-compose*.yml` trigger glob.
- Airflow version is `apache/airflow:2.10.2` (Dockerfile base). `airflow dags list-import-errors`
  supports `--output json` in 2.10, and `db migrate` is the current migration command.

Scope is unchanged from the proposal/spec. Where reality diverges from the proposal's wording, this
document **flags it as a note for the user** (Notes N1–N6) rather than silently expanding or shrinking
scope.

## Goals / Non-Goals

- **Goals:** resolve the 5 deferred questions with concrete, scriptable answers; specify the exact
  file changes, uv command set, `docker-compose.test.yml` structure, and `openspec/config.yaml`
  YAML shape needed for `sdd-tasks` to proceed mechanically.
- **Non-Goals:** unchanged from spec — no CI, no production compose/Dockerfile edits, no task-level
  e2e, no lint/coverage threshold changes.

---

## Resolved Design Questions

### Q1 — uv dependency-group syntax

**Decision:** use **PEP 735 `[dependency-groups]`** with a single `dev` group, and mark the project
**non-packaged** with `[tool.uv] package = false`.

```toml
[dependency-groups]
dev = [
    "pytest>=7.4,<8.0",
    "pytest-cov>=4.1,<5.0",
    "pytest-mock>=3.12,<4.0",
    "pytest-xdist>=3.5,<4.0",
    "freezegun>=1.4,<2.0",
    "coverage[toml]>=7.4,<8.0",
]

[tool.uv]
package = false
```

**Canonical commands** (what config/docs reference):
- Install: `uv sync` (no flags)
- Run tests: `uv run pytest`

**Rationale / tradeoffs:**
- `[dependency-groups]` (PEP 735) is uv's current idiomatic mechanism for **local-only, non-published**
  tooling. `uv sync` installs the `dev` group **by default** (no `--group`/`--extra` flag needed) — this
  directly serves the proposal's core intent: *a single self-contained command with nothing to
  remember*. This is why `dev` (default-synced) beats a named `test` group that would force everyone
  to remember `uv sync --group test`.
- Rejected `[project.optional-dependencies]`: extras are semantically *published* package features
  installed with `--extra`; they pollute the package's public metadata and only make sense for an
  installable/distributed package. `airflow-dags` is a DAG repo, not a distributed library, so dev
  tooling does not belong in published extras.
- `[tool.uv] package = false` is **required**, not optional. `pyproject.toml` currently declares a
  setuptools `[build-system]` but the repo has **no installable package layout** (no `src/`, no
  `[tool.setuptools] packages`, DAGs are imported via `conftest.py` `sys.path` manipulation). Without
  `package = false`, `uv sync` would try to *build and install the project itself* and fail. Marking it
  a **virtual project** makes uv create the venv and install `dev` deps **without building the
  project** — which is exactly how the tests already run today (imports resolved by `conftest.py`).
- `uv.lock` is generated on first `uv sync` and committed as the lockfile companion to
  `pyproject.toml`, making the two the single source of truth (per proposal decision #1).

### Q2 — Exit codes for `scripts/test-airflow-e2e.sh`

**Decision — deterministic, distinct, documented codes:**

| Code | Constant | Meaning | Verify classification |
|------|----------|---------|-----------------------|
| `0`  | `EXIT_SUCCESS` | Stack healthy, `list-import-errors` empty | **pass** |
| `1`  | `EXIT_IMPORT_ERRORS` | Stack healthy, `list-import-errors` non-empty | **failed** |
| `2`  | `EXIT_HEALTH_TIMEOUT` | Scheduler/webserver never healthy within bounded timeout | **failed** |
| `3`  | `EXIT_UV_UNAVAILABLE` | `uv --version` failed during preflight (hard-fail, actionable) | **failed** |
| `4`  | `EXIT_DOCKER_UNAVAILABLE` | Docker / `docker compose` not available | **unavailable** (graceful) |
| `5`  | `EXIT_INTERNAL_ERROR` | Unexpected/internal error (fail-closed) | **failed** |

**Wrapper mapping rule (fail-closed):** the invoking flow (`sdd-verify` / the config-documented
snippet) maps exactly:
- `0` → **pass**
- `4` → **unavailable**
- **any other non-zero (including 127, signals, etc.)** → **failed**

**Rationale / tradeoffs:**
- `4` (`DOCKER_UNAVAILABLE`) is the **only** code mapped to `unavailable`. Everything else that is
  non-zero is treated as a real failure. This is deliberately **fail-closed**: an unexpected crash
  (e.g. `127 command not found`) can never be misread as "unavailable" and can never produce a false
  PASS — satisfying success criterion #6 ("never a false PASS").
- `uv` missing (`3`) is a **hard failure**, not `unavailable`, per proposal decision #2 and the spec's
  uv-hard-fail requirement — uv absence is a real problem to surface, not a condition to tolerate.
- Small contiguous integers keep the mapping trivially scriptable (`case "$rc" in`), and the named
  constants (declared `readonly` at the top of the script) keep the script self-documenting.

### Q3 — Bounded health-check timeout

**Decision:** two-stage bounded wait, all values env-overridable, defaults sized for a **lightweight
stock-image boot with no runtime pip install** (revised Note N1):

- DB init/migrate gate: handled by compose `depends_on: { airflow-init: { condition:
  service_completed_successfully } }` — no custom polling.
- Service health poll: `E2E_HEALTH_TIMEOUT` default **`120` seconds (2 min)**, poll interval
  `E2E_POLL_INTERVAL` default **`5` seconds**. The script polls `docker compose ps`/health status for
  **both** scheduler and webserver, prints a heartbeat line each interval (`waited Ns / 120s;
  scheduler=starting webserver=starting`), and on timeout prints which service(s) never became
  healthy plus their last log tail, then exits `2`.

**Rationale / tradeoffs:**
- With the lightweight stock image there is **no `_PIP_ADDITIONAL_REQUIREMENTS` install** at boot, so the
  dominant cost collapses to a one-time image pull plus Airflow's own startup (`db migrate`, scheduler and
  webserver becoming healthy). On a warm-image machine that is well under a minute; `120s` is generous
  enough to absorb a first-run image pull and a slow ephemeral box while staying **bounded and
  non-infinite** (spec requirement). This short timeout is a direct consequence of the decision-#3
  reversal: dropping the ~15-min torch/whisper install is exactly what makes it safe.
- The per-interval heartbeat is a hard requirement of this design: it is what prevents the *"stuck
  forever"* feel even during a slow boot — the caller always sees forward progress and a countdown.
- All three knobs (`E2E_HEALTH_TIMEOUT`, `E2E_POLL_INTERVAL`, and a per-service `start_period` in the
  compose healthchecks, now `60s`) are env-overridable so a fast local machine can shorten the loop and
  CI-less ephemeral boxes can lengthen it without editing the script.

### Q4 — Diff-detection mechanism for the conditional trigger

**Decision:** a dedicated, single-responsibility gate script `scripts/dag-paths-changed.sh` using
`git diff --name-only` against the merge-base with the integration branch.

Core logic (concrete, scriptable):

```bash
# Base ref resolution (integration branch is `dev` in this repo; prod syncs main->dev).
BASE_REF="${E2E_DIFF_BASE:-origin/dev}"
if ! git rev-parse --verify --quiet "$BASE_REF" >/dev/null; then
  BASE_REF="$(git rev-parse --verify --quiet origin/main >/dev/null && echo origin/main || echo HEAD~1)"
fi
MERGE_BASE="$(git merge-base HEAD "$BASE_REF" 2>/dev/null || echo "$BASE_REF")"

# Locked glob set -> extended-regex (do NOT expand; see spec).
PATTERN='^(congress_videos/|examples/|utils/|docker-compose[^/]*\.yml$|Dockerfile$)'

if git diff --name-only "$MERGE_BASE"...HEAD | grep -Eq "$PATTERN"; then
  exit 0   # DAG-relevant paths changed -> run e2e
else
  exit 1   # no DAG-relevant paths -> skip e2e
fi
```

**Contract:** exit `0` = run e2e, exit `1` = skip. `sdd-verify` runs `uv run pytest` **always**, then:

```bash
if bash scripts/dag-paths-changed.sh; then
  bash scripts/test-airflow-e2e.sh
fi
```

**Rationale / tradeoffs:**
- Base ref = `origin/dev`: the repo's integration branch is `dev` (compose `GIT_SYNC_BRANCH` default
  `dev`; the sole GitHub Action syncs `main`→`dev`). Fallbacks to `origin/main` then `HEAD~1` make the
  gate robust in detached/shallow checkouts and in subagent sandboxes where remotes may be absent.
- `A...B` (three-dot) diffs against the **merge-base**, so it reflects "what this change introduces
  relative to where it branched", not unrelated commits already on the base — the correct semantics
  for a change-scoped trigger.
- The glob→regex translation preserves the **locked** list exactly: `docker-compose[^/]*\.yml$`
  matches `docker-compose.yml`, `docker-compose.prod.yml`, and `docker-compose.test.yml` at repo root
  (matching the spec's `docker-compose*.yml`); `Dockerfile$` matches the root Dockerfile. The list is
  **not** expanded (no `pyproject.toml`, no `tests/**`) per the locked decision.
- Kept as its **own** script (not inlined into the e2e driver) so it is independently testable and so
  `test-airflow-e2e.sh` stays single-responsibility (a developer can force-run the e2e script locally
  regardless of what changed). The known false-negative for indirect dependency-driven breakage
  remains a documented, accepted limitation (spec scenario).

### Q5 — Teardown / disposability mechanism

**Decision:** three layered guarantees so re-runs are always idempotent and disposable:

1. **No persistent named volumes.** Postgres data uses `tmpfs` (RAM-backed, vanishes on container
   stop); Airflow logs use an **anonymous** volume. `docker-compose.test.yml` declares **no**
   top-level `volumes:` that persist across runs.
2. **Isolated project name.** All commands use `-p airflow-dags-e2e` so the ephemeral stack never
   collides with or mutates the production stack's containers/volumes/networks.
3. **Unconditional teardown via `trap`.** The script installs a bash `trap` on `EXIT` (covers success,
   failure, health-timeout, and `INT`/`TERM`) that runs:
   ```bash
   docker compose -p airflow-dags-e2e -f docker-compose.test.yml down -v --remove-orphans --timeout 30
   ```

**Rationale / tradeoffs:**
- `down -v --remove-orphans` guarantees removal of containers, networks, and any (anonymous or named)
  volumes even if a future edit adds one — belt-and-suspenders against state leaking between runs.
- Running teardown from a `trap` (not just at the end of the happy path) is what makes the
  **timeout/failure paths** also clean up — directly satisfying the spec's "tears down the stack
  before exiting" for the health-timeout scenario.
- `tmpfs` for Postgres also **speeds up** the DB and leaves zero disk residue, reinforcing
  disposability at the storage layer independent of the `trap`.
- The `trap` handler is written to preserve the original exit code (capture `rc=$?` first, tear down,
  then `exit "$rc"`), so teardown never masks the real result code from Q2.

---

## File Changes

| File | New/Changed | Responsibility |
|------|-------------|----------------|
| `pyproject.toml` | changed | Add `[dependency-groups] dev` (6 pkgs) + `[tool.uv] package = false`. Do **not** touch pytest/coverage config (`--cov-fail-under=80` stays). **Post-apply correction:** also add `[project.dependencies]` with the actual runtime deps (`utils/**`/`congress_videos/**` module-scope imports) — union of `congress_videos/requirements.txt` + docker-compose `_PIP_ADDITIONAL_REQUIREMENTS` + discovered gaps (`apache-airflow==2.10.2`, `psycopg2-binary`, `numpy`, `python-dotenv`), excluding `openai-whisper` (lazy import, Python 3.14-incompatible build chain). Without this, `dev`-only was insufficient and caused 11 real collection errors. See `apply-progress.md` CORRECTION section. |
| `uv.lock` | new | Generated by `uv sync`; committed alongside `pyproject.toml` as the lock source of truth. Regenerated after the runtime dependency addition above. |
| `requirements-dev.txt` | deleted | Removed once `dev` group verified equivalent (proposal decision #1). Grep repo for stray references first (see Note N5). |
| `CLAUDE.md` | changed | Rewrite "Development Environment" section to uv (zero `conda`); add "Testing" note. See command set below. |
| `openspec/config.yaml` | changed | `rules.apply/verify.test_command`, `testing.runner.command`, `testing.commands.unit` → uv; add `testing.commands.e2e`. Exact YAML below. |
| Engram `sdd-init/airflow-dags` | changed | Superseding update: replace conda commands with uv, record conditional e2e capability. Must **supersede**, not append (risk in proposal). |
| `docker-compose.test.yml` | new | Ephemeral e2e stack. Exact structure below. Adds a `./e2e/stubs:/opt/airflow/stubs:ro` mount + `PYTHONPATH=/opt/airflow/stubs` (N7 stubs). |
| `e2e/stubs/sitecustomize.py` | new | No-op stub modules for heavy top-level imports (N7). Container analog of `conftest.py`; auto-run by Python `site` at every interpreter/parse-subprocess startup. Exact package list + shape below. |
| `scripts/test-airflow-e2e.sh` | new | Start stack (stock image pull) → poll health (Q3) → `list-import-errors` → assert empty → teardown (Q5) → exit codes (Q2). uv + docker preflight. |
| `scripts/dag-paths-changed.sh` | new | Diff-detection gate (Q4). Exit 0 = run e2e, 1 = skip. |

**Untouched (guarded):** `docker-compose.yml`, `docker-compose.prod.yml`, production `Dockerfile`
runtime behavior, `.github/workflows/**`, DAG business logic, lint/coverage thresholds.

---

## uv Command Set for CLAUDE.md

Replace the conda "Development Environment" block with:

```markdown
## Development Environment
- **Package manager**: uv (no conda; no manual venv activation required)
- **Install / sync dependencies**: `uv sync`  (creates .venv and installs the dev/test group)
- **Run a script**: `uv run python <script.py>`
- **Add a runtime dependency**: `uv add <package>`
- **Add a dev/test dependency**: `uv add --dev <package>`
- If `uv` is not installed, install it (https://docs.astral.sh/uv/) — commands hard-fail with an
  actionable message rather than silently falling back.

## Testing
- **Unit tests**: `uv run pytest`  (runs on every `sdd-verify`)
- **Parallel**: `uv run pytest -n auto`
- **Docker e2e smoke test**: `bash scripts/test-airflow-e2e.sh` — boots an ephemeral Airflow stack and
  asserts `airflow dags list-import-errors` is empty. Runs automatically during `sdd-verify` only when
  the change touches `congress_videos/**`, `examples/**`, `utils/**`, `docker-compose*.yml`, or
  `Dockerfile`. If Docker is unavailable it reports `unavailable` (not a failure); run it manually
  before merge in that case.
```

---

## docker-compose.test.yml Structure

Ephemeral, self-contained (no `.env` dependency), no NAS / external networks / git-sync.

```yaml
# Ephemeral e2e smoke-test stack. NOT for production. Disposable state only.
x-airflow-common: &airflow-common
  image: apache/airflow:2.10.2-python3.12   # lightweight stock image; matches prod Airflow 2.10.2 (Dockerfile base). No build, no _PIP_ADDITIONAL_REQUIREMENTS (proposal decision #3, revised)
  environment: &airflow-env
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__FERNET_KEY: "T1Qut8bLd0Bpi6r6l3wYK0eXf2m0m0m0m0m0m0m0m0="  # throwaway test key
    AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    # NOTE (N1/N7 — RESOLVED): NO _PIP_ADDITIONAL_REQUIREMENTS — project-specific heavy deps are
    # intentionally absent (lightweight-image tradeoff). Parse-time top-level heavy imports are
    # satisfied by no-op STUB modules shipped in ./e2e/stubs and auto-loaded at interpreter startup
    # via PYTHONPATH + sitecustomize.py (the container analog of conftest.py). See the
    # "Stub modules for e2e (N7 resolution)" section below and Note N7.
    PYTHONPATH: /opt/airflow/stubs   # site imports /opt/airflow/stubs/sitecustomize.py -> registers heavy-package stubs in sys.modules before DagBag parses
    # Neutralize env vars DAGs read at import time (parity with conftest test env)
    OPENAI_API_KEY: "test-openai-key-not-real"
    YOUTUBE_API_KEY: "test-youtube-api-key"
    REAP_API_KEY: "test-reap-api-key"
    POSTGRES_HOST: "postgres"
    POSTGRES_PORT: "5432"
    POSTGRES_DB: "airflow"
    POSTGRES_USER: "airflow"
    POSTGRES_PASSWORD: "airflow"
    POSTGRES_SCHEMA: "public"
  volumes:
    - ./:/opt/airflow/dags:ro           # local checkout bind-mount (no git-sync); read-only
    - ./e2e/stubs:/opt/airflow/stubs:ro # no-op stub modules for heavy top-level imports (N7); mounted OUTSIDE the dags folder so DagBag never parses them
    - airflow_logs:/opt/airflow/logs
  # default bridge network only (no external networks)

services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    tmpfs:
      - /var/lib/postgresql/data   # ephemeral, no named volume
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U airflow"]
      interval: 5s
      timeout: 5s
      retries: 20

  airflow-init:
    <<: *airflow-common
    depends_on:
      postgres:
        condition: service_healthy
    entrypoint: /bin/bash
    user: "50000:0"
    command:
      - -c
      - "airflow db migrate && airflow users create --username admin --password admin --firstname a --lastname b --role Admin --email a@b.c || true"
    restart: "no"

  airflow-scheduler:
    <<: *airflow-common
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    command: scheduler
    healthcheck:
      test: ["CMD-SHELL", "airflow jobs check --job-type SchedulerJob --hostname $(hostname)"]
      interval: 15s
      timeout: 10s
      retries: 20
      start_period: 60s

  airflow-webserver:
    <<: *airflow-common
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    command: webserver
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 15s
      timeout: 10s
      retries: 20
      start_period: 60s

volumes:
  airflow_logs:    # anonymous-style; removed by `down -v` every run
```

**Assertion command** (inside the script, after both services healthy):

```bash
docker compose -p airflow-dags-e2e -f docker-compose.test.yml \
  exec -T airflow-scheduler airflow dags list-import-errors --output json
# pass iff parsed JSON == []  (empty). Non-empty -> exit 1 (EXIT_IMPORT_ERRORS).
```

Using `--output json` and asserting `[]` gives an exact, parseable pass/fail condition (spec requires
"empty result = pass"), avoiding brittle text scraping of the table format.

---

## Stub modules for e2e (N7 resolution)

**Problem (N7).** The lightweight image has no project-specific heavy packages. Several project modules
import heavy packages at **module top level**, so the import runs at DAG **parse** time. Without the real
packages, `airflow dags list-import-errors` would report `ModuleNotFoundError` purely because a heavy dep
is absent — exactly the case the user chose **not** to gate on.

**Decision (option (b) from N7, chosen by the user).** Ship **minimal no-op stub modules** for those heavy
packages inside the e2e image, mirroring what `conftest.py` already does for unit tests — the same testing
philosophy applied one layer down: at the container/interpreter level instead of inside the pytest process.
The smoke test then validates DAG/project **code** parsing (import wiring, top-level DAG construction), not
the behavior of the real heavy libraries.

### Confirmed package list (verified by repo grep, not inherited from the prior summary)

Grep of `utils/**` and `congress_videos/**` for module-top-level (column-1) imports of non-Airflow,
non-stdlib packages. **These MUST be stubbed** (absent from the lightweight image, imported at parse time):

| Stub (import name) | Distribution | Confirmed top-level site(s) |
|---|---|---|
| `yt_dlp` | yt-dlp | `utils/youtube_downloader.py:13` |
| `openai` (+ `OpenAI`) | openai | `utils/ai_chapter_analyzer.py:12`, `congress_videos/modules/thumbnail_generator.py:17` |
| `PIL` (`Image`,`ImageDraw`,`ImageFont`,`ImageEnhance`) | Pillow | `congress_videos/modules/thumbnail_generator.py:16` |
| `bs4` (`BeautifulSoup`) | beautifulsoup4 | `congress_videos/modules/youtube/youtube_channel.py:15` |
| `googleapiclient` + `.discovery` (`build`) + `.http` (`MediaFileUpload`) | google-api-python-client | `youtube_channel.py:16`, `utils/youtube_helpers.py:19-20` |
| `PyPDF2` (`PdfReader`) | PyPDF2 | `youtube_channel.py:17` |
| `google.auth.transport.requests` (`Request`) + `google.oauth2.credentials` (`Credentials`) | google-auth | `utils/youtube_helpers.py:17-18` |
| `numpy` | numpy | `congress_videos/modules/vad_helpers.py:29` |

> **Two of these were MISSING from the prior N7 summary** and were caught by re-grepping: `numpy`
> (top-level in `vad_helpers.py`, reached at parse time by `youtube_channel_monitor_dag.py` →
> `from congress_videos.modules.vad_helpers import ...`) and the `google.auth`/`google.oauth2`
> (`google-auth`) top-level imports in `utils/youtube_helpers.py`. The prior summary only named
> `google-api-python-client`.

**Do NOT stub (Airflow's own base image already provides these):** `requests`, `urllib3`, `psycopg2`
(Airflow itself connects to its metadata DB via `postgresql+psycopg2://`, so `psycopg2` is present).

**Lazy / function-level imports — NOT parse-blocking (no stub required for the parse check):** `whisper`
(`utils/whisper_helpers.py:144`), `webrtcvad` (`vad_helpers.py:236`), `pytubefix` +
`pytubefix.cli` (`utils/youtube_downloader.py:110-111`), `moviepy` (`youtube_downloader.py:774`), `dotenv`
(`utils/env_loader.py:11`). `pytubefix` and `whisper` MAY still be stubbed for 1:1 parity with `conftest.py`
(cheap, keeps the two lists aligned). This is why the ~15-min torch/whisper install was never needed for a
parse-level check (Note N1).

> **Note (broad DAGS_FOLDER):** the repo is mounted whole at `/opt/airflow/dags`, so DagBag safe-mode also
> scans non-DAG trees. `congress_videos/scripts/generate_youtube_token.py` imports `google_auth_oauthlib`
> (also absent). It is a helper script, not a DAG, and is not expected to be parsed — but if it (or
> `tests/**`, `openspec/**`) surfaces spurious parse errors, the cleaner complementary fix is a root
> `.airflowignore` scoping DagBag to `congress_videos/`, `examples/`, `utils/` rather than growing the stub
> list. Flagged for `sdd-tasks` as a small hardening step, not required to close N7.

### Mechanism — `sitecustomize.py` on `PYTHONPATH` (chosen over a dir of physical stub files)

Layout (new, at repo root, **outside** the DAG trees so it is never itself parsed as a DAG):

```
e2e/
  stubs/
    sitecustomize.py   # registers each stub into sys.modules; mirrors conftest.py's _install_stub_module
```

Wiring in `docker-compose.test.yml` (already added above): mount `./e2e/stubs:/opt/airflow/stubs:ro` and set
`PYTHONPATH=/opt/airflow/stubs`. Python's `site` module auto-imports `sitecustomize` from any directory on
`sys.path` at interpreter startup, so it runs **before** Airflow parses the DAG folder — and, crucially, in
every `DagFileProcessor` subprocess too (they inherit `PYTHONPATH`). This is the exact container analog of
how `conftest.py` runs before test imports.

`sitecustomize.py` reuses `conftest.py`'s proven shape (an `_install_stub_module(name, attrs)` that does
`sys.modules.setdefault(...)` with a fresh `types.ModuleType`). Concrete skeleton for `sdd-tasks`:

```python
# e2e/stubs/sitecustomize.py  — no-op stubs so DAG *code* parses without the real heavy libs (N7).
# Container analog of conftest.py. Only registers modules Airflow itself does not use.
import sys, types

def _stub(name, **attrs):
    if name in sys.modules:            # never clobber a real package if one is present
        return sys.modules[name]
    mod = types.ModuleType(name)
    for k, v in attrs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod

_stub("yt_dlp", YoutubeDL=object, DownloadError=type("DownloadError", (Exception,), {}))
_stub("openai", OpenAI=object)
_stub("PIL", Image=object, ImageDraw=object, ImageFont=object, ImageEnhance=object)
_stub("bs4", BeautifulSoup=object)
_stub("googleapiclient"); _stub("googleapiclient.discovery", build=lambda *a, **k: None)
_stub("googleapiclient.http", MediaFileUpload=object)
_stub("PyPDF2", PdfReader=object)
_stub("numpy")
# google-auth: register ONLY the submodules, never a fake top-level `google`, so a real `google`
# namespace (e.g. google.protobuf pulled in by Airflow/grpc) is NOT shadowed.
_stub("google.auth"); _stub("google.auth.transport"); _stub("google.auth.transport.requests", Request=object)
_stub("google.oauth2"); _stub("google.oauth2.credentials", Credentials=object)
# parity with conftest.py (function-level in prod, harmless to stub):
_stub("pytubefix", YouTube=object); _stub("pytubefix.cli", on_progress=lambda *a, **k: None)
_stub("whisper", load_model=lambda *a, **k: None)
```

**Why `sitecustomize` (sys.modules injection) and NOT a `stubs/` dir of physical `.py` files on `PYTHONPATH`:**
a physical `google/__init__.py` on `PYTHONPATH` would **shadow** the real `google` namespace package
(`google.protobuf`, etc. that Airflow/grpc may load), breaking the runtime. Registering only
`google.auth`/`google.oauth2` into `sys.modules` leaves the real `google` namespace intact — which is exactly
why `conftest.py` injects into `sys.modules` instead of dropping shadowing files on the path. Using one
`sitecustomize.py` for all stubs keeps the mechanism uniform and namespace-safe.

### Maintenance note (manual, not automation to build now)

The e2e stub set is a **superset** of the `conftest.py` stub set (e2e parses more top-level imports than unit
tests exercise). When project code gains a **new** module-top-level import of a heavy package:
1. add a stub entry to `e2e/stubs/sitecustomize.py` (required for the e2e parse check), and
2. add it to `conftest.py` too **iff** unit tests import that module.

Until then, a new unstubbed heavy top-level import surfacing as `ModuleNotFoundError` in `list-import-errors`
is an **accepted known limitation** (add a stub), not a design defect — see the spec requirement/scenarios.
An optional future guard (a tiny test asserting the two stub lists stay in sync) is a possible follow-up,
**not** in scope for this change.

---

## openspec/config.yaml — Exact New YAML Shape

Change the following keys (only these; leave `strict_tdd`, `context`, `quality`, etc. as-is):

```yaml
rules:
  # ...unchanged keys above...
  apply:
    test_command: "uv run pytest"
  verify:
    test_command: "uv run pytest"
testing:
  detected: "2026-07-23"
  runner:
    command: "uv run pytest"
    framework: "pytest"
  layers:
    unit: "uv run pytest"
    integration: ""
    e2e: "bash scripts/test-airflow-e2e.sh"
  commands:
    unit:
      - scope: "."
        command: "uv run pytest"
        framework: "pytest"
    integration: []
    e2e:
      - scope: "."
        command: "bash scripts/test-airflow-e2e.sh"
        framework: "docker-compose"
        gate_command: "bash scripts/dag-paths-changed.sh"
        trigger_globs:
          - "congress_videos/**"
          - "examples/**"
          - "utils/**"
          - "docker-compose*.yml"
          - "Dockerfile"
        unavailable_exit_code: 4
        notes: >
          Runs during sdd-verify only when the change diff touches one of trigger_globs
          (decided by gate_command: exit 0 = run, exit 1 = skip). Unit tests run every verify
          regardless. Docker unavailable => exit code 4 => status `unavailable` (not failed);
          any other non-zero => failed; 0 => pass. uv missing => exit 3 hard-fail.
```

`gate_command`, `trigger_globs`, `unavailable_exit_code`, and `notes` are **new** sub-keys under the
e2e entry. They encode Q2/Q4 machine-readably so `sdd-verify` reads them from config instead of
hard-coding.

---

## Engram sdd-init/airflow-dags Update (superseding)

Replace (not append) the conda-era test guidance:
- `test_command`: `conda run -n airflow python -m pytest` → `uv run pytest`
- Record the new conditional e2e capability (script, gate script, trigger globs, exit-code contract,
  `unavailable` semantics).
- Explicitly mark the old conda "Learned" note as **superseded** so future SDD phases never see mixed
  conda/uv guidance (proposal risk: "engram note must supersede, not append").

---

## Data / Control Flow (sdd-verify)

```
sdd-verify
  ├─ uv preflight: `uv --version` -> if fail, hard-fail (exit 3), actionable message
  ├─ ALWAYS: `uv run pytest`  -> unit pass/fail
  └─ CONDITIONAL: `bash scripts/dag-paths-changed.sh`
        ├─ exit 1 (no DAG paths) -> skip e2e; verify result = unit only
        └─ exit 0 (DAG paths)    -> `bash scripts/test-airflow-e2e.sh`
              ├─ docker preflight fail -> exit 4 -> e2e status = UNAVAILABLE (not failed)
              ├─ up (-p airflow-dags-e2e, stock image pull) [trap EXIT: down -v --remove-orphans]
              ├─ poll health <= E2E_HEALTH_TIMEOUT (heartbeat each E2E_POLL_INTERVAL)
              │     └─ timeout -> exit 2 (which service + log tail)
              ├─ `airflow dags list-import-errors --output json`
              │     ├─ []      -> exit 0 (pass)
              │     └─ non-[]  -> exit 1 (fail, print errors)
              └─ teardown always via trap (success/fail/timeout/signal)
```

---

## Notes for the User (flagged, not silently actioned)

- **N1 — RESOLVED by the user: lightweight stock image chosen over full-parity image.**
  The original proposal decision #3 said the test stack "builds from the same production Dockerfile (same
  yt-dlp, openai-whisper…)". Design review surfaced that those packages are **not** in the Dockerfile —
  they are injected via `_PIP_ADDITIONAL_REQUIREMENTS` in `docker-compose.yml`, so true parity would
  require replicating that env var, which costs ~15 min per run (torch/whisper installed on every
  container each boot — the reason Q3 originally used a 900s timeout). The user reviewed this cost and
  **deliberately reversed decision #3**: the e2e now uses a **lightweight stock
  `apache/airflow:2.10.2-python3.12` image with no `_PIP_ADDITIONAL_REQUIREMENTS`** and a fast (~2 min)
  boot (Q3 timeout now 120s). **Accepted, documented tradeoff:** the smoke test proves DAG *code* parses
  against Airflow's own bundled providers only; it does **not** prove parsing against project-specific
  heavy deps (`yt-dlp`, `openai-whisper`, `google-api-python-client`, etc.). A **heavy-dependency-aware
  e2e is now an explicit non-goal / future follow-up**, no longer a residual risk of this design. See
  **Note N7** — the one mechanical consequence (top-level heavy imports) is now **resolved** via no-op stub
  modules (see the "Stub modules for e2e (N7 resolution)" section), which `sdd-tasks` implements.
- **N2 — `docker-compose.prod.yml` also exists** and the trigger glob `docker-compose*.yml` matches it,
  `docker-compose.yml`, **and** the new `docker-compose.test.yml`. So a change that only edits
  `docker-compose.test.yml` will self-trigger the e2e step (harmless, arguably desirable). Also, the
  non-goals name only `docker-compose.yml`/`Dockerfile` as untouched — `docker-compose.prod.yml` should
  be treated as untouched too. Not changing anything; just confirming the boundary.
- **N3 — Webserver is not strictly required** for `airflow dags list-import-errors` (the scheduler/CLI
  against the DB is enough). The spec **mandates** booting scheduler **and** webserver and waiting for
  both healthy, so this design honors that. A future slice could drop the webserver to roughly halve
  boot cost. Flagging the optionality, not changing scope.
- **N4 — Fernet key / test creds are hardcoded throwaways** in `docker-compose.test.yml` so the stack
  never depends on `.env`. These are non-secret, test-only values. Confirming this is intended.
- **N5 — Before deleting `requirements-dev.txt`,** grep the repo for references (docs, Makefile, any
  script). CI is out of scope, but a doc reference should be updated in the same change to avoid a
  dangling pointer. `sdd-tasks` should include this as an explicit step.
- **N6 — `conftest.py` stubs `yt_dlp`/`whisper`/`pytubefix` for unit tests.** With the lightweight-image
  reversal, the e2e stack **no longer installs the real heavy packages** either — so both the unit path
  (stubbed) and the e2e path (absent) run without them. This is why the e2e proves DAG *code* parsing but
  not real heavy-dependency imports (see N1/N7). Worth stating so no one "re-adds" the heavy deps to the
  test stack expecting full parity.
- **N7 — RESOLVED (user decision): stub heavy top-level imports in the e2e image.** The reconciliation the
  earlier revision correctly left blocking is now closed: the e2e image ships **no-op stub modules** for
  every confirmed heavy package imported at module top level (see the "Stub modules for e2e (N7 resolution)"
  section for the grep-verified list, the `e2e/stubs/sitecustomize.py` mechanism, the `PYTHONPATH` wiring,
  and the maintenance note). This mirrors `conftest.py`'s unit-test stubs one layer down, so
  `list-import-errors == []` stays meaningful (real DAG-code correctness) without installing the heavy deps.
  Option (a) (`.airflowignore` scoping) is retained only as a small complementary hardening step for the
  broad DAGS_FOLDER mount, not as the N7 resolution. Original analysis kept below for the record.

  Dropping `_PIP_ADDITIONAL_REQUIREMENTS` means `yt-dlp`,
  `openai`, `beautifulsoup4`, `google-api-python-client`, `PyPDF2`, `Pillow`, `pytubefix` etc. are
  **absent**. Several importable modules import these at **module top level** (verified in the repo:
  `utils/youtube_downloader.py` `import yt_dlp`; `utils/ai_chapter_analyzer.py` `import openai`;
  `congress_videos/modules/thumbnail_generator.py` `from openai import OpenAI` / `from PIL import ...`;
  `congress_videos/modules/youtube/youtube_channel.py` `from bs4 ...` / `from googleapiclient ...` /
  `from PyPDF2 ...`; `utils/youtube_helpers.py` `from googleapiclient ...`). (`whisper`/`webrtcvad` are
  imported lazily *inside functions*, so they do not affect parse time — which is partly why the heavy
  torch install was never needed for a parse-level check.) If Airflow's DagBag parses any DAG-definition
  file that transitively imports one of the top-level heavy modules, `airflow dags list-import-errors`
  will report a `ModuleNotFoundError` — i.e. the smoke test would fail purely because a heavy dep is
  absent, which is exactly the case the user chose **not** to gate on. The three candidate reconciliations
  considered were: (a) scoping what Airflow parses (`AIRFLOW__CORE__DAGS_FOLDER` pointed at the real
  DAG-entry directory and/or a `.airflowignore`) and confirming those entry files defer heavy imports into
  task callables (Airflow best practice); (b) shipping tiny stub shims for the heavy modules in the e2e
  image, mirroring what `conftest.py` already does for unit tests, so absent packages don't mask real code
  errors; or (c) an explicit allowlist that treats "ModuleNotFoundError for a known heavy package" as
  non-blocking. **The user chose (b)** — stub shims — now specified concretely in the "Stub modules for e2e
  (N7 resolution)" section; (a) is retained only as optional complementary hardening. The assertion
  `list-import-errors == []` therefore stays meaningful because heavy-dep-absence import errors are
  excluded by the stubs.

  > **NOTE:** the paragraph above is the original blocking analysis, retained for the record; it is
  > **resolved**, not open. The grep-verified package list in the "Stub modules for e2e" section is
  > authoritative and also adds `numpy` and `google.auth`/`google.oauth2`, which this original prose omitted.

## Risks (design-level)

- **Narrow parity from the lightweight image** (N1) — the smoke test does not exercise project-specific
  heavy deps; DAG breakage that only manifests once `yt-dlp`/`openai-whisper`/`google-api-python-client`
  are present is out of this slice's reach. Accepted, documented tradeoff; heavy-dep-aware e2e is a future
  follow-up.
- **Top-level heavy imports vs. absent packages** (N7) — **RESOLVED.** Several importable modules import
  heavy deps at module top level, so `list-import-errors` would otherwise report `ModuleNotFoundError` for
  absent heavy packages. Closed by shipping no-op **stub modules** in the e2e image
  (`e2e/stubs/sitecustomize.py` on `PYTHONPATH`), mirroring `conftest.py`. Residual, accepted limitation: a
  *new* top-level import of an as-yet-unstubbed heavy package in a future DAG would need a stub entry added
  (documented in the spec as a known limitation, not a defect).
- **Path-glob false negatives** — accepted, documented limitation (spec).

## Migration / Rollout

Additive + revertible per proposal Rollback section. Order for `sdd-tasks`: (1) `pyproject.toml`
group + `[tool.uv] package=false`, generate `uv.lock`, verify `uv run pytest` green; (2) delete
`requirements-dev.txt` (after N5 grep); (3) `CLAUDE.md` + `openspec/config.yaml` + engram update;
(4) `docker-compose.test.yml` + `e2e/stubs/sitecustomize.py` (N7 heavy-import stubs) +
`scripts/dag-paths-changed.sh` + `scripts/test-airflow-e2e.sh`; (5) verify a DAG-touching diff triggers
e2e, that `list-import-errors` is empty against the stubbed image (no `ModuleNotFoundError` for a stubbed
heavy package), and that a docs-only diff does not.
