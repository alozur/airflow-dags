# Exploration: VPS ephemeral NAS inputs (auto pull-then-release for congress_videos DAGs)

Change: `vps-ephemeral-nas-inputs`
Date: 2026-09-11
Phase: explore (read-only; grounded against `vps-dev-foundation` @ 71ed3e7)
Engram mirror: topic `sdd/vps-ephemeral-nas-inputs/explore` (observation #2929, project `airflow-dags`)

---

## Current State

Two DAGs exist: `nas_archive` (daily 04:00 UTC: DB-completeness-gated rsync push + verify + age-gated `prune_local` + `.nas_archived.json` marker; sync cap 20/run, prune cap 2/run, `NAS_ARCHIVE_MIN_AGE_DAYS=14`) and `nas_fetch` (on-demand, operator-triggered with explicit `video_id`(s); marker-mode or SSH-discovery fallback against `settings.root` then `settings.legacy_root`; pulls, verifies, refreshes mtime, removes the marker in marker mode).

Three consumer DAGs (`speaker_turns_dag.py`, `trim_proposals_dag.py`, `speaker_turn_videos_dag.py`) call `nas_fetch.is_archived_elsewhere()` when the local source video is missing and return `status: "skipped_archived"`; none of them trigger `nas_fetch`. Two more consumers (`reap_clip_preparer_dag.py`, `speaker_turn_prepare_dag.py`) have no NAS awareness at all (no `nas_fetch` / `is_archived_elsewhere` hits), matching follow-up item 8 in Engram topic `architecture/vps-migration-todo` (project `homeserver-config`, obs #2904).

VPS disk: one Docker named volume `data` with no quota, sharing the 99 GB host root disk with Postgres volumes, logs and pre-seeded ML model caches; ~28 GB free as of the 2026-09-11 cutover. There is no reference counting and no disk-pressure-aware scheduling: a video's local material is either "present" or "archived+pruned" (marker file).

## Affected Areas

- `congress_videos/nas_fetch_dag.py`, `congress_videos/modules/nas_fetch.py`: fetch / verify / refresh-retention / marker-remove primitives any auto-pull design reuses.
- `congress_videos/nas_archive_dag.py`, `congress_videos/modules/nas_archive.py`: prune / verify / marker-write and the DB-completeness query (`_query_complete_video_ids`) a reclaim design should reuse as its safety gate.
- `congress_videos/speaker_turns_dag.py`, `trim_proposals_dag.py`, `speaker_turn_videos_dag.py`: today's `skipped_archived` branches are the hook points to replace with an inline fetch.
- `congress_videos/reap_clip_preparer_dag.py`, `speaker_turn_prepare_dag.py`, `congress_videos/modules/vad_helpers.py` (`_find_source_video`, `_find_source_video_any_date`): need the same NAS-awareness hook added from scratch.
- `congress_videos/config/paths.py`: the three local directory shapes (`downloads/{date}/{video_id}`, `{channel}/{video_id}`, legacy top-level `{video_id}`) any fetch / reclaim must respect.
- `deploy/vps-dev/compose.yml`, `deploy/vps-dev/test_contract.py`: `NAS_ARCHIVE_*` / `NAS_FETCH_LEGACY_ROOT` env contract already wired; no compose change needed for the recommended approach.
- `homeserver-config` `ansible/vps-dev/vars.yml`, `tasks/nas_sync.yml`: SSH identity + env var rendering; already generic enough, no new Ansible task required.
- `tests/congress_videos/test_nas_archive.py`, `test_nas_archive_dag.py`, `test_nas_fetch.py`, `test_nas_fetch_dag.py`: existing pure-function + DAG-callable TDD patterns to extend.

## Approaches

1. **Per-DAG "stage → process → release" with reference counting.** Each consumer fetches inline; a shared cleanup step deletes only once every consumer releases its reference.
   - Pros: fetch exactly when needed; no new scheduled DAG.
   - Cons: cross-DAG reference counting is a real race (`max_active_runs=1` is per-DAG, not global); new code in 5 call sites; leaked local copies on mid-run crash; largest diff, likely over the 400-line PR budget in one shot.
   - Effort: High.

2. **Central on-demand `nas_fetch` (existing) + a new DB-gated `nas_reclaim` DAG.** Consumer DAGs call the fetch primitive inline instead of returning `skipped_archived`; a new scheduled `nas_reclaim` DAG reuses `nas_archive`'s DB-completeness query plus `prune_local` / `verify_synced` to re-prune local copies of videos that are archived and verified on the NAS and have nothing pending.
   - Pros: reuses every existing tested primitive (`fetch_rsync_command`, `verify_fetched`, `prune_local`, `is_archived`, the DB completeness query as the de facto reference count); mirrors `nas_archive`'s sync/prune cadence split; small per-DAG diffs; the two NAS-unaware DAGs get the same hook the three aware DAGs already have.
   - Cons: reclaim is schedule-gated rather than triggered right after last use (mitigated by running `nas_reclaim` more often than `nas_archive`); inherits `nas_archive`'s known completeness-query gap (SUCCESS when every video fails) until fixed.
   - Effort: Medium. **Recommended.**

3. **NFS/SSHFS mount instead of copies.** Mount the NAS so consumer DAGs read NAS-backed paths.
   - Pros: removes the fetch / archive / prune state machine and the disk budget problem.
   - Cons: VPS and NAS talk only over a Tailscale tailnet with documented silent breakage (Engram `nas-tailscale-ip-change-dns`, `nas-tailnet-subnet-route-traefik`); ffmpeg / diarization are latency-sensitive for random-access I/O over a WAN mount; loses the byte-verified rsync integrity check; breaks the "legacy_root is pull-only" invariant; large infra change outside this repo.
   - Effort: High, infra-heavy.

## Recommendation

Approach 2. Keep `nas_fetch`'s tested primitives as the fetch mechanism, wire all 5 consumer DAGs to call it inline instead of skipping, and add a new `nas_reclaim` DAG reusing `nas_archive`'s completeness query and prune / verify functions. Zero Ansible changes. Fix the completeness-query bug (follow-up 8) before or alongside, since 5 DAGs will depend on that gate.

## Disk Budget Reality

- 99 GB disk, ~28 GB free; single unquotaed Docker `data` volume shared with Postgres, logs and model caches.
- Must stay local: Postgres data, YouTube tokens / cookies, `assets/`, `thumbnails/` (`MIRROR_ONLY_DIRS`, never pruned), model caches.
- Reclaimable: raw downloads (`downloads/{date}/{video_id}/*.mp4`, biggest item, whole-dir `rmtree`), derived chapter / turn `*.mp4` (sidecars kept), legacy top-level `{video_id}` tree.
- Scale: the NAS-only diarization backlog is ~75 GiB / 31 videos, about 2.4 GiB per video; the VPS can hold roughly 10 to 12 such videos at once, so frequent automatic reclaim is a necessity.

## Homeserver-config (Ansible) Changes

None required: `NAS_ARCHIVE_*` and `NAS_FETCH_LEGACY_ROOT` are rendered by `ansible/vps-dev/vars.yml` + `tasks/nas_sync.yml`; a new `nas_reclaim` DAG reuses the same `ArchiveSettings` contract. Optional out-of-scope follow-up: a host disk-usage alarm via Beszel / Uptime Kuma.

## Open Questions (recommended defaults)

1. Which DAGs get auto-fetch in v1? Default: all 5; `nas_reclaim` as a separate PR / task.
2. Reclaim retention window: run `nas_reclaim` more often than `nas_archive` (for example every 4 h); the grace window for fetched copies is a design decision (14 days would defeat the purpose of freeing disk after use).
3. Reclaim safety gate: reuse `nas_archive`'s exact DB-completeness query, accepting some transient re-fetch churn for multi-consumer videos.
4. Fix the SUCCESS-on-all-failed bug (follow-up 8) in the same PR set.
5. `thumbnail_republish abandon=True` on NAS-only trees stays out of scope.

## Risks

- `nas_archive`'s completeness query has a known correctness gap that a reclaim gate inherits until fixed.
- Concurrent consumer DAGs needing the same `video_id` could each trigger a fetch; rsync + verify is idempotent, but concurrent `prune_local` right after is not analyzed and needs explicit locking / marker guards in design.
- Fetch-on-demand inside consumer tasks changes their runtime profile (multi-GB rsync inside a previously skip-only task); `_RSYNC_TIMEOUT_SECS=3600` exists but per-task timeout / retry policy needs review.
- Tailnet outages now block 5 DAGs instead of producing a skip.
- 400-line PR budget: 5 consumer DAGs + a new DAG in one PR is oversized; slice into chained PRs.

## Ready for Proposal

Yes: proceed to `sdd-propose` with approach 2 pre-selected and the defaults above treated as confirmed.
