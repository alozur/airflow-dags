# Tasks: VPS ephemeral NAS inputs (auto-fetch then reclaim)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~1730 (145+375+230+185+205+320+270, per design.md slicing) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1/7 (1a) → PR 2/7 (1b) → PR 3/7 (2) → PR 4/7 (3) → PR 5/7 (4a) → PR 6/7 (4b) → PR 7/7 (4c) |
| Delivery strategy | ask-on-risk |
| Chain strategy | stacked-to-main (already resolved by the orchestrator; PRs target `dev`, each slice branch `feat/vps-nas-<slice>` cut from current `dev` tip after the previous slice merges) |

Decision needed before apply: No (chain strategy pre-resolved: stacked-to-main)
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1a | Fetch-outcome truth + `verify_*` returncode hardening | PR 1/7 `feat/vps-nas-1a` | `uv run pytest tests/congress_videos/test_nas_archive.py tests/congress_videos/test_nas_fetch.py tests/congress_videos/test_nas_fetch_dag.py` | N/A — no live NAS; unit tests inject the runner | Revert commits; `nas_fetch_dag.py` returns to always-success summary |
| 1b | `fetch_lock` + `ensure_local_video` shared helper, D6a mapping | PR 2/7 `feat/vps-nas-1b` | `uv run pytest tests/congress_videos/test_nas_fetch.py tests/congress_videos/test_nas_fetch_dag.py` | N/A — lock/rsync mocked via injected runner + `tmp_path` | Revert commits; `fetch_one_video` reverts to pre-delegation body |
| 2 | Wire 3 NAS-aware consumers to inline fetch | PR 3/7 `feat/vps-nas-2` | `uv run pytest tests/congress_videos/test_speaker_turns_dag.py tests/congress_videos/test_trim_proposals_dag.py tests/congress_videos/test_speaker_turn_videos_dag.py` | N/A — DAG callables tested via `importlib` + `monkeypatch` | Revert commits; consumers return to `skipped_archived` |
| 3 | Wire 2 unaware consumers to inline fetch | PR 4/7 `feat/vps-nas-3` | `uv run pytest tests/congress_videos/test_reap_clip_preparer_dag.py tests/congress_videos/test_speaker_turn_prepare_dag.py` | N/A — same DAG-callable harness | Revert commits; consumers return to today's missing-source behavior |
| 4a | `reclaim_grace_hours` setting + `nas_completeness` extraction + env contract | PR 5/7 `feat/vps-nas-4a` | `uv run pytest tests/congress_videos/test_nas_archive.py tests/congress_videos/test_nas_archive_dag.py deploy/vps-dev/test_contract.py` | N/A — settings/env parsing only | Revert commits; drop env var, restore inline `complete_video_ids` in `nas_archive_dag.py` |
| 4b | `nas_reclaim` module: gates + `reclaim_one_video` | PR 6/7 `feat/vps-nas-4b` | `uv run pytest tests/congress_videos/test_nas_reclaim.py` | N/A — deterministic single-process, injected `now`/runner | Delete `modules/nas_reclaim.py`; no DAG references it yet |
| 4c | `nas_reclaim` DAG, paused on creation | PR 7/7 `feat/vps-nas-4c` | `uv run pytest tests/congress_videos/test_nas_reclaim_dag.py` | `bash scripts/test-airflow-e2e.sh` (DAG import check) | Pause/delete `nas_reclaim_dag.py`; zero data loss, NAS stays authoritative |

## Phase 1: Slice 1a — Fetch outcome truth (PR 1/7, `feat/vps-nas-1a`)

- [x] 1.1 RED: `tests/congress_videos/test_nas_archive.py` — assert `verify_synced` returns `False` when the dry-run rsync `returncode != 0`.
- [x] 1.2 RED: `tests/congress_videos/test_nas_fetch.py` — assert `verify_fetched` returns `False` when `returncode != 0`.
- [x] 1.3 GREEN: `congress_videos/modules/nas_archive.py` — add the returncode guard to `verify_synced` (D4).
- [x] 1.4 GREEN: `congress_videos/modules/nas_fetch.py` — add the returncode guard to `verify_fetched`; hoist `RSYNC_TIMEOUT_SECS` as the module's single source of truth (D7).
- [x] 1.5 RED: `tests/congress_videos/test_nas_fetch_dag.py` — single all-failed `video_id` raises; mixed batch surfaces the one failure without discarding the success; empty conf stays its own distinct failure (spec scenarios).
- [x] 1.6 GREEN: `congress_videos/nas_fetch_dag.py` — `_run_fetch_videos` raises when every requested video fails (D6); import `RSYNC_TIMEOUT_SECS` from `modules/nas_fetch.py`.
- [x] 1.7 REFACTOR: remove any duplicated returncode-check logic between `nas_archive.py`/`nas_fetch.py`.
- [x] 1.8 Run `uv run pytest tests/congress_videos/test_nas_archive.py tests/congress_videos/test_nas_fetch.py tests/congress_videos/test_nas_fetch_dag.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 1.9 Check `git diff --stat` stays under 400 lines (~145 target).

## Phase 2: Slice 1b — Shared fetch helper + lock (PR 2/7, `feat/vps-nas-1b`)

- [x] 2.1 RED: `tests/congress_videos/test_nas_fetch.py` — `fetch_lock` atomicity (`O_CREAT|O_EXCL`), non-blocking contention returns busy, stale lock (`> RSYNC_TIMEOUT_SECS + 600`) is broken and reacquired, release is a no-op on foreign token (D2).
- [x] 2.2 GREEN: `congress_videos/modules/nas_fetch.py` — add `NasFetchError`, `FetchLockBusy`, `fetch_lock` contextmanager at `{channel_slug}/{video_id}/.nas_fetch.lock` with a `{token, pid, acquired_at}` JSON payload.
- [x] 2.3 RED: `tests/congress_videos/test_nas_fetch.py` — `ensure_local_video` branches: `fetched` (marker/discovery → rsync → `verify_fetched` → `refresh_retention` → unlock), `in_progress` (lock busy), `unavailable/not_on_nas`, `unavailable/disabled`.
- [x] 2.4 GREEN: `congress_videos/modules/nas_fetch.py` — implement `ensure_local_video(project_dir, channel_slug, video_id, settings, *, runner=None, now=None) -> dict` per the Interfaces contract in design.md.
- [x] 2.5 RED (D6a): `tests/congress_videos/test_nas_fetch_dag.py` — single absent `video_id` fails the run; absent + restorable id succeeds with the absent id only in `failed`, never in `restored`; `in_progress`/`disabled` statuses raise `NasFetchError` out of `fetch_one_video`.
- [x] 2.6 GREEN: `congress_videos/nas_fetch_dag.py` — `fetch_one_video` delegates to `ensure_local_video` and converts every non-`fetched` status per the D6a table; extend `_run_fetch_videos`' per-video catch tuple to include `NasFetchError`.
- [x] 2.7 REFACTOR: delete any pre-delegation marker/discovery code left duplicated in `nas_fetch_dag.py`.
- [x] 2.8 Run `uv run pytest tests/congress_videos/test_nas_fetch.py tests/congress_videos/test_nas_fetch_dag.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 2.9 Check `git diff --stat` stays under 400 lines (~375 target). **RESOLVED**: initial pass was 864 changed lines (over budget); per coordinator direction, split along the existing two-commit boundary and trimmed docstrings/merged parametrized tests (no spec scenario dropped, no behaviour/API change). Final: `git diff --stat origin/dev` → 5 files changed, 641 insertions(+), 104 deletions(-) = 745 total across 2 commits; commit 1 (`1f9eb30`, module+tests) = 390 changed lines (≤390); commit 2 (`15ffefb`, DAG+tests) = 355 changed lines (≤400). Both commits individually within budget.

## Phase 3: Slice 2 — Wire 3 NAS-aware consumers (PR 3/7, `feat/vps-nas-2`)

- [x] 3.1 RED: `tests/congress_videos/test_speaker_turns_dag.py` — `fetched` proceeds, `in_progress` defers (not a failure), `unavailable` degrades to today's `skipped_no_video` (design D6a table; `skipped_archived` is retired, see Deviations), `fetch_failed` is subject to the D6 all-failed rule.
- [x] 3.2 RED: `tests/congress_videos/test_trim_proposals_dag.py` — same four branch scenarios.
- [x] 3.3 RED: `tests/congress_videos/test_speaker_turn_videos_dag.py` — same four branch scenarios; summary shape and counters preserved.
- [x] 3.4 GREEN: `congress_videos/speaker_turns_dag.py` — replace the `skipped_archived` branch with an `ensure_local_video` call; add `execution_timeout=6h` (D7).
- [x] 3.5 GREEN: `congress_videos/trim_proposals_dag.py` — same hook; `execution_timeout=6h`.
- [x] 3.6 GREEN: `congress_videos/speaker_turn_videos_dag.py` — same hook; `execution_timeout=6h`; keep the summary's existing shape plus the new counters.
- [x] 3.7 REFACTOR: not duplicated verbatim across the three DAGs (different loop variables, different summary shapes) — no shared helper extracted, per the task's own condition. `speaker_turn_videos_dag.py` did need a local `_resolve_missing_source` extraction to satisfy ruff's C901 complexity gate on `_materialize_task`; that extraction is internal to that one file only.
- [x] 3.8 Run `uv run pytest tests/congress_videos/test_speaker_turns_dag.py tests/congress_videos/test_trim_proposals_dag.py tests/congress_videos/test_speaker_turn_videos_dag.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 3.9 Check `git diff --stat` stays under 400 lines (~230 target). **NOT MET**: `git diff --stat origin/dev` = 354 insertions(+), 99 deletions(-) = 453 changed lines, exceeding the ≤390 hard cap given for this apply batch. See apply-progress for the trimming pass and `size:exception` recommendation.

## Phase 4: Slice 3 — Wire 2 unaware consumers (PR 4/7, `feat/vps-nas-3`)

- [x] 4.1 RED: `tests/congress_videos/test_reap_clip_preparer_dag.py` — missing `output_path` triggers `ensure_local_video` then re-probes; NAS-missing preserves the current skip/fail contract unchanged.
- [x] 4.2 RED: `tests/congress_videos/test_speaker_turn_prepare_dag.py` — same hook placed before the decode check; NAS-missing unchanged.
- [x] 4.3 GREEN: `congress_videos/reap_clip_preparer_dag.py` — call `ensure_local_video` on missing `output_path`, re-probe, `execution_timeout=2h` (D7).
- [x] 4.4 GREEN: `congress_videos/speaker_turn_prepare_dag.py` — same hook before the decode check; `execution_timeout=2h`.
- [x] 4.5 REFACTOR: confirm `congress_videos/modules/vad_helpers.py` needs no change (locators stay pure per design) — no edit expected.
- [x] 4.6 Run `uv run pytest tests/congress_videos/test_reap_clip_preparer_dag.py tests/congress_videos/test_speaker_turn_prepare_dag.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 4.7 Check `git diff --stat` stays under 400 lines (~185 target).

## Phase 5: Slice 4a — Reclaim settings + completeness extraction + env contract (PR 5/7, `feat/vps-nas-4a`)

- [x] 5.1 RED: `tests/congress_videos/test_nas_archive.py` — `ArchiveSettings.reclaim_grace_hours` parses `NAS_RECLAIM_GRACE_HOURS`, defaults to 12, validates `>= 0`.
- [x] 5.2 GREEN: `congress_videos/modules/nas_archive.py` — add `reclaim_grace_hours: int = 12` to `ArchiveSettings`.
- [x] 5.3 RED: `tests/congress_videos/test_nas_archive_dag.py` — pin today's `complete_video_ids()` behavior as the baseline before the move.
- [x] 5.4 GREEN: create `congress_videos/modules/nas_completeness.py` — move `complete_video_ids()` verbatim out of `nas_archive_dag.py`.
- [x] 5.5 GREEN: `congress_videos/nas_archive_dag.py` — import `complete_video_ids` from `modules/nas_completeness.py`; remove the inline definition.
- [x] 5.6 GREEN: `deploy/vps-dev/compose.yml` — add `NAS_RECLAIM_GRACE_HOURS: ${NAS_RECLAIM_GRACE_HOURS:-12}`.
- [x] 5.7 RED: `deploy/vps-dev/test_contract.py` — extend `test_nas_archive_mount_...` with one `assertEqual` for the new env var and its default.
- [x] 5.8 GREEN: confirm `deploy/vps-dev/test_contract.py` passes against the 5.6 compose change.
- [x] 5.9 Docs: `deploy/vps-dev/README.md` — document `NAS_RECLAIM_GRACE_HOURS` (no RED needed, doc-only).
- [x] 5.10 Run `uv run pytest tests/congress_videos/test_nas_archive.py tests/congress_videos/test_nas_archive_dag.py deploy/vps-dev/test_contract.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 5.11 Check `git diff --stat` stays under 400 lines (~205 target).
- [x] 5.12 (Added by verifier of slice 1a, Engram obs #2936) RED→GREEN: `congress_videos/nas_archive_dag.py::_run_archive_videos` gains per-candidate isolation — one candidate's `AirflowException` (from `archive_one_video`) is caught, logged, and recorded in `summary["failed"]`; the batch continues; `AirflowException` is raised only when every requested candidate failed. Mirrors `nas_fetch_dag._run_fetch_videos`. Tests in `tests/congress_videos/test_nas_archive_dag.py`.

## Phase 6: Slice 4b — `nas_reclaim` module: gates + reclaim (PR 6/7, `feat/vps-nas-4b`)

- [x] 6.1 RED: create `tests/congress_videos/test_nas_reclaim.py` — three-gate happy path (NAS verified + DB complete + grace elapsed → deletes, marker kept). **Deviation**: "NAS verified" at selection time is NOT an archive-marker check (see 6.9 deviation) — the happy-path test covers `reclaim_one_video`'s real `verify_synced` gate directly.
- [x] 6.2 RED: same file — blocked by pending DB-completeness work (`test_excluded_when_video_id_not_in_complete_video_ids`).
- [x] 6.3 RED: same file — blocked by the grace window (mtime age `< NAS_RECLAIM_GRACE_HOURS`) — both at selection and re-checked inside `reclaim_one_video`.
- [x] 6.4 RED: same file — blocked by an unverified/errored NAS copy (D4 hardening reused here) — parametrized `test_blocked_when_nas_verification_fails` (dirty itemized-changes + non-zero returncode).
- [x] 6.5 RED: same file — `MIRROR_ONLY_DIRS` (e.g. `thumbnails/`) and non-media sidecar files are never touched.
- [x] 6.6 RED: same file — in-flight fetch lock held → video skipped even if the other gates would pass (selection excludes it; `reclaim_one_video` returns `skipped`/`locked`).
- [x] 6.7 RED: same file — `batch` caps candidates per run; the remainder stays a candidate for the next run (`NAS_RECLAIM_BATCH` env knob itself is DAG-level, slice 4c).
- [x] 6.8 GREEN: create `congress_videos/modules/nas_reclaim.py` — `select_reclaim_candidates(settings, project_dir, channel_slug, complete_video_ids, *, now, batch) -> list[dict]` per the Interfaces contract.
- [x] 6.9 GREEN: same file — `reclaim_one_video(settings, project_dir, channel_slug, video_id, *, runner, now) -> dict` re-evaluates lock/grace/NAS-verify (D3) inside the lock immediately before `prune_local`, then writes/keeps `.nas_archived.json`. **Deviation**: the archive marker is NOT a selection-time gate and DB completeness (gate 3) is NOT re-queried inside the lock — see apply-progress Deviations for the full rationale (D4's own "recoverable, self-healing" reasoning; the fetched-back target scenario has its marker removed for the lease duration, so requiring it would exclude the primary case this module exists for).
- [x] 6.10 REFACTOR: the in-lock re-check reuses `_local_paths_or_none`/`_grace_elapsed` from the selection pass (no duplicated gate logic); `_newest_mtime` is media-suffix-only (not "every file") because `fetch_lock` writes a fresh-mtime lock file in the same directory before the grace check runs.
- [x] 6.11 Run `uv run pytest tests/congress_videos/test_nas_reclaim.py` and the full `tests/congress_videos/test_nas_*` set; confirm green. 13 passed / 234 passed (nas_* full set).
- [x] 6.12 Check `git diff --stat` stays under 400 lines (~320 target). `git diff --stat 16a5b5f` → 2 files changed, 388 insertions(+) (≤390 cap).

## Phase 7: Slice 4c — `nas_reclaim` DAG (PR 7/7, `feat/vps-nas-4c`)

- [x] 7.1 RED: create `tests/congress_videos/test_nas_reclaim_dag.py` — DAG callables wrap `select_reclaim_candidates`/`reclaim_one_video` via `importlib` DAG-module import + `monkeypatch.setattr` on `_subprocess_runner`, matching `test_nas_fetch_dag.py`'s existing pattern.
- [x] 7.2 RED: same file — schedule `0 */4 * * *`, `max_active_runs=1`, `is_paused_upon_creation=True`.
- [x] 7.3 RED: same file — batch cap enforced at the task level; an all-candidates-failed run is visibly non-silent (mirrors D6 outcome semantics).
- [x] 7.4 GREEN: create `congress_videos/nas_reclaim_dag.py` — schedule/`max_active_runs`/pause flags above, task callables wired to `modules/nas_reclaim.py`.
- [x] 7.5 REFACTOR: confirm the DAG module is thin wiring only, no gate logic duplicated from `modules/nas_reclaim.py`.
- [x] 7.6 Run `uv run pytest tests/congress_videos/test_nas_reclaim_dag.py` and the full `tests/congress_videos/test_nas_*` set; confirm green.
- [x] 7.7 Check `git diff --stat` stays under 400 lines (~270 target) — **MET per commit** after coordinator-directed split (no `size:exception` available): no behaviour change, `git reset --soft origin/dev` then re-committed along the task boundary into two stacked commits on `feat/vps-nas-4c`. Commit A `acd54c1` (`check_enabled` + `select_candidates` only, no `reclaim_videos` task): `2 files changed, 268 insertions(+)` ≤ 300 target. Commit B `6a8847a` (adds `reclaim_videos` + its tests): own diff vs commit A `2 files changed, 179 insertions(+), 9 deletions(-)` = 188 changed lines ≤ 390. Combined `git diff --stat origin/dev` is still `2 files changed, 438 insertions(+)` (both files are brand-new so the combined total is unchanged by the split), but each individual commit now clears its own budget. Confirmed byte-identical final tree vs the pre-split single commit (`git diff <pre-split-sha> HEAD` empty for both files) — zero behaviour change. See apply-progress.md for the full breakdown.

## Phase 8: Slice-independent verification (after PR 7/7 merges to `dev`)

- [x] 8.1 Run `deploy/vps-dev/test_contract.py` in full (not just the 5.7 assertion) and confirm it passes.
- [x] 8.2 DAG import check: `uv run python -c "import congress_videos.nas_reclaim_dag"`; no dedicated DAG-import test file exists under `tests/` — `bash scripts/test-airflow-e2e.sh` (asserts `airflow dags list-import-errors` is empty) auto-runs during `sdd-verify` because this change touches `congress_videos/**`.
- [ ] 8.3 Manual production check 1 (VPS): trigger `nas_fetch` for one known NAS-only `video_id`; confirm the run succeeds and that id appears under `restored`.
- [ ] 8.4 Manual production check 2 (VPS): the design defines no `dag_run.conf` override for the grace window — `ArchiveSettings.reclaim_grace_hours` is env-only (`NAS_RECLAIM_GRACE_HOURS`), and the Interfaces contract has no conf plumbing for `nas_reclaim`. To validate reclaim without waiting the full window, temporarily lower `NAS_RECLAIM_GRACE_HOURS` in `deploy/vps-dev/compose.yml` and redeploy, then trigger `nas_reclaim` after the shortened window elapses; confirm the 8.3 video's local material is pruned and `.nas_archived.json` is kept.
- [ ] 8.5 Revert the temporary `NAS_RECLAIM_GRACE_HOURS` override and redeploy.
