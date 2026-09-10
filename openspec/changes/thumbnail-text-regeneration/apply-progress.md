# Apply Progress: thumbnail-text-regeneration (issue #545) — PR 1 + PR 2

**Mode**: Strict TDD
**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-545`
**Branch (PR 2 batch)**: `feat/545-slice2-regen-helper` (base: `feat/545-slice1-migration`, which is
already in this branch's history at `793c771`)
**Batch**: Second batch. Merged with PR 1's progress below — no completed task from PR 1 was lost.

## Scope Delivered

- **PR 1 (Phase 1)**: migration 052 + two `database.py` accessors. Completed in the prior batch.
- **PR 2 (Phase 2, this batch)**: the bounded, deliberately UNWIRED `_regenerate_flagged_thumbnail`
  trigger/poll helper + its two poll constants in `congress_videos/youtube_upload_dag.py`. No caller
  exists yet — wiring `t6b` is Phase 3 / PR 3, explicitly NOT touched.

## Completed Tasks

### Phase 1 (PR 1)

- [x] 1.1 Migration 052: 7 additive columns on `speaker_turn_videos` only (D1)
- [x] 1.2 DOWN block commented out (verified against 050/051 convention)
- [x] 1.3 RED: `test_charges_before_second_call`
- [x] 1.4 GREEN: `claim_thumbnail_text_regeneration`
- [x] 1.5 RED: `test_refuses_at_ceiling`
- [x] 1.6 GREEN: confirmed by 1.4's WHERE clause (no separate code path)
- [x] 1.7 RED: `test_idempotent_on_rerun` (mutation-sensitive WHERE-guard pin)
- [x] 1.8 RED: `test_unknown_output_path_returns_none`
- [x] 1.9 RED: `test_prior_brief_write_once`
- [x] 1.10 GREEN: `record_thumbnail_text_regeneration_outcome`
- [x] 1.11 RED: `test_persists_regenerated_brief`
- [x] 1.12 GREEN: implemented by 1.10's single UPDATE (no separate code path)

### Phase 2 (PR 2, this batch)

- [x] 2.1 Constants added to `youtube_upload_dag.py`: `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10`,
      `_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv("UPLOAD_THUMBNAIL_REGEN_MAX_POLLS", "100"))`.
      `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS` was deliberately NOT duplicated here (see Deviations).
- [x] 2.2 RED: `TestRegenerateFlaggedThumbnail::test_completes_within_bound_returns_regenerated_result`
- [x] 2.3 GREEN: `_regenerate_flagged_thumbnail(output_path, prior_brief, run_id, db=None) -> dict | None`
- [x] 2.4 RED: `test_times_out_after_exactly_max_polls` (mutation check executed live — see below)
- [x] 2.5 RED: `test_trigger_exception_returns_trigger_failed_never_raises`
- [x] 2.6 RED: `test_child_dag_failed_state_returns_child_failed`
- [x] 2.7 RED: `test_malformed_xcom_returns_invalid_result` (parametrized ×4) +
      `test_valid_success_shape_but_nonexistent_path_is_invalid_result`
- [x] 2.8 GREEN: single `try/except Exception`, no bare `raise` anywhere in the body — AND enforced by a
      new test in this same PR (`test_no_path_ever_raises`, parametrized ×5), not deferred to 3.9, per the
      launch prompt's non-negotiable #1.

## Files Changed

### PR 1

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql` | Created | 7 additive/nullable columns on `speaker_turn_videos`; DOWN block commented out, matching 050/051 |
| `congress_videos/modules/database.py` | Modified | Added `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS = 2` constant; `claim_thumbnail_text_regeneration()`; `record_thumbnail_text_regeneration_outcome()` |
| `tests/congress_videos/modules/test_database.py` | Modified | Added `TestClaimThumbnailTextRegeneration` (8 tests) and `TestRecordThumbnailTextRegenerationOutcome` (5 tests) |

### PR 2 (this batch)

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/youtube_upload_dag.py` | Modified | Added `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS`/`_THUMBNAIL_REGEN_MAX_POLLS` constants and the standalone `_regenerate_flagged_thumbnail` helper (trigger + bounded poll + outcome recording), placed after `trigger_thumbnail_generation`. +148 lines, 0 deletions. |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Added `TestRegenerateFlaggedThumbnail` (17 tests): happy path, child-conf shape (×2), exact-100-poll timeout with a live mutation check, trigger exception, child `failed` state, malformed/missing/nonexistent-path XCom (×5 parametrized cases), outcome-recording-failure isolation, and a 5-way parametrized "never raises" test. +286 lines, 0 deletions. |
| `openspec/changes/thumbnail-text-regeneration/tasks.md` | Modified | Ticked Phase 2 tasks `[x]`; corrected the Suggested Work Units table's stale `-k regenerate_flagged_thumbnail` filter (matched 0 tests) to `-k TestRegenerateFlaggedThumbnail`. |

## TDD Cycle Evidence (PR 2)

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 2.2/2.3 | `tests/congress_videos/test_youtube_upload_dag.py` | Unit | 215 pre-existing tests in file passing before edit | ✅ `ImportError` confirmed for all 17 new tests before implementation | ✅ All 17 passed after implementation | ✅ happy path, timeout, trigger-exception, child-failed, invalid-result (×5 shapes), outcome-write-failure, no-raise (×5) | ➖ None needed — matches `_poll_thumbnail_dag_run`'s established bounded shape |
| 2.4 | same | Unit | (same file, same batch) | ✅ Written; loop-count assertion pinned to exactly 100 | ✅ Passed | ✅ **Live mutation check executed**: temporarily changed `range(_THUMBNAIL_REGEN_MAX_POLLS)` → `range(_THUMBNAIL_REGEN_MAX_POLLS + 1)`, confirmed `test_times_out_after_exactly_max_polls` fails (`101 == 100`), then reverted and re-confirmed green | ➖ None needed |
| 2.5 | same | Unit | (same file, same batch) | ✅ Written (`trigger_dag_api` `side_effect=RuntimeError`) | ✅ Passed | ✅ covered jointly with 2.8's no-raise parametrization | ➖ None needed |
| 2.6 | same | Unit | (same file, same batch) | ✅ Written (`dag_run.state == "failed"`) | ✅ Passed | ➖ single case | ➖ None needed |
| 2.7 | same | Unit | (same file, same batch) | ✅ Written (5 parametrized malformed/nonexistent-path cases) | ✅ Passed | ✅ 5 cases: `None`, empty `output_path`, missing `output_path`, `success: False`, well-formed-but-nonexistent-path | ➖ None needed |
| 2.8 | same | Unit | (same file, same batch) | ✅ `test_no_path_ever_raises` written FIRST as an `ImportError` failure alongside the rest | ✅ Passed (5/5 parametrized cases, including an unanticipated mid-poll `refresh_from_db()` exception) | ✅ 5 cases: trigger-raises, child-failed, invalid-result, timeout, unexpected-poll-exception | ➖ None needed |

### Test Summary (PR 2)

- **Total tests written**: 17 (5 base RED items from tasks.md 2.2/2.4–2.7, expanded to 10 test functions/parametrized cases for full coverage of every stated GIVEN, plus 7 additional tests demanded by the launch prompt's non-negotiables: 2 child-conf shape tests, 1 outcome-recording-failure-isolation test, and the 5-way `test_no_path_ever_raises` parametrization)
- **Total tests passing**: 17/17 (new), 232/232 (full `test_youtube_upload_dag.py` file), 5241/5241 (full repo suite, non-Postgres-live tests)
- **Layers used**: Unit (17)
- **Approval tests**: None — no refactoring of existing behavior, purely additive
- **Pure functions created**: 0 — the helper is I/O by necessity (triggers and polls a child DAG run, calls the DB accessor)

## Deviations from Design

1. **`THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS` was NOT added to `youtube_upload_dag.py`**, despite tasks.md 2.1
   listing it. PR 1 already defined this constant in `database.py` and explicitly flagged the drift risk
   for this batch to resolve by importing rather than duplicating. On inspection, nothing in
   `youtube_upload_dag.py` needs the ceiling value directly for Phase 2 — the DB claim
   (`claim_thumbnail_text_regeneration`, PR 1) already enforces it atomically. Importing an unused symbol
   would fail `ruff check` (F401), so it was left undefined here; PR 3's author should import it from
   `congress_videos.modules.database` only if/when `_verify_final_copy`'s wiring actually references it
   (e.g., for a log line), rather than re-declaring a second copy.
2. **`_regenerate_flagged_thumbnail`'s child `conf` is minimal** (`{"output_path": ..., "previous_brief":
   ...}` when a prior brief exists) — exactly the fields discussed in design.md's Data Flow section and
   tasks.md's fixed function signature (`output_path`, `prior_brief`, `run_id`, `db` — no chapter/session/
   domain context parameters). **Flagging for PR 3's author**: `generic_thumbnail_generator`'s own
   `validate_input()` requires `chapter_id`, `debate_summary`, `session`, and `domain` in the conf
   (`_REQUIRED_CONF_KEYS`, confirmed by reading `generic_thumbnail_generator_dag.py`) — a conf with only
   `output_path` and `previous_brief` will fail `validate_input` and the child run will land in a `failed`
   state (which the helper correctly maps to `outcome="child_failed"`, so it is NOT a blocking bug for THIS
   slice — the helper's own non-blocking contract still holds). PR 3's wiring at `_verify_final_copy` has
   access to `video`/`chapter_id`/context that this helper's fixed 4-argument signature does not; either
   the signature needs a `child_conf` extension parameter, or PR 3 needs to enrich `prior_brief`/pass
   additional context another way. This was NOT solved in this PR to avoid scope creep into PR 3's wiring
   task, matching the instruction to build only the standalone, unwired helper.
3. **Return shape is asymmetric by design, matching tasks.md literally, not a uniform `None`-on-failure
   contract.** On success the helper returns the raw child DAG result dict (`{"success": True,
   "output_path": ..., "title": ..., "title_generation_input": ...}`); on any of the four failure modes it
   returns `{"outcome": ..., "error": ...}`. Both are truthy dicts — never `None` and never a raised
   exception. This matches tasks.md 2.2/2.4–2.7's literal wording ("the helper returns the regenerated
   brief dict" / "the helper returns `{"outcome": "timeout", ...}`") rather than a hypothetical uniform
   `None`-return contract; the `db.record_thumbnail_text_regeneration_outcome` call inside the helper is
   what performs the actual "record the outcome" side effect referenced by the launch prompt's
   non-negotiable #1.

## Issues Found

None beyond the two deviations noted above (both are documentation/handoff notes for PR 3, not bugs in
this PR's own scope).

## Remaining Tasks (NOT this batch's scope)

- [ ] Phase 3 (PR 3): `t6b` wiring, hoisted `xcom_push`, operator-signal line, docs — including resolving
      Deviation #2 above (the child conf's missing required keys) before wiring a real call site.

## Workload / PR Boundary

- Mode: chained PR slice (`auto-chain`, `feature-branch-chain`)
- Current work unit: Unit 2 — "Bounded trigger/poll helper `_regenerate_flagged_thumbnail` (unwired) +
  constants"
- Boundary: starts from PR 1's landed migration + DB accessors (no thumbnail-regeneration behavior yet);
  ends with a fully tested, unused (no caller) helper function. PR 2 targets PR 1's branch
  (`feat/545-slice1-migration`).
- Rollback boundary: delete `_regenerate_flagged_thumbnail` and its two constants from
  `youtube_upload_dag.py`; nothing in the codebase imports or calls either symbol yet.
- **Review budget note (overage, reported honestly per policy)**: code + tests = 148 + 286 = **434 changed
  lines** (all additions, 0 deletions), against the 400-line session budget — **34 lines over (~8.5%)**.
  This is the smallest cohesive PR 2 deliverable (one helper function is not further splittable across
  PRs in a way that keeps each slice independently reviewable), and the overage is directly attributable to
  the launch prompt's own non-negotiable #1 mandating a dedicated "never raises" test class beyond
  tasks.md's 5 base RED items — comments, docs, and tests were not trimmed to fit the budget, per the
  `chained-pr`/`work-unit-commits` "budget is not code-golf" rule. Recommending `size:exception` for this
  slice rather than further splitting or shrinking test coverage.

## Work Unit Evidence (PR 2)

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -k TestRegenerateFlaggedThumbnail -q --no-cov` → `17 passed`; full file: `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -q --no-cov` → `232 passed` |
| Runtime harness command/scenario and exact result | N/A — the helper is standalone/unwired with no DAG task-graph change yet; `bash scripts/test-airflow-e2e.sh` is deferred to Phase 3 per design.md's slicing table, since this PR does not touch the `t6b` operator or the task graph |
| Rollback boundary | Revert the `_regenerate_flagged_thumbnail` function and its two constants from `congress_videos/youtube_upload_dag.py`, and the `TestRegenerateFlaggedThumbnail` class from `tests/congress_videos/test_youtube_upload_dag.py`; nothing else in the codebase references either symbol |

## Full-Suite Verification (after PR 2)

- `uv run pytest -q` (full repo): `5241 passed, 34 skipped in 105.95s`, exit code 0. (5224 baseline after
  PR 1 + 17 new tests in this batch = 5241; zero failures, zero new skips.)
- `uv run ruff check .` → `All checks passed!`
- `uv run ruff format --check .` → initially flagged 1 file (a single long call-site line in the new test
  class exceeded the line-length wrap point); ran `uv run ruff format` to auto-wrap it, then re-verified
  `320 files already formatted` clean. No logic changed by the reformat — confirmed by re-running the
  targeted test class (17 passed) and the full file (232 passed) after formatting.

## Status

12/12 Phase 1 tasks + 8/8 Phase 2 tasks complete (20/34 total tasks across all three phases). Ready for
verify of PR 1 + PR 2 scope; Phase 3 remains for a subsequent `sdd-apply` batch, including resolving the
child-conf context gap flagged in Deviation #2 before wiring `t6b`.
