# Apply Progress: video_analytics_actions candidates XCom TZ Normalization (issue #605)

**Change**: analytics-candidates-xcom-tz
**Mode**: Strict TDD
**Status**: 15/15 original tasks + remediation batch complete — Ready for re-verify

## Remediation Batch (post sdd-verify FAIL)

`sdd-verify` (evidence_revision `sha256:7ba9ef6868...`) returned **FAIL**: 2 CRITICAL findings
— spec scenarios "Empty candidate list normalizes without raising" and "Snapshot age in days is
unchanged by normalization" had no covering test. Also flagged one non-blocking WARNING: this
file's "55/55 pre-existing tests passed before edit" claim was off by one (actual pre-existing
count is 54; 57 total collected at HEAD minus the 3 new tests). Both are fixed in this batch.

- [x] R.1 Added `test_empty_candidate_list_normalizes_without_raising` to
  `TestCandidatesXComNormalization` — patches `get_unactioned_snapshots` to return `[]`, asserts
  `_run_select_candidates` returns/pushes `[]`, and that `[]` survives the real
  `_xcom_round_trip`.
- [x] R.2 Added `test_snapshot_age_days_unchanged_by_normalization` to the same class — computes
  `_snapshot_age_days` on the raw fixed `+02:00` `_PSYCOPG2_COLLECTED_AT` versus its
  `utc_normalize_row`-normalized (UTC) form, under `freeze_time("2026-09-10 12:00:00+00:00")` to
  make the comparison deterministic; asserts both calls return the same `int` (`21`).
- [x] R.3 Corrected the pre-existing-count claim below: **54**, not 55.

**Strict TDD note — RED not applicable, by design.** These are *characterization* tests: the
production code (`_run_select_candidates` push-site `utc_normalize_rows`, and
`_snapshot_age_days`'s `.astimezone(UTC)`-invariant instant arithmetic) was already correct going
into this batch — the gap verify found was in *test coverage*, not in behavior. Both new tests
pass on first run against unmodified HEAD (`6481158`); there is no code change in this batch, so a
literal "write failing test, then fix code" RED→GREEN cycle does not exist here. Per the
orchestrator's explicit instruction, meaningfulness was instead proven by breaking each target
function locally (uncommitted), confirming the corresponding new test failed, then reverting via
`git checkout --`:

| Test | Throwaway break (uncommitted) | Observed failure | Reverted |
|---|---|---|---|
| `test_empty_candidate_list_normalizes_without_raising` | `utils/airflow_helpers.py::utc_normalize_rows` — inserted `first = rows[0]` before the comprehension | `IndexError: list index out of range` at the `_run_select_candidates` call site | ✅ `git checkout -- utils/airflow_helpers.py` |
| `test_snapshot_age_days_unchanged_by_normalization` | `utils/airflow_helpers.py::_to_utc` — added `+ timedelta(days=1)` to the tz-aware `.astimezone(UTC)` branch | `assert 21 == 20` | ✅ `git checkout -- utils/airflow_helpers.py` |

`git status`/`git diff` confirmed `utils/airflow_helpers.py` was byte-identical to HEAD after each
revert; only the test file carries a diff in this batch. No production code was changed.

### Remediation Verification

| Command | Observed result |
|---|---|
| `uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py --no-cov -q` | 59 passed (57 pre-existing + 2 new) |
| `uv run pytest -n auto` | 5553 passed, 34 skipped (pre-existing Postgres-live/env skips, unrelated) |
| `uv run ruff check .` | All checks passed! |
| `uv run ruff format --check .` | 341 files already formatted |

Commit: `6481158` — `test(analytics-actions): cover empty candidates and snapshot age under
normalization` on `fix/605-analytics-candidates-xcom-tz` (parent `52de4b6`, base `8fddf0e`).
`git diff --stat 8fddf0e..HEAD`: 2 files changed, 133 insertions(+), 3 deletions(-)
(`congress_videos/video_analytics_actions_dag.py` +10/-3 from the original fix,
`tests/congress_videos/test_video_analytics_actions_dag.py` +126 cumulative). `openspec/` change
artifacts remain untracked per commit policy.

## Original Apply Batch (superseded counts corrected above)

## Completed Tasks

### Phase 1: RED — Failing Tests First (TDD)
- [x] 1.1 Added `import json`, `timezone` to the datetime import, and `from airflow.utils.json import XComDecoder, XComEncoder`.
- [x] 1.2 Added module-level constant `_PSYCOPG2_COLLECTED_AT`.
- [x] 1.3 Added module-level `_xcom_round_trip(value)` helper (byte-identical to `tests/utils/test_airflow_helpers.py:21-23`, per design D3).
- [x] 1.4 Added `_raw_candidate_row()` builder.
- [x] 1.5 Inserted `class TestCandidatesXComNormalization` after `TestRecordNoOps`, before the `apply_actions (8.2 - 8.5)` section comment.
- [x] 1.6 Added bug-pin test `test_raw_candidate_row_breaks_real_xcom_round_trip`.
- [x] 1.7 Added test `test_select_candidates_payload_survives_real_xcom_round_trip`.
- [x] 1.8 Added test `test_evaluate_candidates_decisions_survive_real_xcom_round_trip`.
- [x] 1.9 Ran `-k XComNormalization`: RED confirmed — 1 passed (bug-pin), 2 failed (T2, T3), exactly as designed.

### Phase 2: GREEN — Production Fix
- [x] 2.1 Added `from utils.airflow_helpers import utc_normalize_rows` in `congress_videos/video_analytics_actions_dag.py`.
- [x] 2.2 `_run_select_candidates` now does `result = utc_normalize_rows(db.get_unactioned_snapshots())`.
- [x] 2.3 Updated `_run_select_candidates` docstring citing issue #605 and the normalization boundary.
- [x] 2.4 Re-ran `-k XComNormalization`: GREEN confirmed — all 3 tests pass.

### Phase 3: Verification
- [x] 3.1 Full suite `uv run pytest -n auto`: 5551 passed, 34 skipped, coverage 92.35% (>= 80% gate).
- [x] 3.2 `uv run ruff check .`: All checks passed.
- [x] 3.3 `uv run ruff format --check .`: 341 files already formatted.
- [x] 3.4 `bash scripts/test-airflow-e2e.sh`: exit 4 — Docker daemon unreachable, reported `unavailable` per contract (not a failure).

### Phase 4: Documentation
- [x] 4.1 Confirmed no other doc updates required — transport-only fix, no migration/CONTEXT.md/ADR change needed.

## TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 1.6-1.8 / 2.1-2.2 | `tests/congress_videos/test_video_analytics_actions_dag.py` | Unit | ✅ 54/54 pre-existing tests in file passed before edit (corrected from an off-by-one "55/55" originally reported here; see Remediation Batch above) | ✅ Written (3 tests) | ✅ Passed (all 3, after DAG fix) | ✅ 2 scenarios (candidates push + derived decisions), matching the 2 spec requirement scenarios beyond the bug-pin | ➖ None needed — code already minimal per design D1/D2 |

### Test Summary
- **Total tests written**: 3 (`test_raw_candidate_row_breaks_real_xcom_round_trip`, `test_select_candidates_payload_survives_real_xcom_round_trip`, `test_evaluate_candidates_decisions_survive_real_xcom_round_trip`)
- **Total tests passing**: 3/3 (post-fix); RED baseline was 1/3 (bug-pin only)
- **Layers used**: Unit (3)
- **Approval tests** (refactoring): None — no refactoring tasks
- **Pure functions created**: 0 (reused existing `utc_normalize_rows`)

## RED Evidence (before DAG edit)

```
$ uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization --no-cov -v
...
FAILED ...::TestCandidatesXComNormalization::test_select_candidates_payload_survives_real_xcom_round_trip
FAILED ...::TestCandidatesXComNormalization::test_evaluate_candidates_decisions_survive_real_xcom_round_trip
================== 2 failed, 1 passed, 54 deselected in 2.08s ==================
```
Failure cause (both): `ValueError: ZoneInfo keys must be normalized relative paths, got: ...` raised inside
Airflow's real `XComDecoder`/`pendulum.timezone` path when decoding the raw fixed-offset `collected_at`.

## GREEN Evidence (after DAG edit)

```
$ uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization --no-cov -v
...
PASSED ...::TestCandidatesXComNormalization::test_raw_candidate_row_breaks_real_xcom_round_trip
PASSED ...::TestCandidatesXComNormalization::test_select_candidates_payload_survives_real_xcom_round_trip
PASSED ...::TestCandidatesXComNormalization::test_evaluate_candidates_decisions_survive_real_xcom_round_trip
======================= 3 passed, 54 deselected in 2.14s =======================
```

## Full Verification

| Command | Observed result |
|---|---|
| `uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization --no-cov` | RED: 1 passed / 2 failed. GREEN: 3 passed. |
| `uv run pytest -n auto` | 5551 passed, 34 skipped, coverage 92.35% (gate 80%) |
| `uv run ruff check .` | All checks passed! |
| `uv run ruff format --check .` | 341 files already formatted |
| `bash scripts/test-airflow-e2e.sh` | exit 4 — `unavailable` (Docker daemon not reachable); run manually before merge |

## Files Changed

| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/video_analytics_actions_dag.py` | Modified (original batch, commit `52de4b6`) | +1 import (`utc_normalize_rows`), `_run_select_candidates` wraps `db.get_unactioned_snapshots()` with `utc_normalize_rows`, docstring updated citing #605 |
| `tests/congress_videos/test_video_analytics_actions_dag.py` | Modified (original batch `52de4b6` + remediation `6481158`) | New imports, `_PSYCOPG2_COLLECTED_AT`, `_xcom_round_trip`, `_raw_candidate_row`, `TestCandidatesXComNormalization` — now 5 tests total after remediation adds `test_empty_candidate_list_normalizes_without_raising` and `test_snapshot_age_days_unchanged_by_normalization` |

## Deviations from Design

None — implementation matches design exactly (D1 push-site normalization, D2 module-level import placement, D3 self-contained round-trip helper, D4 new raw-row builder).

## Issues Found

None.

## Workload / PR Boundary

- Mode: single PR (auto-chain resolved to a single work unit; forecast risk was Low)
- Current work unit: Unit 1 — "RED tests + `utc_normalize_rows` fix at the `candidates` push site"
- Boundary: starts from base `origin/dev` (8fddf0e), ends with commit `52de4b6` on `fix/605-analytics-candidates-xcom-tz`
- Estimated review budget impact: 97 changed lines (10 DAG, 90 test — actual `git diff --stat` — under the ~85 estimate and well under the 400 budget)
- Commit: `52de4b6487d4fe67d7985d01f3127873cf7eeb13` — "fix(analytics-actions): normalize candidates rows before the XCom push"
- `openspec/` change artifacts intentionally left untracked per commit policy

## Status

15/15 original tasks + 3/3 remediation items (R.1-R.3) complete. All 8 spec scenarios now have
covering tests (6 from the original batch + 2 added in remediation). Ready for re-verify.
