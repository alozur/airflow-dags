# Tasks: video_analytics_actions candidates XCom TZ Normalization (issue #605)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~85 (DAG +4, tests +80) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: stacked-to-main
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | RED tests + `utc_normalize_rows` fix at the `candidates` push site | `fix/605-analytics-candidates-xcom-tz` -> `dev` | `uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization` | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`); report `unavailable` if Docker missing | Revert the single commit; `utils/airflow_helpers.py` and the 8.2-8.5 apply tests are untouched |

## Phase 1: RED — Failing Tests First (TDD)

- [x] 1.1 In `tests/congress_videos/test_video_analytics_actions_dag.py`, add `import json` to the stdlib import group, add `timezone` to the existing `from datetime import UTC, datetime, timedelta` import (line 11), and add `from airflow.utils.json import XComDecoder, XComEncoder` after `import pytest` (line 14).
- [x] 1.2 Add module-level constant `_PSYCOPG2_COLLECTED_AT = datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))` near `_decision_row` (after line 112).
- [x] 1.3 Add module-level `_xcom_round_trip(value)` helper, byte-identical to `tests/utils/test_airflow_helpers.py:21-23` (per design D3 — do not import across test modules).
- [x] 1.4 Add `_raw_candidate_row()` builder: calls `_decision_row()`, pops `decision`, `views`, `median_views`, `sample_size`, then sets `collected_at=_PSYCOPG2_COLLECTED_AT` (models the raw `get_unactioned_snapshots()` row shape).
- [x] 1.5 Insert a new `class TestCandidatesXComNormalization` after `TestRecordNoOps` (after line 213), before the `# apply_actions (8.2 - 8.5)` section comment (line 216), with a section header comment.
- [x] 1.6 [Threat-matrix: raw payload must keep failing] Add test `test_raw_candidate_row_breaks_real_xcom_round_trip`: calls `_xcom_round_trip([_raw_candidate_row()])` and asserts it raises `ValueError` matching `"ZoneInfo keys must be normalized relative paths"` — this is the bug-pin from spec Requirement "Un-normalized candidates payload is pinned as failing"; it MUST pass on current code (proves the guard has teeth) and MUST keep passing after the fix.
- [x] 1.7 Add test `test_select_candidates_payload_survives_real_xcom_round_trip`: patch `CongressionalVideoDB.get_unactioned_snapshots` to return `[_raw_candidate_row()]`, call `_run_select_candidates(ti=mock_task_instance)`, assert `result is mock_task_instance.xcom_store["candidates"]`, round-trip that value through `_xcom_round_trip`, then assert the decoded `collected_at` is a `datetime` with `.utcoffset() == timedelta(0)` and equals `_PSYCOPG2_COLLECTED_AT` (satisfies spec scenarios "Candidates with a tz-aware collected_at are normalized before push" and "Round-tripped collected_at is UTC and instant-preserving").
- [x] 1.8 Add test `test_evaluate_candidates_decisions_survive_real_xcom_round_trip`: same `get_unactioned_snapshots` patch plus the `get_checkpoint_view_medians`/`get_video_action_history` patches used at line 164, run `_run_select_candidates` then `_run_evaluate_candidates` on the same `mock_task_instance` with no round trip between them, then round-trip `xcom_store["decisions"]` and assert `decision == "thumbnail_regenerated"` and `collected_at` has UTC offset 0, equal to the original instant (satisfies spec scenario "Derived decisions payload round-trips without raising").
- [x] 1.9 Run `uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization` and confirm exactly: 1.6 passes, 1.7 fails, 1.8 fails (RED evidence for `sdd-apply`; capture this output before touching the DAG file).

## Phase 2: GREEN — Production Fix

- [x] 2.1 In `congress_videos/video_analytics_actions_dag.py`, add `from utils.airflow_helpers import utc_normalize_rows` on its own line between line 48 (`from congress_videos.config.youtube_channels ...`) and line 49 (`from utils.env_loader ...`) — isort accepts this order because `airflow_helpers` sorts before `env_loader`.
- [x] 2.2 In `_run_select_candidates` (line 81), change `result = db.get_unactioned_snapshots()` to `result = utc_normalize_rows(db.get_unactioned_snapshots())` so the same normalized object is both pushed via `ti.xcom_push(key="candidates", value=result)` and returned.
- [x] 2.3 Update the `_run_select_candidates` docstring (or add 1-2 lines) citing issue #605 and the normalization boundary.
- [x] 2.4 Re-run `uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization` and confirm all 3 tests pass (T1 still passes as a bug-pin, T2 and T3 now pass).

## Phase 3: Verification

- [x] 3.1 Run `uv run pytest -n auto` (full suite) and confirm no regressions, including the 8.2-8.5 `TestApplyActions*` classes which consume `decisions` payloads unchanged (spec: "Downstream candidate/decision consumption is unaffected by normalization").
- [x] 3.2 Run `uv run ruff check .` and fix any lint findings introduced by the new import or test additions.
- [x] 3.3 Run `uv run ruff format --check .` and format if needed.
- [x] 3.4 Run `bash scripts/test-airflow-e2e.sh` (change touches `congress_videos/**`, per CLAUDE.md rule); report `unavailable` rather than failing if Docker is not present, and run it manually before merge in that case.

## Phase 4: Documentation

- [x] 4.1 Confirm no other doc updates are required — this is a transport-only fix (design: no migration, no `CONTEXT.md`/ADR change needed per design's Migration/Rollout section).
