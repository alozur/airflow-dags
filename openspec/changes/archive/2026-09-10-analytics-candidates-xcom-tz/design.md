# Design: video_analytics_actions candidates XCom TZ Normalization (issue #605)

## Technical Approach

Normalize once at the push site. `_run_select_candidates`
(`congress_videos/video_analytics_actions_dag.py:81`) wraps the DB result in
`utc_normalize_rows`. The one normalized list is both pushed as `candidates` and returned
(`return_value`). `_run_evaluate_candidates` spreads `**candidate` (line 127), which carries the
UTC `collected_at` into `decisions`, so the latent second crash site closes with no edit. This is
the same convention as `video_analytics_dag.py:211`.

## Architecture Decisions

| # | Option | Tradeoff | Decision |
|---|---|---|---|
| D1 | Push-site `utc_normalize_rows` | One call site. It covers `candidates`, `return_value` and `decisions` | **Chosen** |
| D1 | Normalize at the pull sites (evaluate, record_no_ops, apply) | Three sites, and a missed one re-crashes | Rejected |
| D2 | Module-level import | Matches `video_analytics_dag.py:29` and this file's top-level `utils.*` imports | **Chosen** |
| D2 | Import inside the function | Diverges from the module's style for no parse-time benefit | Rejected |
| D3 | Copy `_xcom_round_trip` into this test module | Self-contained. A bug-pin should not weaken if another suite changes | **Chosen** |
| D3 | `from tests.utils.test_airflow_helpers import _xcom_round_trip` | Imports a private name across test modules. The repo's shared-helper home is `tests/helpers/`, not another test module | Rejected |
| D4 | New `_raw_candidate_row()` builder | Models the `get_unactioned_snapshots` shape. Leaves the `_decision_row` fixtures used by the apply tests alone | **Chosen** |
| D4 | Add `collected_at` to `_decision_row` | Changes the `_snapshot_age_days` input in about 50 apply tests with no gain | Rejected |

**D2 details.** The new import goes on its own line between line 48
(`from congress_videos.config.youtube_channels ...`) and line 49 (`from utils.env_loader ...`).
isort accepts this order because `airflow_helpers` sorts before `env_loader`.

**DagBag safe-mode gotcha: not applicable.** That gotcha affects non-DAG helper modules that get
parsed standalone. This change edits a real DAG file. `utils/airflow_helpers.py` is unchanged,
and `video_analytics_dag.py` already imports it at parse time. The test file sits under
`tests/`, which `.airflowignore` excludes.

**D3 details.** The copy is byte-identical to `tests/utils/test_airflow_helpers.py:21-23`, which
is the same choice #546's D3 made. That precedent said to revisit extraction "if a fourth site
appears", and the repo now has 6 copies. Moving them into `tests/helpers/xcom.py` is a follow-up.
It is out of scope for a hotfix that is blocking production.

## Data Flow

    get_unactioned_snapshots ─→ utc_normalize_rows ─→ XCom "candidates" (+ return_value)
       (collected_at +02:00)       (boundary)                 │
                                                  evaluate: {**candidate, ...}
                                                              └─→ XCom "decisions" ─→ record_no_ops / apply_actions

## File Changes

| File | Action | Description | ~Lines |
|---|---|---|---|
| `congress_videos/video_analytics_actions_dag.py` | Modify | +1 import. Line 81 becomes `result = utc_normalize_rows(db.get_unactioned_snapshots())`. +1–2 docstring lines citing #605 | ~4 |
| `tests/congress_videos/test_video_analytics_actions_dag.py` | Modify | Imports, helper, builder and new test class (see below) | ~80 |
| `utils/airflow_helpers.py` | Unchanged | Reused as is | 0 |

Estimated total: **about 85 authored lines**, well under 400. The DAG file grows from 587 to
about 590 lines. It has no per-file ignores, and the wrap adds no branches.

## Interfaces / Contracts

Test-module additions:
- **Imports:** `import json` (stdlib group), `timezone` added to the `datetime` import, and
  `from airflow.utils.json import XComDecoder, XComEncoder` after `import pytest`.
- **`_PSYCOPG2_COLLECTED_AT`:** `datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))`.
- **`_raw_candidate_row()`:** takes `_decision_row()`, pops `decision`, `views`, `median_views`
  and `sample_size` (the same pattern as line 157), then sets `collected_at`.
- **`_xcom_round_trip(value)`:** module-level, as described in D3.

## Testing Strategy

The new class is `TestCandidatesXComNormalization`. It goes after `TestRecordNoOps`, around line
214, under a section comment. Every assertion goes through the real encoder and decoder. Dict
equality against `mock_task_instance.xcom_store` does not count as evidence, because that store
never serializes anything.

| Test | How it runs | What it asserts | Current code | After fix |
|---|---|---|---|---|
| T1 `test_raw_candidate_row_breaks_real_xcom_round_trip` | `_xcom_round_trip([_raw_candidate_row()])` | Raises `ValueError` with `match="ZoneInfo keys must be normalized relative paths"`. Never normalizes (bug-pin) | PASS | PASS |
| T2 `test_select_candidates_payload_survives_real_xcom_round_trip` | Patch `CongressionalVideoDB.get_unactioned_snapshots` to return `[_raw_candidate_row()]`, then run `_run_select_candidates(ti=mock_task_instance)` | `result is xcom_store["candidates"]` (so `return_value` is covered). After the round trip, `collected_at` is a `datetime` with `.utcoffset() == timedelta(0)` and equals `_PSYCOPG2_COLLECTED_AT` | **FAIL** (ValueError in the round trip) | PASS |
| T3 `test_evaluate_candidates_decisions_survive_real_xcom_round_trip` | Same patch, plus the `get_checkpoint_view_medians` and `get_video_action_history` patches from line 164. Run select, then `_run_evaluate_candidates` on the **same** ti, with no round trip in between | After the `xcom_store["decisions"]` round trip: `decision == "thumbnail_regenerated"`, `collected_at` has UTC offset 0 and equals the original instant | **FAIL** at the decisions hop | PASS |

T3 skips the round trip between the two tasks on purpose. That way its failure on current code
lands on the latent `decisions` site rather than the `candidates` hop, which T2 already covers.
The candidates evaluate receives are exactly what select pushed.

**RED evidence for `sdd-apply`:** run
`uv run pytest tests/congress_videos/test_video_analytics_actions_dag.py -k XComNormalization`
before editing the DAG. Expect 2 failed (T2, T3) and 1 passed (T1). After the edit, expect 3
passed. Then run `uv run pytest`, `uv run ruff check .`, `uv run ruff format --check .`, and the
Docker e2e (the change touches `congress_videos/**`).

## Threat Matrix

N/A. No routing, shell, subprocess, VCS/PR automation, executable-file classification or
process-integration boundary changes.

## Migration / Rollout

No migration is needed. The fix only takes effect in production after it reaches `main` and the
NAS runs `git_sync`; until then every `video_analytics_actions` run keeps crashing at
`evaluate_candidates`. To roll back, revert the single commit.

## Open Questions

None blocking. Follow-up: move the 6 `_xcom_round_trip` copies into `tests/helpers/`.
