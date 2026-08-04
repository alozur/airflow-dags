# Tasks: Conditional Thumbnail Retry

Change: `conditional-thumbnail-retry`
TDD mode: strict (failing tests before every implementation step)
Review budget: 400 lines | Estimated total: ~283 lines | Risk: Low
Chained PRs: No

---

## Group A — Config Layer (parallel with each other, no code dependencies)

### A-1. Add `ART_DIRECTION_RETRY_INSTRUCTION` constant
- **File**: `congress_videos/config/ai_prompts.py`
- **Action**: Add module-level string constant `ART_DIRECTION_RETRY_INSTRUCTION` containing the Spanish instruction to generate a visually distinct alternative brief from `previous_brief`.
- **Spec**: "Retry Art-Direction Brief Is Verifiably Different"
- **TDD order**: Write B-1 tests first (RED), then implement A-1 (GREEN).
- [x] DONE

### A-2. Add `score_retry_threshold: 60` to `_get_thumbnail_config()`
- **File**: `congress_videos/config/thumbnail_config.py`
- **Action**: Add `"score_retry_threshold": 60` to the `congreso` dict inside `_get_thumbnail_config()`. Fix the stale `"Exactly 2 entries"` styles comment to `"1 or 2 entries"`.
- **Spec**: "`score_retry_threshold` Is Domain-Configurable, Default 60"
- **TDD order**: Write B-2 tests first (RED), then implement A-2 (GREEN).
- [x] DONE

---

## Group B — Failing Tests: Config + `art_direct` (before implementation; B-1 and B-2 can run in parallel)

### B-1. Write failing tests for `art_direct(previous_brief=...)` and `ART_DIRECTION_RETRY_INSTRUCTION`
- **File**: `tests/congress_videos/modules/test_thumbnail_generation.py`
- **Add class** `TestArtDirectRetry` (4 tests)
- [x] DONE — all 4 tests GREEN after C-1

### B-2. Write failing tests for `score_retry_threshold` config
- **File**: `tests/congress_videos/modules/test_thumbnail_generation.py`
- **Add class** `TestScoreRetryThreshold` (2 tests)
- [x] DONE — both tests GREEN after A-2

---

## Group C — Implementation: `thumbnail_generation.py` (sequential, after B-1)

### C-1. Add `previous_brief` parameter and retry-instruction injection to `art_direct`
- **File**: `congress_videos/modules/thumbnail_generation.py`
- **Action**: Updated signature; imported ART_DIRECTION_RETRY_INSTRUCTION; injects instruction when previous_brief is set
- [x] DONE — turns B-1 GREEN

### C-2. Write failing tests for 1-option filtering in `choose_best_option` and `persist_results`
- **Note**: Covered by D-4 tests instead (TestTaskChooseBestEmptyFilter + TestTaskPersistResultsEmptyFilter)
- [x] DONE (via D-4)

---

## Group D — Failing Tests: DAG Structure + New Callables (can run in parallel with each other; after C-1)

### D-1. Extend `EXPECTED_TASK_IDS` and `TestDagDependencies`
- **File**: `tests/congress_videos/modules/test_generic_thumbnail_dag.py`
- Added check_score_threshold + art_direction_retry to EXPECTED_TASK_IDS (now 15 IDs)
- Updated generate_thumbnail_option_b upstream assertion to {"art_direction_retry"}
- Added TestDagBranchAndTriggerRule class (BranchPythonOperator type + trigger_rule assertions)
- [x] DONE — all assertions GREEN after E-2

### D-2. Write failing unit tests for `_task_check_score_threshold` callable
- **File**: `tests/congress_videos/modules/test_generic_thumbnail_dag.py`
- Added `TestTaskCheckScoreThreshold` (6 tests)
- [x] DONE — all 6 GREEN after E-1

### D-3. Write failing unit tests for `_task_art_direction_retry` callable
- **File**: `tests/congress_videos/modules/test_generic_thumbnail_dag.py`
- Added `TestTaskArtDirectionRetry` (3 tests)
- [x] DONE — all 3 GREEN after E-1

### D-4. Write failing unit tests for empty-filter in `_task_choose_best` and `_task_persist_results`
- **File**: `tests/congress_videos/modules/test_generic_thumbnail_dag.py`
- Added `TestTaskChooseBestEmptyFilter` + `TestTaskPersistResultsEmptyFilter`
- [x] DONE — all GREEN after E-1

---

## Group E — DAG Implementation (sequential, E-1 then E-2; after all D-* tests)

### E-1. Add new task callables to the DAG module
- **File**: `congress_videos/generic_thumbnail_generator_dag.py`
- Added _task_check_score_threshold, _task_art_direction_retry
- Updated _task_choose_best and _task_persist_results with empty-option filter
- Updated _task_generate_thumbnail to pull correct brief source per option
- [x] DONE

### E-2. Rewire DAG task graph and add new operator instances
- **File**: `congress_videos/generic_thumbnail_generator_dag.py`
- Added BranchPythonOperator for check_score_threshold
- Added art_direction_retry PythonOperator
- trigger_rule="none_failed_min_one_success" on choose_best_option
- Rewired: score_a >> check_score >> [art_direction_retry, choose]; retry path >> choose
- [x] DONE

---

## Group F — Green + Verify (sequential, after E-2)

### F-1. Run full test suite
- **Result**: 1200 passed, 6 pre-existing failures (unrelated to this change), 1 skipped
- [x] DONE

### F-2. Confirm updated assertion for `generate_thumbnail_option_b` upstream
- **Result**: test_generate_thumbnail_option_b_upstream_is_art_direction_retry PASSED
- [x] DONE

---

## Dependency / Parallelism Summary

```
A-1 ──┐                   B-1 ← A-1 → C-1 ──┐
      │ (parallel)                             │
A-2 ──┘                   B-2 ← A-2 ──────────┤
                                               │
C-2 (independent)                              │
                                               ↓
                     D-1, D-2, D-3, D-4 (parallel with each other) ← C-1
                                               │
                                              E-1 → E-2 → F-1 → F-2
```

Critical path: A-1 → B-1 → C-1 → D-* → E-1 → E-2 → F-1

---

## Review Workload Forecast

| File | Estimated changed lines |
|---|---|
| `congress_videos/config/ai_prompts.py` | ~8 |
| `congress_videos/config/thumbnail_config.py` | ~5 |
| `congress_videos/modules/thumbnail_generation.py` | ~15 |
| `congress_videos/generic_thumbnail_generator_dag.py` | ~55 |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | ~80 |
| `tests/congress_videos/modules/test_generic_thumbnail_dag.py` | ~120 |
| **Total** | **~283 lines** |

- Chained PRs recommended: **No**
- 400-line budget risk: **Low**
- Decision needed before apply: **No**
