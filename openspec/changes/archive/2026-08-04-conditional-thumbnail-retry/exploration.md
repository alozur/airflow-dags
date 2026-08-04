# Exploration — conditional-thumbnail-retry

> SDD exploration artifact (mirror of Engram topic `sdd/conditional-thumbnail-retry/explore`, id 331).
> Source: GitHub issue #44 — feat(thumbnail-dag): 1 thumbnail con retry condicional si score < 60.

## Executive Summary

The `generic_thumbnail_generator` DAG currently always makes 2 Pikzels calls in
parallel. Replacing this with a single call + `BranchPythonOperator` conditional
retry (triggered when `main_score < 60`) requires ~4 new/modified task callables
in the DAG, a backward-compatible signature extension to `art_direct`, one new
config key in `thumbnail_config.py`, and significant test updates to the frozen
`EXPECTED_TASK_IDS` set and dependency-graph assertions. The approach is
well-contained and ready for proposal.

## Current State

```
validate_input → resolve_participant_photo → art_direction
  → generate_thumbnail_option_a → download_option_a → score_option_a ─┐
  → generate_thumbnail_option_b → download_option_b → score_option_b ─┘
                                                         → choose_best_option
                                                           → generate_title
                                                             → persist_results → thumbnail_result
```

Every run makes 2 Pikzels generation calls and 2 download+score calls
unconditionally.

## Affected Areas

| File | Why affected |
|------|-------------|
| `congress_videos/generic_thumbnail_generator_dag.py` | Core DAG wiring; add `check_score_threshold` (BranchPythonOperator), `art_direction_retry` task; update `_task_choose_best` and `_task_persist_results` for 1-or-2 option logic; update `trigger_rule` on `choose_best_option`. |
| `congress_videos/modules/thumbnail_generation.py` | Extend `art_direct` with `previous_brief: dict \| None = None` parameter. |
| `congress_videos/config/thumbnail_config.py` | Add `score_retry_threshold: int = 60` per domain; update stale "Exactly 2 entries" comment. |
| `congress_videos/config/ai_prompts.py` | Optional: add `ART_DIRECTION_RETRY_INSTRUCTION` constant for the "different approach" instruction. |
| `tests/congress_videos/modules/test_generic_thumbnail_dag.py` | Update `EXPECTED_TASK_IDS` frozen set; update `TestDagDependencies` upstream assertions; add `TestCheckScoreThreshold`, `TestArtDirectionRetry`, updated `TestChooseBest`/`TestPersistResults` cases. |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Add `previous_brief` cases in `TestArtDirect`; add 1-option case in `TestPersistResults`. |

## Approaches

| Approach | Pros | Cons | Effort |
|----------|------|------|--------|
| **A. BranchPythonOperator** (recommended) | Native Airflow branching; visual clarity in UI; skipped tasks clearly shown; correct task-state semantics | `trigger_rule` must be `none_failed_min_one_success` on `choose_best_option`; +2 task IDs | Medium |
| **B. No-op gate PythonOperator** | No trigger_rule changes; simpler test graph assertions | Retry tasks show "success" even when not run; misleading UI | Low-Medium |
| **C. Inline retry in `_task_generate_thumbnail`** | Zero new tasks; minimal test surface | Breaks per-task retry granularity; loses observability | Low |

**Recommendation: Approach A.**

## Key Risk — trigger_rule on converging tasks

On the fast path (score >= 60), `generate_thumbnail_option_b`, `download_option_b`,
`score_option_b` are skipped by Airflow. With default `trigger_rule="all_success"`,
`choose_best_option` would also be skipped — silently skipping persist/title/result.
Fix: `trigger_rule="none_failed_min_one_success"` on `choose_best_option`. Highest-risk
implementation detail.

## Risks

1. `trigger_rule` misconfiguration on `choose_best_option` → silent DAG skip on fast path (highest risk).
2. `_task_choose_best` XCom pull must guard against empty `option_b` dict (filter list).
3. `art_direction_retry` must pull the ORIGINAL `art_direction` XCom as `previous_brief`, not generate a new brief from scratch.
4. `EXPECTED_TASK_IDS` is a frozen set — all graph-shape tests RED immediately (intended per strict TDD).
5. Stale `option_b` rows in `video_thumbnails` from previous runs are not cleaned by this change (out of scope per issue).

## Next Recommended

`sdd-propose`
