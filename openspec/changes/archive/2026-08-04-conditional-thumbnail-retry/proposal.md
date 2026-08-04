# Proposal: Conditional Thumbnail Retry (score-gated single generation)

## Intent

`generic_thumbnail_generator` always makes 2 parallel Pikzels calls per run — doubling
generation cost and latency even when the first thumbnail is already good. Source: issue #44.
Change it to generate **1** thumbnail by default and only re-generate a second, *different*
option when the first score is below a domain-configurable threshold (default 60). Success =
lower average Pikzels spend with no quality regression on borderline thumbnails.

## Scope

### In Scope
- Generate 1 thumbnail → score it; keep it when `main_score >= threshold` (1 Pikzels call, 1 row).
- On `main_score < threshold`: re-run art direction with `previous_brief` (forced *different* approach), generate + score a 2nd option, keep the higher score (2 calls, 2 rows, `is_chosen=TRUE` on winner).
- Domain-configurable threshold (`score_retry_threshold`, default 60) in `thumbnail_config.py`.
- Extend `art_direct(...)` with backward-compatible `previous_brief: dict | None = None`.
- Update DAG wiring, `choose_best_option`/`persist_results` for 1-or-2 options dynamically, and tests.

### Out of Scope
- Post-generation thumbnail editing (#18); titles pipeline changes; DB migration.
- Cleanup of stale `option_b` rows from prior 2-option runs.

## Capabilities

### New Capabilities
None (no `openspec/specs/` capability directory exists; behavior lives in DAG + module code).

### Modified Capabilities
None at the formal-spec level — this is a behavioral change to the `generic_thumbnail_generator` DAG, verified via tests.

## Approach

**Approach A — BranchPythonOperator** (recommended by exploration). Add `check_score_threshold`
(BranchPythonOperator) after `score_option_a`: returns the retry path (`art_direction_retry →
generate_thumbnail_option_b → download_option_b → score_option_b`) when `score < threshold`, else
branches straight to `choose_best_option`. `art_direction_retry` pulls the ORIGINAL brief from
`art_direction` and passes it as `previous_brief`. `choose_best_option` and downstream join must
use `trigger_rule="none_failed_min_one_success"` so the fast path (skipped retry tasks) still
reaches persist. `choose_best_option`/`persist_results` build options as `[x for x in [a, b] if x]`.

Chosen over the no-op gate (misleading "success" states, no UI branch) and inline-retry (loses
per-task observability, coarse retry granularity) for native Airflow branching + UI clarity.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/generic_thumbnail_generator_dag.py` | Modified | Add branch + retry art-direction; dynamic choose/persist |
| `congress_videos/modules/thumbnail_generation.py` | Modified | `art_direct` gains `previous_brief` |
| `congress_videos/config/thumbnail_config.py` | Modified | Add `score_retry_threshold`; fix stale "styles" comment |
| `congress_videos/config/ai_prompts.py` | Modified | Optional `ART_DIRECTION_RETRY_INSTRUCTION` constant |
| `tests/congress_videos/modules/test_generic_thumbnail_dag.py` | Modified | `EXPECTED_TASK_IDS`, deps, branch/retry cases |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Modified | `previous_brief` + 1-option persist cases |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Default `trigger_rule="all_success"` silently skips persist on fast path | High | Set `none_failed_min_one_success` on join; assert in tests (KEY design constraint for sdd-design) |
| `choose_best_option` scores empty option_b as 0 | Med | Filter `[x for x in [a,b] if x]` before max |
| `art_direction_retry` pulls wrong brief | Med | Pull `task_ids="art_direction"`, verify in test |

## Rollback Plan

Behavioral change confined to one DAG + one module + config. Revert the PR (single git revert) —
no schema/data migration, so no cleanup required. `persist_results` upsert keys are unchanged.

## Dependencies

- Exploration `sdd/conditional-thumbnail-retry/explore` (Engram #331).

## Success Criteria

- [ ] `score >= threshold` → exactly 1 Pikzels generation call + 1 persisted row.
- [ ] `score < threshold` → 2 generation calls + 2 rows, `is_chosen=TRUE` on higher score.
- [ ] Retry brief is verifiably different (previous brief passed with "different approach" instruction).
- [ ] Threshold read from domain config (default 60).
- [ ] Fast path reaches `persist_results` (trigger_rule correct); tests green for high/low/comparison.
