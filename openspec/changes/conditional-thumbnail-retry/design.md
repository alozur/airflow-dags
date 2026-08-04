# Design: Conditional Thumbnail Retry (score-gated single generation)

## Technical Approach

Replace the always-2-parallel Pikzels pipeline in `generic_thumbnail_generator` with a single primary generation gated by a `BranchPythonOperator` (Approach A from the proposal). Generate `option_a` → score it; a branch task compares `main_score` against a domain-configurable `score_retry_threshold` (default 60) and either short-cuts to `choose_best_option` (fast path, 1 call/1 row) or runs a retry lane (`art_direction_retry → generate/download/score option_b`) that forces a DIFFERENT brief via `previous_brief`. `choose_best_option` and its downstream join must accept the fast path where retry tasks are `skipped`.

## New Task Graph

```
validate_input
  → resolve_participant_photo
    → art_direction                                [PythonOperator]
      → generate_thumbnail_option_a → download_option_a → score_option_a
        → check_score_threshold                    [BranchPythonOperator]
            ├─[score <  threshold]→ art_direction_retry [PythonOperator]
            │     → generate_thumbnail_option_b → download_option_b → score_option_b ─┐
            └─[score >= threshold]───────────────────────────────────────────────────┤
                                                              → choose_best_option ◄──┘  (trigger_rule=none_failed_min_one_success)
                                                                → generate_title
                                                                  → persist_results
                                                                    → thumbnail_result
```

| Task | Operator | Upstream |
|------|----------|----------|
| `check_score_threshold` | `BranchPythonOperator` | `{score_option_a}` |
| `art_direction_retry` | `PythonOperator` | `{check_score_threshold}` |
| `generate_thumbnail_option_b` | `PythonOperator` | `{art_direction_retry}` |
| `download_option_b` / `score_option_b` | `PythonOperator` | chain (unchanged callables) |
| `choose_best_option` | `PythonOperator` (`trigger_rule="none_failed_min_one_success"`) | `{check_score_threshold, score_option_b}` |

Existing `option_a` chain and the `_task_generate_thumbnail`/`_task_download_option`/`_task_score_option` callables are reused unchanged for `option_b`.

## Architecture Decisions

### Decision: BranchPythonOperator over no-op gate / inline retry
**Choice**: `BranchPythonOperator` named `check_score_threshold`.
**Alternatives considered**: (B) single "gate" PythonOperator with a no-op skip; (C) inline retry inside `_task_generate_thumbnail`.
**Rationale**: Native branching renders the decision in the Airflow UI, marks the retry lane `skipped` (not fake-`success`), preserves per-task retry granularity, and matches the existing `_task_*`-callable pattern. B produces misleading success states; C loses observability.

### Decision: `trigger_rule="none_failed_min_one_success"` on `choose_best_option`
**Choice**: Set it ONLY on `choose_best_option`. Downstream `generate_title`/`persist_results`/`thumbnail_result` keep default `all_success` (their sole upstream never skips once `choose_best_option` runs).
**Alternatives considered**: leave default `all_success` (KEY defect — fast path skips retry tasks, so `all_success` on the join never fires and persist is silently skipped); `none_failed`.
**Rationale**: On the fast path the retry lane is `skipped`; `none_failed_min_one_success` fires when no upstream failed and ≥1 succeeded (score_option_a on fast path, score_option_b on retry path). This is the single most error-prone point in the change.

### Decision: `art_direction_retry` pulls the ORIGINAL brief
**Choice**: `art_direction_retry` calls `art_direct(summary, domain_cfg, previous_brief=<brief pulled from task_ids="art_direction">)`.
**Rationale**: Passing the original brief lets the retry force a contrasting approach. Pulling from itself or `score_option_a` would be wrong.

### Decision: retry instruction constant
**Choice**: Add `ART_DIRECTION_RETRY_INSTRUCTION` in `ai_prompts.py`; inside `art_direct`, when `previous_brief` is not None, append it (formatted with the prior brief) to the initial `user_prompt` — reusing the same `extra_instruction` append mechanism already in `_call_api`.
**Rationale**: Keeps prompt text auditable in one place; no new plumbing.

## Interfaces / Contracts

```python
# thumbnail_generation.py — backward-compatible
def art_direct(debate_summary: str, domain_cfg: dict,
               previous_brief: dict | None = None) -> dict: ...

# generic_thumbnail_generator_dag.py — branch decision
def _task_check_score_threshold(ti, **ctx) -> str:
    score = (ti.xcom_pull(task_ids="score_option_a") or {}).get("main_score", 0.0)
    conf = ti.xcom_pull(task_ids="validate_input") or {}
    threshold = get_domain_config(conf["domain"]).get("score_retry_threshold", 60)
    return "art_direction_retry" if score < threshold else "choose_best_option"

# choose / persist — handle 1-or-2 options
options = [x for x in [option_a, option_b] if x]  # option_b XCom is {} on fast path
```

`thumbnail_config.py`: add `"score_retry_threshold": 60` to the `congreso` domain dict inside `_get_thumbnail_config()`; update the stale `"styles"` comment ("Exactly 2 entries…" → "primary `option_a` + retry `option_b`"). Read via `get_domain_config(domain).get("score_retry_threshold", 60)`.

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/generic_thumbnail_generator_dag.py` | Modify | Add `check_score_threshold` (BranchPythonOperator) + `art_direction_retry`; rewire graph; `trigger_rule` on `choose_best_option`; `_task_choose_best`/`_task_persist_results` filter empties |
| `congress_videos/modules/thumbnail_generation.py` | Modify | `art_direct` gains `previous_brief` param + conditional instruction |
| `congress_videos/config/thumbnail_config.py` | Modify | Add `score_retry_threshold: 60`; fix stale styles comment |
| `congress_videos/config/ai_prompts.py` | Modify | Add `ART_DIRECTION_RETRY_INSTRUCTION` |
| `tests/.../test_generic_thumbnail_dag.py` | Modify | `EXPECTED_TASK_IDS`, dependency assertions, branch/retry cases |
| `tests/.../test_thumbnail_generation.py` | Modify | `previous_brief` cases; 1-option persist case |

## Testing Strategy

| Layer | What to Test | Approach |
|-------|-------------|----------|
| Unit | `_task_check_score_threshold` returns `art_direction_retry` (score<threshold) / `choose_best_option` (score>=threshold) | fake TI + patched `get_domain_config` |
| Unit | `art_direction_retry` pulls `task_ids="art_direction"` and forwards as `previous_brief` | assert `art_direct` call args |
| Unit | `art_direct(previous_brief=...)` injects retry instruction into user_prompt | patch `generate_json_completion`, assert prompt substring |
| Unit | `choose`/`persist` with only `option_a` (empty `option_b`) → 1 option, no score-0 comparison | fake TI with `{}` for option_b |
| Structure | `EXPECTED_TASK_IDS` includes `check_score_threshold`, `art_direction_retry` | `TestDagTaskIds` |
| Structure | upstream sets: `check_score_threshold={score_option_a}`, `art_direction_retry={check_score_threshold}`, `generate_thumbnail_option_b={art_direction_retry}`, `choose_best_option={check_score_threshold, score_option_b}`; assert `choose_best_option.trigger_rule == "none_failed_min_one_success"` | `TestDagDependencies` |

T-03 (`EXPECTED_TASK_IDS` frozen set) fails first — the correct RED signal.

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or process-integration boundary. Change is confined to in-process Airflow task wiring and Python callables.

## Migration / Rollout

No migration required. `persist_results` upserts by `(chapter_id, label)`; a high-score run now writes only `option_a`. Any pre-existing `option_b` row from earlier 2-option runs is left in place (stale-row cleanup is out of scope per issue). Single `git revert` rolls back — no schema/data change.

## Open Questions

- None. All coupling points, XCom sources, and the `trigger_rule` constraint are grounded in current code.
