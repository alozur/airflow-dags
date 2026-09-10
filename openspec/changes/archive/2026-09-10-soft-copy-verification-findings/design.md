# Design: Soft Copy-Verification Findings (issue #604)

## Technical Approach

This is proposal approach A. `_copy_verification_problems` stays byte-identical. Only the call site in `_check_upload_failures` (`congress_videos/youtube_upload_dag.py:2031-2036`) changes: the payload is routed on `is None`. A missing payload still appends the helper's own sentence to the blocking `problems` list. Every other finding gets one WARNING log and is pushed to XCom, and it is never added to `problems`. The raise condition (`if problems:`) is unchanged. This implements the delta spec `final-copy-verification` (the ADDED "Non-Blocking Surfacing..." requirement).

## Architecture Decisions

| Decision | Choice | Rejected | Rationale |
|---|---|---|---|
| Missing-XCom routing | `if payload is None: problems.extend(_copy_verification_problems(None))` | Hard-code the sentence; filter by string prefix | Calling the helper reuses its exact sentence, so no string drift. Branching on `None` states the rule instead of pattern-matching text. |
| XCom key | `copy_verification_warnings` | `copy_verification_findings` | `copy_verification_findings` is already a JSONB column (migration 050) holding raw verifier dicts. The XCom holds operator sentences, and a shared name would mislead operators reading either. |
| Push on every path | Always push, with `[]` when the run is clean or the payload is missing | Push only when non-empty | Gives a stable contract: the key exists whenever t9 gets past the chapter guard, which is the spec's "clean run pushes empty list" scenario. It also makes retries deterministic without depending on Airflow's per-try XCom clearing. |
| Push before raise | Push precedes `if problems: raise` | Push after | In mixed runs the soft findings must still reach the XCom (spec scenario "Mixed..."). |
| C901 | Inline, no new helper | Extract `_split_copy_verification(...)` | The file has no C901 per-file-ignore (`max-complexity = 10`). Ruff's mccabe counts `if`/`for`, not bool-ops or comprehensions. Complexity is 5 today (1 plus 4 `if`s) and becomes 7 after the change (+1 `if`, +1 `for`). A helper would add API surface for no gain. |
| Missing payload on the legitimate `upload_config=None` path | Stays blocking | Narrow it to soft | Confirmed decision. It preserves today's behavior and is the only failure signal on that path. |

## Interfaces / Contracts

Replacement for lines 2031-2036:

```python
        # Issue #604 (supersedes #512 design D7 for these findings): by t9
        # the video is already published, so copy-verification findings
        # carry no re-upload consequence. They surface as WARNING logs plus
        # the `copy_verification_warnings` XCom and never fail this task on
        # their own. A MISSING `copy_verification` XCom is structural (t6b
        # may not have run) and stays a blocking finding. A title reject
        # never reaches here — it already raised in _verify_final_copy.
        copy_payload = ti.xcom_pull(key="copy_verification")
        if copy_payload is None:
            problems.extend(_copy_verification_problems(None))
            copy_warnings: list[str] = []
        else:
            copy_warnings = _copy_verification_problems(copy_payload)
        for warning in copy_warnings:
            logging.warning("Non-blocking final-copy verification finding: %s", warning)
        ti.xcom_push(key="copy_verification_warnings", value=copy_warnings)
```

Docstring of `_check_upload_failures`: the blocking list becomes chapter DB failures, unpublished thumbnails, turn findings and a missing `copy_verification` XCom. Add one sentence stating that copy-verification findings are logged at WARNING and pushed to `copy_verification_warnings` without raising (#604). In the `_verify_final_copy` docstring (~1709), change "surfaced through the `_check_upload_failures` accumulator" to "surfaced by `_check_upload_failures` as non-blocking WARNING + XCom".

## Data Flow

    t6b verify_final_copy ──XCom copy_verification──→ t9 check_upload_failures
                                                        ├─ None   → problems (raise)
                                                        └─ dict   → helper → WARNING ×N
                                                                          → XCom copy_verification_warnings

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/youtube_upload_dag.py` | Modify | Call-site split, t9 docstring, `_verify_final_copy` docstring line |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modify | 5 new tests (9 cases) in `TestCheckUploadFailures` |
| `docs/DAGS.md` | Modify | Add `copy_verification_warnings` to the `### XCom keys` list (no drift test guards this list) |

Estimated change: roughly +20/-7 in the DAG, about +120 in tests and +1/-1 in docs, for a total of about 150 lines. That is well under the 400-line budget.

## Testing Strategy

The new tests go in `TestCheckUploadFailures`. They use `_make_ti` with a clean chapter payload and a clean turn payload, and read the pushed key through `ti.xcom_store`. Logs are checked with `caplog.at_level(logging.WARNING)`; the DAG logs to the root logger.

| Test | Arrange | Assert | RED on current code |
|---|---|---|---|
| `test_each_soft_copy_category_alone_does_not_raise` (parametrized, 5 ids: inconclusive, description_reject, discarded_correction, audit_skip, unlanded_thumbnail_regen) | one category per payload | no raise; `xcom_store["copy_verification_warnings"] == _copy_verification_problems(payload)` and non-empty | Yes (raises) |
| `test_soft_copy_findings_are_each_logged_at_warning` | inconclusive + unsupported finding, `corrected_applied=False` (2 findings) | 2 WARNING records, each containing one finding | Yes |
| `test_blocking_and_soft_findings_raise_with_blocking_text_only` | chapter `recorded_failures=1` + discarded correction | raises; message contains "Chapter upload failures" and NOT "Final-copy verification"; XCom holds the finding; WARNING logged | Yes (message contains copy text) |
| `test_missing_copy_verification_xcom_still_raises` | copy key absent, everything else clean | `pytest.raises(match="copy_verification XCom missing after prepare_upload_config succeeded")`; `xcom_store["copy_verification_warnings"] == []` | Yes (on the XCom assertion only; the raise already happens) |
| `test_clean_run_pushes_empty_warning_list` | clean payload | no raise; key equals `[]`; no WARNING records | Yes (key absent) |

**Existing tests.** The actual counts are 13 in `TestCheckUploadFailures` and 10 in `TestCopyVerificationProblems`, not the 12 and 9 quoted in the brief. All of them stay unchanged and keep passing:
- Tests without a copy payload either raise for a blocking reason (`match` is `re.search`) or still raise on the missing payload.
- The three no-raise tests supply a clean `pass` payload.
- The helper is untouched.

I could not run anything in this phase. The RED column is predicted from reading the code, and `sdd-apply` must confirm it.

## Threat Matrix

N/A. The change touches no routing, shell, subprocess, VCS/PR automation, executable-file classification, or process-integration boundary.

## Migration / Rollout

No migration is required. Rollback is a single-PR revert.

## Open Questions

- [ ] The `_copy_verification_problems` docstring ("worth failing the daily upload gate... appended to the accumulator") becomes inaccurate, but the helper is kept untouched per the confirmed decision. A docstring-only fix would need orchestrator approval. This does not block the change.
