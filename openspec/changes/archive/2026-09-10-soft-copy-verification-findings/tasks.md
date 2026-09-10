# Tasks: Soft Copy-Verification Findings (Issue #604)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~150 code+tests+docs.md, +~350 SDD docs ≈ 500 total |
| 400-line budget risk | Medium |
| Chained PRs recommended | No |
| Suggested split | Single PR — one atomic call-site change |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: stacked-to-main
400-line budget risk: Medium

Note: the code/test/doc diff (~150 lines) is under budget alone; the
overage is SDD narrative docs, not reviewable code. One call site, one
docstring set, one test class — no split boundary — ships as
`fix/604-soft-copy-findings` -> `dev`.

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|-----------------------|-----------------|-------------------|
| 1 | Non-blocking findings in `_check_upload_failures` | `fix/604-soft-copy-findings` | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -k TestCheckUploadFailures` | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`); `unavailable` if no Docker | Revert the PR; no migration, no persisted state |

## Phase 1: RED Tests

- [x] 1.1 `tests/congress_videos/test_youtube_upload_dag.py::TestCheckUploadFailures`: add parametrized `test_each_soft_copy_category_alone_does_not_raise` (5 ids: inconclusive, description_reject, discarded_correction, audit_skip, unlanded_thumbnail_regen) — no raise, `xcom_store["copy_verification_warnings"] == _copy_verification_problems(payload)`.
- [x] 1.2 Add `test_soft_copy_findings_are_each_logged_at_warning` — 2 findings, assert 2 WARNING `caplog` records.
- [x] 1.3 Add `test_blocking_and_soft_findings_raise_with_blocking_text_only` — chapter failure + soft finding; raised text has "Chapter upload failures", not "Final-copy verification"; finding still pushed+logged.
- [x] 1.4 Add `test_missing_copy_verification_xcom_still_raises` — raise unchanged AND `xcom_store["copy_verification_warnings"] == []`.
- [x] 1.5 Add `test_clean_run_pushes_empty_warning_list` — no raise, key `== []`, no WARNING logs.
- [x] 1.6 Run `uv run pytest ... -k TestCheckUploadFailures`; confirm the 5 new tests fail RED and the 13 existing `TestCheckUploadFailures` + 10 `TestCopyVerificationProblems` tests still pass.

## Phase 2: GREEN Implementation

- [x] 2.1 `congress_videos/youtube_upload_dag.py` `_check_upload_failures` (~2031-2036): replace the call site with design.md's branch — `None` payload keeps extending `problems`, `copy_warnings=[]`; else `copy_warnings=_copy_verification_problems(copy_payload)`, kept out of `problems`; `logging.warning` each; `ti.xcom_push(key="copy_verification_warnings", value=copy_warnings)` before the raise.
- [x] 2.2 Rewrite the `# NEW (issue #512)` comment as a `# Issue #604` comment.
- [x] 2.3 Re-run the Phase 1 command; confirm all tests pass GREEN.

## Phase 3: Docstring-Only Updates (authorized: docstrings only)

- [x] 3.1 `_copy_verification_problems` docstring (~714-738): findings feed `copy_verification_warnings` XCom, not the `problems` accumulator.
- [x] 3.2 `_check_upload_failures` docstring (~1986-1997): mention the non-blocking split alongside the four blocking sources.
- [x] 3.3 `_verify_final_copy` docstring (~1705-1710): "accumulator" → "non-blocking WARNING + XCom".

## Phase 4: Documentation

- [x] 4.1 Add `copy_verification_warnings` to `docs/DAGS.md` `### XCom keys` list (~line 191).

## Phase 5: Verification

- [x] 5.1 `uv run pytest -n auto` — 0 failures.
- [x] 5.2 `uv run ruff check .` — no new violations (complexity 5→7, under max 10).
- [x] 5.3 `uv run ruff format --check .` — no diffs.
- [ ] 5.4 `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`); deferred to orchestrator (concurrent Docker compose usage in another worktree).
