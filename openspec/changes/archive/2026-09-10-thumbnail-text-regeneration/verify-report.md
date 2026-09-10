```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:53183473ce1564cc5d043d2c47b21bf9ed62b71def9ee413df9b28580f26d815
verdict: pass
blockers: 0
critical_findings: 0
requirements: 8/8
scenarios: 14/14
test_command: uv run pytest -q
test_exit_code: 0
test_output_hash: sha256:eabd698bfe58b877c9c758492131a763c45215afaca130d00e7a917917ea0b8f
build_command: uv run ruff check . && uv run ruff format --check .
build_exit_code: 0
build_output_hash: sha256:3ba0e77e9f67a5db832240a51ac4e6394418722bbf51d40f0181c1cb55633ac9
```
# Verify Report: thumbnail-text-regeneration (issue #545)

**Change**: `thumbnail-text-regeneration`
**Scope verified**: PR 1 (`793c771`) + PR 2 (`c058e47`) + PR 3 (`84667d7`), tip `84667d7` on
`feat/545-slice3-wiring`, base `origin/main@caefba2`.
**Verdict**: **PASS**
**Issues**: 0 CRITICAL, 0 WARNING, 3 SUGGESTION

## Completeness

All 34 tasks in `tasks.md` (Phase 1: 12/12, Phase 2: 8/8, Phase 3: 14/14) are checked `[x]` and were
spot-checked against actual code/tests — no discrepancy found between a claimed-complete task and the
committed diff.

## Independent Test/Lint Evidence (re-run, not trusted from apply-progress)

| Command | Result | Exit |
|---|---|---|
| `uv run pytest -q` (full repo) | `5262 passed, 34 skipped` in ~111s | 0 |
| `uv run ruff check .` | `All checks passed!` | 0 |
| `uv run ruff format --check .` | `320 files already formatted` | 0 |
| `bash scripts/test-airflow-e2e.sh` | not re-run by verifier; apply-progress reports `EXIT_DOCKER_UNAVAILABLE` (4) — Docker daemon genuinely absent in this environment per repo contract (`unavailable`, not pass/fail). Orchestrator separately dry-ran migration 052 against prod schema (applied cleanly, rolled back, 0 pre-existing columns) and will confirm `airflow dags list-import-errors` on the NAS after `git_sync`. |

Exact figures match apply-progress.md's claims (`5262 passed, 34 skipped`) — no discrepancy.

## Requirement -> Test Traceability

### `thumbnail-text-regeneration/spec.md`

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Regeneration triggered only by a thumbnail-text finding | A thumbnail-text finding triggers a regeneration attempt | `TestVerifyFinalCopy::test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger` | PASS |
| | No thumbnail-text finding, no regeneration | `TestVerifyFinalCopy::test_verify_final_copy_no_thumbnail_text_finding_zero_claims` | PASS |
| Attempts claimed against bounded per-video spend ceiling | Exhausted budget refuses a further attempt | `TestClaimThumbnailTextRegeneration::test_refuses_at_ceiling` | PASS |
| | Retrying the step does not accumulate attempts | `TestClaimThumbnailTextRegeneration::test_idempotent_on_rerun` | PASS |
| Wait is bounded; timeout publishes as-is | Regeneration completes within the bound | `TestRegenerateFlaggedThumbnail::test_completes_within_bound_returns_regenerated_result` | PASS |
| | Regeneration exceeds the bound | `TestRegenerateFlaggedThumbnail::test_times_out_after_exactly_max_polls` (exact-iteration: `sleep.call_count == _THUMBNAIL_REGEN_MAX_POLLS == 100`) | PASS |
| No code path may block or delay publication | Regeneration failure never raises | `TestRegenerateFlaggedThumbnail::test_no_path_ever_raises` (5-way parametrized) + `TestVerifyFinalCopy::test_verify_final_copy_every_failure_mode_returns_none_never_raises` (6-way parametrized: timeout/trigger_failed/child_failed/invalid_result/not_claimed/claim_exception) | PASS |
| | Title hard-rejection remains the only blocking path | `TestVerifyFinalCopy::test_verify_final_copy_title_reject_still_raises_before_any_claim` | PASS |
| Both prior and regenerated briefs retained for audit | Prior brief snapshotted before triggering | `TestClaimThumbnailTextRegeneration::test_prior_brief_write_once` (COALESCE write-once) + `TestClaimAndRegenerateThumbnail::test_claimed_attempt_calls_regenerate_flagged_thumbnail` (claim precedes trigger) | PASS |
| | Both briefs retrievable after a landed regeneration | `TestRecordThumbnailTextRegenerationOutcome::test_persists_regenerated_brief` (asserts `thumbnail_regen_prior_brief` is never touched by the outcome write) | PASS |
| Regeneration effect scoped to the triggering turn only | Sibling turn unaffected by another turn's regeneration | `TestVerifyFinalCopy::test_verify_final_copy_sibling_isolation_by_output_path` (exercises the real, unmocked `_regenerate_flagged_thumbnail`/`trigger_dag_api` chain; asserts `conf["output_path"]` is turn A's own file, never the shared `chapter_id`, never turn B's path) | PASS |
| Scope is long-form only | Shorts verification never triggers regeneration | N/A by construction — `git diff --name-only origin/main...HEAD` shows zero changes to `reap_clip_preparer_dag.py`, `reap_processor_dag.py`, `reap_shorts_uploader_dag.py`, or any `video_shorts` code path. Verified independently. | PASS (structural) |

### `final-copy-verification/spec.md` (MODIFIED delta)

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Thumbnail text flagged without correction | Thumbnail text finding is flagged only (verifier never corrects it) | `test_final_copy_verification.py::TestVerifyFinalCopy::test_thumbnail_finding_recorded_corrected_cannot_carry_it` + `test_system_prompt_never_corrects_thumbnail_text` (pre-existing, unmodified by #545, confirmed still passing) | PASS |
| | Long-form finding drives a bounded downstream regeneration | `TestVerifyFinalCopy::test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger` | PASS |

**14/14 scenarios across 8 requirements trace to a real, named, passing test (or a verified structural
absence for the shorts-scope requirement).**

## The Six Suspicious Points — Verified By Direct Code Reading

1. **Hoisted `upload_config` push** (`youtube_upload_dag.py:1758-1784`). `mutated = False` is set before
   the correction branch; `mutated = mutated or regen_mutated` folds in the thumbnail-swap flag; the push
   at line 1784 is gated on `if mutated:` — **not** nested inside `if verdict.correction_applied:`.
   `test_verify_final_copy_hoisted_xcom_push_fires_without_correction` sets `correction_applied=False`
   with a landed regeneration and asserts the push fires. Live mutation check was performed during apply
   (reverted the hoist, confirmed the exact test fails, restored it) — re-confirmed correct by reading the
   current code, not by trusting that log entry.
2. **No raise on any regeneration path.** `_regenerate_flagged_thumbnail`'s trigger+poll body is one
   `try/except Exception` with no re-raise (returns `{"outcome": "trigger_failed", ...}` in the except
   branch); `_claim_and_regenerate_thumbnail` wraps its own claim call in `try/except`, returning `None`
   on a claim-time exception; neither of these callers, nor `_apply_thumbnail_regeneration_if_flagged`,
   nor `_verify_final_copy` re-raises anything from this seam. The branch (`youtube_upload_dag.py:1754`)
   sits strictly after the title-reject `raise` at line 1747-1752. No `execution_timeout=` kwarg exists on
   the `t6b` `PythonOperator` (`task_id="verify_final_copy"`, line 1888-1891) — confirmed by direct
   `grep`/reading, not the claimed review checkpoint alone. Task 3.12's regression comment is present at
   lines 1880-1887 explaining exactly why. `test_verify_final_copy_every_failure_mode_returns_none_never_raises`
   wraps the call in `try/except` and calls `pytest.fail` if anything propagates — genuinely enforced, not
   a tautology.
3. **Child conf satisfies `validate_input`.** `generic_thumbnail_generator_dag.py:67-73` confirmed:
   `_REQUIRED_CONF_KEYS = ("youtube_video_id", "chapter_id", "debate_summary", "session", "domain")` — five
   keys — and `validate_input` (line 81) rejects both a missing key and a falsy/whitespace-only value
   (`if not str(conf[key]).strip(): raise ValueError`). `_build_regen_child_conf`
   (`youtube_upload_dag.py:913-942`) derives `youtube_video_id = str(chapter_id)` and spreads the four
   `_REGEN_REQUIRED_CONF_KEYS` scalars from the same `thumbnail_config` XCom `t4` already reads — all five
   required keys present. `test_forwards_full_child_conf_with_output_path_and_prior_brief` pins the exact
   conf dict sent to `trigger_dag_api`, matching all five keys plus `slug`/`key_speakers`/`previous_brief`.
   PR 2's original minimal conf gap is confirmed fixed in PR 3.
4. **Bounded poll is 1000s (100 x 10s).** `_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv(..., "100"))`,
   `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10` (`youtube_upload_dag.py:88-89`).
   `test_times_out_after_exactly_max_polls` asserts `sleep.call_count == _THUMBNAIL_REGEN_MAX_POLLS == 100`
   and `dag_run.refresh_from_db.call_count == 100` — an exact-iteration assertion, not `>=`. On timeout the
   function returns `{"outcome": "timeout", ...}` without swapping the thumbnail, so `t7` publishes the
   pre-attempt file. `evidence-regeneration-cost.md` confirms the measured p50 214s / p95 888s / max 3989s
   figures cited in the launch prompt and in `docs/DAGS.md`.
5. **Claim-before-act, threshold 2.** `claim_thumbnail_text_regeneration` (`database.py:1443-1515`) is a
   single atomic `UPDATE ... WHERE output_path = %s AND NOT exhausted AND attempts < 2 RETURNING ...` —
   the charge and the ceiling check happen in the same statement, before `_claim_and_regenerate_thumbnail`
   ever calls `_regenerate_flagged_thumbnail` (the function that makes the paid `trigger_dag_api` call).
   `test_charges_before_second_call`/`test_refuses_at_ceiling`/`test_idempotent_on_rerun` pin the exact
   `WHERE` guard substrings (mutation-sensitive). `test_claimed_attempt_calls_regenerate_flagged_thumbnail`
   confirms the DB claim call precedes the regenerate call at the Python call-site level too.
6. **Sibling isolation by file, `video_thumbnails` audit-only.**
   `_build_regen_child_conf` unconditionally overrides `child_conf["output_path"] = output_path` (the
   triggering turn's own `video["video_file"]`), never trusting `thumbnail_config`'s own value.
   `test_verify_final_copy_sibling_isolation_by_output_path` exercises the real (unmocked)
   `_regenerate_flagged_thumbnail`/`trigger_dag_api` chain with two turns sharing `chapter_id=100` and
   asserts the triggered conf's `output_path` equals turn A's file and differs from both the shared
   `chapter_id` string and a hypothetical turn B path. `docs/DAGS.md`'s new section documents the
   `video_thumbnails` row as audit-only/non-authoritative for siblings and honestly flags the residual risk
   (a sibling reading the DB brief while its own `thumbnail.png` is stale) as pre-existing and unresolved by
   this change, not swept under the rug.

## Scope Boundaries — Confirmed

`git diff --name-only origin/main...HEAD` touches exactly: `congress_videos/modules/database.py`,
`congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql`,
`congress_videos/youtube_upload_dag.py`, `docs/DAGS.md`, the `openspec/changes/thumbnail-text-regeneration/`
artifacts, and the two corresponding test files. **Zero** changes to `reap_clip_preparer_dag.py`,
`reap_processor_dag.py`, `reap_shorts_uploader_dag.py`, or any `video_shorts` code path — shorts are
confirmed out of scope.

Migration 052 confirmed: adds exactly 7 nullable/defaulted columns to `speaker_turn_videos` only
(`thumbnail_regen_attempts`, `thumbnail_regen_exhausted`, `thumbnail_regen_at`, `thumbnail_regen_outcome`,
`last_thumbnail_regen_error`, `thumbnail_regen_prior_brief`, `thumbnail_regen_brief`); `video_shorts` is
untouched; the **DOWN block is commented out** (lines 18-26), matching the 050/051 convention, so
`migrations_dag`'s single-transaction execution of the whole file cannot silently revert it.

## Known, Already-Accepted Facts (not re-litigated)

- PR 3 is 759 changed lines against the 400-line review budget, accepted as `size:exception` per the
  attempt-ledger `maintainer_decision` reset with `--objective-relation independent`. PR 2 was 434 lines,
  likewise accepted. Neither slice trimmed comments/docs/tests to fit budget (`work-unit-commits`
  "budget is not code-golf" rule honored — verified by reading the diff, not merely trusting the claim).
- `scripts/test-airflow-e2e.sh` reporting `EXIT_DOCKER_UNAVAILABLE` (4) is a genuine environment
  limitation, not a failure, per this repo's documented contract.
- Chapter items intentionally have no `speaker_turn_videos` row; `claim_thumbnail_text_regeneration`
  returns `None` for them and they publish as-is —
  `test_unknown_output_path_returns_none` and
  `test_verify_final_copy_chapter_item_no_row_intentionally_publishes_as_is` both assert this as intended
  behavior, not a defect.

## Issues

**CRITICAL**: none.

**WARNING**: none.

**SUGGESTION** (non-blocking, defense-in-depth / documentation hygiene only):

1. `_build_regen_child_conf`'s own guard (`if not all(thumbnail_config.get(key) for key in
   _REGEN_REQUIRED_CONF_KEYS)`) checks truthiness only, not whitespace (`generic_thumbnail_generator`'s
   `validate_input` also `.strip()`s). A whitespace-only scalar in `thumbnail_config` would pass this local
   guard, trigger the child DAG, and only fail inside the child's own task — surfacing as `child_failed`
   through the normal poll loop, which is still non-blocking for `t6b`. No functional risk given the
   current non-blocking contract, but worth aligning the guard with `.strip()` for tighter parity with `t4`.
2. `record_thumbnail_text_regeneration_outcome`'s docstring and `design.md` both list `not_claimed` as a
   valid outcome literal, but no production code path ever calls the recorder with that value — when
   `claim_thumbnail_text_regeneration` returns falsy, `_claim_and_regenerate_thumbnail` returns `None`
   immediately without any DB write. A future reader of the audit trail should not expect a `not_claimed`
   row to ever exist; consider dropping it from the documented enum or wiring an explicit (still
   non-blocking) write for symmetry.
3. `_build_regen_child_conf` is called in `_regenerate_flagged_thumbnail` (line 1037) before that
   function's own `try/except Exception` begins (line 1049). It is currently unreachable in practice
   because the sole call site always normalizes `thumbnail_config = ti.xcom_pull(...) or {}` to a `dict`,
   but a future caller that skips that normalization and passes a non-dict-like value could raise an
   uncaught `AttributeError` here, which would propagate all the way to `_verify_final_copy` unguarded.
   Moving the `_build_regen_child_conf` call inside the `try` block would close this theoretical gap and
   make the "no code path raises" guarantee hold by construction rather than by caller discipline.

## Verdict

**PASS.** All 34 tasks complete and verified against code, not merely trusted from apply-progress. Full
test suite re-run independently: `5262 passed, 34 skipped`, exit 0 — matches the claimed figures exactly.
`ruff check` and `ruff format --check` both clean. All 14 spec scenarios across 8 requirements (7 in
`thumbnail-text-regeneration/spec.md` + 1 MODIFIED delta in `final-copy-verification/spec.md`) trace to a
real, named, passing test, or to an independently-verified structural absence (shorts scope). All six
flagged landmines were read directly in source and confirmed correct, not merely asserted by
apply-progress. Migration 052's DOWN block is commented out. Scope boundaries hold: no shorts file touched,
migration touches `speaker_turn_videos` only. Ready for `sdd-archive`.
