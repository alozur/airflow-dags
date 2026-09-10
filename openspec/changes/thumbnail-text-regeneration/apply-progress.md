# Apply Progress: thumbnail-text-regeneration (issue #545) — PR 1 + PR 2 + PR 3

**Mode**: Strict TDD
**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-545`
**Branch (PR 3 batch)**: `feat/545-slice3-wiring` (base: `feat/545-slice2-regen-helper`, which is
already in this branch's history at `c058e47`; `feat/545-slice1-migration` at `793c771`)
**Batch**: Third and final batch. Merged with PR 1's and PR 2's progress below — no completed task
from either prior batch was lost.

## Scope Delivered

- **PR 1 (Phase 1)**: migration 052 + two `database.py` accessors. Completed in an earlier batch.
- **PR 2 (Phase 2)**: the bounded, deliberately UNWIRED `_regenerate_flagged_thumbnail` trigger/poll
  helper + its two poll constants in `congress_videos/youtube_upload_dag.py`. Completed in an earlier
  batch.
- **PR 3 (Phase 3, this batch)**: wired `t6b` (`_verify_final_copy`) to claim-and-trigger a regeneration
  on a `thumbnail_text` finding, hoisted the `upload_config` XCom push, added the operator-signal line to
  `_copy_verification_problems`, the D4 regression comment on the `t6b` operator, and the `docs/DAGS.md`
  entry. Resolved the child-conf gap PR2 explicitly flagged for this batch.

## Completed Tasks

### Phase 1 (PR 1)

- [x] 1.1 Migration 052: 7 additive columns on `speaker_turn_videos` only (D1)
- [x] 1.2 DOWN block commented out (verified against 050/051 convention)
- [x] 1.3 RED: `test_charges_before_second_call`
- [x] 1.4 GREEN: `claim_thumbnail_text_regeneration`
- [x] 1.5 RED: `test_refuses_at_ceiling`
- [x] 1.6 GREEN: confirmed by 1.4's WHERE clause (no separate code path)
- [x] 1.7 RED: `test_idempotent_on_rerun` (mutation-sensitive WHERE-guard pin)
- [x] 1.8 RED: `test_unknown_output_path_returns_none`
- [x] 1.9 RED: `test_prior_brief_write_once`
- [x] 1.10 GREEN: `record_thumbnail_text_regeneration_outcome`
- [x] 1.11 RED: `test_persists_regenerated_brief`
- [x] 1.12 GREEN: implemented by 1.10's single UPDATE (no separate code path)

### Phase 2 (PR 2)

- [x] 2.1 Constants added to `youtube_upload_dag.py`: `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10`,
      `_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv("UPLOAD_THUMBNAIL_REGEN_MAX_POLLS", "100"))`.
      `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS` was deliberately NOT duplicated here (see PR2 deviations below).
- [x] 2.2 RED: `TestRegenerateFlaggedThumbnail::test_completes_within_bound_returns_regenerated_result`
- [x] 2.3 GREEN: `_regenerate_flagged_thumbnail(output_path, prior_brief, run_id, db=None) -> dict | None`
      (signature later extended in PR 3 — see below)
- [x] 2.4 RED: `test_times_out_after_exactly_max_polls` (mutation check executed live)
- [x] 2.5 RED: `test_trigger_exception_returns_trigger_failed_never_raises`
- [x] 2.6 RED: `test_child_dag_failed_state_returns_child_failed`
- [x] 2.7 RED: `test_malformed_xcom_returns_invalid_result` (parametrized ×4) +
      `test_valid_success_shape_but_nonexistent_path_is_invalid_result`
- [x] 2.8 GREEN: single `try/except Exception`, no bare `raise` anywhere in the body — AND enforced by a
      dedicated test in the same PR (`test_no_path_ever_raises`, parametrized ×5), not deferred to Phase 3.

### Phase 3 (PR 3, this batch)

- [x] 3.1 RED: `test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger`
- [x] 3.2 RED: `test_verify_final_copy_no_thumbnail_text_finding_zero_claims`
- [x] 3.3 GREEN: `t6b` branch wired — `_apply_thumbnail_regeneration_if_flagged` calls
      `_claim_and_regenerate_thumbnail`, which calls `db.claim_thumbnail_text_regeneration` then, if
      claimed, `_regenerate_flagged_thumbnail`. See Deviations for the child-conf signature change.
- [x] 3.4 RED: `test_verify_final_copy_hoisted_xcom_push_fires_without_correction` (live mutation check:
      reverted the hoist, confirmed the test fails, restored it)
- [x] 3.5 GREEN: hoisted `ti.xcom_push(key="upload_config", value=config)` into a single `if mutated:`
      guard covering both the correction mutation and the thumbnail-file swap
- [x] 3.6 GREEN: `video["thumbnail_file"] = regen["output_path"]` on a landed regeneration whose path
      exists on disk, before the hoisted push
- [x] 3.7 RED: `test_verify_final_copy_sibling_isolation_by_output_path` (exercises the real, unmocked
      `_regenerate_flagged_thumbnail` so the actual `trigger_dag_api` conf is inspectable end to end)
- [x] 3.8 RED: `test_verify_final_copy_every_failure_mode_returns_none_never_raises` (parametrized ×6:
      `timeout`, `trigger_failed`, `child_failed`, `invalid_result`, `not_claimed`, `claim_exception`;
      live mutation check: let the claim exception re-raise, confirmed `claim_exception` fails, reverted)
- [x] 3.9 RED: `test_verify_final_copy_title_reject_still_raises_before_any_claim`
- [x] 3.10 GREEN: confirmed by 3.3's branch placement (strictly after the title-reject `raise`) — no
      separate code path needed
- [x] 3.11 GREEN: `_copy_verification_problems` gained the informational "regeneration did not land" line;
      `test_copy_verification_problems_reports_unlanded_thumbnail_regen` +
      `test_copy_verification_problems_landed_regen_is_not_a_finding`
- [x] 3.12 Review checkpoint: no `execution_timeout` on the `t6b` `PythonOperator` (verified); regression
      comment added citing design D4
- [x] 3.13 `docs/DAGS.md` updated (see Deviations — `CONTEXT.md`/`docs/adr/` intentionally not created)
- [x] 3.14 Full `tests/congress_videos/test_youtube_upload_dag.py` run + full-repo `uv run pytest` run +
      `bash scripts/test-airflow-e2e.sh` attempted (Docker unavailable, exit 4, not a failure)

## Files Changed

### PR 1

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql` | Created | 7 additive/nullable columns on `speaker_turn_videos`; DOWN block commented out, matching 050/051 |
| `congress_videos/modules/database.py` | Modified | Added `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS = 2` constant; `claim_thumbnail_text_regeneration()`; `record_thumbnail_text_regeneration_outcome()` |
| `tests/congress_videos/modules/test_database.py` | Modified | Added `TestClaimThumbnailTextRegeneration` (8 tests) and `TestRecordThumbnailTextRegenerationOutcome` (5 tests) |

### PR 2

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/youtube_upload_dag.py` | Modified | Added `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS`/`_THUMBNAIL_REGEN_MAX_POLLS` constants and the standalone `_regenerate_flagged_thumbnail` helper (trigger + bounded poll + outcome recording) |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Added `TestRegenerateFlaggedThumbnail` (17 tests) |

### PR 3 (this batch)

| File | Action | What Was Done |
|------|--------|----------------|
| `congress_videos/youtube_upload_dag.py` | Modified | `_regenerate_flagged_thumbnail` signature extended with `thumbnail_config: dict`; new `_build_regen_child_conf` (child-conf builder + t4-style guard idiom); new `_claim_and_regenerate_thumbnail` (claim-before-act, never raises); new `_apply_thumbnail_regeneration_if_flagged` (t6b-facing orchestration, extracted to keep C901 complexity bounded); `_verify_final_copy` wired: hoisted `upload_config` push, thumbnail-file swap, `thumbnail_regen_landed` in the `copy_verification` XCom; `_copy_verification_problems` gained the unlanded-regen finding; D4 regression comment on the `t6b` `PythonOperator`. +250/−31 lines. |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Updated all 17 PR2 `TestRegenerateFlaggedThumbnail` call sites for the new signature; added `test_forwards_full_child_conf_with_output_path_and_prior_brief` (replaces the old minimal-conf assertion) and `test_incomplete_thumbnail_config_never_triggers_records_trigger_failed` (parametrized ×4); added `TestClaimAndRegenerateThumbnail` (3 tests); added 9 new tests to `TestVerifyFinalCopy` (3.1, 3.2, 3.4, 3.7, 3.8 ×6 parametrized, 3.9, plus the chapter-item non-negotiable test); added 2 tests to `TestCopyVerificationProblems`. +440/−19 lines. |
| `docs/DAGS.md` | Modified | New "Regeneracion acotada de miniatura por texto flagged (issue #545)" section: 2-attempt spend ceiling, 1000s poll bound (with measured p50/p95/max), the deliberate absence of `execution_timeout`, the audit-only `video_thumbnails` semantics for siblings, and the intentional no-row behaviour for chapter items. +38/−1 lines. |

## TDD Cycle Evidence (PR 3)

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 3.1/3.3 | `tests/congress_videos/test_youtube_upload_dag.py` | Unit | 253 pre-batch tests in file passing before edit | ✅ written against the pre-wiring code (no `_claim_and_regenerate_thumbnail` call site existed) | ✅ passed once the branch + helpers were wired | ✅ covered jointly with 3.4/3.6 (thumbnail_file swap, hoisted push) | ✅ extracted `_apply_thumbnail_regeneration_if_flagged`/`_build_regen_child_conf` post-GREEN to satisfy `ruff` C901; re-ran full file (253 passed) after each extraction |
| 3.2 | same | Unit | (same batch) | ✅ written | ✅ passed | ➖ single case (findings=[]) | ➖ none needed |
| 3.4 | same | Unit | (same batch) | ✅ written FIRST as a failing assertion against the pre-hoist code shape | ✅ passed once hoisted | ✅ **Live mutation check executed**: reverted the hoist to push only inside `if verdict.correction_applied:`, confirmed `test_verify_final_copy_hoisted_xcom_push_fires_without_correction` fails with an `AssertionError` naming the missing `xcom_push(key='upload_config', ...)` call, then restored the hoist and re-confirmed green | ➖ none needed |
| 3.7 | same | Unit | (same batch) | ✅ written against the real (unmocked) `_regenerate_flagged_thumbnail`/`trigger_dag_api` chain | ✅ passed | ➖ single case, by construction the override is unconditional | ➖ none needed |
| 3.8 | same | Unit | (same batch) | ✅ written, 6-way parametrized | ✅ passed | ✅ 6 cases: timeout/trigger_failed/child_failed/invalid_result (all via a mocked `_regenerate_flagged_thumbnail` return), not_claimed (claim returns `None`), claim_exception (claim raises) | ✅ **Live mutation check executed**: temporarily let the claim-time exception re-raise inside `_claim_and_regenerate_thumbnail` (removed its `except`), confirmed exactly the `claim_exception` parametrized case fails with `RuntimeError('db is down')` propagating, then reverted |
| 3.9 | same | Unit | (same batch) | ✅ written | ✅ passed — confirmed by 3.3's placement, no new code | ➖ single case | ➖ none needed |
| 3.11 | same | Unit | (same batch) | ✅ written (2 tests: unlanded-is-a-finding, landed-is-not) | ✅ passed | ✅ both polarities | ➖ none needed |
| n/a (guard idiom) | same | Unit | (same batch) | ✅ `test_incomplete_thumbnail_config_never_triggers_records_trigger_failed`, 4-way parametrized over each required key, written before the guard existed in `_regenerate_flagged_thumbnail` | ✅ passed | ✅ 4 cases: `chapter_id`, `debate_summary`, `session`, `domain` each independently missing | ✅ **Live mutation check executed**: temporarily replaced the guard condition with `if False:`, ran the guard test — it correctly hung on a real (unbounded, unmocked) `time.sleep` inside the poll loop it should never have entered, confirming the guard is load-bearing; killed the runaway process and reverted the guard immediately |
| chapter-item non-negotiable | same | Unit | (same batch) | ✅ `test_verify_final_copy_chapter_item_no_row_intentionally_publishes_as_is` written | ✅ passed | ➖ single case (claim returns `None` for a row-less `output_path`) | ➖ none needed |

### Test Summary (PR 3)

- **Total tests added/changed**: 21 net new tests (9 in `TestVerifyFinalCopy`, 3 in the new
  `TestClaimAndRegenerateThumbnail`, 2 new guard/conf tests replacing 1 old one in
  `TestRegenerateFlaggedThumbnail`, 2 in `TestCopyVerificationProblems`); all 17 pre-existing PR2
  `TestRegenerateFlaggedThumbnail` tests updated in place for the new `thumbnail_config` parameter with
  NO behavioural change to what they assert.
- **File total**: 253 tests in `tests/congress_videos/test_youtube_upload_dag.py` (was 232 after PR 2).
- **Full repo**: `5262 passed, 34 skipped` (was `5241 passed, 34 skipped` after PR 2 — net +21, matching
  the new tests above; zero regressions, zero new skips).
- **Layers used**: Unit only — this is a DAG-callable wiring change with no new runtime/process boundary.
- **Live mutation checks performed this batch**: 3 (hoisted push, claim-exception never-raises, guard
  idiom) — all confirmed RED under the mutation and GREEN after revert.

## Deviations from Design

1. **Child-conf gap resolved via a signature change to `_regenerate_flagged_thumbnail`** (flagged by PR2
   as Deviation #2, explicitly resolved per the orchestrator's direction for this batch). PR2's helper
   built a minimal `{"output_path": ..., "previous_brief": ...}` conf that would fail
   `generic_thumbnail_generator`'s own `validate_input` (`_REQUIRED_CONF_KEYS = ("youtube_video_id",
   "chapter_id", "debate_summary", "session", "domain")` — five keys, not the four scalar values named in
   PR2's own note). Fixed by adding a `thumbnail_config: dict` first parameter, built the SAME way `t4`
   (`trigger_thumbnail_generation`) builds its own child conf, from the SAME `thumbnail_config` XCom that
   `t6b` already has access to (pushed once per run by `_prepare_thumbnail_config`, t3). `output_path` is
   always overridden to the parameter value (the triggering turn's own `video["video_file"]`), never
   trusting whatever `thumbnail_config` itself carries under that key — this is the concrete mechanism
   behind design D6's sibling isolation. When any of the four required scalars is missing/empty, the new
   `_build_regen_child_conf` returns `None` and the caller records a non-blocking `trigger_failed` outcome
   without ever calling `trigger_dag_api` — mirroring t4's own guard idiom exactly, per the launch prompt.
   This DOES change PR2's already-landed function signature; all 17 of PR2's tests were updated in the
   same commit to pass a valid `thumbnail_config` fixture, with zero change to what each test asserts
   about polling/timeout/failure-mode behaviour.
2. **Two extraction refactors were required to satisfy the repo's `ruff` C901 complexity gate (issue
   #269)**, discovered only after wiring `t6b` end-to-end: `_regenerate_flagged_thumbnail` (11 > 10) and
   `_verify_final_copy` (11 > 10). Resolved by extracting `_build_regen_child_conf` (conf-building +
   guard) out of the former, and `_apply_thumbnail_regeneration_if_flagged` (the entire claim-trigger-swap
   seam) out of the latter. Both extractions are REFACTOR-phase, behavior-preserving — confirmed by
   re-running the full test file (253 passed) after each extraction, with no test assertions changed.
   `_apply_thumbnail_regeneration_if_flagged` is a new testable unit fully covered indirectly by every
   `TestVerifyFinalCopy` regeneration-path test; no redundant direct unit tests were added for it, since
   the strict-TDD requirement was "test the behavior", not "test every extracted function in isolation".
3. **`docs/DAGS.md` was updated instead of `CONTEXT.md`/`docs/adr/`** (task 3.13's literal wording).
   Neither `CONTEXT.md` nor `docs/adr/` exists anywhere in this repository, and
   `docs/agents/domain.md` is explicit that these are created LAZILY by the `/domain-modeling` skill when
   terms or decisions actually get resolved — never created upfront by an unrelated feature PR. This
   repo's actual living DAG-behavior doc is `docs/DAGS.md` (it already documents issue #512's
   `verify_final_copy` addition in the same task-graph section), so the #545 entry landed there, in
   Spanish, matching that file's existing language and section conventions.
4. **`thumbnail_regen_landed` was added as a new field on the `copy_verification` XCom payload** — not
   explicitly named in tasks.md, but required to implement 3.11's operator-signal line without
   re-deriving "did the regeneration land" from scratch inside `_copy_verification_problems` (which has no
   access to the regeneration outcome otherwise). Backward compatible: `payload.get("thumbnail_regen_landed")`
   defaults to falsy for any payload predating this field.

## Issues Found

None beyond the deviations documented above (all pre-flagged handoffs from PR2, or mechanical
consequences of wiring/complexity-gate compliance — no design contradictions or open bugs).

## Remaining Tasks

None. All 34 tasks across Phase 1 + Phase 2 + Phase 3 are complete.

## Workload / PR Boundary

- Mode: chained PR slice (`auto-chain`, `feature-branch-chain`)
- Current work unit: Unit 3 — "`t6b` branch + hoisted `xcom_push` + operator-signal line + docs" (final
  slice of the chain)
- Boundary: starts from PR 2's landed, unwired helper (no DAG behavior change yet); ends with `t6b` fully
  wired, hoisted-push non-negotiable satisfied, operator-signal line live, and the D4 no-timeout
  regression comment in place. PR 3 targets PR 2's branch (`feat/545-slice2-regen-helper`).
- Rollback boundary: revert `t6b`'s regeneration branch, the `_verify_final_copy` hoist, the
  `_copy_verification_problems` line, `_claim_and_regenerate_thumbnail`,
  `_apply_thumbnail_regeneration_if_flagged`, `_build_regen_child_conf`, and the `docs/DAGS.md` section —
  this restores the exact pre-#545 flag-and-publish behaviour. Migration 052 and the PR2 helper stay
  inert (additive, unused by anything else) exactly as design.md's Migration/Rollout section states.
- **Review budget note (overage, reported honestly per policy)**: `git diff --numstat` for this batch:
  `congress_videos/youtube_upload_dag.py` +250/−31, `tests/congress_videos/test_youtube_upload_dag.py`
  +440/−19, `docs/DAGS.md` +38/−1 — **759 total changed lines** against the 400-line session budget
  (≈ 90% over). This is the smallest cohesive PR 3 deliverable: the orchestrator explicitly scoped this
  batch as "PR 3 ONLY (final slice)" bundling `t6b` wiring + the hoisted-push non-negotiable + the
  operator-signal line + docs as one atomic unit, and additionally directed resolving PR2's child-conf gap
  in this same batch (a signature change cascading into updating all 17 pre-existing PR2 tests). Neither
  comments, docs, nor test coverage were trimmed to fit the budget, per the `chained-pr`/`work-unit-commits`
  "budget is not code-golf" rule — the six explicitly-mandated non-negotiable tests (3.1, 3.2, 3.4, 3.7,
  3.8 ×6, 3.9) plus the guard-idiom and chapter-item non-negotiable tests account for the majority of the
  test-file growth. One honest slicing pass was made (extracting `_build_regen_child_conf` and
  `_apply_thumbnail_regeneration_if_flagged` for the complexity gate, which is a genuine size reduction,
  not a re-slice); no further cohesive split of this already-final, already-narrowly-scoped chain slice
  was identified. Recommending `size:exception` for this slice.

## Work Unit Evidence (PR 3)

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -q --no-cov` → `253 passed` |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → `[test-airflow-e2e] Docker daemon is not reachable (docker info failed); skipping e2e (unavailable).` exit code `4` (`EXIT_DOCKER_UNAVAILABLE`) — genuinely unavailable in this environment, not a failure; DAG-import cleanliness for this change is otherwise covered by `tests/congress_videos/test_youtube_upload_dag.py::TestYoutubeUploadDagLoads` (part of the 253 passing) |
| Rollback boundary | Revert `_verify_final_copy`'s regeneration branch/hoist, `_copy_verification_problems`'s new line, `_claim_and_regenerate_thumbnail`, `_apply_thumbnail_regeneration_if_flagged`, `_build_regen_child_conf`, the `_regenerate_flagged_thumbnail` signature change, the `t6b` operator comment, and the `docs/DAGS.md` section — restores the exact pre-#545 flag-and-publish behaviour; migration 052 and PR2's helper stay inert |

## Full-Suite Verification (after PR 3)

- `uv run pytest -q` (full repo): `5262 passed, 34 skipped` in ~41s, exit code 0. (5241 baseline after
  PR 1 + PR 2, + 21 net new tests in this batch = 5262; zero failures, zero new skips.)
- `uv run ruff check .` → `All checks passed!` (including the two C901 complexity findings surfaced and
  resolved by extraction during this batch — see Deviation #2).
- `uv run ruff format --check .` → initially flagged 2 files (one long comprehension line in production
  code, one long parametrize/call line in tests); ran `uv run ruff format .` to auto-wrap them, then
  re-verified `320 files already formatted` clean and `ruff check .` still `All checks passed!`. Re-ran the
  full test file (253 passed) and the full repo suite (5262 passed, 34 skipped) after formatting — no
  logic changed by the reformat.
- `bash scripts/test-airflow-e2e.sh` → Docker daemon unreachable, `EXIT_DOCKER_UNAVAILABLE` (exit 4) — not
  a failure; reported honestly, not faked as a pass.

## Status

12/12 Phase 1 + 8/8 Phase 2 + 14/14 Phase 3 = **34/34 total tasks complete across all three phases**.
Ready for `sdd-verify` of PR 1 + PR 2 + PR 3 scope (the complete `thumbnail-text-regeneration` change).
