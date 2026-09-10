# Tasks: Thumbnail Text Regeneration

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~250 / ~300 / ~340 per slice (~890 total) |
| 400-line budget risk | Medium |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (migration+DB) → PR 2 (helper) → PR 3 (wiring) |
| Delivery strategy | auto-chain |
| Chain strategy | feature-branch-chain |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: feature-branch-chain
400-line budget risk: Medium

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Migration 052 + `claim_thumbnail_text_regeneration`/`record_thumbnail_text_regeneration_outcome` | PR 1 → tracker | `uv run pytest tests/congress_videos/test_database.py -k thumbnail_text_regen` | N/A — no DAG behavior yet, DB-only | Drop the two accessors; columns unused, no callers |
| 2 | Bounded trigger/poll helper `_regenerate_flagged_thumbnail` (unwired) + constants | PR 2 → PR 1 | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -k TestRegenerateFlaggedThumbnail` (actual class name; the forecast's `-k regenerate_flagged_thumbnail` matches zero tests) | N/A — helper has no caller yet | Delete the helper function and constants; nothing imports them |
| 3 | `t6b` branch + hoisted `xcom_push` + operator-signal line + docs | PR 3 → PR 2 | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -k verify_final_copy` | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) | Revert `t6b` to pre-#545 flag-and-publish; migration/helper stay inert |

PR base chain: PR 1 base = `feat/545-thumbnail-text-regeneration` (tracker) · PR 2 base = PR 1 branch · PR 3 base = PR 2 branch.

## Phase 1: Migration + DB Accessors (PR 1)

- [x] 1.1 Create `congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql` adding 7 nullable/defaulted columns to `speaker_turn_videos` only (D1): `thumbnail_regen_attempts`, `thumbnail_regen_exhausted`, `thumbnail_regen_at`, `thumbnail_regen_outcome`, `last_thumbnail_regen_error`, `thumbnail_regen_prior_brief`, `thumbnail_regen_brief`. `video_shorts` untouched.
- [x] 1.2 **DOWN block MUST be commented out** in `052_thumbnail_text_regeneration.sql`, matching `050_final_copy_verification_audit.sql`/`051_persist_title_generation_input.sql` — `migrations_dag` runs the whole file in one transaction, so a live DOWN silently reverts the migration.
- [x] 1.3 RED: write `tests/congress_videos/modules/test_database.py::TestClaimThumbnailTextRegeneration::test_charges_before_second_call` asserting the claim UPDATE increments `thumbnail_regen_attempts`, sets `thumbnail_regen_exhausted` only once attempts reach `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS` (2), and is atomic (conditional `WHERE ... AND NOT exhausted AND attempts < 2`). **Deviation**: the repo's actual DB-accessor test convention (every existing accessor test, including `record_copy_verification_turn`/`record_title_generation_input_turn`) uses the mocked-cursor `db` fixture in `tests/congress_videos/modules/test_database.py`, not a literal live-Postgres fixture — no such fixture exists anywhere in this repo's unit suite. Followed the actual established convention; confirmed RED via `AttributeError: 'CongressionalVideoDB' object has no attribute 'claim_thumbnail_text_regeneration'` before implementation.
- [x] 1.4 GREEN: add `claim_thumbnail_text_regeneration(output_path: str, *, prior_brief: dict | None) -> dict | None` to `congress_videos/modules/database.py`, following the D3 claim-before-act SQL shape (COALESCE write-once `thumbnail_regen_prior_brief`).
- [x] 1.5 RED: write `test_refuses_at_ceiling` — GIVEN 2 prior attempts already recorded and `thumbnail_regen_exhausted = TRUE`, WHEN claim is called again, THEN it returns `None` and `thumbnail_regen_attempts` does not increase (spec: "Exhausted budget refuses a further attempt").
- [x] 1.6 GREEN: confirm 1.4's `WHERE` clause satisfies 1.5 (already passed — no separate code path needed).
- [x] 1.7 RED: write `test_idempotent_on_rerun` — GIVEN a video's attempt was already claimed once, WHEN the upload task is re-run for the same `output_path` (same `t6b` execution semantics), THEN a second claim call within the same attempt cycle does not double-count (spec: "Retrying the step does not accumulate attempts"). Mutation check: pins the exact `WHERE` guard substrings; removing the guard fails this test.
- [x] 1.8 RED: write `test_unknown_output_path_returns_none` — unknown/no `speaker_turn_videos` row (chapter items) returns `None`, not an exception (spec note 8 / design D3: chapter items intentionally publish as-is).
- [x] 1.9 RED: write `test_prior_brief_write_once` — a second successful claim on the same row does NOT overwrite `thumbnail_regen_prior_brief` (COALESCE keeps the first/true-original brief).
- [x] 1.10 GREEN: add `record_thumbnail_text_regeneration_outcome(output_path: str, *, outcome: str, error: str | None = None, regenerated_brief: dict | None = None) -> int` to `database.py`; outcome ∈ `{applied, timeout, trigger_failed, child_failed, invalid_result, not_claimed}`.
- [x] 1.11 RED: write `test_persists_regenerated_brief` — GIVEN `outcome="applied"` with a `regenerated_brief`, THEN both `thumbnail_regen_brief` (new) and `thumbnail_regen_prior_brief` (untouched, from claim) remain independently retrievable (spec: "Both briefs remain retrievable after a landed regeneration").
- [x] 1.12 GREEN: implement 1.11 in `record_thumbnail_text_regeneration_outcome`.

## Phase 2: Bounded Trigger/Poll Helper (PR 2, unwired)

- [x] 2.1 Add constants to `congress_videos/youtube_upload_dag.py`: `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10`, `_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv("UPLOAD_THUMBNAIL_REGEN_MAX_POLLS", "100"))` (1000s bound, D2), `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS = 2` (D3). **Deviation**: `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS` was NOT duplicated in this file — PR1 already defined it in `database.py` and flagged the drift risk; this file has no reference to the ceiling (the DB claim already enforces it), so an unused import would fail `ruff` F401. Only the two poll constants were added here.
- [x] 2.2 RED: write `tests/congress_videos/test_youtube_upload_dag.py::TestRegenerateFlaggedThumbnail::test_completes_within_bound_returns_regenerated_result` with a fake `dag_run` whose `refresh_from_db` flips to `success` after N < 100 polls, monkeypatched `XCom.get_one` returning a valid `{"success": True, "output_path": ...}`, and monkeypatched `time.sleep`. Asserts the helper returns the regenerated result dict. Confirmed RED via `ImportError` before implementation (all 17 new tests in this class failed the same way).
- [x] 2.3 GREEN: implemented `_regenerate_flagged_thumbnail(output_path: str, prior_brief: dict | None, run_id: str, db=None) -> dict | None` in `youtube_upload_dag.py`, modeled on `video_analytics_actions_dag.py::_poll_thumbnail_dag_run` bounded-loop shape (NOT `trigger_thumbnail_generation`'s unbounded `while True`). The function is standalone — no caller yet.
- [x] 2.4 RED: write `test_times_out_after_exactly_max_polls` — GIVEN `dag_run.state` never reaches `success`/`failed`, THEN the helper returns exactly after `_THUMBNAIL_REGEN_MAX_POLLS` (100) iterations with an outcome of `timeout`, never raising (spec: "Regeneration exceeds the bound"). Mutation check executed live: temporarily changed the loop to `range(_THUMBNAIL_REGEN_MAX_POLLS + 1)`, confirmed this test fails (`101 == 100`), then reverted.
- [x] 2.5 RED: write `test_trigger_exception_returns_trigger_failed_never_raises` — `trigger_dag_api` raises; helper catches, returns `{"outcome": "trigger_failed", ...}`, never propagates (spec: "Regeneration failure never raises").
- [x] 2.6 RED: write `test_child_dag_failed_state_returns_child_failed` — `dag_run.state == "failed"`; helper returns `{"outcome": "child_failed", ...}`.
- [x] 2.7 RED: write `test_malformed_xcom_returns_invalid_result` (parametrized: `None`/missing `output_path`/empty `output_path`/`success: False`) plus `test_valid_success_shape_but_nonexistent_path_is_invalid_result` (`os.path.exists` mocked `False`); helper returns `{"outcome": "invalid_result", ...}` and never treats a nonexistent path as a swap-in candidate (design D5).
- [x] 2.8 GREEN: wired 2.4–2.7 into `_regenerate_flagged_thumbnail`'s single `try/except Exception` (no bare `raise` anywhere in the body). **Strengthened beyond the task's own note** — the launch prompt's non-negotiable #1 required this to be enforced by a test in THIS PR, not deferred to 3.9: added `test_no_path_ever_raises`, parametrized over `trigger_raises`/`child_failed`/`invalid_result`/`timeout`/`unexpected_poll_exception` (a `dag_run.refresh_from_db()` exception mid-poll, proving the outer `try/except` covers the whole poll loop, not just the trigger call). Also added `test_outcome_recording_failure_is_swallowed` (design D4 point 3: a DB outage on the bookkeeping write never blocks the regeneration result) and two child-conf shape tests (`previous_brief` forwarded when present, omitted when falsy).

## Phase 3: `t6b` Wiring, Hoisted XCom, Docs (PR 3)

- [ ] 3.1 RED: write `test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger` — GIVEN `verdict.findings` includes `field == "thumbnail_text"`, WHEN `_verify_final_copy` (t6b) runs, THEN `claim_thumbnail_text_regeneration` and `_regenerate_flagged_thumbnail` are each called exactly once (spec: "A thumbnail-text finding triggers a regeneration attempt").
- [ ] 3.2 RED: write `test_verify_final_copy_no_thumbnail_text_finding_zero_claims` — GIVEN no such finding, THEN zero calls to `claim_thumbnail_text_regeneration` (spec: "No thumbnail-text finding, no regeneration"). Mutation check: temporarily always call claim and confirm this test fails.
- [ ] 3.3 GREEN: in `congress_videos/youtube_upload_dag.py::_verify_final_copy` (~line 1420, after the title-reject `raise` at line 1414-1419), add the branch: on a `thumbnail_text` finding, call `db.claim_thumbnail_text_regeneration(output_path, prior_brief=...)`; if claimed, call `_regenerate_flagged_thumbnail(...)`.
- [ ] 3.4 RED: write `test_verify_final_copy_hoisted_xcom_push_fires_without_correction` — GIVEN `verdict.correction_applied == False` AND a landed thumbnail regeneration, WHEN `t6b` completes, THEN `ti.xcom_push(key="upload_config", ...)` IS called (regression guard for the non-negotiable hoist — today's push lives inside `if verdict.correction_applied:` at line 1426 and would silently drop a correction-free regeneration).
- [ ] 3.5 GREEN: hoist the `ti.xcom_push(key="upload_config", value=config)` at line 1426 out of `if verdict.correction_applied:` into a single guarded push covering both the correction mutation and the thumbnail-file swap (design D5: `mutated = True` pattern, ONE push).
- [ ] 3.6 GREEN: on a landed regeneration with an existing output file (`os.path.exists`), set `video["thumbnail_file"] = regen["output_path"]` before the hoisted push (design D5).
- [ ] 3.7 RED: write `test_verify_final_copy_sibling_isolation_by_output_path` — GIVEN two sibling turns sharing `chapter_id` but distinct `output_path`/`video_file`, WHEN turn A's regeneration is triggered, THEN the triggered child `conf["output_path"]` equals turn A's own `video["video_file"]`, never the shared `chapter_id` or turn B's path (spec: "A sibling turn is unaffected by another turn's regeneration"; design D6).
- [ ] 3.8 RED: write `test_verify_final_copy_every_failure_mode_returns_none_never_raises` — parametrized over all six outcomes (`timeout`, `trigger_failed`, `child_failed`, `invalid_result`, `not_claimed`, DB-claim-exception) asserting `_verify_final_copy` returns `None`, `upload_config` XCom is unmutated when no correction/regen landed, and no exception propagates (spec: "No Code Path May Block Or Indefinitely Delay Publication"). Mutation check: temporarily let one branch re-raise and confirm the test catches it.
- [ ] 3.9 RED: write `test_verify_final_copy_title_reject_still_raises_before_any_claim` — GIVEN a verdict with both a `thumbnail_text` finding and a `title` reject, THEN `ValueError` still raises at the existing title-reject line and `claim_thumbnail_text_regeneration` is never called (spec: "Title hard-rejection remains the only blocking path").
- [ ] 3.10 GREEN: confirm 3.3's branch placement (strictly after line 1419's `raise`) satisfies 3.9 without additional code.
- [ ] 3.11 GREEN: add one line to `_copy_verification_problems` (~line 703-750) surfacing "a `thumbnail_text` finding was raised and regeneration did not land" as an informational, non-blocking entry feeding `_check_upload_failures` (design D4 operator signal). RED test first: `test_copy_verification_problems_reports_unlanded_thumbnail_regen`.
- [ ] 3.12 Confirm no Airflow `execution_timeout` is added to the `t6b` `PythonOperator` — add a regression comment near the operator definition citing design D4's rejected-alternative rationale (a task timeout fails `t6b`, skips `t7`, converting a finding into a publication block).
- [ ] 3.13 Update `CONTEXT.md` / add an entry under `docs/adr/` recording: the 1000s poll bound, the 2-attempt spend ceiling, and the audit-only (non-authoritative) semantics of the `video_thumbnails` row for siblings.
- [ ] 3.14 Run `uv run pytest tests/congress_videos/test_youtube_upload_dag.py` in full; run `bash scripts/test-airflow-e2e.sh` to confirm `congress_videos/**` DAG import stays clean.

## Traceability

| Spec requirement / scenario | Task(s) |
|---|---|
| Regeneration triggered only by a thumbnail-text finding | 3.1, 3.2 |
| Attempts claimed against a bounded per-video ceiling (exhausted refuses) | 1.3–1.9 |
| Retrying the step does not accumulate attempts | 1.7 |
| Wait is bounded; timeout publishes as-is | 2.2, 2.4 |
| No code path may block or delay publication | 2.5–2.8, 3.8, 3.12 |
| Title hard-rejection remains the only blocking path | 3.9, 3.10 |
| Both prior and regenerated briefs retained for audit | 1.9, 1.11, 1.12 |
| Regeneration effect scoped to the triggering turn only | 3.7 |
| Scope is long-form only (no shorts wiring) | N/A by construction — no shorts file is touched in any phase |
| final-copy-verification delta: flagged without correction, drives bounded regen | 3.1, 3.3, 3.11 |
| Non-negotiable: hoisted `xcom_push` | 3.4, 3.5 |
| Non-negotiable: migration 052 DOWN commented out | 1.2 |
| Non-negotiable: claim-before-act, threshold 2 | 1.4 |
| Non-negotiable: no `raise` on regeneration path | 2.8, 3.8 |
| Non-negotiable: 1000s poll bound | 2.1, 2.4 |
| Non-negotiable: sibling isolation by file | 3.6, 3.7 |
| Non-negotiable: migration columns on `speaker_turn_videos` | 1.1 |
| Note: chapter items publish as-is (no `speaker_turn_videos` row) | 1.8 (documented, not a bug) |
