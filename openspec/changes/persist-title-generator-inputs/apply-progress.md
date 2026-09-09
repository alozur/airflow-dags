# Apply Progress: Persist Title Generator Input Payloads (issue #549)

## Slice 1 — Migration + write methods (base: `feat/549-generator-input-payloads`)

**Branch**: `feat/549-slice1-migration-db` (base `feat/549-generator-input-payloads`, contains planning
commit `0d9d11c`)
**Commit**: `12f3728` — `feat(db): persist title generator input payloads (migration 051)`
**Status**: COMPLETE — all slice 1 tasks (1.1-1.9) done. Not pushed, no PR opened (orchestrator handles
delivery).

### Completed Tasks

- [x] 1.1 `congress_videos/sql/migrations/051_persist_title_generation_input.sql` — two
  `ADD COLUMN IF NOT EXISTS title_generation_input JSONB` statements (`speaker_turn_videos`,
  `video_shorts`); DOWN block fully commented out.
- [x] 1.2 Mirrored both `ADD COLUMN` lines into `congress_videos/sql/production_schema.sql`
  (`speaker_turn_videos` block, before the `UNIQUE (turn_id)` constraint; `video_shorts` block, as the
  final column).
- [x] 1.3 Added `"title_generation_input"` to `TABLE_COLUMNS["speaker_turn_videos"]` and to
  `VIDEO_SHORTS_COLUMNS` in `tests/congress_videos/sql/test_production_schema.py` (see Deviations for
  why `video_shorts` uses a different constant than `TABLE_COLUMNS`).
- [x] 1.4 `CongressionalVideoDB.record_title_generation_input_turn(output_path, *, payload) -> int`
  added to `congress_videos/modules/database.py`, right after `record_copy_verification_short`.
  Unguarded `UPDATE ... SET title_generation_input = %s::jsonb WHERE output_path = %s` — no
  `IS DISTINCT FROM` guard, per Req 3 / design D3 / C4. Binds via
  `json.dumps(payload, ensure_ascii=False)` (the `_brief_json` convention from
  `thumbnail_generation.py:880-884`). Raises `ValueError` on falsy `output_path` or non-dict/empty
  `payload`. Returns `cur.rowcount` unconditionally — the caller (slice 2) is responsible for treating
  0 as a loud `no_row` outcome.
- [x] 1.5 `CongressionalVideoDB.record_title_generation_input_short(short_id, *, payload) -> int` —
  same shape, keyed by `video_shorts.id`.
- [x] 1.6 Unit tests in `tests/congress_videos/modules/test_database.py`
  (`TestRecordTitleGenerationInputTurn`, `TestRecordTitleGenerationInputShort`): assert the `UPDATE`
  SQL text, the `%s::jsonb` bind, `json.dumps(..., ensure_ascii=False)` argument, `rowcount`
  passthrough (including `rowcount == 0` returned faithfully, never swallowed), and `ValueError` on
  falsy key / non-dict / empty-dict payload for both methods.
- [x] 1.7 Scenario 3.1 test (`test_grouped_siblings_update_by_output_path_only`): asserts the `WHERE`
  clause is `output_path = %s` only, with no `turn_id` filter in the SQL text, and that a mocked
  `rowcount=3` (3 sibling rows) is returned — i.e. one call structurally updates every row sharing that
  `output_path`.
- [x] 1.8 Scenario 3.2a test (`test_rerun_same_output_path_overwrites_without_raising`): two
  sequential calls with the same `output_path` both succeed, `rowcount >= 1` both times, no exception.
- [x] 1.9 `uv run pytest tests/congress_videos/sql/test_production_schema.py
  tests/congress_videos/modules/test_database.py` — 314 passed. Full-repo `uv run pytest` — 5115
  passed, 34 skipped (Postgres-dependent live tests correctly skipped), exit code 0.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and result | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database.py -q` → `314 passed` |
| Full-suite command and result | `uv run pytest -q` → `5115 passed, 34 skipped`, exit 0 |
| Runtime harness | N/A for slice 1 — no DAG import surface changed (`database.py` write methods are unwired until slice 2/3); `bash scripts/test-airflow-e2e.sh` deferred to task 3.10 at the end of the chain per tasks.md |
| Rollback boundary | `git revert 12f3728` (or drop the branch pre-merge) fully removes migration 051, the `production_schema.sql` mirror, the two `database.py` write methods, and all new tests; columns stay additive/nullable and no caller references them, so revert is safe with zero blast radius |

### TDD Cycle Evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 1.3 schema drift columns | `test_column_present_in_block[title_generation_input]` failed (2 tests) before the `production_schema.sql` mirror | Mirrored both `ADD COLUMN` lines → both tests pass | Updated `TestVideoShortsTableSnapshot` docstring column count (29→30) for accuracy |
| 1.4/1.5 write methods | 20 new tests failed with `AttributeError: no attribute 'record_title_generation_input_turn'/'_short'` before implementation | Added both methods → all 20 pass | `ruff format` reflowed one over-length call; re-verified green after format |
| 1.6/1.7/1.8 | Same RED batch as above (single implementation pass covers all three task IDs — the SQL/bind/rowcount/ValueError/Scenario-3.1/Scenario-3.2a tests were all written before the methods existed) | Same GREEN as above | — |

### Files Changed

| File | Action | What Was Done |
|---|---|---|
| `congress_videos/sql/migrations/051_persist_title_generation_input.sql` | Created | Two `ADD COLUMN IF NOT EXISTS title_generation_input JSONB`; DOWN commented out |
| `congress_videos/sql/production_schema.sql` | Modified | Mirrored both `ADD COLUMN` lines into the `speaker_turn_videos` and `video_shorts` blocks |
| `congress_videos/modules/database.py` | Modified | Added `record_title_generation_input_turn` and `record_title_generation_input_short` |
| `tests/congress_videos/sql/test_production_schema.py` | Modified | Added `"title_generation_input"` to `TABLE_COLUMNS["speaker_turn_videos"]` and `VIDEO_SHORTS_COLUMNS`; updated a docstring count |
| `tests/congress_videos/modules/test_database.py` | Modified | Added `TestRecordTitleGenerationInputTurn` and `TestRecordTitleGenerationInputShort` (20 tests) |
| `openspec/changes/persist-title-generator-inputs/tasks.md` | Modified | Marked tasks 1.1-1.9 `[x]` |

### Deviations from Design

1. **Task 1.3 / design C1-c wording vs. actual test structure**: the design and tasks say "add
   `title_generation_input` to the `TABLE_COLUMNS` tuple entries for BOTH `speaker_turn_videos` and
   `video_shorts`". In the actual `test_production_schema.py`, only `speaker_turn_videos` lives in the
   `TABLE_COLUMNS` dict; `video_shorts`'s authoritative column list is a separate constant,
   `VIDEO_SHORTS_COLUMNS`, inside `TestVideoShortsTableSnapshot` (confirmed by reading the file — the
   `test_column_present_in_block` parametrization at line ~499 iterates `VIDEO_SHORTS_COLUMNS`, not
   `TABLE_COLUMNS`). I verified this before assuming the task description was literally accurate (per
   the launch prompt's explicit instruction to verify, not assume). I updated both structures — the
   intent (make the drift check catch a missing `video_shorts` column) is fully satisfied; only the
   named-constant wording in the tasks file was imprecise. Confirmed by RED→GREEN: both
   `test_column_present_in_block[title_generation_input]` (speaker_turn_videos) and
   `test_column_present_in_block[title_generation_input]` (video_shorts) failed before the schema
   mirror and passed after.
2. No other deviations — implementation otherwise matches design.md D3/C1/C4 and tasks.md exactly.

### Issues Found

None.

### Remaining Tasks (slice 1 scope — none; carried forward from slice 1 for history)

- [x] 2.1-2.13 Turn path — see Slice 2 section below.
- [ ] 3.1-3.10 Shorts path: `build_shorts_title_payload` + `_generate_metadata` hook (base: slice 2
  branch) — NOT started, untouched.

### Workload / PR Boundary

- Mode: feature-branch-chain (slice 1 of 3), `auto-chain` delivery strategy
- Current work unit: Unit 1 — "Migration 051 + schema snapshot mirror + `database.py` write methods"
- Boundary: starts from `feat/549-generator-input-payloads` (planning commit `0d9d11c` only), ends at
  commit `12f3728` on `feat/549-slice1-migration-db`. Independently verifiable: `uv run pytest`
  (unit + drift test all green); the two columns exist in the migration and the schema snapshot; both
  write methods are callable and unit-tested in isolation, with zero callers wired yet.
- Estimated review budget impact: 293 insertions + 14 deletions = 307 changed lines (git diff stat),
  under the 400-line budget and close to the design's ~305-line forecast for this slice.

### Status (slice 1 only)

9/9 slice-1 tasks complete. Ready for verify (slice 1 scope only) / ready for slice 2 apply.

---

## Slice 2 — Turn path (base: `feat/549-slice1-migration-db`)

**Branch**: `feat/549-slice2-turn-path` (base `feat/549-slice1-migration-db`, contains commits `12f3728`,
`6c5459b`)
**Commit**: `94f1a27` — `feat(thumbnails): persist turn title-generator input payloads (#549)`
**Status**: COMPLETE — all slice 2 tasks (2.1-2.13) done. Not pushed, no PR opened (orchestrator handles
delivery).

### Completed Tasks

- [x] 2.1 `build_turn_title_payload(summary, best, title, *, sibling_titles=None, key_speakers=None,
  forbidden_title=None, participant_slug=None) -> dict` added to
  `congress_videos/modules/thumbnail_generation.py`, right after `generate_title`. Built from explicit
  literal keys only; `best` reduced to `{"label", "style", "prompt"}`; `key_speakers` entries
  normalized (`str` kept, `dict` reduced to `entry["name"]`, anything else dropped, empty list → `None`
  matching `sibling_titles`' own falsy-normalization contract). Never spreads `{**best}` or a source
  dict.
- [x] 2.2 `TestBuildTurnTitlePayload` in `tests/congress_videos/modules/test_thumbnail_generation.py`:
  declared-keys-only assertion (`set(json.loads(json.dumps(payload)))` == the 9 schema keys), a
  recursive credential/URL/path scanner finding nothing, `best`'s `local_path`/`output_url` dropped, and
  `key_speakers` dict entries (carrying `photo_url`/`slug`) reduced to names only.
- [x] 2.3 `_task_thumbnail_result` in `congress_videos/generic_thumbnail_generator_dag.py` now pulls
  `fetch_recent_history` XCom and calls `build_turn_title_payload` with the same values
  `_task_generate_title` consumed (immutable XCom rebuild — byte-exact with the prompt), attaching the
  result under `title_generation_input` alongside the 4 legacy keys. Only built when `title` is truthy
  (mirrors the E-2 "no title → no payload" case). `_task_generate_title`'s `str` return type is
  untouched — zero consumers changed.
- [x] 2.4 `TestTaskThumbnailResult` (updated) + new `TestTaskGenerateTitleReturnTypeUnchanged` in
  `tests/congress_videos/modules/test_generic_thumbnail_dag.py`: exact-dict contract test now includes
  `title_generation_input`; a new sibling-titles-forwarding test; a "title is None → payload is None"
  test; and an explicit `isinstance(result, str)` assertion on `_task_generate_title`'s return.
- [x] 2.5 `trigger_thumbnail_generation` in `congress_videos/youtube_upload_dag.py` gains an optional
  `db=None` parameter (matching `_prepare_thumbnail_config(item, db)`). Immediately before the existing
  `ti.xcom_push(key="thumbnail_result", value=result)`, reads `payload = result.get("title_generation_input")`
  (never added to the strict 4-key validation conjunction at `:808-816`) and
  `key = thumbnail_config.get("output_path")` (never `result["output_path"]`, per D3).
- [x] 2.6 New `_write_title_provenance(payload, key, db=None) -> dict` helper (extracted to keep
  `trigger_thumbnail_generation`'s cyclomatic complexity under ruff's C901 threshold of 10 — issue #272
  is an active repo-wide concern) implements the `upload_marking.py`-style `try/except`: catches any DB
  exception → `status="failed"`; `rowcount == 0` → `status="no_row"` (WARNING logged); `rowcount >= 1`
  → `status="written"`; missing/malformed payload or key → `status="skipped"`. Never re-raises, never
  swallows silently. Result pushed to a new `title_provenance` XCom key.
- [x] 2.7 `test_zero_rows_is_no_row_not_success` in
  `tests/congress_videos/test_youtube_upload_dag.py`: injected fake `db` returning `0` from
  `record_title_generation_input_turn` → `title_provenance == {"status": "no_row", "rows": 0, "error": None}`,
  WARNING asserted via `caplog`.
- [x] 2.8 `test_db_exception_is_caught_and_publication_continues`: injected fake `db` raising
  `RuntimeError` → `title_provenance["status"] == "failed"` with the error string, exception does not
  propagate, `trigger_thumbnail_generation` still returns the child run id and a valid
  `thumbnail_result`.
- [x] 2.9 Verified via the pre-existing `TestPrepareUploadConfigTurnRequiresThumbnailTitle` (untouched —
  no new test added; `_prepare_upload_config` itself was not modified by this slice, so its existing
  parametrized coverage for missing/failed/empty-title `ValueError` already satisfies this scenario and
  stayed green through the full-suite run).
- [x] 2.10 `test_round_trip_replays_generate_title_from_stored_fields_only` in
  `TestBuildTurnTitlePayload` (`test_thumbnail_generation.py`): builds a payload, then calls
  `generate_title(**stored fields)` with `_request_title` mocked — asserts the replay succeeds using
  only stored fields, no `fetch_recent_history` re-call.
- [x] 2.11 Verified via the pre-existing `TestVerifyFinalCopy.test_description_reject_persists_and_does_not_raise`
  (untouched — `_verify_final_copy` was not modified by this slice; its existing
  `mock_db.record_copy_verification_turn.assert_called_once()` assertion already covers this scenario
  and stayed green).
- [x] 2.12 `test_write_key_is_thumbnail_config_output_path_not_child_result_output_path`: injected fake
  `db` captures the positional key argument, asserts it equals
  `thumbnail_config["output_path"]` (`/data/oradores/42/video.mp4`) and explicitly differs from the
  child result's `output_path` (`/data/oradores/42/thumbnail.png`).
- [x] 2.13 `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py
  tests/congress_videos/modules/test_generic_thumbnail_dag.py tests/congress_videos/test_youtube_upload_dag.py`
  → 503 passed. Full-repo `uv run pytest` → 5130 passed, 34 skipped, exit code 0.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and result | `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py tests/congress_videos/modules/test_generic_thumbnail_dag.py tests/congress_videos/test_youtube_upload_dag.py -q --no-cov` → `503 passed` |
| Full-suite command and result | `uv run pytest -q` → `5130 passed, 34 skipped`, exit 0 |
| Runtime harness | N/A for this apply pass — `bash scripts/test-airflow-e2e.sh` deferred to task 3.10 at the end of the 3-slice chain per tasks.md (touches `congress_videos/**`, run once after slice 3 lands) |
| Rollback boundary | `git revert 94f1a27` (or drop the branch pre-merge) fully removes `build_turn_title_payload`, the `_task_thumbnail_result` extension, the `trigger_thumbnail_generation`/`_write_title_provenance` hook, and all new/modified tests; slice 1's columns and write methods remain valid and unused, zero blast radius on slice 1 or the shorts path (slice 3, not yet started) |

### TDD Cycle Evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 2.1/2.2 `build_turn_title_payload` | 7 new tests in `TestBuildTurnTitlePayload` failed with `ImportError` (function did not exist) | Implemented the builder → all 7 pass | — |
| 2.3/2.4 `_task_thumbnail_result` | Updated exact-dict test + 2 new tests failed (`KeyError`/`AssertionError` — old return dict had no `title_generation_input`) | Extended `_task_thumbnail_result` → all pass | — |
| 2.5-2.8/2.12 `trigger_thumbnail_generation` hook | 5 new tests in `TestTriggerThumbnailGenerationTitleProvenance` failed with `AttributeError`/`TypeError` (no `db=` param, no `title_provenance` XCom) | Added the hook inline first → all 5 pass | Extracted `_write_title_provenance()` to fix a ruff C901 complexity violation (12 > 10) on `trigger_thumbnail_generation`; re-ran all 503 focused tests green after the extraction, then `ruff format` fixed one line-length wrap |

### Files Changed

| File | Action | What Was Done |
|---|---|---|
| `congress_videos/modules/thumbnail_generation.py` | Modified | Added `build_turn_title_payload` (+70 lines) |
| `congress_videos/generic_thumbnail_generator_dag.py` | Modified | Extended `_task_thumbnail_result`; added `build_turn_title_payload` import (+21 lines) |
| `congress_videos/youtube_upload_dag.py` | Modified | Added `_write_title_provenance` helper; extended `trigger_thumbnail_generation` with `db=None` param and the provenance-write call site (+64/-2 lines) |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Modified | Added `TestBuildTurnTitlePayload` (7 tests, +176 lines) |
| `tests/congress_videos/modules/test_generic_thumbnail_dag.py` | Modified | Updated 1 exact-dict test, added 2 tests to `TestTaskThumbnailResult`, added `TestTaskGenerateTitleReturnTypeUnchanged` (+68 lines) |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Added `TestTriggerThumbnailGenerationTitleProvenance` (6 tests, +144 lines) |
| `openspec/changes/persist-title-generator-inputs/tasks.md` | Modified | Marked tasks 2.1-2.13 `[x]` |

### Deviations from Design

1. **Tasks 2.9 and 2.11 satisfied by pre-existing tests, not new ones.** Both tasks describe adding a
   test asserting behavior in code this slice does not touch (`_prepare_upload_config`'s empty-title
   guard; `_verify_final_copy`'s `record_copy_verification_turn` call). I verified both scenarios are
   already covered by `TestPrepareUploadConfigTurnRequiresThumbnailTitle` and
   `TestVerifyFinalCopy.test_description_reject_persists_and_does_not_raise` respectively (both ran
   green in the full-suite pass), and did not duplicate that coverage — this also helped keep the diff
   from growing further past budget (see Risks below).
2. **`title_generation_input` is only built when `title` is truthy** (task 2.3 does not state this
   condition explicitly, but Scenario 1.1's GIVEN clause is "a title is generated"). When
   `generate_title`'s XCom is `None` (the pre-existing E-2 fallback path — issue #317), the payload key
   is `None` rather than a payload built with a `None` title. Confirmed non-breaking against the
   pre-existing `test_task_thumbnail_result_title_none` test (still asserts only `result["title"] is None`).
3. **Extracted `_write_title_provenance()`** as a standalone function rather than inlining the hook
   inside `trigger_thumbnail_generation`, to satisfy ruff's C901 complexity gate (issue #272 is an
   active repo-wide effort; introducing a fresh C901 violation would contradict that ongoing work). This
   is a structural choice, not a design deviation — the design's inline pseudocode is preserved
   behaviorally, just factored into a named, independently testable function.
4. No other deviations — implementation otherwise matches design.md D1/D2/D3/C2/C4 and tasks.md.

### Issues Found

None.

### Remaining Tasks (slice 3 — NOT started, untouched)

- [ ] 3.1-3.10 Shorts path: `build_shorts_title_payload` + `_generate_metadata` hook (base: this slice's
  branch, `feat/549-slice2-turn-path`)

### Workload / PR Boundary

- Mode: feature-branch-chain (slice 2 of 3), `auto-chain` delivery strategy
- Current work unit: Unit 2 — "Turn path: `build_turn_title_payload`, `_task_thumbnail_result`, parent
  hook"
- Boundary: starts from `feat/549-slice1-migration-db` (commits `12f3728`, `6c5459b`), ends at commit
  `94f1a27` on `feat/549-slice2-turn-path`. Independently verifiable: `uv run pytest` (contract +
  isolation tests all green); a published turn writes a non-NULL `title_generation_input` on every
  sibling row via the new hook; slice 1's columns/write methods are now exercised for the first time.
- Estimated review budget impact: **541 insertions(+) / 2 deletions(-) = 543 changed lines** (git diff
  vs. slice 1 tip), computed via `git diff --shortstat` before staging (the committed diff including the
  `tasks.md` checkbox updates is 554/15). This is **over the 400-line review budget** and over the
  design's ~360-line forecast for this slice. Breakdown: 155 production lines (21+70+64, close to the
  ~150 forecast) vs. 388 test lines (68+176+144, vs. the ~210 forecast) — the overrun is concentrated
  entirely in test coverage for D2 (optional-key validation), D3 (write-key selection vs. the child's
  reconciled path), C2 (failure isolation for both the exception and zero-row cases), and C4 (loud
  `no_row` semantics), each of which needed its own fake-`db`/`caplog` scenario to be independently
  verifiable per the tasks' own acceptance language. Per the launch instructions, this was implemented
  honestly rather than trimmed to fit the budget (tests/comments must never be deleted to hit a line
  target) — **flagging `size:exception` for orchestrator/reviewer decision** rather than splitting
  further on my own.

### Status (slice 2 only)

13/13 slice-2 tasks complete. `git diff --shortstat` 541(+)/2(-) across 6 production+test files (554/15
including the `tasks.md` checkbox commit) — over the 400-line budget, flagged as `size:exception`. Ready
for verify (slice 2 scope) / ready for slice 3 apply.
