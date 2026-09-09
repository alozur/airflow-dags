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

### Remaining Tasks (slices 2 and 3 — NOT started, untouched)

- [ ] 2.1-2.13 Turn path: `build_turn_title_payload`, `_task_thumbnail_result`,
  `trigger_thumbnail_generation` hook (base: this slice's branch)
- [ ] 3.1-3.10 Shorts path: `build_shorts_title_payload` + `_generate_metadata` hook (base: slice 2
  branch)

### Workload / PR Boundary

- Mode: feature-branch-chain (slice 1 of 3), `auto-chain` delivery strategy
- Current work unit: Unit 1 — "Migration 051 + schema snapshot mirror + `database.py` write methods"
- Boundary: starts from `feat/549-generator-input-payloads` (planning commit `0d9d11c` only), ends at
  commit `12f3728` on `feat/549-slice1-migration-db`. Independently verifiable: `uv run pytest`
  (unit + drift test all green); the two columns exist in the migration and the schema snapshot; both
  write methods are callable and unit-tested in isolation, with zero callers wired yet.
- Estimated review budget impact: 293 insertions + 14 deletions = 307 changed lines (git diff stat),
  under the 400-line budget and close to the design's ~305-line forecast for this slice.

### Status

9/9 slice-1 tasks complete. Ready for verify (slice 1 scope only) / ready for slice 2 apply.
