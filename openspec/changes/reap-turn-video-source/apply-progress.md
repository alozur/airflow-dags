# Apply Progress: reap-turn-video-source

## Batch 1 (PR1, `feat/467-a-turn-id-migration`)

**Status**: Phase 1 complete. Ready for `sdd-verify` on this work unit / next PR (Phase 2)
in a follow-up `sdd-apply` batch.

### What landed

- `congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql` (new): adds
  nullable `video_shorts.turn_id INTEGER REFERENCES speaker_turn_videos(turn_id)
  ON DELETE SET NULL`, `idx_video_shorts_turn_id` index, and a column comment.
  DOWN block fully commented per the 046 convention (the migration runner executes
  the whole file in one transaction with no automatic rollback).
- `congress_videos/sql/production_schema.sql`: appended the `turn_id` column to
  the `video_shorts` table block (after `last_upload_error`) and the matching
  `CREATE INDEX idx_video_shorts_turn_id` statement to the INDEXES section.
- `tests/congress_videos/sql/test_production_schema.py`:
  - Added `"turn_id"` to `VIDEO_SHORTS_COLUMNS` (20 → 21) and updated the class
    docstring column count.
  - Added `TestVideoShortsTableSnapshot.test_turn_id_fk_is_production_qualified`
    asserting `REFERENCES PRODUCTION.SPEAKER_TURN_VIDEOS(TURN_ID) ON DELETE SET NULL`.
  - Added `TestVideoShortsIndexCompleteness` (new class, mirrors
    `TestVideoChaptersIndexCompleteness`) asserting the `idx_video_shorts_turn_id`
    `CREATE INDEX` statement is present in the snapshot.

### TDD evidence

- RED: `uv run pytest tests/congress_videos/sql/test_production_schema.py -o addopts=`
  → 3 failed (`test_column_present_in_block[turn_id]`,
  `test_turn_id_fk_is_production_qualified`,
  `TestVideoShortsIndexCompleteness::test_index_statement_present`), 215 passed.
- GREEN: same scoped command → 218 passed, 0 failed.
- Full suite: `uv run pytest -n auto` → 4583 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB, as expected in this environment), 0 failed.
  `--cov-fail-under=80` is enforced by `pyproject.toml` addopts and the run passed.
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 301 files already formatted.

### Changed lines

`git diff --cached --stat` (3 files, PR1 scope only):

```
congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql | 28 ++++++++++++++++++
congress_videos/sql/production_schema.sql                       |  8 +++++-
tests/congress_videos/sql/test_production_schema.py             | 31 ++++++++++++++++++++--
3 files changed, 64 insertions(+), 3 deletions(-)
```

Total: 64 additions + 3 deletions = 67 changed lines (budget: 400).

### Deviations from design

None. Migration text, column placement, index name, and test additions match
`design.md` §"Interfaces / Contracts" → "2. Migration `047_add_video_shorts_turn_id.sql`"
verbatim, including the FK's `ON DELETE SET NULL` semantics and the `chapter_rank`-style
comment conventions used elsewhere in the file.

### Manual DOWN-block verification

Confirmed every line of the `-- DOWN` section in `047_add_video_shorts_turn_id.sql`
starts with `--` (checked with `bat -A`), matching the 044/046 convention that the
migration runner (`utils/migrations_dag.py`) executes the whole file in one
transaction with no rollback support.

### Not in scope for this batch

Phases 2–5 (`get_turn_videos_for_shorts`, preparer rewrite, processor/sidecar wiring,
Tier-1 partition, and post-merge ops) are untouched — this batch is PR1 only, per the
orchestrator's work-unit scope.

## Batch 2 (PR2, `feat/467-b-turn-selection`, base `feat/467-a-turn-id-migration` @ `a2ab9fd`)

**Status**: Phase 2 complete. Ready for `sdd-verify` on this work unit / next PR
(Phase 3) in a follow-up `sdd-apply` batch.

**Commit**: `32412e5` — `feat(reap): source turn-video candidate selection from speaker turns`

### What landed

- `congress_videos/modules/database.py`:
  - Deleted `get_chapters_for_shorts` (chapter-only Reap candidate selection,
    the `is_uploaded_to_youtube = TRUE` publish gate, `min_relevance_score`
    threshold, and the interval-arithmetic duration check).
  - Added `get_turn_videos_for_shorts(max_turns: int | None = None) -> list[dict]`
    per design §1: unfiltered `group_spans` CTE (`MIN`/`MAX(start/end_seconds)`
    + summed `procedural_seconds`, no `WHERE` inside the CTE — issue #151 trap),
    `DISTINCT ON (stv.output_path)` representative row, dedup via
    `NOT EXISTS (... vs.turn_id = stv.turn_id)` (turn-keyed, not chapter-keyed),
    `NOT COALESCE(st.is_procedural, FALSE)` exclusion, `group_duration_seconds
    >= 120` floor with no upper ceiling, `LIMIT %s` appended only when
    `max_turns is not None`, and ordering by `COALESCE(interest_score, 1) DESC,
    relevance_score DESC, session_date DESC, turn_id ASC`. No `prepared_at`,
    relevance threshold, or parent-upload-date gate — confirmed absent from the
    emitted SQL by dedicated tests.
  - Extended `insert_video_short` with a trailing optional `turn_id: int | None
    = None` keyword parameter (backward compatible for existing positional/
    keyword callers) while inserting it as the SQL column right after
    `chapter_id`: `(chapter_id, turn_id, reap_project_id, reap_status,
    pretrim_start_secs, pretrim_end_secs, pretrim_used_srt, staged_clip_path,
    scoring_reasoning)` — 9 placeholders, `params[1]` is `turn_id`.
- `tests/congress_videos/modules/test_reap_db_methods.py`:
  - Replaced `TestGetChaptersForShorts` (4 tests) with `TestGetTurnVideosForShorts`
    (14 tests) covering: empty/non-empty results, `LIMIT` presence/absence tied
    to `max_turns`, `group_spans` CTE presence, the CTE being unfiltered by
    `is_procedural` (issue #151 regression guard), `DISTINCT ON` usage, dedup
    keying on `turn_id` (and NOT `chapter_id`), procedural exclusion, the 120s
    floor, absence of any upper ceiling (`<=` anywhere in the query), absence
    of `prepared_at`/`youtube_upload_date`/`is_uploaded_to_youtube` gates, and
    the exact editorial ordering keys.
  - Extended `TestInsertVideoShort` with 5 new tests: 9-placeholder count,
    column-list ordering (`chapter_id, turn_id, reap_project_id`), `turn_id`
    defaulting to `None`, `turn_id` being passed through when given, and a
    regression test that the pre-existing positional-call shape (no `turn_id`)
    still returns the inserted id.
- `tests/congress_videos/modules/test_database_surface.py`: moved
  `get_chapters_for_shorts` from `LIVE_METHOD_NAMES` to `DEAD_METHOD_NAMES`
  (with an issue #467 comment) and added `get_turn_videos_for_shorts` to
  `LIVE_METHOD_NAMES`.
- `openspec/changes/reap-turn-video-source/tasks.md`: marked tasks 2.1–2.6 `[x]`.

### TDD evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 2.1 `get_turn_videos_for_shorts` SQL-text tests | 14 new tests fail with `AttributeError: get_turn_videos_for_shorts` | method implemented per design §1; all 14 pass | ruff clean |
| 2.2 `insert_video_short(turn_id=)` | 3 new tests fail with `TypeError: unexpected keyword argument 'turn_id'` | signature + column list extended; all pass | ruff clean |
| 2.3 surface guard swap | `test_dead_method_absent[get_chapters_for_shorts]` and `test_live_method_present[get_turn_videos_for_shorts]` fail | swap applied; both pass | ruff clean |

- RED: `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/modules/test_database_surface.py -o addopts=`
  → 19 failed, 84 passed (exact failures: 14 `TestGetTurnVideosForShorts` tests,
  3 `TestInsertVideoShort` turn_id tests, 2 surface-guard parametrized cases).
- GREEN: same scoped command → 103 passed, 0 failed.
- Full suite: `uv run pytest -n auto` → 4599 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB), 0 failed. `--cov-fail-under=80` enforced by
  `pyproject.toml` addopts and the run passed.
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 301 files already formatted.
- DagBag import check: `uv run python -c "from airflow.models import DagBag; ..."`
  → `16 {}` (16 DAGs, zero import errors — `reap_clip_preparer_dag.py` still calls
  the now-removed `get_chapters_for_shorts` inside task-function bodies only,
  which DagBag import never executes; see "Deviations" below).

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/modules/test_database_surface.py -o addopts=` → 103 passed |
| Runtime harness command/scenario and exact result | N/A — MagicMock cursor, no live DB (per tasks.md work-unit table); DagBag import check run instead as the closest applicable runtime boundary → `16 {}` |
| Rollback boundary | Revert commit `32412e5`; the unit is inert without a caller until PR3 rewrites the preparer, so reverting removes no other work |

### Changed lines

`git diff --stat a2ab9fd..32412e5` (4 files, PR2 scope):

```
congress_videos/modules/database.py                | 108 ++++++++----
openspec/changes/reap-turn-video-source/tasks.md    |  12 +-
tests/congress_videos/modules/test_database_surface.py |   5 +-
tests/congress_videos/modules/test_reap_db_methods.py  | 186 +++++++++++++++++++--
4 files changed, 255 insertions(+), 56 deletions(-)
```

Total: 255 additions + 56 deletions = 311 changed lines (budget: 400).

### Deviations from design

None. `get_turn_videos_for_shorts` SQL, `insert_video_short` column order/
placeholder count, and the surface guard swap match `design.md` §1, §3, and §8
verbatim.

One scope clarification, not a deviation: `congress_videos/reap_clip_preparer_dag.py`
still calls `db.get_chapters_for_shorts(...)` inside `_query_chapters` and
`_extract_and_pretrim_clip` (task functions, not module-level code), and its test
file still mocks that method name via plain `mocker.patch(...)` (no `autospec`).
Design explicitly defers the preparer rewrite to PR3 ("Phase 3: Preparer rewrite")
and does not prescribe a stub — the DAG module still imports cleanly (confirmed:
DagBag reports 0 import errors) because Python does not validate attribute
existence until the attribute is actually accessed at task-execution time, which
DagBag import never triggers. This means the `congress_reap_clip_preparer` DAG
would raise `AttributeError` if actually **run** between PR2 landing and PR3
merging — acceptable per the design's explicit "order is load-bearing" rollout
note, since PRs 1–3 are meant to merge as a stack before any deploy, not
individually deployed.

### Issues Found

None.

### Remaining Tasks

- [ ] Phase 3: Preparer rewrite (`reap_clip_preparer_dag.py` — `output_path` staging,
  leading pre-trim, dead-code removal, `max_chapters`→`max_turns`)
- [ ] Phase 4a: `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` +
  processor/sidecar wiring
- [ ] Phase 4b: `pending_shorts_candidate_sql` partition + parent-gate drop
- [ ] Phase 5: Orchestrator-run ops (migration 047 on NAS dev+prod, git_sync,
  validation query, manual trigger)

### Workload / PR Boundary

- Mode: stacked PR slice (auto-chain, `stacked-to-main`)
- Current work unit: PR2 — `get_turn_videos_for_shorts` + `insert_video_short(turn_id=)`
  + surface swap, branch `feat/467-b-turn-selection`, base `feat/467-a-turn-id-migration`
- Boundary: starts from PR1's `a2ab9fd` (migration 047 + schema snapshot), ends at
  commit `32412e5` — turn-video candidate selection is implemented and unit-inert
  until PR3 wires a caller
- Estimated review budget impact: 311 changed lines, within the 400-line budget

### Not in scope for this batch

Phases 3–5 (preparer rewrite, processor/sidecar wiring, Tier-1 partition, and
post-merge ops) are untouched — this batch is PR2 only, per the orchestrator's
work-unit scope. The orchestrator settles the native attempt ledger; this batch
does not call `sdd-attempt settle`.
