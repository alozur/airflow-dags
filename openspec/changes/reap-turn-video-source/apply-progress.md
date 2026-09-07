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
