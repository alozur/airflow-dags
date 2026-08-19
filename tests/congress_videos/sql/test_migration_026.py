"""[RED] Test: migration 026 — create video_analytics_snapshots table.

Reads the SQL file statically and asserts structural properties:
file exists, CREATE TABLE IF NOT EXISTS, required columns, FK to video_chapters,
UNIQUE constraint on (youtube_video_id, checkpoint), JSONB metrics column,
nullable action_taken column, and DOWN block.
No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "026_create_video_analytics_snapshots.sql"
)


class TestMigration026FileExists:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration026TableDefinition:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_creates_table_if_not_exists(self):
        """Migration must use CREATE TABLE IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "CREATE TABLE IF NOT EXISTS VIDEO_ANALYTICS_SNAPSHOTS" in sql

    def test_chapter_id_column_fk_references_video_chapters(self):
        """chapter_id must reference video_chapters(chapter_id)."""
        sql = self._sql()
        assert "REFERENCES video_chapters(chapter_id)" in sql

    def test_fk_has_on_delete_cascade(self):
        """FK must include ON DELETE CASCADE."""
        sql = self._sql().upper()
        assert "ON DELETE CASCADE" in sql

    def test_youtube_video_id_column_not_null(self):
        """youtube_video_id must be NOT NULL."""
        sql = self._sql().upper()
        assert "YOUTUBE_VIDEO_ID" in sql
        assert re.search(r"YOUTUBE_VIDEO_ID\s+VARCHAR\S*\s+NOT\s+NULL", sql) or \
               re.search(r"YOUTUBE_VIDEO_ID\s+TEXT\s+NOT\s+NULL", sql), (
            "youtube_video_id must be NOT NULL"
        )

    def test_checkpoint_column_text_not_null(self):
        """checkpoint column must be TEXT NOT NULL."""
        sql = self._sql().upper()
        assert re.search(r"CHECKPOINT\s+TEXT\s+NOT\s+NULL", sql), (
            "checkpoint must be TEXT NOT NULL"
        )

    def test_metrics_column_jsonb_not_null(self):
        """metrics column must be JSONB NOT NULL."""
        sql = self._sql().upper()
        assert re.search(r"METRICS\s+JSONB\s+NOT\s+NULL", sql), (
            "metrics must be JSONB NOT NULL"
        )

    def test_action_taken_column_text_nullable(self):
        """action_taken column must exist as TEXT with no NOT NULL constraint (nullable)."""
        sql = self._sql().upper()
        assert "ACTION_TAKEN" in sql
        # action_taken must NOT be followed by NOT NULL
        lines = [line for line in sql.splitlines() if "ACTION_TAKEN" in line]
        assert lines, "action_taken column must be present"
        for line in lines:
            assert "NOT NULL" not in line, (
                f"action_taken must be nullable (no NOT NULL), got: {line}"
            )

    def test_collected_at_column_timestamptz_not_null_default_now(self):
        """collected_at must be TIMESTAMPTZ NOT NULL DEFAULT NOW()."""
        sql = self._sql().upper()
        assert re.search(
            r"COLLECTED_AT\s+TIMESTAMPTZ\s+NOT\s+NULL\s+DEFAULT\s+NOW\(\)",
            sql,
        ), "collected_at must be TIMESTAMPTZ NOT NULL DEFAULT NOW()"

    def test_unique_constraint_on_youtube_video_id_and_checkpoint(self):
        """UNIQUE constraint on (youtube_video_id, checkpoint) must be present."""
        sql = self._sql().upper()
        # Named CONSTRAINT ... UNIQUE (youtube_video_id, checkpoint)
        assert re.search(
            r"UNIQUE\s*\(\s*YOUTUBE_VIDEO_ID\s*,\s*CHECKPOINT\s*\)",
            sql,
        ), "UNIQUE (youtube_video_id, checkpoint) constraint must be present"

    def test_index_on_chapter_id_is_idempotent(self):
        """Index on chapter_id must use CREATE INDEX IF NOT EXISTS."""
        sql = self._sql().upper()
        assert "CREATE INDEX IF NOT EXISTS" in sql
        assert "VIDEO_ANALYTICS" in sql  # index references this table

    def test_no_schema_qualification(self):
        """Migration must use unqualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.video_analytics_snapshots\b", sql)
        assert not re.search(r"\bpublic\.video_chapters\b", sql)


class TestMigration026DownBlock:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_down_block_references_video_analytics_snapshots(self):
        """DOWN block must reference video_analytics_snapshots for rollback."""
        sql = self._sql().upper()
        drop_lines = [
            line for line in sql.splitlines()
            if "VIDEO_ANALYTICS_SNAPSHOTS" in line and "DROP" in line
        ]
        assert drop_lines, "DOWN block must drop video_analytics_snapshots table"
