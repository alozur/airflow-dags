"""[RED] Test: migration 022 — create speaker_turns table.

Reads the SQL file statically and asserts structural properties:
file exists, table creation is idempotent (IF NOT EXISTS), FK references
video_chapters(chapter_id) with ON DELETE CASCADE, UNIQUE constraint, CHECK
constraints for confidence and source, both required indexes, and correct
cascade behaviour. No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "022_create_speaker_turns.sql"
)


class TestMigration022FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration022TableDefinition:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_creates_table_if_not_exists(self):
        """Migration must use CREATE TABLE IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "CREATE TABLE IF NOT EXISTS SPEAKER_TURNS" in sql

    def test_fk_references_video_chapters(self):
        """chapter_id column must reference video_chapters(chapter_id)."""
        sql = self._sql()
        assert "REFERENCES video_chapters(chapter_id)" in sql

    def test_fk_has_on_delete_cascade(self):
        """FK must include ON DELETE CASCADE."""
        sql = self._sql().upper()
        assert "ON DELETE CASCADE" in sql

    def test_unique_constraint_chapter_start(self):
        """UNIQUE (chapter_id, start_seconds) constraint must be present."""
        sql = self._sql().upper()
        assert "UNIQUE" in sql
        assert "CHAPTER_ID" in sql
        assert "START_SECONDS" in sql
        # Check that UNIQUE appears with both columns in proximity
        assert re.search(r"UNIQUE\s*\(\s*CHAPTER_ID\s*,\s*START_SECONDS\s*\)", sql)

    def test_confidence_check_constraint(self):
        """confidence column must have CHECK (confidence >= 0 AND confidence <= 1)."""
        sql = self._sql()
        assert "CHECK" in sql.upper()
        assert "confidence >= 0" in sql or "confidence>=0" in sql
        assert "confidence <= 1" in sql or "confidence<=1" in sql

    def test_source_check_constraint(self):
        """source column must have CHECK (source IN ('acoustic','text_confirmed','text_named'))."""
        sql = self._sql()
        assert "'acoustic'" in sql
        assert "'text_confirmed'" in sql
        assert "'text_named'" in sql
        assert re.search(r"source\s+IN\s*\(", sql) or re.search(r"CHECK\s*\(.*source.*IN\s*\(", sql, re.DOTALL)

    def test_index_speaker_turns_chapter(self):
        """idx_speaker_turns_chapter index must be created."""
        sql = self._sql()
        assert "idx_speaker_turns_chapter" in sql

    def test_index_speaker_turns_name(self):
        """idx_speaker_turns_name index on resolved_name must be created."""
        sql = self._sql()
        assert "idx_speaker_turns_name" in sql

    def test_indexes_are_conditional(self):
        """Both indexes must use CREATE INDEX IF NOT EXISTS."""
        sql = self._sql().upper()
        assert sql.count("CREATE INDEX IF NOT EXISTS") >= 2
