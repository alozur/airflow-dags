"""[RED] Test: migration 025 — create speaker_turn_videos idempotency table.

Reads the SQL file statically and asserts structural properties:
file exists, CREATE TABLE IF NOT EXISTS, required columns, FK to speaker_turns,
UNIQUE constraint on turn_id, and DOWN block drops the table.
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
    / "025_create_speaker_turn_videos.sql"
)


class TestMigration025FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration025TableDefinition:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_creates_table_if_not_exists(self):
        """Migration must use CREATE TABLE IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "CREATE TABLE IF NOT EXISTS SPEAKER_TURN_VIDEOS" in sql

    def test_turn_id_column_integer_not_null(self):
        """turn_id column must be INTEGER NOT NULL."""
        sql = self._sql().upper()
        assert re.search(r"TURN_ID\s+INTEGER\s+NOT\s+NULL", sql), "turn_id must be INTEGER NOT NULL"

    def test_output_path_column_text_not_null(self):
        """output_path column must be TEXT NOT NULL."""
        sql = self._sql().upper()
        assert re.search(r"OUTPUT_PATH\s+TEXT\s+NOT\s+NULL", sql), "output_path must be TEXT NOT NULL"

    def test_materialized_at_column_timestamptz_not_null_default_now(self):
        """materialized_at must be TIMESTAMPTZ NOT NULL DEFAULT NOW()."""
        sql = self._sql().upper()
        assert "MATERIALIZED_AT" in sql
        assert re.search(
            r"MATERIALIZED_AT\s+TIMESTAMPTZ\s+NOT\s+NULL\s+DEFAULT\s+NOW\(\)",
            sql,
        ), "materialized_at must be TIMESTAMPTZ NOT NULL DEFAULT NOW()"

    def test_fk_references_speaker_turns(self):
        """turn_id must reference speaker_turns(turn_id)."""
        sql = self._sql()
        assert "REFERENCES speaker_turns(turn_id)" in sql

    def test_fk_has_on_delete_cascade(self):
        """FK must include ON DELETE CASCADE."""
        sql = self._sql().upper()
        assert "ON DELETE CASCADE" in sql

    def test_unique_constraint_on_turn_id(self):
        """UNIQUE constraint on turn_id must be present."""
        sql = self._sql().upper()
        assert "UNIQUE" in sql
        # Could be inline UNIQUE or a named CONSTRAINT
        assert re.search(
            r"UNIQUE\s*\(\s*TURN_ID\s*\)|TURN_ID.*UNIQUE|UNIQUE.*TURN_ID",
            sql,
        ), "UNIQUE constraint on turn_id must be present"

    def test_no_schema_qualification(self):
        """Migration must use unqualified table names (runner sets search_path)."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.speaker_turn_videos\b", sql)
        assert not re.search(r"\bpublic\.speaker_turns\b", sql)


class TestMigration025DownBlock:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_down_block_present(self):
        """Migration must include a down-migration block."""
        sql = self._sql().upper()
        assert "DROP TABLE" in sql or "-- DOWN" in sql or "DOWN MIGRATION" in sql

    def test_down_drops_speaker_turn_videos(self):
        """DOWN block must reference DROP TABLE for speaker_turn_videos."""
        sql = self._sql().upper()
        # Table name must appear in context of drop
        assert "SPEAKER_TURN_VIDEOS" in sql
        # At minimum a drop comment or statement must exist alongside the table name
        drop_lines = [line for line in sql.splitlines() if "SPEAKER_TURN_VIDEOS" in line and "DROP" in line]
        assert drop_lines, "DOWN block must drop speaker_turn_videos table"
