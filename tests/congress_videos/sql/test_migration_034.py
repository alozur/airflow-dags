"""Tests for migration 034 — add speaker resolution columns + update uploadable_turns view.

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_032.py pattern).
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "034_add_speaker_resolution.sql"
)


class TestMigration034FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration034Columns:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_resolved_participant_slug_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS resolved_participant_slug TEXT."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS RESOLVED_PARTICIPANT_SLUG TEXT" in sql

    def test_adds_speaker_resolution_confidence_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS speaker_resolution_confidence FLOAT."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS SPEAKER_RESOLUTION_CONFIDENCE FLOAT" in sql

    def test_adds_speaker_resolution_method_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS speaker_resolution_method TEXT."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS SPEAKER_RESOLUTION_METHOD TEXT" in sql

    def test_three_add_column_if_not_exists_statements(self):
        """Migration must have exactly 3 ADD COLUMN IF NOT EXISTS statements."""
        sql = self._sql().upper()
        count = sql.count("ADD COLUMN IF NOT EXISTS")
        assert count == 3, f"Expected 3 ADD COLUMN IF NOT EXISTS, got {count}"

    def test_alter_table_speaker_turn_videos(self):
        """Migration must ALTER TABLE speaker_turn_videos."""
        sql = self._sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS" in sql


class TestMigration034Constraint:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_check_constraint_name_present(self):
        """Migration must reference chk_speaker_resolution_method constraint name."""
        sql = self._sql()
        assert "chk_speaker_resolution_method" in sql

    def test_check_constraint_enum_values(self):
        """CHECK constraint must include all three valid method values."""
        sql = self._sql()
        assert "ai_srt_context" in sql
        assert "fuzzy" in sql
        assert "manual" in sql

    def test_constraint_guarded_by_do_block(self):
        """Constraint addition must be guarded by a DO $$ BEGIN ... END $$ block for idempotency."""
        sql = self._sql().upper()
        assert "DO $$" in sql or "DO $" in sql or "DO\n$$" in sql or "DO\n$" in sql or "DO " in sql
        # Must check pg_constraint for existence
        assert "PG_CONSTRAINT" in sql

    def test_constraint_guards_if_not_exists(self):
        """The DO block must use IF NOT EXISTS to guard the constraint addition."""
        sql = self._sql().upper()
        assert "IF NOT EXISTS" in sql


class TestMigration034UploadableTurnsView:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_drops_view_before_create(self):
        """Migration must DROP VIEW IF EXISTS uploadable_turns before recreating."""
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_creates_uploadable_turns_view(self):
        """Migration must CREATE VIEW uploadable_turns."""
        sql = self._sql().upper()
        assert "CREATE" in sql and "VIEW UPLOADABLE_TURNS" in sql

    def test_view_retains_distinct_on_output_path(self):
        """View must retain DISTINCT ON (stv.output_path) dedup strategy."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql

    def test_view_retains_prepared_at_gate(self):
        """View WHERE clause must retain prepared_at IS NOT NULL gate from migration 030."""
        sql = self._sql().upper()
        assert re.search(
            r"AND\s+STV\.PREPARED_AT\s+IS\s+NOT\s+NULL|AND\s+PREPARED_AT\s+IS\s+NOT\s+NULL",
            sql,
        ), "View must retain AND stv.prepared_at IS NOT NULL gate"

    def test_view_retains_is_upload_abandoned_gate(self):
        """View WHERE clause must retain NOT stv.is_upload_abandoned gate (migration 032)."""
        sql = self._sql().upper()
        assert re.search(
            r"NOT\s+STV\.IS_UPLOAD_ABANDONED",
            sql,
        ), "View must contain NOT stv.is_upload_abandoned in WHERE clause"

    def test_view_retains_interest_score_ordering(self):
        """Outer ORDER BY must retain COALESCE(dedup.interest_score, 1) DESC."""
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC",
            sql,
        ), "Outer ORDER BY must retain COALESCE(dedup.interest_score, 1) DESC"

    def test_view_includes_resolved_participant_slug(self):
        """View SELECT must include stv.resolved_participant_slug."""
        sql = self._sql().upper()
        assert "STV.RESOLVED_PARTICIPANT_SLUG" in sql

    def test_view_includes_speaker_resolution_confidence(self):
        """View SELECT must include stv.speaker_resolution_confidence."""
        sql = self._sql().upper()
        assert "STV.SPEAKER_RESOLUTION_CONFIDENCE" in sql

    def test_view_includes_speaker_resolution_method(self):
        """View SELECT must include stv.speaker_resolution_method."""
        sql = self._sql().upper()
        assert "STV.SPEAKER_RESOLUTION_METHOD" in sql


class TestMigration034Hygiene:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_no_schema_qualification(self):
        """Migration must not use public.- or production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        """Migration must include a DOWN block comment with rollback instructions."""
        sql = self._sql().upper()
        assert "-- DOWN" in sql

    def test_idempotent_add_column(self):
        """All ADD COLUMN statements must use IF NOT EXISTS."""
        sql = self._sql().upper()
        add_col_count = sql.count("ADD COLUMN")
        add_col_safe_count = sql.count("ADD COLUMN IF NOT EXISTS")
        assert add_col_count > 0, "Must have at least one ADD COLUMN"
        assert add_col_count == add_col_safe_count, "All ADD COLUMN statements must use IF NOT EXISTS for idempotency"
