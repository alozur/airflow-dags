"""Tests for migration 030 — add prepared_at column + update uploadable_turns view.

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_029.py pattern).
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "030_add_prepared_at.sql"
)


class TestMigration030FileExists:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration030ColumnAndView:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_prepared_at_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS prepared_at TIMESTAMPTZ."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS PREPARED_AT TIMESTAMPTZ" in sql

    def test_alter_table_speaker_turn_videos(self):
        """Migration must ALTER TABLE speaker_turn_videos."""
        sql = self._sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS" in sql

    def test_creates_uploadable_turns_view(self):
        """Migration must CREATE OR REPLACE VIEW uploadable_turns."""
        sql = self._sql().upper()
        assert "CREATE" in sql and "VIEW UPLOADABLE_TURNS" in sql

    def test_drops_view_before_create(self):
        """Migration must DROP VIEW IF EXISTS uploadable_turns before creating."""
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_view_contains_prepared_at_is_not_null_gate(self):
        """View WHERE clause must contain 'AND prepared_at IS NOT NULL'."""
        sql = self._sql().upper()
        assert re.search(r"AND\s+STv?\.?PREPARED_AT\s+IS\s+NOT\s+NULL", sql) or \
               re.search(r"AND\s+PREPARED_AT\s+IS\s+NOT\s+NULL", sql), (
            "View must contain AND prepared_at IS NOT NULL in WHERE clause"
        )

    def test_view_retains_is_uploaded_gate(self):
        """View WHERE clause must retain is_uploaded_to_youtube = FALSE gate."""
        sql = self._sql().upper()
        assert "IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_interest_score_ordering(self):
        """Outer ORDER BY must still include COALESCE(dedup.interest_score, 1) DESC."""
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC",
            sql,
        ), "Outer ORDER BY must retain COALESCE(dedup.interest_score, 1) DESC"

    def test_no_schema_qualification(self):
        """Migration must not use public.- or production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        """Migration must include a DOWN block comment with rollback instructions."""
        sql = self._sql().upper()
        assert "DOWN" in sql

    def test_view_retains_distinct_on_output_path(self):
        """View must retain DISTINCT ON (stv.output_path) dedup strategy."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql
