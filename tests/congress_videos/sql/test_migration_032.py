"""Tests for migration 032 — add upload verification columns + update uploadable_turns view.

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_030.py pattern).
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "032_add_upload_verification.sql"
)


class TestMigration032FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration032VideoChaptersColumn:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_upload_verified_at_to_video_chapters(self):
        """Migration must ADD COLUMN IF NOT EXISTS upload_verified_at TIMESTAMPTZ to video_chapters."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS UPLOAD_VERIFIED_AT TIMESTAMPTZ" in sql

    def test_alter_table_video_chapters(self):
        """Migration must ALTER TABLE video_chapters."""
        sql = self._sql().upper()
        assert "ALTER TABLE VIDEO_CHAPTERS" in sql


class TestMigration032SpeakerTurnVideosColumns:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_alter_table_speaker_turn_videos(self):
        """Migration must ALTER TABLE speaker_turn_videos."""
        sql = self._sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS" in sql

    def test_adds_upload_attempts_column(self):
        """Migration must add upload_attempts INTEGER DEFAULT 0 to speaker_turn_videos."""
        sql = self._sql().upper()
        assert "UPLOAD_ATTEMPTS INTEGER DEFAULT 0" in sql

    def test_adds_is_upload_abandoned_column(self):
        """Migration must add is_upload_abandoned BOOLEAN DEFAULT FALSE to speaker_turn_videos."""
        sql = self._sql().upper()
        assert "IS_UPLOAD_ABANDONED BOOLEAN DEFAULT FALSE" in sql

    def test_adds_last_upload_error_column(self):
        """Migration must add last_upload_error TEXT to speaker_turn_videos."""
        sql = self._sql().upper()
        assert "LAST_UPLOAD_ERROR TEXT" in sql

    def test_adds_upload_verified_at_to_speaker_turn_videos(self):
        """Migration must add upload_verified_at TIMESTAMPTZ to speaker_turn_videos."""
        # The migration adds this column to BOTH tables
        sql = self._sql().upper()
        assert sql.count("ADD COLUMN IF NOT EXISTS UPLOAD_VERIFIED_AT TIMESTAMPTZ") >= 2


class TestMigration032UploadableTurnsView:
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

    def test_view_contains_is_upload_abandoned_gate(self):
        """View WHERE clause must contain NOT stv.is_upload_abandoned gate."""
        sql = self._sql().upper()
        assert re.search(
            r"NOT\s+STV\.IS_UPLOAD_ABANDONED",
            sql,
        ), "View must contain NOT stv.is_upload_abandoned in WHERE clause"

    def test_view_retains_is_uploaded_gate(self):
        """View WHERE clause must retain is_uploaded_to_youtube = FALSE gate."""
        sql = self._sql().upper()
        assert "IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_prepared_at_gate(self):
        """View WHERE clause must retain prepared_at IS NOT NULL gate from migration 030."""
        sql = self._sql().upper()
        assert re.search(
            r"AND\s+STV\.PREPARED_AT\s+IS\s+NOT\s+NULL|AND\s+PREPARED_AT\s+IS\s+NOT\s+NULL",
            sql,
        ), "View must retain AND stv.prepared_at IS NOT NULL gate"

    def test_view_retains_distinct_on_output_path(self):
        """View must retain DISTINCT ON (stv.output_path) dedup strategy."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql

    def test_view_retains_interest_score_ordering(self):
        """Outer ORDER BY must retain COALESCE(dedup.interest_score, 1) DESC."""
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC",
            sql,
        ), "Outer ORDER BY must retain COALESCE(dedup.interest_score, 1) DESC"


class TestMigration032Hygiene:
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
        # Count ADD COLUMN occurrences and ensure each uses IF NOT EXISTS
        add_col_count = sql.count("ADD COLUMN")
        add_col_safe_count = sql.count("ADD COLUMN IF NOT EXISTS")
        assert add_col_count > 0, "Must have at least one ADD COLUMN"
        assert add_col_count == add_col_safe_count, "All ADD COLUMN statements must use IF NOT EXISTS for idempotency"
