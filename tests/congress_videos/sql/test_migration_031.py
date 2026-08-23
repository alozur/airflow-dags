"""Tests for migration 031 — add turns_detected_at column to video_chapters.

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_030.py pattern).
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
    / "031_add_turns_detected_at.sql"
)


class TestMigration031:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_adds_turns_detected_at_column(self):
        """Migration must ADD COLUMN IF NOT EXISTS turns_detected_at."""
        sql = self._sql().upper()
        assert "TURNS_DETECTED_AT" in sql

    def test_column_type_is_timestamptz(self):
        """Column must be of type TIMESTAMPTZ."""
        sql = self._sql().upper()
        assert "TIMESTAMPTZ" in sql

    def test_targets_video_chapters_table(self):
        """Migration must alter the video_chapters table."""
        sql = self._sql().upper()
        assert "VIDEO_CHAPTERS" in sql

    def test_uses_add_column_if_not_exists(self):
        """Migration must use ADD COLUMN IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS" in sql

    def test_has_no_unqualified_schema_prefix(self):
        """Migration must not use public.- or production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_does_not_recreate_uploadable_chapters_view(self):
        """Migration must NOT recreate or alter the uploadable_chapters view."""
        sql = self._sql().upper()
        assert "UPLOADABLE_CHAPTERS" not in sql

    def test_has_down_block_comment(self):
        """Migration must include a DOWN block comment with DROP COLUMN rollback."""
        sql = self._sql().upper()
        assert "DOWN" in sql
        assert "DROP COLUMN IF EXISTS" in sql
