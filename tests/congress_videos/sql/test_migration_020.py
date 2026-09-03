"""[RED] Test: migration 020 — add resolved_participant_slug to video_chapters.

Reads the SQL file statically and asserts structural properties:
file exists, column added idempotently, FK references congress_participants(slug),
index created, backfill is idempotent (WHERE slug IS NULL), unqualified table names.
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
    / "020_add_resolved_participant_slug.sql"
)


class TestMigration020FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration020ColumnDefinition:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_column_if_not_exists(self):
        """Migration must use ADD COLUMN IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS" in sql
        assert "RESOLVED_PARTICIPANT_SLUG" in sql

    def test_column_is_text(self):
        """resolved_participant_slug column must be TEXT type."""
        sql = self._sql().upper()
        # ADD COLUMN IF NOT EXISTS resolved_participant_slug TEXT
        assert re.search(r"RESOLVED_PARTICIPANT_SLUG\s+TEXT", sql)

    def test_column_references_congress_participants_slug(self):
        """Column must have FK REFERENCES congress_participants(slug)."""
        sql = self._sql()
        assert "REFERENCES congress_participants(slug)" in sql

    def test_creates_index_if_not_exists(self):
        """Migration must create an index with CREATE INDEX IF NOT EXISTS."""
        sql = self._sql().upper()
        assert "CREATE INDEX IF NOT EXISTS" in sql
        assert "RESOLVED_PARTICIPANT_SLUG" in sql


class TestMigration020Backfill:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_backfill_updates_video_chapters(self):
        """Backfill must UPDATE video_chapters."""
        sql = self._sql().upper()
        assert "UPDATE VIDEO_CHAPTERS" in sql

    def test_backfill_joins_speaker_normalization_cache(self):
        """Backfill must JOIN speaker_normalization_cache."""
        sql = self._sql().upper()
        assert "SPEAKER_NORMALIZATION_CACHE" in sql

    def test_backfill_joins_congress_participants(self):
        """Backfill must JOIN congress_participants to get authoritative slug."""
        sql = self._sql().upper()
        assert "CONGRESS_PARTICIPANTS" in sql

    def test_backfill_filters_matched_status(self):
        """Backfill must only apply to cache rows with status='matched'."""
        sql = self._sql()
        assert "'matched'" in sql or "= 'matched'" in sql

    def test_backfill_is_idempotent_null_guard(self):
        """Backfill WHERE clause must guard with IS NULL to avoid overwriting filled rows."""
        sql = self._sql().upper()
        assert "IS NULL" in sql
        assert "RESOLVED_PARTICIPANT_SLUG IS NULL" in sql

    def test_backfill_sets_cp_slug(self):
        """Backfill must set resolved_participant_slug to cp.slug (authoritative FK source)."""
        sql = self._sql()
        # Accepts either 'cp.slug' or aliased equivalent
        assert "cp.slug" in sql or "congress_participants.slug" in sql


class TestMigration020TableNames:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_no_schema_qualified_table_names(self):
        """Migration must use unqualified table names (runner sets search_path)."""
        content = self._sql()
        assert not re.search(r"\bpublic\.video_chapters\b", content)
        assert not re.search(r"\bpublic\.congress_participants\b", content)
        assert not re.search(r"\bpublic\.speaker_normalization_cache\b", content)
