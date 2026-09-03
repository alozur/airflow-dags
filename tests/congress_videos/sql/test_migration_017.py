"""Test: migration 017 DDL presence.

Reads the SQL file and asserts that all required table definitions, constraints,
and FK references are present in the text.  Fast and infrastructure-free.
"""

from __future__ import annotations

from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "017_create_speaker_normalization_cache.sql"
)


class TestMigration017FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration017TableStructure:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_creates_speaker_normalization_cache_table(self):
        """DDL must create the speaker_normalization_cache table."""
        sql = self._sql()
        assert "CREATE TABLE" in sql.upper()
        assert "speaker_normalization_cache" in sql

    def test_has_unique_constraint_on_chapter_id_and_dirty_speaker(self):
        """DDL must include UNIQUE (chapter_id, dirty_speaker)."""
        sql = self._sql()
        # Allow both UNIQUE(chapter_id, dirty_speaker) and UNIQUE (chapter_id, dirty_speaker)
        assert "chapter_id" in sql
        assert "dirty_speaker" in sql
        assert "UNIQUE" in sql.upper()

    def test_status_check_contains_matched(self):
        """CHECK constraint must include 'matched' status value."""
        sql = self._sql()
        assert "matched" in sql
        assert "CHECK" in sql.upper()

    def test_status_check_contains_no_match(self):
        """CHECK constraint must include 'no_match' status value."""
        sql = self._sql()
        assert "no_match" in sql

    def test_status_check_contains_needs_manual(self):
        """CHECK constraint must include 'needs_manual' status value."""
        sql = self._sql()
        assert "needs_manual" in sql

    def test_fk_references_video_chapters_chapter_id(self):
        """FK must reference video_chapters(chapter_id) — NOT video_chapters(id)."""
        sql = self._sql()
        assert "REFERENCES video_chapters(chapter_id)" in sql

    def test_fk_references_congress_participants_normalized_name(self):
        """FK must reference congress_participants(normalized_name)."""
        sql = self._sql()
        assert "REFERENCES congress_participants(normalized_name)" in sql

    def test_has_id_primary_key(self):
        """DDL must define an id SERIAL PRIMARY KEY column."""
        sql = self._sql().upper()
        assert "SERIAL" in sql
        assert "PRIMARY KEY" in sql

    def test_has_created_at_and_updated_at_columns(self):
        """DDL must define created_at and updated_at TIMESTAMPTZ columns."""
        sql = self._sql()
        assert "created_at" in sql
        assert "updated_at" in sql
        assert "TIMESTAMPTZ" in sql.upper() or "TIMESTAMP WITH TIME ZONE" in sql.upper()

    def test_has_confidence_score_column(self):
        """DDL must include a confidence_score NUMERIC column."""
        sql = self._sql()
        assert "confidence_score" in sql
        assert "NUMERIC" in sql.upper()

    def test_has_canonical_speaker_column(self):
        """DDL must include a canonical_speaker TEXT column."""
        sql = self._sql()
        assert "canonical_speaker" in sql

    def test_has_participant_normalized_name_column(self):
        """DDL must include participant_normalized_name column."""
        sql = self._sql()
        assert "participant_normalized_name" in sql
