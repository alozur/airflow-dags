"""A1 [RED] Test: migration 013 column presence.

Tests that the migration SQL file contains the correct column definition.
We parse the SQL directly rather than spinning up a real DB, which keeps
the test fast and infrastructure-free while still verifying the intended
schema change.
"""

from __future__ import annotations

from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "congress_videos"
    / "sql"
    / "migrations"
    / "013_add_source_integrity_tracking.sql"
)


class TestMigration013ColumnPresence:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_migration_adds_download_retry_after_column(self):
        """Migration must add download_retry_after TIMESTAMP DEFAULT NULL."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8")
        # The ALTER TABLE line must include the column name with TIMESTAMP type
        assert "download_retry_after" in sql, "Migration must define the download_retry_after column"
        assert "TIMESTAMP" in sql.upper(), "download_retry_after must be of type TIMESTAMP"

    def test_migration_uses_add_column_if_not_exists(self):
        """Migration must be idempotent (ADD COLUMN IF NOT EXISTS)."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "ADD COLUMN IF NOT EXISTS" in sql, "Migration must use ADD COLUMN IF NOT EXISTS for idempotency"

    def test_migration_targets_youtube_source_videos(self):
        """Migration must target the youtube_source_videos table."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8")
        assert "youtube_source_videos" in sql, "Migration must ALTER TABLE youtube_source_videos"

    def test_migration_has_search_path_header(self):
        """Migration must use SET search_path (runner-driven, unqualified names)."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8")
        assert "search_path" in sql.lower(), "Migration must include SET search_path directive"

    def test_migration_default_is_null(self):
        """download_retry_after column must DEFAULT to NULL."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        # Check that DEFAULT NULL appears near the column definition
        assert "DEFAULT NULL" in sql, "download_retry_after must DEFAULT NULL"

    def test_migration_has_no_comment_on_column(self):
        """Migration must NOT include COMMENT ON COLUMN (chapters convention)."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "COMMENT ON COLUMN" not in sql, (
            "Migration must NOT use COMMENT ON COLUMN (matches 011 chapters convention)"
        )
