"""A1 [RED] Test: migration 018 — add slug column to congress_participants.

Tests that the migration SQL file has the correct structural properties:
file exists, adds slug column idempotently, backfills before NOT NULL,
enforces uniqueness, and uses unqualified table names.

Parsed from the SQL file directly — no DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).parent.parent.parent.parent
    / "congress_videos"
    / "sql"
    / "migrations"
    / "018_add_participant_slug.sql"
)


class TestMigration018:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_adds_slug_column_if_not_exists(self):
        """Migration must add slug column idempotently (ADD COLUMN IF NOT EXISTS)."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "ADD COLUMN IF NOT EXISTS SLUG" in sql

    def test_backfills_slug_before_not_null(self):
        """Backfill UPDATE must appear before SET NOT NULL to avoid constraint failure."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        update_pos = sql.find("UPDATE CONGRESS_PARTICIPANTS")
        notnull_pos = sql.find("SET NOT NULL")
        assert update_pos != -1 and notnull_pos != -1
        assert update_pos < notnull_pos, "Backfill UPDATE must precede SET NOT NULL"

    def test_backfill_uses_replace_space_hyphen(self):
        """Backfill must use REPLACE(normalized_name, ' ', '-') to derive slug."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "REPLACE(NORMALIZED_NAME, ' ', '-')" in sql

    def test_sets_not_null(self):
        """Migration must enforce NOT NULL on slug after backfill."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "ALTER COLUMN SLUG SET NOT NULL" in sql

    def test_enforces_uniqueness(self):
        """Migration must create a UNIQUE INDEX on slug."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert "UNIQUE INDEX IF NOT EXISTS" in sql
        assert "SLUG" in sql

    def test_table_name_unqualified(self):
        """Migration must use unqualified table names (runner sets search_path)."""
        content = MIGRATION_PATH.read_text(encoding="utf-8")
        assert not re.search(r"\bpublic\.congress_participants\b", content)
