"""Tests for migration 043 — art direction brief persistence (issue #292).

Static SQL assertions, no DB connection (mirrors test_migration_042.py).
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "043_persist_art_direction_brief.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration043FileExists:
    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_042(self):
        names = sorted(p.name for p in MIGRATION_PATH.parent.glob("*.sql"))
        assert names.index(MIGRATION_PATH.name) > names.index("042_thumbnail_republish.sql")


class TestMigration043AddsColumnVerbatim:
    NEW_COLUMN_CLAUSE = "ADD COLUMN IF NOT EXISTS art_direction_brief JSONB"

    def test_column_added_verbatim_on_video_thumbnails(self):
        sql = _executable_sql()
        assert "ALTER TABLE video_thumbnails".lower() in sql.lower()
        assert self.NEW_COLUMN_CLAUSE in sql, f"Missing clause: {self.NEW_COLUMN_CLAUSE!r}"

    def test_column_type_is_jsonb(self):
        sql = _executable_sql()
        match = re.search(r"ADD COLUMN IF NOT EXISTS art_direction_brief\s+(\w+)", sql)
        assert match is not None
        assert match.group(1).upper() == "JSONB"


class TestMigration043HygieneAndDown:
    """Mirrors test_migration_042.py's hygiene checks: no schema
    qualification (the runner sets search_path), and a DOWN block."""

    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql)
        assert not re.search(r"\bdevelopment\.\w+", sql)
        assert not re.search(r"\bproduction\.\w+", sql)

    def test_down_block_present(self):
        sql = _sql().lower()
        assert "-- down" in sql
