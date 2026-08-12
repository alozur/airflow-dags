"""Test: migration 021 — expose resolved_participant_slug in uploadable_chapters.

Reads the SQL file statically and asserts structural properties: file exists,
recreates the view via CREATE OR REPLACE, appends resolved_participant_slug at
the END of the select list (required for CREATE OR REPLACE VIEW), and uses
unqualified names. No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "021_expose_resolved_participant_slug_in_uploadable_chapters.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def test_migration_file_exists():
    assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


def test_recreates_view_idempotently():
    sql = _sql().upper()
    assert "CREATE OR REPLACE VIEW UPLOADABLE_CHAPTERS" in sql


def test_exposes_resolved_participant_slug():
    assert "vc.resolved_participant_slug" in _sql()


def test_new_column_appended_at_end():
    """CREATE OR REPLACE VIEW can only append columns; the new column must come
    after the previously-last column (days_since_created)."""
    sql = _sql()
    assert sql.index("days_since_created") < sql.index("vc.resolved_participant_slug")


def test_no_schema_qualified_names():
    content = _sql()
    assert not re.search(r"\bpublic\.uploadable_chapters\b", content)
    assert not re.search(r"\bpublic\.video_chapters\b", content)
