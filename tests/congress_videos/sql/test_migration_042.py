"""Tests for migration 042 — thumbnail republish state (issue #331).

Static SQL assertions, no DB connection (mirrors test_migration_041.py).
"""
from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "042_thumbnail_republish.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration042FileExists:

    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_041(self):
        names = sorted(p.name for p in MIGRATION_PATH.parent.glob("*.sql"))
        assert names.index(MIGRATION_PATH.name) > names.index(
            "041_analytics_actions.sql"
        )


class TestMigration042AddsColumnsVerbatim:

    NEW_COLUMN_CLAUSES = (
        "ADD COLUMN IF NOT EXISTS thumbnail_republish_needed_at  TIMESTAMPTZ",
        "ADD COLUMN IF NOT EXISTS thumbnail_republished_at       TIMESTAMPTZ",
        "ADD COLUMN IF NOT EXISTS thumbnail_republish_attempts   INTEGER DEFAULT 0",
        "ADD COLUMN IF NOT EXISTS thumbnail_republish_abandoned  BOOLEAN DEFAULT FALSE",
        "ADD COLUMN IF NOT EXISTS last_thumbnail_republish_error TEXT",
    )

    def test_all_five_columns_added_verbatim_on_speaker_turn_videos(self):
        sql = _executable_sql()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS".lower() in sql.lower()
        for clause in self.NEW_COLUMN_CLAUSES:
            assert clause in sql, f"Missing clause: {clause!r}"


class TestMigration042ViewDownAndHygiene:
    """uploadable_turns is structurally unaffected (no VIEW DDL); DOWN block
    documents the no-revert caveat; hygiene mirrors test_migrations_dag.py's
    parametrized idempotency check, only qualification is checked here."""

    def test_no_view_ddl_in_executable_sql(self):
        assert "VIEW" not in _executable_sql().upper()

    def test_down_block_documents_no_youtube_revert(self):
        sql = _sql().lower()
        assert "-- down" in sql
        down_text = sql[sql.index("-- down"):]
        assert "not" in down_text and "rollback" in down_text

    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql)
        assert not re.search(r"\bdevelopment\.\w+", sql)
        assert not re.search(r"\bproduction\.\w+", sql)
