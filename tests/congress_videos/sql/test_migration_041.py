"""Tests for migration 041 — analytics checkpoint actions (issues #102 + #185).

Static SQL assertions, no DB connection (mirrors test_migration_039.py).
Adds video_thumbnails.archetype, video_analytics_snapshots.action_detail,
and the FIRST named CHECK constraint on the pre-existing
video_analytics_snapshots.action_taken column (reserved NULL placeholder
since migration 026). Mirrors 039's DROP CONSTRAINT IF EXISTS + ADD
CONSTRAINT idempotent shape even though no prior constraint exists on this
column, per the design-amendments binding shape.
"""
from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "041_analytics_actions.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration040FileExists:

    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_039(self):
        names = sorted(p.name for p in MIGRATION_PATH.parent.glob("*.sql"))
        assert names.index(MIGRATION_PATH.name) > names.index(
            "039_extend_speaker_turns_source.sql"
        )


class TestMigration040ArchetypeColumn:

    def test_adds_archetype_column_to_video_thumbnails(self):
        sql = _executable_sql()
        assert "ALTER TABLE video_thumbnails ADD COLUMN IF NOT EXISTS archetype TEXT" in sql


class TestMigration040ActionDetailColumn:

    def test_adds_action_detail_jsonb_column(self):
        sql = _executable_sql()
        assert (
            "ALTER TABLE video_analytics_snapshots ADD COLUMN IF NOT EXISTS action_detail JSONB"
            in sql
        )


class TestMigration040ActionTakenCheckConstraint:

    def test_drops_the_named_constraint_first(self):
        """Mirrors 039's shape: DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT."""
        sql = _executable_sql().upper()
        assert (
            "DROP CONSTRAINT IF EXISTS VIDEO_ANALYTICS_SNAPSHOTS_ACTION_TAKEN_CHECK"
            in sql
        )

    def test_adds_constraint_with_same_name(self):
        sql = _executable_sql().upper()
        assert "ADD CONSTRAINT VIDEO_ANALYTICS_SNAPSHOTS_ACTION_TAKEN_CHECK" in sql

    def test_drop_precedes_add(self):
        sql = _executable_sql().upper()
        drop_idx = sql.index("DROP CONSTRAINT IF EXISTS VIDEO_ANALYTICS_SNAPSHOTS_ACTION_TAKEN_CHECK")
        add_idx = sql.index("ADD CONSTRAINT VIDEO_ANALYTICS_SNAPSHOTS_ACTION_TAKEN_CHECK")
        assert drop_idx < add_idx

    def test_constraint_is_null_permissive(self):
        """NULL (unevaluated) must remain a valid value — the CHECK is
        additive-only and every existing row is NULL."""
        sql = _executable_sql().upper()
        assert "ACTION_TAKEN IS NULL OR ACTION_TAKEN IN" in sql

    def test_constraint_allows_exactly_the_seven_vocabulary_values(self):
        sql = _executable_sql()
        expected = {
            "cold_start",
            "ok",
            "capped",
            "in_progress",
            "thumbnail_regenerated",
            "thumbnail_and_title_regenerated",
            "failed",
        }
        for value in expected:
            assert f"'{value}'" in sql, f"Missing action_taken value: {value}"

    def test_does_not_redeclare_action_taken_column(self):
        """action_taken already exists from migration 026 — 040 must only
        add the CHECK constraint, never ADD COLUMN action_taken."""
        sql = _executable_sql().upper()
        assert "ADD COLUMN IF NOT EXISTS ACTION_TAKEN" not in sql


class TestMigration040DownBlock:

    def test_down_block_present(self):
        sql = _sql().upper()
        assert "-- DOWN" in sql

    def test_down_documents_published_changes_not_reverted(self):
        sql = _sql().lower()
        down_idx = sql.index("-- down")
        down_text = sql[down_idx:]
        assert "not" in down_text and "revert" in down_text


class TestMigration040Hygiene:
    """Bare CREATE/DROP/INSERT hygiene is already covered for every migration
    file by tests/utils/test_migrations_dag.py's parametrized
    TestMigrationIdempotency (which includes this file automatically) — only
    the schema-qualification check is asserted here to avoid duplication."""

    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bdevelopment\.\w+", sql), "Must not use development.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"
