"""Tests for migration 036 — TIMESTAMPTZ unification + FK hygiene (issue #209).

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_035.py pattern).
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
    / "036_timestamptz_and_fk_hygiene.sql"
)

# The 18 (table, column) pairs that must convert to TIMESTAMPTZ.
CONVERTED_COLUMNS = [
    ("congressional_sessions", "processed_at"),
    ("congressional_sessions", "updated_at"),
    ("video_topics", "ai_interest_evaluated_at"),
    ("video_topics", "youtube_upload_date"),
    ("video_topics", "youtube_metadata_generated_at"),
    ("video_topics", "thumbnail_generated_at"),
    ("video_topics", "created_at"),
    ("video_topics", "updated_at"),
    ("upload_queue", "queued_at"),
    ("upload_queue", "last_attempt_at"),
    ("youtube_source_videos", "published_at"),
    ("youtube_source_videos", "download_retry_after"),
    ("youtube_source_videos", "created_at"),
    ("youtube_source_videos", "updated_at"),
    ("video_chapters", "scored_at"),
    ("video_chapters", "youtube_upload_date"),
    ("video_chapters", "created_at"),
    ("video_chapters", "updated_at"),
]


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration036FileExists:

    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_exactly_18_converted_columns_documented(self):
        """Sanity check on this test file's own fixture — 18 columns, 5 tables."""
        assert len(CONVERTED_COLUMNS) == 18
        assert len({t for t, _ in CONVERTED_COLUMNS}) == 5


class TestMigration036TimestamptzConversion:

    @pytest.mark.parametrize("table, column", CONVERTED_COLUMNS)
    def test_column_pair_present(self, table, column):
        """Each of the 18 (table, column) pairs must appear in the migration."""
        sql = _sql()
        assert f"'{table}'" in sql and f"'{column}'" in sql, (
            f"Migration must reference {table}.{column}"
        )

    def test_uses_at_time_zone_utc(self):
        sql = _sql().upper()
        assert "AT TIME ZONE 'UTC'" in sql

    def test_guarded_on_information_schema(self):
        sql = _sql().upper()
        assert "INFORMATION_SCHEMA.COLUMNS" in sql
        assert "TIMESTAMP WITHOUT TIME ZONE" in sql

    def test_guard_scoped_to_current_schema(self):
        """The information_schema guard must scope to current_schema(), not a
        hardcoded schema name, so the migration works for both dev and prod."""
        sql = _sql().upper()
        assert "CURRENT_SCHEMA()" in sql

    def test_alter_uses_dynamic_execute_format(self):
        """Column names come from a loop, so the ALTER must be built via
        EXECUTE format(...) rather than static SQL."""
        sql = _sql().upper()
        assert "EXECUTE FORMAT(" in sql
        assert "ALTER TABLE %I ALTER COLUMN %I TYPE TIMESTAMPTZ" in sql


class TestMigration036ViewRecreation:

    def test_drops_uploadable_chapters_before_recreate(self):
        sql = _sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_CHAPTERS" in sql

    def test_drops_uploadable_videos_before_recreate(self):
        sql = _sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_VIDEOS" in sql

    def test_recreates_uploadable_chapters(self):
        sql = _sql().upper()
        assert "CREATE VIEW UPLOADABLE_CHAPTERS AS" in sql

    def test_recreates_uploadable_videos(self):
        sql = _sql().upper()
        assert "CREATE VIEW UPLOADABLE_VIDEOS AS" in sql

    def test_uploadable_videos_recreation_guarded_on_legacy_tables(self):
        """The legacy video_topic pipeline tables never existed in the NAS
        development/production schemas, so the uploadable_videos recreation
        must be guarded on to_regclass for all three base tables (design D4)
        instead of failing the whole transactional migration run."""
        sql = _sql().upper()
        create_pos = sql.index("CREATE VIEW UPLOADABLE_VIDEOS AS")
        guard_region = sql[:create_pos]
        assert "TO_REGCLASS('UPLOAD_QUEUE')" in guard_region
        assert "TO_REGCLASS('VIDEO_TOPICS')" in guard_region
        assert "TO_REGCLASS('CONGRESSIONAL_SESSIONS')" in guard_region

    def test_uploadable_chapters_body_matches_021_verbatim(self):
        """The recreated view must select resolved_participant_slug and order
        by session_date DESC NULLS LAST (021's body), and must NOT resurrect
        011's is_upload_abandoned gate in EXECUTABLE SQL (explanatory comment
        prose mentioning the rejected gate is allowed)."""
        sql = _sql().upper()
        assert "VC.RESOLVED_PARTICIPANT_SLUG" in sql
        assert "SESSION_DATE DESC NULLS LAST" in sql
        assert "IS_UPLOAD_ABANDONED" not in _executable_sql().upper()

    def test_uploadable_turns_never_mentioned(self):
        """This migration must never touch uploadable_turns in EXECUTABLE SQL
        — the 035-hardcoded drift test must stay green untouched. Explanatory
        comment prose noting it is untouched is allowed."""
        assert "UPLOADABLE_TURNS" not in _executable_sql().upper()


class TestMigration036FkHygiene:

    def test_speaker_normalization_cache_fk_cascade_block_present(self):
        sql = _sql().upper()
        assert "SPEAKER_NORMALIZATION_CACHE" in sql
        assert "ON DELETE CASCADE" in sql

    def test_fk_lookup_via_pg_constraint(self):
        sql = _sql().upper()
        assert "PG_CONSTRAINT" in sql

    def test_speaker_turn_videos_new_fk_present(self):
        sql = _sql().upper()
        assert "SPEAKER_TURN_VIDEOS" in sql
        assert "CONGRESS_PARTICIPANTS(SLUG)" in sql

    def test_orphan_slugs_nulled_before_fk_added(self):
        """The UPDATE nulling orphans must appear before the ADD CONSTRAINT
        for the new FK, in the same DO block."""
        sql = _sql().upper()
        update_idx = sql.index("SET RESOLVED_PARTICIPANT_SLUG = NULL")
        fk_idx = sql.index("SPEAKER_TURN_VIDEOS_RESOLVED_PARTICIPANT_SLUG_FKEY")
        assert update_idx < fk_idx

    def test_orphan_count_logged_via_raise_notice(self):
        sql = _sql().upper()
        assert "GET DIAGNOSTICS" in sql
        assert re.search(r"RAISE\s+NOTICE.*ORPHAN", sql, re.IGNORECASE)


class TestMigration036Hygiene:

    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bdevelopment\.\w+", sql), "Must not use development.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        sql = _sql().upper()
        assert "-- DOWN" in sql

    def test_down_block_mentions_all_18_reversals(self):
        down_section = _sql().split("-- DOWN")[1]
        for table, column in CONVERTED_COLUMNS:
            assert table in down_section and column in down_section, (
                f"DOWN block must document reversal for {table}.{column}"
            )

    def test_no_bare_create_table(self):
        """Static idempotency guard mirrored from test_migrations_dag.py."""
        sql = _sql()
        assert not re.search(r"\bCREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_seed_inserts(self):
        sql = _sql()
        assert not re.search(r"\bINSERT\s+INTO\b", sql, re.IGNORECASE)
