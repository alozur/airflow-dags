"""Tests for migration 037 — upload-path partial indexes (issue #204).

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_035.py / 036.py pattern).
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "037_upload_path_indexes.sql"
)

# (index_name, predicate_substring) — predicate matched verbatim against the
# current view/query text (see design.md C4 table).
INDEXES = [
    (
        "idx_video_chapters_pending_priority",
        "is_uploaded_to_youtube = FALSE",
    ),
    (
        "idx_speaker_turn_videos_uploadable",
        "is_uploaded_to_youtube = FALSE AND prepared_at IS NOT NULL AND NOT is_upload_abandoned",
    ),
    (
        "idx_speaker_turn_videos_unprepared",
        "prepared_at IS NULL AND is_uploaded_to_youtube = FALSE",
    ),
    (
        "idx_video_shorts_uploaded_recent",
        "is_uploaded = TRUE",
    ),
    (
        "idx_video_shorts_pending_downloaded",
        (
            "is_uploaded = FALSE AND is_upload_abandoned = FALSE AND local_file_path IS NOT NULL AND "
            "reap_status = 'downloaded'"
        ),
    ),
]


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


class TestMigration037FileExists:
    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_exactly_5_indexes_documented(self):
        assert len(INDEXES) == 5


class TestMigration037Indexes:
    @pytest.mark.parametrize("index_name, predicate", INDEXES)
    def test_index_name_present(self, index_name, predicate):
        assert index_name in _sql()

    @pytest.mark.parametrize("index_name, predicate", INDEXES)
    def test_index_uses_if_not_exists(self, index_name, predicate):
        sql = _sql()
        assert f"CREATE INDEX IF NOT EXISTS {index_name}" in sql

    @pytest.mark.parametrize("index_name, predicate", INDEXES)
    def test_predicate_present_verbatim(self, index_name, predicate):
        assert predicate in _sql()

    def test_no_concurrently_anywhere(self):
        """Plain CREATE INDEX only — the runner cannot support CONCURRENTLY
        (every migration file runs inside an implicit transaction). Explanatory
        comment prose mentioning CONCURRENTLY is allowed."""
        assert "CONCURRENTLY" not in _executable_sql().upper()

    def test_video_chapters_index_columns(self):
        sql = _sql()
        assert "relevance_score DESC" in sql
        assert "created_at DESC" in sql

    def test_speaker_turn_videos_indexes_use_output_path_and_turn_id(self):
        sql = _sql()
        assert sql.count("(output_path, turn_id)") == 2

    def test_video_shorts_uploaded_recent_orders_by_updated_at_desc(self):
        sql = _sql()
        assert "updated_at DESC" in sql

    def test_video_shorts_pending_downloaded_orders_by_virality_nulls_last(self):
        sql = _sql()
        assert "reap_virality_score DESC NULLS LAST" in sql


class TestMigration037Hygiene:
    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bdevelopment\.\w+", sql), "Must not use development.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        sql = _sql().upper()
        assert "-- DOWN" in sql

    def test_down_block_has_5_drop_index_statements(self):
        down_section = _sql().split("-- DOWN")[1]
        for index_name, _ in INDEXES:
            assert f"DROP INDEX IF EXISTS {index_name}" in down_section

    def test_no_bare_create_table(self):
        sql = _sql()
        assert not re.search(r"\bCREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_seed_inserts(self):
        sql = _sql()
        assert not re.search(r"\bINSERT\s+INTO\b", sql, re.IGNORECASE)
