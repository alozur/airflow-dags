"""Tests for migration 038 — restore is_upload_abandoned gate on
uploadable_chapters (issue #251).

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_036.py / 037.py pattern).
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "038_restore_chapter_abandoned_gate.sql"
)

MIGRATION_021_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "021_expose_resolved_participant_slug_in_uploadable_chapters.sql"
)


def _sql() -> str:
    return MIGRATION_PATH.read_text(encoding="utf-8")


def _executable_sql() -> str:
    """Strip `-- ...` line comments, leaving only executable SQL."""
    return re.sub(r"--[^\n]*", "", _sql())


def _select_list_slice(sql_upper: str) -> str:
    """Whitespace-normalized SELECT...FROM VIDEO_CHAPTERS slice."""
    select_idx = sql_upper.index("SELECT")
    from_idx = sql_upper.index("FROM VIDEO_CHAPTERS")
    return re.sub(r"\s+", " ", sql_upper[select_idx:from_idx]).strip()


class TestMigration038FileExists:
    def test_migration_file_exists(self):
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"

    def test_filename_sorts_after_037(self):
        migrations_dir = MIGRATION_PATH.parent
        names = sorted(p.name for p in migrations_dir.glob("*.sql"))
        assert "038_restore_chapter_abandoned_gate.sql" in names
        idx_038 = names.index("038_restore_chapter_abandoned_gate.sql")
        idx_037 = names.index("037_upload_path_indexes.sql")
        assert idx_038 > idx_037


class TestMigration038ViewRecreation:
    def test_uses_create_or_replace_view(self):
        sql = _sql().upper()
        assert "CREATE OR REPLACE VIEW UPLOADABLE_CHAPTERS AS" in sql

    def test_drop_view_absent_from_executable_sql(self):
        """No DROP+CREATE — D1: CREATE OR REPLACE VIEW supersedes the
        proposal's DROP+CREATE wording (explanatory comment prose mentioning
        DROP is allowed, only executable SQL is checked)."""
        assert "DROP VIEW" not in _executable_sql().upper()

    def test_gate_present(self):
        sql = _executable_sql().upper()
        assert re.search(r"AND\s+VC\.IS_UPLOAD_ABANDONED\s*=\s*FALSE", sql)

    def test_gate_ordered_after_relevance_score_and_before_order_by(self):
        sql = _executable_sql().upper()
        gate_idx = sql.index("IS_UPLOAD_ABANDONED")
        relevance_idx = sql.index("RELEVANCE_SCORE >= 2")
        order_by_idx = sql.index("ORDER BY")
        assert relevance_idx < gate_idx < order_by_idx

    def test_select_list_matches_021_verbatim(self):
        """SELECT-list slice (SELECT..FROM video_chapters), whitespace
        normalized, must be byte-identical to 021's — catches drift and
        guarantees resolved_participant_slug is retained as the last column."""
        sql_038 = _select_list_slice(_executable_sql().upper())
        sql_021 = _select_list_slice(re.sub(r"--[^\n]*", "", MIGRATION_021_PATH.read_text(encoding="utf-8")).upper())
        assert sql_038 == sql_021

    def test_retains_is_uploaded_to_youtube_predicate(self):
        sql = _executable_sql().upper()
        assert "VC.IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_retains_relevance_score_predicate(self):
        sql = _executable_sql().upper()
        assert "VC.RELEVANCE_SCORE >= 2" in sql

    def test_order_by_matches_021_and_036(self):
        sql = _executable_sql().upper()
        assert "SESSION_DATE DESC NULLS LAST" in sql
        assert "VC.RELEVANCE_SCORE DESC" in sql
        assert "VC.CREATED_AT DESC" in sql


class TestMigration038Hygiene:
    def test_no_schema_qualification(self):
        sql = _sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bdevelopment\.\w+", sql), "Must not use development.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        sql = _sql().upper()
        assert "-- DOWN" in sql

    def test_no_bare_create_table(self):
        sql = _sql()
        assert not re.search(r"\bCREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_bare_create_index(self):
        sql = _sql()
        assert not re.search(r"\bCREATE\s+INDEX\s+(?!IF\s+NOT\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_bare_drop_table(self):
        sql = _sql()
        assert not re.search(r"\bDROP\s+TABLE\s+(?!IF\s+EXISTS\b)", sql, re.IGNORECASE)

    def test_no_seed_inserts(self):
        sql = _sql()
        assert not re.search(r"\bINSERT\s+INTO\b", sql, re.IGNORECASE)
