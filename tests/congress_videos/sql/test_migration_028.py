"""[RED] Test: migration 028 — deduplicate uploadable_turns view by output_path.

Reads the SQL file statically and asserts structural properties:
DISTINCT ON dedup, inner tiebreaker ORDER BY, outer presentation ORDER BY,
DROP VIEW IF EXISTS guard, and no schema-qualified names (unqualified for runner).
No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "028_dedup_uploadable_turns_view.sql"
)


class TestMigration028FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration028ViewStructure:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_drops_view_before_create(self):
        """Migration must DROP VIEW IF EXISTS uploadable_turns before creating it."""
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_creates_uploadable_turns_view(self):
        """Migration must CREATE VIEW uploadable_turns."""
        sql = self._sql().upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in sql

    def test_contains_distinct_on_output_path(self):
        """View must use DISTINCT ON (stv.output_path) to deduplicate grouped turns."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql, "Migration must contain DISTINCT ON (stv.output_path)"

    def test_inner_order_by_output_path_then_turn_id(self):
        """Inner ORDER BY must be stv.output_path, stv.turn_id for deterministic dedup."""
        sql = self._sql().upper()
        assert re.search(
            r"ORDER\s+BY\s+STV\.OUTPUT_PATH\s*,\s*STV\.TURN_ID",
            sql,
        ), "Inner ORDER BY must be stv.output_path, stv.turn_id"

    def test_outer_order_by_relevance_score_desc(self):
        """Outer SELECT must ORDER BY relevance_score DESC."""
        sql = self._sql().upper()
        assert re.search(r"ORDER\s+BY.*RELEVANCE_SCORE\s+DESC", sql, re.DOTALL), (
            "Outer ORDER BY must include relevance_score DESC"
        )

    def test_outer_order_by_session_date_desc(self):
        """Outer SELECT must include session_date DESC in ORDER BY."""
        sql = self._sql().upper()
        assert re.search(r"SESSION_DATE\s+DESC", sql), "Outer ORDER BY must include session_date DESC"

    def test_filters_stv_not_uploaded(self):
        """View must filter stv.is_uploaded_to_youtube = FALSE."""
        sql = self._sql().upper()
        assert "IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_filters_chapters_not_uploaded(self):
        """View must filter vc.is_uploaded_to_youtube = FALSE."""
        sql = self._sql()
        assert re.search(r"vc\.is_uploaded_to_youtube\s*=\s*FALSE", sql, re.IGNORECASE), (
            "View must filter vc.is_uploaded_to_youtube = FALSE"
        )

    def test_filters_relevance_score_gte_2(self):
        """View must filter relevance_score >= 2."""
        sql = self._sql()
        assert re.search(r"relevance_score\s*>=\s*2", sql, re.IGNORECASE), "View must filter relevance_score >= 2"

    def test_no_public_schema_qualification(self):
        """Migration must not use public.-qualified table names (runner sets search_path)."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Migration must not contain public.-qualified table names"

    def test_no_production_schema_qualification(self):
        """Migration must not use production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bproduction\.\w+", sql), "Migration must not contain production.-qualified table names"
