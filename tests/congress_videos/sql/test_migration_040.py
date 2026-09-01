"""Tests for migration 040 — procedural turn filter (issue #143).

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
    / "040_add_procedural_turn_filter.sql"
)

# The 21 SELECT expressions inherited verbatim from migration 035, plus the
# new procedural_seconds column introduced by this migration (22 total).
INHERITED_035_SELECT_EXPRESSIONS = [
    "STV.TURN_ID",
    "STV.OUTPUT_PATH",
    "ST.CHAPTER_ID",
    "ST.RESOLVED_NAME",
    "ST.START_SECONDS",
    "ST.END_SECONDS",
    "ST.INTEREST_SCORE",
    "GS.GROUP_START_SECONDS",
    "GS.GROUP_END_SECONDS",
    "VC.VIDEO_ID",
    "VC.TITLE AS CHAPTER_TITLE",
    "VC.DESCRIPTION",
    "VC.RELEVANCE_SCORE",
    "VC.KEY_SPEAKERS",
    "YSV.SESSION_NUMBER",
    "YSV.SESSION_DATE",
    "STV.MATERIALIZED_AT",
    "STV.PREPARED_AT",
    "STV.RESOLVED_PARTICIPANT_SLUG",
    "STV.SPEAKER_RESOLUTION_CONFIDENCE",
    "STV.SPEAKER_RESOLUTION_METHOD",
]

NEW_PROCEDURAL_SELECT_EXPRESSIONS = [
    "GS.PROCEDURAL_SECONDS",
]

ALL_SELECT_EXPRESSIONS = (
    INHERITED_035_SELECT_EXPRESSIONS + NEW_PROCEDURAL_SELECT_EXPRESSIONS
)


class TestMigration040FileExists:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration040Columns:

    @staticmethod
    def _up_section() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8").split("-- DOWN")[0]

    def test_adds_is_procedural_column(self):
        """speaker_turns.is_procedural BOOLEAN NOT NULL DEFAULT FALSE."""
        sql = self._up_section().upper()
        assert re.search(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+IS_PROCEDURAL\s+BOOLEAN\s+NOT\s+NULL\s+DEFAULT\s+FALSE",
            sql,
        ), "Must add speaker_turns.is_procedural BOOLEAN NOT NULL DEFAULT FALSE"

    def test_adds_procedural_reason_column(self):
        """speaker_turns.procedural_reason TEXT (nullable)."""
        sql = self._up_section().upper()
        assert re.search(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+PROCEDURAL_REASON\s+TEXT", sql
        ), "Must add speaker_turns.procedural_reason TEXT"

    def test_adds_keep_intervals_column(self):
        """speaker_turn_videos.keep_intervals JSONB (nullable = legacy)."""
        sql = self._up_section().upper()
        assert re.search(
            r"ADD\s+COLUMN\s+IF\s+NOT\s+EXISTS\s+KEEP_INTERVALS\s+JSONB", sql
        ), "Must add speaker_turn_videos.keep_intervals JSONB"

    def test_alters_speaker_turns_table(self):
        sql = self._up_section().upper()
        assert re.search(r"ALTER\s+TABLE\s+SPEAKER_TURNS\b", sql)

    def test_alters_speaker_turn_videos_table(self):
        sql = self._up_section().upper()
        assert re.search(r"ALTER\s+TABLE\s+SPEAKER_TURN_VIDEOS\b", sql)


class TestMigration040ProceduralGate:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_drops_view_before_create(self):
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_creates_uploadable_turns_view(self):
        sql = self._sql().upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in sql

    def test_inner_where_excludes_procedural_turns(self):
        """Inner dedup WHERE must exclude is_procedural rows via NOT COALESCE."""
        sql = self._sql().upper()
        assert re.search(
            r"NOT\s+COALESCE\s*\(\s*ST\.IS_PROCEDURAL\s*,\s*FALSE\s*\)", sql
        ), "Inner WHERE must gate on NOT COALESCE(st.is_procedural, FALSE)"

    def test_cte_computes_procedural_seconds(self):
        """group_spans CTE must sum procedural durations per output_path."""
        sql = self._sql().upper()
        assert re.search(
            r"SUM\s*\(\s*CASE\s+WHEN\s+ST\.IS_PROCEDURAL\s+THEN\s+"
            r"ST\.END_SECONDS\s*-\s*ST\.START_SECONDS\s+ELSE\s+0\s+END\s*\)\s+"
            r"AS\s+PROCEDURAL_SECONDS",
            sql,
        ), "CTE must compute SUM(CASE WHEN st.is_procedural THEN ... ELSE 0 END) AS procedural_seconds"

    def test_outer_where_enforces_floor_minus_procedural_seconds(self):
        """Outer floor must subtract procedural_seconds from the published span."""
        sql = self._sql().upper()
        assert re.search(
            r"WHERE\s+DEDUP\.GROUP_END_SECONDS\s*-\s*DEDUP\.GROUP_START_SECONDS"
            r"\s*-\s*DEDUP\.PROCEDURAL_SECONDS\s*>=\s*300",
            sql,
        ), "Outer WHERE must gate on group span minus procedural_seconds >= 300"

    def test_threshold_documented_via_named_constant_comment(self):
        sql = self._sql()
        assert "MIN_TURN_UPLOAD_DURATION_SECONDS" in sql


class TestMigration040PreservesPriorGates:
    """Every gate from the 035 body must be carried forward verbatim."""

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    @staticmethod
    def _up_section() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8").split("-- DOWN")[0]

    def test_defines_group_spans_cte(self):
        sql = self._sql().upper()
        assert "WITH GROUP_SPANS AS (" in sql

    def test_cte_computes_min_start_and_max_end(self):
        sql = self._sql().upper()
        assert re.search(
            r"MIN\s*\(\s*ST\.START_SECONDS\s*\)\s+AS\s+GROUP_START_SECONDS", sql
        )
        assert re.search(
            r"MAX\s*\(\s*ST\.END_SECONDS\s*\)\s+AS\s+GROUP_END_SECONDS", sql
        )

    def test_cte_groups_by_output_path(self):
        sql = self._sql().upper()
        assert re.search(r"GROUP\s+BY\s+STV\.OUTPUT_PATH", sql)

    def test_joins_group_spans_back_on_output_path(self):
        sql = self._sql().upper()
        assert re.search(
            r"JOIN\s+GROUP_SPANS\s+GS\s+ON\s+GS\.OUTPUT_PATH\s*=\s*STV\.OUTPUT_PATH", sql
        )

    def test_view_retains_distinct_on_output_path(self):
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql

    def test_view_retains_prepared_at_gate(self):
        sql = self._sql().upper()
        assert re.search(r"STV\.PREPARED_AT\s+IS\s+NOT\s+NULL", sql)

    def test_view_retains_is_upload_abandoned_gate(self):
        sql = self._sql().upper()
        assert re.search(r"NOT\s+STV\.IS_UPLOAD_ABANDONED", sql)

    def test_view_retains_stv_is_uploaded_to_youtube_gate(self):
        sql = self._sql().upper()
        assert "STV.IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_vc_is_uploaded_to_youtube_gate(self):
        sql = self._sql().upper()
        assert "VC.IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_relevance_score_gate(self):
        sql = self._sql().upper()
        assert re.search(r"VC\.RELEVANCE_SCORE\s*>=\s*2", sql)

    def test_view_retains_interest_score_gate(self):
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*ST\.INTEREST_SCORE\s*,\s*1\s*\)\s*>=\s*1", sql
        )

    def test_view_retains_outer_ordering(self):
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC", sql
        )
        assert "DEDUP.RELEVANCE_SCORE DESC" in sql
        assert "DEDUP.SESSION_DATE DESC" in sql

    def test_view_retains_all_four_joins(self):
        sql = self._sql().upper()
        assert "JOIN SPEAKER_TURNS ST ON STV.TURN_ID = ST.TURN_ID" in sql
        assert "JOIN VIDEO_CHAPTERS VC ON ST.CHAPTER_ID = VC.CHAPTER_ID" in sql
        assert "JOIN YOUTUBE_SOURCE_VIDEOS YSV ON VC.VIDEO_ID = YSV.VIDEO_ID" in sql
        assert re.search(
            r"JOIN\s+GROUP_SPANS\s+GS\s+ON\s+GS\.OUTPUT_PATH\s*=\s*STV\.OUTPUT_PATH", sql
        )

    @pytest.mark.parametrize("expression", ALL_SELECT_EXPRESSIONS)
    def test_select_expression_present_in_up_section(self, expression):
        up_sql = self._up_section().upper()
        assert expression in up_sql, f"UP section must select {expression}"


class TestMigration040Hygiene:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_no_schema_qualification(self):
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql)
        assert not re.search(r"\bproduction\.\w+", sql)

    def test_down_block_present(self):
        sql = self._sql().upper()
        assert "-- DOWN" in sql

    def test_down_block_restores_035_view_without_procedural_gate(self):
        """DOWN section's restored CREATE VIEW body must not reference
        is_procedural or procedural_seconds (the trailing DROP COLUMN
        statements legitimately name is_procedural to remove it)."""
        sql = self._sql()
        down_section = sql.split("-- DOWN")[1]
        down_upper = down_section.upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in down_upper
        view_block = down_upper.split("CREATE VIEW UPLOADABLE_TURNS")[1].split(
            "ALTER TABLE"
        )[0]
        assert "IS_PROCEDURAL" not in view_block
        assert "PROCEDURAL_SECONDS" not in view_block

    def test_down_block_drops_new_columns(self):
        """DOWN section must drop the three new columns for a full rollback."""
        sql = self._sql()
        down_section = sql.split("-- DOWN")[1].upper()
        assert "DROP COLUMN IF EXISTS IS_PROCEDURAL" in down_section
        assert "DROP COLUMN IF EXISTS PROCEDURAL_REASON" in down_section
        assert "DROP COLUMN IF EXISTS KEEP_INTERVALS" in down_section
