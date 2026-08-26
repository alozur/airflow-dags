"""Tests for migration 035 — minimum grouped-video duration gate (issue #234).

Reads the SQL file statically and asserts structural properties.
No DB connection required (mirrors test_migration_034.py pattern).
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
    / "035_add_min_turn_duration.sql"
)

# The 19 SELECT expressions inherited verbatim from migration 034, plus the 2
# new group-span columns introduced by this migration (21 total).
INHERITED_034_SELECT_EXPRESSIONS = [
    "STV.TURN_ID",
    "STV.OUTPUT_PATH",
    "ST.CHAPTER_ID",
    "ST.RESOLVED_NAME",
    "ST.START_SECONDS",
    "ST.END_SECONDS",
    "ST.INTEREST_SCORE",
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

NEW_GROUP_SPAN_SELECT_EXPRESSIONS = [
    "GS.GROUP_START_SECONDS",
    "GS.GROUP_END_SECONDS",
]

ALL_034_AND_NEW_SELECT_EXPRESSIONS = (
    INHERITED_034_SELECT_EXPRESSIONS + NEW_GROUP_SPAN_SELECT_EXPRESSIONS
)


class TestMigration035FileExists:

    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration035DurationGate:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_drops_view_before_create(self):
        """Migration must DROP VIEW IF EXISTS uploadable_turns before recreating."""
        sql = self._sql().upper()
        assert "DROP VIEW IF EXISTS UPLOADABLE_TURNS" in sql

    def test_creates_uploadable_turns_view(self):
        """Migration must CREATE VIEW uploadable_turns."""
        sql = self._sql().upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in sql

    def test_outer_where_enforces_300_second_floor(self):
        """Outer dedup WHERE must enforce group_end - group_start >= 300."""
        sql = self._sql().upper()
        assert re.search(
            r"WHERE\s+DEDUP\.GROUP_END_SECONDS\s*-\s*DEDUP\.GROUP_START_SECONDS\s*>=\s*300",
            sql,
        ), "Outer WHERE must gate on dedup.group_end_seconds - dedup.group_start_seconds >= 300"

    def test_group_span_columns_selected_from_gs_alias(self):
        """Both group span columns must be selected via the gs. alias."""
        sql = self._sql().upper()
        assert "GS.GROUP_START_SECONDS" in sql
        assert "GS.GROUP_END_SECONDS" in sql

    def test_threshold_documented_via_named_constant_comment(self):
        """The 300s literal must be documented via a MIN_TURN_UPLOAD_DURATION_SECONDS comment."""
        sql = self._sql()
        assert "MIN_TURN_UPLOAD_DURATION_SECONDS" in sql


class TestMigration035GroupSpanCte:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_defines_group_spans_cte(self):
        """Migration must define a WITH group_spans AS ( ... ) CTE."""
        sql = self._sql().upper()
        assert "WITH GROUP_SPANS AS (" in sql

    def test_cte_computes_min_start_seconds(self):
        """CTE must compute MIN(st.start_seconds) AS group_start_seconds."""
        sql = self._sql().upper()
        assert re.search(
            r"MIN\s*\(\s*ST\.START_SECONDS\s*\)\s+AS\s+GROUP_START_SECONDS", sql
        ), "CTE must compute MIN(st.start_seconds) AS group_start_seconds"

    def test_cte_computes_max_end_seconds(self):
        """CTE must compute MAX(st.end_seconds) AS group_end_seconds."""
        sql = self._sql().upper()
        assert re.search(
            r"MAX\s*\(\s*ST\.END_SECONDS\s*\)\s+AS\s+GROUP_END_SECONDS", sql
        ), "CTE must compute MAX(st.end_seconds) AS group_end_seconds"

    def test_cte_groups_by_output_path(self):
        """CTE must GROUP BY stv.output_path."""
        sql = self._sql().upper()
        assert re.search(r"GROUP\s+BY\s+STV\.OUTPUT_PATH", sql)

    def test_joins_group_spans_back_on_output_path(self):
        """Main query must JOIN group_spans gs ON gs.output_path = stv.output_path."""
        sql = self._sql().upper()
        assert re.search(
            r"JOIN\s+GROUP_SPANS\s+GS\s+ON\s+GS\.OUTPUT_PATH\s*=\s*STV\.OUTPUT_PATH", sql
        ), "Must JOIN group_spans gs ON gs.output_path = stv.output_path"


class TestMigration035BugClassGuards:
    """Negative regression guards for the issue #151 bug class: the group span
    must be computed independently of the eligibility gates, never from a
    single representative row's own duration or a post-WHERE window function.

    Assertions here run against EXECUTABLE SQL only (comment lines stripped),
    since the migration's explanatory prose legitimately documents the
    rejected `OVER (PARTITION BY ...)` alternative and the `prepared_at`
    reasoning without actually using them in code.
    """

    @staticmethod
    def _strip_sql_comments(text: str) -> str:
        """Remove `-- ...` line comments, leaving only executable SQL."""
        return re.sub(r"--[^\n]*", "", text)

    @classmethod
    def _executable_cte_text(cls) -> str:
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        cte = sql.split("WITH GROUP_SPANS AS (")[1].split("SELECT * FROM (")[0]
        return cls._strip_sql_comments(cte)

    @classmethod
    def _executable_sql(cls) -> str:
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        return cls._strip_sql_comments(sql)

    def test_cte_has_no_eligibility_gate_tokens(self):
        """The group_spans CTE must be unfiltered — no eligibility gate columns
        in the executable portion (comment prose is allowed to explain why)."""
        cte = self._executable_cte_text()
        for forbidden in (
            "PREPARED_AT",
            "IS_UPLOAD_ABANDONED",
            "RELEVANCE_SCORE",
            "INTEREST_SCORE",
            "IS_UPLOADED_TO_YOUTUBE",
        ):
            assert forbidden not in cte, (
                f"group_spans CTE must not reference {forbidden} in executable SQL — "
                "the span must be computed before any eligibility gate is applied"
            )

    def test_no_partition_by_window_function_anywhere(self):
        """The migration must never USE OVER (PARTITION BY ...) in executable SQL
        — it evaluates after WHERE and collapses to a single sibling once
        prepared_at gates. Comment prose explaining the rejected alternative
        is allowed."""
        sql = self._executable_sql()
        assert "OVER (PARTITION BY" not in sql

    def test_no_naive_representative_row_duration_predicate(self):
        """Duration must never be derived from a single representative row's
        own start/end columns (st.* or dedup.* difference)."""
        sql = MIGRATION_PATH.read_text(encoding="utf-8").upper()
        assert not re.search(
            r"(ST|DEDUP)\.END_SECONDS\s*-\s*(ST|DEDUP)\.START_SECONDS", sql
        ), "Duration must be derived from the group span, not a single sibling's own duration"


class TestMigration035PreservesPriorGates:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    @staticmethod
    def _up_section() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8").split("-- DOWN")[0]

    def test_view_retains_distinct_on_output_path(self):
        """View must retain DISTINCT ON (stv.output_path) dedup strategy (028)."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql

    def test_view_retains_prepared_at_gate(self):
        """View WHERE clause must retain stv.prepared_at IS NOT NULL gate (030)."""
        sql = self._sql().upper()
        assert re.search(r"STV\.PREPARED_AT\s+IS\s+NOT\s+NULL", sql)

    def test_view_retains_is_upload_abandoned_gate(self):
        """View WHERE clause must retain NOT stv.is_upload_abandoned gate (032)."""
        sql = self._sql().upper()
        assert re.search(r"NOT\s+STV\.IS_UPLOAD_ABANDONED", sql)

    def test_view_retains_stv_is_uploaded_to_youtube_gate(self):
        """View WHERE clause must retain stv.is_uploaded_to_youtube = FALSE gate."""
        sql = self._sql().upper()
        assert "STV.IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_vc_is_uploaded_to_youtube_gate(self):
        """View WHERE clause must retain vc.is_uploaded_to_youtube = FALSE gate."""
        sql = self._sql().upper()
        assert "VC.IS_UPLOADED_TO_YOUTUBE = FALSE" in sql

    def test_view_retains_relevance_score_gate(self):
        """View WHERE clause must retain vc.relevance_score >= 2 gate."""
        sql = self._sql().upper()
        assert re.search(r"VC\.RELEVANCE_SCORE\s*>=\s*2", sql)

    def test_view_retains_interest_score_gate(self):
        """View WHERE clause must retain COALESCE(st.interest_score, 1) >= 1 gate."""
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*ST\.INTEREST_SCORE\s*,\s*1\s*\)\s*>=\s*1", sql
        )

    def test_view_retains_outer_ordering(self):
        """Outer ORDER BY must retain interest, relevance, session_date descending."""
        sql = self._sql().upper()
        assert re.search(
            r"COALESCE\s*\(\s*DEDUP\.INTEREST_SCORE\s*,\s*1\s*\)\s+DESC", sql
        )
        assert "DEDUP.RELEVANCE_SCORE DESC" in sql
        assert "DEDUP.SESSION_DATE DESC" in sql

    def test_view_retains_all_four_joins(self):
        """All four JOINs from migration 034 must remain (plus the new group_spans JOIN)."""
        sql = self._sql().upper()
        assert "JOIN SPEAKER_TURNS ST ON STV.TURN_ID = ST.TURN_ID" in sql
        assert "JOIN VIDEO_CHAPTERS VC ON ST.CHAPTER_ID = VC.CHAPTER_ID" in sql
        assert "JOIN YOUTUBE_SOURCE_VIDEOS YSV ON VC.VIDEO_ID = YSV.VIDEO_ID" in sql
        assert re.search(
            r"JOIN\s+GROUP_SPANS\s+GS\s+ON\s+GS\.OUTPUT_PATH\s*=\s*STV\.OUTPUT_PATH", sql
        )

    @pytest.mark.parametrize("expression", ALL_034_AND_NEW_SELECT_EXPRESSIONS)
    def test_select_expression_present_in_up_section(self, expression):
        """Each of the 19 034-inherited + 2 new SELECT expressions (21 total)
        must appear in the UP section of the migration."""
        up_sql = self._up_section().upper()
        assert expression in up_sql, f"UP section must select {expression}"


class TestMigration035Hygiene:

    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_no_schema_qualification(self):
        """Migration must not use public.- or production.-qualified table names."""
        sql = self._sql()
        assert not re.search(r"\bpublic\.\w+", sql), "Must not use public.-qualified names"
        assert not re.search(r"\bproduction\.\w+", sql), "Must not use production.-qualified names"

    def test_down_block_present(self):
        """Migration must include a DOWN block comment with rollback instructions."""
        sql = self._sql().upper()
        assert "-- DOWN" in sql

    def test_down_block_restores_034_view_without_duration_gate(self):
        """DOWN section must restore the 034 view but must not reference the
        300s floor or the group_spans CTE."""
        sql = self._sql()
        down_section = sql.split("-- DOWN")[1]
        down_upper = down_section.upper()
        assert "CREATE VIEW UPLOADABLE_TURNS" in down_upper
        assert "300" not in down_section
        assert "GROUP_SPANS" not in down_upper

    def test_no_alter_table_anywhere(self):
        """Migration must be view-only — no ALTER TABLE statements."""
        sql = self._sql().upper()
        assert "ALTER TABLE" not in sql
