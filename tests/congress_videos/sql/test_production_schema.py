"""Tests for production_schema.sql's uploadable_turns view snapshot (issue #238).

Guards against the snapshot silently drifting from the latest applied view
migration (currently 035). Static SQL-text checks only — no DB connection.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

SCHEMA_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "production_schema.sql"
)

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "035_add_min_turn_duration.sql"
)


def _normalize_view_sql(text: str) -> str:
    """Comment-free, qualification-free, whitespace-collapsed CREATE VIEW body."""
    stripped = re.sub(r"--[^\n]*", " ", text)  # also kills the DOWN block
    stripped = re.sub(r"(?i)\bproduction\.", "", stripped)
    for segment in stripped.split(";"):
        normalized = re.sub(r"\s+", " ", segment).strip().upper()
        if "CREATE VIEW UPLOADABLE_TURNS" in normalized:
            return normalized
    raise AssertionError("no CREATE VIEW uploadable_turns statement found")


class TestProductionSchemaFileExists:

    def test_schema_file_exists(self):
        """production_schema.sql must exist at the expected path."""
        assert SCHEMA_PATH.exists(), f"Schema file not found: {SCHEMA_PATH}"


class TestUploadableTurnsGates:
    """Snapshot's uploadable_turns view WHERE clause must carry every gate up
    through migration 035."""

    @staticmethod
    def _sql() -> str:
        return SCHEMA_PATH.read_text(encoding="utf-8")

    def test_retains_is_upload_abandoned_gate(self):
        """032: NOT stv.is_upload_abandoned gate must be present."""
        sql = self._sql().upper()
        assert re.search(r"NOT\s+STV\.IS_UPLOAD_ABANDONED", sql)

    def test_retains_prepared_at_gate(self):
        """030: stv.prepared_at IS NOT NULL readiness gate must be present."""
        sql = self._sql().upper()
        assert re.search(r"STV\.PREPARED_AT\s+IS\s+NOT\s+NULL", sql)

    def test_retains_distinct_on_output_path(self):
        """028: DISTINCT ON (stv.output_path) grouped-turn dedup must be present."""
        sql = self._sql().upper()
        assert "DISTINCT ON (STV.OUTPUT_PATH)" in sql

    def test_retains_relevance_score_gate(self):
        """vc.relevance_score >= 2 gate must be present."""
        sql = self._sql().upper()
        assert re.search(r"VC\.RELEVANCE_SCORE\s*>=\s*2", sql)

    def test_retains_interest_score_gate(self):
        """COALESCE(st.interest_score, 1) >= 1 gate must be present."""
        sql = self._sql().upper()
        assert re.search(r"COALESCE\s*\(\s*ST\.INTEREST_SCORE\s*,\s*1\s*\)\s*>=\s*1", sql)


class TestUploadableTurns034Columns:
    """034: speaker-resolution columns must be exposed by the snapshot's view."""

    @pytest.mark.parametrize(
        "column",
        [
            "resolved_participant_slug",
            "speaker_resolution_confidence",
            "speaker_resolution_method",
        ],
    )
    def test_column_present(self, column):
        sql = SCHEMA_PATH.read_text(encoding="utf-8").upper()
        assert column.upper() in sql, f"Snapshot must select {column}"


class TestUploadableTurns035GroupSpans:
    """035: group_spans CTE and 300s minimum grouped-clip duration floor."""

    @staticmethod
    def _sql() -> str:
        return SCHEMA_PATH.read_text(encoding="utf-8")

    def test_defines_group_spans_cte(self):
        sql = self._sql().upper()
        assert "WITH GROUP_SPANS AS (" in sql

    def test_cte_computes_min_start_and_max_end(self):
        sql = self._sql().upper()
        assert re.search(
            r"MIN\s*\(\s*ST\.START_SECONDS\s*\)\s+AS\s+GROUP_START_SECONDS", sql
        ), "CTE must compute MIN(st.start_seconds) AS group_start_seconds"
        assert re.search(
            r"MAX\s*\(\s*ST\.END_SECONDS\s*\)\s+AS\s+GROUP_END_SECONDS", sql
        ), "CTE must compute MAX(st.end_seconds) AS group_end_seconds"

    def test_cte_groups_by_output_path(self):
        sql = self._sql().upper()
        assert re.search(r"GROUP\s+BY\s+STV\.OUTPUT_PATH", sql)

    def test_joins_group_spans_back_on_output_path(self):
        sql = self._sql().upper()
        assert re.search(
            r"JOIN\s+GROUP_SPANS\s+GS\s+ON\s+GS\.OUTPUT_PATH\s*=\s*STV\.OUTPUT_PATH", sql
        ), "Must JOIN group_spans gs ON gs.output_path = stv.output_path"

    def test_outer_where_enforces_300_second_floor(self):
        sql = self._sql().upper()
        assert re.search(
            r"WHERE\s+DEDUP\.GROUP_END_SECONDS\s*-\s*DEDUP\.GROUP_START_SECONDS\s*>=\s*300",
            sql,
        ), "Outer WHERE must gate on dedup.group_end_seconds - dedup.group_start_seconds >= 300"


class TestProductionQualification:
    """Every base-table FROM/JOIN in the snapshot's view must be production.-qualified;
    aliases and the group_spans CTE reference stay bare."""

    @staticmethod
    def _view_block() -> str:
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        marker = "-- View: uploadable_turns"
        start = sql.index(marker)
        end = sql.index("COMMENT ON VIEW production.uploadable_turns", start)
        return sql[start:end]

    def test_no_bare_from_speaker_turn_videos(self):
        block = self._view_block().upper()
        assert not re.search(r"FROM\s+SPEAKER_TURN_VIDEOS\b", block), (
            "Base table speaker_turn_videos must be production.-qualified"
        )

    @pytest.mark.parametrize(
        "table",
        [
            "speaker_turn_videos",
            "speaker_turns",
            "video_chapters",
            "youtube_source_videos",
        ],
    )
    def test_base_table_is_qualified(self, table):
        block = self._view_block()
        assert f"production.{table}" in block, (
            f"Base table {table} must appear qualified as production.{table}"
        )

    def test_group_spans_cte_reference_stays_unqualified(self):
        block = self._view_block().upper()
        assert "PRODUCTION.GROUP_SPANS" not in block, (
            "group_spans is a CTE reference, not a base table — must stay unqualified"
        )
        assert "JOIN GROUP_SPANS GS" in block


class TestSnapshotLockstepWithLatestMigration:
    """The snapshot's uploadable_turns view must be semantically identical to
    the latest applied view migration (035), modulo comments/qualification/
    whitespace."""

    def test_normalized_view_matches_migration_035(self):
        snapshot_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")

        assert _normalize_view_sql(snapshot_sql) == _normalize_view_sql(migration_sql), (
            "production_schema.sql's uploadable_turns view has drifted from "
            "migration 035 — update the snapshot to stay in lockstep"
        )
