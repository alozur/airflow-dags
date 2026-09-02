"""Tests for production_schema.sql's base tables and uploadable_turns view
snapshot (issues #238, #299, #304).

Guards against the snapshot silently drifting from the latest applied view
migration (currently 040) and from the live production DDL for the 11
snapshotted base tables. Static SQL-text checks only — no DB connection.
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
    / "040_add_procedural_turn_filter.sql"
)

MIGRATION_038_PATH = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "sql"
    / "migrations"
    / "038_restore_chapter_abandoned_gate.sql"
)


def _normalize_view_sql(text: str, view_name: str = "UPLOADABLE_TURNS") -> str:
    """Comment-free, qualification-free, whitespace-collapsed CREATE VIEW body.

    `CREATE OR REPLACE VIEW` is rewritten to `CREATE VIEW` so a migration that
    uses one form stays comparable to a snapshot that uses the other (038 vs
    the snapshot's DROP + CREATE). No-op for uploadable_turns/040.
    """
    target = view_name.upper()
    stripped = re.sub(r"--[^\n]*", " ", text)  # also kills the DOWN block
    stripped = re.sub(r"(?i)\bproduction\.", "", stripped)
    for segment in stripped.split(";"):
        normalized = re.sub(r"\s+", " ", segment).strip().upper()
        normalized = normalized.replace("CREATE OR REPLACE VIEW ", "CREATE VIEW ")
        if f"CREATE VIEW {target}" in normalized:
            return normalized
    raise AssertionError(f"no CREATE VIEW {view_name.lower()} statement found")


def _table_block(table: str) -> str:
    """Only the `CREATE TABLE production.<table> (...)` body — never the whole file.

    Column assertions MUST be scoped to a single block: `created_at` exists in
    11 tables and `chapter_id` in 6, so a whole-file substring search would stay
    green even after a column is deleted from one specific table.

    The terminator is the first `);` after the marker. That is safe for every
    block today, but it means no table block may contain a literal `);` before
    its own closing paren — not inside a CHECK constraint, not inside a comment.
    `test_extracted_block_has_balanced_parens` guards that invariant.
    """
    sql = SCHEMA_PATH.read_text(encoding="utf-8")
    start = sql.index(f"CREATE TABLE IF NOT EXISTS production.{table} (")
    return sql[start : sql.index(");", start) + 2]


# 133 columns across 10 tables (the 8 from #299 plus video_chapters +
# youtube_source_videos, issue #304), transcribed from the live
# `\d production.<table>` DDL (canonical source), in FK-dependency order.
TABLE_COLUMNS: dict[str, tuple[str, ...]] = {
    "youtube_source_videos": (
        "video_id", "video_title", "video_url", "session_number",
        "session_date", "duration_seconds", "published_at", "channel_id",
        "is_processed", "total_chapters", "created_at", "updated_at",
        "download_retry_after",
    ),
    "video_chapters": (
        "chapter_id", "video_id", "title", "description", "start_time",
        "end_time", "duration_minutes", "speakers", "topics",
        "relevance_score", "speaker_relevance_points",
        "topic_relevance_points", "public_interest_points",
        "scoring_reasoning", "key_speakers", "is_current_topic",
        "scoring_error", "scored_at", "is_uploaded_to_youtube",
        "youtube_video_id", "youtube_upload_date", "created_at", "updated_at",
        "timeline", "upload_attempts", "is_upload_abandoned",
        "last_upload_error", "resolved_participant_slug", "turns_detected_at",
        "upload_verified_at",
    ),
    "llm_cache": ("cache_key", "model", "response", "created_at"),
    "congress_participants": (
        "normalized_name", "display_name", "party", "parliamentary_group",
        "constituency", "biography", "full_membership_date", "start_date",
        "group_entry_date", "photo_url", "created_at", "updated_at",
        "nickname", "slug",
    ),
    "speaker_normalization_cache": (
        "id", "chapter_id", "dirty_speaker", "canonical_speaker",
        "participant_normalized_name", "status", "confidence_score",
        "created_at", "updated_at",
    ),
    "video_thumbnails": (
        "thumbnail_id", "chapter_id", "youtube_video_id", "label", "style",
        "prompt", "main_score", "local_path", "output_url", "openai_title",
        "is_chosen", "created_at", "archetype",
    ),
    "speaker_turns": (
        "turn_id", "chapter_id", "start_seconds", "end_seconds",
        "speaker_label", "resolved_name", "confidence", "source",
        "created_at", "updated_at", "interest_score", "is_procedural",
        "procedural_reason",
    ),
    "speaker_turn_trim_proposals": (
        "proposal_id", "turn_id", "start_seconds", "end_seconds", "tipo",
        "score", "source", "is_voice_free", "created_at", "updated_at",
        "is_approved", "approved_at",
    ),
    "speaker_turn_videos": (
        "video_id", "turn_id", "output_path", "materialized_at",
        "is_uploaded_to_youtube", "youtube_video_id", "youtube_upload_date",
        "prepared_at", "upload_verified_at", "upload_attempts",
        "is_upload_abandoned", "last_upload_error", "turn_type",
        "resolved_participant_slug", "speaker_resolution_confidence",
        "speaker_resolution_method", "keep_intervals",
        "thumbnail_republish_needed_at", "thumbnail_republished_at",
        "thumbnail_republish_attempts", "thumbnail_republish_abandoned",
        "last_thumbnail_republish_error",
    ),
    "video_analytics_snapshots": (
        "snapshot_id", "chapter_id", "youtube_video_id", "checkpoint",
        "metrics", "action_taken", "collected_at", "action_detail",
    ),
}

# Every FK in every snapshotted block must be production.-qualified. Fragments
# are matched against the whitespace-collapsed, uppercased block.
FK_QUALIFICATIONS: tuple[tuple[str, str], ...] = (
    ("video_chapters", "REFERENCES PRODUCTION.YOUTUBE_SOURCE_VIDEOS(VIDEO_ID) ON DELETE CASCADE"),
    ("video_chapters", "REFERENCES PRODUCTION.CONGRESS_PARTICIPANTS(SLUG)"),
    ("speaker_normalization_cache", "REFERENCES PRODUCTION.VIDEO_CHAPTERS(CHAPTER_ID) ON DELETE CASCADE"),
    ("speaker_normalization_cache", "REFERENCES PRODUCTION.CONGRESS_PARTICIPANTS(NORMALIZED_NAME)"),
    ("video_thumbnails", "REFERENCES PRODUCTION.VIDEO_CHAPTERS(CHAPTER_ID) ON DELETE CASCADE"),
    ("speaker_turns", "REFERENCES PRODUCTION.VIDEO_CHAPTERS(CHAPTER_ID) ON DELETE CASCADE"),
    ("speaker_turn_trim_proposals", "REFERENCES PRODUCTION.SPEAKER_TURNS(TURN_ID) ON DELETE CASCADE"),
    ("speaker_turn_videos", "REFERENCES PRODUCTION.SPEAKER_TURNS(TURN_ID) ON DELETE CASCADE"),
    ("speaker_turn_videos", "REFERENCES PRODUCTION.CONGRESS_PARTICIPANTS(SLUG)"),
    ("video_analytics_snapshots", "REFERENCES PRODUCTION.VIDEO_CHAPTERS(CHAPTER_ID) ON DELETE CASCADE"),
)


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
            r"WHERE\s+DEDUP\.GROUP_END_SECONDS\s*-\s*DEDUP\.GROUP_START_SECONDS"
            r"\s*-\s*DEDUP\.PROCEDURAL_SECONDS\s*>=\s*300",
            sql,
        ), "Outer WHERE must gate on group span minus procedural_seconds >= 300 (issue #143)"


class TestUploadableTurns040ProceduralGate:
    """040: is_procedural exclusion + procedural_seconds floor adjustment
    (issue #143)."""

    @staticmethod
    def _sql() -> str:
        return SCHEMA_PATH.read_text(encoding="utf-8")

    def test_inner_where_excludes_procedural_turns(self):
        sql = self._sql().upper()
        assert re.search(
            r"NOT\s+COALESCE\s*\(\s*ST\.IS_PROCEDURAL\s*,\s*FALSE\s*\)", sql
        ), "Snapshot must gate on NOT COALESCE(st.is_procedural, FALSE)"

    def test_cte_computes_procedural_seconds(self):
        sql = self._sql().upper()
        assert re.search(
            r"SUM\s*\(\s*CASE\s+WHEN\s+ST\.IS_PROCEDURAL\s+THEN\s+"
            r"ST\.END_SECONDS\s*-\s*ST\.START_SECONDS\s+ELSE\s+0\s+END\s*\)\s+"
            r"AS\s+PROCEDURAL_SECONDS",
            sql,
        )

    def test_procedural_seconds_column_selected(self):
        sql = self._sql().upper()
        assert "GS.PROCEDURAL_SECONDS" in sql


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


class TestUploadableTurnsUnaffectedByThumbnailRepublish:
    """Migration 042 (issue #331) is purely additive on speaker_turn_videos
    and must not touch the uploadable_turns view — it gates on
    is_uploaded_to_youtube = FALSE, and every thumbnail-republish healer
    candidate has that column TRUE, so the view is structurally
    unaffected (design DD1)."""

    def test_view_block_has_no_thumbnail_republish_token(self):
        block = TestProductionQualification._view_block().upper()
        assert "THUMBNAIL_REPUBLISH" not in block


class TestVideoShortsTableSnapshot:
    """production.video_shorts must be present in the snapshot, folding
    migrations 004 (create) + 005 (staged_clip_path) + 006 (scoring_reasoning)
    + 012 (upload failure tracking) — 20 columns total (issue #275).

    Column assertions are scoped to the extracted `CREATE TABLE ... (...)`
    block only, never the whole file: 9 of the 20 column names also exist on
    `production.video_chapters`, so a whole-file substring search would stay
    green even if a column were deleted from `video_shorts` alone.
    """

    VIDEO_SHORTS_COLUMNS = (
        "id",
        "chapter_id",
        "pretrim_start_secs",
        "pretrim_end_secs",
        "pretrim_used_srt",
        "reap_project_id",
        "reap_clip_id",
        "reap_status",
        "reap_virality_score",
        "reap_clip_url",
        "local_file_path",
        "youtube_video_id",
        "is_uploaded",
        "created_at",
        "updated_at",
        "staged_clip_path",
        "scoring_reasoning",
        "upload_attempts",
        "is_upload_abandoned",
        "last_upload_error",
    )

    @staticmethod
    def _video_shorts_block() -> str:
        """Only the `CREATE TABLE video_shorts (...)` body — never the whole file."""
        return _table_block("video_shorts")

    @pytest.mark.parametrize("column", VIDEO_SHORTS_COLUMNS)
    def test_column_present_in_block(self, column):
        block = self._video_shorts_block().upper()
        assert re.search(rf"\b{column.upper()}\b", block), (
            f"video_shorts column {column!r} missing from the extracted "
            "CREATE TABLE block"
        )

    def test_chapter_id_fk_is_production_qualified(self):
        block = re.sub(r"\s+", " ", self._video_shorts_block()).upper()
        assert "REFERENCES PRODUCTION.VIDEO_CHAPTERS(CHAPTER_ID) ON DELETE CASCADE" in block, (
            "video_shorts.chapter_id must reference production.video_chapters "
            "with an explicit schema qualification"
        )


class TestRemainingBaseTableSnapshots:
    """10 base tables — the 8 added by issue #299 plus video_chapters and
    youtube_source_videos (issue #304) — must be present in the snapshot,
    transcribed from the live `\\d production.<table>` DDL — 133 columns
    total.

    Same block-scoping discipline as TestVideoShortsTableSnapshot: assertions
    run against the extracted CREATE TABLE block, never the whole file.
    """

    @pytest.mark.parametrize(
        "table,column",
        [(t, c) for t, cols in TABLE_COLUMNS.items() for c in cols],
    )
    def test_column_present_in_block(self, table, column):
        block = _table_block(table).upper()
        assert re.search(rf"\b{column.upper()}\b", block), (
            f"{table} column {column!r} missing from the extracted "
            "CREATE TABLE block"
        )

    @pytest.mark.parametrize("table,references", FK_QUALIFICATIONS)
    def test_fk_is_production_qualified(self, table, references):
        block = re.sub(r"\s+", " ", _table_block(table)).upper()
        assert references in block, (
            f"{table} must carry an explicitly schema-qualified FK: {references}"
        )

    @pytest.mark.parametrize("table", (*TABLE_COLUMNS, "video_shorts"))
    def test_extracted_block_has_balanced_parens(self, table):
        """Guards the `);` terminator: a premature match truncates the block
        and would silently weaken every assertion above."""
        body = re.sub(r"--[^\n]*", "", _table_block(table))
        assert body.count("(") == body.count(")"), (
            f"{table} block extraction stopped early — a literal '); ' appears "
            "before the table's own closing paren"
        )


class TestVideoChaptersIndexCompleteness:
    """The INDEXES section must contain a CREATE [UNIQUE] INDEX statement for
    each of the three video_chapters indexes added by this change. Column
    presence in the CREATE TABLE block does not guarantee the index
    statement itself wasn't dropped from the INDEXES section (issue #304)."""

    @pytest.mark.parametrize(
        "index_name",
        (
            "uq_video_chapters_segment",
            "idx_video_chapters_resolved_participant_slug",
            "idx_video_chapters_pending_priority",
        ),
    )
    def test_index_statement_present(self, index_name):
        sql = SCHEMA_PATH.read_text(encoding="utf-8")
        assert re.search(
            rf"CREATE\s+(?:UNIQUE\s+)?INDEX\s+{index_name}\s+ON\s+production\.video_chapters",
            sql,
            re.IGNORECASE,
        ), f"missing CREATE INDEX statement for {index_name} on production.video_chapters"


class TestSnapshotLockstepWithLatestMigration:
    """The snapshot's uploadable_turns view must be semantically identical to
    the latest applied view migration (040), modulo comments/qualification/
    whitespace."""

    def test_normalized_view_matches_migration_040(self):
        snapshot_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        migration_sql = MIGRATION_PATH.read_text(encoding="utf-8")

        assert _normalize_view_sql(snapshot_sql) == _normalize_view_sql(migration_sql), (
            "production_schema.sql's uploadable_turns view has drifted from "
            "migration 040 — update the snapshot to stay in lockstep"
        )


class TestChaptersSnapshotLockstepWithMigration038:
    """The snapshot's uploadable_chapters view must be semantically identical
    to the latest migration that touches it (038), modulo comments/
    qualification/whitespace.

    This view drifted silently three times — 021 dropped the abandon gate, 036
    carried the drop forward, and the snapshot's ORDER BY never picked up
    migration 007's leading session_date key — with no test catching any of it
    (issue #304). uploadable_turns got this guard in #299; this is its sibling.
    """

    def test_normalized_view_matches_migration_038(self):
        snapshot_sql = SCHEMA_PATH.read_text(encoding="utf-8")
        migration_sql = MIGRATION_038_PATH.read_text(encoding="utf-8")

        assert _normalize_view_sql(
            snapshot_sql, "UPLOADABLE_CHAPTERS"
        ) == _normalize_view_sql(migration_sql, "UPLOADABLE_CHAPTERS"), (
            "production_schema.sql's uploadable_chapters view has drifted from "
            "migration 038 — update the snapshot to stay in lockstep"
        )
