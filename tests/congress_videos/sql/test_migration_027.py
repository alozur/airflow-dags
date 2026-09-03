"""[RED] Test: migration 027 — add turn upload tracking columns + uploadable_turns view.

Reads the SQL file statically and asserts structural properties:
file exists, ADD COLUMN IF NOT EXISTS for the three tracking columns,
CREATE VIEW uploadable_turns with required filters and ORDER BY clause.
No DB connection required.
"""

from __future__ import annotations

import re
from pathlib import Path

MIGRATION_PATH = (
    Path(__file__).resolve().parents[3] / "congress_videos" / "sql" / "migrations" / "027_add_turn_upload_tracking.sql"
)


class TestMigration027FileExists:
    def test_migration_file_exists(self):
        """Migration file must exist at the expected path."""
        assert MIGRATION_PATH.exists(), f"Migration file not found: {MIGRATION_PATH}"


class TestMigration027TrackingColumns:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_adds_is_uploaded_to_youtube_column_if_not_exists(self):
        """Migration must use ADD COLUMN IF NOT EXISTS for is_uploaded_to_youtube."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS IS_UPLOADED_TO_YOUTUBE" in sql

    def test_is_uploaded_to_youtube_boolean_not_null_default_false(self):
        """is_uploaded_to_youtube must be BOOLEAN NOT NULL DEFAULT FALSE."""
        sql = self._sql().upper()
        assert re.search(
            r"IS_UPLOADED_TO_YOUTUBE\s+BOOLEAN\s+NOT\s+NULL\s+DEFAULT\s+FALSE",
            sql,
        ), "is_uploaded_to_youtube must be BOOLEAN NOT NULL DEFAULT FALSE"

    def test_adds_youtube_video_id_column_if_not_exists(self):
        """Migration must use ADD COLUMN IF NOT EXISTS for youtube_video_id."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS YOUTUBE_VIDEO_ID" in sql

    def test_adds_youtube_upload_date_column_if_not_exists(self):
        """Migration must use ADD COLUMN IF NOT EXISTS for youtube_upload_date."""
        sql = self._sql().upper()
        assert "ADD COLUMN IF NOT EXISTS YOUTUBE_UPLOAD_DATE" in sql

    def test_youtube_upload_date_is_timestamptz(self):
        """youtube_upload_date must be TIMESTAMPTZ."""
        sql = self._sql().upper()
        assert re.search(r"YOUTUBE_UPLOAD_DATE\s+TIMESTAMPTZ", sql), "youtube_upload_date must be TIMESTAMPTZ"

    def test_alters_speaker_turn_videos_table(self):
        """Migration must ALTER TABLE speaker_turn_videos."""
        sql = self._sql().upper()
        assert "ALTER TABLE SPEAKER_TURN_VIDEOS" in sql

    def test_idempotent_on_reruns(self):
        """All ADD COLUMN statements must use IF NOT EXISTS for idempotency."""
        sql = self._sql().upper()
        # All ADD COLUMN occurrences must include IF NOT EXISTS
        add_column_matches = re.findall(r"ADD COLUMN(?:\s+IF\s+NOT\s+EXISTS)?", sql)
        for match in add_column_matches:
            assert "IF NOT EXISTS" in match, f"Found ADD COLUMN without IF NOT EXISTS guard: {match!r}"


class TestMigration027UploadableTurnsView:
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

    def test_view_filters_stv_not_uploaded(self):
        """View must filter speaker_turn_videos.is_uploaded_to_youtube = FALSE."""
        sql = self._sql().upper()
        assert "IS_UPLOADED_TO_YOUTUBE" in sql
        assert "FALSE" in sql

    def test_view_filters_chapters_not_uploaded(self):
        """View must also filter video_chapters.is_uploaded_to_youtube = FALSE (Guard B)."""
        sql = self._sql()
        # Both stv and vc are joined; vc.is_uploaded_to_youtube = FALSE must appear
        assert re.search(r"vc\.is_uploaded_to_youtube\s*=\s*FALSE", sql, re.IGNORECASE), (
            "View must filter vc.is_uploaded_to_youtube = FALSE (Guard B)"
        )

    def test_view_filters_relevance_score_gte_2(self):
        """View must filter relevance_score >= 2."""
        sql = self._sql()
        assert re.search(r"relevance_score\s*>=\s*2", sql, re.IGNORECASE), "View must filter relevance_score >= 2"

    def test_view_orders_by_relevance_desc_then_session_date_desc(self):
        """View must ORDER BY relevance_score DESC, session_date DESC (spec ordering)."""
        sql = self._sql().upper()
        assert re.search(r"ORDER\s+BY.*RELEVANCE_SCORE\s+DESC", sql, re.DOTALL), (
            "View must ORDER BY relevance_score DESC"
        )
        assert re.search(r"SESSION_DATE\s+DESC", sql), "View must include SESSION_DATE DESC as secondary ORDER BY"

    def test_view_joins_speaker_turn_videos(self):
        """View must JOIN speaker_turn_videos."""
        sql = self._sql().upper()
        assert "SPEAKER_TURN_VIDEOS" in sql

    def test_view_joins_speaker_turns(self):
        """View must JOIN speaker_turns."""
        sql = self._sql().upper()
        assert "SPEAKER_TURNS" in sql

    def test_view_joins_video_chapters(self):
        """View must JOIN video_chapters."""
        sql = self._sql().upper()
        assert "VIDEO_CHAPTERS" in sql

    def test_view_joins_youtube_source_videos(self):
        """View must JOIN youtube_source_videos for session_date."""
        sql = self._sql().upper()
        assert "YOUTUBE_SOURCE_VIDEOS" in sql

    def test_view_exposes_turn_id(self):
        """View must expose turn_id column."""
        sql = self._sql().upper()
        assert "TURN_ID" in sql

    def test_view_exposes_resolved_name(self):
        """View must expose resolved_name column."""
        sql = self._sql().upper()
        assert "RESOLVED_NAME" in sql

    def test_view_exposes_start_seconds_and_end_seconds(self):
        """View must expose start_seconds and end_seconds."""
        sql = self._sql().upper()
        assert "START_SECONDS" in sql
        assert "END_SECONDS" in sql


class TestMigration027DownBlock:
    @staticmethod
    def _sql() -> str:
        return MIGRATION_PATH.read_text(encoding="utf-8")

    def test_down_block_drops_view(self):
        """DOWN block must drop the uploadable_turns view."""
        sql = self._sql().upper()
        drop_view_lines = [line for line in sql.splitlines() if "UPLOADABLE_TURNS" in line and "DROP" in line]
        assert drop_view_lines, "DOWN block must DROP VIEW uploadable_turns"
