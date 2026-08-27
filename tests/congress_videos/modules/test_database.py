"""Tests for CongressionalVideoDB database operations — TASK-021."""

from __future__ import annotations

import logging
import os
from datetime import date
from unittest.mock import MagicMock, call, patch

import pytest

# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    """Provide minimal env vars so PostgresConnection.__init__ does not raise."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def db(mocker):
    """Return a (CongressionalVideoDB, mock_cursor) pair with DB fully mocked."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)

    from congress_videos.modules.database import CongressionalVideoDB
    instance = CongressionalVideoDB()
    return instance, mock_cursor


# --------------------------------------------------------------------------- #
# save_youtube_chapters_to_db
# --------------------------------------------------------------------------- #

class TestSaveYoutubeChaptersToDB:

    def test_empty_input_returns_zero_counts(self, db):
        """Empty dict input returns all-zero result without touching DB."""
        instance, mock_cursor = db

        result = instance.save_youtube_chapters_to_db({})

        assert result["total_videos_saved"] == 0
        assert result["total_chapters_saved"] == 0
        assert result["videos"] == []
        mock_cursor.execute.assert_not_called()

    def test_none_input_returns_zero_counts(self, db):
        """None input returns all-zero result."""
        instance, mock_cursor = db

        result = instance.save_youtube_chapters_to_db(None)

        assert result["total_videos_saved"] == 0

    def test_video_with_error_is_skipped(self, db):
        """Videos with error key are skipped and included in results with error."""
        instance, mock_cursor = db

        data = {
            "total_videos": 1,
            "videos": [
                {
                    "video_id": "vid1",
                    "video_title": "Test",
                    "scored_chapters": [],
                    "error": "timeout",
                }
            ],
        }
        result = instance.save_youtube_chapters_to_db(data)

        assert result["total_videos_saved"] == 0
        assert result["videos"][0]["error"] == "timeout"

    def test_single_video_with_two_chapters_saved(self, db):
        """Valid video with 2 chapters: 1 video INSERT + 2 chapter INSERTs."""
        instance, mock_cursor = db
        # The video upsert has NO fetchone() call.
        # Only chapter INSERTs call fetchone() with RETURNING chapter_id.
        call_count = [0]
        responses = [
            {"chapter_id": 1},   # chapter 1 INSERT RETURNING
            {"chapter_id": 2},   # chapter 2 INSERT RETURNING
        ]

        def _fetchone_side_effect():
            idx = call_count[0]
            call_count[0] += 1
            if idx < len(responses):
                return responses[idx]
            return None

        mock_cursor.fetchone.side_effect = _fetchone_side_effect

        chapter_base = {
            "title": "Chapter",
            "description": "Desc",
            "start_time": "00:00:00",
            "end_time": "00:10:00",
            "duration_minutes": 10.0,
            "speakers": ["A"],
            "topics": ["X"],
            "relevance_score": 4,
            "speaker_relevance_points": 1,
            "topic_relevance_points": 2,
            "public_interest_points": 1,
            "scoring_reasoning": "Good",
            "key_speakers": ["A"],
            "is_current_topic": True,
            "scoring_error": None,
        }
        data = {
            "total_videos": 1,
            "videos": [
                {
                    "video_id": "vid-abc",
                    "video_title": "Plenary Session",
                    "scored_chapters": [
                        {**chapter_base, "title": "Ch1"},
                        {**chapter_base, "title": "Ch2"},
                    ],
                }
            ],
        }
        result = instance.save_youtube_chapters_to_db(data)

        assert result["total_videos_saved"] == 1
        assert result["total_chapters_saved"] == 2
        assert result["videos"][0]["chapters_saved"] == 2
        assert result["videos"][0]["error"] is None

    def test_no_videos_key_returns_zero_counts(self, db):
        """Input dict with no 'videos' key returns zero counts."""
        instance, mock_cursor = db

        result = instance.save_youtube_chapters_to_db({"total_videos": 0})

        assert result["total_videos_saved"] == 0
        assert result["total_chapters_saved"] == 0

    def test_result_keys_always_present(self, db):
        """Return dict always has required keys even on empty input."""
        instance, mock_cursor = db

        result = instance.save_youtube_chapters_to_db(None)

        assert "total_videos_saved" in result
        assert "total_chapters_saved" in result
        assert "videos" in result


# --------------------------------------------------------------------------- #
# get_uploadable_chapters
# --------------------------------------------------------------------------- #

class TestGetUploadableChapters:

    def test_returns_chapters_with_default_score(self, db):
        """Default min_relevance_score=4 is passed to query params."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [{"chapter_id": 1, "relevance_score": 5}]

        result = instance.get_uploadable_chapters()

        assert len(result) == 1
        _, params = mock_cursor.execute.call_args[0]
        assert params == (4,)

    def test_returns_chapters_with_limit(self, db):
        """LIMIT clause is appended when limit is given."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_uploadable_chapters(limit=3, min_relevance_score=3)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" in sql

    def test_returns_empty_list_when_no_chapters(self, db):
        """Returns empty list when view has no matching rows."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_uploadable_chapters()

        assert result == []

    def test_no_limit_when_limit_is_none(self, db):
        """LIMIT is NOT added when limit=None."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_uploadable_chapters(limit=None)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" not in sql


# --------------------------------------------------------------------------- #
# mark_chapter_uploaded
# --------------------------------------------------------------------------- #

class TestMarkChapterUploaded:

    def test_executes_update_with_correct_params(self, db):
        """UPDATE sets is_uploaded_to_youtube, youtube_video_id for chapter_id."""
        instance, mock_cursor = db

        instance.mark_chapter_uploaded(chapter_id=7, youtube_video_id="yt-chap-001")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "is_uploaded_to_youtube" in sql
        assert params == ("yt-chap-001", 7)

    def test_parameterized_no_injection(self, db):
        """youtube_video_id and chapter_id are params, not in SQL string."""
        instance, mock_cursor = db

        instance.mark_chapter_uploaded(chapter_id=42, youtube_video_id="yt-safe-id")

        sql, params = mock_cursor.execute.call_args[0]
        assert "yt-safe-id" not in sql
        assert "yt-safe-id" in params


# --------------------------------------------------------------------------- #
# record_chapter_upload_failure
# --------------------------------------------------------------------------- #

class TestRecordChapterUploadFailure:

    def test_normal_increment_updates_attempts_and_error(self, db):
        """Non-threshold-crossing failure increments upload_attempts, stores error."""
        instance, mock_cursor = db

        instance.record_chapter_upload_failure(chapter_id=7, error_message="quota exceeded")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "upload_attempts = upload_attempts + 1" in sql
        assert "is_upload_abandoned" in sql
        assert "last_upload_error" in sql
        assert params == ("quota exceeded", 7)

    def test_threshold_crossing_sets_abandoned_condition(self, db):
        """SQL encodes the >= 3 (CHAPTER_UPLOAD_ABANDON_THRESHOLD) abandon condition."""
        instance, mock_cursor = db

        instance.record_chapter_upload_failure(chapter_id=9, error_message="timeout")

        sql, _ = mock_cursor.execute.call_args[0]
        assert ">= 3" in sql

    def test_error_message_none_path(self, db):
        """error_message=None is passed through as a None param, not a crash."""
        instance, mock_cursor = db

        instance.record_chapter_upload_failure(chapter_id=11, error_message=None)

        sql, params = mock_cursor.execute.call_args[0]
        assert params == (None, 11)

    def test_warning_logged_when_threshold_crossed_on_this_call(self, db, caplog):
        """A distinct WARNING fires when this call is the one crossing the abandon threshold."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 3,
            "is_upload_abandoned": True,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_chapter_upload_failure(chapter_id=42, error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "42" in warnings[0].message
        assert "abandoned" in warnings[0].message.lower()

    def test_no_warning_logged_for_ordinary_retry_increment(self, db, caplog):
        """An ordinary (non-crossing) failure increment does not emit the abandonment WARNING."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 1,
            "is_upload_abandoned": False,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_chapter_upload_failure(chapter_id=43, error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 0


# --------------------------------------------------------------------------- #
# count_chapters_uploaded_today
# --------------------------------------------------------------------------- #

class TestCountChaptersUploadedToday:

    def test_returns_zero_when_no_uploads_today(self, db):
        """Returns 0 when no chapters have youtube_upload_date today."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 0}

        result = instance.count_chapters_uploaded_today()

        assert result == 0

    def test_returns_count_when_uploads_exist(self, db):
        """Returns N when N chapters were uploaded today."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 2}

        result = instance.count_chapters_uploaded_today()

        assert result == 2

    def test_query_filters_by_current_date(self, db):
        """SQL uses CURRENT_DATE to filter youtube_upload_date."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 0}

        instance.count_chapters_uploaded_today()

        sql = mock_cursor.execute.call_args[0][0]
        assert "youtube_upload_date" in sql
        assert "CURRENT_DATE" in sql

    def test_returns_zero_when_fetchone_none(self, db):
        """Returns 0 gracefully when fetchone returns None."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.count_chapters_uploaded_today()

        assert result == 0


# --------------------------------------------------------------------------- #
# count_turns_uploaded_today
# --------------------------------------------------------------------------- #

class TestCountTurnsUploadedToday:

    def test_query_counts_distinct_output_path(self, db):
        """SQL uses COUNT(DISTINCT output_path), not COUNT(*) (issue #244)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 0}

        instance.count_turns_uploaded_today()

        sql = mock_cursor.execute.call_args[0][0]
        assert "COUNT(DISTINCT" in sql
        assert "output_path" in sql
        assert "youtube_upload_date" in sql
        assert "CURRENT_DATE" in sql

    def test_grouped_siblings_count_once(self, db):
        """N>1 rows sharing one output_path still count as 1 distinct video."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 1}

        result = instance.count_turns_uploaded_today()

        assert result == 1

    def test_two_distinct_videos_count_as_two(self, db):
        """Rows spanning exactly 2 distinct output_path values return 2."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 2}

        result = instance.count_turns_uploaded_today()

        assert result == 2

    def test_returns_zero_when_fetchone_none(self, db):
        """Returns 0 gracefully when fetchone returns None."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.count_turns_uploaded_today()

        assert result == 0


# --------------------------------------------------------------------------- #
# count_pending_uploadable_chapters
# --------------------------------------------------------------------------- #

class TestCountPendingUploadableChapters:

    def test_returns_count_with_default_min_score(self, db):
        """Returns pending count using default min_relevance_score=2."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 7}

        result = instance.count_pending_uploadable_chapters()

        assert result == 7
        params = mock_cursor.execute.call_args[0][1]
        assert params == (2,)

    def test_respects_custom_min_score(self, db):
        """Passes custom min_relevance_score to query."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"count": 3}

        instance.count_pending_uploadable_chapters(min_relevance_score=4)

        params = mock_cursor.execute.call_args[0][1]
        assert params == (4,)

    def test_returns_zero_when_fetchone_none(self, db):
        """Returns 0 gracefully when fetchone returns None."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.count_pending_uploadable_chapters()

        assert result == 0


# --------------------------------------------------------------------------- #
# get_chapter_metadata — session_number / session_date via LEFT JOIN (task 4.2)
# --------------------------------------------------------------------------- #

class TestGetChapterMetadataSessionData:

    def test_sql_contains_left_join_and_session_columns(self, db):
        """The SQL issued by get_chapter_metadata must include LEFT JOIN and session columns."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(chapter_id=1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql
        assert "session_number" in sql
        assert "session_date" in sql

    def test_returns_none_when_chapter_not_found(self, db):
        """Returns None for a missing chapter_id (unchanged behaviour)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.get_chapter_metadata(chapter_id=9999)

        assert result is None

    def test_returns_session_data_when_linked_row_present(self, db):
        """Returned dict contains session_number and session_date keys from the join."""
        from datetime import date as dt_date

        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 5,
            "title": "Test chapter",
            "description": "desc",
            "speakers": ["Alice"],
            "key_speakers": ["Alice"],
            "topics": ["topic1"],
            "scoring_reasoning": "high",
            "relevance_score": 4,
            "source_video_title": "Sesion 80",
            "source_video_url": "https://youtube.com/watch?v=xyz",
            "session_number": 80,
            "session_date": dt_date(2024, 6, 10),
        }

        result = instance.get_chapter_metadata(chapter_id=5)

        assert result is not None
        assert result["session_number"] == 80
        assert result["session_date"] == dt_date(2024, 6, 10)

    def test_returns_none_session_fields_when_no_linked_video(self, db):
        """session_number and session_date are None when no youtube_source_videos row matches."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 7,
            "title": "Orphan chapter",
            "description": "desc",
            "speakers": [],
            "key_speakers": [],
            "topics": [],
            "scoring_reasoning": "",
            "relevance_score": 2,
            "source_video_title": None,
            "source_video_url": None,
            "session_number": None,
            "session_date": None,
        }

        result = instance.get_chapter_metadata(chapter_id=7)

        assert result is not None
        assert result["session_number"] is None
        assert result["session_date"] is None


# --------------------------------------------------------------------------- #
# get_processed_video_ids — idempotency pre-download lookup
# --------------------------------------------------------------------------- #

class TestGetProcessedVideoIds:

    def test_empty_input_returns_empty_set_without_querying(self, db):
        """Empty input -> empty set and the DB is never touched."""
        instance, mock_cursor = db

        result = instance.get_processed_video_ids([])

        assert result == set()
        mock_cursor.execute.assert_not_called()

    def test_returns_set_of_processed_ids(self, db):
        """Returns the subset of video_ids found as processed rows."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"video_id": "vidA"},
            {"video_id": "vidB"},
        ]

        result = instance.get_processed_video_ids(["vidA", "vidB", "vidC"])

        assert result == {"vidA", "vidB"}

    def test_no_matches_returns_empty_set(self, db):
        """fetchall empty -> empty set (nothing processed yet)."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_processed_video_ids(["vidA", "vidB"])

        assert result == set()

    def test_uses_parametrized_any_query_on_correct_table(self, db):
        """SQL targets youtube_source_videos, filters is_processed = TRUE via ANY(%s),
        and passes the video_ids list as params (no value interpolation)."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []
        video_ids = ["vidA", "vidB"]

        instance.get_processed_video_ids(video_ids)

        sql = mock_cursor.execute.call_args[0][0]
        params = mock_cursor.execute.call_args[0][1]
        assert "youtube_source_videos" in sql
        assert "is_processed = TRUE" in sql
        assert "ANY(%s)" in sql
        assert params == (video_ids,)


# --------------------------------------------------------------------------- #
# update_thumbnail_youtube_video_id
# --------------------------------------------------------------------------- #

class TestUpdateThumbnailYoutubeVideoId:

    def test_executes_update_with_correct_params(self, db):
        """UPDATE video_thumbnails SET youtube_video_id uses correct param order."""
        instance, mock_cursor = db

        instance.update_thumbnail_youtube_video_id(
            chapter_id=42, youtube_video_id="abc123"
        )

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "video_thumbnails" in sql
        assert "youtube_video_id" in sql
        assert params == ("abc123", 42)

    def test_returns_none(self, db):
        """Method has no return value (returns None)."""
        instance, mock_cursor = db

        result = instance.update_thumbnail_youtube_video_id(
            chapter_id=7, youtube_video_id="xyz789"
        )

        assert result is None

    def test_accepts_empty_string_video_id(self, db):
        """Empty-string youtube_video_id is forwarded as a param without error."""
        instance, mock_cursor = db

        instance.update_thumbnail_youtube_video_id(chapter_id=1, youtube_video_id="")

        _, params = mock_cursor.execute.call_args[0]
        assert params == ("", 1)


# --------------------------------------------------------------------------- #
# select_unprepared_turns — window-aggregate columns (issue #151)
# --------------------------------------------------------------------------- #

class TestSelectUnpreparedTurnsQueryShape:
    """Assert that select_unprepared_turns emits SQL with the two new window-aggregate
    columns (group_start_seconds / group_end_seconds) needed to fix empty
    subtitles.srt for grouped speaker-turn videos.

    Uses the same mock-cursor pattern as the existing `db` fixture.
    """

    def test_query_contains_group_start_seconds_window(self, db):
        """SQL must include MIN(st.start_seconds) OVER and alias group_start_seconds."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "MIN(st.start_seconds) OVER" in sql, (
            "select_unprepared_turns must use MIN(st.start_seconds) OVER window aggregate"
        )
        assert "group_start_seconds" in sql, (
            "select_unprepared_turns must expose group_start_seconds column alias"
        )

    def test_query_contains_group_end_seconds_window(self, db):
        """SQL must include MAX(st.end_seconds) OVER and alias group_end_seconds."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "MAX(st.end_seconds) OVER" in sql, (
            "select_unprepared_turns must use MAX(st.end_seconds) OVER window aggregate"
        )
        assert "group_end_seconds" in sql, (
            "select_unprepared_turns must expose group_end_seconds column alias"
        )
