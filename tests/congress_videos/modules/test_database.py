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
# create_or_update_session
# --------------------------------------------------------------------------- #

class TestCreateOrUpdateSession:

    def test_update_path_when_session_exists(self, db):
        """UPDATE path returns session_number from existing row."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"session_number": 42}

        result = instance.create_or_update_session(
            session_number=42,
            session_date=date(2025, 5, 22),
            target_date=date(2025, 5, 22),
            session_url="https://example.com/session/42",
        )

        assert result == 42
        first_call_sql = mock_cursor.execute.call_args_list[0][0][0]
        assert "UPDATE" in first_call_sql

    def test_insert_path_when_session_does_not_exist(self, db):
        """INSERT path is executed when UPDATE returns no row."""
        instance, mock_cursor = db
        # First fetchone (UPDATE check) returns None -> triggers INSERT
        # Second fetchone (INSERT RETURNING) returns the new row
        mock_cursor.fetchone.side_effect = [None, {"session_number": 99}]

        result = instance.create_or_update_session(
            session_number=99,
            session_date=date(2025, 10, 1),
            target_date=date(2025, 10, 1),
        )

        assert result == 99
        sql_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("UPDATE" in sql for sql in sql_calls)
        assert any("INSERT" in sql for sql in sql_calls)

    def test_uses_parametrized_queries_not_fstrings_with_user_data(self, db):
        """Verify placeholders %s are used — no raw SQL injection risk."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"session_number": 1}

        instance.create_or_update_session(
            session_number=1,
            session_date=date(2025, 1, 1),
            target_date=date(2025, 1, 1),
            session_url="http://test.com",
        )

        execute_args = mock_cursor.execute.call_args_list[0][0]
        params = execute_args[1]
        assert "http://test.com" in params  # user data passed as param, not in SQL

    def test_session_url_none_is_accepted(self, db):
        """session_url defaults to None without error."""
        instance, mock_cursor = db
        mock_cursor.fetchone.side_effect = [None, {"session_number": 5}]

        result = instance.create_or_update_session(
            session_number=5,
            session_date=date(2025, 3, 10),
            target_date=date(2025, 3, 10),
        )

        assert result == 5


# --------------------------------------------------------------------------- #
# upsert_video_topic
# --------------------------------------------------------------------------- #

class TestUpsertVideoTopic:

    def test_update_existing_topic(self, db):
        """UPDATE branch when entry_id already in DB."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"entry_id": "topic-001"}

        result = instance.upsert_video_topic(
            session_number=10,
            entry_id="topic-001",
            topic_data={"topic_title": "Presupuestos", "is_main_topic": True},
        )

        assert result == "topic-001"
        sql_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("UPDATE" in sql for sql in sql_calls)

    def test_insert_new_topic(self, db):
        """INSERT branch when entry_id not found."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None  # SELECT returns nothing

        result = instance.upsert_video_topic(
            session_number=10,
            entry_id="topic-new",
            topic_data={"topic_title": "Debate de Politica", "is_main_topic": False},
        )

        assert result == "topic-new"
        sql_calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("INSERT" in sql for sql in sql_calls)

    def test_upload_eligible_true_for_main_topics(self, db):
        """upload_eligible mirrors is_main_topic=True for INSERT path."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None  # INSERT path

        instance.upsert_video_topic(
            session_number=1,
            entry_id="main-001",
            topic_data={"is_main_topic": True},
        )

        # INSERT params: last two are (is_main_topic, upload_eligible)
        insert_call = mock_cursor.execute.call_args_list[-1]
        params = insert_call[0][1]
        assert params[-2] is True   # is_main_topic
        assert params[-1] is True   # upload_eligible

    def test_upload_eligible_false_for_non_main_topics(self, db):
        """upload_eligible mirrors is_main_topic=False for INSERT path."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.upsert_video_topic(
            session_number=1,
            entry_id="sub-001",
            topic_data={"is_main_topic": False},
        )

        insert_call = mock_cursor.execute.call_args_list[-1]
        params = insert_call[0][1]
        assert params[-2] is False
        assert params[-1] is False

    def test_entry_id_returned_on_update(self, db):
        """entry_id is returned unchanged from the UPDATE path."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"entry_id": "existing-999"}

        result = instance.upsert_video_topic(
            session_number=5,
            entry_id="existing-999",
            topic_data={},
        )

        assert result == "existing-999"


# --------------------------------------------------------------------------- #
# get_uploadable_videos
# --------------------------------------------------------------------------- #

class TestGetUploadableVideos:

    def test_returns_all_videos_without_limit(self, db):
        """No LIMIT appended when limit is None."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [{"entry_id": "v1"}, {"entry_id": "v2"}]

        result = instance.get_uploadable_videos()

        assert len(result) == 2
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" not in executed_sql

    def test_appends_limit_when_provided(self, db):
        """LIMIT clause is appended when a limit is given."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [{"entry_id": "v1"}]

        result = instance.get_uploadable_videos(limit=1)

        assert len(result) == 1
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" in executed_sql

    def test_returns_empty_list_when_no_videos(self, db):
        """Returns empty list when the view has no rows."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_uploadable_videos()

        assert result == []


# --------------------------------------------------------------------------- #
# mark_video_uploaded
# --------------------------------------------------------------------------- #

class TestMarkVideoUploaded:

    def test_executes_update_with_correct_params(self, db):
        """UPDATE sets is_uploaded_to_youtube and youtube_video_id."""
        instance, mock_cursor = db

        instance.mark_video_uploaded("entry-123", "yt-abc")

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "is_uploaded_to_youtube" in sql
        assert params == ("yt-abc", "entry-123")

    def test_uses_parameterized_query(self, db):
        """User data is never concatenated into SQL string."""
        instance, mock_cursor = db

        instance.mark_video_uploaded("entry-xss", "yt-xss")

        sql, params = mock_cursor.execute.call_args[0]
        assert "entry-xss" not in sql
        assert "yt-xss" not in sql
        assert "entry-xss" in params


# --------------------------------------------------------------------------- #
# add_to_upload_queue
# --------------------------------------------------------------------------- #

class TestAddToUploadQueue:

    def test_inserts_with_default_priority(self, db):
        """ON CONFLICT upsert uses default priority 5."""
        instance, mock_cursor = db

        instance.add_to_upload_queue("entry-001")

        sql, params = mock_cursor.execute.call_args[0]
        assert "INSERT" in sql
        assert "ON CONFLICT" in sql
        assert params == ("entry-001", 5)

    def test_inserts_with_custom_priority(self, db):
        """Custom priority value is forwarded to DB."""
        instance, mock_cursor = db

        instance.add_to_upload_queue("entry-002", priority=9)

        _, params = mock_cursor.execute.call_args[0]
        assert params == ("entry-002", 9)


# --------------------------------------------------------------------------- #
# get_session_by_number_and_date
# --------------------------------------------------------------------------- #

class TestGetSessionByNumberAndDate:

    def test_returns_none_when_not_found(self, db):
        """Returns None when no matching session exists."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.get_session_by_number_and_date(999, date(2025, 1, 1))

        assert result is None

    def test_returns_row_when_found(self, db):
        """Returns the row dict when the session exists."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "session_number": 42,
            "session_date": date(2025, 5, 22),
        }

        result = instance.get_session_by_number_and_date(42, date(2025, 5, 22))

        assert result["session_number"] == 42

    def test_passes_correct_params(self, db):
        """Correct parameters forwarded to cursor.execute."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_session_by_number_and_date(10, date(2025, 3, 15))

        _, params = mock_cursor.execute.call_args[0]
        assert params == (10, date(2025, 3, 15))


# --------------------------------------------------------------------------- #
# get_video_topics_by_session
# --------------------------------------------------------------------------- #

class TestGetVideoTopicsBySession:

    def test_returns_list_of_topics(self, db):
        """Returns list when topics exist."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [{"entry_id": "t1"}, {"entry_id": "t2"}]

        result = instance.get_video_topics_by_session(42)

        assert len(result) == 2

    def test_returns_empty_list_when_no_topics(self, db):
        """Returns empty list when no topics for session."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_video_topics_by_session(999)

        assert result == []

    def test_filters_by_session_number(self, db):
        """Query parameters include the session_number."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_video_topics_by_session(77)

        _, params = mock_cursor.execute.call_args[0]
        assert params == (77,)


# --------------------------------------------------------------------------- #
# update_session_total_topics
# --------------------------------------------------------------------------- #

class TestUpdateSessionTotalTopics:

    def test_executes_update_with_session_number(self, db):
        """UPDATE uses session_number twice (subquery + WHERE)."""
        instance, mock_cursor = db

        instance.update_session_total_topics(10)

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "total_topics" in sql
        assert params == (10, 10)


# --------------------------------------------------------------------------- #
# update_download_info
# --------------------------------------------------------------------------- #

class TestUpdateDownloadInfo:

    def test_executes_update_with_all_params(self, db):
        """All four parameters forwarded in correct order."""
        instance, mock_cursor = db

        instance.update_download_info("entry-001", "/data/video.mp4", 1024, 300)

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "video_file_path" in sql
        assert params == ("/data/video.mp4", 1024, 300, "entry-001")

    def test_accepts_none_file_size_and_duration(self, db):
        """Optional args default to None without error."""
        instance, mock_cursor = db

        instance.update_download_info("entry-002", "/data/video.mp4")

        _, params = mock_cursor.execute.call_args[0]
        assert params == ("/data/video.mp4", None, None, "entry-002")


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
# get_top_videos_for_upload
# --------------------------------------------------------------------------- #

class TestGetTopVideosForUpload:

    def test_returns_videos_with_defaults(self, db):
        """Default max_videos=5, min_score=6 passed as params."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [{"entry_id": "v1", "ai_interest_score": 8}]

        result = instance.get_top_videos_for_upload()

        assert len(result) == 1
        _, params = mock_cursor.execute.call_args[0]
        # (min_score, max_videos)
        assert params == (6, 5)

    def test_returns_videos_with_custom_params(self, db):
        """Custom max_videos and min_score are forwarded correctly."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_top_videos_for_upload(max_videos=10, min_score=8)

        _, params = mock_cursor.execute.call_args[0]
        assert params == (8, 10)

    def test_returns_empty_when_no_qualifying_videos(self, db):
        """Returns empty list when no videos meet criteria."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_top_videos_for_upload()

        assert result == []

    def test_query_filters_by_upload_status_and_eligibility(self, db):
        """SQL contains filters for is_uploaded_to_youtube, upload_eligible, is_main_topic."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_top_videos_for_upload()

        sql = mock_cursor.execute.call_args[0][0]
        assert "is_main_topic" in sql
        assert "is_uploaded_to_youtube" in sql
        assert "upload_eligible" in sql
        assert "ai_interest_score" in sql


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
