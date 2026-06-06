"""Tests for CongressionalVideoDB database operations — TASK-021."""

from __future__ import annotations

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
