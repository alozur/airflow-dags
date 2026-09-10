"""Tests for CongressionalVideoDB database operations — TASK-021."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock

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
            {"chapter_id": 1},  # chapter 1 INSERT RETURNING
            {"chapter_id": 2},  # chapter 2 INSERT RETURNING
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

    def test_limit_is_parameterized_not_interpolated(self, db):
        """LIMIT is sent as a bound parameter, never string-interpolated."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_uploadable_chapters(limit=50)

        sql, params = mock_cursor.execute.call_args[0]
        assert "LIMIT %s" in sql
        assert params[-1] == 50

    def test_string_limit_is_cast_and_appended_to_params(self, db):
        """A numeric string limit is cast to int and appended after the score param."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_uploadable_chapters(limit="7")

        _, params = mock_cursor.execute.call_args[0]
        assert params == (4, 7)

    def test_non_integer_string_limit_raises_value_error_before_query(self, db):
        """A non-numeric string limit raises ValueError before any query executes."""
        instance, mock_cursor = db

        with pytest.raises(ValueError):
            instance.get_uploadable_chapters(limit="abc")

        mock_cursor.execute.assert_not_called()

    def test_non_coercible_limit_raises_type_error_before_query(self, db):
        """A limit that cannot be coerced to int raises TypeError before any query executes."""
        instance, mock_cursor = db

        with pytest.raises(TypeError):
            instance.get_uploadable_chapters(limit=object())

        mock_cursor.execute.assert_not_called()

    def test_zero_limit_is_falsy_and_omits_limit_clause(self, db):
        """limit=0 is treated as falsy (no LIMIT), matching current behavior."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_uploadable_chapters(limit=0)

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
        assert params == ("yt-chap-001", True, 7)

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
        assert "counts_toward_daily_quota = TRUE" in sql

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
        assert "counts_toward_daily_quota = TRUE" in sql

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

    def test_sql_selects_mentioned_participant_slugs_and_updated_at(self, db):
        """T2 — the query gains mentioned_participant_slugs and updated_at (issue #433)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(chapter_id=1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "mentioned_participant_slugs" in sql
        assert "updated_at" in sql

    def test_returns_multi_slug_mentioned_participants(self, db):
        """T2 — a populated mentioned_participant_slugs array passes through unchanged."""
        from datetime import datetime as dt_datetime

        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 8,
            "title": "Debate energía",
            "description": "desc",
            "speakers": ["Ana Pérez"],
            "key_speakers": ["Ana Pérez"],
            "topics": ["energía"],
            "scoring_reasoning": "high",
            "relevance_score": 4,
            "source_video_title": "Sesion 90",
            "source_video_url": "https://youtube.com/watch?v=uvw",
            "session_number": 90,
            "session_date": None,
            "mentioned_participant_slugs": ["ana-perez", "luis-gomez"],
            "updated_at": dt_datetime(2026, 9, 1, 12, 0, 0),
        }

        result = instance.get_chapter_metadata(chapter_id=8)

        assert result is not None
        assert result["mentioned_participant_slugs"] == ["ana-perez", "luis-gomez"]
        assert result["updated_at"] == dt_datetime(2026, 9, 1, 12, 0, 0)

    def test_null_mentioned_participant_slugs_stays_none(self, db):
        """T2 — a never-analysed chapter returns None, distinct from an empty list."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 9,
            "title": "Sin analizar",
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
            "mentioned_participant_slugs": None,
            "updated_at": None,
        }

        result = instance.get_chapter_metadata(chapter_id=9)

        assert result is not None
        assert result["mentioned_participant_slugs"] is None


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

        instance.update_thumbnail_youtube_video_id(chapter_id=42, youtube_video_id="abc123")

        mock_cursor.execute.assert_called_once()
        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "video_thumbnails" in sql
        assert "youtube_video_id" in sql
        assert params == ("abc123", 42)

    def test_returns_none(self, db):
        """Method has no return value (returns None)."""
        instance, mock_cursor = db

        result = instance.update_thumbnail_youtube_video_id(chapter_id=7, youtube_video_id="xyz789")

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
        assert "group_start_seconds" in sql, "select_unprepared_turns must expose group_start_seconds column alias"

    def test_query_contains_group_end_seconds_window(self, db):
        """SQL must include MAX(st.end_seconds) OVER and alias group_end_seconds."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "MAX(st.end_seconds) OVER" in sql, (
            "select_unprepared_turns must use MAX(st.end_seconds) OVER window aggregate"
        )
        assert "group_end_seconds" in sql, "select_unprepared_turns must expose group_end_seconds column alias"


# --------------------------------------------------------------------------- #
# select_unprepared_turns — procedural-turn filter (issue #143)
# --------------------------------------------------------------------------- #


class TestSelectUnpreparedTurnsProceduralGate:
    def test_query_excludes_procedural_turns(self, db):
        """SQL must add NOT st.is_procedural so a flagged turn's own row
        is never selected for representative attribution."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "NOT st.is_procedural" in sql, f"select_unprepared_turns must exclude is_procedural rows; got: {sql}"

    def test_query_selects_keep_intervals_column(self, db):
        """SQL must select stv.keep_intervals so _write_turn_sidecars can
        retime the SRT from the EXECUTED cut boundaries."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "stv.keep_intervals" in sql, f"select_unprepared_turns must select stv.keep_intervals; got: {sql}"


# --------------------------------------------------------------------------- #
# select_unprepared_turns — chapter span columns (issue #322)
# --------------------------------------------------------------------------- #


class TestSelectUnpreparedTurnsChapterSpan:
    def test_query_selects_chapter_start_time_column(self, db):
        """SQL must select vc.start_time so _chapter_span can anchor the
        evidence-gate region's backward clamp to the chapter's own start."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "vc.start_time" in sql, f"select_unprepared_turns must select vc.start_time; got: {sql}"

    def test_query_selects_chapter_end_time_column(self, db):
        """SQL must select vc.end_time so the chapter-wide prompt context
        (slice 2) can bound the qa window to the chapter's own span."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "vc.end_time" in sql, f"select_unprepared_turns must select vc.end_time; got: {sql}"

    def test_query_selects_turn_type_column(self, db):
        """SQL must select stv.turn_type so resolve_speaker can gate the
        chapter-wide prompt context to turn_type == 'qa' (slice 2)."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "stv.turn_type" in sql, f"select_unprepared_turns must select stv.turn_type; got: {sql}"


# --------------------------------------------------------------------------- #
# select_unprepared_turns — chapter-first-substantive-turn signal (issue #613)
# --------------------------------------------------------------------------- #


class TestSelectUnpreparedTurnsChapterFirstSubstantive:
    """Spec: 'Chapter First-Substantive-Turn Signal' — an additive BOOL_OR
    window column that must be computed over ALL chapter turns (prepared,
    procedural, or unprepared), not just the rows surviving the WHERE/dedup
    filters (design.md D1)."""

    def test_query_contains_is_chapter_first_substantive_alias(self, db):
        """SQL must expose the is_chapter_first_substantive column alias."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "is_chapter_first_substantive" in sql, (
            f"select_unprepared_turns must expose is_chapter_first_substantive; got: {sql}"
        )

    def test_query_uses_bool_or_over_output_path(self, db):
        """The signal must be a BOOL_OR window aggregate partitioned by
        stv.output_path, matching group_start_seconds/group_end_seconds."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "BOOL_OR" in sql, f"select_unprepared_turns must use BOOL_OR; got: {sql}"
        assert "PARTITION BY stv.output_path" in sql

    def test_query_uses_not_exists_correlated_over_raw_speaker_turns(self, db):
        """D1: the predicate must be a NOT EXISTS subquery over the raw
        speaker_turns table (st2), correlated on chapter_id, so an earlier
        prepared/procedural turn still counts."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert "NOT EXISTS" in sql, f"select_unprepared_turns must use NOT EXISTS; got: {sql}"
        assert "st2.chapter_id = st.chapter_id" in sql

    def test_query_uses_substantive_turn_min_secs_threshold(self, db):
        """D2: the 30.0s threshold must appear in the SQL text as the
        SUBSTANTIVE_TURN_MIN_SECS constant, f-string-interpolated."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.select_unprepared_turns(limit=2)

        sql = mock_cursor.execute.call_args[0][0]
        assert ">= 30.0" in sql, f"select_unprepared_turns must use the 30.0s substantive threshold; got: {sql}"


# --------------------------------------------------------------------------- #
# record_title_generation_input_turn / _short (issue #549)
# --------------------------------------------------------------------------- #


class TestRecordTitleGenerationInputTurn:
    """record_title_generation_input_turn is an unguarded UPDATE keyed by
    output_path (design.md D3): every sibling row of a grouped turn shares
    one output_path, so one call writes the identical payload to all of
    them. Unlike record_copy_verification_turn there is NO
    ``IS DISTINCT FROM`` content guard — rowcount == 0 unambiguously means
    the key matched no row, and the method must return that count
    faithfully so the call site can treat it as a loud `no_row` outcome
    (Req 3 / design C4)."""

    def test_update_statement_targets_speaker_turn_videos_no_content_guard(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1

        instance.record_title_generation_input_turn("/path/turn1.mp4", payload={"title": "t"})

        sql = mock_cursor.execute.call_args[0][0].upper()
        assert "UPDATE" in sql
        assert "SPEAKER_TURN_VIDEOS" in sql
        assert "TITLE_GENERATION_INPUT = %S::JSONB" in sql
        assert "WHERE OUTPUT_PATH = %S" in sql
        assert "IS DISTINCT FROM" not in sql

    def test_binds_json_dumps_payload_and_output_path(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1
        payload = {"generator": "turn_title", "schema_version": 1, "title": "t"}

        instance.record_title_generation_input_turn("/path/turn1.mp4", payload=payload)

        params = mock_cursor.execute.call_args[0][1]
        assert params == (json.dumps(payload, ensure_ascii=False), "/path/turn1.mp4")

    def test_returns_cursor_rowcount(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 3

        result = instance.record_title_generation_input_turn("/path/turn1.mp4", payload={"title": "t"})

        assert result == 3

    def test_zero_rowcount_is_returned_faithfully_not_swallowed(self, db):
        """No content guard exists, so rowcount == 0 means the key matched
        no row (Req 3/C4) — the method must not mask it as success."""
        instance, mock_cursor = db
        mock_cursor.rowcount = 0

        result = instance.record_title_generation_input_turn("/path/turn1.mp4", payload={"title": "t"})

        assert result == 0

    def test_raises_value_error_on_empty_output_path(self, db):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_title_generation_input_turn("", payload={"title": "t"})

    @pytest.mark.parametrize("bad_payload", [None, {}, "not-a-dict", []])
    def test_raises_value_error_on_invalid_payload(self, db, bad_payload):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_title_generation_input_turn("/path/turn1.mp4", payload=bad_payload)

    def test_grouped_siblings_update_by_output_path_only(self, db):
        """Scenario 3.1: one call, keyed by output_path, updates every
        sibling row sharing that path with the identical payload — the
        WHERE clause carries no turn_id filter, so a row under a different
        output_path is structurally untouched."""
        instance, mock_cursor = db
        mock_cursor.rowcount = 3  # 3 sibling rows share this output_path

        result = instance.record_title_generation_input_turn("/path/grouped.mp4", payload={"title": "grouped"})

        sql = mock_cursor.execute.call_args[0][0].upper()
        params = mock_cursor.execute.call_args[0][1]
        assert "WHERE OUTPUT_PATH = %S" in sql
        assert "TURN_ID" not in sql
        assert params[-1] == "/path/grouped.mp4"
        assert result == 3

    def test_rerun_same_output_path_overwrites_without_raising(self, db):
        """Scenario 3.2a: calling twice with the same key overwrites the
        payload and returns rowcount >= 1 both times, without raising."""
        instance, mock_cursor = db
        mock_cursor.rowcount = 1

        first = instance.record_title_generation_input_turn("/path/turn1.mp4", payload={"title": "v1"})
        second = instance.record_title_generation_input_turn("/path/turn1.mp4", payload={"title": "v2"})

        assert first >= 1
        assert second >= 1
        assert mock_cursor.execute.call_count == 2


class TestRecordTitleGenerationInputShort:
    """record_title_generation_input_short mirrors
    record_title_generation_input_turn exactly, keyed by video_shorts.id."""

    def test_update_statement_targets_video_shorts_no_content_guard(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1

        instance.record_title_generation_input_short(42, payload={"title": "t"})

        sql = mock_cursor.execute.call_args[0][0].upper()
        assert "UPDATE" in sql
        assert "VIDEO_SHORTS" in sql
        assert "TITLE_GENERATION_INPUT = %S::JSONB" in sql
        assert "WHERE ID = %S" in sql
        assert "IS DISTINCT FROM" not in sql

    def test_binds_json_dumps_payload_and_short_id(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1
        payload = {"generator": "shorts_metadata", "schema_version": 1, "title": "t"}

        instance.record_title_generation_input_short(42, payload=payload)

        params = mock_cursor.execute.call_args[0][1]
        assert params == (json.dumps(payload, ensure_ascii=False), 42)

    def test_returns_cursor_rowcount(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1

        result = instance.record_title_generation_input_short(42, payload={"title": "t"})

        assert result == 1

    def test_zero_rowcount_is_returned_faithfully(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 0

        result = instance.record_title_generation_input_short(42, payload={"title": "t"})

        assert result == 0

    def test_raises_value_error_on_falsy_short_id(self, db):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_title_generation_input_short(0, payload={"title": "t"})

    @pytest.mark.parametrize("bad_payload", [None, {}, "not-a-dict", []])
    def test_raises_value_error_on_invalid_payload(self, db, bad_payload):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_title_generation_input_short(42, payload=bad_payload)


# --------------------------------------------------------------------------- #
# claim_thumbnail_text_regeneration / record_thumbnail_text_regeneration_outcome
# (issue #545, design.md D1/D3)
# --------------------------------------------------------------------------- #


class TestClaimThumbnailTextRegeneration:
    """claim_thumbnail_text_regeneration is a claim-before-act atomic UPDATE
    (design.md D3): it charges the attempt (and, at the ceiling, sets
    thumbnail_regen_exhausted) BEFORE any paid Pikzels/OpenAI call is made,
    so a crash after the claim cannot re-spend for free. The WHERE clause
    is the sole guard against re-claiming an exhausted/at-ceiling row —
    every assertion below pins the exact guard text so removing it (the
    mutation check) fails these tests.

    THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS is 2, not #331's 3: each attempt here
    spends 1-2 Pikzels images + 1 OpenAI call with NO throttle anywhere in
    the codebase, so this counter is the only spend ceiling (design.md D3).
    """

    def test_charges_before_second_call(self, db):
        """First claim on a fresh row: attempts becomes 1, not yet exhausted
        (ceiling is 2). The UPDATE must increment atomically and gate on
        the WHERE clause, never a read-then-write from Python."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }

        result = instance.claim_thumbnail_text_regeneration("/path/turn1.mp4", prior_brief=None)

        sql = mock_cursor.execute.call_args[0][0].upper()
        assert "UPDATE" in sql
        assert "SPEAKER_TURN_VIDEOS" in sql
        assert "THUMBNAIL_REGEN_ATTEMPTS = COALESCE(THUMBNAIL_REGEN_ATTEMPTS, 0) + 1" in sql
        assert ">= 2" in sql  # exhausted flips true only once attempts reach the ceiling
        assert "WHERE OUTPUT_PATH = %S" in sql
        assert "AND NOT COALESCE(THUMBNAIL_REGEN_EXHAUSTED, FALSE)" in sql
        assert "AND COALESCE(THUMBNAIL_REGEN_ATTEMPTS, 0) < 2" in sql
        assert "RETURNING" in sql
        assert result == {"thumbnail_regen_attempts": 1, "thumbnail_regen_exhausted": False}

    def test_refuses_at_ceiling(self, db):
        """GIVEN 2 prior attempts already recorded and thumbnail_regen_exhausted
        = TRUE, WHEN claim is called again, THEN it returns None and the
        WHERE guard means Postgres would affect zero rows — no attempts
        column bump is possible from this call."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None  # WHERE guard matched zero rows

        result = instance.claim_thumbnail_text_regeneration("/path/exhausted.mp4", prior_brief=None)

        assert result is None

    def test_idempotent_on_rerun(self, db):
        """Retrying the upload step for the same output_path must not
        double-count: the atomic WHERE guard (attempts < 2 AND NOT
        exhausted) is the only thing preventing a second concurrent/rerun
        claim from over-charging. Mutation check: this exact guard text
        must be present, or a removed guard would let every rerun re-claim
        indefinitely."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "thumbnail_regen_attempts": 2,
            "thumbnail_regen_exhausted": True,
        }

        instance.claim_thumbnail_text_regeneration("/path/turn1.mp4", prior_brief=None)

        sql = mock_cursor.execute.call_args[0][0].upper()
        assert "AND NOT COALESCE(THUMBNAIL_REGEN_EXHAUSTED, FALSE)" in sql
        assert "AND COALESCE(THUMBNAIL_REGEN_ATTEMPTS, 0) < 2" in sql

    def test_unknown_output_path_returns_none(self, db):
        """Chapter items have no speaker_turn_videos row at all (design.md
        D3 / spec note 8) — the claim must return None, not raise, so the
        caller publishes as-is. This is intended behaviour, not a bug."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.claim_thumbnail_text_regeneration("/no/such/row.mp4", prior_brief={"a": 1})

        assert result is None

    def test_prior_brief_write_once(self, db):
        """A second successful claim on the same row must NOT overwrite
        thumbnail_regen_prior_brief — COALESCE keeps the first/true-original
        brief across attempts (spec: "Prior brief is snapshotted before
        triggering")."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }
        prior_brief = {"title": "Original Brief"}

        instance.claim_thumbnail_text_regeneration("/path/turn1.mp4", prior_brief=prior_brief)

        sql = mock_cursor.execute.call_args[0][0].upper()
        params = mock_cursor.execute.call_args[0][1]
        assert "THUMBNAIL_REGEN_PRIOR_BRIEF = COALESCE(THUMBNAIL_REGEN_PRIOR_BRIEF, %S::JSONB)" in sql
        assert params[0] == json.dumps(prior_brief, ensure_ascii=False)

    def test_prior_brief_none_binds_null(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }

        instance.claim_thumbnail_text_regeneration("/path/turn1.mp4", prior_brief=None)

        params = mock_cursor.execute.call_args[0][1]
        assert params[0] is None

    def test_raises_value_error_on_empty_output_path(self, db):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.claim_thumbnail_text_regeneration("", prior_brief=None)


class TestRecordThumbnailTextRegenerationOutcome:
    """record_thumbnail_text_regeneration_outcome is the terminal write for
    a claimed attempt (design.md D4): outcome is one of {applied, timeout,
    trigger_failed, child_failed, invalid_result, not_claimed}. It must
    never touch thumbnail_regen_prior_brief — the claim call already
    snapshotted it write-once — so both briefs stay independently
    retrievable after a landed regeneration."""

    def test_persists_regenerated_brief(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1
        regenerated_brief = {"title": "New Brief"}

        instance.record_thumbnail_text_regeneration_outcome(
            "/path/turn1.mp4",
            outcome="applied",
            regenerated_brief=regenerated_brief,
        )

        sql = " ".join(mock_cursor.execute.call_args[0][0].upper().split())
        params = mock_cursor.execute.call_args[0][1]
        assert "UPDATE" in sql
        assert "SPEAKER_TURN_VIDEOS" in sql
        assert "THUMBNAIL_REGEN_OUTCOME = %S" in sql
        assert "THUMBNAIL_REGEN_BRIEF = %S::JSONB" in sql
        assert "THUMBNAIL_REGEN_PRIOR_BRIEF" not in sql  # never touched by this call
        assert "WHERE OUTPUT_PATH = %S" in sql
        assert json.dumps(regenerated_brief, ensure_ascii=False) in params
        assert "applied" in params

    def test_failure_outcome_binds_error_and_null_brief(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 1

        instance.record_thumbnail_text_regeneration_outcome(
            "/path/turn1.mp4",
            outcome="timeout",
            error="poll bound exceeded",
        )

        params = mock_cursor.execute.call_args[0][1]
        assert "timeout" in params
        assert "poll bound exceeded" in params
        assert None in params  # no regenerated_brief for a non-landed outcome

    def test_returns_cursor_rowcount(self, db):
        instance, mock_cursor = db
        mock_cursor.rowcount = 3

        result = instance.record_thumbnail_text_regeneration_outcome("/path/grouped.mp4", outcome="not_claimed")

        assert result == 3

    def test_raises_value_error_on_empty_output_path(self, db):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_thumbnail_text_regeneration_outcome("", outcome="applied")

    def test_raises_value_error_on_empty_outcome(self, db):
        instance, _ = db

        with pytest.raises(ValueError):
            instance.record_thumbnail_text_regeneration_outcome("/path/turn1.mp4", outcome="")
