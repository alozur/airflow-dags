"""Tests for prepare-phase DB helper methods in CongressionalVideoDB (issue #146).

TDD RED tests:
- select_unprepared_turns(limit): returns turns where prepared_at IS NULL
- mark_turn_prepared(turn_id): UPDATE speaker_turn_videos SET prepared_at = now()
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_conn(rows=None, scalar=None):
    """Return a fake pg_conn context-manager whose cursor returns rows or scalar."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.fetchone.return_value = {"count": scalar} if scalar is not None else None
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor

    pg_conn_mock = MagicMock()
    pg_conn_mock.get_connection.return_value = conn
    pg_conn_mock.get_qualified_table.side_effect = lambda name: name

    return pg_conn_mock, cursor


# ---------------------------------------------------------------------------
# 2.1 select_unprepared_turns
# ---------------------------------------------------------------------------

class TestSelectUnpreparedTurns:
    """select_unprepared_turns(limit) fetches turns where prepared_at IS NULL."""

    def test_sql_contains_prepared_at_is_null(self):
        """Query must filter prepared_at IS NULL."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "PREPARED_AT IS NULL" in query, (
            f"Query must contain PREPARED_AT IS NULL; got: {call_args[0][0]}"
        )

    def test_sql_contains_is_uploaded_to_youtube_false(self):
        """Query must also filter is_uploaded_to_youtube = FALSE."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "IS_UPLOADED_TO_YOUTUBE" in query and "FALSE" in query, (
            "Query must contain is_uploaded_to_youtube = FALSE"
        )

    def test_sql_contains_coalesce_interest_score_order(self):
        """Query must ORDER BY COALESCE(interest_score, 1) DESC."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "COALESCE" in query and "INTEREST_SCORE" in query and "DESC" in query, (
            "Query must ORDER BY COALESCE(interest_score,1) DESC"
        )

    def test_sql_contains_limit(self):
        """Query must pass LIMIT clause with the limit parameter."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        params = call_args[0][1] if len(call_args[0]) > 1 else ()
        assert "LIMIT" in query, "Query must contain LIMIT"
        # Limit value either inline or as param
        assert 2 in params or "2" in query

    def test_default_limit_is_2(self):
        """Default limit must be 2 (the nightly buffer size)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns()  # No explicit limit

        call_args = cursor.execute.call_args
        params = call_args[0][1] if len(call_args[0]) > 1 else ()
        query = call_args[0][0].upper()
        assert 2 in params or "LIMIT 2" in query, (
            "Default limit must be 2"
        )

    def test_returns_list_of_dicts(self):
        """Return value must be a list of turn dicts."""
        from congress_videos.modules.database import CongressionalVideoDB

        fake_rows = [
            {"turn_id": 1, "output_path": "/data/v1.mp4", "prepared_at": None},
        ]
        pg_mock, cursor = _make_conn(rows=fake_rows)
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.select_unprepared_turns(limit=1)

        assert result == fake_rows

    def test_sql_joins_video_chapters_and_youtube_source_videos(self):
        """Query must JOIN video_chapters and youtube_source_videos (mirror uploadable_turns)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "VIDEO_CHAPTERS" in query, "Query must JOIN video_chapters"
        assert "YOUTUBE_SOURCE_VIDEOS" in query, "Query must JOIN youtube_source_videos"

    def test_sql_selects_video_id_chapter_title_session_date(self):
        """Query must select video_id, vc.title AS chapter_title, and session_date."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0]
        upper = query.upper()
        assert "VIDEO_ID" in upper, "Query must select video_id"
        assert "AS CHAPTER_TITLE" in upper, "Query must expose vc.title AS chapter_title"
        assert "SESSION_DATE" in upper, "Query must select session_date"

    def test_sql_filters_relevance_score_and_both_upload_gates(self):
        """Query must filter vc.relevance_score >= 2 and is_uploaded_to_youtube FALSE on stv and vc."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "RELEVANCE_SCORE >= 2" in query, "Query must filter vc.relevance_score >= 2"
        # Both stv.is_uploaded_to_youtube = FALSE and vc.is_uploaded_to_youtube = FALSE
        assert query.count("IS_UPLOADED_TO_YOUTUBE = FALSE") == 2, (
            "Query must gate both stv and vc on is_uploaded_to_youtube = FALSE"
        )


# ---------------------------------------------------------------------------
# 2.2 mark_turn_prepared
# ---------------------------------------------------------------------------

class TestMarkTurnPrepared:
    """mark_turn_prepared(turn_id) updates only prepared_at on matching turn_id row."""

    def test_sql_updates_prepared_at_only(self):
        """UPDATE must set prepared_at = now() on the matching turn_id."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_prepared(turn_id=42)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "UPDATE" in query
        assert "PREPARED_AT" in query
        assert "NOW()" in query or "CURRENT_TIMESTAMP" in query
        # Must NOT update is_uploaded_to_youtube
        assert "IS_UPLOADED_TO_YOUTUBE" not in query, (
            "mark_turn_prepared must NOT touch is_uploaded_to_youtube"
        )

    def test_sql_uses_correct_turn_id(self):
        """UPDATE must filter WHERE turn_id = %s and pass 42 as param."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_prepared(turn_id=42)

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        params = call_args[0][1] if len(call_args[0]) > 1 else ()
        assert "WHERE" in query and "TURN_ID" in query
        assert 42 in params

    def test_returns_none(self):
        """mark_turn_prepared must return None (void operation)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.mark_turn_prepared(turn_id=1)

        assert result is None
