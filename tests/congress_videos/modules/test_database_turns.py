"""Tests for turn upload queue DB methods in CongressionalVideoDB.

Tests:
- get_uploadable_turns(limit) returns rows up to limit
- mark_turns_uploaded(turn_id, youtube_video_id) sets all three tracking cols
- count_pending_uploadable_turns() counts unuploaded turns
- count_chapters_uploaded_today still counts chapters only (not turns)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


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


class TestGetUploadableTurns:
    """get_uploadable_turns(limit) fetches from uploadable_turns view."""

    def test_returns_rows_from_view(self):
        from congress_videos.modules.database import CongressionalVideoDB

        fake_rows = [
            {"turn_id": 1, "output_path": "/path/turn1.mp4", "resolved_name": "Ana García"},
            {"turn_id": 2, "output_path": "/path/turn2.mp4", "resolved_name": "Pedro López"},
        ]
        pg_mock, cursor = _make_conn(rows=fake_rows)

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.get_uploadable_turns(limit=2)

        assert result == fake_rows

    def test_respects_limit_parameter(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.get_uploadable_turns(limit=1)

        # The SQL executed must contain LIMIT
        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "LIMIT" in query

    def test_passes_limit_value_to_query(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.get_uploadable_turns(limit=3)

        call_args = cursor.execute.call_args
        # Limit value passed as parameter or inline
        assert 3 in call_args[0][1] or "3" in call_args[0][0]

    def test_default_limit_is_one(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.get_uploadable_turns()  # no limit arg

        call_args = cursor.execute.call_args
        # Default limit = 1
        assert 1 in call_args[0][1] or "1" in call_args[0][0]

    def test_queries_uploadable_turns_view(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.get_uploadable_turns(limit=1)

        call_args = cursor.execute.call_args
        query = call_args[0][0].lower()
        assert "uploadable_turns" in query


class TestMarkTurnsUploaded:
    """mark_turns_uploaded sets all three tracking columns."""

    def test_sets_is_uploaded_to_youtube_true(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=42, youtube_video_id="abc123")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "IS_UPLOADED_TO_YOUTUBE" in query
        assert "TRUE" in query

    def test_sets_youtube_video_id(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=42, youtube_video_id="abc123")

        call_args = cursor.execute.call_args
        params = call_args[0][1]
        assert "abc123" in params

    def test_sets_youtube_upload_date_to_now(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=42, youtube_video_id="abc123")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "YOUTUBE_UPLOAD_DATE" in query
        # now() or CURRENT_TIMESTAMP
        assert "NOW()" in query or "CURRENT_TIMESTAMP" in query

    def test_targets_correct_turn_id(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=99, youtube_video_id="xyz")

        call_args = cursor.execute.call_args
        params = call_args[0][1]
        assert 99 in params

    def test_updates_speaker_turn_videos_table(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=1, youtube_video_id="v1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "SPEAKER_TURN_VIDEOS" in query


class TestCountPendingUploadableTurns:
    """count_pending_uploadable_turns returns count from uploadable_turns view."""

    def test_returns_count_of_unuploaded_turns(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(scalar=5)

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.count_pending_uploadable_turns()

        assert result == 5

    def test_returns_zero_when_all_uploaded(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(scalar=0)

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.count_pending_uploadable_turns()

        assert result == 0

    def test_queries_uploadable_turns_view(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(scalar=2)

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.count_pending_uploadable_turns()

        call_args = cursor.execute.call_args
        query = call_args[0][0].lower()
        assert "uploadable_turns" in query


class TestMarkTurnsUploadedSiblingMarking:
    """mark_turns_uploaded must mark ALL siblings sharing output_path, not just turn_id."""

    def test_where_clause_uses_output_path_subquery(self):
        """UPDATE WHERE must use output_path = (SELECT output_path ...) not WHERE turn_id = %s."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=1, youtube_video_id="vid1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "WHERE OUTPUT_PATH = (" in query, (
            "UPDATE must filter by output_path = (subquery), not by turn_id directly"
        )

    def test_inner_subquery_selects_output_path(self):
        """Inner subquery must SELECT output_path FROM the same table."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=1, youtube_video_id="vid1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "SELECT OUTPUT_PATH FROM" in query, (
            "SQL must contain inner SELECT output_path FROM <table> WHERE turn_id = %s"
        )

    def test_qualified_table_name_appears_twice(self):
        """Qualified table name must appear in both the outer UPDATE and the inner subquery."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        pg_mock.get_qualified_table.side_effect = lambda t: f"development.{t}"

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=5, youtube_video_id="vidX")

        call_args = cursor.execute.call_args
        query = call_args[0][0]
        count = query.count("development.speaker_turn_videos")
        assert count >= 2, f"Qualified table name must appear at least twice (outer + subquery), got {count}"

    def test_params_tuple_is_youtube_video_id_then_turn_id(self):
        """Params must be (youtube_video_id, turn_id) — youtube_video_id for SET, turn_id for subquery WHERE."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded(turn_id=7, youtube_video_id="vid_abc")

        call_args = cursor.execute.call_args
        params = call_args[0][1]
        assert params == ("vid_abc", 7), f"Params must be (youtube_video_id, turn_id) = ('vid_abc', 7), got {params}"


class TestMarkTurnsUploadedByOutputPath:
    """mark_turns_uploaded_by_output_path is the fallback marking method (issue #230)."""

    def test_where_clause_uses_output_path_equality(self):
        """UPDATE WHERE must filter by output_path = %s, no subquery."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded_by_output_path(output_path="/path/turn1.mp4", youtube_video_id="vid1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "WHERE OUTPUT_PATH = %S" in query
        assert "SELECT" not in query, "output_path fallback must not use a subquery like mark_turns_uploaded"

    def test_sets_all_three_tracking_columns(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded_by_output_path(output_path="/path/turn1.mp4", youtube_video_id="vid1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "IS_UPLOADED_TO_YOUTUBE" in query
        assert "TRUE" in query
        assert "YOUTUBE_VIDEO_ID" in query
        assert "YOUTUBE_UPLOAD_DATE" in query
        assert "NOW()" in query or "CURRENT_TIMESTAMP" in query

    def test_params_tuple_is_youtube_video_id_then_output_path(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded_by_output_path(output_path="/path/turn1.mp4", youtube_video_id="vid_abc")

        call_args = cursor.execute.call_args
        params = call_args[0][1]
        assert params == ("vid_abc", "/path/turn1.mp4"), f"Params must be (youtube_video_id, output_path), got {params}"

    def test_returns_cursor_rowcount(self):
        """Return value must be cur.rowcount, so callers can distinguish 0 rows matched."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        cursor.rowcount = 3

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.mark_turns_uploaded_by_output_path(output_path="/path/grouped.mp4", youtube_video_id="vid1")

        assert result == 3

    def test_raises_value_error_on_empty_output_path(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            with pytest.raises(ValueError):
                db.mark_turns_uploaded_by_output_path(output_path="", youtube_video_id="vid1")

    def test_raises_value_error_on_none_output_path(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            with pytest.raises(ValueError):
                db.mark_turns_uploaded_by_output_path(output_path=None, youtube_video_id="vid1")

    def test_updates_speaker_turn_videos_table(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turns_uploaded_by_output_path(output_path="/path/turn1.mp4", youtube_video_id="vid1")

        call_args = cursor.execute.call_args
        query = call_args[0][0].upper()
        assert "SPEAKER_TURN_VIDEOS" in query


class TestCountChaptersUploadedTodayUnchanged:
    """count_chapters_uploaded_today must still count chapters only (not turns)."""

    def test_counts_chapters_not_turns(self):
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(scalar=3)

        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.count_chapters_uploaded_today()

        assert result == 3
        # Must query video_chapters table, not speaker_turn_videos
        call_args = cursor.execute.call_args
        query = call_args[0][0].lower()
        assert "video_chapters" in query
        assert "speaker_turn_videos" not in query
