"""Tests for the thumbnail republish marker DB method (issue #331, slice 1).

Dual-key since the caller's reality is dual-key (issue #230). Mirrors the
mocked-cursor convention in test_database_turns.py:17.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _make_db():
    """CongressionalVideoDB wired to a fake cursor that stubs execute()."""
    from congress_videos.modules.database import CongressionalVideoDB

    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cursor

    pg_conn_mock = MagicMock()
    pg_conn_mock.get_connection.return_value = conn
    pg_conn_mock.get_qualified_table.side_effect = lambda name: name

    with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_conn_mock):
        return CongressionalVideoDB(), cursor


class TestMarkTurnThumbnailRepublishNeeded:

    def test_raises_value_error_when_both_keys_falsy(self):
        db, _ = _make_db()

        with pytest.raises(ValueError):
            db.mark_turn_thumbnail_republish_needed(output_path=None, turn_id=None)
        with pytest.raises(ValueError):
            db.mark_turn_thumbnail_republish_needed(output_path="", turn_id=0)

    def test_output_path_predicate_no_subquery(self):
        db, cursor = _make_db()

        db.mark_turn_thumbnail_republish_needed(output_path="/path/turn1.mp4")

        query = cursor.execute.call_args[0][0].upper()
        assert "WHERE OUTPUT_PATH = %S" in query
        assert "SELECT" not in query
        assert "SPEAKER_TURN_VIDEOS" in query

    def test_turn_id_uses_sibling_resolution_subselect(self):
        """Mirrors mark_turns_uploaded's subselect (database.py:956)."""
        db, cursor = _make_db()

        db.mark_turn_thumbnail_republish_needed(turn_id=42)

        query = cursor.execute.call_args[0][0].upper()
        assert "WHERE OUTPUT_PATH = (SELECT OUTPUT_PATH FROM" in query
        assert "WHERE TURN_ID = %S" in query
        assert cursor.execute.call_args[0][1][-1] == 42

    def test_rearms_and_leaves_counters_untouched(self):
        """Attempts/abandoned stay cumulative — see database.py:433."""
        db, cursor = _make_db()

        db.mark_turn_thumbnail_republish_needed(
            output_path="/path/turn1.mp4", error_message="quota exceeded"
        )

        query = cursor.execute.call_args[0][0]
        set_clause = query[: query.upper().index("WHERE")].upper()
        assert "THUMBNAIL_REPUBLISH_NEEDED_AT" in set_clause and "= NOW()" in set_clause
        assert "THUMBNAIL_REPUBLISHED_AT" in set_clause and "= NULL" in set_clause
        assert "LAST_THUMBNAIL_REPUBLISH_ERROR" in set_clause
        assert "THUMBNAIL_REPUBLISH_ATTEMPTS" not in set_clause
        assert "THUMBNAIL_REPUBLISH_ABANDONED" not in set_clause
        assert "quota exceeded" in cursor.execute.call_args[0][1]

    def test_returns_cursor_rowcount(self):
        db, cursor = _make_db()
        cursor.rowcount = 5

        result = db.mark_turn_thumbnail_republish_needed(output_path="/path/grouped.mp4")

        assert result == 5

    def test_positive_marker_is_never_a_mark_turns_uploaded_side_effect(self):
        """Positive marker: needed_at stays NULL unless explicitly set, so
        backfill of pre-existing rows is a non-event."""
        db, cursor = _make_db()

        db.mark_turns_uploaded(turn_id=1, youtube_video_id="vid1")

        query = cursor.execute.call_args[0][0].upper()
        assert "THUMBNAIL_REPUBLISH_NEEDED_AT" not in query
