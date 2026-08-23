"""Tests for speaker-resolution DB methods in CongressionalVideoDB (issue #177).

TDD RED cycle: written before implementation.
Tests: mark_turn_resolved + select_unprepared_turns resolution columns.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_conn(rows=None):
    """Return a fake pg_conn context-manager whose cursor returns rows."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows or []
    cursor.fetchone.return_value = None
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
# mark_turn_resolved
# ---------------------------------------------------------------------------

class TestMarkTurnResolved:
    """mark_turn_resolved(output_path, slug, confidence, method) updates all grouped rows."""

    def test_sql_updates_resolved_participant_slug(self):
        """UPDATE must set resolved_participant_slug."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context"
            )

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG" in query

    def test_sql_updates_speaker_resolution_confidence(self):
        """UPDATE must set speaker_resolution_confidence."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context"
            )

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_CONFIDENCE" in query

    def test_sql_updates_speaker_resolution_method(self):
        """UPDATE must set speaker_resolution_method."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context"
            )

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_METHOD" in query

    def test_sql_filters_by_output_path(self):
        """UPDATE WHERE must filter by output_path (fan-out for grouped turns)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context"
            )

        query = cursor.execute.call_args[0][0].upper()
        assert "OUTPUT_PATH" in query and "WHERE" in query

    def test_sql_passes_all_four_params(self):
        """UPDATE must pass slug, confidence, method, output_path as params."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        output_path = "/data/turns/1/video.mp4"
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.mark_turn_resolved(output_path, "pedro-sanchez", 0.92, "ai_srt_context")

        params = cursor.execute.call_args[0][1]
        assert "pedro-sanchez" in params
        assert 0.92 in params
        assert "ai_srt_context" in params
        assert output_path in params

    def test_returns_none(self):
        """mark_turn_resolved must return None (void operation)."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn()
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            result = db.mark_turn_resolved(
                "/data/turns/1/video.mp4", "pedro-sanchez", 0.92, "ai_srt_context"
            )

        assert result is None


# ---------------------------------------------------------------------------
# select_unprepared_turns — resolution columns
# ---------------------------------------------------------------------------

class TestSelectUnpreparedTurnsResolutionColumns:
    """select_unprepared_turns must expose resolution columns for idempotency check."""

    def test_sql_selects_resolved_participant_slug(self):
        """Query must SELECT stv.resolved_participant_slug."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "RESOLVED_PARTICIPANT_SLUG" in query

    def test_sql_selects_speaker_resolution_confidence(self):
        """Query must SELECT stv.speaker_resolution_confidence."""
        from congress_videos.modules.database import CongressionalVideoDB

        pg_mock, cursor = _make_conn(rows=[])
        with patch("congress_videos.modules.database.PostgresConnection", return_value=pg_mock):
            db = CongressionalVideoDB()
            db.select_unprepared_turns(limit=2)

        query = cursor.execute.call_args[0][0].upper()
        assert "SPEAKER_RESOLUTION_CONFIDENCE" in query
