"""Tests for the thumbnail republish DB methods (issue #331).

Slice 1: the upload-time marker method. Dual-key since the caller's reality
is dual-key (issue #230).

Slice 2 (this batch, WU2a): the healer DB layer — candidate selection,
success recording, failure recording. The DD4 structural guard proving none
of these methods can ever touch upload-verification state lives in a
follow-up commit on this same branch (kept as its own reviewable unit).

Mirrors the mocked-cursor convention in test_database_turns.py:17.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


def _set_clause(query: str) -> str:
    """Extract the substring between SET and WHERE/RETURNING, uppercased.

    Returns "" for statements with no SET clause (e.g. a plain SELECT) —
    which is itself a valid, vacuously-true proof that a read-only query
    cannot write any forbidden column.
    """
    upper = query.upper()
    if "SET" not in upper:
        return ""
    start = upper.index("SET") + len("SET")
    end = len(upper)
    for stop_word in ("WHERE", "RETURNING"):
        idx = upper.find(stop_word, start)
        if idx != -1:
            end = min(end, idx)
    return upper[start:end]


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


class TestSelectTurnsNeedingThumbnailRepublish:
    """Candidate selection (DD3): wrapped-dedup shape, mirrors
    select_unverified_uploads (database.py:1309-1319)."""

    def test_uses_distinct_on_output_path(self):
        db, cursor = _make_db()
        cursor.fetchall.return_value = []

        db.select_turns_needing_thumbnail_republish()

        query = cursor.execute.call_args[0][0].upper()
        assert "DISTINCT ON (OUTPUT_PATH)" in query

    def test_where_predicates_all_present(self):
        db, cursor = _make_db()
        cursor.fetchall.return_value = []

        db.select_turns_needing_thumbnail_republish()

        # Collapse whitespace so multi-line SQL formatting doesn't break
        # substring assertions on wrapped predicates.
        query = " ".join(cursor.execute.call_args[0][0].upper().split())
        assert "THUMBNAIL_REPUBLISH_NEEDED_AT IS NOT NULL" in query
        assert "THUMBNAIL_REPUBLISHED_AT IS NULL" in query
        assert "NOT COALESCE(THUMBNAIL_REPUBLISH_ABANDONED, FALSE)" in query
        assert "COALESCE(THUMBNAIL_REPUBLISH_ATTEMPTS, 0) <" in query
        assert "IS_UPLOADED_TO_YOUTUBE = TRUE" in query
        assert "YOUTUBE_VIDEO_ID IS NOT NULL" in query

    def test_outer_order_by_is_three_key_and_ordered(self):
        """attempts ASC, needed_at ASC, output_path ASC — output_path is the
        real tiebreaker (issue #300's lesson: the tiebreaker decides)."""
        db, cursor = _make_db()
        cursor.fetchall.return_value = []

        db.select_turns_needing_thumbnail_republish()

        query = cursor.execute.call_args[0][0].upper()
        outer_order_by = query.rsplit("ORDER BY", 1)[1]
        attempts_idx = outer_order_by.index("THUMBNAIL_REPUBLISH_ATTEMPTS")
        needed_at_idx = outer_order_by.index("THUMBNAIL_REPUBLISH_NEEDED_AT")
        output_path_idx = outer_order_by.index("OUTPUT_PATH")
        assert attempts_idx < needed_at_idx < output_path_idx

    def test_limit_is_parametrized_not_inlined(self):
        db, cursor = _make_db()
        cursor.fetchall.return_value = []

        db.select_turns_needing_thumbnail_republish(limit=7)

        query, params = cursor.execute.call_args[0]
        assert "LIMIT %S" in query.upper()
        assert 7 in params

    def test_returns_fetchall_rows(self):
        db, cursor = _make_db()
        rows = [{"turn_id": 1, "output_path": "/a.mp4"}]
        cursor.fetchall.return_value = rows

        result = db.select_turns_needing_thumbnail_republish()

        assert result == rows


class TestMarkTurnThumbnailRepublished:
    """Success recording (DD4): sets exactly two columns."""

    def test_sets_only_republished_at_and_clears_error(self):
        db, cursor = _make_db()

        db.mark_turn_thumbnail_republished(output_path="/a.mp4")

        set_clause = _set_clause(cursor.execute.call_args[0][0])
        assert "THUMBNAIL_REPUBLISHED_AT" in set_clause and "NOW()" in set_clause
        assert "LAST_THUMBNAIL_REPUBLISH_ERROR" in set_clause and "NULL" in set_clause
        assert "THUMBNAIL_REPUBLISH_ATTEMPTS" not in set_clause
        assert "THUMBNAIL_REPUBLISH_ABANDONED" not in set_clause

    def test_where_clause_uses_output_path(self):
        db, cursor = _make_db()

        db.mark_turn_thumbnail_republished(output_path="/a.mp4")

        query, params = cursor.execute.call_args[0]
        assert "WHERE OUTPUT_PATH = %S" in query.upper()
        assert params[-1] == "/a.mp4"

    def test_returns_cursor_rowcount(self):
        db, cursor = _make_db()
        cursor.rowcount = 5

        result = db.mark_turn_thumbnail_republished(output_path="/a.mp4")

        assert result == 5


class TestRecordTurnThumbnailRepublishFailure:
    """Bounded retry and abandonment (DD4/spec): threshold=3, abandon=True
    forces immediate abandonment (missing thumbnail.png, proposal D3)."""

    def test_increments_attempts_and_records_error(self):
        db, cursor = _make_db()
        cursor.fetchone.return_value = {
            "thumbnail_republish_attempts": 1,
            "thumbnail_republish_abandoned": False,
        }

        db.record_turn_thumbnail_republish_failure(
            "/a.mp4", error_message="quota exceeded"
        )

        set_clause = _set_clause(cursor.execute.call_args[0][0])
        assert "THUMBNAIL_REPUBLISH_ATTEMPTS" in set_clause and "COALESCE" in set_clause
        assert "LAST_THUMBNAIL_REPUBLISH_ERROR" in set_clause
        assert "THUMBNAIL_REPUBLISH_ABANDONED" in set_clause
        assert "quota exceeded" in cursor.execute.call_args[0][1]

    def test_transient_failure_stays_eligible(self):
        """attempts was 0 -> 1, below threshold -> not abandoned."""
        db, cursor = _make_db()
        cursor.fetchone.return_value = {
            "thumbnail_republish_attempts": 1,
            "thumbnail_republish_abandoned": False,
        }

        result = db.record_turn_thumbnail_republish_failure("/a.mp4", "transient")

        assert result == {
            "thumbnail_republish_attempts": 1,
            "thumbnail_republish_abandoned": False,
        }

    def test_abandons_at_threshold(self):
        """attempts was 2 -> 3rd failure reaches threshold -> abandoned."""
        db, cursor = _make_db()
        cursor.fetchone.return_value = {
            "thumbnail_republish_attempts": 3,
            "thumbnail_republish_abandoned": True,
        }

        result = db.record_turn_thumbnail_republish_failure("/a.mp4", "still failing")

        assert result["thumbnail_republish_abandoned"] is True

    def test_abandon_flag_forces_immediate_abandonment_at_attempt_one(self):
        """Missing thumbnail.png (proposal D3): abandon=True short-circuits
        the threshold, no regeneration attempted."""
        db, cursor = _make_db()
        cursor.fetchone.return_value = {
            "thumbnail_republish_attempts": 1,
            "thumbnail_republish_abandoned": True,
        }

        db.record_turn_thumbnail_republish_failure(
            "/a.mp4", "Thumbnail file not found: /a/thumbnail.png", abandon=True
        )

        query, params = cursor.execute.call_args[0]
        assert True in params  # the abandon literal is passed through as a param

    def test_returns_none_when_no_matching_row(self):
        db, cursor = _make_db()
        cursor.fetchone.return_value = None

        result = db.record_turn_thumbnail_republish_failure("/missing.mp4", "err")

        assert result is None
