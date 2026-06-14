"""Tests for CongressionalVideoDB.save_youtube_chapters_to_db savepoint resilience
and get_processed_video_ids idempotency lookup.

These tests use MagicMock for the PostgresConnection / connection / cursor — there
is NO real Postgres. They verify the per-video SAVEPOINT isolation that prevents a
single failing video from poisoning the whole batch (the production bug), and that a
total failure raises so the Airflow task fails visibly.
"""

from __future__ import annotations

from contextlib import closing  # noqa: F401  (documents the construct under test)
from datetime import date
from unittest.mock import MagicMock

import pytest


# --------------------------------------------------------------------------- #
# Helpers / fixtures
# --------------------------------------------------------------------------- #

def _make_cursor() -> MagicMock:
    """A cursor mock usable as a context manager (`with conn.cursor() as cur`)."""
    cur = MagicMock(name="cursor")
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    # Default: RETURNING chapter_id / video_id rows are dicts (RealDictCursor).
    cur.fetchone.return_value = {"chapter_id": 1}
    cur.fetchall.return_value = []
    return cur


def _make_db(cur: MagicMock):
    """Build a CongressionalVideoDB with its pg_conn fully mocked.

    The connection mock supports both `with closing(conn)` (calls .close()) and
    `with conn:` (transaction context manager), and `conn.cursor()` yields `cur`.
    """
    conn = MagicMock(name="connection")
    conn.__enter__ = MagicMock(return_value=conn)
    conn.__exit__ = MagicMock(return_value=False)
    conn.cursor.return_value = cur

    pg_conn = MagicMock(name="pg_conn")
    pg_conn.get_connection.return_value = conn
    pg_conn.get_qualified_table.side_effect = lambda t: f"development.{t}"

    from congress_videos.modules.database import CongressionalVideoDB

    db = CongressionalVideoDB.__new__(CongressionalVideoDB)
    db.pg_conn = pg_conn
    return db, conn


def _executed_sql(cur: MagicMock) -> list[str]:
    """Return the SQL string of every cur.execute(...) call, in order."""
    return [c.args[0] for c in cur.execute.call_args_list if c.args]


def _video(video_id: str, n_chapters: int) -> dict:
    return {
        "video_id": video_id,
        "video_title": f"Title {video_id}",
        "scored_chapters": [
            {
                "title": f"Chapter {i}",
                "description": "desc",
                "start_time": f"00:0{i}:00,000",
                "end_time": f"00:0{i + 1}:00,000",
                "duration_minutes": 1.0,
                "speakers": ["A"],
                "topics": ["T"],
                "relevance_score": 4,
                "speaker_relevance_points": 2,
                "topic_relevance_points": 1,
                "public_interest_points": 1,
                "scoring_reasoning": "because",
                "key_speakers": ["A"],
                "is_current_topic": True,
                "scoring_error": None,
            }
            for i in range(1, n_chapters + 1)
        ],
    }


# --------------------------------------------------------------------------- #
# 1. Happy path
# --------------------------------------------------------------------------- #

class TestHappyPath:

    def test_single_video_two_chapters_saved(self):
        cur = _make_cursor()
        db, _conn = _make_db(cur)

        data = {"videos": [_video("vidA", 2)]}

        result = db.save_youtube_chapters_to_db(
            scored_chapters_data=data,
            session_number=12,
            session_date=date(2026, 6, 12),
        )

        sql = _executed_sql(cur)

        # SAVEPOINT issued, then released, no rollback.
        assert any(s.strip() == "SAVEPOINT sp_video" for s in sql)
        assert any("RELEASE SAVEPOINT sp_video" in s for s in sql)
        assert not any("ROLLBACK TO SAVEPOINT" in s for s in sql)

        # Both chapter INSERTs ran against video_chapters.
        chapter_inserts = [s for s in sql if "video_chapters" in s and "INSERT INTO" in s]
        assert len(chapter_inserts) == 2
        assert all("ON CONFLICT (video_id, start_time, end_time)" in s for s in chapter_inserts)

        # Result counts.
        assert result["total_videos_saved"] == 1
        assert result["total_chapters_saved"] == 2
        assert result["total_videos_failed"] == 0
        assert len(result["videos"]) == 1
        assert result["videos"][0]["video_id"] == "vidA"
        assert result["videos"][0]["chapters_saved"] == 2
        assert result["videos"][0]["error"] is None


# --------------------------------------------------------------------------- #
# 2. Isolation on failure — one bad video must not poison the batch
# --------------------------------------------------------------------------- #

class TestIsolationOnFailure:

    def test_first_video_fails_second_succeeds(self):
        cur = _make_cursor()

        # Track which source video is currently being processed, then raise on the
        # chapter INSERT for vid1 only. This proves vid1's failure is isolated and
        # vid2 still persists.
        state = {"current_video": None}

        def execute_side_effect(sql, params=None):
            if "youtube_source_videos" in sql and "INSERT INTO" in sql:
                state["current_video"] = params[0]  # params order: (video_id, ...)
            if (
                "video_chapters" in sql
                and "INSERT INTO" in sql
                and state["current_video"] == "vid1"
            ):
                raise Exception("simulated chapter insert failure for vid1")
            return None

        cur.execute.side_effect = execute_side_effect
        cur.fetchone.return_value = {"chapter_id": 99}

        db, _conn = _make_db(cur)

        data = {"videos": [_video("vid1", 1), _video("vid2", 1)]}

        result = db.save_youtube_chapters_to_db(
            scored_chapters_data=data,
            session_number=1,
            session_date=date(2026, 6, 12),
        )

        sql = _executed_sql(cur)

        # ROLLBACK TO SAVEPOINT was issued (for vid1) AND a RELEASE happened (for vid2).
        assert any("ROLLBACK TO SAVEPOINT sp_video" in s for s in sql)
        assert any("RELEASE SAVEPOINT sp_video" in s for s in sql)

        videos_by_id = {v["video_id"]: v for v in result["videos"]}

        # vid1 recorded with an error, NOT counted as saved.
        assert videos_by_id["vid1"]["error"] is not None
        assert videos_by_id["vid1"]["chapters_saved"] == 0

        # vid2 saved successfully.
        assert videos_by_id["vid2"]["error"] is None
        assert videos_by_id["vid2"]["chapters_saved"] == 1

        assert result["total_videos_saved"] == 1
        assert result["total_chapters_saved"] == 1
        assert result["total_videos_failed"] == 1


# --------------------------------------------------------------------------- #
# 3. Total failure raises (visible task failure)
# --------------------------------------------------------------------------- #

class TestTotalFailureRaises:

    def test_all_videos_fail_raises_runtime_error(self):
        cur = _make_cursor()

        def execute_side_effect(sql, params=None):
            if "video_chapters" in sql and "INSERT INTO" in sql:
                raise Exception("always fails")
            return None

        cur.execute.side_effect = execute_side_effect
        db, _conn = _make_db(cur)

        data = {"videos": [_video("vidX", 1)]}

        with pytest.raises(RuntimeError, match="failed to save"):
            db.save_youtube_chapters_to_db(
                scored_chapters_data=data,
                session_number=1,
                session_date=date(2026, 6, 12),
            )

        # Rollback was issued before raising.
        assert any("ROLLBACK TO SAVEPOINT sp_video" in s for s in _executed_sql(cur))


# --------------------------------------------------------------------------- #
# 4. get_processed_video_ids
# --------------------------------------------------------------------------- #

class TestGetProcessedVideoIds:

    def test_empty_input_returns_empty_set_no_query(self):
        cur = _make_cursor()
        db, _conn = _make_db(cur)

        result = db.get_processed_video_ids([])

        assert result == set()
        cur.execute.assert_not_called()

    def test_non_empty_input_filters_is_processed_true(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [{"video_id": "a"}, {"video_id": "c"}]
        db, _conn = _make_db(cur)

        result = db.get_processed_video_ids(["a", "b", "c"])

        assert result == {"a", "c"}
        sql = _executed_sql(cur)
        assert len(sql) == 1
        assert "is_processed = TRUE" in sql[0]
        assert "SELECT" in sql[0]


# --------------------------------------------------------------------------- #
# 5. Timeline persistence — JSONB column round-trips through the INSERT
# --------------------------------------------------------------------------- #

class TestTimelinePersistence:

    @staticmethod
    def _chapter_insert(cur: MagicMock):
        """Return (sql, params) of the video_chapters INSERT call."""
        for c in cur.execute.call_args_list:
            if c.args and "video_chapters" in c.args[0] and "INSERT INTO" in c.args[0]:
                return c.args[0], (c.args[1] if len(c.args) > 1 else None)
        return None, None

    def test_timeline_serialized_as_jsonb_in_insert(self):
        import json

        cur = _make_cursor()
        db, _conn = _make_db(cur)

        timeline = [
            {"time": "00:01:00", "speaker": "A", "content": "intro"},
            {"time": "00:02:00", "speaker": "B", "content": "reply"},
        ]
        chapter = {
            "title": "Chapter 1",
            "description": "desc",
            "start_time": "00:00:00,000",
            "end_time": "00:05:00,000",
            "duration_minutes": 5.0,
            "speakers": ["A"],
            "topics": ["T"],
            "timeline": timeline,
            "relevance_score": 4,
            "speaker_relevance_points": 2,
            "topic_relevance_points": 1,
            "public_interest_points": 1,
            "scoring_reasoning": "because",
            "key_speakers": ["A"],
            "is_current_topic": True,
            "scoring_error": None,
        }
        data = {"videos": [{"video_id": "vidA", "video_title": "T", "scored_chapters": [chapter]}]}

        db.save_youtube_chapters_to_db(scored_chapters_data=data, session_number=1)

        sql, params = self._chapter_insert(cur)
        # Column present and bound with a jsonb cast.
        assert "timeline" in sql
        assert "%s::jsonb" in sql
        assert "timeline = EXCLUDED.timeline" in sql
        # Timeline is serialized with json.dumps at the 9th param (after topics).
        assert params[8] == json.dumps(timeline)
        assert json.loads(params[8]) == timeline

    def test_missing_timeline_defaults_to_empty_json_array(self):
        import json

        cur = _make_cursor()
        db, _conn = _make_db(cur)

        # No "timeline" key — must default to an empty JSON array, never NULL.
        chapter = {
            "title": "Chapter 1",
            "description": "desc",
            "start_time": "00:00:00,000",
            "end_time": "00:05:00,000",
            "duration_minutes": 5.0,
            "speakers": ["A"],
            "topics": ["T"],
            "relevance_score": 4,
            "speaker_relevance_points": 2,
            "topic_relevance_points": 1,
            "public_interest_points": 1,
            "scoring_reasoning": "because",
            "key_speakers": ["A"],
            "is_current_topic": True,
            "scoring_error": None,
        }
        data = {"videos": [{"video_id": "vidA", "video_title": "T", "scored_chapters": [chapter]}]}

        db.save_youtube_chapters_to_db(scored_chapters_data=data, session_number=1)

        _sql, params = self._chapter_insert(cur)
        assert params[8] == json.dumps([])
