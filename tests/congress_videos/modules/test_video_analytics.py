"""Unit tests for congress_videos.modules.video_analytics pure functions
and CongressionalVideoDB analytics methods.

Covers:
- pending_checkpoints(): window filtering, pair exclusion, multi-checkpoint
- parse_analytics_response(): column mapping, missing column → None
- should_persist(): skip-and-retry contract (all-None, all-zero, any-nonzero)
- CongressionalVideoDB.get_pending_analytics_checkpoints(): SQL query shape
- CongressionalVideoDB.record_analytics_snapshot(): ON CONFLICT DO NOTHING, no action_taken
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _utcnow() -> datetime:
    return datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)


def _video(chapter_id: int, youtube_video_id: str, hours_ago: float) -> dict:
    """Build a synthetic video_chapters-style row."""
    now = _utcnow()
    return {
        "chapter_id": chapter_id,
        "youtube_video_id": youtube_video_id,
        "youtube_upload_date": now - timedelta(hours=hours_ago),
    }


# ---------------------------------------------------------------------------
# Tests: pending_checkpoints
# ---------------------------------------------------------------------------


class TestPendingCheckpoints:
    """Spec: Monitoring Window + Pending Checkpoint Selection."""

    def test_video_within_window_is_included(self):
        """GIVEN a video uploaded 25h ago, no collected snapshots
        WHEN pending_checkpoints is called
        THEN the 24h pair is included."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "abc123", 25.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)
        pairs = [(r["youtube_video_id"], r["checkpoint"]) for r in result]

        assert ("abc123", "24h") in pairs

    def test_video_beyond_90d_is_excluded(self):
        """GIVEN a video uploaded 91 days (2184h) ago
        WHEN pending_checkpoints is called
        THEN it is NOT in the result."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "old_vid", 91 * 24)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)

        assert result == []

    def test_null_youtube_video_id_is_excluded(self):
        """GIVEN a video with youtube_video_id=None
        WHEN pending_checkpoints is called
        THEN it is NOT in the result."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [{
            "chapter_id": 1,
            "youtube_video_id": None,
            "youtube_upload_date": now - timedelta(hours=30),
        }]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)

        assert result == []

    def test_pair_not_yet_collected_is_returned(self):
        """GIVEN a 25h-old video with no snapshot for 24h
        WHEN pending_checkpoints is called
        THEN (youtube_video_id, '24h') is returned."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "abc123", 25.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)

        assert any(r["checkpoint"] == "24h" and r["youtube_video_id"] == "abc123" for r in result)

    def test_already_collected_pair_is_excluded(self):
        """GIVEN a 25h-old video AND a snapshot for (abc123, 24h)
        WHEN pending_checkpoints is called
        THEN 24h is NOT returned."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "abc123", 25.0)]
        collected: set[tuple[str, str]] = {("abc123", "24h")}

        result = pending_checkpoints(now, videos, collected)
        pairs = [(r["youtube_video_id"], r["checkpoint"]) for r in result]

        assert ("abc123", "24h") not in pairs

    def test_30h_elapsed_returns_only_24h(self):
        """GIVEN a video uploaded 30h ago, no snapshots
        WHEN pending_checkpoints is called
        THEN only '24h' is returned, NOT '48h'."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "vid30h", 30.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)
        checkpoints = [r["checkpoint"] for r in result if r["youtube_video_id"] == "vid30h"]

        assert "24h" in checkpoints
        assert "48h" not in checkpoints

    def test_50h_elapsed_returns_24h_and_48h(self):
        """GIVEN a video uploaded 50h ago, no snapshots
        WHEN pending_checkpoints is called
        THEN both '24h' and '48h' are returned."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "vid50h", 50.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)
        checkpoints = {r["checkpoint"] for r in result if r["youtube_video_id"] == "vid50h"}

        assert "24h" in checkpoints
        assert "48h" in checkpoints

    def test_multiple_checkpoints_crossed_simultaneously(self):
        """GIVEN a video uploaded 200h ago (>7d=168h), no snapshots
        WHEN pending_checkpoints is called
        THEN 24h, 48h, and 7d are all returned."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(1, "vid200h", 200.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)
        checkpoints = {r["checkpoint"] for r in result if r["youtube_video_id"] == "vid200h"}

        assert "24h" in checkpoints
        assert "48h" in checkpoints
        assert "7d" in checkpoints

    def test_result_contains_chapter_id(self):
        """Each returned dict must carry chapter_id for FK persistence."""
        from congress_videos.modules.video_analytics import pending_checkpoints

        now = _utcnow()
        videos = [_video(42, "chk42", 25.0)]
        collected: set[tuple[str, str]] = set()

        result = pending_checkpoints(now, videos, collected)

        assert all(r["chapter_id"] == 42 for r in result)


# ---------------------------------------------------------------------------
# Tests: parse_analytics_response
# ---------------------------------------------------------------------------


class TestParseAnalyticsResponse:
    """Spec: Snapshot Persistence Shape."""

    def _sample_response(self, values: list) -> dict:
        """Build a minimal Analytics API response covering all METRIC_FIELDS."""
        from congress_videos.config.analytics_config import METRIC_FIELDS

        return {
            "columnHeaders": [{"name": name} for name in METRIC_FIELDS],
            "rows": [values],
        }

    def test_all_fields_are_mapped(self):
        """GIVEN a full Analytics response with one row
        WHEN parse_analytics_response is called
        THEN each METRIC_FIELDS key maps to its column value."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import parse_analytics_response

        values = list(range(len(METRIC_FIELDS)))
        result = parse_analytics_response(self._sample_response(values))

        for field, value in zip(METRIC_FIELDS, values):
            assert result[field] == value

    def test_missing_column_returns_none(self):
        """GIVEN a response that lacks some columns
        WHEN parse_analytics_response is called
        THEN missing columns map to None."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import parse_analytics_response

        # Only 'views' present
        resp = {
            "columnHeaders": [{"name": "views"}],
            "rows": [[42]],
        }
        result = parse_analytics_response(resp)

        assert result["views"] == 42
        for field in METRIC_FIELDS:
            if field != "views":
                assert result[field] is None

    def test_empty_rows_returns_all_none(self):
        """GIVEN an Analytics response with no rows
        WHEN parse_analytics_response is called
        THEN every metric field is None."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import parse_analytics_response

        resp = {
            "columnHeaders": [{"name": name} for name in METRIC_FIELDS],
            "rows": [],
        }
        result = parse_analytics_response(resp)

        assert all(result[field] is None for field in METRIC_FIELDS)

    def test_result_keys_match_metric_fields(self):
        """Result dict must contain exactly the METRIC_FIELDS keys."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import parse_analytics_response

        values = list(range(len(METRIC_FIELDS)))
        result = parse_analytics_response(self._sample_response(values))

        assert set(result.keys()) == set(METRIC_FIELDS)


# ---------------------------------------------------------------------------
# Tests: should_persist
# ---------------------------------------------------------------------------


class TestShouldPersist:
    """Spec: Skip-and-Retry on NULL or Zero Metrics."""

    def test_all_none_returns_false(self):
        """GIVEN all metrics are None
        WHEN should_persist is called
        THEN it returns False (skip-and-retry)."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import should_persist

        metrics = dict.fromkeys(METRIC_FIELDS)
        assert should_persist(metrics) is False

    def test_all_zero_returns_false(self):
        """GIVEN all metrics are 0 / 0.0
        WHEN should_persist is called
        THEN it returns False (analytics lag, retry next hour)."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import should_persist

        metrics = dict.fromkeys(METRIC_FIELDS, 0)
        assert should_persist(metrics) is False

    def test_one_nonzero_returns_true(self):
        """GIVEN at least one nonzero metric
        WHEN should_persist is called
        THEN it returns True."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import should_persist

        metrics = dict.fromkeys(METRIC_FIELDS, 0)
        metrics["views"] = 1
        assert should_persist(metrics) is True

    def test_mixed_none_and_nonzero_returns_true(self):
        """GIVEN some None values but at least one nonzero value
        WHEN should_persist is called
        THEN it returns True."""
        from congress_videos.config.analytics_config import METRIC_FIELDS
        from congress_videos.modules.video_analytics import should_persist

        metrics = dict.fromkeys(METRIC_FIELDS)
        metrics["estimatedMinutesWatched"] = 90.6
        assert should_persist(metrics) is True


# ---------------------------------------------------------------------------
# DB method helpers
# ---------------------------------------------------------------------------


def _make_cursor() -> MagicMock:
    """Cursor mock usable as a context manager."""
    cur = MagicMock(name="cursor")
    cur.__enter__ = MagicMock(return_value=cur)
    cur.__exit__ = MagicMock(return_value=False)
    cur.fetchall.return_value = []
    cur.fetchone.return_value = None
    return cur


def _make_db(cur: MagicMock):
    """Build a CongressionalVideoDB with pg_conn fully mocked."""
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
    """Return the SQL of every cur.execute() call, in order."""
    return [c.args[0] for c in cur.execute.call_args_list if c.args]


# ---------------------------------------------------------------------------
# Tests: CongressionalVideoDB.get_pending_analytics_checkpoints
# ---------------------------------------------------------------------------


class TestGetPendingAnalyticsCheckpoints:
    """Spec: Monitoring Window / Pending Checkpoint Selection (DB layer)."""

    def test_query_filters_null_youtube_video_id(self):
        """SQL must include youtube_video_id IS NOT NULL filter."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_pending_analytics_checkpoints()

        sql_calls = _executed_sql(cur)
        assert len(sql_calls) >= 1
        assert "youtube_video_id IS NOT NULL" in sql_calls[0]

    def test_query_filters_uploaded_to_youtube(self):
        """SQL must filter is_uploaded_to_youtube = TRUE."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_pending_analytics_checkpoints()

        sql_calls = _executed_sql(cur)
        assert any("is_uploaded_to_youtube" in s for s in sql_calls)

    def test_query_applies_90_day_window(self):
        """SQL must restrict to uploads within the 90-day monitoring window."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_pending_analytics_checkpoints()

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "90" in combined or "2160" in combined or "days" in combined.lower()

    def test_returns_list_of_rows(self):
        """Method must return a list (fetchall result)."""
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"chapter_id": 1, "youtube_video_id": "abc", "youtube_upload_date": None}
        ]
        db, _ = _make_db(cur)

        result = db.get_pending_analytics_checkpoints()

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["chapter_id"] == 1


# ---------------------------------------------------------------------------
# Tests: CongressionalVideoDB.record_analytics_snapshot
# ---------------------------------------------------------------------------


class TestRecordAnalyticsSnapshot:
    """Spec: Exactly-Once Snapshot Persistence / Snapshot Persistence Shape."""

    def _metrics(self) -> dict:
        return {
            "views": 120,
            "estimatedMinutesWatched": 90.6,
            "averageViewDuration": 45.3,
            "averageViewPercentage": 52.1,
            "likes": 40,
            "dislikes": 2,
            "comments": 8,
            "shares": 5,
            "subscribersGained": 3,
            "subscribersLost": 1,
        }

    def test_insert_uses_on_conflict_do_nothing(self):
        """INSERT must include ON CONFLICT DO NOTHING for idempotency."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.record_analytics_snapshot(
            chapter_id=1,
            youtube_video_id="abc123",
            checkpoint="24h",
            metrics=self._metrics(),
        )

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls).upper()
        assert "ON CONFLICT" in combined
        assert "DO NOTHING" in combined

    def test_action_taken_is_not_written(self):
        """action_taken must NOT appear in the INSERT SQL (reserved NULL placeholder)."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.record_analytics_snapshot(
            chapter_id=1,
            youtube_video_id="abc123",
            checkpoint="24h",
            metrics=self._metrics(),
        )

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "action_taken" not in combined

    def test_metrics_are_passed_as_jsonb(self):
        """metrics dict must be serialised to JSON in the execute call."""
        cur = _make_cursor()
        db, _ = _make_db(cur)
        metrics = self._metrics()

        db.record_analytics_snapshot(
            chapter_id=1,
            youtube_video_id="abc123",
            checkpoint="24h",
            metrics=metrics,
        )

        # Verify the params passed to cursor.execute contain a JSON string.
        all_params = []
        for call in cur.execute.call_args_list:
            if len(call.args) > 1:
                all_params.extend(call.args[1])

        json_params = [p for p in all_params if isinstance(p, str) and "views" in p]
        assert len(json_params) >= 1
        parsed = json.loads(json_params[0])
        assert parsed["views"] == 120

    def test_chapter_id_and_checkpoint_are_bound(self):
        """chapter_id and checkpoint value must appear in the execute parameters."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.record_analytics_snapshot(
            chapter_id=42,
            youtube_video_id="xyz999",
            checkpoint="7d",
            metrics=self._metrics(),
        )

        all_params = []
        for call in cur.execute.call_args_list:
            if len(call.args) > 1:
                all_params.extend(call.args[1])

        assert 42 in all_params
        assert "xyz999" in all_params
        assert "7d" in all_params


# ---------------------------------------------------------------------------
# Tests: CongressionalVideoDB.get_collected_analytics_pairs
# ---------------------------------------------------------------------------


class TestGetCollectedAnalyticsPairs:
    """Spec: Quota-saving optimization — query already-collected pairs before
    calling the Analytics API, so pending_checkpoints() can exclude them and
    avoid re-fetching data we already have.

    Contract:
    - Empty youtube_video_ids input → return empty set WITHOUT hitting the DB.
    - Non-empty input → SELECT (youtube_video_id, checkpoint) WHERE
      youtube_video_id = ANY(%s) and return as set of tuples.
    - SQL must reference 'video_analytics_snapshots' table.
    - SQL must use 'youtube_video_id = ANY' for batch lookup.
    """

    def test_empty_input_returns_empty_set_without_db_call(self):
        """GIVEN an empty list of youtube_video_ids
        WHEN get_collected_analytics_pairs is called
        THEN an empty set is returned and the DB cursor is never queried."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        result = db.get_collected_analytics_pairs([])

        assert result == set()
        # DB must NOT be hit for empty input (idempotent fast-path)
        cur.execute.assert_not_called()

    def test_returns_set_of_tuples(self):
        """GIVEN a list with one youtube_video_id and DB returns two rows
        WHEN get_collected_analytics_pairs is called
        THEN a set of (youtube_video_id, checkpoint) tuples is returned."""
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"youtube_video_id": "abc123", "checkpoint": "24h"},
            {"youtube_video_id": "abc123", "checkpoint": "48h"},
        ]
        db, _ = _make_db(cur)

        result = db.get_collected_analytics_pairs(["abc123"])

        assert isinstance(result, set)
        assert ("abc123", "24h") in result
        assert ("abc123", "48h") in result
        assert len(result) == 2

    def test_sql_queries_video_analytics_snapshots(self):
        """SQL must target the video_analytics_snapshots table."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_collected_analytics_pairs(["vid1", "vid2"])

        sql_calls = _executed_sql(cur)
        assert len(sql_calls) >= 1
        assert "video_analytics_snapshots" in sql_calls[0]

    def test_sql_uses_any_for_batch_lookup(self):
        """SQL must use ANY(%s) for an efficient batch lookup."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_collected_analytics_pairs(["vid1", "vid2"])

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "ANY" in combined.upper()

    def test_multiple_ids_and_no_existing_rows_returns_empty_set(self):
        """GIVEN multiple ids and DB returns no rows
        WHEN get_collected_analytics_pairs is called
        THEN an empty set is returned (not an empty list)."""
        cur = _make_cursor()
        cur.fetchall.return_value = []
        db, _ = _make_db(cur)

        result = db.get_collected_analytics_pairs(["x1", "x2", "x3"])

        assert result == set()
        assert isinstance(result, set)


# ---------------------------------------------------------------------------
# Tests: evaluate_action (issue #102)
#
# Spec: Per-checkpoint underperformance evaluation / Lifetime action cap per
# video / Checkpoint-scoped action types / action_taken vocabulary.
# Gate precedence (load-bearing): capped -> cold_start -> ok -> act.
# ---------------------------------------------------------------------------


class TestEvaluateActionCapped:
    """capped takes precedence over cold_start/ok."""

    def test_thumbnail_cap_reached_returns_capped(self):
        """GIVEN prior_actions shows the lifetime thumbnail cap reached
        WHEN evaluate_action runs at a non-title checkpoint, deeply
             underperforming, with a healthy sample
        THEN it returns 'capped' — precedence over cold_start/ok."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=1,
            median_views=1000,
            sample_size=20,
            checkpoint="48h",
            prior_actions={"thumbnail": 1, "title": 0},
        )
        assert result == "capped"

    def test_title_cap_reached_at_24h_returns_capped(self):
        """GIVEN prior_actions shows the lifetime title cap reached
        WHEN evaluate_action runs at the 24h checkpoint, underperforming,
             with a healthy sample
        THEN it returns 'capped'."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=1,
            median_views=1000,
            sample_size=20,
            checkpoint="24h",
            prior_actions={"thumbnail": 0, "title": 1},
        )
        assert result == "capped"

    def test_title_cap_at_non_title_checkpoint_does_not_cap(self):
        """Title cap is only relevant at TITLE_UPDATE_CHECKPOINTS (24h); a
        capped title must NOT block thumbnail-only action elsewhere."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=1,
            median_views=1000,
            sample_size=20,
            checkpoint="48h",
            prior_actions={"thumbnail": 0, "title": 1},
        )
        assert result == "thumbnail_regenerated"


class TestEvaluateActionColdStart:
    """cold_start: no retroactive evaluation below MIN_PRIOR_SNAPSHOTS."""

    def test_below_min_prior_snapshots_returns_cold_start(self):
        """GIVEN sample_size - 1 < MIN_PRIOR_SNAPSHOTS (10)
        WHEN evaluate_action runs
        THEN it returns 'cold_start' regardless of views/median."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=1,
            median_views=1000,
            sample_size=10,  # sample_size - 1 = 9 < 10
            checkpoint="24h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "cold_start"


class TestEvaluateActionOk:
    """ok: not underperforming, boundary case at exactly 50%."""

    def test_sufficient_sample_and_ok_views_returns_ok(self):
        """GIVEN sample_size - 1 >= 10 and views >= 50% of median
        WHEN evaluate_action runs
        THEN it returns 'ok'."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=600,
            median_views=1000,
            sample_size=11,  # sample_size - 1 = 10 == MIN_PRIOR_SNAPSHOTS
            checkpoint="48h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "ok"

    def test_exactly_fifty_percent_boundary_returns_ok(self):
        """GIVEN views == exactly 50% of the median
        WHEN evaluate_action runs
        THEN it returns 'ok', not underperforming (boundary case)."""
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=500,
            median_views=1000,
            sample_size=11,
            checkpoint="48h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "ok"


class TestEvaluateActionThumbnailOnly:
    """Non-24h underperformer: thumbnail_regenerated only."""

    def test_underperforming_48h_within_cap_returns_thumbnail_regenerated(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=100,
            median_views=1000,
            sample_size=11,
            checkpoint="48h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "thumbnail_regenerated"

    def test_underperforming_7d_within_cap_returns_thumbnail_regenerated(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=100,
            median_views=1000,
            sample_size=15,
            checkpoint="7d",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "thumbnail_regenerated"

    def test_underperforming_30d_within_cap_returns_thumbnail_regenerated(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=100,
            median_views=1000,
            sample_size=15,
            checkpoint="30d",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "thumbnail_regenerated"

    def test_underperforming_90d_within_cap_returns_thumbnail_regenerated(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=100,
            median_views=1000,
            sample_size=15,
            checkpoint="90d",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "thumbnail_regenerated"


class TestEvaluateActionThumbnailAndTitle:
    """24h underperformer: thumbnail_and_title_regenerated."""

    def test_underperforming_24h_within_cap_returns_thumbnail_and_title(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=100,
            median_views=1000,
            sample_size=11,
            checkpoint="24h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "thumbnail_and_title_regenerated"


class TestEvaluateActionZeroMedian:
    """Defensive: a zero/negative median must never raise a ZeroDivisionError
    and must not be treated as underperforming (nothing to compare against)."""

    def test_zero_median_returns_ok(self):
        from congress_videos.modules.video_analytics import evaluate_action

        result = evaluate_action(
            views=0,
            median_views=0,
            sample_size=11,
            checkpoint="48h",
            prior_actions={"thumbnail": 0, "title": 0},
        )
        assert result == "ok"


# ---------------------------------------------------------------------------
# Tests: CongressionalVideoDB Video Analytics Actions methods (issue #102)
# ---------------------------------------------------------------------------


class TestGetUnactionedSnapshots:
    """Spec: action_taken vocabulary and audit snapshot — candidate selection.

    get_unactioned_snapshots() returns NULL-action_taken rows joined to
    video_chapters for conf fields consumed downstream by
    _prepare_thumbnail_config-style regeneration.
    """

    def test_sql_filters_null_action_taken(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_unactioned_snapshots()

        sql_calls = _executed_sql(cur)
        assert len(sql_calls) >= 1
        assert "action_taken IS NULL" in sql_calls[0]

    def test_sql_joins_video_chapters(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_unactioned_snapshots()

        sql_calls = _executed_sql(cur)
        assert any("video_chapters" in s for s in sql_calls)

    def test_sql_aliases_title_as_chapter_title_and_joins_source_videos(self):
        """video_chapters has no chapter_title/session_number/session_date
        columns directly — title must be aliased, and session fields come
        from a join to youtube_source_videos (see production_schema.sql)."""
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_unactioned_snapshots()

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "vc.title AS chapter_title" in combined
        assert "youtube_source_videos" in combined
        assert "ysv.session_number" in combined
        assert "ysv.session_date" in combined

    def test_returns_list_with_conf_fields(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {
                "snapshot_id": 1,
                "chapter_id": 5,
                "youtube_video_id": "abc123",
                "checkpoint": "24h",
                "metrics": {"views": 100},
                "chapter_title": "Title",
                "description": "Desc",
                "session_number": 3,
                "session_date": None,
                "key_speakers": [],
                "resolved_participant_slug": None,
            }
        ]
        db, _ = _make_db(cur)

        result = db.get_unactioned_snapshots()

        assert isinstance(result, list)
        assert result[0]["snapshot_id"] == 1
        assert result[0]["chapter_title"] == "Title"


class TestGetCheckpointViewMedians:
    """Spec: single grouped-query median, self-included."""

    def test_sql_uses_percentile_cont_grouped_by_checkpoint(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_checkpoint_view_medians()

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls).upper()
        assert "PERCENTILE_CONT" in combined
        assert "GROUP BY CHECKPOINT" in combined

    def test_returns_dict_keyed_by_checkpoint(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"checkpoint": "24h", "median_views": 500.0, "sample_size": 42},
            {"checkpoint": "48h", "median_views": 700.0, "sample_size": 30},
        ]
        db, _ = _make_db(cur)

        result = db.get_checkpoint_view_medians()

        assert result["24h"]["median_views"] == 500.0
        assert result["24h"]["sample_size"] == 42
        assert result["48h"]["median_views"] == 700.0


class TestGetVideoActionHistory:
    """Spec: in_progress rows count as consumed cap slots alongside completed
    swap/title records."""

    def test_completed_thumbnail_swap_counts_as_one(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"youtube_video_id": "abc123", "checkpoint": "48h", "action_taken": "thumbnail_regenerated"},
        ]
        db, _ = _make_db(cur)

        result = db.get_video_action_history(["abc123"])

        assert result["abc123"]["thumbnail"] == 1
        assert result["abc123"]["title"] == 0

    def test_in_progress_row_counts_as_consumed_thumbnail_slot(self):
        """GIVEN a video has one action_taken='in_progress' row for a
        thumbnail action and no completed swap record
        WHEN get_video_action_history computes cap usage
        THEN the in_progress row counts as a consumed thumbnail-action slot."""
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"youtube_video_id": "abc123", "checkpoint": "48h", "action_taken": "in_progress"},
        ]
        db, _ = _make_db(cur)

        result = db.get_video_action_history(["abc123"])

        assert result["abc123"]["thumbnail"] == 1

    def test_in_progress_at_24h_counts_as_consumed_title_slot(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {"youtube_video_id": "abc123", "checkpoint": "24h", "action_taken": "in_progress"},
        ]
        db, _ = _make_db(cur)

        result = db.get_video_action_history(["abc123"])

        assert result["abc123"]["thumbnail"] == 1
        assert result["abc123"]["title"] == 1

    def test_thumbnail_and_title_regenerated_counts_both(self):
        cur = _make_cursor()
        cur.fetchall.return_value = [
            {
                "youtube_video_id": "abc123",
                "checkpoint": "24h",
                "action_taken": "thumbnail_and_title_regenerated",
            },
        ]
        db, _ = _make_db(cur)

        result = db.get_video_action_history(["abc123"])

        assert result["abc123"]["thumbnail"] == 1
        assert result["abc123"]["title"] == 1

    def test_video_with_no_rows_returns_zero_counts(self):
        cur = _make_cursor()
        cur.fetchall.return_value = []
        db, _ = _make_db(cur)

        result = db.get_video_action_history(["novideo"])

        assert result["novideo"] == {"thumbnail": 0, "title": 0}

    def test_empty_input_returns_empty_dict_without_db_call(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        result = db.get_video_action_history([])

        assert result == {}
        cur.execute.assert_not_called()


class TestGetChosenThumbnail:
    """Spec: Archetype persistence — read back the chosen thumbnail row."""

    def test_sql_filters_chapter_id_and_is_chosen(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.get_chosen_thumbnail(chapter_id=5)

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "is_chosen" in combined or "IS_CHOSEN" in combined.upper()

    def test_returns_row_including_archetype(self):
        cur = _make_cursor()
        cur.fetchone.return_value = {
            "thumbnail_id": 1,
            "chapter_id": 5,
            "label": "A",
            "prompt": "...",
            "openai_title": "Title",
            "archetype": "outrage",
            "is_chosen": True,
        }
        db, _ = _make_db(cur)

        result = db.get_chosen_thumbnail(chapter_id=5)

        assert result["archetype"] == "outrage"

    def test_no_chosen_row_returns_none(self):
        cur = _make_cursor()
        cur.fetchone.return_value = None
        db, _ = _make_db(cur)

        result = db.get_chosen_thumbnail(chapter_id=999)

        assert result is None


class TestClaimSnapshotAction:
    """Spec: Claim-before-act retry semantics — rowcount-gated claim."""

    def test_successful_claim_returns_true(self):
        cur = _make_cursor()
        cur.rowcount = 1
        db, _ = _make_db(cur)

        result = db.claim_snapshot_action(snapshot_id=42)

        assert result is True

    def test_sql_sets_in_progress_where_action_taken_is_null(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)

        db.claim_snapshot_action(snapshot_id=42)

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "in_progress" in combined
        assert "action_taken IS NULL" in combined

    def test_second_claim_on_same_row_returns_false(self):
        """A concurrent/second claim attempt on an already-claimed row must
        return False (rowcount=0)."""
        cur = _make_cursor()
        cur.rowcount = 0
        db, _ = _make_db(cur)

        result = db.claim_snapshot_action(snapshot_id=42)

        assert result is False


class TestMarkActionTaken:
    """Spec: action_taken vocabulary and audit snapshot — final write."""

    def test_writes_action_taken_and_action_detail(self):
        cur = _make_cursor()
        db, _ = _make_db(cur)
        detail = {"checkpoint": "24h", "views": 10, "median_views": 1000}

        db.mark_action_taken(
            snapshot_id=42, action="thumbnail_regenerated", detail=detail
        )

        sql_calls = _executed_sql(cur)
        combined = " ".join(sql_calls)
        assert "action_taken" in combined
        assert "action_detail" in combined

        all_params = []
        for call in cur.execute.call_args_list:
            if len(call.args) > 1:
                all_params.extend(call.args[1])
        assert "thumbnail_regenerated" in all_params
        assert 42 in all_params
        json_params = [p for p in all_params if isinstance(p, str) and "checkpoint" in p]
        assert len(json_params) >= 1
        assert json.loads(json_params[0])["checkpoint"] == "24h"
