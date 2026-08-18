"""Tests for congress_videos.video_analytics_dag.

Spec: DAG Shape and Scheduling / No Public YouTube Writes.
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# DAG load + structure
# ---------------------------------------------------------------------------


class TestVideoAnalyticsDagLoads:
    """DAG must load without errors and appear in DagBag."""

    def test_dag_is_importable(self):
        """DagBag must have zero import errors for video_analytics_dag."""
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "video_analytics" not in bag.import_errors

    def test_dag_object_is_defined(self):
        """The module must expose a 'dag' object."""
        from congress_videos.video_analytics_dag import dag

        assert dag is not None

    def test_dag_id(self):
        """dag_id must be 'video_analytics'."""
        from congress_videos.video_analytics_dag import dag

        assert dag.dag_id == "video_analytics"

    def test_schedule_is_hourly(self):
        """schedule_interval must be '@hourly'."""
        from congress_videos.video_analytics_dag import dag

        assert dag.schedule_interval == "@hourly"

    def test_catchup_is_false(self):
        """catchup must be False to avoid backfill storms."""
        from congress_videos.video_analytics_dag import dag

        assert dag.catchup is False


# ---------------------------------------------------------------------------
# Task graph shape
# ---------------------------------------------------------------------------


class TestVideoAnalyticsDagGraph:
    """Task graph must have staleness guard → pending select → fetch → record."""

    def test_has_four_tasks(self):
        """DAG must have exactly 4 tasks: t0, t1, t2, t3."""
        from congress_videos.video_analytics_dag import dag

        assert len(dag.tasks) == 4

    def test_expected_task_ids_present(self):
        """Required task IDs must be present in the DAG."""
        from congress_videos.video_analytics_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "staleness_guard" in task_ids
        assert "get_pending_checkpoints" in task_ids
        assert "fetch_analytics" in task_ids
        assert "record_snapshots" in task_ids

    def test_staleness_guard_is_upstream_of_get_pending(self):
        """staleness_guard → get_pending_checkpoints."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t0 = tasks_by_id["staleness_guard"]
        t1 = tasks_by_id["get_pending_checkpoints"]

        downstream_ids = {t.task_id for t in t0.downstream_list}
        assert t1.task_id in downstream_ids

    def test_get_pending_is_upstream_of_fetch(self):
        """get_pending_checkpoints → fetch_analytics."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t1 = tasks_by_id["get_pending_checkpoints"]
        t2 = tasks_by_id["fetch_analytics"]

        downstream_ids = {t.task_id for t in t1.downstream_list}
        assert t2.task_id in downstream_ids

    def test_fetch_is_upstream_of_record(self):
        """fetch_analytics → record_snapshots."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t2 = tasks_by_id["fetch_analytics"]
        t3 = tasks_by_id["record_snapshots"]

        downstream_ids = {t.task_id for t in t2.downstream_list}
        assert t3.task_id in downstream_ids


# ---------------------------------------------------------------------------
# _fetch_analytics wiring: collected pairs from DB
# ---------------------------------------------------------------------------


class TestFetchAnalyticsUsesCollectedPairs:
    """_fetch_analytics must query the DB for already-collected pairs and pass
    them to pending_checkpoints() instead of using an always-empty set.

    This saves Analytics API quota by skipping pairs we already have.
    """

    def test_fetch_analytics_calls_get_collected_pairs(self, monkeypatch):
        """GIVEN candidate rows with youtube_video_ids
        WHEN _fetch_analytics executes
        THEN get_collected_analytics_pairs is called with those ids."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "abc123",
                "youtube_upload_date": None,
            }
        ]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        get_collected_calls = []

        def fake_get_collected(ids):
            get_collected_calls.append(ids)
            return {("abc123", "24h")}  # pretend 24h already collected

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
            side_effect=fake_get_collected,
        ), patch(
            "congress_videos.modules.video_analytics.pending_checkpoints",
            return_value=[],
        ) as mock_pending:
            _fetch_analytics(ti=mock_ti)

        # get_collected_analytics_pairs must have been called
        assert len(get_collected_calls) == 1
        assert "abc123" in get_collected_calls[0]

    def test_fetch_analytics_passes_collected_set_to_pending_checkpoints(
        self, monkeypatch
    ):
        """The collected set from DB must be forwarded as the 'collected'
        argument to pending_checkpoints(), NOT an empty set."""
        from unittest.mock import MagicMock, call, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 2,
                "youtube_video_id": "xyz999",
                "youtube_upload_date": None,
            }
        ]
        already_collected = {("xyz999", "24h"), ("xyz999", "48h")}

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        captured_pending_calls = []

        def fake_pending(now, videos, collected):
            captured_pending_calls.append(collected)
            return []

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
            return_value=already_collected,
        ), patch(
            "congress_videos.modules.video_analytics.pending_checkpoints",
            side_effect=fake_pending,
        ):
            _fetch_analytics(ti=mock_ti)

        # pending_checkpoints must have received the real collected set, not set()
        assert len(captured_pending_calls) == 1
        assert captured_pending_calls[0] == already_collected
