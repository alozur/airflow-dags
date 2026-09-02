"""Tests for congress_videos.thumbnail_republish_dag (issue #331).

Covers DAG load hygiene, task graph shape, and the staleness guard's 180m
tolerance (DD6). heal_thumbnails/select_thumbnail_republish_candidates
callable coverage (status dispatch, error isolation, per-run cap, XCom)
ships in a follow-up commit — see apply-progress sdd/thumbnail-republish-healer/apply-progress.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# DAG load + hygiene
# ---------------------------------------------------------------------------


class TestThumbnailRepublishDagLoads:
    """DAG must load without import errors and appear in DagBag."""

    def test_dag_has_no_import_errors(self):
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "thumbnail_republish" not in bag.import_errors

    def test_dag_object_is_defined(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag is not None

    def test_dag_id(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.dag_id == "thumbnail_republish"

    def test_schedule_is_daily_at_15_utc(self):
        """DD6: schedule is a literal 15:00 UTC cron, not bare @daily."""
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.schedule_interval == "0 15 * * *"

    def test_catchup_is_false(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.catchup is False

    def test_tags_present(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert "congress" in dag.tags
        assert "youtube" in dag.tags


# ---------------------------------------------------------------------------
# Task graph shape
# ---------------------------------------------------------------------------


class TestThumbnailRepublishDagGraph:
    """Task graph: staleness_guard -> select -> heal."""

    def test_has_three_tasks(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert len(dag.tasks) == 3

    def test_expected_task_ids_present(self):
        from congress_videos.thumbnail_republish_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "staleness_guard" in task_ids
        assert "select_thumbnail_republish_candidates" in task_ids
        assert "heal_thumbnails" in task_ids

    def test_staleness_guard_upstream_of_select(self):
        from congress_videos.thumbnail_republish_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        guard = tasks_by_id["staleness_guard"]
        select = tasks_by_id["select_thumbnail_republish_candidates"]
        assert select.task_id in {t.task_id for t in guard.downstream_list}

    def test_select_upstream_of_heal(self):
        from congress_videos.thumbnail_republish_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        select = tasks_by_id["select_thumbnail_republish_candidates"]
        heal = tasks_by_id["heal_thumbnails"]
        assert heal.task_id in {t.task_id for t in select.downstream_list}


# ---------------------------------------------------------------------------
# _staleness_guard: 180m tolerance (DD6), not the hourly DAG's 30m
# ---------------------------------------------------------------------------


class TestStalenessGuard:
    """Spec: staleness_guard skips replays stale beyond 180m tolerance."""

    def test_fresh_run_returns_true(self):
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        assert _staleness_guard(data_interval_end=datetime.now(timezone.utc)) is True

    def test_no_data_interval_end_returns_true(self):
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        assert _staleness_guard() is True

    def test_within_180m_tolerance_returns_true(self):
        """150 minutes ago is still within the 180m tolerance -- must proceed."""
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        recent = datetime.now(timezone.utc) - timedelta(minutes=150)
        assert _staleness_guard(data_interval_end=recent) is True

    def test_beyond_180m_tolerance_returns_false(self):
        """4 hours ago exceeds the 180m tolerance -- must skip."""
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        stale = datetime.now(timezone.utc) - timedelta(hours=4)
        assert _staleness_guard(data_interval_end=stale) is False
