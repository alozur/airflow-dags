"""Tests for congress_participants_sync DAG — Slice 2 (T-15 through T-16)."""

from __future__ import annotations

from datetime import timedelta

import pytest

# ===========================================================================
# T-15 RED: DAG structure tests
# ===========================================================================


class TestCongressParticipantsSyncDAGLoads:
    """Verify the DAG module loads without import errors and has correct metadata."""

    def test_dag_loads_without_import_errors(self):
        """Importing the module must not raise ImportError or DAG parse error."""
        from congress_videos.congress_participants_sync_dag import dag
        assert dag is not None

    def test_dag_id(self):
        """DAG id is 'congress_participants_sync'."""
        from congress_videos.congress_participants_sync_dag import dag
        assert dag.dag_id == "congress_participants_sync"

    def test_dag_schedule_is_weekly_monday(self):
        """DAG schedule is '0 3 * * 1' (Monday 03:00 weekly)."""
        from congress_videos.congress_participants_sync_dag import dag
        assert dag.schedule_interval == "0 3 * * 1"

    def test_dag_catchup_is_false(self):
        """catchup=False — no backfill on first enable."""
        from congress_videos.congress_participants_sync_dag import dag
        assert dag.catchup is False

    def test_dag_max_active_runs_is_one(self):
        """max_active_runs=1 — serializes concurrent runs."""
        from congress_videos.congress_participants_sync_dag import dag
        assert dag.max_active_runs == 1


class TestCongressParticipantsSyncDAGTasks:
    """Verify the task graph: exactly 4 tasks with correct IDs."""

    def test_dag_has_exactly_four_tasks(self):
        """DAG contains exactly 4 tasks."""
        from congress_videos.congress_participants_sync_dag import dag
        assert len(dag.tasks) == 4

    def test_task_id_fill_congreso_photo_fallback_exists(self):
        """Task 'fill_congreso_photo_fallback' is present."""
        from congress_videos.congress_participants_sync_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "fill_congreso_photo_fallback" in task_ids

    def test_task_id_fetch_and_parse_exists(self):
        """Task 'fetch_and_parse' is present."""
        from congress_videos.congress_participants_sync_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "fetch_and_parse" in task_ids

    def test_task_id_upsert_participants_exists(self):
        """Task 'upsert_participants' is present."""
        from congress_videos.congress_participants_sync_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "upsert_participants" in task_ids

    def test_task_id_enrich_missing_photos_exists(self):
        """Task 'enrich_missing_photos' is present."""
        from congress_videos.congress_participants_sync_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "enrich_missing_photos" in task_ids


class TestCongressParticipantsSyncDAGDependencies:
    """Verify the linear dependency chain: t1 >> t2 >> t3."""

    def test_fetch_and_parse_is_upstream_of_upsert(self):
        """fetch_and_parse >> upsert_participants."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        fetch_task = tasks["fetch_and_parse"]
        downstream_ids = {t.task_id for t in fetch_task.downstream_list}
        assert "upsert_participants" in downstream_ids

    def test_upsert_is_upstream_of_enrich(self):
        """upsert_participants >> enrich_missing_photos."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        upsert_task = tasks["upsert_participants"]
        downstream_ids = {t.task_id for t in upsert_task.downstream_list}
        assert "enrich_missing_photos" in downstream_ids

    def test_fetch_is_not_direct_upstream_of_enrich(self):
        """fetch_and_parse is NOT a direct upstream of enrich_missing_photos (linear, not fan-out)."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        fetch_task = tasks["fetch_and_parse"]
        direct_downstream_ids = {t.task_id for t in fetch_task.downstream_list}
        assert "enrich_missing_photos" not in direct_downstream_ids

    def test_enrich_is_upstream_of_fill_congreso(self):
        """enrich_missing_photos >> fill_congreso_photo_fallback."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        enrich_task = tasks["enrich_missing_photos"]
        downstream_ids = {t.task_id for t in enrich_task.downstream_list}
        assert "fill_congreso_photo_fallback" in downstream_ids

    def test_fill_congreso_has_no_downstream(self):
        """fill_congreso_photo_fallback is the terminal task — no downstream tasks."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        fill_task = tasks["fill_congreso_photo_fallback"]
        assert len(fill_task.downstream_list) == 0

    def test_fill_congreso_sole_upstream_is_enrich(self):
        """fill_congreso_photo_fallback has exactly one upstream: enrich_missing_photos."""
        from congress_videos.congress_participants_sync_dag import dag
        tasks = {t.task_id: t for t in dag.tasks}
        fill_task = tasks["fill_congreso_photo_fallback"]
        upstream_ids = {t.task_id for t in fill_task.upstream_list}
        assert upstream_ids == {"enrich_missing_photos"}


class TestCongressParticipantsSyncDAGDefaultArgs:
    """Verify default_args retries and retry_delay for Wikidata rate-limit tolerance."""

    def test_retries_is_three(self):
        """default_args retries == 3."""
        from congress_videos.congress_participants_sync_dag import default_args
        assert default_args["retries"] == 3

    def test_retry_delay_is_ten_minutes(self):
        """default_args retry_delay == timedelta(minutes=10)."""
        from congress_videos.congress_participants_sync_dag import default_args
        assert default_args["retry_delay"] == timedelta(minutes=10)

    def test_catchup_not_in_default_args(self):
        """catchup is set at DAG level, not in default_args (to keep it clean)."""
        from congress_videos.congress_participants_sync_dag import default_args
        assert "catchup" not in default_args
