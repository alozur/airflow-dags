"""Tests for congress_youtube_channel_monitor DAG
(congress_videos.youtube_channel_monitor_dag)."""

from __future__ import annotations


# ---------------------------------------------------------------------------
# DAG load tests
# ---------------------------------------------------------------------------

class TestCongressYoutubeChannelMonitorDAGLoads:

    def test_dag_loads(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_youtube_channel_monitor"

    def test_dag_has_correct_schedule(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        assert dag.schedule == '0 22 * * *'

    def test_filter_unprocessed_videos_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "filter_unprocessed_videos" in task_ids


# ---------------------------------------------------------------------------
# Topology tests
# ---------------------------------------------------------------------------

class TestFilterUnprocessedVideosTopology:

    def test_sits_between_filter_plenary_and_check_if_plenary_found(self):
        """filter_unprocessed_videos must be downstream of filter_plenary_sessions
        and upstream of check_if_plenary_found (production path: t2 >> t2b >> t2a)."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t2 = tasks_by_id["filter_plenary_sessions"]
        t2b = tasks_by_id["filter_unprocessed_videos"]
        t2a = tasks_by_id["check_if_plenary_found"]

        # t2 -> t2b
        assert t2b.task_id in {t.task_id for t in t2.downstream_list}
        # t2b -> t2a
        assert t2a.task_id in {t.task_id for t in t2b.downstream_list}

    def test_not_on_test_mode_path(self):
        """The test-mode path (create_test_video_data >> [t3a, t3b]) must NOT
        reach filter_unprocessed_videos."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t0_test = tasks_by_id["create_test_video_data"]
        t2b = tasks_by_id["filter_unprocessed_videos"]

        # filter_unprocessed_videos is not a direct downstream of the test task
        assert "filter_unprocessed_videos" not in {t.task_id for t in t0_test.downstream_list}
        # nor anywhere in the test task's transitive downstream set
        downstream_ids = {t.task_id for t in t0_test.get_flat_relatives(upstream=False)}
        assert "filter_unprocessed_videos" not in downstream_ids

        # and the test task is not upstream of t2b
        upstream_ids = {t.task_id for t in t2b.get_flat_relatives(upstream=True)}
        assert "create_test_video_data" not in upstream_ids
