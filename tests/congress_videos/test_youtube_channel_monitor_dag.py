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
        assert dag.schedule == '0 * * * *'

    def test_dag_serializes_runs(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        assert dag.max_active_runs == 1

    def test_filter_unprocessed_videos_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "filter_unprocessed_videos" in task_ids


# ---------------------------------------------------------------------------
# Topology tests
# ---------------------------------------------------------------------------

class TestFilterUnprocessedVideosTopology:

    def test_sits_between_filter_plenary_and_finished_stream_guard(self):
        """filter_unprocessed_videos must be downstream of filter_plenary_sessions
        and upstream of the finished-stream guard
        (production path: t2 >> t2b >> t2_guard >> t2a)."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t2 = tasks_by_id["filter_plenary_sessions"]
        t2b = tasks_by_id["filter_unprocessed_videos"]
        guard = tasks_by_id["filter_finished_streams"]

        # t2 -> t2b
        assert t2b.task_id in {t.task_id for t in t2.downstream_list}
        # t2b -> t2_guard (guard now sits between t2b and check_if_plenary_found)
        assert guard.task_id in {t.task_id for t in t2b.downstream_list}

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


# ---------------------------------------------------------------------------
# Improvement #9 — dynamic task mapping for per-chunk summarization
# (t5f_flatten -> t5f_map (.partial().expand()) -> t5f aggregate)
# ---------------------------------------------------------------------------

class TestDynamicChunkSummarizationMapping:
    """Verify improvement #9: per-chunk summarization is fanned out via
    Airflow dynamic task mapping (.expand) instead of a single serial task."""

    def test_mapped_summarize_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "flatten_chunks_for_mapping" in task_ids
        assert "summarize_one_chunk" in task_ids
        assert "aggregate_chunk_summaries" in task_ids

    def test_summarize_one_chunk_is_a_mapped_task(self):
        """The summarization task must be an expanded/mapped operator, not a
        plain PythonOperator. Checked by class name to stay version-robust:
        Airflow 2.10 exposes MappedOperator under airflow.models.mappedoperator,
        Airflow 3.x under airflow.sdk.definitions.mappedoperator."""
        from congress_videos.youtube_channel_monitor_dag import dag
        mapped = dag.get_task("summarize_one_chunk")
        assert type(mapped).__name__ == "MappedOperator"

    def test_dynamic_mapping_wiring_is_present(self):
        """Structural wiring: flatten -> map -> aggregate."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        flatten = tasks_by_id["flatten_chunks_for_mapping"]
        mapped = tasks_by_id["summarize_one_chunk"]
        aggregate = tasks_by_id["aggregate_chunk_summaries"]

        # flatten -> map
        assert mapped.task_id in {t.task_id for t in flatten.downstream_list}
        # map -> aggregate
        assert aggregate.task_id in {t.task_id for t in mapped.downstream_list}


# ---------------------------------------------------------------------------
# finished-stream-guard (F.1) — filter_finished_streams topology + params
# ---------------------------------------------------------------------------

class TestFilterFinishedStreamsTopology:

    def test_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "filter_finished_streams" in task_ids

    def test_sits_between_filter_unprocessed_and_check_if_plenary_found(self):
        """Production path: t2b >> t2_guard >> t2a."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t2b = tasks_by_id["filter_unprocessed_videos"]
        guard = tasks_by_id["filter_finished_streams"]
        t2a = tasks_by_id["check_if_plenary_found"]

        # t2b -> guard
        assert guard.task_id in {t.task_id for t in t2b.downstream_list}
        # guard -> t2a
        assert t2a.task_id in {t.task_id for t in guard.downstream_list}

    def test_not_on_test_mode_path(self):
        """The test-mode path must NOT reach filter_finished_streams."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t0_test = tasks_by_id["create_test_video_data"]
        downstream_ids = {t.task_id for t in t0_test.get_flat_relatives(upstream=False)}
        assert "filter_finished_streams" not in downstream_ids

    def test_guard_params_present_with_defaults(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        params = dag.params
        assert "guard_enabled" in params
        assert "guard_floor_minutes" in params
        # ParamsDict.__getitem__ resolves to the raw value
        assert bool(params["guard_enabled"]) is True
        assert int(params["guard_floor_minutes"]) == 10

    def test_min_hours_since_end_param_unchanged(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        assert int(dag.params["min_hours_since_end"]) == 2

    def test_empty_guard_result_routes_to_no_plenary_sessions(self):
        """When the guard drops every candidate (total_matches == 0), the
        downstream branch must route to 'no_plenary_sessions'."""
        from unittest.mock import MagicMock
        from congress_videos.youtube_channel_monitor_dag import dag

        branch = {t.task_id: t for t in dag.tasks}["check_if_plenary_found"]
        ti = MagicMock()
        ti.xcom_pull.return_value = {
            "total_matches": 0,
            "videos": [],
            "target_date": "2025-10-08",
        }

        assert branch.python_callable(ti) == "no_plenary_sessions"
