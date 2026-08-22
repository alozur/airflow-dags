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
        assert dag.schedule_interval == '0 * * * *'

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

    def test_min_hours_since_end_param_default_is_12(self):
        """min_hours_since_end raised from 2 to 12 (fix-video-integrity #24)."""
        from congress_videos.youtube_channel_monitor_dag import dag
        assert int(dag.params["min_hours_since_end"]) == 12

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


# ---------------------------------------------------------------------------
# TASK 8 (RED) — t_normalize_speakers topology
# ---------------------------------------------------------------------------

class TestNormalizeSpeakersTask:
    """TASK 8 — t_normalize_speakers exists, is wired after save_chapters_to_db,
    and has trigger_rule='all_done'."""

    def test_normalize_speakers_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "normalize_speakers" in task_ids, (
            "Expected task_id 'normalize_speakers' in DAG tasks"
        )

    def test_normalize_speakers_trigger_rule_is_all_done(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t = tasks_by_id["normalize_speakers"]
        assert str(t.trigger_rule) == "all_done", (
            f"Expected trigger_rule='all_done', got {t.trigger_rule!r}"
        )

    def test_normalize_speakers_is_downstream_of_save_chapters_to_db(self):
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t9_db = tasks_by_id["save_chapters_to_db"]
        normalize = tasks_by_id["normalize_speakers"]

        downstream_ids = {t.task_id for t in t9_db.downstream_list}
        assert normalize.task_id in downstream_ids, (
            "'normalize_speakers' must be a direct downstream of 'save_chapters_to_db'"
        )


# ---------------------------------------------------------------------------
# Monitor fire-and-forget trigger for speaker_turns_dag (issue #117)
# ---------------------------------------------------------------------------

def _make_ti_monitor(xcom_store: dict | None = None):
    """Return a TaskInstance double with an in-memory XCom store."""
    from unittest.mock import MagicMock
    store: dict = xcom_store or {}
    ti = MagicMock(name="TaskInstance")
    ti.xcom_store = store

    def _push(key, value, **_kw):
        store[key] = value

    def _pull(key=None, **_kw):
        if key is None:
            return None
        return store.get(key)

    ti.xcom_push.side_effect = _push
    ti.xcom_pull.side_effect = _pull
    return ti


class TestMonitorTriggerRefinementTask:
    """t_trigger_refinement topology tests (structural, no DB)."""

    def test_trigger_refinement_task_exists(self):
        """trigger_refinement task must exist in the monitor DAG."""
        from congress_videos.youtube_channel_monitor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "trigger_refinement" in task_ids, (
            "Expected task_id 'trigger_refinement' in monitor DAG tasks"
        )

    def test_trigger_refinement_is_downstream_of_normalize_speakers(self):
        """trigger_refinement must be wired after normalize_speakers."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}

        normalize = tasks_by_id["normalize_speakers"]
        trigger = tasks_by_id["trigger_refinement"]

        downstream_ids = {t.task_id for t in normalize.downstream_list}
        assert trigger.task_id in downstream_ids, (
            "'trigger_refinement' must be a direct downstream of 'normalize_speakers'"
        )

    def test_trigger_refinement_trigger_rule_is_all_done(self):
        """trigger_refinement must use trigger_rule='all_done' so it runs even when
        normalize_speakers short-circuits."""
        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t = tasks_by_id["trigger_refinement"]
        assert str(t.trigger_rule) == "all_done", (
            f"Expected trigger_rule='all_done', got {t.trigger_rule!r}"
        )

    def test_dag_import_is_clean(self):
        """DAG must import without errors after adding trigger_dag_api import."""
        from congress_videos.youtube_channel_monitor_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_youtube_channel_monitor"


class TestMonitorTriggerRefinementCallable:
    """Callable unit tests: fire-and-forget, error swallowing, zero-chapters skip."""

    def test_trigger_called_when_chapters_saved(self, mocker):
        """trigger_dag_api called when db_save_results.total_chapters_saved > 0."""
        from congress_videos.youtube_channel_monitor_dag import _trigger_refinement

        mock_trigger = mocker.patch(
            "congress_videos.youtube_channel_monitor_dag.trigger_dag_api"
        )
        ti = _make_ti_monitor(
            {"db_save_results": {"total_chapters_saved": 3, "total_videos_saved": 1}}
        )

        _trigger_refinement(ti=ti)

        mock_trigger.assert_called_once()
        call_kwargs = mock_trigger.call_args
        # Must target speaker_turns_dag
        assert call_kwargs[1].get("dag_id") == "speaker_turns_dag" or (
            len(call_kwargs[0]) > 0 and call_kwargs[0][0] == "speaker_turns_dag"
        )

    def test_trigger_not_called_when_zero_chapters_saved(self, mocker):
        """trigger_dag_api NOT called when total_chapters_saved == 0."""
        from congress_videos.youtube_channel_monitor_dag import _trigger_refinement

        mock_trigger = mocker.patch(
            "congress_videos.youtube_channel_monitor_dag.trigger_dag_api"
        )
        ti = _make_ti_monitor(
            {"db_save_results": {"total_chapters_saved": 0, "total_videos_saved": 0}}
        )

        _trigger_refinement(ti=ti)

        mock_trigger.assert_not_called()

    def test_trigger_not_called_when_no_db_save_results(self, mocker):
        """trigger_dag_api NOT called when db_save_results XCom is absent."""
        from congress_videos.youtube_channel_monitor_dag import _trigger_refinement

        mock_trigger = mocker.patch(
            "congress_videos.youtube_channel_monitor_dag.trigger_dag_api"
        )
        ti = _make_ti_monitor({})  # no db_save_results key

        _trigger_refinement(ti=ti)

        mock_trigger.assert_not_called()

    def test_trigger_exception_is_swallowed(self, mocker):
        """trigger_dag_api raising an exception must NOT propagate — task succeeds."""
        from congress_videos.youtube_channel_monitor_dag import _trigger_refinement

        mocker.patch(
            "congress_videos.youtube_channel_monitor_dag.trigger_dag_api",
            side_effect=Exception("Network error"),
        )
        ti = _make_ti_monitor(
            {"db_save_results": {"total_chapters_saved": 2, "total_videos_saved": 1}}
        )

        # Must not raise — fire-and-forget
        _trigger_refinement(ti=ti)
