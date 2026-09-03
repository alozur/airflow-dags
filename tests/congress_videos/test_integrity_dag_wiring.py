"""F1 [RED] Test: DAG task dependency graph for integrity gate.

Scenarios (from spec — DAG Task Wiring for Integrity Gate):
- t3c2_integrity is a direct downstream of t3c2
- t3c2_integrity is a direct upstream of t_trim
- t3c2 is NOT a direct upstream of t_trim
- DAG loads without import errors (catches missing __init__.py registration from C3)
"""

from __future__ import annotations

from unittest.mock import MagicMock


class TestIntegrityTaskExistsInDAG:
    def test_dag_loads_without_import_errors(self):
        """DAG must load cleanly — catches missing __init__ registration for
        check_source_video_integrity (task C3 is a prerequisite)."""
        from congress_videos.youtube_channel_monitor_dag import dag

        assert dag is not None
        assert dag.dag_id == "congress_youtube_channel_monitor"

    def test_check_source_integrity_task_exists(self):
        """A task with task_id='check_source_integrity' must be present in the DAG."""
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "check_source_integrity" in task_ids, "DAG must contain a 'check_source_integrity' task (t3c2_integrity)"


class TestIntegrityTaskTopology:
    def test_t3c2_integrity_is_direct_downstream_of_t3c2(self):
        """check_source_integrity must be a direct downstream of download_video_from_youtube."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t3c2 = tasks_by_id["download_video_from_youtube"]
        t3c2_integrity = tasks_by_id["check_source_integrity"]

        direct_downstream_ids = {t.task_id for t in t3c2.downstream_list}
        assert t3c2_integrity.task_id in direct_downstream_ids, (
            "check_source_integrity must be a direct downstream of download_video_from_youtube"
        )

    def test_t3c2_integrity_is_direct_upstream_of_t_trim(self):
        """check_source_integrity must be a direct upstream of trim_chapter_silence."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t3c2_integrity = tasks_by_id["check_source_integrity"]
        t_trim = tasks_by_id["trim_chapter_silence"]

        direct_upstream_of_trim = {t.task_id for t in t_trim.upstream_list}
        assert t3c2_integrity.task_id in direct_upstream_of_trim, (
            "check_source_integrity must be a direct upstream of trim_chapter_silence"
        )

    def test_t3c2_is_not_direct_upstream_of_t_trim(self):
        """download_video_from_youtube must NOT be a direct upstream of trim_chapter_silence
        (the integrity gate now sits between them)."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t3c2 = tasks_by_id["download_video_from_youtube"]
        t_trim = tasks_by_id["trim_chapter_silence"]

        direct_upstream_of_trim = {t.task_id for t in t_trim.upstream_list}
        assert t3c2.task_id not in direct_upstream_of_trim, (
            "download_video_from_youtube must NOT be a direct upstream of trim_chapter_silence "
            "(check_source_integrity gates that edge)"
        )

    def test_t_trim_trigger_rule_is_all_done(self):
        """trim_chapter_silence must keep trigger_rule='all_done' (best-effort VAD)."""
        from congress_videos.youtube_channel_monitor_dag import dag

        t_trim = dag.get_task("trim_chapter_silence")
        assert str(t_trim.trigger_rule) == "all_done", (
            f"t_trim trigger_rule must be 'all_done', got {t_trim.trigger_rule!r}"
        )

    def test_integrity_task_trigger_rule_is_none_failed_min_one_success(self):
        """check_source_integrity must use trigger_rule='none_failed_min_one_success'."""
        from congress_videos.youtube_channel_monitor_dag import dag

        t3c2_integrity = dag.get_task("check_source_integrity")
        assert str(t3c2_integrity.trigger_rule) == "none_failed_min_one_success", (
            f"check_source_integrity trigger_rule must be 'none_failed_min_one_success', "
            f"got {t3c2_integrity.trigger_rule!r}"
        )


class TestCheckSourceIntegrityCallable:
    def test_calls_batch_method_once_instead_of_per_id(self, mocker):
        """_check_source_integrity must call record_source_integrity_failures once
        for the whole batch, not record_source_integrity_failure per id."""
        from congress_videos.youtube_channel_monitor_dag import _check_source_integrity

        mocker.patch(
            "congress_videos.modules.youtube.check_source_video_integrity",
            return_value={"total_downloaded": 3, "videos": [], "failed_video_ids": ["a", "b", "c"]},
        )
        mock_db_cls = mocker.patch("congress_videos.modules.database.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = MagicMock()
        ti.xcom_pull.return_value = [{"video_id": "a"}, {"video_id": "b"}, {"video_id": "c"}]

        _check_source_integrity(ti)

        mock_db.record_source_integrity_failures.assert_called_once_with(["a", "b", "c"], retry_after_hours=12)
        mock_db.record_source_integrity_failure.assert_not_called()
