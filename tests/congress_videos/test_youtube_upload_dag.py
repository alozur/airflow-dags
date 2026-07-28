"""Tests for congress_youtube_chapter_uploader DAG (congress_videos.youtube_upload_dag)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def _make_ti(xcom_store: dict | None = None):
    """Return a TaskInstance double with an in-memory XCom store."""
    store: dict = xcom_store or {}
    ti = MagicMock(name="TaskInstance")
    ti.xcom_store = store

    def _push(key: str, value, **_kw) -> None:
        store[key] = value

    def _pull(key: str | None = None, **_kw):
        if key is None:
            return None
        return store.get(key)

    ti.xcom_push.side_effect = _push
    ti.xcom_pull.side_effect = _pull
    return ti


# ---------------------------------------------------------------------------
# DAG load + dependency chain
# ---------------------------------------------------------------------------

class TestYoutubeUploadDagLoads:

    def test_dag_loads(self):
        from congress_videos.youtube_upload_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_youtube_chapter_uploader"

    def test_dag_has_twelve_tasks(self):
        from congress_videos.youtube_upload_dag import dag
        assert len(dag.tasks) == 12

    def test_expected_task_ids_present(self):
        from congress_videos.youtube_upload_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "trigger_youtube_upload" in task_ids
        assert "mark_chapters_uploaded" in task_ids
        assert "check_upload_failures" in task_ids

    def test_chain_t7_t8_t9(self):
        from congress_videos.youtube_upload_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t7 = tasks_by_id["trigger_youtube_upload"]
        t8 = tasks_by_id["mark_chapters_uploaded"]
        t9 = tasks_by_id["check_upload_failures"]

        assert t8.task_id in {t.task_id for t in t7.downstream_list}
        assert t9.task_id in {t.task_id for t in t8.downstream_list}


# ---------------------------------------------------------------------------
# _check_upload_failures
# ---------------------------------------------------------------------------

class TestCheckUploadFailures:

    def test_raises_on_recorded_failures(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 1, "failed_updates": 0}})
        with pytest.raises(Exception, match="Chapter upload failures"):
            _check_upload_failures(ti)

    def test_raises_on_failed_updates(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 2}})
        with pytest.raises(Exception, match="Chapter upload failures"):
            _check_upload_failures(ti)

    def test_raises_on_missing_xcom(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({})
        with pytest.raises(Exception, match="chapter_upload_updates XCom missing"):
            _check_upload_failures(ti)

    def test_noop_on_zeros(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0}})
        _check_upload_failures(ti)  # should not raise

    def test_noop_on_empty_payload_lacking_recorded_failures(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"updated_chapters": 0, "failed_updates": 0, "details": []}})
        _check_upload_failures(ti)  # should not raise


# ---------------------------------------------------------------------------
# trigger_upload_with_config (t7)
# ---------------------------------------------------------------------------

class TestTriggerUploadWithConfig:

    def test_no_raise_on_child_failure_and_pushes_fallback(self, mocker):
        from congress_videos.youtube_upload_dag import trigger_upload_with_config

        mock_run = MagicMock()
        mock_run.run_id = "failed_run_001"
        mock_run.state = "failed"
        mock_run.execution_date = "2026-07-28T00:00:00+00:00"
        mock_run.refresh_from_db = MagicMock()

        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=mock_run)
        mocker.patch("time.sleep")
        mock_xcom = mocker.patch("airflow.models.XCom")
        mock_xcom.get_many.return_value = []

        ti = _make_ti({
            "upload_config": {
                "videos": [
                    {"chapter_id": "c-1", "video_id": "v-1", "video_file": "/c1.mp4"},
                    {"chapter_id": "c-2", "video_id": "v-2", "video_file": "/c2.mp4"},
                ]
            }
        })

        result = trigger_upload_with_config(ti, run_id="test_run")

        assert result == "failed_run_001"
        fallback = ti.xcom_store["upload_results"]
        assert len(fallback["upload_details"]) == 2
        assert all(d["success"] is False for d in fallback["upload_details"])
