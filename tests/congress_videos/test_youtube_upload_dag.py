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

    def test_schedule_is_three_times_daily(self):
        """DAG schedule is '0 11,14,17 * * *' — three runs per day (REQ-SCHED-01)."""
        from congress_videos.youtube_upload_dag import dag
        assert dag.schedule_interval == "0 11,14,17 * * *"

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


# ---------------------------------------------------------------------------
# THRESHOLD_BY_HOUR constant (REQ-THRESH-01/02/03)
# ---------------------------------------------------------------------------

class TestThresholdByHour:

    def test_constant_exists(self):
        """THRESHOLD_BY_HOUR is importable from the DAG module."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR
        assert THRESHOLD_BY_HOUR is not None

    def test_threshold_at_11(self):
        """11:00 threshold is 10 (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR
        assert THRESHOLD_BY_HOUR[11] == 10

    def test_threshold_at_14(self):
        """14:00 threshold is 20 (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR
        assert THRESHOLD_BY_HOUR[14] == 20

    def test_threshold_at_17(self):
        """17:00 threshold is 0 (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR
        assert THRESHOLD_BY_HOUR[17] == 0


# ---------------------------------------------------------------------------
# should_upload function (REQ-GATE-01, REQ-THRESH-01/02/03)
# ---------------------------------------------------------------------------

def _make_context_for_should_upload(queue_size: int, hour: int) -> dict:
    """Build a minimal Airflow context for should_upload tests."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    logical_date = datetime(2026, 7, 31, hour, 0, 0, tzinfo=timezone.utc)

    ti = MagicMock(name="TaskInstance")
    ti.xcom_pull.return_value = {"queue_size": queue_size, "uploads_today": 0}

    return {"ti": ti, "logical_date": logical_date}


class TestShouldUpload:

    # 11:00 — threshold 10
    def test_11_queue_10_is_false(self):
        """11:00, queue=10 → False (exactly at threshold, not above) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=10, hour=11)
        assert should_upload(**ctx) is False

    def test_11_queue_11_is_true(self):
        """11:00, queue=11 → True (above threshold) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        assert should_upload(**ctx) is True

    def test_11_queue_5_is_false(self):
        """11:00, queue=5 → False (below threshold) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=5, hour=11)
        assert should_upload(**ctx) is False

    # 14:00 — threshold 20
    def test_14_queue_20_is_false(self):
        """14:00, queue=20 → False (boundary — exactly at threshold) (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=20, hour=14)
        assert should_upload(**ctx) is False

    def test_14_queue_21_is_true(self):
        """14:00, queue=21 → True (above threshold) (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=21, hour=14)
        assert should_upload(**ctx) is True

    # 17:00 — threshold 0
    def test_17_queue_0_is_false(self):
        """17:00, queue=0 → False (threshold 0, not strictly above) (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=0, hour=17)
        assert should_upload(**ctx) is False

    def test_17_queue_1_is_true(self):
        """17:00, queue=1 → True (above threshold 0) (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=1, hour=17)
        assert should_upload(**ctx) is True

    # Unknown hour — defaults to threshold 0
    def test_unknown_hour_queue_0_is_false(self):
        """Unknown hour (e.g. 8), queue=0 → False (defaults to threshold 0)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=0, hour=8)
        assert should_upload(**ctx) is False

    def test_unknown_hour_queue_1_is_true(self):
        """Unknown hour (e.g. 8), queue=1 → True (above default threshold 0)."""
        from congress_videos.youtube_upload_dag import should_upload
        ctx = _make_context_for_should_upload(queue_size=1, hour=8)
        assert should_upload(**ctx) is True
