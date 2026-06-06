"""Tests for congress_reap_processor DAG (congress_videos.reap_processor_dag)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowException

from congress_videos.reap_api import ReapCreditsExhausted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_ti(xcom_store: dict | None = None) -> MagicMock:
    """Create a minimal Airflow TaskInstance double with in-memory XCom."""
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


def _make_context(xcom_store: dict | None = None) -> dict:
    return {"ti": _make_ti(xcom_store)}


# ---------------------------------------------------------------------------
# DAG load tests
# ---------------------------------------------------------------------------

class TestCongressReapProcessorDAGLoads:

    def test_dag_loads(self):
        from congress_videos.reap_processor_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_reap_processor"

    def test_dag_has_correct_task_count(self):
        from congress_videos.reap_processor_dag import dag
        # Tasks: claim_clip_from_queue, upload_to_reap, create_reap_job,
        #        wait_for_reap, check_credits_status
        assert len(dag.tasks) == 5

    def test_dag_has_correct_schedule(self):
        from congress_videos.reap_processor_dag import dag
        assert dag.schedule == '0 9 * * *'

    def test_dag_has_max_active_runs_1(self):
        from congress_videos.reap_processor_dag import dag
        assert dag.max_active_runs == 1

    def test_dag_correct_task_ids(self):
        from congress_videos.reap_processor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "claim_clip_from_queue" in task_ids
        assert "upload_to_reap" in task_ids
        assert "create_reap_job" in task_ids
        assert "wait_for_reap" in task_ids
        assert "check_credits_status" in task_ids
        assert "load_clip_results" not in task_ids

    def test_dag_correct_dependency_chain(self):
        from congress_videos.reap_processor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t0 = tasks_by_id["claim_clip_from_queue"]
        t1 = tasks_by_id["upload_to_reap"]
        t2 = tasks_by_id["create_reap_job"]
        t3 = tasks_by_id["wait_for_reap"]
        t4 = tasks_by_id["check_credits_status"]

        assert t1.task_id in {t.task_id for t in t0.downstream_list}
        assert t2.task_id in {t.task_id for t in t1.downstream_list}
        assert t3.task_id in {t.task_id for t in t2.downstream_list}
        assert t4.task_id in {t.task_id for t in t3.downstream_list}


# ---------------------------------------------------------------------------
# TestClaimClipFromQueue
# ---------------------------------------------------------------------------

class TestClaimClipFromQueue:

    def test_empty_result_returns_false(self, mocker):
        from congress_videos.reap_processor_dag import _claim_clip_from_queue

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db_cls.return_value.claim_pending_clip.return_value = None

        ti = _make_ti()
        result = _claim_clip_from_queue(ti)

        assert result is False
        assert "claimed_clip" not in ti.xcom_store

    def test_non_empty_result_returns_true(self, mocker):
        from congress_videos.reap_processor_dag import _claim_clip_from_queue

        claimed = {"id": 42, "chapter_id": 10, "staged_clip_path": "/data/clip.mp4"}
        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db_cls.return_value.claim_pending_clip.return_value = claimed

        ti = _make_ti()
        result = _claim_clip_from_queue(ti)

        assert result is True
        assert ti.xcom_store["claimed_clip"] == claimed

    def test_claimed_clip_pushed_to_xcom(self, mocker):
        from congress_videos.reap_processor_dag import _claim_clip_from_queue

        claimed = {"id": 7, "chapter_id": 3, "staged_clip_path": "/data/ch3.mp4"}
        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db_cls.return_value.claim_pending_clip.return_value = claimed

        ti = _make_ti()
        _claim_clip_from_queue(ti)

        assert ti.xcom_store["claimed_clip"]["id"] == 7
        assert ti.xcom_store["claimed_clip"]["chapter_id"] == 3


# ---------------------------------------------------------------------------
# TestReapJobSensor
# ---------------------------------------------------------------------------

class TestReapJobSensor:

    def _build_sensor(self):
        from congress_videos.reap_processor_dag import ReapJobSensor
        return ReapJobSensor(
            task_id="wait_for_reap_test",
            reap_project_id_key="reap_project_id_for_sensor",
            chapter_id_key="chapter_id_for_sensor",
            poke_interval=900,
            timeout=7200,
            mode="reschedule",
        )

    def test_processing_status_returns_false(self, mocker):
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        mocker.patch(
            "congress_videos.reap_processor_dag.ReapApiClient.get_project_status",
            return_value={"status": "processing"},
        )
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        result = sensor.poke(context)

        assert result is False

    def test_completed_status_downloads_clips_inserts_rows_returns_true(self, mocker):
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        clips = [
            {"clip_id": "clip-1", "clip_url": "https://cdn.reap.video/c1.mp4", "virality_score": 0.8},
            {"clip_id": "clip-2", "clip_url": "https://cdn.reap.video/c2.mp4", "virality_score": 0.5},
        ]

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short_clip.return_value = 1

        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_processor_dag.get_short_file_path",
            side_effect=lambda ch, cl: f"/data/{ch}/{cl}.mp4",
        )

        result = sensor.poke(context)

        assert result is True
        assert mock_db.insert_video_short_clip.call_count == 2
        mock_client.download_clip.assert_called()

    def test_completed_status_calls_update_video_short_status_done(self, mocker):
        """After successful sensor completion, update_video_short_status('done') must be called."""
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = []
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        mocker.patch(
            "congress_videos.reap_processor_dag.get_short_file_path",
            side_effect=lambda ch, cl: f"/data/{ch}/{cl}.mp4",
        )

        sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", "done")

    def test_failed_status_raises_airflow_exception(self, mocker):
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "failed"}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        with pytest.raises(AirflowException):
            sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", "failed")

    def test_credits_exhausted_pushes_flag_and_returns_true(self, mocker):
        sensor = self._build_sensor()
        ti = _make_ti({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })
        context = {"ti": ti}

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.side_effect = ReapCreditsExhausted("no credits")
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        result = sensor.poke(context)

        assert result is True
        assert ti.xcom_store.get("credits_exhausted") is True

    def test_missing_project_id_raises_airflow_exception(self, mocker):
        sensor = self._build_sensor()
        context = _make_context({})  # no reap_project_id in xcom

        mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        with pytest.raises(AirflowException):
            sensor.poke(context)

    @pytest.mark.parametrize("terminal_status", ["invalid", "expired", "error"])
    def test_various_failure_states_raise_airflow_exception(self, mocker, terminal_status: str):
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": terminal_status}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        with pytest.raises(AirflowException):
            sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", terminal_status)


# ---------------------------------------------------------------------------
# TestUploadToReap
# ---------------------------------------------------------------------------

class TestUploadToReap:

    def _make_claimed_clip(self, chapter_id=1, clip_path="/data/c1.mp4", short_id=10):
        return {
            "id": short_id,
            "chapter_id": chapter_id,
            "staged_clip_path": clip_path,
            "scoring_reasoning": "",
            "reap_project_id": None,
        }

    def test_missing_file_raises_airflow_exception(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        claimed = self._make_claimed_clip(clip_path="/nonexistent/clip.mp4")
        ti = _make_ti({"claimed_clip": claimed})

        with pytest.raises(AirflowException):
            _upload_to_reap(ti, params={})

    def test_successful_upload_pushes_upload_id_and_chapter_id(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mocker.patch("os.path.exists", return_value=True)

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_upload_url.return_value = {
            "upload_id": "up-001",
            "uploadUrl": "https://s3.example.com/up",
        }
        mock_client.upload_file.return_value = None

        claimed = self._make_claimed_clip(chapter_id=5, clip_path="/data/chapter5.mp4")
        ti = _make_ti({"claimed_clip": claimed})
        _upload_to_reap(ti, params={})

        assert ti.xcom_store["upload_id"] == "up-001"
        assert ti.xcom_store["chapter_id"] == 5

    def test_credits_exhausted_stops_upload_and_sets_flag(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mocker.patch("os.path.exists", return_value=True)

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_upload_url.side_effect = ReapCreditsExhausted("no credits")

        claimed = self._make_claimed_clip()
        ti = _make_ti({"claimed_clip": claimed})
        _upload_to_reap(ti, params={})

        assert ti.xcom_store.get("credits_exhausted") is True
        assert "upload_id" not in ti.xcom_store


# ---------------------------------------------------------------------------
# TestCreateReapJob
# ---------------------------------------------------------------------------

class TestCreateReapJob:

    def _make_claimed_clip(self, short_id=10, chapter_id=5):
        return {
            "id": short_id,
            "chapter_id": chapter_id,
            "staged_clip_path": "/data/clip.mp4",
            "scoring_reasoning": "test reasoning",
        }

    def test_no_upload_id_skips_job_creation(self, mocker):
        from congress_videos.reap_processor_dag import _create_reap_job

        mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        claimed = self._make_claimed_clip()
        ti = _make_ti({"claimed_clip": claimed})  # no upload_id in xcom
        _create_reap_job(ti, params={})

        assert "reap_project_id_for_sensor" not in ti.xcom_store

    def test_successful_job_creation_calls_update_video_short_project(self, mocker):
        """create_reap_job must call update_video_short_project NOT insert_video_short."""
        from congress_videos.reap_processor_dag import _create_reap_job

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.create_clips_job.return_value = {"project_id": "proj-001"}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        claimed = self._make_claimed_clip(short_id=42, chapter_id=5)
        ti = _make_ti({"claimed_clip": claimed, "upload_id": "up-001"})
        _create_reap_job(ti, params={})

        # Must call update_video_short_project with (short_id, reap_project_id)
        mock_db.update_video_short_project.assert_called_once_with(42, "proj-001")
        # Must NOT call insert_video_short
        mock_db.insert_video_short.assert_not_called()

    def test_successful_job_creation_sets_sensor_xcoms(self, mocker):
        from congress_videos.reap_processor_dag import _create_reap_job

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.create_clips_job.return_value = {"project_id": "proj-001"}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        claimed = self._make_claimed_clip(short_id=42, chapter_id=5)
        ti = _make_ti({"claimed_clip": claimed, "upload_id": "up-001"})
        _create_reap_job(ti, params={})

        assert ti.xcom_store["reap_project_id_for_sensor"] == "proj-001"
        assert ti.xcom_store["chapter_id_for_sensor"] == 5

    def test_credits_exhausted_sets_flag_and_empty_results(self, mocker):
        from congress_videos.reap_processor_dag import _create_reap_job

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.create_clips_job.side_effect = ReapCreditsExhausted("no credits")

        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        claimed = self._make_claimed_clip()
        ti = _make_ti({"claimed_clip": claimed, "upload_id": "up-001"})
        _create_reap_job(ti, params={})

        assert ti.xcom_store.get("credits_exhausted") is True
