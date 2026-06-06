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
        # Tasks: load_clip_results, upload_to_reap, create_reap_job,
        #        wait_for_reap, check_credits_status
        assert len(dag.tasks) == 5

    def test_dag_has_no_schedule(self):
        from congress_videos.reap_processor_dag import dag
        assert dag.schedule is None

    def test_dag_correct_task_ids(self):
        from congress_videos.reap_processor_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "load_clip_results" in task_ids
        assert "upload_to_reap" in task_ids
        assert "create_reap_job" in task_ids
        assert "wait_for_reap" in task_ids
        assert "check_credits_status" in task_ids

    def test_dag_correct_dependency_chain(self):
        from congress_videos.reap_processor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t0 = tasks_by_id["load_clip_results"]
        t1 = tasks_by_id["upload_to_reap"]
        t2 = tasks_by_id["create_reap_job"]
        t3 = tasks_by_id["wait_for_reap"]
        t4 = tasks_by_id["check_credits_status"]

        assert t1.task_id in {t.task_id for t in t0.downstream_list}
        assert t2.task_id in {t.task_id for t in t1.downstream_list}
        assert t3.task_id in {t.task_id for t in t2.downstream_list}
        assert t4.task_id in {t.task_id for t in t3.downstream_list}


# ---------------------------------------------------------------------------
# TestLoadClipResults
# ---------------------------------------------------------------------------

class TestLoadClipResults:

    def test_empty_conf_returns_false(self):
        from congress_videos.reap_processor_dag import _load_clip_results

        dag_run = MagicMock()
        dag_run.conf = {}
        ti = _make_ti()
        result = _load_clip_results(ti, dag_run=dag_run)

        assert result is False
        assert ti.xcom_store["clip_results"] == []

    def test_missing_conf_returns_false(self):
        from congress_videos.reap_processor_dag import _load_clip_results

        dag_run = MagicMock()
        dag_run.conf = None
        ti = _make_ti()
        result = _load_clip_results(ti, dag_run=dag_run)

        assert result is False
        assert ti.xcom_store["clip_results"] == []

    def test_no_dag_run_in_context_returns_false(self):
        from congress_videos.reap_processor_dag import _load_clip_results

        ti = _make_ti()
        result = _load_clip_results(ti)

        assert result is False
        assert ti.xcom_store["clip_results"] == []

    def test_valid_conf_pushes_to_xcom_returns_true(self):
        from congress_videos.reap_processor_dag import _load_clip_results

        clips = [{"chapter_id": 1, "clip_path": "/data/c1.mp4"}]
        dag_run = MagicMock()
        dag_run.conf = {"clip_results": clips}
        ti = _make_ti()
        result = _load_clip_results(ti, dag_run=dag_run)

        assert result is True
        assert ti.xcom_store["clip_results"] == clips


# ---------------------------------------------------------------------------
# TestReapJobSensor — migrated from test_reap_sensor.py
# ---------------------------------------------------------------------------

class TestReapJobSensor:

    def _build_sensor(self):
        from congress_videos.reap_processor_dag import ReapJobSensor
        return ReapJobSensor(
            task_id="wait_for_reap_test",
            reap_project_id_key="reap_project_id_for_sensor",
            short_id_key="short_id_for_sensor",
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
# TestUploadToReap — migrated from test_reap_sensor.py
# ---------------------------------------------------------------------------

class TestUploadToReap:

    def test_empty_clip_results_pushes_empty_list(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")

        ti = _make_ti({"clip_results": []})
        _upload_to_reap(ti, params={})

        assert ti.xcom_store["upload_results"] == []

    def test_successful_upload_pushes_upload_results(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_upload_url.return_value = {
            "upload_id": "up-001",
            "uploadUrl": "https://s3.example.com/up",
        }
        mock_client.upload_file.return_value = None

        clip_results = [{
            "chapter_id": 1,
            "clip_path": "/data/chapter1/chapter_video.mp4",
            "pretrim_start": None,
            "pretrim_end": None,
            "pretrim_used_srt": False,
            "scoring_reasoning": "interesting",
        }]

        ti = _make_ti({"clip_results": clip_results})
        _upload_to_reap(ti, params={})

        results = ti.xcom_store["upload_results"]
        assert len(results) == 1
        assert results[0]["upload_id"] == "up-001"

    def test_credits_exhausted_stops_uploads_and_sets_flag(self, mocker):
        from congress_videos.reap_processor_dag import _upload_to_reap

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_upload_url.side_effect = ReapCreditsExhausted("no credits")

        clip_results = [
            {"chapter_id": 1, "clip_path": "/data/c1.mp4", "pretrim_start": None,
             "pretrim_end": None, "pretrim_used_srt": False, "scoring_reasoning": ""},
            {"chapter_id": 2, "clip_path": "/data/c2.mp4", "pretrim_start": None,
             "pretrim_end": None, "pretrim_used_srt": False, "scoring_reasoning": ""},
        ]

        ti = _make_ti({"clip_results": clip_results})
        _upload_to_reap(ti, params={})

        assert ti.xcom_store.get("credits_exhausted") is True
        assert mock_client.get_upload_url.call_count == 1


# ---------------------------------------------------------------------------
# TestCreateReapJob — migrated from test_reap_sensor.py
# ---------------------------------------------------------------------------

class TestCreateReapJob:

    def test_empty_upload_results_pushes_empty_list(self, mocker):
        from congress_videos.reap_processor_dag import _create_reap_job

        mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        ti = _make_ti({"upload_results": []})
        _create_reap_job(ti, params={})

        assert ti.xcom_store["reap_job_results"] == []

    def test_successful_job_creation_sets_sensor_xcoms(self, mocker):
        from congress_videos.reap_processor_dag import _create_reap_job

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.create_clips_job.return_value = {"project_id": "proj-001"}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short.return_value = 99

        upload_results = [{
            "chapter_id": 5,
            "upload_id": "up-001",
            "pretrim_start": None,
            "pretrim_end": None,
            "pretrim_used_srt": False,
            "scoring_reasoning": "test",
        }]

        ti = _make_ti({"upload_results": upload_results})
        _create_reap_job(ti, params={})

        assert ti.xcom_store["reap_project_id_for_sensor"] == "proj-001"
        assert ti.xcom_store["chapter_id_for_sensor"] == 5
