"""Tests for congress_reap_processor DAG (congress_videos.reap_processor_dag)."""

from __future__ import annotations

from unittest.mock import MagicMock

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

        assert dag.schedule_interval == "30 14,17 * * *"

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
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

        mocker.patch(
            "congress_videos.reap_processor_dag.ReapApiClient.get_project_status",
            return_value={"status": "processing"},
        )
        mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")

        result = sensor.poke(context)

        assert result is False

    def test_completed_status_downloads_clips_inserts_rows_returns_true(self, mocker):
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

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
        mock_db.get_source_video_id_for_chapter.return_value = "src_vid_001"

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(side_effect=lambda: "/data/canonical/clip.mp4")
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        result = sensor.poke(context)

        assert result is True
        assert mock_db.insert_video_short_clip.call_count == 2
        mock_client.download_clip.assert_called()

    def test_completed_status_calls_update_video_short_status_done(self, mocker):
        """After successful sensor completion, update_video_short_status('done') must be called."""
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = []
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_source_video_id_for_chapter.return_value = "src_vid_001"

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(return_value="/data/canonical/clip.mp4")
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", "done")

    def test_failed_status_raises_airflow_exception(self, mocker):
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

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
        ti = _make_ti(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )
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
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": terminal_status}

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        with pytest.raises(AirflowException):
            sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", terminal_status)


# ---------------------------------------------------------------------------
# TestShortSrtSidecarHook — issue #431 (PR1)
# ---------------------------------------------------------------------------


class TestShortSrtSidecarHook:
    """The short SRT sidecar write is best-effort and must never fail the sensor."""

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

    def _base_mocks(self, mocker, clips=None):
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

        clips = (
            clips
            if clips is not None
            else [
                {"clip_id": "clip-1", "clip_url": "https://cdn.reap.video/c1.mp4", "virality_score": 0.8},
            ]
        )

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short_clip.return_value = 1
        mock_db.get_source_video_id_for_chapter.return_value = "src_vid_001"
        mock_db.get_chapter_srt_context.return_value = {
            "video_id": "src_vid_001",
            "start_time": "00:05:00,000",
            "end_time": "00:10:00,000",
            "session_date": "2024-01-15",
        }

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(side_effect=lambda: "/data/canonical/clip.mp4")
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        return sensor, context, mock_client, mock_db

    def test_sensor_still_downloads_mp4_when_sidecar_raises(self, mocker):
        sensor, context, mock_client, mock_db = self._base_mocks(mocker)
        mocker.patch(
            "congress_videos.reap_processor_dag.write_short_srt_sidecar",
            side_effect=RuntimeError("boom"),
        )

        result = sensor.poke(context)

        assert result is True
        mock_db.insert_video_short_clip.assert_called_once()

    def test_sidecar_called_with_chapter_bounds_and_pretrim_offsets(self, mocker):
        sensor, context, mock_client, mock_db = self._base_mocks(mocker)
        context["ti"].xcom_store["claimed_clip"] = {
            "id": 1,
            "chapter_id": 42,
            "pretrim_start_secs": 30,
            "pretrim_end_secs": 100,
        }
        mock_sidecar = mocker.patch("congress_videos.reap_processor_dag.write_short_srt_sidecar")

        result = sensor.poke(context)

        assert result is True
        mock_sidecar.assert_called_once()
        _args, kwargs = mock_sidecar.call_args
        assert kwargs["pretrim_start_secs"] == 30
        assert kwargs["pretrim_end_secs"] == 100

    def test_missing_claimed_clip_xcom_falls_back_to_none_offsets(self, mocker):
        sensor, context, mock_client, mock_db = self._base_mocks(mocker)
        mock_sidecar = mocker.patch("congress_videos.reap_processor_dag.write_short_srt_sidecar")

        result = sensor.poke(context)

        assert result is True
        _args, kwargs = mock_sidecar.call_args
        assert kwargs["pretrim_start_secs"] is None
        assert kwargs["pretrim_end_secs"] is None

    def test_get_chapter_srt_context_failure_does_not_crash_sensor(self, mocker):
        sensor, context, mock_client, mock_db = self._base_mocks(mocker)
        mock_db.get_chapter_srt_context.side_effect = RuntimeError("db down")
        mock_sidecar = mocker.patch("congress_videos.reap_processor_dag.write_short_srt_sidecar")

        result = sensor.poke(context)

        assert result is True
        mock_db.insert_video_short_clip.assert_called_once()
        mock_sidecar.assert_called_once()


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


# ---------------------------------------------------------------------------
# TestReapJobSensorCanonicalPath — Slice 6 (#133)
# ---------------------------------------------------------------------------


class TestReapJobSensorCanonicalPath:
    """Tests for the canonical write-point rewire in ReapJobSensor.poke."""

    def _build_sensor(self):
        from congress_videos.reap_processor_dag import ReapJobSensor

        return ReapJobSensor(
            task_id="wait_for_reap_canonical_test",
            reap_project_id_key="reap_project_id_for_sensor",
            chapter_id_key="chapter_id_for_sensor",
            poke_interval=900,
            timeout=7200,
            mode="reschedule",
        )

    def test_canonical_path_used_when_source_video_id_present(self, mocker):
        """download_clip and insert_video_short_clip both receive the canonical str path."""
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 7,
            }
        )

        clips = [{"clip_id": "clip-1", "clip_url": "https://cdn.reap.video/c1.mp4", "virality_score": 0.8}]

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_source_video_id_for_chapter.return_value = "src001"

        canonical_str = "/data/congreso-es-tv/src001/video_chapters/7/shorts/clip-1.mp4"
        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(return_value=canonical_str)
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        sensor.poke(context)

        # download_clip must receive str, not Path
        download_args = mock_client.download_clip.call_args[0]
        assert download_args[1] == canonical_str
        assert isinstance(download_args[1], str)

        # insert_video_short_clip must receive the same str via local_file_path
        insert_kwargs = mock_db.insert_video_short_clip.call_args[1]
        assert insert_kwargs["local_file_path"] == canonical_str

    def test_skip_with_warning_when_source_video_id_is_none(self, mocker, caplog):
        """When get_source_video_id_for_chapter returns None, no download/insert, no exception."""
        import logging

        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-xyz",
                "chapter_id_for_sensor": 99,
            }
        )

        clips = [{"clip_id": "clip-X", "clip_url": "https://cdn.reap.video/cx.mp4", "virality_score": 0.5}]

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_source_video_id_for_chapter.return_value = None

        mocker.patch("congress_videos.reap_processor_dag.get_chapter_short_file_path")

        with caplog.at_level(logging.WARNING):
            result = sensor.poke(context)

        assert result is True
        mock_client.download_clip.assert_not_called()
        mock_db.insert_video_short_clip.assert_not_called()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING and "99" in r.message]
        assert len(warnings) >= 1

    def test_parent_mkdir_called_before_download(self, mocker):
        """dest_path.parent.mkdir(parents=True, exist_ok=True) must be called before download."""
        from pathlib import Path

        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-mkdir",
                "chapter_id_for_sensor": 5,
            }
        )

        clips = [{"clip_id": "c1", "clip_url": "https://cdn.reap.video/c1.mp4", "virality_score": 0.6}]

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_source_video_id_for_chapter.return_value = "svid5"

        mock_path = MagicMock(spec=Path)
        mock_path.__str__ = MagicMock(return_value="/data/canonical/c1.mp4")
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        call_order = []
        mock_path.parent.mkdir.side_effect = lambda **kw: call_order.append("mkdir")
        mock_client.download_clip.side_effect = lambda *a: call_order.append("download")

        sensor.poke(context)

        mock_path.parent.mkdir.assert_called_once_with(parents=True, exist_ok=True)
        assert call_order.index("mkdir") < call_order.index("download")


class TestClipIdValidation:
    """Unsafe clip ids from the Reap API must never reach the filesystem (issue #198)."""

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

    def _run_with_clips(self, mocker, clips):
        sensor = self._build_sensor()
        context = _make_context(
            {
                "reap_project_id_for_sensor": "proj-abc",
                "chapter_id_for_sensor": 42,
            }
        )

        mock_client_cls = mocker.patch("congress_videos.reap_processor_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_processor_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short_clip.return_value = 1
        mock_db.get_source_video_id_for_chapter.return_value = "src_vid_001"

        mock_path = MagicMock()
        mock_path.__str__ = MagicMock(side_effect=lambda: "/data/canonical/clip.mp4")
        mock_path.parent = MagicMock()
        mocker.patch(
            "congress_videos.reap_processor_dag.get_chapter_short_file_path",
            return_value=mock_path,
        )

        result = sensor.poke(context)
        return result, mock_client, mock_db

    def test_traversal_clip_id_is_skipped(self, mocker):
        clips = [
            {"clip_id": "../../etc/evil", "clip_url": "https://cdn.reap.video/x.mp4", "virality_score": 0.9},
        ]
        result, mock_client, mock_db = self._run_with_clips(mocker, clips)

        assert result is True
        mock_client.download_clip.assert_not_called()
        mock_db.insert_video_short_clip.assert_not_called()

    def test_safe_sibling_still_processed(self, mocker):
        clips = [
            {"clip_id": "../evil", "clip_url": "https://cdn.reap.video/x.mp4", "virality_score": 0.9},
            {"clip_id": "clip_ok-1", "clip_url": "https://cdn.reap.video/ok.mp4", "virality_score": 0.5},
        ]
        result, mock_client, mock_db = self._run_with_clips(mocker, clips)

        assert result is True
        assert mock_client.download_clip.call_count == 1
        assert mock_db.insert_video_short_clip.call_count == 1
        kwargs = mock_db.insert_video_short_clip.call_args.kwargs
        assert kwargs["reap_clip_id"] == "clip_ok-1"

    @pytest.mark.parametrize("bad_id", ["", None, "a/b", "a\\b", "..", "clip id", "clip;rm"])
    def test_rejected_charset_variants(self, mocker, bad_id):
        clips = [
            {"clip_id": bad_id, "clip_url": "https://cdn.reap.video/x.mp4", "virality_score": 0.9},
        ]
        result, mock_client, _ = self._run_with_clips(mocker, clips)
        assert result is True
        mock_client.download_clip.assert_not_called()
