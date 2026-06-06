"""Tests for ReapJobSensor (congress_videos.reap_shorts_dag) and DAG load tests."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

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
# 7.5 (sensor portion) — ReapJobSensor.poke()
# ---------------------------------------------------------------------------

class TestReapJobSensorPoke:

    def _build_sensor(self):
        from congress_videos.reap_shorts_dag import ReapJobSensor
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
            "congress_videos.reap_shorts_dag.ReapApiClient.get_project_status",
            return_value={"status": "processing"},
        )
        mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")

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

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "completed"}
        mock_client.get_project_clips.return_value = clips
        mock_client.download_clip.return_value = None

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short_clip.return_value = 1

        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_shorts_dag.get_short_file_path",
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

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": "failed"}

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
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

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.side_effect = ReapCreditsExhausted("no credits")
        mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")

        result = sensor.poke(context)

        assert result is True
        assert ti.xcom_store.get("credits_exhausted") is True

    def test_missing_project_id_raises_airflow_exception(self, mocker):
        sensor = self._build_sensor()
        context = _make_context({})  # no reap_project_id in xcom

        mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")

        with pytest.raises(AirflowException):
            sensor.poke(context)

    @pytest.mark.parametrize("terminal_status", ["invalid", "expired", "error"])
    def test_various_failure_states_raise_airflow_exception(self, mocker, terminal_status: str):
        sensor = self._build_sensor()
        context = _make_context({
            "reap_project_id_for_sensor": "proj-abc",
            "chapter_id_for_sensor": 42,
        })

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.get_project_status.return_value = {"status": terminal_status}

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        with pytest.raises(AirflowException):
            sensor.poke(context)

        mock_db.update_video_short_status.assert_called_once_with("proj-abc", terminal_status)


# ---------------------------------------------------------------------------
# 7.8 — DAG 1 load test (congress_reap_shorts)
# ---------------------------------------------------------------------------

class TestCongressReapShortsDAGLoads:

    def test_congress_reap_shorts_dag_loads(self):
        from congress_videos.reap_shorts_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_reap_shorts"

    def test_dag_has_correct_task_count(self):
        from congress_videos.reap_shorts_dag import dag
        # Tasks: ensure_data_directory, query_chapters, extract_and_pretrim_clip,
        #        validate_clip_durations, upload_to_reap, create_reap_job,
        #        wait_for_reap, check_credits_status
        assert len(dag.tasks) == 8

    def test_dag_schedule(self):
        from congress_videos.reap_shorts_dag import dag
        assert dag.schedule == "0 15 * * *"


# ---------------------------------------------------------------------------
# 7.9 — DAG 1 task functions (_query_chapters, _extract_and_pretrim_clip,
#         _upload_to_reap, _create_reap_job, _check_credits_status)
# ---------------------------------------------------------------------------

class TestQueryChapters:

    def test_pushes_chapters_to_xcom(self, mocker):
        from congress_videos.reap_shorts_dag import _query_chapters

        chapters = [{"chapter_id": 1}, {"chapter_id": 2}]
        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapters_for_shorts.return_value = chapters

        ti = _make_ti()
        _query_chapters(ti, params={"max_chapters": 2, "min_relevance_score": 3})

        assert ti.xcom_store["chapters_for_shorts"] == chapters

    def test_passes_params_to_db(self, mocker):
        from congress_videos.reap_shorts_dag import _query_chapters

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapters_for_shorts.return_value = []

        ti = _make_ti()
        _query_chapters(ti, params={"max_chapters": 5, "min_relevance_score": 4})

        mock_db.get_chapters_for_shorts.assert_called_once_with(limit=5, min_relevance_score=4)


class TestExtractAndPretrimClip:

    def _default_chapter(self):
        return {
            "chapter_id": 10,
            "video_id": "vid-abc",
            "start_time": "00:01:00",
            "end_time": "00:05:00",
            "session_date": "2025-10-08",
            "scoring_reasoning": "Good debate",
        }

    def _setup_mocks(self, mocker, *, source_video="/data/video.mp4"):
        mocker.patch(
            "congress_videos.reap_shorts_dag._find_source_video",
            return_value=source_video,
        )
        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_shorts_dag.split_video_chapter",
            return_value={"success": True, "error": None},
        )

    def test_short_duration_chapter_no_pretrim_appended(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        clips = ti.xcom_store.get("clip_results", [])
        assert len(clips) == 1
        assert clips[0]["pretrim_used_srt"] is False

    def test_no_source_video_skips_chapter(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        mocker.patch(
            "congress_videos.reap_shorts_dag._find_source_video",
            return_value=None,
        )

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        assert ti.xcom_store.get("clip_results", []) == []

    def test_extraction_failure_skips_chapter(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)
        mocker.patch(
            "congress_videos.reap_shorts_dag.split_video_chapter",
            return_value={"success": False, "error": "ffmpeg error"},
        )

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        assert ti.xcom_store.get("clip_results", []) == []

    def test_long_chapter_with_srt_calls_ffmpeg_window(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # 720s > threshold 480

        mocker.patch(
            "congress_videos.reap_shorts_dag.find_srt_for_chapter",
            return_value="/data/srt.srt",
        )
        mocker.patch(
            "congress_videos.reap_shorts_dag.select_pretrim_window",
            return_value={"start_seconds": 60.0, "end_seconds": 420.0},
        )
        mock_ffmpeg = mocker.patch("congress_videos.reap_shorts_dag._ffmpeg_extract_window")

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_ffmpeg.assert_called_once()
        clips = ti.xcom_store.get("clip_results", [])
        assert len(clips) == 1
        assert clips[0]["pretrim_used_srt"] is True

    def test_long_chapter_no_srt_falls_back_to_first_target_secs(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # 720s > threshold 480

        mocker.patch(
            "congress_videos.reap_shorts_dag.find_srt_for_chapter",
            return_value=None,
        )
        mock_ffmpeg = mocker.patch("congress_videos.reap_shorts_dag._ffmpeg_extract_window")

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_ffmpeg.assert_called_once()
        call_kwargs = mock_ffmpeg.call_args
        assert call_kwargs.kwargs["start_secs"] == 0.0
        assert call_kwargs.kwargs["end_secs"] == 360.0

        clips = ti.xcom_store.get("clip_results", [])
        assert len(clips) == 1
        assert clips[0]["pretrim_used_srt"] is False
        assert clips[0]["pretrim_start"] == 0.0
        assert clips[0]["pretrim_end"] == 360.0

    def test_long_chapter_phrase_not_found_falls_back_to_first_target_secs(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # 720s > threshold 480

        mocker.patch(
            "congress_videos.reap_shorts_dag.find_srt_for_chapter",
            return_value="/data/srt.srt",
        )
        mocker.patch(
            "congress_videos.reap_shorts_dag.select_pretrim_window",
            return_value=None,  # phrase lookup failed
        )
        mock_ffmpeg = mocker.patch("congress_videos.reap_shorts_dag._ffmpeg_extract_window")

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_ffmpeg.assert_called_once()
        call_kwargs = mock_ffmpeg.call_args
        assert call_kwargs.kwargs["start_secs"] == 0.0
        assert call_kwargs.kwargs["end_secs"] == 360.0

        clips = ti.xcom_store.get("clip_results", [])
        assert len(clips) == 1
        assert clips[0]["pretrim_used_srt"] is False

    def test_long_chapter_fallback_ffmpeg_error_skips_chapter(self, mocker):
        from congress_videos.reap_shorts_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"

        mocker.patch(
            "congress_videos.reap_shorts_dag.find_srt_for_chapter",
            return_value=None,
        )
        mocker.patch(
            "congress_videos.reap_shorts_dag._ffmpeg_extract_window",
            side_effect=RuntimeError("disk full"),
        )

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        assert ti.xcom_store.get("clip_results", []) == []


class TestValidateClipDurations:

    def _clip(self, chapter_id=1, clip_path="/data/chapter1/chapter_video_trimmed.mp4"):
        return {
            "chapter_id": chapter_id,
            "clip_path": clip_path,
            "pretrim_start": 0.0,
            "pretrim_end": 300.0,
            "pretrim_used_srt": False,
            "scoring_reasoning": "",
        }

    def _patch_ffprobe(self, mocker, duration_secs: float):
        import json
        fake_output = json.dumps({"format": {"duration": str(duration_secs)}})
        mocker.patch(
            "subprocess.run",
            return_value=MagicMock(stdout=fake_output, returncode=0),
        )

    def test_clip_within_limit_passes(self, mocker):
        from congress_videos.reap_shorts_dag import _validate_clip_durations

        self._patch_ffprobe(mocker, duration_secs=295.0)

        ti = _make_ti({"clip_results": [self._clip()]})
        _validate_clip_durations(ti, params={"pre_trim_target_secs": 300})

        assert len(ti.xcom_store["clip_results"]) == 1

    def test_clip_exactly_at_limit_passes_due_to_tolerance(self, mocker):
        from congress_videos.reap_shorts_dag import _validate_clip_durations

        # ffmpeg -c copy produces 300.033s for a 300s target — must pass within tolerance
        self._patch_ffprobe(mocker, duration_secs=300.033)

        ti = _make_ti({"clip_results": [self._clip()]})
        _validate_clip_durations(ti, params={"pre_trim_target_secs": 300})

        assert len(ti.xcom_store["clip_results"]) == 1

    def test_clip_exceeding_limit_plus_tolerance_raises(self, mocker):
        from congress_videos.reap_shorts_dag import _validate_clip_durations

        self._patch_ffprobe(mocker, duration_secs=1800.0)  # 30 minutes — way over

        ti = _make_ti({"clip_results": [self._clip()]})
        with pytest.raises(AirflowException, match="blocked"):
            _validate_clip_durations(ti, params={"pre_trim_target_secs": 300})

    def test_ffprobe_failure_raises(self, mocker):
        from congress_videos.reap_shorts_dag import _validate_clip_durations

        mocker.patch("subprocess.run", side_effect=Exception("ffprobe not found"))

        ti = _make_ti({"clip_results": [self._clip()]})
        with pytest.raises(AirflowException, match="blocked"):
            _validate_clip_durations(ti, params={"pre_trim_target_secs": 300})


class TestUploadToReap:

    def test_empty_clip_results_pushes_empty_list(self, mocker):
        from congress_videos.reap_shorts_dag import _upload_to_reap

        mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")

        ti = _make_ti({"clip_results": []})
        _upload_to_reap(ti, params={})

        assert ti.xcom_store["upload_results"] == []

    def test_successful_upload_pushes_upload_results(self, mocker):
        from congress_videos.reap_shorts_dag import _upload_to_reap

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
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
        from congress_videos.reap_shorts_dag import _upload_to_reap

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
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


class TestCreateReapJob:

    def test_empty_upload_results_pushes_empty_list(self, mocker):
        from congress_videos.reap_shorts_dag import _create_reap_job

        mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")

        ti = _make_ti({"upload_results": []})
        _create_reap_job(ti, params={})

        assert ti.xcom_store["reap_job_results"] == []

    def test_successful_job_creation_sets_sensor_xcoms(self, mocker):
        from congress_videos.reap_shorts_dag import _create_reap_job

        mock_client_cls = mocker.patch("congress_videos.reap_shorts_dag.ReapApiClient")
        mock_client = mock_client_cls.return_value
        mock_client.create_clips_job.return_value = {"project_id": "proj-001"}

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_dag.CongressionalVideoDB")
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


class TestCheckCreditsStatus:

    def test_credits_exhausted_true_logs_warning(self, mocker):
        from congress_videos.reap_shorts_dag import _check_credits_status

        mock_warn = mocker.patch("congress_videos.reap_shorts_dag.logging.warning")

        ti = _make_ti({"credits_exhausted": True})
        _check_credits_status(ti, params={})

        mock_warn.assert_called_once()

    def test_credits_not_exhausted_no_warning(self, mocker):
        from congress_videos.reap_shorts_dag import _check_credits_status

        mock_warn = mocker.patch("congress_videos.reap_shorts_dag.logging.warning")

        ti = _make_ti({})
        _check_credits_status(ti, params={})

        mock_warn.assert_not_called()
