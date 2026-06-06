"""Tests for congress_reap_clip_preparer DAG (congress_videos.reap_clip_preparer_dag)."""

from __future__ import annotations

import json

import pytest
from unittest.mock import MagicMock, call
from airflow.exceptions import AirflowException


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


# ---------------------------------------------------------------------------
# DAG load tests
# ---------------------------------------------------------------------------

class TestCongressReapClipPreparerDAGLoads:

    def test_dag_loads(self):
        from congress_videos.reap_clip_preparer_dag import dag
        assert dag is not None
        assert dag.dag_id == "congress_reap_clip_preparer"

    def test_dag_has_correct_task_count(self):
        from congress_videos.reap_clip_preparer_dag import dag
        # Tasks: ensure_data_directory, query_chapters, extract_and_pretrim_clip,
        #        log_queue_summary
        assert len(dag.tasks) == 4

    def test_dag_has_correct_schedule(self):
        from congress_videos.reap_clip_preparer_dag import dag
        assert dag.schedule == '0 15 * * *'

    def test_dag_correct_task_ids(self):
        from congress_videos.reap_clip_preparer_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "ensure_data_directory" in task_ids
        assert "query_chapters" in task_ids
        assert "extract_and_pretrim_clip" in task_ids
        assert "log_queue_summary" in task_ids
        assert "validate_clip_durations" not in task_ids
        assert "trigger_reap_processor" not in task_ids

    def test_dag_correct_dependency_chain(self):
        from congress_videos.reap_clip_preparer_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t0 = tasks_by_id["ensure_data_directory"]
        t1 = tasks_by_id["query_chapters"]
        t2 = tasks_by_id["extract_and_pretrim_clip"]
        t3 = tasks_by_id["log_queue_summary"]

        assert t1.task_id in {t.task_id for t in t0.downstream_list}
        assert t2.task_id in {t.task_id for t in t1.downstream_list}
        assert t3.task_id in {t.task_id for t in t2.downstream_list}


# ---------------------------------------------------------------------------
# TestQueryChapters
# ---------------------------------------------------------------------------

class TestQueryChapters:

    def test_empty_result_returns_false(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_chapters

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapters_for_shorts.return_value = []

        ti = _make_ti()
        result = _query_chapters(ti, params={"max_chapters": 100, "min_relevance_score": 3})

        assert result is False
        assert ti.xcom_store["chapters_for_shorts"] == []

    def test_non_empty_result_returns_true(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_chapters

        chapters = [{"chapter_id": 1}, {"chapter_id": 2}]
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapters_for_shorts.return_value = chapters

        ti = _make_ti()
        result = _query_chapters(ti, params={"max_chapters": 100, "min_relevance_score": 3})

        assert result is True
        assert ti.xcom_store["chapters_for_shorts"] == chapters

    def test_pushes_chapters_to_xcom(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_chapters

        chapters = [{"chapter_id": 1}, {"chapter_id": 2}]
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapters_for_shorts.return_value = chapters

        ti = _make_ti()
        _query_chapters(ti, params={"max_chapters": 100, "min_relevance_score": 3})

        assert ti.xcom_store["chapters_for_shorts"] == chapters

    def test_passes_params_to_db(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_chapters

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapters_for_shorts.return_value = []

        ti = _make_ti()
        _query_chapters(ti, params={"max_chapters": 5, "min_relevance_score": 4})

        mock_db.get_chapters_for_shorts.assert_called_once_with(limit=5, min_relevance_score=4)


# ---------------------------------------------------------------------------
# TestExtractAndPretrimClip
# ---------------------------------------------------------------------------

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

    def _setup_mocks(self, mocker, *, source_video="/data/video.mp4", duration_seconds=240):
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag._find_source_video",
            return_value=source_video,
        )
        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.split_video_chapter",
            return_value={"success": True, "error": None, "duration_seconds": duration_seconds},
        )

    def _patch_ffprobe_ok(self, mocker, duration_secs: float = 240.0):
        """Mock ffprobe returning a valid duration within safe limits."""
        fake_output = json.dumps({"format": {"duration": str(duration_secs)}})
        mocker.patch(
            "subprocess.run",
            return_value=MagicMock(stdout=fake_output, returncode=0),
        )

    def _patch_ffprobe_blocked(self, mocker, duration_secs: float = 1800.0):
        """Mock ffprobe returning a duration that exceeds the safety gate."""
        fake_output = json.dumps({"format": {"duration": str(duration_secs)}})
        mocker.patch(
            "subprocess.run",
            return_value=MagicMock(stdout=fake_output, returncode=0),
        )

    def test_short_chapter_no_pretrim(self, mocker):
        """Short chapter (below threshold) should insert a pending row without pre-trim."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)
        self._patch_ffprobe_ok(mocker, duration_secs=240.0)

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short.return_value = 1

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_db.insert_video_short.assert_called_once()
        call_kwargs = mock_db.insert_video_short.call_args.kwargs
        assert call_kwargs["reap_status"] == "pending"
        assert call_kwargs["staged_clip_path"] is not None
        assert call_kwargs.get("reap_project_id") is None
        assert call_kwargs["pretrim_used_srt"] is False
        assert ti.xcom_store["clips_queued"] == 1

    def test_long_chapter_with_srt_window(self, mocker):
        """Long chapter with SRT available should call ffmpeg, then insert pending row."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker, duration_seconds=720)
        self._patch_ffprobe_ok(mocker, duration_secs=360.0)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # 720s > threshold 480

        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.find_srt_for_chapter",
            return_value="/data/srt.srt",
        )
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.select_pretrim_window",
            return_value={"start_seconds": 60.0, "end_seconds": 420.0},
        )
        mock_ffmpeg = mocker.patch("congress_videos.reap_clip_preparer_dag._ffmpeg_extract_window")

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short.return_value = 2

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_ffmpeg.assert_called_once()
        mock_db.insert_video_short.assert_called_once()
        call_kwargs = mock_db.insert_video_short.call_args.kwargs
        assert call_kwargs["reap_status"] == "pending"
        assert call_kwargs["pretrim_used_srt"] is True
        assert call_kwargs["pretrim_start_secs"] == 60.0
        assert call_kwargs["pretrim_end_secs"] == 420.0
        assert ti.xcom_store["clips_queued"] == 1

    def test_long_chapter_no_srt_fallback(self, mocker):
        """Long chapter without SRT should fallback to first target_secs and insert pending row."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker, duration_seconds=720)
        self._patch_ffprobe_ok(mocker, duration_secs=360.0)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # 720s > threshold 480

        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.find_srt_for_chapter",
            return_value=None,
        )
        mock_ffmpeg = mocker.patch("congress_videos.reap_clip_preparer_dag._ffmpeg_extract_window")

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short.return_value = 3

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_ffmpeg.assert_called_once()
        call_kwargs = mock_ffmpeg.call_args
        assert call_kwargs.kwargs["start_secs"] == 0.0
        assert call_kwargs.kwargs["end_secs"] == 360.0

        mock_db.insert_video_short.assert_called_once()
        db_kwargs = mock_db.insert_video_short.call_args.kwargs
        assert db_kwargs["reap_status"] == "pending"
        assert db_kwargs["pretrim_used_srt"] is False
        assert ti.xcom_store["clips_queued"] == 1

    def test_blocked_clip_not_inserted(self, mocker):
        """When ffprobe reports excessive duration, clip must NOT be inserted and AirflowException raised."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)
        self._patch_ffprobe_blocked(mocker, duration_secs=1800.0)

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        with pytest.raises(AirflowException, match="blocked"):
            _extract_and_pretrim_clip(
                ti,
                params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 300},
            )

        mock_db.insert_video_short.assert_not_called()

    def test_ffprobe_failure_blocks_clip(self, mocker):
        """When ffprobe raises an exception, clip must NOT be inserted and AirflowException raised."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)
        mocker.patch("subprocess.run", side_effect=Exception("ffprobe not found"))

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        with pytest.raises(AirflowException, match="blocked"):
            _extract_and_pretrim_clip(
                ti,
                params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 300},
            )

        mock_db.insert_video_short.assert_not_called()

    def test_no_source_video_skips_chapter(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        mocker.patch(
            "congress_videos.reap_clip_preparer_dag._find_source_video",
            return_value=None,
        )
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_db.insert_video_short.assert_not_called()
        assert ti.xcom_store.get("clips_queued") == 0

    def test_extraction_failure_skips_chapter(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker)
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.split_video_chapter",
            return_value={"success": False, "error": "ffmpeg error"},
        )
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"chapters_for_shorts": [self._default_chapter()]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_db.insert_video_short.assert_not_called()
        assert ti.xcom_store.get("clips_queued") == 0

    def test_post_trim_clip_below_120s_skipped(self, mocker):
        """Clip that ends up below 120s after trim should be skipped — not inserted."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        self._setup_mocks(mocker, duration_seconds=720)

        chapter = self._default_chapter()
        chapter["start_time"] = "00:00:00"
        chapter["end_time"] = "00:12:00"  # long enough to trigger pre-trim

        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.find_srt_for_chapter",
            return_value="/data/srt.srt",
        )
        # Window is only 60s — below 120s minimum
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.select_pretrim_window",
            return_value={"start_seconds": 0.0, "end_seconds": 60.0},
        )
        mocker.patch("congress_videos.reap_clip_preparer_dag._ffmpeg_extract_window")

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"chapters_for_shorts": [chapter]})
        _extract_and_pretrim_clip(
            ti,
            params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360},
        )

        mock_db.insert_video_short.assert_not_called()
        assert ti.xcom_store.get("clips_queued") == 0

    def test_partial_success_blocked_raises_after_inserting_good_clips(self, mocker):
        """Batch with 1 good + 1 blocked clip: good clip is inserted, then AirflowException raised."""
        from congress_videos.reap_clip_preparer_dag import _extract_and_pretrim_clip

        chapter_good = {**self._default_chapter(), "chapter_id": 1}
        chapter_blocked = {**self._default_chapter(), "chapter_id": 2}

        mocker.patch(
            "congress_videos.reap_clip_preparer_dag._find_source_video",
            return_value="/data/video.mp4",
        )
        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.split_video_chapter",
            return_value={"success": True, "error": None, "duration_seconds": 240},
        )

        call_count = {"n": 0}
        def fake_subprocess_run(cmd, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # First chapter: OK duration
                return MagicMock(stdout=json.dumps({"format": {"duration": "240.0"}}), returncode=0)
            else:
                # Second chapter: blocked duration
                return MagicMock(stdout=json.dumps({"format": {"duration": "1800.0"}}), returncode=0)

        mocker.patch("subprocess.run", side_effect=fake_subprocess_run)

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.insert_video_short.return_value = 1

        ti = _make_ti({"chapters_for_shorts": [chapter_good, chapter_blocked]})
        with pytest.raises(AirflowException, match="blocked"):
            _extract_and_pretrim_clip(
                ti,
                params={"pre_trim_threshold_secs": 480, "pre_trim_target_secs": 300},
            )

        # Good clip was inserted before the exception
        mock_db.insert_video_short.assert_called_once()
        assert ti.xcom_store.get("clips_queued") == 1
