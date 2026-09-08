"""Tests for congress_reap_clip_preparer DAG (congress_videos.reap_clip_preparer_dag)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ti() -> MagicMock:
    """Create a minimal Airflow TaskInstance double with in-memory XCom."""
    store: dict = {}
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

        assert dag.schedule_interval == "0 15 * * *"

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

    def test_dag_params_are_turn_based(self):
        from congress_videos.reap_clip_preparer_dag import dag

        assert "max_turns" in dag.params
        assert "max_chapters" not in dag.params
        assert "min_relevance_score" not in dag.params
        assert dag.params["pre_trim_threshold_secs"] == 900
        assert dag.params["pre_trim_target_secs"] == 900


# ---------------------------------------------------------------------------
# _ffmpeg_extract_window (#10 precise cut + #12 adaptive timeout wiring)
# ---------------------------------------------------------------------------


class TestFfmpegExtractWindow:
    def test_uses_precise_input_seek_command_and_adaptive_timeout(self, mocker, tmp_path):
        """The window extractor must build a frame-accurate (-ss before -i)
        re-encode command with -err_detect ignore_err and pass an adaptive
        timeout that scales with the clip duration (base=120 + factor=8 * duration)."""
        import congress_videos.reap_clip_preparer_dag as mod

        mocker.patch("os.makedirs")
        run = mocker.patch(
            "congress_videos.reap_clip_preparer_dag.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )

        dest = str(tmp_path / "out.mp4")
        mod._ffmpeg_extract_window(source_path="src.mp4", dest_path=dest, start_secs=10.0, end_secs=40.0)

        run.assert_called_once()
        cmd = run.call_args[0][0]
        assert isinstance(cmd, list)
        assert "shell" not in run.call_args.kwargs
        # Frame accuracy: -ss before -i (input seek + accurate_seek), full re-encode (no stream copy).
        assert cmd.index("-ss") < cmd.index("-i"), (
            "-ss before -i with accurate_seek gives frame accuracy without full-prefix decode"
        )
        assert "-err_detect" in cmd and cmd[cmd.index("-err_detect") + 1] == "ignore_err"
        assert "copy" not in cmd
        assert "libx264" in cmd
        # Adaptive timeout for a 30s clip: 120 + 8 * 30 = 360.
        assert run.call_args[1]["timeout"] == 360

    def test_nonzero_returncode_raises_runtime_error(self, mocker, tmp_path):
        import congress_videos.reap_clip_preparer_dag as mod

        mocker.patch("os.makedirs")
        mocker.patch(
            "congress_videos.reap_clip_preparer_dag.subprocess.run",
            return_value=MagicMock(returncode=1, stderr="boom"),
        )
        with pytest.raises(RuntimeError, match="ffmpeg window extract failed"):
            mod._ffmpeg_extract_window(
                source_path="src.mp4",
                dest_path=str(tmp_path / "o.mp4"),
                start_secs=0.0,
                end_secs=5.0,
            )

    def test_reencode_true_default_preserves_backward_compat(self, mocker, tmp_path):
        """Omitting reencode keeps the existing re-encode default for backward compat."""
        import congress_videos.reap_clip_preparer_dag as mod

        mocker.patch("os.makedirs")
        run = mocker.patch(
            "congress_videos.reap_clip_preparer_dag.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )

        mod._ffmpeg_extract_window(
            source_path="src.mp4",
            dest_path=str(tmp_path / "out.mp4"),
            start_secs=10.0,
            end_secs=40.0,
        )

        cmd = run.call_args[0][0]
        assert "libx264" in cmd
        assert "copy" not in cmd

    def test_reencode_false_selects_stream_copy_shape(self, mocker, tmp_path):
        import congress_videos.reap_clip_preparer_dag as mod

        mocker.patch("os.makedirs")
        run = mocker.patch(
            "congress_videos.reap_clip_preparer_dag.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )

        mod._ffmpeg_extract_window(
            source_path="src.mp4",
            dest_path=str(tmp_path / "out.mp4"),
            start_secs=10.0,
            end_secs=40.0,
            reencode=False,
        )

        cmd = run.call_args[0][0]
        assert "copy" in cmd
        assert "libx264" not in cmd


# ---------------------------------------------------------------------------
# TestQueryTurns
# ---------------------------------------------------------------------------


class TestQueryTurns:
    def test_empty_result_returns_false_and_logs_warning(self, mocker, caplog):
        from congress_videos.reap_clip_preparer_dag import _query_turns

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_turn_videos_for_shorts.return_value = []

        ti = _make_ti()
        with caplog.at_level("WARNING"):
            result = _query_turns(ti, params={"max_turns": 100})

        # ShortCircuitOperator contract: returns False to skip downstream when
        # there are no turns. It does NOT push to XCom — staging re-queries the DB.
        assert result is False
        warnings = [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]
        assert any("No eligible turn videos for Reap" in msg and "0" in msg for msg in warnings)

    def test_non_empty_result_returns_true(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_turns

        turns = [{"turn_id": 1}, {"turn_id": 2}]
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_turn_videos_for_shorts.return_value = turns

        ti = _make_ti()
        result = _query_turns(ti, params={"max_turns": 100})

        assert result is True

    def test_does_not_push_to_xcom(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _query_turns

        turns = [{"turn_id": 1}, {"turn_id": 2}]
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_turn_videos_for_shorts.return_value = turns

        ti = _make_ti()
        _query_turns(ti, params={"max_turns": 100})

        ti.xcom_push.assert_not_called()
        assert ti.xcom_store == {}

    @pytest.mark.parametrize(("max_turns", "expected"), [(5, 5), (0, None)])
    def test_passes_max_turns_to_db(self, mocker, max_turns, expected):
        from congress_videos.reap_clip_preparer_dag import _query_turns

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_turn_videos_for_shorts.return_value = []

        ti = _make_ti()
        _query_turns(ti, params={"max_turns": max_turns})

        mock_db.get_turn_videos_for_shorts.assert_called_once_with(max_turns=expected)


# ---------------------------------------------------------------------------
# TestStageAndPretrimClip
# ---------------------------------------------------------------------------


class TestStageAndPretrimClip:
    def _params(self, **overrides):
        base = {"max_turns": 0, "pre_trim_threshold_secs": 480, "pre_trim_target_secs": 360}
        return {**base, **overrides}

    def _default_turn(self, **overrides):
        turn = {
            "turn_id": 55,
            "output_path": "/data/output/turn55.mp4",
            "turn_type": "monologue",
            "keep_intervals": None,
            "chapter_id": 10,
            "resolved_name": "Jane Doe",
            "interest_score": 5,
            "group_start_seconds": 60.0,
            "group_end_seconds": 600.0,
            "procedural_seconds": 0.0,
            "group_duration_seconds": 540.0,
            "video_id": "vid-abc",
            "relevance_score": 4,
            "scoring_reasoning": "Good debate",
            "session_number": 12,
            "session_date": "2025-10-08",
        }
        turn.update(overrides)
        return turn

    def _patch_ffprobe(self, mocker, durations: list[float]):
        """Mock ffprobe to return the given durations in sequence, one per call."""
        outputs = [MagicMock(stdout=json.dumps({"format": {"duration": str(d)}}), returncode=0) for d in durations]
        mocker.patch("subprocess.run", side_effect=outputs)

    def test_short_turn_stages_output_path_unmodified(self, mocker):
        """Under-threshold clip: no pre-trim, staged path is output_path itself."""
        from congress_videos.reap_clip_preparer_dag import _stage_and_pretrim_clip

        self._patch_ffprobe(mocker, [240.0])
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        turn = self._default_turn()
        mock_db.get_turn_videos_for_shorts.return_value = [turn]
        mock_db.insert_video_short.return_value = 1

        ti = _make_ti()
        _stage_and_pretrim_clip(ti, params=self._params())

        mock_db.insert_video_short.assert_called_once()
        call_kwargs = mock_db.insert_video_short.call_args.kwargs
        assert call_kwargs["staged_clip_path"] == turn["output_path"]
        assert call_kwargs["pretrim_start_secs"] is None
        assert call_kwargs["pretrim_end_secs"] is None
        assert call_kwargs["pretrim_used_srt"] is False
        assert call_kwargs["turn_id"] == 55
        assert call_kwargs["chapter_id"] == 10
        assert ti.xcom_store["clips_queued"] == 1

    def test_over_threshold_writes_turn_reap_path_with_leading_window(self, mocker):
        """Over-threshold clip: staged to turn_{id}_reap.mp4 with offsets (0.0, target_secs)."""
        from congress_videos.reap_clip_preparer_dag import PROJECT_DATA_DIR, _stage_and_pretrim_clip

        self._patch_ffprobe(mocker, [720.0, 360.0])  # pre-probe over threshold, post-trim at target
        mocker.patch("os.makedirs")
        mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        mock_ffmpeg = mocker.patch("congress_videos.reap_clip_preparer_dag._ffmpeg_extract_window")

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        turn = self._default_turn()
        mock_db.get_turn_videos_for_shorts.return_value = [turn]
        mock_db.insert_video_short.return_value = 2

        ti = _make_ti()
        _stage_and_pretrim_clip(ti, params=self._params())

        mock_ffmpeg.assert_called_once()
        ffmpeg_kwargs = mock_ffmpeg.call_args.kwargs
        assert ffmpeg_kwargs["source_path"] == turn["output_path"]
        assert ffmpeg_kwargs["start_secs"] == 0.0
        assert ffmpeg_kwargs["end_secs"] == 360.0

        mock_db.insert_video_short.assert_called_once()
        call_kwargs = mock_db.insert_video_short.call_args.kwargs
        expected_path = f"{PROJECT_DATA_DIR}/vid-abc/10/turn_55_reap.mp4"
        assert call_kwargs["staged_clip_path"] == expected_path
        assert call_kwargs["staged_clip_path"] != turn["output_path"]
        assert call_kwargs["pretrim_start_secs"] == 0.0
        assert call_kwargs["pretrim_end_secs"] == 360.0
        assert call_kwargs["pretrim_used_srt"] is False
        assert call_kwargs["turn_id"] == 55

    def test_below_120s_actual_duration_is_skipped(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _stage_and_pretrim_clip

        self._patch_ffprobe(mocker, [90.0])
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_turn_videos_for_shorts.return_value = [self._default_turn()]

        ti = _make_ti()
        _stage_and_pretrim_clip(ti, params=self._params())

        mock_db.insert_video_short.assert_not_called()
        assert ti.xcom_store.get("clips_queued") == 0

    def test_ffprobe_failure_blocks_clip(self, mocker):
        from congress_videos.reap_clip_preparer_dag import _stage_and_pretrim_clip

        mocker.patch("subprocess.run", side_effect=Exception("ffprobe not found"))
        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_turn_videos_for_shorts.return_value = [self._default_turn()]

        ti = _make_ti()
        with pytest.raises(AirflowException, match="blocked"):
            _stage_and_pretrim_clip(ti, params=self._params())

        mock_db.insert_video_short.assert_not_called()

    def test_safety_gate_blocks_when_staged_duration_exceeds_tolerance(self, mocker):
        """If the post-trim probe still exceeds target + tolerance, block without inserting."""
        from congress_videos.reap_clip_preparer_dag import _stage_and_pretrim_clip

        self._patch_ffprobe(mocker, [720.0, 900.0])  # post-trim probe still too long
        mocker.patch("os.makedirs")
        mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        mocker.patch("congress_videos.reap_clip_preparer_dag._ffmpeg_extract_window")

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_turn_videos_for_shorts.return_value = [self._default_turn()]

        ti = _make_ti()
        with pytest.raises(AirflowException, match="blocked"):
            _stage_and_pretrim_clip(ti, params=self._params())

        mock_db.insert_video_short.assert_not_called()

    def test_partial_success_blocked_raises_after_inserting_good_clips(self, mocker):
        """Batch with 1 good + 1 blocked turn: good turn is inserted, then AirflowException raised."""
        from congress_videos.reap_clip_preparer_dag import _stage_and_pretrim_clip

        turn_good = self._default_turn(turn_id=1, output_path="/data/output/turn1.mp4")
        turn_blocked = self._default_turn(turn_id=2, output_path="/data/output/turn2.mp4")

        self._patch_ffprobe(mocker, [240.0, 1800.0])  # first OK, second exceeds threshold+tolerance

        mock_db_cls = mocker.patch("congress_videos.reap_clip_preparer_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_turn_videos_for_shorts.return_value = [turn_good, turn_blocked]
        mock_db.insert_video_short.return_value = 1

        ti = _make_ti()
        with pytest.raises(AirflowException, match="blocked"):
            _stage_and_pretrim_clip(ti, params=self._params(pre_trim_threshold_secs=1900))

        mock_db.insert_video_short.assert_called_once()
        assert ti.xcom_store.get("clips_queued") == 1
