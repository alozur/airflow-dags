"""Tests for speaker_turn_prepare DAG (issue #146, #152).

TDD cycle: RED tests written first; GREEN implementations follow.
"""
from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from subprocess import PIPE
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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


def _make_turn(turn_id: int = 1, output_path: str = "/data/video.mp4") -> dict:
    return {
        "turn_id": turn_id,
        "output_path": output_path,
        "chapter_id": 100,
        "resolved_name": "Speaker Name",
        "start_seconds": 10.0,
        "end_seconds": 300.0,
        "interest_score": 5,
        "video_id": "vidXYZ",
        "chapter_title": "Test Chapter",
        "description": "A test description",
        "relevance_score": 4,
        "key_speakers": ["Speaker Name"],
        "session_number": 1,
        "session_date": "2026-01-01",
        "materialized_at": "2026-01-01T00:00:00",
        # issue #177: resolution columns exposed by select_unprepared_turns
        "resolved_participant_slug": None,
        "speaker_resolution_confidence": None,
    }


# ---------------------------------------------------------------------------
# 1.1 DAG-load tests
# ---------------------------------------------------------------------------

class TestSpeakerTurnPrepareDagLoads:
    """Verify the DAG loads cleanly and has the required structural properties."""

    def test_dag_loads_without_error(self):
        """DagBag must import speaker_turn_prepare without errors."""
        from airflow.models import DagBag

        dagbag = DagBag(dag_folder="congress_videos", include_examples=False)
        assert "speaker_turn_prepare" in dagbag.dags, (
            f"speaker_turn_prepare DAG not found; errors: {dagbag.import_errors}"
        )
        assert not dagbag.import_errors.get("congress_videos/speaker_turn_prepare_dag.py"), (
            f"Import errors: {dagbag.import_errors}"
        )

    def test_dag_schedule_is_0_2_daily(self):
        """DAG schedule must be None (driven by chain trigger, not cron)."""
        from congress_videos.speaker_turn_prepare_dag import dag

        assert dag.schedule_interval is None

    def test_max_active_runs_is_1(self):
        """DAG must have max_active_runs=1 to serialise chain-triggered runs."""
        from congress_videos.speaker_turn_prepare_dag import dag

        assert dag.max_active_runs == 1

    def test_prepare_task_uses_nas_ffmpeg_pool(self):
        """The prepare_turns task must use pool='nas_ffmpeg'."""
        from congress_videos.speaker_turn_prepare_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        assert "prepare_turns" in tasks_by_id, (
            f"prepare_turns task not found; tasks={list(tasks_by_id.keys())}"
        )
        prepare_task = tasks_by_id["prepare_turns"]
        assert prepare_task.pool == "nas_ffmpeg"

    def test_prepare_task_pool_slots_is_1(self):
        """prepare_turns task must use pool_slots=1."""
        from congress_videos.speaker_turn_prepare_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        prepare_task = tasks_by_id["prepare_turns"]
        assert prepare_task.pool_slots == 1

    def test_dag_has_no_expand_call(self):
        """DAG source must not use .expand() as a live method call (no dynamic task mapping)."""
        from congress_videos.speaker_turn_prepare_dag import dag
        import ast

        # Resolve the DAG source relative to this test file so the test is
        # portable across worktrees.
        dag_source = Path(__file__).parent.parent.parent / "congress_videos" / "speaker_turn_prepare_dag.py"
        content = dag_source.read_text(encoding="utf-8")
        tree = ast.parse(content)
        # Find any call nodes whose function is an attribute named 'expand'
        for node in ast.walk(tree):
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr == "expand"
            ):
                raise AssertionError(
                    f"DAG uses .expand() at line {node.lineno} — dynamic task mapping is prohibited"
                )


# ---------------------------------------------------------------------------
# 3.1–3.4 Sequential loop and sidecar tests
# ---------------------------------------------------------------------------

class TestPrepareTurnsCallableSequentialLoop:
    """Verify the prepare callable iterates sequentially and gates on the ffmpeg decode check/sidecars.

    After issue #169: _trigger_thumbnail_for_turn is deleted; tests no longer patch it.
    _prepare_turns_callable steps: srt-sidecar → decode-check → mark_turn_prepared.
    """

    def test_two_turns_processed_sequentially(self):
        """Given 2 turns, callable iterates both; no concurrent fork."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        call_order = []
        turns = [_make_turn(1, "/data/v1.mp4"), _make_turn(2, "/data/v2.mp4")]

        def fake_db_select(limit=2):
            return turns

        def fake_mark_prepared(turn_id):
            call_order.append(("mark_prepared", turn_id))

        mock_db = MagicMock()
        mock_db.select_unprepared_turns.side_effect = fake_db_select
        mock_db.mark_turn_prepared.side_effect = fake_mark_prepared

        def fake_write_sidecars(turn, **kwargs):
            call_order.append(("sidecars", turn["turn_id"]))

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("subprocess.run") as mock_subproc,
        ):
            mock_subproc.return_value = MagicMock(returncode=0)
            _prepare_turns_callable()

        # Both turns must be prepared; turn 1 must fully complete before turn 2 starts
        sidecar_indices = [i for i, item in enumerate(call_order) if item[0] == "sidecars"]
        assert len(sidecar_indices) == 2
        # First sidecars call (turn 1) must come before second (turn 2)
        assert sidecar_indices[0] < sidecar_indices[1]
        # mark_prepared for turn 1 must precede sidecars for turn 2
        mark1_idx = next(i for i, item in enumerate(call_order) if item == ("mark_prepared", 1))
        sidecar2_idx = next(i for i, item in enumerate(call_order) if item == ("sidecars", 2))
        assert mark1_idx < sidecar2_idx

    def test_mark_turn_prepared_not_called_when_ffmpeg_nonzero(self):
        """If the ffmpeg decode check returns non-zero rc, mark_turn_prepared must NOT be called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run") as mock_subproc,
        ):
            mock_subproc.return_value = MagicMock(returncode=1)
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()

    def test_mark_turn_prepared_not_called_when_sidecar_raises(self):
        """If a sidecar write raises, mark_turn_prepared must NOT be called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch(
                "congress_videos.speaker_turn_prepare_dag._write_turn_sidecars",
                side_effect=OSError("disk full"),
            ),
            patch("subprocess.run") as mock_subproc,
        ):
            mock_subproc.return_value = MagicMock(returncode=0)
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()

    def test_mark_turn_prepared_called_last_after_ffmpeg_passes(self):
        """mark_turn_prepared must be called AFTER the ffmpeg decode check rc==0 (last step)."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        call_order = []
        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns
        mock_db.mark_turn_prepared.side_effect = lambda tid: call_order.append("mark_prepared")

        def fake_write_sidecars(turn, **kwargs):
            call_order.append("write_sidecars")

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("subprocess.run") as mock_subproc,
        ):
            def fake_run(*args, **kwargs):
                call_order.append("ffmpeg")
                return MagicMock(returncode=0)
            mock_subproc.side_effect = fake_run
            _prepare_turns_callable()

        assert "write_sidecars" in call_order
        assert "ffmpeg" in call_order
        assert "mark_prepared" in call_order
        # ffmpeg decode check must come before mark_prepared
        assert call_order.index("ffmpeg") < call_order.index("mark_prepared")
        # write_sidecars must come before mark_prepared
        assert call_order.index("write_sidecars") < call_order.index("mark_prepared")

    def test_no_sidecar_call_when_turn_list_empty(self):
        """When no turns are available, _write_turn_sidecars must not be called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = []

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars") as mock_side,
            patch("subprocess.run") as mock_sub,
        ):
            _prepare_turns_callable()

        mock_side.assert_not_called()
        mock_sub.assert_not_called()
        mock_db.mark_turn_prepared.assert_not_called()

    def test_zero_turns_no_sidecar_writes_no_db_updates(self):
        """Given zero unprepared turns, callable returns without error and no side effects."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = []

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run") as mock_sub,
        ):
            # Must not raise
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()
        mock_sub.assert_not_called()

    def test_ffmpeg_corrupt_check_prepared_at_not_set_when_nonzero(self):
        """ffmpeg decode rc != 0 must leave prepared_at NULL (mark_turn_prepared not called)."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=1)),
        ):
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()

    def test_ffmpeg_corrupt_check_prepared_at_set_when_zero(self):
        """ffmpeg decode rc == 0 + all sidecars OK must result in mark_turn_prepared called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# 3.5 Integrity-check helper: _run_ffmpeg_decode_check
# Note: TestTriggerThumbnailPollTimeout (3 tests) deleted — the function
# _trigger_thumbnail_for_turn was removed in issue #169 unify-upload-metadata.
# ---------------------------------------------------------------------------

class TestWriteTurnSidecarsGroupedRange:
    """Verify _write_turn_sidecars uses group_start/end_seconds for SRT windowing.

    Mocks: find_srt_for_chapter, _parse_srt_blocks,
           generate_youtube_metadata_for_selected_videos, _write_orador_sidecars.
    """

    def _make_grouped_turn(
        self,
        tmp_path,
        turn_id: int = 278,
        group_start: float = 19157.0,
        group_end: float = 19784.0,
    ) -> dict:
        video_dir = tmp_path / "output_turn_278"
        video_dir.mkdir()
        return {
            "turn_id": turn_id,
            "output_path": str(video_dir / "video.mp4"),
            "chapter_id": 100,
            "resolved_name": "Speaker Name",
            "start_seconds": group_start,
            "end_seconds": group_end,
            "group_start_seconds": group_start,
            "group_end_seconds": group_end,
            "interest_score": 5,
            "video_id": "vidXYZ",
            "chapter_title": "Test Chapter",
            "description": "A test description",
            "relevance_score": 4,
            "key_speakers": ["Speaker Name"],
            "session_number": 1,
            "session_date": "2026-01-01",
            "materialized_at": "2026-01-01T00:00:00",
        }

    def test_chapter278_grouped_produces_nonempty_retimed_srt(self, tmp_path):
        """Grouped turn with group_start=19157/group_end=19784 and overlapping SRT
        blocks produces a non-empty subtitles.srt whose first entry is near 00:00:00.

        After issue #169: generate_youtube_metadata and _write_orador_sidecars patches
        removed — _write_turn_sidecars no longer calls them.
        """
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        turn = self._make_grouped_turn(tmp_path)
        video_dir = Path(turn["output_path"]).parent

        # One SRT block fully inside the group window
        fake_blocks = [
            {"start_secs": 19200.0, "end_secs": 19210.0, "text": "Bloque de prueba"},
        ]

        with (
            patch(
                "congress_videos.srt_helpers.find_srt_for_chapter",
                return_value="/fake/source.srt",
            ),
            patch(
                "congress_videos.srt_helpers._parse_srt_blocks",
                return_value=fake_blocks,
            ),
        ):
            _write_turn_sidecars(turn)

        srt_path = video_dir / "subtitles.srt"
        assert srt_path.exists(), "subtitles.srt must be written"
        content = srt_path.read_text(encoding="utf-8")
        assert len(content) > 0, "subtitles.srt must be non-empty for overlapping blocks"
        # First SRT entry must be re-timed to near 00:00:00 (19200 - 19157 = 43s)
        assert "00:00:43" in content, (
            f"First entry should be at ~43s (19200-19157), got: {content[:200]}"
        )

    def test_group_span_no_overlap_writes_empty_file(self, tmp_path):
        """When no SRT blocks overlap the group span, subtitles.srt is 0 bytes and
        no exception is raised.

        After issue #169: generate_youtube_metadata and _write_orador_sidecars patches
        removed — _write_turn_sidecars no longer calls them.
        """
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        turn = self._make_grouped_turn(
            tmp_path, group_start=50000.0, group_end=51000.0
        )
        video_dir = Path(turn["output_path"]).parent

        # Blocks entirely outside the 50000-51000 window
        fake_blocks = [
            {"start_secs": 100.0, "end_secs": 200.0, "text": "Outside"},
            {"start_secs": 300.0, "end_secs": 400.0, "text": "Also outside"},
        ]

        with (
            patch(
                "congress_videos.srt_helpers.find_srt_for_chapter",
                return_value="/fake/source.srt",
            ),
            patch(
                "congress_videos.srt_helpers._parse_srt_blocks",
                return_value=fake_blocks,
            ),
        ):
            _write_turn_sidecars(turn)  # must not raise

        srt_path = video_dir / "subtitles.srt"
        assert srt_path.exists()
        assert srt_path.stat().st_size == 0, "subtitles.srt must be 0 bytes when no blocks overlap"

    def test_single_turn_no_group_fields_regression(self, tmp_path):
        """Turn dict without group_start/end_seconds uses per-turn fallback — backward compat.

        After issue #169: generate_youtube_metadata and _write_orador_sidecars patches
        removed — _write_turn_sidecars no longer calls them.
        """
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        video_dir = tmp_path / "single_turn"
        video_dir.mkdir()
        turn = {
            "turn_id": 1,
            "output_path": str(video_dir / "video.mp4"),
            "chapter_id": 50,
            "resolved_name": "Solo Speaker",
            "start_seconds": 300.0,
            "end_seconds": 420.0,
            # No group_start_seconds / group_end_seconds keys
            "interest_score": 3,
            "video_id": "vidABC",
            "chapter_title": "Chapter 1",
            "description": "desc",
            "relevance_score": 3,
            "key_speakers": [],
            "session_number": 2,
            "session_date": "2026-01-02",
            "materialized_at": "2026-01-02T00:00:00",
        }

        # Block inside [300, 420] → should produce non-empty SRT
        fake_blocks = [{"start_secs": 305.0, "end_secs": 310.0, "text": "Solo"}]

        with (
            patch(
                "congress_videos.srt_helpers.find_srt_for_chapter",
                return_value="/fake/source.srt",
            ),
            patch(
                "congress_videos.srt_helpers._parse_srt_blocks",
                return_value=fake_blocks,
            ),
        ):
            _write_turn_sidecars(turn)

        srt_path = video_dir / "subtitles.srt"
        assert srt_path.exists()
        content = srt_path.read_text(encoding="utf-8")
        assert len(content) > 0, "Single-turn SRT must be non-empty for overlapping block"


class TestIntegrityCheckUsesFFmpeg:
    """_run_ffmpeg_decode_check must invoke ffmpeg -f null, NOT ffprobe.

    ffprobe does not support the -f null output muxer and always returns rc=1,
    which means prepared_at is never set (live-confirmed on prod 2026-08-22).
    """

    def test_integrity_check_helper_calls_module_subprocess(self):
        """_run_ffmpeg_decode_check must call the module-bound subprocess.run exactly once
        with the correct argv and return its returncode.
        """
        from congress_videos.speaker_turn_prepare_dag import _run_ffmpeg_decode_check

        mock_result = MagicMock()
        mock_result.returncode = 0

        with patch(
            "congress_videos.speaker_turn_prepare_dag.subprocess.run",
            return_value=mock_result,
        ) as mock_run:
            rc = _run_ffmpeg_decode_check("/data/v1.mp4")

        mock_run.assert_called_once_with(
            ["ffmpeg", "-v", "error", "-i", "/data/v1.mp4", "-f", "null", "-"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        assert rc == 0

    def test_integrity_check_helper_propagates_nonzero(self):
        """_run_ffmpeg_decode_check returns the non-zero rc without raising."""
        from congress_videos.speaker_turn_prepare_dag import _run_ffmpeg_decode_check

        mock_result = MagicMock()
        mock_result.returncode = 1

        with patch(
            "congress_videos.speaker_turn_prepare_dag.subprocess.run",
            return_value=mock_result,
        ):
            rc = _run_ffmpeg_decode_check("/data/bad.mp4")

        assert rc == 1

    def test_integrity_check_invokes_ffmpeg_decode(self):
        """_prepare_turns_callable must reach _run_ffmpeg_decode_check with ffmpeg argv.

        Regression guard: verifies cmd[0] == 'ffmpeg' (not 'ffprobe') at the
        _prepare_turns_callable level via module-bound subprocess.run patch.
        After issue #169: _trigger_thumbnail_for_turn is deleted; patch removed.
        """
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        captured_cmd = []

        def fake_run(cmd, **kwargs):
            captured_cmd.append(cmd)
            return MagicMock(returncode=0)

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.subprocess.run", side_effect=fake_run),
        ):
            _prepare_turns_callable()

        assert len(captured_cmd) == 1, "subprocess.run must be called exactly once"
        cmd = captured_cmd[0]
        assert cmd[0] == "ffmpeg", (
            f"Integrity check must use 'ffmpeg', got '{cmd[0]}'. "
            "ffprobe rejects -f null and always returns rc=1."
        )
        assert cmd == ["ffmpeg", "-v", "error", "-i", "/data/v1.mp4", "-f", "null", "-"], (
            f"Full command mismatch: {cmd}"
        )


# ---------------------------------------------------------------------------
# Smoke tests: _run_ffmpeg_decode_check with a real ffmpeg binary
# ---------------------------------------------------------------------------

class TestDecodeCheckSmoke:
    """Real ffmpeg smoke tests for _run_ffmpeg_decode_check (integration, slow)."""

    @pytest.mark.integration
    @pytest.mark.slow
    def test_valid_clip_returns_zero(self, tmp_path):
        """A valid libx264 clip must be accepted (rc == 0) by _run_ffmpeg_decode_check."""
        shutil.which("ffmpeg") or pytest.skip("ffmpeg not on PATH")
        from congress_videos.speaker_turn_prepare_dag import _run_ffmpeg_decode_check

        out = tmp_path / "out.mp4"
        subprocess.run(
            [
                "ffmpeg", "-y",
                "-f", "lavfi", "-i", "testsrc=duration=1:size=128x64:rate=25",
                "-c:v", "libx264", "-preset", "ultrafast",
                str(out),
            ],
            stdout=PIPE,
            stderr=PIPE,
            timeout=120,
        )
        assert _run_ffmpeg_decode_check(str(out)) == 0

    @pytest.mark.integration
    @pytest.mark.slow
    def test_corrupt_file_returns_nonzero(self, tmp_path):
        """A garbage file must be rejected (rc != 0) by _run_ffmpeg_decode_check."""
        shutil.which("ffmpeg") or pytest.skip("ffmpeg not on PATH")
        from congress_videos.speaker_turn_prepare_dag import _run_ffmpeg_decode_check

        bad = tmp_path / "bad.mp4"
        bad.write_bytes(b"not a real mp4 file\x00\x01" * 8)
        assert _run_ffmpeg_decode_check(str(bad)) != 0


# ---------------------------------------------------------------------------
# Phase 2 RED tests: prepare DAG shrink (issue #169)
# After unify-upload-metadata, _write_turn_sidecars must NOT call AI and
# _prepare_turns_callable must NOT trigger the thumbnail DAG.
# ---------------------------------------------------------------------------


class TestWriteTurnSidecarsNoAiCall:
    """_write_turn_sidecars must write only subtitles.srt; no AI call (issue #169)."""

    def test_write_turn_sidecars_no_ai_call(self, tmp_path):
        """_write_turn_sidecars must not call generate_youtube_metadata_for_selected_videos."""
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        video_dir = tmp_path / "turn_1"
        video_dir.mkdir()
        turn = _make_turn(turn_id=1, output_path=str(video_dir / "video.mp4"))

        fake_blocks = [{"start_secs": 10.0, "end_secs": 15.0, "text": "test block"}]

        with (
            patch(
                "congress_videos.srt_helpers.find_srt_for_chapter",
                return_value="/fake/source.srt",
            ),
            patch(
                "congress_videos.srt_helpers._parse_srt_blocks",
                return_value=fake_blocks,
            ),
            patch(
                "congress_videos.modules.youtube.youtube_ai"
                ".generate_youtube_metadata_for_selected_videos",
            ) as mock_ai,
        ):
            _write_turn_sidecars(turn)

        mock_ai.assert_not_called(), (
            "_write_turn_sidecars must not call AI metadata generation after issue #169"
        )

    def test_write_turn_sidecars_writes_subtitles_srt(self, tmp_path):
        """_write_turn_sidecars still writes subtitles.srt after removing AI call."""
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        video_dir = tmp_path / "turn_1"
        video_dir.mkdir()
        turn = _make_turn(turn_id=1, output_path=str(video_dir / "video.mp4"))
        turn["start_seconds"] = 10.0
        turn["end_seconds"] = 20.0

        fake_blocks = [{"start_secs": 10.0, "end_secs": 15.0, "text": "contenido srt"}]

        with (
            patch(
                "congress_videos.srt_helpers.find_srt_for_chapter",
                return_value="/fake/source.srt",
            ),
            patch(
                "congress_videos.srt_helpers._parse_srt_blocks",
                return_value=fake_blocks,
            ),
        ):
            _write_turn_sidecars(turn)

        srt_path = video_dir / "subtitles.srt"
        assert srt_path.exists(), "subtitles.srt must still be written after AI removal"
        content = srt_path.read_text(encoding="utf-8")
        assert len(content) > 0


class TestPrepareTurnsCallableNoThumbnailTrigger:
    """_prepare_turns_callable must not call _trigger_thumbnail_for_turn (issue #169)."""

    def test_prepare_turns_callable_no_thumbnail_trigger(self):
        """After issue #169, _trigger_thumbnail_for_turn must not be called from _prepare_turns_callable."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        # _trigger_thumbnail_for_turn is deleted — accessing it via hasattr must return False,
        # OR if the module attr is absent, the patch would raise AttributeError.
        # We assert no thumbnail trigger is called and the prepare still succeeds.
        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            # _trigger_thumbnail_for_turn must not be callable (deleted) or must not be called.
            # After deletion, accessing it on the module raises AttributeError.
            import congress_videos.speaker_turn_prepare_dag as prepare_mod
            assert not hasattr(prepare_mod, "_trigger_thumbnail_for_turn"), (
                "_trigger_thumbnail_for_turn must be deleted from the module (issue #169)"
            )
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# Phase 2 RED tests: speaker resolution step (issue #177)
# ---------------------------------------------------------------------------


class TestSpeakerResolutionStep:
    """Resolution step runs after select_unprepared_turns and before sidecars.

    All tests in this class require the resolution columns in _make_turn().
    """

    def _make_participants(self):
        return [{"slug": "pedro-sanchez", "display_name": "Pedro Sanchez", "party": "PSOE"}]

    def test_resolution_runs_before_sidecar_write(self):
        """resolve_speaker must be called before _write_turn_sidecars in the prepare loop."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        call_order = []
        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        def fake_resolve(turn, participants, completion_fn=None):
            call_order.append("resolve")
            return None

        def fake_write_sidecars(turn, **kwargs):
            call_order.append("sidecars")

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", side_effect=fake_resolve),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert "resolve" in call_order
        assert "sidecars" in call_order
        assert call_order.index("resolve") < call_order.index("sidecars"), (
            "resolve_speaker must be called before _write_turn_sidecars"
        )

    def test_resolution_skips_when_already_resolved_above_threshold(self):
        """Skip resolve_speaker when turn has slug set AND confidence >= 0.80."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turn = _make_turn(1, "/data/v1.mp4")
        turn["resolved_participant_slug"] = "pedro-sanchez"
        turn["speaker_resolution_confidence"] = 0.92

        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = [turn]

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker") as mock_resolve,
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        mock_resolve.assert_not_called()

    def test_resolution_proceeds_on_none_result(self):
        """When resolve_speaker returns None, sidecar step still runs (never blocks)."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars") as mock_sidecars,
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=None),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        mock_sidecars.assert_called_once()

    def test_resolution_patches_resolved_name_in_memory(self):
        """On successful resolution, turn['resolved_name'] is updated before sidecar call."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        resolved_names_at_sidecar = []

        def fake_write_sidecars(turn, **kwargs):
            resolved_names_at_sidecar.append(turn.get("resolved_name"))

        participants = [{"slug": "pedro-sanchez", "display_name": "Pedro Sanchez", "party": "PSOE"}]
        resolution_result = {"participant_slug": "pedro-sanchez", "confidence": 0.92, "evidence": "..."}

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=resolution_result),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=participants),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert resolved_names_at_sidecar == ["Pedro Sanchez"], (
            f"resolved_name must be 'Pedro Sanchez' before sidecars; got {resolved_names_at_sidecar}"
        )

    def test_resolution_failure_does_not_block_preparation(self):
        """If resolve_speaker raises unexpectedly, the sidecar step still runs."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars") as mock_sidecars,
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", side_effect=RuntimeError("unexpected")),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        mock_sidecars.assert_called_once()


# ---------------------------------------------------------------------------
# Phase 3 RED tests: _write_turn_sidecars window adjustment (issue #175)
# ---------------------------------------------------------------------------


class TestWriteTurnSidecarsVadWindowAdjustment:
    """_write_turn_sidecars must narrow the SRT window when trim offsets are given.

    Tests 3.1–3.3: window_start, window_end adjusted by trim offsets before
    _window_srt_blocks is called.
    """

    def _make_grouped_turn(self, tmp_path, group_start: float = 100.0, group_end: float = 400.0) -> dict:
        video_dir = tmp_path / "output_turn"
        video_dir.mkdir()
        return {
            "turn_id": 1,
            "output_path": str(video_dir / "video.mp4"),
            "chapter_id": 10,
            "resolved_name": "Speaker",
            "start_seconds": group_start,
            "end_seconds": group_end,
            "group_start_seconds": group_start,
            "group_end_seconds": group_end,
            "interest_score": 5,
            "video_id": "vidXYZ",
            "chapter_title": "Chapter",
            "description": "Desc",
            "relevance_score": 4,
            "key_speakers": [],
            "session_number": 1,
            "session_date": "2026-01-01",
            "materialized_at": "2026-01-01T00:00:00",
        }

    def test_sidecar_window_narrowed_by_trim(self, tmp_path):
        """_write_turn_sidecars(turn, trim_start_secs=2.5, trim_end_secs=3.0) must
        call _window_srt_blocks with window_start+2.5 and window_end-3.0."""
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        group_start = 100.0
        group_end = 400.0
        turn = self._make_grouped_turn(tmp_path, group_start, group_end)

        captured_bounds = []

        def fake_window_srt_blocks(blocks, ws, we):
            captured_bounds.append((ws, we))
            return []

        with (
            patch("congress_videos.srt_helpers.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.srt_helpers._parse_srt_blocks", return_value=[]),
            patch("congress_videos.srt_helpers._window_srt_blocks", side_effect=fake_window_srt_blocks),
        ):
            _write_turn_sidecars(turn, trim_start_secs=2.5, trim_end_secs=3.0)

        assert len(captured_bounds) == 1, "must call _window_srt_blocks exactly once"
        ws, we = captured_bounds[0]
        assert ws == pytest.approx(group_start + 2.5), (
            f"window_start must be {group_start+2.5}, got {ws}"
        )
        assert we == pytest.approx(group_end - 3.0), (
            f"window_end must be {group_end-3.0}, got {we}"
        )

    def test_sidecar_window_unchanged_on_zero_trim(self, tmp_path):
        """trim_start_secs=0.0, trim_end_secs=0.0 → _window_srt_blocks receives original bounds."""
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        group_start = 100.0
        group_end = 400.0
        turn = self._make_grouped_turn(tmp_path, group_start, group_end)

        captured_bounds = []

        def fake_window_srt_blocks(blocks, ws, we):
            captured_bounds.append((ws, we))
            return []

        with (
            patch("congress_videos.srt_helpers.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.srt_helpers._parse_srt_blocks", return_value=[]),
            patch("congress_videos.srt_helpers._window_srt_blocks", side_effect=fake_window_srt_blocks),
        ):
            _write_turn_sidecars(turn, trim_start_secs=0.0, trim_end_secs=0.0)

        ws, we = captured_bounds[0]
        assert ws == pytest.approx(group_start), f"window_start must be {group_start}, got {ws}"
        assert we == pytest.approx(group_end), f"window_end must be {group_end}, got {we}"

    def test_sidecar_grouped_turn_window_adjusted(self, tmp_path):
        """Grouped turn: _window_srt_blocks gets group_start+trim_start, group_end-trim_end."""
        from congress_videos.speaker_turn_prepare_dag import _write_turn_sidecars

        group_start = 19157.0
        group_end = 19784.0
        turn = self._make_grouped_turn(tmp_path, group_start, group_end)

        captured_bounds = []

        def fake_window_srt_blocks(blocks, ws, we):
            captured_bounds.append((ws, we))
            return []

        with (
            patch("congress_videos.srt_helpers.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.srt_helpers._parse_srt_blocks", return_value=[]),
            patch("congress_videos.srt_helpers._window_srt_blocks", side_effect=fake_window_srt_blocks),
        ):
            _write_turn_sidecars(turn, trim_start_secs=5.0, trim_end_secs=8.0)

        ws, we = captured_bounds[0]
        assert ws == pytest.approx(group_start + 5.0)
        assert we == pytest.approx(group_end - 8.0)


# ---------------------------------------------------------------------------
# Phase 4 RED tests: DAG prepare callable — VAD step integration (issue #175)
# ---------------------------------------------------------------------------


class TestPrepareTurnsCallableVadStep:
    """VAD trim must be called after speaker-resolution and before _write_turn_sidecars.

    Tests 4.1–4.5: placement, offsets threaded, zero-trim, decode-check, turn_type parity.
    """

    def _make_participants(self):
        return []

    def test_vad_called_before_sidecars(self):
        """VAD trim must be called after speaker-resolution and before _write_turn_sidecars."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        call_order = []
        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        def fake_vad(path, **kwargs):
            call_order.append("vad")
            return (0.0, 0.0)

        def fake_sidecars(turn, **kwargs):
            call_order.append("sidecars")

        def fake_resolve(turn, participants, **kwargs):
            call_order.append("resolve")
            return None

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", side_effect=fake_vad),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", side_effect=fake_resolve),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert "vad" in call_order, "trim_turn_silence_with_vad must be called"
        assert "sidecars" in call_order, "_write_turn_sidecars must be called"
        vad_idx = call_order.index("vad")
        sidecars_idx = call_order.index("sidecars")
        assert vad_idx < sidecars_idx, "VAD must be called before sidecars"

    def test_vad_offsets_flow_into_sidecars(self):
        """VAD returning (1.5, 2.0) → _write_turn_sidecars called with trim_start_secs=1.5, trim_end_secs=2.0."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        captured_kwargs = []

        def fake_sidecars(turn, **kwargs):
            captured_kwargs.append(kwargs)

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(1.5, 2.0)),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=None),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert len(captured_kwargs) == 1
        kw = captured_kwargs[0]
        assert kw.get("trim_start_secs") == pytest.approx(1.5), f"trim_start_secs must be 1.5, got {kw}"
        assert kw.get("trim_end_secs") == pytest.approx(2.0), f"trim_end_secs must be 2.0, got {kw}"

    def test_vad_zero_offsets_no_change(self):
        """VAD returning (0.0, 0.0) → _write_turn_sidecars called with trim_start_secs=0.0, trim_end_secs=0.0."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        captured_kwargs = []

        def fake_sidecars(turn, **kwargs):
            captured_kwargs.append(kwargs)

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(0.0, 0.0)),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_sidecars),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=None),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert len(captured_kwargs) == 1
        kw = captured_kwargs[0]
        assert kw.get("trim_start_secs") == pytest.approx(0.0)
        assert kw.get("trim_end_secs") == pytest.approx(0.0)

    def test_decode_check_on_trimmed_file(self):
        """After VAD trim with non-zero offsets, decode check runs on same output_path."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", return_value=(2.0, 3.0)),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=None),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("congress_videos.speaker_turn_prepare_dag.subprocess.run") as mock_sub,
        ):
            mock_sub.return_value = MagicMock(returncode=0)
            _prepare_turns_callable()

        # ffmpeg decode check must be called with the output_path
        mock_sub.assert_called_once()
        cmd = mock_sub.call_args[0][0]
        assert "/data/v1.mp4" in cmd, (
            f"Decode check must operate on output_path /data/v1.mp4; cmd={cmd}"
        )

    def test_monologue_and_qa_both_vad_called(self):
        """Two turns (different turn_type) → VAD called for both."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turn1 = _make_turn(1, "/data/v1.mp4")
        turn1["turn_type"] = "monologue"
        turn2 = _make_turn(2, "/data/v2.mp4")
        turn2["turn_type"] = "qa"
        turns = [turn1, turn2]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        vad_calls = []

        def fake_vad(path, **kwargs):
            vad_calls.append(path)
            return (0.0, 0.0)

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag.trim_turn_silence_with_vad", side_effect=fake_vad),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("congress_videos.speaker_turn_prepare_dag.resolve_speaker", return_value=None),
            patch("congress_videos.speaker_turn_prepare_dag.get_all_participants", return_value=self._make_participants()),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert len(vad_calls) == 2, f"VAD must be called for both turns; calls={vad_calls}"
        assert "/data/v1.mp4" in vad_calls
        assert "/data/v2.mp4" in vad_calls
