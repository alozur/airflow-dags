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
        """DAG schedule must be '0 2 * * *' (UTC 02:00, off-peak)."""
        from congress_videos.speaker_turn_prepare_dag import dag

        assert dag.schedule_interval == "0 2 * * *" or dag.schedule == "0 2 * * *"

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
    """Verify the prepare callable iterates sequentially and gates on ffprobe/sidecars."""

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

        def fake_trigger_thumbnail(turn):
            call_order.append(("thumbnail", turn["turn_id"]))

        def fake_write_sidecars(turn):
            call_order.append(("sidecars", turn["turn_id"]))

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn", side_effect=fake_trigger_thumbnail),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("subprocess.run") as mock_subproc,
        ):
            mock_subproc.return_value = MagicMock(returncode=0)
            _prepare_turns_callable()

        # Both turns must be prepared; turn 1 must fully complete before turn 2 starts
        thumbnail_indices = [i for i, item in enumerate(call_order) if item[0] == "thumbnail"]
        assert len(thumbnail_indices) == 2
        # First thumbnail call (turn 1) must come before second thumbnail call (turn 2)
        assert thumbnail_indices[0] < thumbnail_indices[1]
        # mark_prepared for turn 1 must precede thumbnail for turn 2
        mark1_idx = next(i for i, item in enumerate(call_order) if item == ("mark_prepared", 1))
        thumb2_idx = next(i for i, item in enumerate(call_order) if item == ("thumbnail", 2))
        assert mark1_idx < thumb2_idx

    def test_mark_turn_prepared_not_called_when_ffprobe_nonzero(self):
        """If ffprobe returns non-zero rc, mark_turn_prepared must NOT be called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
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
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch(
                "congress_videos.speaker_turn_prepare_dag._write_turn_sidecars",
                side_effect=OSError("disk full"),
            ),
            patch("subprocess.run") as mock_subproc,
        ):
            mock_subproc.return_value = MagicMock(returncode=0)
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()

    def test_mark_turn_prepared_called_last_after_ffprobe_passes(self):
        """mark_turn_prepared must be called AFTER ffprobe rc==0 (last step)."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        call_order = []
        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns
        mock_db.mark_turn_prepared.side_effect = lambda tid: call_order.append("mark_prepared")

        def fake_write_sidecars(turn):
            call_order.append("write_sidecars")

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars", side_effect=fake_write_sidecars),
            patch("subprocess.run") as mock_subproc,
        ):
            def fake_run(*args, **kwargs):
                call_order.append("ffprobe")
                return MagicMock(returncode=0)
            mock_subproc.side_effect = fake_run
            _prepare_turns_callable()

        assert "write_sidecars" in call_order
        assert "ffprobe" in call_order
        assert "mark_prepared" in call_order
        # ffprobe must come before mark_prepared
        assert call_order.index("ffprobe") < call_order.index("mark_prepared")
        # write_sidecars must come before mark_prepared
        assert call_order.index("write_sidecars") < call_order.index("mark_prepared")

    def test_thumbnail_triggered_once_per_turn(self):
        """_trigger_thumbnail_for_turn must be called once per turn."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4"), _make_turn(2, "/data/v2.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        thumbnail_calls = []

        def fake_trigger(turn):
            thumbnail_calls.append(turn["turn_id"])

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn", side_effect=fake_trigger),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        assert thumbnail_calls == [1, 2]

    def test_no_thumbnail_call_when_turn_list_empty(self):
        """When no turns are available, _trigger_thumbnail_for_turn must not be called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = []

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn") as mock_thumb,
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars") as mock_side,
            patch("subprocess.run") as mock_sub,
        ):
            _prepare_turns_callable()

        mock_thumb.assert_not_called()
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
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run") as mock_sub,
        ):
            # Must not raise
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()
        mock_sub.assert_not_called()

    def test_ffprobe_corrupt_check_prepared_at_not_set_when_nonzero(self):
        """ffprobe rc != 0 must leave prepared_at NULL (mark_turn_prepared not called)."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=1)),
        ):
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_not_called()

    def test_ffprobe_corrupt_check_prepared_at_set_when_zero(self):
        """ffprobe rc == 0 + all sidecars OK must result in mark_turn_prepared called."""
        from congress_videos.speaker_turn_prepare_dag import _prepare_turns_callable

        turns = [_make_turn(1, "/data/v1.mp4")]
        mock_db = MagicMock()
        mock_db.select_unprepared_turns.return_value = turns

        with (
            patch("congress_videos.speaker_turn_prepare_dag.CongressionalVideoDB", return_value=mock_db),
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
            patch("subprocess.run", return_value=MagicMock(returncode=0)),
        ):
            _prepare_turns_callable()

        mock_db.mark_turn_prepared.assert_called_once_with(1)


# ---------------------------------------------------------------------------
# 3.5 Thumbnail poll-loop timeout (issue #146 Fix B)
# ---------------------------------------------------------------------------

class TestTriggerThumbnailPollTimeout:
    """_trigger_thumbnail_for_turn must bound the poll loop with a deadline."""

    def test_timeout_raises_runtime_error(self):
        """A dag_run stuck in 'running' past the deadline must raise RuntimeError."""
        from congress_videos.speaker_turn_prepare_dag import _trigger_thumbnail_for_turn

        dag_run = MagicMock()
        dag_run.run_id = "prepare_turn_1_stuck"
        dag_run.state = "running"  # never resolves

        # monotonic advances well past the timeout on the second read.
        monotonic_values = iter([0.0, 0.0, 100000.0, 100000.0, 100000.0])

        with (
            patch(
                "congress_videos.speaker_turn_prepare_dag.trigger_dag_api",
                return_value=dag_run,
            ),
            patch("congress_videos.speaker_turn_prepare_dag.time.sleep"),
            patch(
                "congress_videos.speaker_turn_prepare_dag.time.monotonic",
                side_effect=lambda: next(monotonic_values),
            ),
        ):
            with pytest.raises(RuntimeError) as exc:
                _trigger_thumbnail_for_turn(_make_turn(1, "/data/v1.mp4"))

        # Error must name the turn_id and run_id so the operator can trace it.
        msg = str(exc.value)
        assert "1" in msg
        assert "prepare_turn_1_stuck" in msg

    def test_success_before_deadline_does_not_raise(self):
        """A dag_run reaching 'success' before the deadline must not raise."""
        from congress_videos.speaker_turn_prepare_dag import _trigger_thumbnail_for_turn

        dag_run = MagicMock()
        dag_run.run_id = "prepare_turn_1_ok"
        dag_run.state = "success"

        with (
            patch(
                "congress_videos.speaker_turn_prepare_dag.trigger_dag_api",
                return_value=dag_run,
            ),
            patch("congress_videos.speaker_turn_prepare_dag.time.sleep"),
            patch(
                "congress_videos.speaker_turn_prepare_dag.time.monotonic",
                side_effect=[0.0, 1.0],
            ),
        ):
            # Must not raise
            _trigger_thumbnail_for_turn(_make_turn(1, "/data/v1.mp4"))

    def test_failed_state_raises_runtime_error(self):
        """A dag_run reaching 'failed' before the deadline must still raise RuntimeError."""
        from congress_videos.speaker_turn_prepare_dag import _trigger_thumbnail_for_turn

        dag_run = MagicMock()
        dag_run.run_id = "prepare_turn_1_failed"
        dag_run.state = "failed"

        with (
            patch(
                "congress_videos.speaker_turn_prepare_dag.trigger_dag_api",
                return_value=dag_run,
            ),
            patch("congress_videos.speaker_turn_prepare_dag.time.sleep"),
            patch(
                "congress_videos.speaker_turn_prepare_dag.time.monotonic",
                side_effect=[0.0, 1.0],
            ),
        ):
            with pytest.raises(RuntimeError):
                _trigger_thumbnail_for_turn(_make_turn(1, "/data/v1.mp4"))


# ---------------------------------------------------------------------------
# 3.6 Integrity-check helper: _run_ffmpeg_decode_check
# ---------------------------------------------------------------------------

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
            patch("congress_videos.speaker_turn_prepare_dag._trigger_thumbnail_for_turn"),
            patch("congress_videos.speaker_turn_prepare_dag._write_turn_sidecars"),
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
