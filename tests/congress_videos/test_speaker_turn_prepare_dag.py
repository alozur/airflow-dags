"""Tests for speaker_turn_prepare DAG (issue #146).

TDD cycle: RED tests written first; GREEN implementations follow.
"""
from __future__ import annotations

from pathlib import Path
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
        import inspect
        import ast

        dag_source = Path(
            "/home/alozur/src/github.com/alozur/airflow-dags-issue-146"
            "/congress_videos/speaker_turn_prepare_dag.py"
        )
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
