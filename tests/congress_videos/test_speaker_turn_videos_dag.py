"""Tests for congress_videos.speaker_turn_videos_dag (PR2).

All I/O collaborators are mocked — no real Airflow execution, no DB, no ffmpeg.

Test organisation:
  TestDagLoads           — DAG import smoke test, schedule=None, task IDs
  TestSelectTurns        — idempotency skip for already-materialized turns
  TestMaterializeTurns   — missing source skip, INSERT on success, no INSERT on failure
  TestApprovedOnlyFilter — only approved+voice-free trims drive excision
"""

from __future__ import annotations

import importlib
import sys
from datetime import date
from unittest.mock import MagicMock, call

import pytest

MODULE = "congress_videos.speaker_turn_videos_dag"


def _fresh():
    """Reload the DAG module fresh each time to avoid caching side effects."""
    if MODULE in sys.modules:
        del sys.modules[MODULE]
    return importlib.import_module(MODULE)


# ---------------------------------------------------------------------------
# 2.8 DAG load test
# ---------------------------------------------------------------------------

class TestDagLoads:
    def test_dag_imports_cleanly(self):
        mod = _fresh()
        assert mod.dag is not None

    def test_schedule_is_none(self):
        mod = _fresh()
        assert mod.dag.schedule_interval is None

    def test_expected_task_ids_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert "select_turns" in task_ids
        assert "materialize_turns" in task_ids
        assert "collect_results" in task_ids

    def test_max_active_tasks_is_one(self):
        mod = _fresh()
        assert mod.dag.max_active_tasks == 1


# ---------------------------------------------------------------------------
# 2.9 select_turns skips already-materialized turns
# ---------------------------------------------------------------------------

class TestSelectTurns:
    """Turns already in speaker_turn_videos must be excluded from the XCom output."""

    def _make_pg_mock(self, monkeypatch, mod, turns_rows, already_materialized_ids):
        """Wire a mock PostgresConnection that returns turns_rows and already_materialized_ids."""
        cur = MagicMock()
        # Two queries: one for turns, one for already-materialized ids
        cur.description = [
            ("turn_id",), ("chapter_id",), ("video_id",), ("session_date",),
            ("start_seconds",), ("end_seconds",),
        ]
        # First fetchall returns speaker_turns rows
        # Second fetchall returns [(turn_id,)] for already-materialized
        already_rows = [(tid,) for tid in already_materialized_ids]
        cur.fetchall.side_effect = [turns_rows, already_rows]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda name: f"test.{name}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        return cur

    def test_already_materialized_turns_excluded(self, monkeypatch):
        mod = _fresh()
        turns_rows = [
            (1, 10, "vid1", "2026-01-01", 0.0, 100.0),
            (2, 10, "vid1", "2026-01-01", 100.0, 200.0),
        ]
        self._make_pg_mock(monkeypatch, mod, turns_rows, already_materialized_ids=[1])

        ti = MagicMock()
        pushed = {}

        def xcom_push(key, value):
            pushed[key] = value

        ti.xcom_push.side_effect = xcom_push
        dag_run = MagicMock()
        dag_run.conf = {}

        mod._select_task(ti=ti, dag_run=dag_run)

        turns_out = pushed["turns"]
        returned_ids = [t["turn_id"] for t in turns_out]
        assert 1 not in returned_ids, "Already-materialized turn_id 1 must be excluded"
        assert 2 in returned_ids, "Non-materialized turn_id 2 must be included"

    def test_no_turns_when_all_already_materialized(self, monkeypatch):
        mod = _fresh()
        turns_rows = [
            (5, 10, "vid1", "2026-01-01", 0.0, 100.0),
        ]
        self._make_pg_mock(monkeypatch, mod, turns_rows, already_materialized_ids=[5])

        ti = MagicMock()
        pushed = {}

        def xcom_push(key, value):
            pushed[key] = value

        ti.xcom_push.side_effect = xcom_push
        dag_run = MagicMock()
        dag_run.conf = {}

        mod._select_task(ti=ti, dag_run=dag_run)

        assert pushed["turns"] == []


# ---------------------------------------------------------------------------
# 2.10 materialize_turns skips when source video is not found
# ---------------------------------------------------------------------------

class TestMaterializeTurns:
    def _turn(self, turn_id=7, chapter_id=3, video_id="vid1",
              session_date="2026-01-01", start=600.0, end=700.0):
        return {
            "turn_id": turn_id,
            "chapter_id": chapter_id,
            "video_id": video_id,
            "session_date": session_date,
            "start_seconds": start,
            "end_seconds": end,
        }

    def test_missing_source_video_skips_without_ffmpeg(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda date, vid: None)
        execute_plan = MagicMock()
        monkeypatch.setattr(mod, "execute_plan", execute_plan)

        ti = MagicMock()
        ti.xcom_pull.return_value = [self._turn()]

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        result = mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        execute_plan.assert_not_called()
        assert result["skipped"] >= 1

    def test_inserts_row_on_success(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda date, vid: "/data/src.mp4")

        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7

        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", lambda *a, **kw: None)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        monkeypatch.setattr(mod, "get_turn_video_path", lambda date, vid, tid: f"/out/{tid}.mp4")
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        # approved trims query returns empty (no trims)
        cur.fetchall.return_value = []
        cur.description = [("turn_id",), ("start_seconds",), ("end_seconds",),
                           ("is_approved",), ("is_voice_free",)]
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        ti = MagicMock()
        ti.xcom_pull.return_value = [self._turn()]

        result = mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        # cur.execute should have been called with an INSERT
        all_calls = [str(c) for c in cur.execute.call_args_list]
        insert_calls = [c for c in all_calls if "INSERT" in c.upper()]
        assert len(insert_calls) >= 1, f"Expected INSERT call; got: {all_calls}"
        assert result["materialized"] >= 1

    def test_no_insert_on_execute_plan_failure(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda date, vid: "/data/src.mp4")

        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7

        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", MagicMock(side_effect=RuntimeError("ffmpeg boom")))
        monkeypatch.setattr(mod, "get_turn_video_path", lambda date, vid, tid: f"/out/{tid}.mp4")
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        cur.description = [("turn_id",), ("start_seconds",), ("end_seconds",),
                           ("is_approved",), ("is_voice_free",)]
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        ti = MagicMock()
        ti.xcom_pull.return_value = [self._turn()]

        result = mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        # No INSERT should happen on failure
        all_calls = [str(c) for c in cur.execute.call_args_list]
        insert_calls = [c for c in all_calls if "INSERT" in c.upper()]
        assert len(insert_calls) == 0, f"INSERT must not be called on ffmpeg failure; got: {insert_calls}"
        assert result["skipped"] >= 1


# ---------------------------------------------------------------------------
# 2.12 collect_results aggregates the summary
# ---------------------------------------------------------------------------

class TestCollectResults:
    def test_aggregates_xcom_summary(self, monkeypatch):
        mod = _fresh()
        ti = MagicMock()
        ti.xcom_pull.return_value = {"materialized": 3, "skipped": 1}

        result = mod._collect_task(ti=ti, dag_run=MagicMock(conf={}))

        assert result["materialized"] == 3
        assert result["skipped"] == 1
