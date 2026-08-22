"""Tests for congress_videos.speaker_turns_dag (PR2).

Covers the DAG-load smoke test and the per-chapter orchestration
(`run_chapter_turns`) with all I/O collaborators mocked — no Airflow
execution, Docker, DB, or filesystem.
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import pytest

MODULE = "congress_videos.speaker_turns_dag"


def _fresh():
    if MODULE in sys.modules:
        del sys.modules[MODULE]
    return importlib.import_module(MODULE)


class TestDagLoads:
    def test_dag_imports_cleanly(self):
        mod = _fresh()
        assert mod.dag is not None

    def test_schedule_is_0_1_5_twice_nightly(self):
        """Detect DAG must run twice nightly at 01:00 and 05:00 UTC."""
        mod = _fresh()
        assert mod.dag.schedule_interval == "0 1,5 * * *"

    def test_max_active_runs_is_1(self):
        """max_active_runs=1 queues rather than drops concurrent cron runs."""
        mod = _fresh()
        assert mod.dag.max_active_runs == 1

    def test_expected_tasks_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert "select_chapters" in task_ids
        assert "process_chapters" in task_ids

    def test_trigger_materialize_task_exists(self):
        """Terminal trigger_materialize task must be in DAG task ids."""
        mod = _fresh()
        assert "trigger_materialize" in {t.task_id for t in mod.dag.tasks}

    def test_trigger_materialize_downstream_of_process_chapters(self):
        """trigger_materialize must be directly downstream of process_chapters."""
        mod = _fresh()
        tasks_by_id = {t.task_id: t for t in mod.dag.tasks}
        process_task = tasks_by_id["process_chapters"]
        downstream_ids = {t.task_id for t in process_task.downstream_list}
        assert "trigger_materialize" in downstream_ids

    def test_trigger_materialize_all_done_rule(self):
        """trigger_materialize must fire even on partial upstream failure."""
        mod = _fresh()
        tasks_by_id = {t.task_id: t for t in mod.dag.tasks}
        t = tasks_by_id["trigger_materialize"]
        assert str(t.trigger_rule) == "all_done"

    def test_trigger_materialize_callable_fires_with_imported_dag_id(self, mocker):
        """trigger callable must call trigger_dag_api with speaker_turn_videos_dag.DAG_ID."""
        import importlib
        import sys
        # ensure a fresh module load so the import is live
        for m in list(sys.modules.keys()):
            if "speaker_turns_dag" in m or "speaker_turn_videos_dag" in m:
                del sys.modules[m]
        mod = importlib.import_module("congress_videos.speaker_turns_dag")
        import congress_videos.speaker_turn_videos_dag as stv_dag

        mock_trigger = mocker.patch(
            "congress_videos.speaker_turns_dag.trigger_dag_api"
        )
        mod._trigger_materialize()

        mock_trigger.assert_called_once()
        call_kwargs = mock_trigger.call_args
        dag_id_arg = call_kwargs[1].get("dag_id") or call_kwargs[0][0]
        assert dag_id_arg == stv_dag.DAG_ID, (
            f"Expected dag_id={stv_dag.DAG_ID!r}, got {dag_id_arg!r}"
        )
        conf_arg = call_kwargs[1].get("conf")
        assert conf_arg == {}, f"Expected conf={{}}, got {conf_arg!r}"


class TestRunChapterTurns:
    def _chapter(self):
        return {
            "chapter_id": 7,
            "video_id": "abc123",
            "session_date": "2026-06-10",
            "start_time": "00:10:00,000",
            "end_time": "00:40:00,000",
        }

    def test_missing_source_video_skips(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda *a, **k: None)
        detect = MagicMock()
        monkeypatch.setattr(mod, "detect_turns", detect)
        cursor = MagicMock()

        result = mod.run_chapter_turns(self._chapter(), cursor)

        assert result["status"] == "skipped_no_video"
        detect.assert_not_called()

    def test_happy_path_detects_and_upserts(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda *a, **k: "/v/src.mp4")
        monkeypatch.setattr(mod, "extract_audio_wav", lambda *a, **k: "/tmp/c.wav")
        monkeypatch.setattr(mod, "find_srt_for_chapter", lambda *a, **k: "/srt/x.srt")
        monkeypatch.setattr(
            mod, "_parse_srt_blocks",
            lambda p: [{"start_secs": 600.0, "end_secs": 602.0, "text": "Tiene la palabra"}],
        )
        turns = [object(), object()]
        detect = MagicMock(return_value=turns)
        monkeypatch.setattr(mod, "detect_turns", detect)
        upsert = MagicMock()
        monkeypatch.setattr(mod, "_upsert_turns", upsert)
        cursor = MagicMock()

        result = mod.run_chapter_turns(self._chapter(), cursor)

        assert result["status"] == "ok"
        assert result["turns"] == 2
        detect.assert_called_once()
        # Default table name flows through when no qualified name is supplied;
        # _process_task passes pg.get_qualified_table("speaker_turns") in prod.
        upsert.assert_called_once_with(cursor, 7, turns, table="speaker_turns")

    def test_missing_srt_runs_acoustic_only(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video", lambda *a, **k: "/v/src.mp4")
        monkeypatch.setattr(mod, "extract_audio_wav", lambda *a, **k: "/tmp/c.wav")
        monkeypatch.setattr(mod, "find_srt_for_chapter", lambda *a, **k: None)
        parse = MagicMock()
        monkeypatch.setattr(mod, "_parse_srt_blocks", parse)
        captured = {}

        def fake_detect(chapter, srt_blocks, diarize_fn, name_resolver=None):
            captured["srt_blocks"] = srt_blocks
            return []

        monkeypatch.setattr(mod, "detect_turns", fake_detect)
        monkeypatch.setattr(mod, "_upsert_turns", MagicMock())

        result = mod.run_chapter_turns(self._chapter(), MagicMock())

        assert result["status"] == "ok"
        assert captured["srt_blocks"] == []  # acoustic-only
        parse.assert_not_called()


class TestSelectChapters:
    def test_maps_view_rows_to_dicts(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        cur.description = [("chapter_id",), ("video_id",), ("session_date",),
                           ("start_time",), ("end_time",)]
        # PostgresConnection uses RealDictCursor, so rows are dict-like, not tuples.
        cur.fetchall.return_value = [{"chapter_id": 7, "video_id": "abc",
                                      "session_date": "2026-06-10",
                                      "start_time": "00:10:00,000",
                                      "end_time": "00:40:00,000"}]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.return_value = "development.uploadable_chapters"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        rows = mod.select_chapters(limit=1)

        assert rows == [{"chapter_id": 7, "video_id": "abc",
                         "session_date": "2026-06-10",
                         "start_time": "00:10:00,000", "end_time": "00:40:00,000"}]


class TestProcessTask:
    def _ti_with(self, chapters):
        ti = MagicMock()
        ti.xcom_pull.return_value = chapters
        return {"ti": ti}

    def test_aggregates_and_skips_failures(self, monkeypatch):
        mod = _fresh()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = MagicMock()
        pg = MagicMock()
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        def fake_run(chapter, cursor, **k):
            cid = chapter["chapter_id"]
            if cid == 1:
                return {"status": "ok", "chapter_id": 1, "turns": 3}
            if cid == 2:
                raise RuntimeError("boom")
            return {"status": "skipped_no_video", "chapter_id": 3, "turns": 0}

        monkeypatch.setattr(mod, "run_chapter_turns", fake_run)

        summary = mod._process_task(
            **self._ti_with([{"chapter_id": 1}, {"chapter_id": 2}, {"chapter_id": 3}])
        )

        assert summary == {"processed": 1, "skipped": 2, "turns": 3}
        conn.commit.assert_called_once()
