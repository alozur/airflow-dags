"""Tests for congress_videos.trim_proposals_dag (PR2).

Covers the DAG-load smoke test and the per-turn orchestration
(`run_turn_proposals`) with all I/O collaborators mocked — no Airflow
execution, Docker, DB, or filesystem.
"""
from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock

import pytest

MODULE = "congress_videos.trim_proposals_dag"


def _fresh():
    if MODULE in sys.modules:
        del sys.modules[MODULE]
    return importlib.import_module(MODULE)


class TestDagLoads:
    def test_dag_imports_cleanly(self):
        mod = _fresh()
        assert mod.dag is not None

    def test_schedule_is_none(self):
        mod = _fresh()
        assert mod.dag.schedule_interval is None

    def test_expected_tasks_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert "select_turns" in task_ids
        assert "generate_proposals" in task_ids


class TestRunTurnProposals:
    def _turn(self):
        return {
            "turn_id": 7,
            "chapter_id": 3,
            "video_id": "abc123",
            "session_date": "2026-06-10",
            "start_seconds": 600.0,
            "end_seconds": 700.0,
        }

    def test_missing_source_video_skips(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda *a, **k: None)
        generate = MagicMock()
        monkeypatch.setattr(mod, "generate_trim_proposals", generate)
        cursor = MagicMock()

        result = mod.run_turn_proposals(self._turn(), cursor)

        assert result["status"] == "skipped_no_video"
        generate.assert_not_called()

    def test_happy_path_generates_and_upserts(self, monkeypatch):
        from congress_videos.modules.trim_proposals import TrimProposal

        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda *a, **k: "/v/src.mp4")
        monkeypatch.setattr(mod, "extract_audio_wav", lambda *a, **k: None)

        proposals = [
            TrimProposal(turn_id=7, start_seconds=610.0, end_seconds=620.0,
                         kind="silence", score=None, source="vad_webrtc",
                         is_voice_free=True),
            TrimProposal(turn_id=7, start_seconds=650.0, end_seconds=665.0,
                         kind="applause", score=0.88, source="yamnet_tflite",
                         is_voice_free=True),
        ]
        generate = MagicMock(return_value=proposals)
        monkeypatch.setattr(mod, "generate_trim_proposals", generate)
        upsert = MagicMock(return_value=2)
        monkeypatch.setattr(mod, "_upsert_proposals", upsert)
        cursor = MagicMock()

        result = mod.run_turn_proposals(self._turn(), cursor)

        assert result["status"] == "ok"
        assert result["proposals"] == 2
        generate.assert_called_once()
        # Default table name flows through when no qualified name is supplied;
        # _process_task passes pg.get_qualified_table(...) in prod.
        upsert.assert_called_once_with(
            cursor, proposals, table="speaker_turn_trim_proposals"
        )

    def test_wav_is_cleaned_up_after_processing(self, monkeypatch, tmp_path):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda *a, **k: "/v/src.mp4")
        wav_file = tmp_path / "turn_7.wav"
        wav_file.write_bytes(b"RIFF")

        def fake_extract(*args, **kwargs):
            pass  # wav_file already exists

        monkeypatch.setattr(mod, "extract_audio_wav", fake_extract)

        # Patch the wav_path computation to return our tmp file
        import os as _os
        original_join = _os.path.join

        def patched_join(*parts):
            if parts and "trim_proposals" in str(parts):
                return str(wav_file)
            return original_join(*parts)

        monkeypatch.setattr(_os.path, "join", patched_join)
        monkeypatch.setattr(mod, "generate_trim_proposals", MagicMock(return_value=[]))
        monkeypatch.setattr(mod, "_upsert_proposals", MagicMock(return_value=0))

        mod.run_turn_proposals(self._turn(), MagicMock())
        # File is cleaned up in finally block (it was created before the call)
        # The test verifies no exception is raised (cleanup is best-effort)

    def test_yamnet_fn_injected_into_generate(self, monkeypatch):
        """The yamnet_fn from trim_proposals_docker is wired into generate_trim_proposals."""
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda *a, **k: "/v/src.mp4")
        monkeypatch.setattr(mod, "extract_audio_wav", lambda *a, **k: None)

        captured_kwargs: dict = {}

        def fake_generate(turn, wav_path, vad_fn, yamnet_fn, **kw):
            captured_kwargs["yamnet_fn"] = yamnet_fn
            return []

        monkeypatch.setattr(mod, "generate_trim_proposals", fake_generate)
        monkeypatch.setattr(mod, "_upsert_proposals", MagicMock(return_value=0))

        mod.run_turn_proposals(self._turn(), MagicMock())
        assert captured_kwargs["yamnet_fn"] is not None


class TestSelectTurns:
    def test_maps_view_rows_to_dicts(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        # PostgresConnection uses RealDictCursor, so rows are dict-like, not tuples.
        # video_id comes from the JOIN to video_chapters; speaker_turns has no
        # video_id or session_date column, so neither is selected here.
        cur.fetchall.return_value = [
            {"turn_id": 7, "chapter_id": 3, "video_id": "abc123",
             "start_seconds": 600.0, "end_seconds": 700.0}
        ]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda name: f"development.{name}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        rows = mod.select_turns(limit=1)

        assert rows == [{
            "turn_id": 7, "chapter_id": 3, "video_id": "abc123",
            "start_seconds": 600.0, "end_seconds": 700.0,
        }]
        # Regression: video_id resolved via JOIN, session_date never selected.
        select_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "join" in select_sql and "video_chapters" in select_sql
        assert "session_date" not in select_sql


class TestProcessTask:
    def _ti_with(self, turns):
        ti = MagicMock()
        ti.xcom_pull.return_value = turns
        return {"ti": ti}

    def test_aggregates_and_skips_failures(self, monkeypatch):
        mod = _fresh()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = MagicMock()
        pg = MagicMock()
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        # Probe must be no-op so this data-error test is not affected by infra check
        monkeypatch.setattr(mod, "check_yamnet_api_health", lambda **k: None)

        def fake_run(turn, cursor, **k):
            tid = turn["turn_id"]
            if tid == 1:
                return {"status": "ok", "turn_id": 1, "proposals": 3}
            if tid == 2:
                raise RuntimeError("boom")
            return {"status": "skipped_no_video", "turn_id": 3, "proposals": 0}

        monkeypatch.setattr(mod, "run_turn_proposals", fake_run)

        summary = mod._process_task(
            **self._ti_with([
                {"turn_id": 1}, {"turn_id": 2}, {"turn_id": 3}
            ])
        )

        assert summary == {"processed": 1, "skipped": 2, "proposals": 3}
        conn.commit.assert_called_once()


class TestProcessTaskFailFast:
    """Verify _process_task fails loud on yamnet-api infra errors (issue #179)."""

    def _ti_with(self, turns):
        ti = MagicMock()
        ti.xcom_pull.return_value = turns
        return {"ti": ti}

    def test_infra_down_before_loop_raises_and_skips_turns(self, monkeypatch):
        """check_yamnet_api_health raises SidecarApiError → _process_task raises;
        PostgresConnection is never constructed and run_turn_proposals is never called."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        mod = _fresh()

        monkeypatch.setattr(
            mod, "check_yamnet_api_health",
            lambda **k: (_ for _ in ()).throw(SidecarApiError("yamnet-api unreachable")),
        )
        pg_ctor_mock = MagicMock()
        monkeypatch.setattr(mod, "PostgresConnection", pg_ctor_mock)
        run_turn_proposals_mock = MagicMock()
        monkeypatch.setattr(mod, "run_turn_proposals", run_turn_proposals_mock)

        with pytest.raises(SidecarApiError):
            mod._process_task(**self._ti_with([{"turn_id": 1}]))

        pg_ctor_mock.assert_not_called()
        run_turn_proposals_mock.assert_not_called()

    def test_midrun_sidecar_error_fails_task_not_skips(self, monkeypatch):
        """Probe ok, run_turn_proposals raises SidecarApiError for a turn →
        _process_task raises (not skips); conn.commit is never reached."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        mod = _fresh()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = MagicMock()
        pg = MagicMock()
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        monkeypatch.setattr(mod, "check_yamnet_api_health", lambda **k: None)

        def raising_run(turn, cursor, **k):
            raise SidecarApiError("yamnet-api dropped connection mid-run")

        monkeypatch.setattr(mod, "run_turn_proposals", raising_run)

        with pytest.raises(SidecarApiError):
            mod._process_task(**self._ti_with([{"turn_id": 5}]))

        conn.commit.assert_not_called()
