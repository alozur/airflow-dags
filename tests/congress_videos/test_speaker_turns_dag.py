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

    def test_schedule_is_single_daily_1400_utc(self):
        """Detect DAG must run once daily at 14:00 UTC (issue #187).

        14:00-20:00 UTC is the NAS quiet window: qBittorrent reads ~100x less
        disk than during 00:00-08:00 UTC, where the previous cron sat.
        """
        mod = _fresh()
        assert mod.dag.schedule_interval == "0 14 * * *"

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


class TestCronBatchSize:
    """Cron-triggered runs must default to a single chapter (issue #193)."""

    def test_default_limit_is_one(self):
        mod = _fresh()
        assert mod.DEFAULT_LIMIT == 1

    def test_select_task_empty_conf_calls_select_chapters_with_limit_one(self, monkeypatch):
        """No --conf override (scheduled cron run) → select_chapters(limit=1)."""
        mod = _fresh()
        select_mock = MagicMock(return_value=[])
        monkeypatch.setattr(mod, "select_chapters", select_mock)
        dag_run = MagicMock()
        dag_run.conf = {}
        ti = MagicMock()

        mod._select_task(dag_run=dag_run, ti=ti)

        select_mock.assert_called_once_with(limit=1, chapter_ids=None)


class TestRunChapterTurns:
    """run_chapter_turns holds NO database connection (issue #200) — it only
    detects turns and returns them; persistence is the caller's job via
    _persist_chapter_turns.
    """

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

        result = mod.run_chapter_turns(self._chapter())

        assert result["status"] == "skipped_no_video"
        assert result["turns"] == []
        detect.assert_not_called()

    def test_happy_path_returns_detected_turns(self, monkeypatch):
        """Detection returns the Turn list directly — no upsert, no cursor."""
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

        result = mod.run_chapter_turns(self._chapter())

        assert result["status"] == "ok"
        assert result["turns"] == turns
        detect.assert_called_once()
        # Detection never persists — no DB write happens during turn detection.
        upsert.assert_not_called()

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

        result = mod.run_chapter_turns(self._chapter())

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


class TestSelectChaptersProgressFilter:
    """Verify the cron-branch SQL filter added in issue #166."""

    def _make_pg_mock(self, monkeypatch, mod, cur):
        """Build a pg mock with side_effect keyed by table name and wire it."""
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()

        def _qualified(table_name):
            return f"development.{table_name}"

        pg.get_qualified_table.side_effect = _qualified
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        return pg, conn

    def test_cron_branch_sql_excludes_detected_chapters(self, monkeypatch):
        """Cron-branch SQL must contain TURNS_DETECTED_AT IS NULL and NOT EXISTS + SPEAKER_TURNS."""
        mod = _fresh()
        cur = MagicMock()
        cur.fetchall.return_value = []
        self._make_pg_mock(monkeypatch, mod, cur)

        mod.select_chapters(limit=5)

        executed_sql = cur.execute.call_args[0][0].upper()
        assert "TURNS_DETECTED_AT IS NULL" in executed_sql
        assert "NOT EXISTS" in executed_sql
        assert "SPEAKER_TURNS" in executed_sql

    def test_cron_branch_qualifies_video_chapters_table(self, monkeypatch):
        """pg.get_qualified_table must be called with 'video_chapters' and 'speaker_turns'."""
        mod = _fresh()
        cur = MagicMock()
        cur.fetchall.return_value = []
        pg, _ = self._make_pg_mock(monkeypatch, mod, cur)

        mod.select_chapters(limit=5)

        call_args = [call.args[0] for call in pg.get_qualified_table.call_args_list]
        assert "video_chapters" in call_args
        assert "speaker_turns" in call_args

    def test_chapter_ids_branch_skips_progress_filter(self, monkeypatch):
        """Explicit chapter_ids bypass must NOT include TURNS_DETECTED_AT in SQL."""
        mod = _fresh()
        cur = MagicMock()
        cur.fetchall.return_value = [
            {"chapter_id": 263, "video_id": "x", "session_date": "2026-06-10",
             "start_time": "00:00:00,000", "end_time": "00:10:00,000"},
        ]
        self._make_pg_mock(monkeypatch, mod, cur)

        mod.select_chapters(chapter_ids=[263])

        executed_sql = cur.execute.call_args[0][0].upper()
        assert "TURNS_DETECTED_AT" not in executed_sql


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
        pg.get_qualified_table.side_effect = lambda t: f"development.{t}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        # Probe must be no-op so this data-error test is not affected by infra check
        monkeypatch.setattr(mod, "check_diarize_api_health", lambda **k: None)
        monkeypatch.setattr(mod, "_upsert_turns", MagicMock())

        def fake_run(chapter, **k):
            cid = chapter["chapter_id"]
            if cid == 1:
                return {"status": "ok", "chapter_id": 1, "turns": [object(), object(), object()]}
            if cid == 2:
                raise RuntimeError("boom")
            return {"status": "skipped_no_video", "chapter_id": 3, "turns": []}

        monkeypatch.setattr(mod, "run_chapter_turns", fake_run)

        summary = mod._process_task(
            **self._ti_with([{"chapter_id": 1}, {"chapter_id": 2}, {"chapter_id": 3}])
        )

        assert summary == {"processed": 1, "skipped": 2, "turns": 3}
        conn.commit.assert_called_once()


class TestProcessTaskConnectionScope:
    """Verify #200: no DB connection is open while a chapter is being detected."""

    def _ti_with(self, chapters):
        ti = MagicMock()
        ti.xcom_pull.return_value = chapters
        return {"ti": ti}

    def test_no_connection_open_during_detection(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda t: f"development.{t}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        monkeypatch.setattr(mod, "check_diarize_api_health", lambda **k: None)
        monkeypatch.setattr(mod, "_upsert_turns", MagicMock())
        captured = {}

        def fake_run(chapter, **k):
            captured["get_connection_call_count"] = pg.get_connection.call_count
            return {"status": "ok", "chapter_id": chapter["chapter_id"], "turns": []}

        monkeypatch.setattr(mod, "run_chapter_turns", fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 1}]))

        assert captured["get_connection_call_count"] == 0


class TestProcessTaskMarkDetected:
    """Verify the turns_detected_at UPDATE written for issue #166."""

    def _ti_with(self, chapters):
        ti = MagicMock()
        ti.xcom_pull.return_value = chapters
        return {"ti": ti}

    def _make_process_mock(self, monkeypatch, mod, fake_run):
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda t: f"development.{t}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        monkeypatch.setattr(mod, "run_chapter_turns", fake_run)
        # Probe must be no-op so existing data-error tests are not affected
        monkeypatch.setattr(mod, "check_diarize_api_health", lambda **k: None)
        # _upsert_turns is exercised on its own in TestPersistChapterTurns;
        # here we only care about the surrounding orchestration.
        monkeypatch.setattr(mod, "_upsert_turns", MagicMock())
        return cur, conn, pg

    def test_ok_chapter_triggers_update_turns_detected_at(self, monkeypatch):
        """status=='ok' must trigger UPDATE turns_detected_at WHERE chapter_id."""
        mod = _fresh()

        def fake_run(chapter, **k):
            return {"status": "ok", "chapter_id": chapter["chapter_id"], "turns": [object(), object()]}

        cur, _, _ = self._make_process_mock(monkeypatch, mod, fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 1}]))

        executed_sqls = [call.args[0].upper() for call in cur.execute.call_args_list]
        update_calls = [s for s in executed_sqls if "TURNS_DETECTED_AT" in s]
        assert len(update_calls) == 1, f"Expected exactly 1 UPDATE with TURNS_DETECTED_AT, got: {update_calls}"
        assert "WHERE CHAPTER_ID" in update_calls[0]

    def test_skipped_no_video_does_not_update(self, monkeypatch):
        """status=='skipped_no_video' must NOT trigger UPDATE turns_detected_at."""
        mod = _fresh()

        def fake_run(chapter, **k):
            return {"status": "skipped_no_video", "chapter_id": chapter["chapter_id"], "turns": []}

        cur, _, _ = self._make_process_mock(monkeypatch, mod, fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 3}]))

        executed_sqls = [call.args[0].upper() for call in cur.execute.call_args_list]
        update_calls = [s for s in executed_sqls if "TURNS_DETECTED_AT" in s]
        assert update_calls == [], f"Expected no UPDATE with TURNS_DETECTED_AT, got: {update_calls}"

    def test_exception_chapter_does_not_update(self, monkeypatch):
        """Exception-caught chapter must NOT trigger UPDATE turns_detected_at."""
        mod = _fresh()

        def fake_run(chapter, **k):
            raise RuntimeError("diarize failed")

        cur, _, _ = self._make_process_mock(monkeypatch, mod, fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 2}]))

        executed_sqls = [call.args[0].upper() for call in cur.execute.call_args_list]
        update_calls = [s for s in executed_sqls if "TURNS_DETECTED_AT" in s]
        assert update_calls == [], f"Expected no UPDATE with TURNS_DETECTED_AT, got: {update_calls}"

    def test_each_ok_chapter_commits_independently(self, monkeypatch):
        """2 ok chapters commit individually; the 3rd raising SidecarApiError aborts the task."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        mod = _fresh()

        def fake_run(chapter, **k):
            cid = chapter["chapter_id"]
            if cid in (1, 2):
                return {"status": "ok", "chapter_id": cid, "turns": [object()]}
            raise SidecarApiError("diarize-api dropped connection mid-run")

        _, conn, _ = self._make_process_mock(monkeypatch, mod, fake_run)

        with pytest.raises(SidecarApiError):
            mod._process_task(
                **self._ti_with([{"chapter_id": 1}, {"chapter_id": 2}, {"chapter_id": 3}])
            )

        assert conn.commit.call_count == 2, (
            f"Expected 2 independent commits (one per ok chapter), got {conn.commit.call_count}"
        )

    def test_skipped_chapter_does_not_open_connection(self, monkeypatch):
        """Non-'ok' status (e.g. skipped_no_video) must never open a DB connection (issue #200)."""
        mod = _fresh()

        def fake_run(chapter, **k):
            return {"status": "skipped_no_video", "chapter_id": chapter["chapter_id"], "turns": []}

        _, conn, pg = self._make_process_mock(monkeypatch, mod, fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 3}]))

        pg.get_connection.assert_not_called()
        conn.commit.assert_not_called()

    def test_data_error_chapter_does_not_open_connection(self, monkeypatch):
        """A generic exception (data error) must never open a DB connection (issue #200)."""
        mod = _fresh()

        def fake_run(chapter, **k):
            raise RuntimeError("diarize failed")

        _, conn, pg = self._make_process_mock(monkeypatch, mod, fake_run)

        mod._process_task(**self._ti_with([{"chapter_id": 2}]))

        pg.get_connection.assert_not_called()
        conn.commit.assert_not_called()


class TestPersistChapterTurns:
    """Direct tests of _persist_chapter_turns — the sole owner of the short-lived
    persistence transaction (issue #200): upsert → mark turns_detected_at → commit.
    """

    def test_persists_in_order_upsert_then_update_then_commit(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        conn = MagicMock()
        manager = MagicMock()
        manager.attach_mock(cur, "cur")
        manager.attach_mock(conn, "conn")
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_connection.return_value.__enter__.return_value = conn

        def fake_upsert(cursor, chapter_id, turns, table):
            cursor.execute("INSERT INTO speaker_turns (...) VALUES (...)")

        monkeypatch.setattr(mod, "_upsert_turns", fake_upsert)

        mod._persist_chapter_turns(
            pg, 7, [object()],
            turns_table="development.speaker_turns",
            vc_table="development.video_chapters",
        )

        call_order = [
            (name, str(args[0]).upper() if args else "")
            for name, args, _kwargs in manager.mock_calls
            if name in ("cur.execute", "conn.commit")
        ]
        insert_idx = next(i for i, (_n, sql) in enumerate(call_order) if "INSERT" in sql)
        update_idx = next(i for i, (_n, sql) in enumerate(call_order) if "TURNS_DETECTED_AT" in sql)
        commit_idx = next(i for i, (n, _sql) in enumerate(call_order) if n == "conn.commit")

        assert insert_idx < update_idx < commit_idx, (
            f"Expected upsert → UPDATE → commit ordering, got: {call_order}"
        )

    def test_update_targets_the_given_chapter_id(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "_upsert_turns", MagicMock())

        mod._persist_chapter_turns(
            pg, 42, [],
            turns_table="development.speaker_turns",
            vc_table="development.video_chapters",
        )

        update_call = next(
            c for c in cur.execute.call_args_list
            if "TURNS_DETECTED_AT" in str(c.args[0]).upper()
        )
        assert update_call.args[1] == (42,)
        conn.commit.assert_called_once()


class TestProcessTaskPersistenceFailure:
    """A persistence-layer failure is a per-chapter skip, not a task failure (#200)."""

    def _ti_with(self, chapters):
        ti = MagicMock()
        ti.xcom_pull.return_value = chapters
        return {"ti": ti}

    def test_persistence_error_counted_skipped_not_task_failure(self, monkeypatch):
        mod = _fresh()
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda t: f"development.{t}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        monkeypatch.setattr(mod, "check_diarize_api_health", lambda **k: None)
        monkeypatch.setattr(
            mod, "_upsert_turns",
            MagicMock(side_effect=RuntimeError("db constraint violation")),
        )

        def fake_run(chapter, **k):
            return {"status": "ok", "chapter_id": chapter["chapter_id"], "turns": [object()]}

        monkeypatch.setattr(mod, "run_chapter_turns", fake_run)

        summary = mod._process_task(**self._ti_with([{"chapter_id": 9}]))

        assert summary == {"processed": 0, "skipped": 1, "turns": 0}
        update_calls = [
            c for c in cur.execute.call_args_list
            if "TURNS_DETECTED_AT" in str(c.args[0]).upper()
        ]
        assert update_calls == [], "turns_detected_at must not be marked on persistence failure"


class TestProcessTaskFailFast:
    """Verify _process_task fails loud on diarize-api infra errors (issue #156)."""

    def _ti_with(self, chapters):
        ti = MagicMock()
        ti.xcom_pull.return_value = chapters
        return {"ti": ti}

    def _make_pg_mock(self, monkeypatch, mod):
        cur = MagicMock()
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda t: f"development.{t}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        return cur, conn

    def test_infra_down_before_loop_raises_and_skips_chapters(self, monkeypatch):
        """check_diarize_api_health raises SidecarApiError → _process_task raises; run_chapter_turns never called."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        mod = _fresh()
        self._make_pg_mock(monkeypatch, mod)

        monkeypatch.setattr(
            mod, "check_diarize_api_health",
            lambda **k: (_ for _ in ()).throw(SidecarApiError("diarize-api unreachable")),
        )
        run_chapter_turns_mock = MagicMock()
        monkeypatch.setattr(mod, "run_chapter_turns", run_chapter_turns_mock)

        with pytest.raises(SidecarApiError):
            mod._process_task(**self._ti_with([{"chapter_id": 1}]))

        run_chapter_turns_mock.assert_not_called()

    def test_midrun_sidecar_error_fails_task_not_skips(self, monkeypatch):
        """Probe ok, run_chapter_turns raises SidecarApiError for chapter → _process_task raises (not skips)."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        mod = _fresh()
        self._make_pg_mock(monkeypatch, mod)

        monkeypatch.setattr(mod, "check_diarize_api_health", lambda **k: None)

        def raising_run(chapter, **k):
            raise SidecarApiError("diarize-api dropped connection mid-run")

        monkeypatch.setattr(mod, "run_chapter_turns", raising_run)

        with pytest.raises(SidecarApiError):
            mod._process_task(**self._ti_with([{"chapter_id": 5}]))
