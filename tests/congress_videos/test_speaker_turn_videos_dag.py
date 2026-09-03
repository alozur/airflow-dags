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
import json
import re
import sys
from unittest.mock import MagicMock

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

    def test_max_active_runs_is_1(self):
        """max_active_runs=1 queues chain-triggered runs instead of running them concurrently."""
        mod = _fresh()
        assert mod.dag.max_active_runs == 1

    def test_trigger_prepare_task_exists(self):
        """Terminal trigger_prepare task must exist in speaker_turn_videos DAG."""
        mod = _fresh()
        assert "trigger_prepare" in {t.task_id for t in mod.dag.tasks}

    def test_trigger_prepare_downstream_of_collect_results(self):
        """trigger_prepare must be directly downstream of collect_results."""
        mod = _fresh()
        tasks_by_id = {t.task_id: t for t in mod.dag.tasks}
        collect_task = tasks_by_id["collect_results"]
        downstream_ids = {t.task_id for t in collect_task.downstream_list}
        assert "trigger_prepare" in downstream_ids

    def test_trigger_prepare_all_done_rule(self):
        """trigger_prepare must fire even when upstream tasks partially fail."""
        mod = _fresh()
        tasks_by_id = {t.task_id: t for t in mod.dag.tasks}
        t = tasks_by_id["trigger_prepare"]
        assert str(t.trigger_rule) == "all_done"

    def test_trigger_prepare_callable_fires_with_imported_dag_id(self, mocker):
        """trigger callable must call trigger_dag_api with speaker_turn_prepare_dag.DAG_ID."""
        import importlib
        import sys

        for m in list(sys.modules.keys()):
            if "speaker_turn_videos_dag" in m or "speaker_turn_prepare_dag" in m:
                del sys.modules[m]
        mod = importlib.import_module("congress_videos.speaker_turn_videos_dag")
        import congress_videos.speaker_turn_prepare_dag as stp_dag

        mock_trigger = mocker.patch("congress_videos.speaker_turn_videos_dag.trigger_dag_api")
        mod._trigger_prepare()

        mock_trigger.assert_called_once()
        call_kwargs = mock_trigger.call_args
        dag_id_arg = call_kwargs[1].get("dag_id") or call_kwargs[0][0]
        assert dag_id_arg == stp_dag.DAG_ID, f"Expected dag_id={stp_dag.DAG_ID!r}, got {dag_id_arg!r}"
        conf_arg = call_kwargs[1].get("conf")
        assert conf_arg == {}, f"Expected conf={{}}, got {conf_arg!r}"


# ---------------------------------------------------------------------------
# 2.9 select_turns skips already-materialized turns
# ---------------------------------------------------------------------------


class TestSelectTurns:
    """Turns already in speaker_turn_videos must be excluded from the XCom output."""

    def _make_pg_mock(self, monkeypatch, mod, turns_rows, already_materialized_ids=None):
        """Wire a mock PostgresConnection that returns turns_rows.

        When ``already_materialized_ids`` is ``None`` (default branch — the
        idempotency filter now lives in SQL via ``NOT EXISTS``), only one
        query/fetch happens: ``cur.fetchall.return_value`` is set to
        ``turns_dicts``. When it is a list (scoped branches, which keep the
        existing post-hoc Python filter), the original two-fetch
        ``side_effect`` is used.
        """
        cur = MagicMock()
        # video_id comes from the JOIN to video_chapters; speaker_turns has no
        # video_id or session_date column.
        cur.description = [
            ("turn_id",),
            ("chapter_id",),
            ("video_id",),
            ("start_seconds",),
            ("end_seconds",),
            ("resolved_name",),
        ]
        # PostgresConnection uses RealDictCursor, so real rows are dict-like.
        # Convert the tuple fixtures into dict rows so the mock matches prod and
        # the old dict(zip(names,row)) bug would surface (keys as values).
        col_names = [d[0] for d in cur.description]
        turns_dicts = [dict(zip(col_names, r)) for r in turns_rows]
        if already_materialized_ids is None:
            cur.fetchall.return_value = turns_dicts
        else:
            already_rows = [{"turn_id": tid} for tid in already_materialized_ids]
            cur.fetchall.side_effect = [turns_dicts, already_rows]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda name: f"test.{name}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        return cur

    def test_already_materialized_turns_excluded(self, monkeypatch):
        """chapter_id-scoped branch keeps the post-hoc Python idempotency filter."""
        mod = _fresh()
        turns_rows = [
            (1, 10, "vid1", 0.0, 100.0, None),
            (2, 10, "vid1", 100.0, 200.0, None),
        ]
        self._make_pg_mock(monkeypatch, mod, turns_rows, already_materialized_ids=[1])

        ti = MagicMock()
        pushed = {}

        def xcom_push(key, value):
            pushed[key] = value

        ti.xcom_push.side_effect = xcom_push
        dag_run = MagicMock()
        dag_run.conf = {"chapter_id": 10}

        mod._select_task(ti=ti, dag_run=dag_run)

        turns_out = pushed["turns"]
        returned_ids = [t["turn_id"] for t in turns_out]
        assert 1 not in returned_ids, "Already-materialized turn_id 1 must be excluded"
        assert 2 in returned_ids, "Non-materialized turn_id 2 must be included"
        # RealDictCursor regression: values must be real data, not column names.
        assert turns_out[0]["video_id"] == "vid1"
        assert not any(k == v for t in turns_out for k, v in t.items()), (
            "row values must be real data, not their own column names"
        )

    def test_select_joins_video_chapters_and_omits_session_date(self, monkeypatch):
        """Regression: speaker_turns has no video_id/session_date columns.

        video_id must be resolved via a JOIN to video_chapters, and
        session_date must never be selected (it does not exist anywhere).
        A prod run failed with UndefinedColumn before this fix.
        """
        mod = _fresh()
        cur = self._make_pg_mock(
            monkeypatch,
            mod,
            turns_rows=[(1, 10, "vid1", 0.0, 100.0, None)],
            already_materialized_ids=[],
        )

        ti = MagicMock()
        ti.xcom_push.side_effect = lambda key, value: None
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"chapter_id": 10}))

        select_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "join" in select_sql and "video_chapters" in select_sql, (
            f"select must JOIN video_chapters to resolve video_id; got: {select_sql}"
        )
        assert "session_date" not in select_sql, (
            f"session_date is not a real column and must not be selected; got: {select_sql}"
        )

    def test_select_includes_resolved_name(self, monkeypatch):
        """_select_task SQL must include st.resolved_name so _materialize_task can classify turns."""
        mod = _fresh()
        cur = self._make_pg_mock(
            monkeypatch,
            mod,
            turns_rows=[(1, 10, "vid1", 0.0, 100.0, None)],
            already_materialized_ids=[],
        )

        ti = MagicMock()
        ti.xcom_push.side_effect = lambda key, value: None
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"chapter_id": 10}))

        select_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "resolved_name" in select_sql, f"_select_task must include st.resolved_name in SELECT; got: {select_sql}"

    def test_select_includes_speaker_label(self, monkeypatch):
        """_select_task SQL must include st.speaker_label so classify_turn_type
        can use real acoustic labels instead of freezing on unresolved names
        (issue #282)."""
        mod = _fresh()
        cur = self._make_pg_mock(
            monkeypatch,
            mod,
            turns_rows=[(1, 10, "vid1", 0.0, 100.0, None)],
            already_materialized_ids=[],
        )

        ti = MagicMock()
        ti.xcom_push.side_effect = lambda key, value: None
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"chapter_id": 10}))

        select_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "speaker_label" in select_sql, f"_select_task must include st.speaker_label in SELECT; got: {select_sql}"

    def test_no_turns_when_all_already_materialized(self, monkeypatch):
        """chapter_id-scoped branch: post-hoc filter drops every already-materialized turn."""
        mod = _fresh()
        turns_rows = [
            (5, 10, "vid1", 0.0, 100.0, None),
        ]
        self._make_pg_mock(monkeypatch, mod, turns_rows, already_materialized_ids=[5])

        ti = MagicMock()
        pushed = {}

        def xcom_push(key, value):
            pushed[key] = value

        ti.xcom_push.side_effect = xcom_push
        dag_run = MagicMock()
        dag_run.conf = {"chapter_id": 10}

        mod._select_task(ti=ti, dag_run=dag_run)

        assert pushed["turns"] == []

    def test_default_limit_constant_removed(self):
        """DEFAULT_LIMIT must not survive: automatic runs are chapter-aligned, not row-capped."""
        mod = _fresh()
        assert not hasattr(mod, "DEFAULT_LIMIT")

    def test_conf_limit_override_passed_to_sql(self, monkeypatch):
        """An explicitly present dag_run.conf['limit'] key must bind unchanged via %s."""
        mod = _fresh()
        cur = self._make_pg_mock(
            monkeypatch,
            mod,
            turns_rows=[(61, 10, "vid1", 0.0, 100.0, None)],
        )

        ti = MagicMock()
        ti.xcom_push.side_effect = lambda key, value: None
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"limit": 50}))

        assert cur.execute.call_count == 1, "explicit-limit branch must issue exactly one query"
        assert cur.execute.call_args_list[0].args[1] == (50,)

    def test_select_includes_is_procedural(self, monkeypatch):
        """issue #143: _select_task SQL must expose st.is_procedural so the
        planner can see (never filter) procedural member turns."""
        mod = _fresh()
        cur = self._make_pg_mock(
            monkeypatch,
            mod,
            turns_rows=[(1, 10, "vid1", 0.0, 100.0, None)],
            already_materialized_ids=[],
        )

        ti = MagicMock()
        ti.xcom_push.side_effect = lambda key, value: None
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"chapter_id": 10}))

        select_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "is_procedural" in select_sql, f"_select_task must include st.is_procedural in SELECT; got: {select_sql}"


def _row(turn_id, chapter_id=42, video_id="vid1", start=0.0, end=1.0, resolved_name=None):
    return {
        "turn_id": turn_id,
        "chapter_id": chapter_id,
        "video_id": video_id,
        "start_seconds": start,
        "end_seconds": end,
        "resolved_name": resolved_name,
    }


def _wire_cursor(
    monkeypatch, mod, *, fetchone=None, fetchall=None, fetchall_side_effect=None, execute_side_effect=None
):
    """Shared mock-cursor wiring for the new selection-precedence test classes below."""
    cur = MagicMock()
    cur.fetchone.return_value = fetchone
    if fetchall_side_effect is not None:
        cur.fetchall.side_effect = fetchall_side_effect
    else:
        cur.fetchall.return_value = fetchall if fetchall is not None else []
    if execute_side_effect is not None:
        cur.execute.side_effect = execute_side_effect
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    pg = MagicMock()
    pg.get_qualified_table.side_effect = lambda name: f"test.{name}"
    pg.get_connection.return_value.__enter__.return_value = conn
    monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
    return cur


# ---------------------------------------------------------------------------
# Chapter-aligned automatic selection (issue #231)
# ---------------------------------------------------------------------------


class TestAutomaticChapterSelection:
    """Empty conf (no chapter_id/video_id/limit key) selects the oldest
    pending chapter and every still-pending turn in it, uncapped.
    """

    def test_oldest_pending_chapter_returns_all_rows_beyond_former_limit(self, monkeypatch):
        mod = _fresh()
        final_rows = [_row(tid) for tid in range(100, 115)]  # 15 rows > former cap of 10
        cur = _wire_cursor(monkeypatch, mod, fetchone={"chapter_id": 42}, fetchall=final_rows)

        ti = MagicMock()
        pushed = {}
        ti.xcom_push.side_effect = lambda key, value: pushed.update({key: value})
        mod._select_task(ti=ti, dag_run=MagicMock(conf={}))

        returned_ids = [t["turn_id"] for t in pushed["turns"]]
        assert returned_ids == list(range(100, 115)), (
            f"expected all 15 pending turns from chapter 42 in ascending order; got {returned_ids}"
        )
        assert all(t["chapter_id"] == 42 for t in pushed["turns"]), "no other chapter's turns"
        assert cur.execute.call_count == 2, "choose chapter, then select its rows"
        sqls = [c.args[0].lower() for c in cur.execute.call_args_list]
        assert all("limit" not in sql for sql in sqls), "automatic branch must never cap rows"

    def test_chapter_choice_query_uses_min_over_pending_turns_only(self, monkeypatch):
        mod = _fresh()
        cur = _wire_cursor(
            monkeypatch,
            mod,
            fetchone={"chapter_id": 7},
            fetchall=[_row(61, chapter_id=7)],
        )

        mod._select_task(ti=MagicMock(), dag_run=MagicMock(conf={}))

        first_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "min(" in first_sql, "chapter choice must use MIN(turn_id) over pending rows"
        assert "not exists" in first_sql and "speaker_turn_videos" in first_sql, (
            "the MIN subquery must exclude materialized turns via NOT EXISTS"
        )
        second_sql = cur.execute.call_args_list[1].args[0].lower()
        assert "chapter_id = %s" in second_sql
        assert cur.execute.call_args_list[1].args[1] == (7,)

    def test_final_selection_reapplies_pending_filter_independently(self, monkeypatch):
        """A turn materialized between the two statements is excluded — proven by a
        final row count smaller than the chapter's conceptual pending set, with no
        client-side re-filtering that would reintroduce stale data.
        """
        mod = _fresh()
        final_rows = [_row(61, chapter_id=7), _row(62, chapter_id=7)]  # turn_id=63 raced out
        cur = _wire_cursor(monkeypatch, mod, fetchone={"chapter_id": 7}, fetchall=final_rows)

        ti = MagicMock()
        pushed = {}
        ti.xcom_push.side_effect = lambda key, value: pushed.update({key: value})
        mod._select_task(ti=ti, dag_run=MagicMock(conf={}))

        assert [t["turn_id"] for t in pushed["turns"]] == [61, 62]
        second_sql = cur.execute.call_args_list[1].args[0].lower()
        assert "not exists" in second_sql and "speaker_turn_videos" in second_sql, (
            "final statement must carry its own independent pending-row anti-join"
        )
        assert cur.execute.call_count == 2, "no third query — no scoped post-hoc filter"

    def test_no_pending_rows_returns_empty_list_after_one_statement(self, monkeypatch):
        mod = _fresh()
        cur = _wire_cursor(monkeypatch, mod, fetchone=None, fetchall=[])

        ti = MagicMock()
        pushed = {}
        ti.xcom_push.side_effect = lambda key, value: pushed.update({key: value})
        mod._select_task(ti=ti, dag_run=MagicMock(conf={}))

        assert pushed["turns"] == []
        assert cur.execute.call_count == 1, "the second statement must never be issued"


# ---------------------------------------------------------------------------
# Explicit limit key preserves global backlog drainage (issue #231)
# ---------------------------------------------------------------------------


class TestExplicitLimitGlobalDrain:
    def test_positive_limit_spans_multiple_chapters_globally_ordered(self, monkeypatch):
        mod = _fresh()
        rows = [_row(10, chapter_id=3, video_id="vidA"), _row(11, chapter_id=9, video_id="vidB")]
        cur = _wire_cursor(monkeypatch, mod, fetchall=rows)

        ti = MagicMock()
        pushed = {}
        ti.xcom_push.side_effect = lambda key, value: pushed.update({key: value})
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"limit": 5}))

        assert [t["turn_id"] for t in pushed["turns"]] == [10, 11], "chapters must not constrain the drain"
        assert cur.execute.call_count == 1
        assert cur.execute.call_args_list[0].args[1] == (5,)
        assert "chapter_id = %s" not in cur.execute.call_args_list[0].args[0].lower()

    @pytest.mark.parametrize(
        "limit_value,rows,expected_ids",
        [
            (0, [], []),
            (None, [_row(1)], [1]),
        ],
    )
    def test_limit_edge_values_bind_unchanged(self, monkeypatch, limit_value, rows, expected_ids):
        """0 returns no rows; None binds unchanged for an unbounded PostgreSQL LIMIT."""
        mod = _fresh()
        cur = _wire_cursor(monkeypatch, mod, fetchall=rows)

        ti = MagicMock()
        pushed = {}
        ti.xcom_push.side_effect = lambda key, value: pushed.update({key: value})
        mod._select_task(ti=ti, dag_run=MagicMock(conf={"limit": limit_value}))

        assert cur.execute.call_args_list[0].args[1] == (limit_value,), "no coercion of the raw value"
        assert [t["turn_id"] for t in pushed["turns"]] == expected_ids

    def test_negative_limit_propagates_database_error_unhandled(self, monkeypatch):
        """Invalid limit values retain the existing DB failure — no coercion or fallback."""
        mod = _fresh()
        _wire_cursor(monkeypatch, mod, execute_side_effect=ValueError("invalid input syntax for LIMIT"))

        with pytest.raises(ValueError):
            mod._select_task(ti=MagicMock(), dag_run=MagicMock(conf={"limit": -1}))


# ---------------------------------------------------------------------------
# Scoped precedence: chapter_id > video_id > limit key (issue #231)
# ---------------------------------------------------------------------------


class TestScopedPrecedenceOverLimit:
    def test_chapter_id_takes_precedence_over_video_id_and_limit(self, monkeypatch):
        mod = _fresh()
        cur = _wire_cursor(monkeypatch, mod, fetchall_side_effect=[[_row(1, chapter_id=10)], []])

        mod._select_task(
            ti=MagicMock(),
            dag_run=MagicMock(conf={"chapter_id": 10, "video_id": "other", "limit": 3}),
        )

        first_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "st.chapter_id = %s" in first_sql
        assert cur.execute.call_args_list[0].args[1] == (10,), "video_id and limit must be ignored"

    @pytest.mark.parametrize(
        "conf,expected_params",
        [
            ({"chapter_id": 10, "limit": 1}, (10,)),
            ({"video_id": "vid1", "limit": 1}, ("vid1",)),
        ],
    )
    def test_scoped_limit_is_ignored(self, monkeypatch, conf, expected_params):
        mod = _fresh()
        cur = _wire_cursor(monkeypatch, mod, fetchall_side_effect=[[_row(1, chapter_id=10)], []])

        mod._select_task(ti=MagicMock(), dag_run=MagicMock(conf=conf))

        first_sql = cur.execute.call_args_list[0].args[0].lower()
        assert "limit" not in first_sql, "an explicit limit must not cap a scoped result"
        assert cur.execute.call_args_list[0].args[1] == expected_params


# ---------------------------------------------------------------------------
# 2.10 materialize_turns skips when source video is not found
# ---------------------------------------------------------------------------


class TestMaterializeTurns:
    def _turn(
        self, turn_id=7, chapter_id=3, video_id="vid1", start=600.0, end=700.0, resolved_name=None, is_procedural=False
    ):
        return {
            "turn_id": turn_id,
            "chapter_id": chapter_id,
            "video_id": video_id,
            "start_seconds": start,
            "end_seconds": end,
            "resolved_name": resolved_name,
            "is_procedural": is_procedural,
        }

    def test_canonical_path_in_insert(self, monkeypatch):
        """TDD RED→GREEN: _materialize_task must store a canonical date-free path.

        Asserts:
        1. The INSERT output_path contains the canonical layout
           /congreso-es-tv/{video_id}/video_chapters/{chapter_id}/oradores/{output_turn_id}/video.mp4
        2. output_path does NOT contain '/turns/'
        3. output_path does NOT contain a YYYY-MM-DD date segment
        4. The same output_path value is passed to execute_plan (wiring assertion)
        """
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: "/data/src.mp4")

        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        execute_plan_mock = MagicMock()
        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", execute_plan_mock)
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        cur.description = [("turn_id",), ("start_seconds",), ("end_seconds",), ("is_approved",), ("is_voice_free",)]
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        ti = MagicMock()
        ti.xcom_pull.return_value = [self._turn(video_id="abc123", chapter_id=3)]

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        # Capture INSERT call args
        all_calls = cur.execute.call_args_list
        insert_calls = [c for c in all_calls if "INSERT" in str(c).upper()]
        assert len(insert_calls) >= 1, f"Expected INSERT call; got: {all_calls}"

        # The second positional arg in the INSERT VALUES tuple is output_path
        insert_args = insert_calls[0].args
        # args[1] is the params tuple: (tid, output_path)
        output_path = insert_args[1][1]

        # Assertion 1: canonical path shape
        assert "/congreso-es-tv/abc123/video_chapters/3/oradores/7/video.mp4" in output_path, (
            f"Expected canonical path in INSERT output_path; got: {output_path}"
        )

        # Assertion 2: no legacy /turns/ segment
        assert "/turns/" not in output_path, f"output_path must not contain '/turns/'; got: {output_path}"

        # Assertion 3: no date segment
        assert re.search(r"\d{4}-\d{2}-\d{2}", output_path) is None, (
            f"output_path must not contain a date segment; got: {output_path}"
        )

        # Assertion 4: wiring — same value passed to execute_plan
        execute_plan_positional_output_path = execute_plan_mock.call_args.args[2]
        assert execute_plan_positional_output_path == output_path, (
            f"execute_plan received different output_path than INSERT: "
            f"execute_plan={execute_plan_positional_output_path!r} vs INSERT={output_path!r}"
        )

    def test_missing_source_video_skips_without_ffmpeg(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: None)
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
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: "/data/src.mp4")

        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", lambda *a, **kw: None)
        monkeypatch.setattr(
            "congress_videos.modules.materialization_executor.get_cached_codec",
            lambda path, cache: "h264",
        )
        from pathlib import Path

        monkeypatch.setattr(mod, "get_orador_video_dir", lambda vid, chid, tid: Path(f"/out/{tid}"))
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        # approved trims query returns empty (no trims)
        cur.fetchall.return_value = []
        cur.description = [("turn_id",), ("start_seconds",), ("end_seconds",), ("is_approved",), ("is_voice_free",)]
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
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: "/data/src.mp4")

        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", MagicMock(side_effect=RuntimeError("ffmpeg boom")))
        from pathlib import Path

        monkeypatch.setattr(mod, "get_orador_video_dir", lambda vid, chid, tid: Path(f"/out/{tid}"))
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        cur.description = [("turn_id",), ("start_seconds",), ("end_seconds",), ("is_approved",), ("is_voice_free",)]
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

    def _make_materialize_mocks(self, monkeypatch, mod, plan_mock, turns):
        """Wire mocks for _materialize_task without touching ffmpeg or DB."""
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: "/data/src.mp4")
        monkeypatch.setattr(mod, "plan_turn_materialization", lambda t, tr: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", lambda *a, **kw: None)
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")
        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        cur.description = [
            ("turn_id",),
            ("start_seconds",),
            ("end_seconds",),
            ("is_approved",),
            ("is_voice_free",),
        ]
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)
        ti = MagicMock()
        ti.xcom_pull.return_value = turns
        return cur, ti

    def test_insert_params_include_turn_type_and_keep_intervals(self, monkeypatch):
        """issue #143: INSERT params tuple must be
        (turn_id, output_path, turn_type, keep_intervals) — a 4-tuple."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [self._turn(resolved_name=None)]
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        all_calls = cur.execute.call_args_list
        insert_calls = [c for c in all_calls if "INSERT" in str(c).upper()]
        assert len(insert_calls) >= 1, f"Expected INSERT; got: {all_calls}"
        # params tuple must be (turn_id, output_path, turn_type, keep_intervals)
        params = insert_calls[0].args[1]
        assert len(params) == 4, (
            f"INSERT params must be a 4-tuple (turn_id, output_path, turn_type, keep_intervals); got: {params}"
        )
        turn_id_param, output_path_param, turn_type_param, keep_intervals_param = params
        assert turn_id_param == 7
        assert isinstance(turn_type_param, str), f"turn_type must be a string; got: {turn_type_param!r}"
        assert turn_type_param in ("monologue", "qa"), (
            f"turn_type must be 'monologue' or 'qa'; got: {turn_type_param!r}"
        )
        assert json.loads(keep_intervals_param) == [[600.0, 700.0]], (
            f"keep_intervals must serialize the plan's own executed cut boundaries; got: {keep_intervals_param!r}"
        )

    def test_single_resolved_name_yields_monologue_in_insert(self, monkeypatch):
        """Single turn with a real resolved_name → INSERT must carry turn_type='monologue'."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [self._turn(resolved_name="Pedro Sanchez")]
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        params = insert_calls[0].args[1]
        assert params[2] == "monologue", f"Single-turn plan with one real name → monologue; got: {params[2]!r}"

    def test_two_distinct_names_yields_qa_in_insert(self, monkeypatch):
        """Grouped plan with 2 distinct real resolved_names → INSERT must carry turn_type='qa'."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7, 8)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [
            self._turn(turn_id=7, resolved_name="Pedro Sanchez"),
            self._turn(turn_id=8, resolved_name="Alberto Gonzalez"),
        ]
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        assert insert_calls, "Expected at least one INSERT for the grouped plan"
        # All rows in the plan share the same turn_type
        for c in insert_calls:
            assert c.args[1][2] == "qa", f"Two-name grouped plan → qa; got: {c.args[1][2]!r}"

    def test_grouped_plan_all_rows_share_same_turn_type(self, monkeypatch):
        """All rows in a grouped plan must receive the same turn_type value."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7, 8, 9)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=800.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [
            self._turn(turn_id=7, resolved_name="Pedro Sanchez"),
            self._turn(turn_id=8, resolved_name="Alberto Gonzalez"),
            self._turn(turn_id=9, resolved_name=None),
        ]
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        assert len(insert_calls) == 3, f"Expected 3 INSERTs for 3 turn_ids; got {len(insert_calls)}"
        turn_types = {c.args[1][2] for c in insert_calls}
        assert len(turn_types) == 1, f"All grouped rows must share one turn_type; got distinct values: {turn_types}"

    def test_classify_turn_type_called_with_row_map(self, monkeypatch):
        """classify_turn_type must receive 3 positional args including a
        turn_id -> row map built from the selected turns (issue #282)."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7,)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [self._turn(turn_id=7, resolved_name=None)]
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        classify_mock = MagicMock(return_value="monologue")
        monkeypatch.setattr(mod, "classify_turn_type", classify_mock)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        classify_mock.assert_called_once()
        call_args = classify_mock.call_args.args
        assert len(call_args) == 3, f"classify_turn_type must receive 3 positional args; got: {call_args}"
        turn_ids_arg, resolved_by_id_arg, turn_rows_by_id_arg = call_args
        assert 7 in turn_rows_by_id_arg
        assert turn_rows_by_id_arg[7]["turn_id"] == 7

    def test_two_label_group_with_null_names_yields_qa_in_insert(self, monkeypatch):
        """Grouped plan with 2 distinct speaker_label values and NULL
        resolved_names -> INSERT must carry turn_type='qa' via the
        label-first rule (issue #282), even though the legacy name rule
        alone would have frozen it at 'monologue'."""
        mod = _fresh()
        plan_mock = MagicMock()
        plan_mock.turn_ids = (7, 8)
        plan_mock.keep_intervals = (MagicMock(start=600.0, end=700.0),)
        plan_mock.needs_reencode = False
        plan_mock.output_turn_id = 7
        plan_mock.chapter_id = 3

        turns = [
            self._turn(turn_id=7, resolved_name=None, start=600.0, end=650.0),
            self._turn(turn_id=8, resolved_name=None, start=650.0, end=700.0),
        ]
        turns[0]["speaker_label"] = "SPEAKER_00"
        turns[1]["speaker_label"] = "SPEAKER_01"
        cur, ti = self._make_materialize_mocks(monkeypatch, mod, plan_mock, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        assert insert_calls, "Expected at least one INSERT"
        for c in insert_calls:
            assert c.args[1][2] == "qa", f"2-label group with NULL names -> qa; got: {c.args[1][2]!r}"


# ---------------------------------------------------------------------------
# Degenerate all-procedural group: no plan, but every turn still gets a row
# (issue #143 D5) — otherwise a permanently pending turn would block
# _select_automatic_chapter's MIN(turn_id) forever.
# ---------------------------------------------------------------------------


class TestDegenerateAllProceduralGroupDropped:
    def _turn(self, turn_id, chapter_id=3, video_id="vid1", start=0.0, end=5.0):
        return {
            "turn_id": turn_id,
            "chapter_id": chapter_id,
            "video_id": video_id,
            "start_seconds": start,
            "end_seconds": end,
            "resolved_name": None,
            "is_procedural": True,
        }

    def _wire(self, monkeypatch, mod, turns):
        find_source_mock = MagicMock(return_value="/data/src.mp4")
        execute_plan_mock = MagicMock()
        monkeypatch.setattr(mod, "_find_source_video_any_date", find_source_mock)
        monkeypatch.setattr(mod, "plan_turn_materialization", lambda t, tr: [])
        monkeypatch.setattr(mod, "execute_plan", execute_plan_mock)
        from pathlib import Path

        monkeypatch.setattr(mod, "get_orador_video_dir", lambda vid, chid, tid: Path(f"/out/{tid}"))

        pg = MagicMock()
        conn = MagicMock()
        cur = MagicMock()
        cur.fetchall.return_value = []
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        ti = MagicMock()
        ti.xcom_pull.return_value = turns
        return cur, ti, execute_plan_mock, find_source_mock

    def test_execute_plan_never_called(self, monkeypatch):
        """No plan exists for the group → execute_plan (and thus ffmpeg) must
        never run."""
        mod = _fresh()
        turns = [self._turn(80, start=0.0, end=5.0), self._turn(81, start=5.0, end=10.0)]
        cur, ti, execute_plan_mock, _ = self._wire(monkeypatch, mod, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        execute_plan_mock.assert_not_called()

    def test_one_row_inserted_per_dropped_turn(self, monkeypatch):
        mod = _fresh()
        turns = [self._turn(80, start=0.0, end=5.0), self._turn(81, start=5.0, end=10.0)]
        cur, ti, *_ = self._wire(monkeypatch, mod, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        assert len(insert_calls) == 2, f"Expected one INSERT per dropped turn; got: {insert_calls}"
        inserted_turn_ids = {c.args[1][0] for c in insert_calls}
        assert inserted_turn_ids == {80, 81}

    def test_keep_intervals_is_empty_json_array(self, monkeypatch):
        mod = _fresh()
        turns = [self._turn(80, start=0.0, end=5.0), self._turn(81, start=5.0, end=10.0)]
        cur, ti, *_ = self._wire(monkeypatch, mod, turns)

        mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        insert_calls = [c for c in cur.execute.call_args_list if "INSERT" in str(c).upper()]
        for c in insert_calls:
            keep_intervals_param = c.args[1][3]
            assert json.loads(keep_intervals_param) == []

    def test_summary_reports_dropped_procedural_count(self, monkeypatch):
        mod = _fresh()
        turns = [self._turn(80, start=0.0, end=5.0), self._turn(81, start=5.0, end=10.0)]
        cur, ti, *_ = self._wire(monkeypatch, mod, turns)

        result = mod._materialize_task(ti=ti, dag_run=MagicMock(conf={}))

        assert result.get("dropped_procedural") == 2


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
