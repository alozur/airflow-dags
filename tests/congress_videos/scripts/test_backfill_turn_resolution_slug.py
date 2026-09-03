"""Tests for the turn-resolution-slug backfill CLI (issue #339).

Covers both the dry-run planning half (PR2) and the `--execute` write
half (PR3): transactional UPDATE per turn_id, drift/rowcount abort,
autocommit guard, method-constraint preflight, and rollback-plan
round-trip. Every test mocks the DB connection/cursor — no live
Postgres is used anywhere in this file.
"""

import importlib.util
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SCRIPT = Path(__file__).resolve().parents[3] / "scripts" / "backfill_turn_resolution_slug.py"


def _load_script():
    spec = importlib.util.spec_from_file_location("backfill_turn_resolution_slug", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


backfill = _load_script()

STV_TABLE = "production.speaker_turn_videos"
ST_TABLE = "production.speaker_turns"


def _write_plan(tmp_path, entries):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(json.dumps(entries))
    return str(plan_path)


def _mock_conn(rows):
    """A MagicMock connection whose cursor's fetchall() returns `rows`."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestLoadPlan:
    def test_parses_entries_including_a_null_expected_current_slug(self, tmp_path):
        path = _write_plan(
            tmp_path,
            [
                {"turn_id": 8124, "expected_current_slug": "old", "new_slug": "new"},
                {"turn_id": 1, "expected_current_slug": None, "new_slug": "n"},
            ],
        )

        entries = backfill.load_plan(path)

        assert entries[0] == backfill.BackfillEntry(turn_id=8124, expected_current_slug="old", new_slug="new")
        assert entries[1].expected_current_slug is None

    @pytest.mark.parametrize(
        "entries, match",
        [
            (
                [
                    {"turn_id": 1, "expected_current_slug": None, "new_slug": "a"},
                    {"turn_id": 1, "expected_current_slug": None, "new_slug": "b"},
                ],
                "duplicate",
            ),
            ([{"turn_id": "8124", "expected_current_slug": None, "new_slug": "a"}], "integer"),
            ([{"turn_id": 1, "expected_current_slug": None}], "new_slug"),
            ([], "empty"),
        ],
    )
    def test_rejects_malformed_entries(self, tmp_path, entries, match):
        path = _write_plan(tmp_path, entries)

        with pytest.raises(backfill.BackfillInputError, match=match):
            backfill.load_plan(path)

    def test_surfaces_json_decode_errors_with_line_and_column(self, tmp_path):
        plan_path = tmp_path / "bad.json"
        plan_path.write_text("{not valid json")

        with pytest.raises(backfill.BackfillInputError, match="line"):
            backfill.load_plan(str(plan_path))


class TestValidateConfidence:
    def test_accepts_boundary_values(self):
        backfill.validate_confidence(0.0)
        backfill.validate_confidence(1.0)

    @pytest.mark.parametrize("bad_value", [1.5, -0.1])
    def test_rejects_out_of_range_values(self, bad_value):
        with pytest.raises(backfill.BackfillInputError, match=r"0\.0, 1\.0"):
            backfill.validate_confidence(bad_value)


class TestFetchCurrentState:
    def test_single_select_with_any_list_param_returns_rows_keyed_by_turn_id(self):
        rows = [
            {
                "turn_id": 8124,
                "output_path": "/data/turn_8124.mp4",
                "resolved_participant_slug": "old-slug",
                "speaker_label": "SPEAKER_02",
            }
        ]
        conn, cursor = _mock_conn(rows)

        result = backfill.fetch_current_state(conn, STV_TABLE, ST_TABLE, [8124, 8125])

        assert cursor.execute.call_count == 1
        query, params = cursor.execute.call_args[0]
        assert "turn_id = ANY(%s)" in query
        assert params == ([8124, 8125],)
        assert "%(" not in query  # no string interpolation of turn_ids
        assert result == {8124: rows[0]}


class TestCheckMethodConstraint:
    def test_admits_when_no_constraint_rows_are_returned(self):
        conn, _cursor = _mock_conn([])

        backfill.check_method_constraint(conn, STV_TABLE, "manual")  # no raise

    @pytest.mark.parametrize(
        "definition, should_raise",
        [
            ("CHECK (speaker_resolution_method = ANY (ARRAY['fuzzy']))", True),
            ("CHECK (speaker_resolution_method = ANY (ARRAY['manual']))", False),
        ],
    )
    def test_refuses_or_admits_based_on_constraint_definition(self, definition, should_raise):
        conn, _cursor = _mock_conn([{"definition": definition}])

        if should_raise:
            with pytest.raises(backfill.BackfillConstraintError, match="manual"):
                backfill.check_method_constraint(conn, STV_TABLE, "manual")
        else:
            backfill.check_method_constraint(conn, STV_TABLE, "manual")  # no raise


def _row(turn_id, slug, output_path="/a.mp4", speaker_label="SPEAKER_00"):
    return {
        "turn_id": turn_id,
        "output_path": output_path,
        "resolved_participant_slug": slug,
        "speaker_label": speaker_label,
    }


class TestRenderSummary:
    def _kwargs(self):
        return dict(mode_label="DRY RUN", qualified_table=STV_TABLE, method="manual", confidence=1.0)

    def test_renders_required_columns_with_output_path_last_and_untruncated(self):
        entry = backfill.BackfillEntry(turn_id=8124, expected_current_slug="old", new_slug="new")
        long_path = "/data/" + ("x" * 80) + "/turn_8124.mp4"

        report = backfill.render_summary([entry], {8124: _row(8124, "old", output_path=long_path)}, **self._kwargs())

        for column in ("turn_id", "speaker_label", "old_slug", "new_slug", "status"):
            assert column in report
        assert long_path in report
        assert report.rindex("status") < report.rindex("output_path")

    @pytest.mark.parametrize(
        "expected_current_slug, new_slug, current_row, want_status",
        [
            ("old", "new", _row(8124, "old"), "WOULD-UPDATE"),
            ("old", "new", _row(8124, "someone-else"), "DRIFT"),
            ("old", "new", None, "MISSING"),
            ("new", "new", _row(8124, "new"), "NO-CHANGE"),
        ],
    )
    def test_derives_the_correct_status(self, expected_current_slug, new_slug, current_row, want_status):
        entry = backfill.BackfillEntry(turn_id=8124, expected_current_slug=expected_current_slug, new_slug=new_slug)
        current_state = {8124: current_row} if current_row else {}

        report = backfill.render_summary([entry], current_state, **self._kwargs())

        assert f"{want_status}: 1" in report

    def test_footer_counts_multiple_statuses_independently(self):
        entries = [
            backfill.BackfillEntry(turn_id=1, expected_current_slug="a", new_slug="b"),
            backfill.BackfillEntry(turn_id=2, expected_current_slug="c", new_slug="d"),
        ]
        current_state = {1: _row(1, "a"), 2: _row(2, "drifted")}

        report = backfill.render_summary(entries, current_state, **self._kwargs())

        assert "WOULD-UPDATE: 1" in report
        assert "DRIFT: 1" in report


class TestParseArgs:
    def test_requires_input_and_confidence(self):
        with pytest.raises(SystemExit):
            backfill.parse_args([])

    @pytest.mark.parametrize(
        "extra_argv, want_method, want_execute",
        [([], "manual", False), (["--method", "manual_backfill", "--execute"], "manual_backfill", True)],
    )
    def test_method_and_execute_defaults_and_overrides(self, extra_argv, want_method, want_execute):
        args = backfill.parse_args(["--input", "p.json", "--confidence", "0.8"] + extra_argv)

        assert args.method == want_method
        assert args.execute is want_execute

    def test_rollback_out_defaults_to_none_and_accepts_a_path(self):
        args = backfill.parse_args(["--input", "p.json", "--confidence", "0.8"])
        assert args.rollback_out is None

        args = backfill.parse_args(["--input", "p.json", "--confidence", "0.8", "--rollback-out", "inv.json"])
        assert args.rollback_out == "inv.json"


def _fake_pg_connection(rows):
    """A MagicMock PostgresConnection whose get_connection() yields a mocked conn/cursor."""
    conn, cursor = _mock_conn(rows)
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    pg_conn = MagicMock()
    pg_conn.get_qualified_table.side_effect = lambda name: f"production.{name}"
    pg_conn.get_connection.return_value = ctx
    return pg_conn, conn, cursor


class TestMainDryRun:
    def test_dry_run_end_to_end_zero_writes_and_zero_commits(self, monkeypatch, tmp_path):
        pg_conn, conn, cursor = _fake_pg_connection([])
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 8124, "expected_current_slug": "old", "new_slug": "new"}])

        exit_code = backfill.main(["--input", path, "--confidence", "1.0"])

        assert exit_code == 0
        assert cursor.execute.call_count == 1  # exactly one SELECT, no other statement
        executed_sql = cursor.execute.call_args[0][0].upper()
        for verb in ("INSERT", "UPDATE", "DELETE"):
            assert verb not in executed_sql
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()

    def test_dry_run_never_calls_apply_backfill(self, monkeypatch, tmp_path):
        called = {"hit": False}
        monkeypatch.setattr(backfill, "apply_backfill", lambda *a, **kw: called.__setitem__("hit", True))
        pg_conn, _conn, _cursor = _fake_pg_connection([])
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 1, "expected_current_slug": None, "new_slug": "n"}])

        backfill.main(["--input", path, "--confidence", "1.0"])

        assert called["hit"] is False

    @pytest.mark.parametrize(
        "entries, confidence",
        [([], "1.0"), ([{"turn_id": 1, "expected_current_slug": None, "new_slug": "a"}], "1.5")],
    )
    def test_input_validation_failures_return_exit_code_2(self, tmp_path, entries, confidence):
        path = _write_plan(tmp_path, entries)

        assert backfill.main(["--input", path, "--confidence", confidence]) == 2


class TestBuildUpdateQuery:
    def test_never_predicates_on_output_path(self):
        query = backfill.build_update_query(STV_TABLE)

        assert "output_path" not in query
        assert "WHERE turn_id = %s" in query
        assert "resolved_participant_slug = %s" in query
        assert "resolved_participant_slug IS NOT DISTINCT FROM %s" in query


def _apply_cursor(rowcounts):
    """A MagicMock cursor whose .execute() sets .rowcount to the next of `rowcounts`."""
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    values = iter(rowcounts)
    cursor.execute.side_effect = lambda *a, **kw: setattr(cursor, "rowcount", next(values))
    return cursor


class TestApplyBackfill:
    def test_happy_path_single_commit_all_updated_no_output_path_predicate(self):
        # turn_id=9001 is deliberately not the MIN(turn_id) for its group — the
        # write path has no representative filter, so it is corrected too.
        cursor = _apply_cursor([1, 1])
        conn = MagicMock()
        conn.cursor.return_value = cursor
        entries = [backfill.BackfillEntry(turn_id=t, expected_current_slug="old", new_slug="new") for t in (9001, 3)]

        statuses = backfill.apply_backfill(conn, STV_TABLE, entries, 0.9, "manual_backfill")

        assert cursor.execute.call_count == 2
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        assert statuses == {9001: "UPDATED", 3: "UPDATED"}
        for call in cursor.execute.call_args_list:
            assert "output_path" not in call[0][0]
        # method/confidence are always the caller's, never inherited from a prior row
        assert cursor.execute.call_args_list[0][0][1] == ("new", 0.9, "manual_backfill", 9001, "old")

    def test_drift_aborts_and_rolls_back_the_whole_run(self):
        cursor = _apply_cursor([1, 0, 1])  # turn_id=8124 is the 2nd of 3 — its slug drifted
        conn = MagicMock()
        conn.cursor.return_value = cursor
        entries = [backfill.BackfillEntry(turn_id=t, expected_current_slug="old", new_slug="new") for t in (1, 8124, 3)]

        with pytest.raises(backfill.BackfillDriftError, match="8124"):
            backfill.apply_backfill(conn, STV_TABLE, entries, 1.0, "manual")

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        assert cursor.execute.call_count == 2  # stopped at the offending statement

    @pytest.mark.parametrize("bad_rowcount", [0, 2])
    def test_rowcount_deviation_any_value_but_one_aborts(self, bad_rowcount):
        cursor = _apply_cursor([bad_rowcount])
        conn = MagicMock()
        conn.cursor.return_value = cursor
        entry = backfill.BackfillEntry(turn_id=1, expected_current_slug="old", new_slug="new")

        with pytest.raises(backfill.BackfillDriftError):
            backfill.apply_backfill(conn, STV_TABLE, [entry], 1.0, "manual")

        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()


class TestCheckAutocommitDisabled:
    def test_admits_default_mock_but_refuses_literal_true(self):
        conn = MagicMock()
        backfill.check_autocommit_disabled(conn)  # admits: auto-generated attr isn't literal True

        conn.autocommit = True
        with pytest.raises(backfill.BackfillUsageError, match="autocommit"):
            backfill.check_autocommit_disabled(conn)


class TestWriteRollbackPlan:
    def test_emits_an_inverse_plan_consumable_by_load_plan(self, tmp_path):
        entries = [backfill.BackfillEntry(turn_id=8124, expected_current_slug="wrong", new_slug="right")]
        out_path = tmp_path / "inverse.json"

        backfill.write_rollback_plan(str(out_path), entries)
        rollback_entries = backfill.load_plan(str(out_path))

        assert rollback_entries == [
            backfill.BackfillEntry(turn_id=8124, expected_current_slug="right", new_slug="wrong")
        ]


def _fake_pg_connection_for_execute(select_rows, update_rowcounts):
    """MagicMock PostgresConnection: SELECTs return `select_rows`; each UPDATE
    consumes the next `update_rowcounts` value as its rowcount."""
    cursor = MagicMock()
    cursor.fetchall.return_value = select_rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)
    values = iter(update_rowcounts)

    def _execute(query, *_args, **_kwargs):
        if query.strip().upper().startswith("UPDATE"):
            cursor.rowcount = next(values)

    cursor.execute.side_effect = _execute
    conn = MagicMock()
    conn.cursor.return_value = cursor
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    pg_conn = MagicMock()
    pg_conn.get_qualified_table.side_effect = lambda name: f"production.{name}"
    pg_conn.get_connection.return_value = ctx
    return pg_conn, conn, cursor


class TestMainExecute:
    def test_happy_path_commits_writes_rollback_plan_and_exits_zero_rendering_updated(
        self, monkeypatch, tmp_path, capsys
    ):
        pg_conn, conn, cursor = _fake_pg_connection_for_execute([], [1, 1])
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(
            tmp_path,
            [
                {"turn_id": 1, "expected_current_slug": "old", "new_slug": "new"},
                {"turn_id": 2, "expected_current_slug": "old2", "new_slug": "new2"},
            ],
        )
        rollback_path = tmp_path / "inverse.json"

        exit_code = backfill.main(
            [
                "--input",
                path,
                "--confidence",
                "1.0",
                "--execute",
                "--method",
                "manual_backfill",
                "--rollback-out",
                str(rollback_path),
            ]
        )

        assert exit_code == 0
        update_calls = [c for c in cursor.execute.call_args_list if c[0][0].strip().upper().startswith("UPDATE")]
        assert len(update_calls) == 2
        for call in update_calls:
            assert "output_path" not in call[0][0]
        conn.commit.assert_called_once()
        conn.rollback.assert_not_called()
        assert "UPDATED" in capsys.readouterr().out
        rollback_entries = backfill.load_plan(str(rollback_path))
        assert rollback_entries[0] == backfill.BackfillEntry(turn_id=1, expected_current_slug="new", new_slug="old")

    def test_drift_aborts_rolls_back_exits_3_and_renders_refused_drift(self, monkeypatch, tmp_path, capsys):
        pg_conn, conn, _cursor = _fake_pg_connection_for_execute([], [0])
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 8124, "expected_current_slug": "old", "new_slug": "new"}])

        exit_code = backfill.main(["--input", path, "--confidence", "1.0", "--execute"])

        assert exit_code == 3
        conn.rollback.assert_called_once()
        conn.commit.assert_not_called()
        assert "REFUSED-DRIFT" in capsys.readouterr().out

    def test_constraint_refusal_aborts_before_any_update_and_exits_3(self, monkeypatch, tmp_path):
        pg_conn, _conn, cursor = _fake_pg_connection_for_execute(
            [{"definition": "CHECK (speaker_resolution_method = ANY (ARRAY['fuzzy']))"}], []
        )
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 1, "expected_current_slug": "old", "new_slug": "new"}])

        exit_code = backfill.main(["--input", path, "--confidence", "1.0", "--execute"])

        assert exit_code == 3
        update_calls = [c for c in cursor.execute.call_args_list if c[0][0].strip().upper().startswith("UPDATE")]
        assert update_calls == []

    def test_autocommit_true_refuses_before_any_query(self, monkeypatch, tmp_path):
        pg_conn, conn, cursor = _fake_pg_connection_for_execute([], [])
        conn.autocommit = True
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 1, "expected_current_slug": "old", "new_slug": "new"}])

        exit_code = backfill.main(["--input", path, "--confidence", "1.0", "--execute"])

        assert exit_code == 3
        cursor.execute.assert_not_called()
