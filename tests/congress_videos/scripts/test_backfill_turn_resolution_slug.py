"""Tests for the turn-resolution-slug backfill CLI (issue #339), read-only half.

PR2 covers only dry-run planning. The `--execute` write path is a
deliberate `NotImplementedError` stub. Every test mocks the DB
connection/cursor — no write (INSERT/UPDATE/DELETE) is ever asserted.
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

        assert entries[0] == backfill.BackfillEntry(
            turn_id=8124, expected_current_slug="old", new_slug="new"
        )
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

        report = backfill.render_summary(
            [entry], {8124: _row(8124, "old", output_path=long_path)}, **self._kwargs()
        )

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
    def test_derives_the_correct_status(
        self, expected_current_slug, new_slug, current_row, want_status
    ):
        entry = backfill.BackfillEntry(
            turn_id=8124, expected_current_slug=expected_current_slug, new_slug=new_slug
        )
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
        path = _write_plan(
            tmp_path, [{"turn_id": 8124, "expected_current_slug": "old", "new_slug": "new"}]
        )

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
        monkeypatch.setattr(
            backfill, "apply_backfill", lambda *a, **kw: called.__setitem__("hit", True)
        )
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

    def test_execute_flag_raises_not_implemented_error(self, monkeypatch, tmp_path):
        pg_conn, _conn, _cursor = _fake_pg_connection([])
        monkeypatch.setattr(backfill, "PostgresConnection", lambda: pg_conn)
        path = _write_plan(tmp_path, [{"turn_id": 1, "expected_current_slug": None, "new_slug": "a"}])

        with pytest.raises(NotImplementedError):
            backfill.main(["--input", path, "--confidence", "1.0", "--execute"])
