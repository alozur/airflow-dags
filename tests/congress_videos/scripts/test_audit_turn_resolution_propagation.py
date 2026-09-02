"""Tests for the read-only speaker-resolution propagation audit (issue #321, AC3).

Loads the standalone script via importlib (mirrors
tests/congress_videos/scripts/test_generate_youtube_token.py) instead of a
package import, since scripts/ is not a package. The audited script stays at
repo-root scripts/ (DagBag safe-mode + its own sys.path hack), so _SCRIPT
resolves via parents[3] rather than a sibling lookup. Every test mocks the DB
connection/cursor — no live Postgres is required or contacted, and no test
here ever asserts a write (INSERT/UPDATE/DELETE) was executed.
"""

import hashlib
import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[3] / "scripts" / "audit_turn_resolution_propagation.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location("audit_turn_resolution_propagation", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


audit = _load_script()

STV_TABLE = "production.speaker_turn_videos"
ST_TABLE = "production.speaker_turns"


def _mock_conn(rows):
    """A MagicMock connection whose cursor's fetchall() returns `rows`."""
    cursor = MagicMock()
    cursor.fetchall.return_value = rows
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn, cursor


class TestBuildAuditQuery:
    def test_joins_speaker_turn_videos_and_speaker_turns_on_turn_id(self):
        query = audit.build_audit_query(STV_TABLE, ST_TABLE)

        assert f"JOIN {ST_TABLE}" in query
        assert "stv.turn_id = st.turn_id" in query

    def test_scopes_to_already_uploaded_groups(self):
        query = audit.build_audit_query(STV_TABLE, ST_TABLE)

        assert "is_uploaded_to_youtube = TRUE" in query

    def test_flags_multi_label_groups_with_a_uniform_slug(self):
        query = audit.build_audit_query(STV_TABLE, ST_TABLE)

        assert "COUNT(DISTINCT st.speaker_label) > 1" in query
        assert "COUNT(DISTINCT stv.resolved_participant_slug) = 1" in query

    def test_query_never_contains_a_write_statement(self):
        query = audit.build_audit_query(STV_TABLE, ST_TABLE)

        upper = query.upper()
        for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER"):
            assert verb not in upper


class TestFetchPropagatedGroups:
    def test_executes_exactly_one_select_statement(self):
        conn, cursor = _mock_conn([])

        audit.fetch_propagated_groups(conn, STV_TABLE, ST_TABLE)

        assert cursor.execute.call_count == 1
        executed_sql = cursor.execute.call_args[0][0]
        assert executed_sql.strip().upper().startswith("SELECT")

    def test_performs_no_writes(self):
        conn, cursor = _mock_conn([])

        audit.fetch_propagated_groups(conn, STV_TABLE, ST_TABLE)

        executed_sql = cursor.execute.call_args[0][0].upper()
        for verb in ("INSERT", "UPDATE", "DELETE"):
            assert verb not in executed_sql
        conn.commit.assert_not_called()

    def test_returns_the_rows_from_cursor_fetchall(self):
        expected = [
            {
                "output_path": "/data/2026-08-30/abc/turn_7.mp4",
                "speaker_labels": ["SPEAKER_03", "SPEAKER_07"],
                "resolved_participant_slug": "felix-bolanos-garcia",
            }
        ]
        conn, _cursor = _mock_conn(expected)

        result = audit.fetch_propagated_groups(conn, STV_TABLE, ST_TABLE)

        assert result == expected

    def test_empty_result_set_returns_an_empty_list(self):
        conn, _cursor = _mock_conn([])

        result = audit.fetch_propagated_groups(conn, STV_TABLE, ST_TABLE)

        assert result == []


class TestFormatReport:
    def test_reports_zero_affected_groups(self):
        report = audit.format_report([])

        assert "Affected groups: 0" in report

    def test_reports_count_output_path_labels_and_slug_per_group(self):
        rows = [
            {
                "output_path": "/data/2026-08-30/abc/turn_7.mp4",
                "speaker_labels": ["SPEAKER_03", "SPEAKER_07"],
                "resolved_participant_slug": "felix-bolanos-garcia",
            },
        ]

        report = audit.format_report(rows)

        assert "Affected groups: 1" in report
        assert "/data/2026-08-30/abc/turn_7.mp4" in report
        assert "SPEAKER_03" in report
        assert "SPEAKER_07" in report
        assert "felix-bolanos-garcia" in report

    def test_reports_multiple_groups_each_on_their_own_detail_line(self):
        rows = [
            {
                "output_path": "/a/video.mp4",
                "speaker_labels": ["SPEAKER_01", "SPEAKER_02"],
                "resolved_participant_slug": "slug-a",
            },
            {
                "output_path": "/b/video.mp4",
                "speaker_labels": ["SPEAKER_03", "SPEAKER_04"],
                "resolved_participant_slug": "slug-b",
            },
        ]

        report = audit.format_report(rows)

        assert "Affected groups: 2" in report
        assert "/a/video.mp4" in report
        assert "/b/video.mp4" in report
        assert "slug-a" in report
        assert "slug-b" in report


# --- Mixed-slug audit mode (issue #339) -----------------------------------
#
# Adds an additive, read-only mixed-slug variant alongside the untouched
# uniform-slug audit above. No live Postgres is required or contacted; the
# mixed-mode HAVING relaxation is exercised via the mocked cursor/connection
# pattern already established for the uniform audit.

_ORIGINAL_AUDIT_QUERY_TEMPLATE = """
    SELECT stv.output_path AS output_path,
           array_agg(DISTINCT st.speaker_label ORDER BY st.speaker_label) AS speaker_labels,
           MAX(stv.resolved_participant_slug) AS resolved_participant_slug
    FROM {stv_table} stv
    JOIN {st_table} st ON stv.turn_id = st.turn_id
    WHERE stv.output_path IN (
        SELECT output_path FROM {stv_table} WHERE is_uploaded_to_youtube = TRUE
    )
    GROUP BY stv.output_path
    HAVING COUNT(DISTINCT st.speaker_label) > 1
       AND COUNT(DISTINCT stv.resolved_participant_slug) = 1
       AND COUNT(stv.resolved_participant_slug) = COUNT(*)
    ORDER BY stv.output_path
"""

BOLANOS_MIXED_ROW = {
    "output_path": "/data/2026-08-30/xyz/turn_12.mp4",
    "speaker_labels": ["SPEAKER_02", "SPEAKER_05"],
    "resolved_slugs": ["felix-bolanos-garcia", "isabel-rodriguez-garcia"],
    "total_rows": 30,
    "resolved_rows": 30,
}


class TestAuditQueryTemplateUnchanged:
    def test_byte_identical_to_pre_change_template(self):
        assert audit.AUDIT_QUERY_TEMPLATE == _ORIGINAL_AUDIT_QUERY_TEMPLATE

    def test_hash_identical_to_pre_change_template(self):
        current_hash = hashlib.sha256(audit.AUDIT_QUERY_TEMPLATE.encode()).hexdigest()
        original_hash = hashlib.sha256(_ORIGINAL_AUDIT_QUERY_TEMPLATE.encode()).hexdigest()

        assert current_hash == original_hash


class TestMixedSlugQueryTemplate:
    def test_having_relaxes_both_counters_to_more_than_one(self):
        assert "COUNT(DISTINCT st.speaker_label) > 1" in audit.MIXED_SLUG_QUERY_TEMPLATE
        assert (
            "COUNT(DISTINCT stv.resolved_participant_slug) > 1"
            in audit.MIXED_SLUG_QUERY_TEMPLATE
        )

    def test_drops_the_all_non_null_requirement(self):
        assert "= COUNT(*)" not in audit.MIXED_SLUG_QUERY_TEMPLATE


class TestBuildMixedSlugQuery:
    def test_emits_filter_clause_for_resolved_slugs(self):
        query = audit.build_mixed_slug_query(STV_TABLE, ST_TABLE)

        assert "FILTER (WHERE stv.resolved_participant_slug IS NOT NULL)" in query
        assert "AS resolved_slugs" in query

    def test_emits_total_rows_and_resolved_rows_aliases(self):
        query = audit.build_mixed_slug_query(STV_TABLE, ST_TABLE)

        assert "COUNT(*) AS total_rows" in query
        assert "COUNT(stv.resolved_participant_slug) AS resolved_rows" in query

    def test_relaxes_having_clause_on_both_counters(self):
        query = audit.build_mixed_slug_query(STV_TABLE, ST_TABLE)

        assert "COUNT(DISTINCT st.speaker_label) > 1" in query
        assert "COUNT(DISTINCT stv.resolved_participant_slug) > 1" in query

    def test_never_contains_a_write_statement(self):
        query = audit.build_mixed_slug_query(STV_TABLE, ST_TABLE)

        upper = query.upper()
        for verb in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "DROP", "ALTER"):
            assert verb not in upper


class TestFetchMixedSlugGroups:
    def test_executes_exactly_one_select_statement(self):
        conn, cursor = _mock_conn([])

        audit.fetch_mixed_slug_groups(conn, STV_TABLE, ST_TABLE)

        assert cursor.execute.call_count == 1
        executed_sql = cursor.execute.call_args[0][0]
        assert executed_sql.strip().upper().startswith("SELECT")

    def test_performs_no_writes(self):
        conn, cursor = _mock_conn([])

        audit.fetch_mixed_slug_groups(conn, STV_TABLE, ST_TABLE)

        executed_sql = cursor.execute.call_args[0][0].upper()
        for verb in ("INSERT", "UPDATE", "DELETE"):
            assert verb not in executed_sql
        conn.commit.assert_not_called()

    def test_returns_the_rows_from_cursor_fetchall(self):
        expected = [BOLANOS_MIXED_ROW]
        conn, _cursor = _mock_conn(expected)

        result = audit.fetch_mixed_slug_groups(conn, STV_TABLE, ST_TABLE)

        assert result == expected

    def test_empty_result_set_returns_an_empty_list(self):
        conn, _cursor = _mock_conn([])

        result = audit.fetch_mixed_slug_groups(conn, STV_TABLE, ST_TABLE)

        assert result == []


class TestFormatMixedReport:
    def test_reports_zero_mixed_groups(self):
        report = audit.format_mixed_report([])

        assert "Mixed-slug groups: 0" in report

    def test_reports_resolved_slugs_total_rows_and_resolved_rows(self):
        report = audit.format_mixed_report([BOLANOS_MIXED_ROW])

        assert "Mixed-slug groups: 1" in report
        assert "/data/2026-08-30/xyz/turn_12.mp4" in report
        assert "felix-bolanos-garcia" in report
        assert "isabel-rodriguez-garcia" in report
        assert "total_rows=30" in report
        assert "resolved_rows=30" in report


class TestMixedSlugFixtureScenario:
    """Bolaños-shaped fixture: 5 rows resolved to one slug, 25 to another."""

    def test_absent_in_uniform_mode_present_in_mixed_mode(self):
        # Uniform-mode HAVING requires exactly one distinct resolved slug, so
        # this group never reaches the cursor's result set in uniform mode.
        uniform_conn, _uniform_cursor = _mock_conn([])
        uniform_rows = audit.fetch_propagated_groups(uniform_conn, STV_TABLE, ST_TABLE)
        assert uniform_rows == []

        # Mixed-mode HAVING is relaxed to `> 1`, so the same group surfaces.
        mixed_conn, _mixed_cursor = _mock_conn([BOLANOS_MIXED_ROW])
        mixed_rows = audit.fetch_mixed_slug_groups(mixed_conn, STV_TABLE, ST_TABLE)

        assert mixed_rows == [BOLANOS_MIXED_ROW]
        report = audit.format_mixed_report(mixed_rows)
        assert "felix-bolanos-garcia" in report
        assert "isabel-rodriguez-garcia" in report
        assert "total_rows=30" in report
        assert "resolved_rows=30" in report


class TestParseArgs:
    def test_default_mode_is_both(self):
        args = audit.parse_args([])

        assert args.mode == "both"

    def test_accepts_each_valid_mode(self):
        for mode in ("uniform", "mixed", "both"):
            args = audit.parse_args(["--mode", mode])
            assert args.mode == mode

    def test_rejects_an_invalid_mode(self):
        with pytest.raises(SystemExit):
            audit.parse_args(["--mode", "bogus"])


class _FakeCursor:
    """Minimal cursor stub: records executed SQL and routes fetchall() by query shape."""

    def __init__(self, uniform_rows, mixed_rows, call_log):
        self._uniform_rows = uniform_rows
        self._mixed_rows = mixed_rows
        self._call_log = call_log
        self._last_query = ""

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def execute(self, query):
        self._call_log.append(query)
        self._last_query = query

    def fetchall(self):
        if "resolved_slugs" in self._last_query:
            return self._mixed_rows
        return self._uniform_rows


class _FakeConn:
    def __init__(self, uniform_rows, mixed_rows, call_log):
        self._uniform_rows = uniform_rows
        self._mixed_rows = mixed_rows
        self._call_log = call_log

    def cursor(self):
        return _FakeCursor(self._uniform_rows, self._mixed_rows, self._call_log)

    def commit(self):
        raise AssertionError("main() must not commit — the audit is read-only")


class _FakeConnCtx:
    def __init__(self, uniform_rows, mixed_rows, call_log):
        self._uniform_rows = uniform_rows
        self._mixed_rows = mixed_rows
        self._call_log = call_log

    def __enter__(self):
        return _FakeConn(self._uniform_rows, self._mixed_rows, self._call_log)

    def __exit__(self, *exc_info):
        return False


class _FakePgConnection:
    def __init__(self, uniform_rows, mixed_rows, call_log):
        self._uniform_rows = uniform_rows
        self._mixed_rows = mixed_rows
        self._call_log = call_log

    def get_qualified_table(self, name):
        return f"production.{name}"

    def get_connection(self):
        return _FakeConnCtx(self._uniform_rows, self._mixed_rows, self._call_log)


class TestMain:
    def test_mode_uniform_runs_exactly_one_select(self, monkeypatch):
        call_log = []
        monkeypatch.setattr(
            audit, "PostgresConnection", lambda: _FakePgConnection([], [], call_log)
        )

        exit_code = audit.main(["--mode", "uniform"])

        assert len(call_log) == 1
        assert exit_code == 0

    def test_mode_mixed_runs_exactly_one_select(self, monkeypatch):
        call_log = []
        monkeypatch.setattr(
            audit, "PostgresConnection", lambda: _FakePgConnection([], [], call_log)
        )

        exit_code = audit.main(["--mode", "mixed"])

        assert len(call_log) == 1
        assert exit_code == 0

    def test_mode_both_runs_exactly_two_selects_and_returns_zero(self, monkeypatch):
        call_log = []
        monkeypatch.setattr(
            audit, "PostgresConnection", lambda: _FakePgConnection([], [], call_log)
        )

        exit_code = audit.main(["--mode", "both"])

        assert len(call_log) == 2
        assert exit_code == 0

    def test_default_argv_none_uses_mode_both(self, monkeypatch):
        call_log = []
        monkeypatch.setattr(
            audit, "PostgresConnection", lambda: _FakePgConnection([], [], call_log)
        )
        monkeypatch.setattr("sys.argv", ["audit_turn_resolution_propagation.py"])

        exit_code = audit.main()

        assert len(call_log) == 2
        assert exit_code == 0

    def test_returns_zero_even_when_groups_are_found(self, monkeypatch):
        call_log = []
        uniform_row = {
            "output_path": "/a.mp4",
            "speaker_labels": ["SPEAKER_00", "SPEAKER_01"],
            "resolved_participant_slug": "slug-a",
        }
        monkeypatch.setattr(
            audit,
            "PostgresConnection",
            lambda: _FakePgConnection([uniform_row], [BOLANOS_MIXED_ROW], call_log),
        )

        exit_code = audit.main(["--mode", "both"])

        assert exit_code == 0
