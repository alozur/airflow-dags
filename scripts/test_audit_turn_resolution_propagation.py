"""Tests for the read-only speaker-resolution propagation audit (issue #321, AC3).

Loads the standalone script via importlib (mirrors
tests/congress_videos/scripts/test_generate_youtube_token.py) instead of a
package import, since scripts/ is not a package. Every test mocks the DB
connection/cursor — no live Postgres is required or contacted, and no test
here ever asserts a write (INSERT/UPDATE/DELETE) was executed.
"""

import importlib.util
from pathlib import Path
from unittest.mock import MagicMock

_SCRIPT = Path(__file__).resolve().parent / "audit_turn_resolution_propagation.py"


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
