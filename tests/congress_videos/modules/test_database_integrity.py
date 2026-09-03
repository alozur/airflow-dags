"""B1/B3 [RED] Tests for source integrity DB operations.

B1: record_source_integrity_failure — three scenarios (upsert idempotency)
B3: get_processed_video_ids retry-window exclusion — three scenarios
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Fixtures (mirrors test_database.py pattern)
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    """Provide minimal env vars so PostgresConnection.__init__ does not raise."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def db(mocker):
    """Return a (CongressionalVideoDB, mock_cursor) pair with DB fully mocked."""
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)

    from congress_videos.modules.database import CongressionalVideoDB

    instance = CongressionalVideoDB()
    return instance, mock_cursor


# ---------------------------------------------------------------------------
# B1: record_source_integrity_failure
# ---------------------------------------------------------------------------


class TestRecordSourceIntegrityFailure:
    def test_method_exists_on_db_class(self, db):
        """CongressionalVideoDB must have a record_source_integrity_failure method."""
        instance, _ = db
        assert hasattr(instance, "record_source_integrity_failure"), (
            "CongressionalVideoDB must define record_source_integrity_failure()"
        )

    def test_executes_upsert_sql_for_video(self, db):
        """The method must execute an INSERT … ON CONFLICT DO UPDATE statement."""
        instance, mock_cursor = db
        instance.record_source_integrity_failure("abc123")

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("INSERT" in sql and "ON CONFLICT" in sql for sql in executed_sqls), (
            "record_source_integrity_failure must use INSERT … ON CONFLICT"
        )

    def test_sets_download_retry_after_in_upsert(self, db):
        """The upsert must reference download_retry_after."""
        instance, mock_cursor = db
        instance.record_source_integrity_failure("vid_retry")

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("download_retry_after" in sql for sql in executed_sqls), (
            "record_source_integrity_failure must set download_retry_after"
        )

    def test_uses_greatest_for_forward_only_semantics(self, db):
        """Must use GREATEST(...) so retry_after never moves backwards."""
        instance, mock_cursor = db
        instance.record_source_integrity_failure("vid_greatest")

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("GREATEST" in sql.upper() for sql in executed_sqls), (
            "record_source_integrity_failure must use GREATEST() for forward-only semantics"
        )

    def test_is_processed_not_in_update_set(self, db):
        """The ON CONFLICT DO UPDATE must NOT reset is_processed to FALSE."""
        instance, mock_cursor = db
        instance.record_source_integrity_failure("vid_no_regression")

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        # Find the UPDATE SET clause
        for sql in executed_sqls:
            if "ON CONFLICT" in sql and "DO UPDATE" in sql:
                update_set_portion = sql.split("DO UPDATE SET")[1] if "DO UPDATE SET" in sql else ""
                assert "is_processed" not in update_set_portion, (
                    "ON CONFLICT DO UPDATE must NOT include is_processed (would regress TRUE→FALSE)"
                )

    def test_default_retry_hours_is_12(self, db):
        """Default retry_after_hours must be 12."""
        import inspect

        from congress_videos.modules.database import CongressionalVideoDB

        sig = inspect.signature(CongressionalVideoDB.record_source_integrity_failure)
        param = sig.parameters.get("retry_after_hours")
        assert param is not None, "must have retry_after_hours parameter"
        assert param.default == 12, f"retry_after_hours default must be 12, got {param.default!r}"

    def test_passes_video_id_and_hours_as_params(self, db):
        """Must pass video_id and retry hours as SQL parameters (not f-string injection)."""
        instance, mock_cursor = db
        instance.record_source_integrity_failure("safe_vid", retry_after_hours=24)

        # Collect all parameter tuples passed to execute
        all_params = []
        for c in mock_cursor.execute.call_args_list:
            args = c[0]
            if len(args) > 1:
                all_params.extend(args[1] if isinstance(args[1], (list, tuple)) else [args[1]])

        # video_id must appear in params
        assert "safe_vid" in all_params, "video_id must be passed as a SQL parameter"


# ---------------------------------------------------------------------------
# B3: get_processed_video_ids retry-window exclusion
# ---------------------------------------------------------------------------


class TestGetProcessedVideoIdsRetryWindow:
    def test_video_in_retry_window_is_in_excluded_set(self, db):
        """Video with download_retry_after > NOW() and is_processed=FALSE must be
        returned in the excluded set (treated as deferred / skip-for-now)."""
        instance, mock_cursor = db
        # Simulate DB returning the video_id (i.e. it matches the extended WHERE clause)
        mock_cursor.fetchall.return_value = [{"video_id": "deferred_vid"}]

        result = instance.get_processed_video_ids(["deferred_vid"])

        assert "deferred_vid" in result, "Video inside retry window must be excluded (returned in processed set)"

    def test_retry_window_predicate_in_sql(self, db):
        """The SQL query must include the retry-window predicate."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_processed_video_ids(["some_vid"])

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("download_retry_after" in sql for sql in executed_sqls), (
            "get_processed_video_ids must filter by download_retry_after > NOW()"
        )

    def test_video_past_retry_window_not_in_excluded_set(self, db):
        """Video with download_retry_after <= NOW() and is_processed=FALSE should NOT
        be returned by the query (not in the excluded set — it's eligible again)."""
        instance, mock_cursor = db
        # DB returns empty (the video does NOT match the WHERE clause)
        mock_cursor.fetchall.return_value = []

        result = instance.get_processed_video_ids(["eligible_vid"])

        assert "eligible_vid" not in result, (
            "Video past retry window must NOT be in the excluded set (eligible for re-download)"
        )

    def test_video_with_null_retry_after_not_in_excluded_set(self, db):
        """Video with download_retry_after = NULL and is_processed=FALSE should NOT
        be in the excluded set (no integrity failure recorded)."""
        instance, mock_cursor = db
        # DB returns empty (NULL retry_after does not match download_retry_after > NOW())
        mock_cursor.fetchall.return_value = []

        result = instance.get_processed_video_ids(["null_retry_vid"])

        assert "null_retry_vid" not in result

    def test_empty_input_returns_empty_set_without_query(self, db):
        """Empty input → empty set, no DB query executed."""
        instance, mock_cursor = db

        result = instance.get_processed_video_ids([])

        assert result == set()
        mock_cursor.execute.assert_not_called()

    def test_sql_includes_is_processed_condition(self, db):
        """The SQL must still include is_processed = TRUE as one branch."""
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_processed_video_ids(["check_sql"])

        executed_sqls = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("is_processed" in sql for sql in executed_sqls), (
            "get_processed_video_ids must still include is_processed condition"
        )
