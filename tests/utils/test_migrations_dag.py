"""Tests for utils/migrations_dag.py — DAG load, migration logic, and idempotency."""

from __future__ import annotations

import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

MIGRATIONS_DIR = Path(__file__).parents[2] / "congress_videos" / "sql" / "migrations"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_mock_pg(schema: str = "development", applied: list[str] | None = None):
    """
    Build a PostgresConnection mock.

    Returns (mock_pg, mock_cursor_write) where mock_cursor_write accumulates
    all execute() calls made during migration writes.
    """
    applied = applied or []

    mock_pg = MagicMock()
    mock_pg.schema = schema

    mock_cursor_read = MagicMock()
    mock_cursor_read.__enter__ = MagicMock(return_value=mock_cursor_read)
    mock_cursor_read.__exit__ = MagicMock(return_value=False)
    mock_cursor_read.fetchall.return_value = [{"migration": m} for m in applied]

    mock_conn_read = MagicMock()
    mock_conn_read.cursor.return_value = mock_cursor_read
    mock_conn_read.__enter__ = MagicMock(return_value=mock_conn_read)
    mock_conn_read.__exit__ = MagicMock(return_value=False)

    mock_cursor_write = MagicMock()
    mock_cursor_write.__enter__ = MagicMock(return_value=mock_cursor_write)
    mock_cursor_write.__exit__ = MagicMock(return_value=False)

    mock_conn_write = MagicMock()
    mock_conn_write.cursor.return_value = mock_cursor_write
    mock_conn_write.__enter__ = MagicMock(return_value=mock_conn_write)
    mock_conn_write.__exit__ = MagicMock(return_value=False)

    mock_pg.get_connection.side_effect = [mock_conn_read] + [mock_conn_write] * 10

    return mock_pg, mock_cursor_write


def _create_migration(base: Path, name: str, sql: str = "CREATE TABLE IF NOT EXISTS t (id SERIAL);") -> Path:
    path = base / "project" / "sql" / "migrations" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(sql)
    return path


# ---------------------------------------------------------------------------
# DAG load
# ---------------------------------------------------------------------------

class TestRunMigrationsDAGLoads:

    def test_dag_loads(self):
        from utils.migrations_dag import dag
        assert dag is not None
        assert dag.dag_id == "run_migrations"

    def test_dag_has_two_tasks(self):
        from utils.migrations_dag import dag
        assert len(dag.tasks) == 2

    def test_dag_schedule_is_none(self):
        from utils.migrations_dag import dag
        assert dag.schedule is None

    def test_ensure_runs_before_apply(self):
        from utils.migrations_dag import dag
        ensure = dag.get_task("ensure_migrations_table")
        apply = dag.get_task("apply_pending_migrations")
        assert apply in ensure.downstream_list


# ---------------------------------------------------------------------------
# _ensure_migrations_table
# ---------------------------------------------------------------------------

class TestEnsureMigrationsTable:

    def _mock_pg(self, mocker, schema: str = "development") -> MagicMock:
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_pg = MagicMock()
        mock_pg.schema = schema
        mock_pg.get_connection.return_value = mock_conn

        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        return mock_cursor

    def test_creates_schema_migrations_table(self, mocker):
        from utils.migrations_dag import _ensure_migrations_table

        mock_cursor = self._mock_pg(mocker)
        _ensure_migrations_table()

        sql = mock_cursor.execute.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "schema_migrations" in sql

    def test_uses_correct_schema(self, mocker):
        from utils.migrations_dag import _ensure_migrations_table

        mock_cursor = self._mock_pg(mocker, schema="production")
        _ensure_migrations_table()

        sql = mock_cursor.execute.call_args[0][0]
        assert "production.schema_migrations" in sql

    def test_tracking_table_has_required_columns(self, mocker):
        from utils.migrations_dag import _ensure_migrations_table

        mock_cursor = self._mock_pg(mocker)
        _ensure_migrations_table()

        sql = mock_cursor.execute.call_args[0][0]
        assert "migration" in sql
        assert "applied_at" in sql


# ---------------------------------------------------------------------------
# _apply_pending_migrations
# ---------------------------------------------------------------------------

class TestApplyPendingMigrations:

    def test_no_migration_files_skips_db(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg, _ = _make_mock_pg()
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        mock_pg.get_connection.assert_not_called()

    def test_already_applied_migration_is_skipped(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_init.sql")
        relative = "project/sql/migrations/001_init.sql"

        mock_pg, _ = _make_mock_pg(applied=[relative])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 1  # read only, no write

    def test_pending_migration_is_applied(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        migration_sql = "CREATE TABLE IF NOT EXISTS my_table (id SERIAL);"
        _create_migration(tmp_path, "001_init.sql", migration_sql)

        mock_pg, mock_cursor_write = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 2  # 1 read + 1 write

        executed = [c[0][0] for c in mock_cursor_write.execute.call_args_list]
        assert any("SET search_path" in s for s in executed)
        assert any(migration_sql in s for s in executed)
        assert any("INSERT INTO" in s and "schema_migrations" in s for s in executed)

    def test_applied_migration_recorded_with_relative_path(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_init.sql")

        mock_pg, mock_cursor_write = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        insert_call = next(
            c for c in mock_cursor_write.execute.call_args_list
            if "INSERT INTO" in c[0][0] and "schema_migrations" in c[0][0]
        )
        recorded_path = insert_call[0][1][0]
        assert recorded_path == "project/sql/migrations/001_init.sql"

    def test_each_migration_uses_separate_connection(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_a.sql")
        _create_migration(tmp_path, "002_b.sql")

        mock_pg, _ = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 3  # 1 read + 2 writes

    def test_mixed_applied_and_pending_only_runs_pending(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_done.sql")
        _create_migration(tmp_path, "002_pending.sql")

        mock_pg, _ = _make_mock_pg(applied=["project/sql/migrations/001_done.sql"])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 2  # 1 read + 1 write

    def test_migrations_applied_in_alphabetical_order(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "003_c.sql", "CREATE TABLE IF NOT EXISTS c (id SERIAL);")
        _create_migration(tmp_path, "001_a.sql", "CREATE TABLE IF NOT EXISTS a (id SERIAL);")
        _create_migration(tmp_path, "002_b.sql", "CREATE TABLE IF NOT EXISTS b (id SERIAL);")

        mock_pg, mock_cursor_write = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        insert_calls = [
            c for c in mock_cursor_write.execute.call_args_list
            if "INSERT INTO" in c[0][0] and "schema_migrations" in c[0][0]
        ]
        recorded = [c[0][1][0] for c in insert_calls]
        assert recorded == sorted(recorded)


# ---------------------------------------------------------------------------
# Idempotency: static analysis of real migration files
# ---------------------------------------------------------------------------

_BARE_CREATE_TABLE = re.compile(r'\bCREATE\s+TABLE\s+(?!IF\s+NOT\s+EXISTS\b)', re.IGNORECASE)
_BARE_CREATE_INDEX = re.compile(r'\bCREATE\s+(?:UNIQUE\s+)?INDEX\s+(?!IF\s+NOT\s+EXISTS\b)', re.IGNORECASE)
_BARE_DROP_TABLE = re.compile(r'\bDROP\s+TABLE\s+(?!IF\s+EXISTS\b)', re.IGNORECASE)
_BARE_INSERT = re.compile(r'\bINSERT\s+INTO\b', re.IGNORECASE)

_MIGRATION_FILES = sorted(MIGRATIONS_DIR.glob("*.sql"))


class TestMigrationIdempotency:

    def test_migration_files_exist(self):
        assert len(_MIGRATION_FILES) > 0, f"No .sql files found in {MIGRATIONS_DIR}"

    def test_008_video_chapters_unique_segment_exists_with_index(self):
        """Migration 008 must exist and create the unique segment index."""
        path = MIGRATIONS_DIR / "008_video_chapters_unique_segment.sql"
        assert path.exists(), f"Missing migration: {path}"
        sql = path.read_text()
        assert "uq_video_chapters_segment" in sql
        assert "CREATE UNIQUE INDEX IF NOT EXISTS" in sql
        assert "(video_id, start_time, end_time)" in sql

    def test_009_llm_cache_table_exists_and_idempotent(self):
        """Migration 009 must create the llm_cache table idempotently with a
        sha256 (CHAR(64)) primary key and use unqualified names (the runner
        sets search_path, so migrations must NOT schema-qualify tables)."""
        path = MIGRATIONS_DIR / "009_create_llm_cache.sql"
        assert path.exists(), f"Missing migration: {path}"
        sql = path.read_text()
        assert "CREATE TABLE IF NOT EXISTS llm_cache" in sql
        assert "cache_key" in sql
        assert "CHAR(64)" in sql
        assert "PRIMARY KEY" in sql
        # Unqualified — no schema prefix like "development.llm_cache".
        assert ".llm_cache" not in sql

    def test_011_chapter_upload_failure_tracking_exists_with_columns_and_view(self):
        """Migration 011 must exist and add failure-tracking columns + updated view."""
        path = MIGRATIONS_DIR / "011_add_chapter_upload_failure_tracking.sql"
        assert path.exists(), f"Missing migration: {path}"
        sql = path.read_text()
        assert "ADD COLUMN IF NOT EXISTS upload_attempts" in sql
        assert "ADD COLUMN IF NOT EXISTS is_upload_abandoned" in sql
        assert "ADD COLUMN IF NOT EXISTS last_upload_error" in sql
        assert "DROP VIEW IF EXISTS uploadable_chapters" in sql
        assert "is_upload_abandoned = FALSE" in sql

    @pytest.mark.parametrize("path", _MIGRATION_FILES, ids=lambda p: p.name)
    def test_no_bare_create_table(self, path: Path):
        sql = path.read_text()
        assert not _BARE_CREATE_TABLE.search(sql), (
            f"{path.name}: CREATE TABLE must use IF NOT EXISTS"
        )

    @pytest.mark.parametrize("path", _MIGRATION_FILES, ids=lambda p: p.name)
    def test_no_bare_create_index(self, path: Path):
        sql = path.read_text()
        assert not _BARE_CREATE_INDEX.search(sql), (
            f"{path.name}: CREATE INDEX must use IF NOT EXISTS"
        )

    @pytest.mark.parametrize("path", _MIGRATION_FILES, ids=lambda p: p.name)
    def test_no_bare_drop_table(self, path: Path):
        sql = path.read_text()
        assert not _BARE_DROP_TABLE.search(sql), (
            f"{path.name}: DROP TABLE must use IF EXISTS"
        )

    @pytest.mark.parametrize("path", _MIGRATION_FILES, ids=lambda p: p.name)
    def test_no_seed_inserts(self, path: Path):
        sql = path.read_text()
        assert not _BARE_INSERT.search(sql), (
            f"{path.name}: migrations must not INSERT data — use a separate seed script"
        )
