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
    all execute() calls made during migration writes. The FIRST connection
    handed out by `get_connection` is always the advisory-lock connection
    (`mock_pg._lock_cursor` / `mock_pg._lock_conn` expose it for lock-specific
    assertions) — every migration-application connection comes after it.
    """
    applied = applied or []

    mock_pg = MagicMock()
    mock_pg.schema = schema

    mock_cursor_lock = MagicMock()
    mock_cursor_lock.__enter__ = MagicMock(return_value=mock_cursor_lock)
    mock_cursor_lock.__exit__ = MagicMock(return_value=False)

    mock_conn_lock = MagicMock()
    mock_conn_lock.cursor.return_value = mock_cursor_lock
    mock_conn_lock.__enter__ = MagicMock(return_value=mock_conn_lock)
    mock_conn_lock.__exit__ = MagicMock(return_value=False)

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

    mock_pg.get_connection.side_effect = (
        [mock_conn_lock, mock_conn_read] + [mock_conn_write] * 10
    )
    mock_pg._lock_conn = mock_conn_lock
    mock_pg._lock_cursor = mock_cursor_lock
    mock_pg._write_conn = mock_conn_write

    # Owner-role CREATE preflight (issue #310): default to "granted" so tests
    # not exercising the preflight itself don't have to configure it.
    mock_cursor_write.fetchone.return_value = {"has_create": True}

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
        assert dag.schedule_interval is None

    def test_ensure_runs_before_apply(self):
        from utils.migrations_dag import dag
        ensure = dag.get_task("ensure_migrations_table")
        apply = dag.get_task("apply_pending_migrations")
        assert apply in ensure.downstream_list


# ---------------------------------------------------------------------------
# _migration_connection (issue #203 — dedicated DDL migration role)
# ---------------------------------------------------------------------------

class TestMigrationCredentialResolution:

    def test_both_env_vars_set_overrides_credentials(self, mocker, monkeypatch):
        """Both MIGRATION_POSTGRES_USER/PASSWORD set -> pg.user/password overridden."""
        from utils.migrations_dag import _migration_connection

        monkeypatch.setenv("MIGRATION_POSTGRES_USER", "airflow_migrations")
        monkeypatch.setenv("MIGRATION_POSTGRES_PASSWORD", "migration-secret")

        mock_pg = MagicMock()
        mock_pg.user = "airflow"
        mock_pg.password = "airflow-pw"
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)

        result = _migration_connection()

        assert result.user == "airflow_migrations"
        assert result.password == "migration-secret"

    def test_neither_env_var_set_falls_back_to_default(self, mocker, monkeypatch):
        """Neither env var set (current NAS default) -> credentials untouched."""
        from utils.migrations_dag import _migration_connection

        monkeypatch.delenv("MIGRATION_POSTGRES_USER", raising=False)
        monkeypatch.delenv("MIGRATION_POSTGRES_PASSWORD", raising=False)

        mock_pg = MagicMock()
        mock_pg.user = "airflow"
        mock_pg.password = "airflow-pw"
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)

        result = _migration_connection()

        assert result.user == "airflow"
        assert result.password == "airflow-pw"

    def test_only_user_set_falls_back_to_default(self, mocker, monkeypatch):
        """Only MIGRATION_POSTGRES_USER set (no password) -> no partial override."""
        from utils.migrations_dag import _migration_connection

        monkeypatch.setenv("MIGRATION_POSTGRES_USER", "airflow_migrations")
        monkeypatch.delenv("MIGRATION_POSTGRES_PASSWORD", raising=False)

        mock_pg = MagicMock()
        mock_pg.user = "airflow"
        mock_pg.password = "airflow-pw"
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)

        result = _migration_connection()

        assert result.user == "airflow"
        assert result.password == "airflow-pw"

    def test_only_password_set_falls_back_to_default(self, mocker, monkeypatch):
        """Only MIGRATION_POSTGRES_PASSWORD set (no user) -> no partial override."""
        from utils.migrations_dag import _migration_connection

        monkeypatch.delenv("MIGRATION_POSTGRES_USER", raising=False)
        monkeypatch.setenv("MIGRATION_POSTGRES_PASSWORD", "migration-secret")

        mock_pg = MagicMock()
        mock_pg.user = "airflow"
        mock_pg.password = "airflow-pw"
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)

        result = _migration_connection()

        assert result.user == "airflow"
        assert result.password == "airflow-pw"


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
        """No migration files: the advisory lock is still acquired (session
        span covers the whole function), but no read/write connection opens."""
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg, _ = _make_mock_pg()
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 1  # lock only, no read/write

    def test_already_applied_migration_is_skipped(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_init.sql")
        relative = "project/sql/migrations/001_init.sql"

        mock_pg, _ = _make_mock_pg(applied=[relative])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 2  # lock + read only, no write

    def test_pending_migration_is_applied(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        migration_sql = "CREATE TABLE IF NOT EXISTS my_table (id SERIAL);"
        _create_migration(tmp_path, "001_init.sql", migration_sql)

        mock_pg, mock_cursor_write = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 3  # lock + 1 read + 1 write

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

        assert mock_pg.get_connection.call_count == 4  # lock + 1 read + 2 writes

    def test_mixed_applied_and_pending_only_runs_pending(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_done.sql")
        _create_migration(tmp_path, "002_pending.sql")

        mock_pg, _ = _make_mock_pg(applied=["project/sql/migrations/001_done.sql"])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg.get_connection.call_count == 3  # lock + 1 read + 1 write

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
# Advisory lock (issue #209) — schema-scoped, session-spanning
# ---------------------------------------------------------------------------

class TestAdvisoryLock:

    def test_lock_acquired_on_first_connection_before_any_file_applies(self, mocker, tmp_path):
        """The advisory lock connection must be the FIRST one opened, and its
        pg_advisory_lock call must happen before any migration read/write."""
        from utils.migrations_dag import _apply_pending_migrations

        _create_migration(tmp_path, "001_init.sql")
        mock_pg, _ = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        # The lock connection (first item handed out by get_connection's
        # side_effect list) must have received the pg_advisory_lock call.
        lock_calls = [
            c for c in mock_pg._lock_cursor.execute.call_args_list
            if "pg_advisory_lock" in c[0][0]
        ]
        assert len(lock_calls) == 1

    def test_lock_timeout_set_to_zero_on_lock_connection(self, mocker, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg, _ = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        executed = [c[0][0] for c in mock_pg._lock_cursor.execute.call_args_list]
        assert any("SET lock_timeout = 0" in s for s in executed)

    def test_lock_connection_autocommit_enabled(self, mocker, tmp_path):
        """D7: autocommit avoids an idle-in-transaction session for the run."""
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg, _ = _make_mock_pg(applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        assert mock_pg._lock_conn.autocommit is True

    def test_schema_passed_as_query_parameter_not_fstring(self, mocker, tmp_path):
        """The schema name must never be interpolated directly into the SQL
        string — it must travel as a bound parameter to hashtext()."""
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg, _ = _make_mock_pg(schema="development", applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        lock_call = next(
            c for c in mock_pg._lock_cursor.execute.call_args_list
            if "pg_advisory_lock" in c[0][0]
        )
        sql_text, params = lock_call[0][0], lock_call[0][1]
        assert "development" not in sql_text
        assert "development" in params

    def test_two_different_schemas_pass_different_key_params(self, mocker, tmp_path):
        """Same namespace, different schema string ⇒ different hashtext() input
        (dev and prod share one Postgres instance but must not block each other)."""
        from utils.migrations_dag import _apply_pending_migrations

        mock_pg_dev, _ = _make_mock_pg(schema="development", applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg_dev)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)
        _apply_pending_migrations()

        mock_pg_prod, _ = _make_mock_pg(schema="production", applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg_prod)
        _apply_pending_migrations()

        dev_call = next(
            c for c in mock_pg_dev._lock_cursor.execute.call_args_list
            if "pg_advisory_lock" in c[0][0]
        )
        prod_call = next(
            c for c in mock_pg_prod._lock_cursor.execute.call_args_list
            if "pg_advisory_lock" in c[0][0]
        )
        assert dev_call[0][1] != prod_call[0][1]
        assert dev_call[0][1][1] == "development"
        assert prod_call[0][1][1] == "production"
        # Same namespace constant on both
        assert dev_call[0][1][0] == prod_call[0][1][0]


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

    def test_012_short_upload_failure_tracking_exists_with_columns(self):
        """Migration 012 must exist and add failure-tracking columns to video_shorts."""
        path = MIGRATIONS_DIR / "012_add_short_upload_failure_tracking.sql"
        assert path.exists(), f"Missing migration: {path}"
        sql = path.read_text()
        assert "ADD COLUMN IF NOT EXISTS upload_attempts" in sql
        assert "ADD COLUMN IF NOT EXISTS is_upload_abandoned" in sql
        assert "ADD COLUMN IF NOT EXISTS last_upload_error" in sql
        assert "COMMENT ON COLUMN video_shorts." in sql

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


# ---------------------------------------------------------------------------
# Owner-role assumption (issue #291)
# ---------------------------------------------------------------------------

class TestOwnerRoleForSchema:

    def test_development_maps_to_airflow_dev(self, monkeypatch):
        from utils.migrations_dag import _owner_role_for_schema

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        assert _owner_role_for_schema("development") == "airflow_dev"

    def test_production_maps_to_airflow_prod(self, monkeypatch):
        from utils.migrations_dag import _owner_role_for_schema

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        assert _owner_role_for_schema("production") == "airflow_prod"

    def test_unknown_schema_maps_to_none(self, monkeypatch):
        from utils.migrations_dag import _owner_role_for_schema

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        assert _owner_role_for_schema("public") is None

    def test_env_override_wins(self, monkeypatch):
        from utils.migrations_dag import _owner_role_for_schema

        monkeypatch.setenv("MIGRATION_OWNER_ROLE", "custom_owner")
        assert _owner_role_for_schema("development") == "custom_owner"
        assert _owner_role_for_schema("public") == "custom_owner"


class TestOwnerRoleAssumption:

    def _mock_single_conn(self, mocker, schema: str):
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        # Owner-role CREATE preflight (issue #310): default to "granted" so
        # tests not exercising the preflight itself don't have to configure it.
        mock_cursor.fetchone.return_value = {"has_create": True}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_pg = MagicMock()
        mock_pg.schema = schema
        mock_pg.get_connection.return_value = mock_conn

        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        return mock_conn, mock_cursor

    def test_ensure_table_assumes_owner_role_first(self, mocker, monkeypatch):
        from utils.migrations_dag import _ensure_migrations_table

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        _, mock_cursor = self._mock_single_conn(mocker, "development")
        _ensure_migrations_table()

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert executed[0] == 'SET ROLE "airflow_dev"'
        assert "CREATE TABLE IF NOT EXISTS" in executed[2]

    def test_apply_sets_role_before_search_path_and_sql(self, mocker, monkeypatch, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        migration_sql = "CREATE VIEW v AS SELECT 1;"
        _create_migration(tmp_path, "001_init.sql", migration_sql)

        mock_pg, mock_cursor_write = _make_mock_pg(schema="production", applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        executed = [c[0][0] for c in mock_cursor_write.execute.call_args_list]
        assert executed[0] == 'SET ROLE "airflow_prod"'
        assert "SET search_path" in executed[2]
        assert migration_sql in executed[3]

    def test_set_role_failure_rolls_back_and_migration_still_applies(
        self, mocker, monkeypatch, tmp_path
    ):
        from utils.migrations_dag import _apply_pending_migrations

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        migration_sql = "CREATE TABLE IF NOT EXISTS t (id SERIAL);"
        _create_migration(tmp_path, "001_init.sql", migration_sql)

        mock_pg, mock_cursor_write = _make_mock_pg(schema="development", applied=[])

        def _fail_set_role(sql, *args, **kwargs):
            if sql.startswith("SET ROLE"):
                raise RuntimeError("permission denied to set role")

        mock_cursor_write.execute.side_effect = _fail_set_role
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        executed = [c[0][0] for c in mock_cursor_write.execute.call_args_list]
        assert any(migration_sql in s for s in executed)
        assert any("INSERT INTO" in s and "schema_migrations" in s for s in executed)
        # The poisoned transaction must be rolled back before the migration
        # statements run on that same connection (psycopg2 aborts the tx on
        # error) — and never on the advisory-lock connection.
        assert mock_pg._write_conn.rollback.called
        assert not mock_pg._lock_conn.rollback.called
        # The CREATE preflight (issue #310) must never run on the fail-soft
        # path — SET ROLE never succeeded, so there is no assumed role to check.
        assert not any("has_schema_privilege" in s for s in executed)

    def test_unknown_schema_issues_no_set_role(self, mocker, monkeypatch, tmp_path):
        from utils.migrations_dag import _apply_pending_migrations

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        _create_migration(tmp_path, "001_init.sql")

        mock_pg, mock_cursor_write = _make_mock_pg(schema="public", applied=[])
        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        mocker.patch("utils.migrations_dag.DAGS_REPO_PATH", tmp_path)

        _apply_pending_migrations()

        executed = [c[0][0] for c in mock_cursor_write.execute.call_args_list]
        assert not any(s.startswith("SET ROLE") for s in executed)
        # No role mapped → the CREATE preflight query is never issued either.
        assert not any("has_schema_privilege" in s for s in executed)


class TestOwnerRoleCreatePreflight:
    """Issue #310: after a successful SET ROLE, the assumed owner role must
    actually hold CREATE on its schema — checked via has_schema_privilege."""

    def _mock_single_conn(self, mocker, schema: str):
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchone.return_value = {"has_create": True}

        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_pg = MagicMock()
        mock_pg.schema = schema
        mock_pg.get_connection.return_value = mock_conn

        mocker.patch("utils.migrations_dag.PostgresConnection", return_value=mock_pg)
        return mock_conn, mock_cursor

    def test_raises_when_create_is_missing(self, mocker, monkeypatch):
        from utils.migrations_dag import MigrationPrivilegeError, _ensure_migrations_table

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        _, mock_cursor = self._mock_single_conn(mocker, "production")
        mock_cursor.fetchone.return_value = {"has_create": False}

        with pytest.raises(MigrationPrivilegeError) as exc_info:
            _ensure_migrations_table()

        message = str(exc_info.value)
        assert "airflow_prod" in message
        assert "production" in message
        assert "GRANT CREATE ON SCHEMA production TO airflow_prod;" in message

    def test_does_not_raise_when_create_is_granted(self, mocker, monkeypatch):
        from utils.migrations_dag import _ensure_migrations_table

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        _, mock_cursor = self._mock_single_conn(mocker, "development")
        mock_cursor.fetchone.return_value = {"has_create": True}

        _ensure_migrations_table()  # must not raise

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        assert any("has_schema_privilege" in s for s in executed)
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in executed)

    def test_preflight_runs_after_set_role_not_inside_its_try(self, mocker, monkeypatch):
        """The preflight query must be issued strictly after SET ROLE — never
        swallowed by the SET ROLE fail-soft except block."""
        from utils.migrations_dag import _ensure_migrations_table

        monkeypatch.delenv("MIGRATION_OWNER_ROLE", raising=False)
        _, mock_cursor = self._mock_single_conn(mocker, "development")
        mock_cursor.fetchone.return_value = {"has_create": True}

        _ensure_migrations_table()

        executed = [c[0][0] for c in mock_cursor.execute.call_args_list]
        set_role_index = next(i for i, s in enumerate(executed) if s.startswith("SET ROLE"))
        preflight_index = next(i for i, s in enumerate(executed) if "has_schema_privilege" in s)
        assert preflight_index > set_role_index
