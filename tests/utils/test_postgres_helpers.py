"""Tests for utils.postgres_helpers module."""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# PostgresConnection.__init__
# ---------------------------------------------------------------------------

class TestPostgresConnectionInit:

    def test_initialises_from_env_vars(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "db.example.com")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "admin")
        monkeypatch.setenv("POSTGRES_PASSWORD", "secret")
        monkeypatch.setenv("POSTGRES_SCHEMA", "myschema")

        from utils.postgres_helpers import PostgresConnection
        conn = PostgresConnection()

        assert conn.host == "db.example.com"
        assert conn.port == "5433"
        assert conn.database == "mydb"
        assert conn.user == "admin"
        assert conn.password == "secret"
        assert conn.schema == "myschema"

    def test_schema_defaults_to_public_when_not_set(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")
        monkeypatch.delenv("POSTGRES_SCHEMA", raising=False)

        from utils.postgres_helpers import PostgresConnection
        conn = PostgresConnection()
        assert conn.schema == "public"

    def test_raises_when_host_missing(self, monkeypatch):
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")

        from utils.postgres_helpers import PostgresConnection
        with pytest.raises(ValueError, match="POSTGRES_HOST"):
            PostgresConnection()

    def test_raises_when_password_missing(self, monkeypatch):
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.delenv("POSTGRES_PASSWORD", raising=False)

        from utils.postgres_helpers import PostgresConnection
        with pytest.raises(ValueError, match="POSTGRES_PASSWORD"):
            PostgresConnection()

    @pytest.mark.parametrize("missing_var", [
        "POSTGRES_HOST",
        "POSTGRES_PORT",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
    ])
    def test_raises_for_each_required_var_missing(self, monkeypatch, missing_var: str):
        required = {
            "POSTGRES_HOST": "localhost",
            "POSTGRES_PORT": "5432",
            "POSTGRES_DB": "mydb",
            "POSTGRES_USER": "user",
            "POSTGRES_PASSWORD": "pass",
        }
        for key, val in required.items():
            monkeypatch.setenv(key, val)
        monkeypatch.delenv(missing_var, raising=False)

        from utils.postgres_helpers import PostgresConnection
        with pytest.raises(ValueError):
            PostgresConnection()

    def test_error_message_lists_all_missing_vars(self, monkeypatch):
        monkeypatch.delenv("POSTGRES_HOST", raising=False)
        monkeypatch.delenv("POSTGRES_PORT", raising=False)
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")

        from utils.postgres_helpers import PostgresConnection
        with pytest.raises(ValueError) as exc_info:
            PostgresConnection()

        error_message = str(exc_info.value)
        assert "POSTGRES_HOST" in error_message
        assert "POSTGRES_PORT" in error_message


# ---------------------------------------------------------------------------
# PostgresConnection.get_qualified_table
# ---------------------------------------------------------------------------

class TestGetQualifiedTable:

    def _make_conn(self, monkeypatch, schema: str = "myschema") -> object:
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")
        monkeypatch.setenv("POSTGRES_SCHEMA", schema)
        from utils.postgres_helpers import PostgresConnection
        return PostgresConnection()

    def test_returns_schema_dot_table(self, monkeypatch):
        conn = self._make_conn(monkeypatch, schema="development")
        result = conn.get_qualified_table("congressional_sessions")
        assert result == "development.congressional_sessions"

    def test_public_schema(self, monkeypatch):
        conn = self._make_conn(monkeypatch, schema="public")
        result = conn.get_qualified_table("users")
        assert result == "public.users"

    @pytest.mark.parametrize("schema,table,expected", [
        ("dev", "sessions", "dev.sessions"),
        ("prod", "videos", "prod.videos"),
        ("test_schema", "congress", "test_schema.congress"),
    ])
    def test_various_schema_and_table_combinations(
        self, monkeypatch, schema: str, table: str, expected: str
    ):
        conn = self._make_conn(monkeypatch, schema=schema)
        assert conn.get_qualified_table(table) == expected


# ---------------------------------------------------------------------------
# PostgresConnection.get_connection
# ---------------------------------------------------------------------------

class TestGetConnection:

    def test_calls_psycopg2_connect_with_correct_args(self, monkeypatch, mock_psycopg2_connection):
        monkeypatch.setenv("POSTGRES_HOST", "db.host.io")
        monkeypatch.setenv("POSTGRES_PORT", "5433")
        monkeypatch.setenv("POSTGRES_DB", "congress_db")
        monkeypatch.setenv("POSTGRES_USER", "congress_user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "s3cr3t")
        monkeypatch.setenv("POSTGRES_SCHEMA", "public")

        mock_connect, mock_conn, _ = mock_psycopg2_connection

        from utils.postgres_helpers import PostgresConnection
        pg = PostgresConnection()
        result = pg.get_connection()

        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args.kwargs
        assert call_kwargs["host"] == "db.host.io"
        assert call_kwargs["port"] == "5433"
        assert call_kwargs["database"] == "congress_db"
        assert call_kwargs["user"] == "congress_user"
        assert call_kwargs["password"] == "s3cr3t"

    def test_get_connection_returns_connection_object(self, monkeypatch, mock_psycopg2_connection):
        monkeypatch.setenv("POSTGRES_HOST", "localhost")
        monkeypatch.setenv("POSTGRES_PORT", "5432")
        monkeypatch.setenv("POSTGRES_DB", "mydb")
        monkeypatch.setenv("POSTGRES_USER", "user")
        monkeypatch.setenv("POSTGRES_PASSWORD", "pass")

        mock_connect, mock_conn, _ = mock_psycopg2_connection

        from utils.postgres_helpers import PostgresConnection
        pg = PostgresConnection()
        result = pg.get_connection()

        assert result is mock_conn
