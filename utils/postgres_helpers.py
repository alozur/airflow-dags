# utils/postgres_helpers.py
import os
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import RealDictCursor

from utils.env_loader import load_env_if_local

load_env_if_local()

# Server-side circuit breakers for the shared NAS Postgres (max_connections=100).
# Overridable per deployment via environment variables.
DEFAULT_STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_LOCK_TIMEOUT_MS = 5_000
DEFAULT_CONNECT_TIMEOUT_S = 10


class PostgresConnection:
    """Manages PostgreSQL connection using environment variables from .env"""

    def __init__(self):
        self.host = os.getenv("POSTGRES_HOST")
        self.port = os.getenv("POSTGRES_PORT")
        self.database = os.getenv("POSTGRES_DB")
        self.user = os.getenv("POSTGRES_USER")
        self.password = os.getenv("POSTGRES_PASSWORD")
        self.schema = os.getenv("POSTGRES_SCHEMA", "public")  # Default to 'public' if not set

        # Validate required environment variables
        required_vars = ["POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER", "POSTGRES_PASSWORD"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]

        if missing_vars:
            raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")

    @contextmanager
    def get_connection(self, statement_timeout_ms: int | None = None):
        """Yield a database connection, committing on success and always closing.

        psycopg2's own ``with conn:`` only manages the transaction — it never
        closes the socket, so every call used to leak a server connection until
        garbage collection. This context manager owns the full lifecycle:
        commit on clean exit, rollback on exception, close always.

        Args:
            statement_timeout_ms: Per-statement server timeout in milliseconds.
                ``None`` (default) reads ``POSTGRES_STATEMENT_TIMEOUT_MS``
                (fallback 30000). ``0`` disables the timeout — required for
                long-running DDL such as migrations.
        """
        if statement_timeout_ms is None:
            statement_timeout_ms = int(os.getenv("POSTGRES_STATEMENT_TIMEOUT_MS", DEFAULT_STATEMENT_TIMEOUT_MS))
        lock_timeout_ms = int(os.getenv("POSTGRES_LOCK_TIMEOUT_MS", DEFAULT_LOCK_TIMEOUT_MS))
        connect_timeout_s = int(os.getenv("POSTGRES_CONNECT_TIMEOUT_S", DEFAULT_CONNECT_TIMEOUT_S))

        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password,
            cursor_factory=RealDictCursor,
            connect_timeout=connect_timeout_s,
            options=(f"-c statement_timeout={int(statement_timeout_ms)} -c lock_timeout={int(lock_timeout_ms)}"),
            application_name=f"airflow-{self.schema}",
        )
        try:
            yield conn
            if not conn.closed:
                conn.commit()
        except Exception:
            if not conn.closed:
                conn.rollback()
            raise
        finally:
            conn.close()

    def get_qualified_table(self, table_name: str) -> str:
        """
        Return schema-qualified table name.

        Args:
            table_name: The table name without schema

        Returns:
            Schema-qualified table name (e.g., 'development.congressional_sessions')
        """
        return f"{self.schema}.{table_name}"
