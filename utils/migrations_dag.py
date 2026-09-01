"""
Schema Migrations DAG

Discovers and applies pending SQL migrations across all projects.
Tracks applied migrations in a schema_migrations table to ensure idempotency.

Migrations are discovered from */sql/migrations/*.sql paths relative to the DAG repo root.
They are applied in alphabetical order — numeric prefixes (001_, 002_, ...) control sequencing.

Numbering gaps (001-003, 014 under congress_videos/sql/migrations/) are expected,
not missing files: 001-003 predate this numbered-migration convention (the
original schema was hand-applied), and 014 was allocated then abandoned before
any commit. Do not "fill" a gap by reusing a skipped number for a new migration.

CREATE INDEX CONCURRENTLY cannot run through this runner: every migration file
executes inside the implicit transaction opened by its own `get_connection()`
call (see _apply_pending_migrations), and CONCURRENTLY requires running
outside any transaction block. Use a plain `CREATE INDEX IF NOT EXISTS`
instead (precedent: migration 020) and, for large tables, schedule the
`run_migrations` DAG trigger during the 14:00-20:00 UTC NAS quiet window.

Concurrency: _apply_pending_migrations holds a pg_advisory_lock scoped to the
target schema (hashtext(schema)) for the whole function, on a dedicated
autocommit connection with lock_timeout disabled. Two runs against the SAME
schema serialize; runs against DIFFERENT schemas (dev vs prod share one
Postgres instance) never block each other.

Trigger: manual only (schedule=None).
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.env_loader import load_env_if_local
from utils.postgres_helpers import PostgresConnection

load_env_if_local()

DAGS_REPO_PATH = Path(os.getenv('AIRFLOW__CORE__DAGS_FOLDER', '/opt/airflow/dags/repo'))

# Two-int pg_advisory_lock() namespace, ASCII 'MIGR' packed into 32 bits
# (< 2**31, so it fits the signed int4 the two-arg advisory-lock form takes).
# Combined with hashtext(schema) as the second key, this scopes the lock to
# the target schema — dev and prod share one Postgres instance but must
# never block each other (issue #209).
_MIGRATION_LOCK_NAMESPACE = 0x4D494752

# Dedicated DDL migration role (issue #203) — least-privilege split from the
# per-environment runtime role. Both-or-neither: a partially configured stack
# must never authenticate as a half-provisioned role.
MIGRATION_USER_ENV = 'MIGRATION_POSTGRES_USER'
MIGRATION_PASSWORD_ENV = 'MIGRATION_POSTGRES_PASSWORD'

# Owner role per target schema (issue #291). DDL executed by the dedicated
# migration role must create objects owned by the stack's runtime-owner role —
# otherwise every DROP/CREATE in a migration (e.g. 040's uploadable_turns
# re-declaration) leaves the object owned by airflow_migrations and the
# runtime role loses access until a manual ALTER ... OWNER TO. SET ROLE here
# relies on the membership grants airflow_dev/airflow_prod -> airflow_migrations
# provisioned on 2026-09-01.
SCHEMA_OWNER_ROLES = {
    'development': 'airflow_dev',
    'production': 'airflow_prod',
}
MIGRATION_OWNER_ROLE_ENV = 'MIGRATION_OWNER_ROLE'


def _owner_role_for_schema(schema: str) -> str | None:
    """Owner role to assume for DDL against *schema*; env override wins."""
    return os.getenv(MIGRATION_OWNER_ROLE_ENV) or SCHEMA_OWNER_ROLES.get(schema)


def _assume_owner_role(conn, cur, schema: str) -> None:
    """SET ROLE to the schema's owner role; fail-soft when not assumable.

    Must be the FIRST statement on a fresh connection: psycopg2 aborts the
    open transaction on any error, so a failed SET ROLE is rolled back here
    before the caller issues further statements on the same connection.
    Failure is logged and tolerated — local/dev environments without the
    role (or without the membership grant) keep the pre-#291 behavior of
    creating objects as the connection role.
    """
    role = _owner_role_for_schema(schema)
    if not role:
        logging.info(
            "No owner role mapped for schema '%s' — running as connection role", schema
        )
        return
    try:
        cur.execute(f'SET ROLE "{role}"')
        logging.info("Assumed owner role '%s' for schema '%s'", role, schema)
    except Exception:
        conn.rollback()
        logging.warning(
            "SET ROLE %s failed — continuing as connection role", role, exc_info=True
        )


def _migration_connection() -> PostgresConnection:
    """PostgresConnection bound to the DDL migration role when fully provisioned.

    Falls back to the standard POSTGRES_USER/PASSWORD role (utils/postgres_helpers.py)
    when MIGRATION_POSTGRES_USER/PASSWORD are unset or only partially set — this keeps
    the current NAS default (no migration role provisioned yet) working unchanged.
    """
    pg = PostgresConnection()
    user = os.getenv(MIGRATION_USER_ENV)
    password = os.getenv(MIGRATION_PASSWORD_ENV)
    if user and password:
        pg.user, pg.password = user, password
        logging.info("Migrations using dedicated role '%s'", user)
    else:
        logging.info(
            "%s/%s unset or partial — using POSTGRES_USER", MIGRATION_USER_ENV, MIGRATION_PASSWORD_ENV
        )
    return pg


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}


def _ensure_migrations_table() -> None:
    pg = _migration_connection()
    schema = pg.schema
    with pg.get_connection(statement_timeout_ms=0) as conn:
        with conn.cursor() as cur:
            _assume_owner_role(conn, cur, schema)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
                    id           SERIAL PRIMARY KEY,
                    migration    VARCHAR(500) NOT NULL UNIQUE,
                    applied_at   TIMESTAMP    NOT NULL DEFAULT NOW()
                )
            """)
    logging.info("schema_migrations table ready in schema '%s'", schema)


def _apply_pending_migrations(**context) -> None:
    pg = _migration_connection()
    schema = pg.schema

    # Schema-scoped advisory lock spanning the ENTIRE function, held on a
    # dedicated connection (issue #209). Dev and prod share one Postgres
    # instance but write to separate schemas — a git_sync-triggered run
    # racing a manually-triggered run against the SAME schema must serialize;
    # runs against DIFFERENT schemas must never wait on each other.
    with pg.get_connection(statement_timeout_ms=0) as lock_conn:
        # Autocommit: this connection issues no DDL/DML of its own, so there is
        # no transaction to keep open — avoids an idle-in-transaction session
        # for the whole run (migration runs have been observed to take ~12min).
        lock_conn.autocommit = True
        with lock_conn.cursor() as cur:
            # The default per-connection lock_timeout (5s, set via connection
            # options) would make a blocked pg_advisory_lock call ERROR after
            # 5s instead of waiting — override it so the second concurrent run
            # blocks until the first releases the lock.
            cur.execute("SET lock_timeout = 0")
            cur.execute(
                "SELECT pg_advisory_lock(%s, hashtext(%s))",
                (_MIGRATION_LOCK_NAMESPACE, schema),
            )
        logging.info(
            "Acquired migration advisory lock for schema '%s' (namespace=%d)",
            schema, _MIGRATION_LOCK_NAMESPACE,
        )

        migration_files = sorted(DAGS_REPO_PATH.glob('*/sql/migrations/*.sql'))

        if not migration_files:
            logging.info("No migration files found under %s", DAGS_REPO_PATH)
            return

        with pg.get_connection(statement_timeout_ms=0) as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT migration FROM {schema}.schema_migrations")
                applied = {row['migration'] for row in cur.fetchall()}

        logging.info(
            "Found %d migration file(s), %d already applied",
            len(migration_files), len(applied),
        )

        applied_count = 0
        for path in migration_files:
            relative = str(path.relative_to(DAGS_REPO_PATH))

            if relative in applied:
                logging.info("Skip (already applied): %s", relative)
                continue

            logging.info("Applying: %s", relative)
            sql = path.read_text()

            # Each migration runs in its own transaction — partial progress is preserved on failure
            with pg.get_connection(statement_timeout_ms=0) as conn:
                with conn.cursor() as cur:
                    _assume_owner_role(conn, cur, schema)
                    cur.execute(f"SET search_path TO {schema}, public")
                    cur.execute(sql)
                    cur.execute(
                        f"INSERT INTO {schema}.schema_migrations (migration) VALUES (%s)",
                        (relative,),
                    )

            logging.info("Applied: %s", relative)
            applied_count += 1

        logging.info(
            "Migrations complete — %d applied, %d skipped",
            applied_count, len(applied),
        )
    # Lock release is implicit: pg.get_connection's context manager closes
    # this session on any exit path, and a session-scoped advisory lock dies
    # with the socket.


with DAG(
    'run_migrations',
    default_args=default_args,
    description='Apply pending SQL migrations across all projects',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['maintenance', 'migrations'],
) as dag:

    t1 = PythonOperator(
        task_id='ensure_migrations_table',
        python_callable=_ensure_migrations_table,
    )

    t2 = PythonOperator(
        task_id='apply_pending_migrations',
        python_callable=_apply_pending_migrations,
    )

    t1 >> t2
