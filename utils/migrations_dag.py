"""
Schema Migrations DAG

Discovers and applies pending SQL migrations across all projects.
Tracks applied migrations in a schema_migrations table to ensure idempotency.

Migrations are discovered from */sql/migrations/*.sql paths relative to the DAG repo root.
They are applied in alphabetical order — numeric prefixes (001_, 002_, ...) control sequencing.

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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
}


def _ensure_migrations_table() -> None:
    pg = PostgresConnection()
    schema = pg.schema
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {schema}.schema_migrations (
                    id           SERIAL PRIMARY KEY,
                    migration    VARCHAR(500) NOT NULL UNIQUE,
                    applied_at   TIMESTAMP    NOT NULL DEFAULT NOW()
                )
            """)
    logging.info("schema_migrations table ready in schema '%s'", schema)


def _apply_pending_migrations(**context) -> None:
    pg = PostgresConnection()
    schema = pg.schema

    migration_files = sorted(DAGS_REPO_PATH.glob('*/sql/migrations/*.sql'))

    if not migration_files:
        logging.info("No migration files found under %s", DAGS_REPO_PATH)
        return

    with pg.get_connection() as conn:
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
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
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
