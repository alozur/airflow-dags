"""Prove the DAG code can talk to the application database as the runtime role.

Runs inside the scheduler with the same POSTGRES_* environment the DAGs use
(POSTGRES_USER=airflow_dev, least-privilege DML-only role). Prints counts and
booleans only, never row content or credentials.
"""

import os
import sys
from pathlib import Path

DAGS_REPO_PATH = Path(os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags/repo"))
sys.path.insert(0, str(DAGS_REPO_PATH))

from utils.postgres_helpers import PostgresConnection  # noqa: E402


def main():
    pg = PostgresConnection()
    expected_migrations = len(list(DAGS_REPO_PATH.glob("*/sql/migrations/*.sql")))

    with pg.get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT current_user AS role_name")
        current_user = cur.fetchone()["role_name"]
        assert current_user == "airflow_dev", f"Unexpected runtime role: {current_user}"

        cur.execute("SELECT rolsuper FROM pg_roles WHERE rolname = %s", (current_user,))
        assert cur.fetchone()["rolsuper"] is False, "Runtime role must never be a superuser"

        cur.execute("SELECT has_schema_privilege(%s, 'development', 'USAGE') AS has_usage", (current_user,))
        assert cur.fetchone()["has_usage"] is True, "Runtime role lacks USAGE on schema development"

        cur.execute("SELECT count(*) AS count FROM development.schema_migrations")
        applied_migrations = cur.fetchone()["count"]
        assert applied_migrations == expected_migrations, (
            f"schema_migrations has {applied_migrations} rows, expected {expected_migrations} files"
        )

        cur.execute("SELECT count(*) AS count FROM pg_tables WHERE schemaname = 'development'")
        table_count = cur.fetchone()["count"]
        assert table_count > 0, "Schema development has no tables after migrations"

        # DML round-trip proof, rolled back on purpose: never leaves data behind.
        cur.execute("INSERT INTO development.schema_migrations (migration) VALUES (%s)", ("app_smoke_probe",))
        conn.rollback()

    print(
        f"application-db smoke: user={current_user} superuser=false schema_usage=true "
        f"migrations_applied={applied_migrations}/{expected_migrations} tables={table_count} dml_round_trip=ok"
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # noqa: BLE001 - report a short reason only
        print(f"app_smoke=failed reason={type(error).__name__}: {error}")
        sys.exit(1)
