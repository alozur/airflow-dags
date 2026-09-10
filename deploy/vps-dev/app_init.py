"""Provision the isolated application database once per release.

Step A runs as the bootstrap superuser (`airflow`, the same legacy role name
grant_permissions*.sql expects on the NAS): create the target schema
(POSTGRES_SCHEMA — `development` or `production`), apply the idempotent
role/grant script matching that schema verbatim, set the runtime and
migration role passwords from the container variables Compose interpolates
(POSTGRES_PASSWORD, MIGRATION_POSTGRES_PASSWORD), create the base tables from
the schema files on a fresh DEV database only (production always arrives
pre-populated via pg_restore from the NAS — see _bootstrap), and hand every
object in the schema to the runtime owner role so migrations (which SET ROLE
to it) can alter them. Step B runs the same migration functions the NAS
`run_migrations` DAG uses, called directly (never via a DAG run) so verify.py's
zero-DAG-run assertion keeps holding. Prints counts only, never credential
values.
"""

import os
import sys
from pathlib import Path

import psycopg2
from psycopg2 import sql

DAGS_REPO_PATH = Path(os.getenv("AIRFLOW__CORE__DAGS_FOLDER", "/opt/airflow/dags/repo"))
SQL_DIR = DAGS_REPO_PATH / "congress_videos/sql"
# One grant script per schema (congress_videos/sql/) — kept in lockstep with
# utils/migrations_dag.py's SCHEMA_OWNER_ROLES, the single source of truth for
# which runtime role owns which schema.
GRANT_SCRIPTS = {
    "development": "grant_permissions.sql",
    "production": "grant_permissions_production.sql",
}
MIGRATION_ROLE = "airflow_migrations"
# The base schema files are later snapshots, not the pristine initial state: they
# are applied only while their sentinel table is absent, and the views the
# migration history re-creates itself (007/010/036/038) are dropped right after,
# otherwise CREATE OR REPLACE VIEW in 007 refuses to drop columns. Views no
# migration touches (chapter_statistics) stay as the base file defines them.
BASE_SCHEMA_FILES = (
    ("congressional_videos_schema.sql", "congressional_sessions", ("uploadable_videos",)),
    ("youtube_chapters_schema.sql", "video_chapters", ("uploadable_chapters",)),
)
# Sentinel used to detect an empty (not yet restored) non-development schema —
# video_chapters is common to every schema (the youtube_chapters family),
# unlike congressional_sessions, which is development/legacy-only and absent
# from production even after a successful restore.
RESTORED_SCHEMA_SENTINEL = "video_chapters"


class EmptyRestoredSchemaError(RuntimeError):
    """A non-development schema has no data yet — the infra restore playbook
    (pg_restore from the NAS) must run before app-init/migrations do."""


# Sequences owned by a column follow their table and refuse a direct ALTER.
OWNERSHIP_HANDOFF = """
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.relname, c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = %(schema)s AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
      AND pg_get_userbyid(c.relowner) <> %(owner)s
      AND NOT (c.relkind = 'S' AND EXISTS (SELECT 1 FROM pg_depend d WHERE d.objid = c.oid AND d.deptype = 'a'))
    ORDER BY (c.relkind = 'S')
  LOOP
    EXECUTE format('ALTER %%s %%I.%%I OWNER TO %%I',
      CASE r.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END,
      %(schema)s, r.relname, %(owner)s);
  END LOOP;
  FOR r IN
    SELECT p.oid::regprocedure AS signature FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = %(schema)s AND pg_get_userbyid(p.proowner) <> %(owner)s
  LOOP
    EXECUTE format('ALTER FUNCTION %%s OWNER TO %%I', r.signature, %(owner)s);
  END LOOP;
END $$;
"""


def _superuser_connection():
    return psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ["POSTGRES_PORT"],
        dbname=os.environ["POSTGRES_DB"],
        user="airflow",
        password=os.environ["APPLICATION_PASSWORD"],
    )


def _resolve_provisioning():
    """Validate POSTGRES_SCHEMA/POSTGRES_USER and return (schema, owner_role, grant_script).

    POSTGRES_USER carries the compose-resolved runtime role (release.env's
    POSTGRES_RUNTIME_ROLE, interpolated by compose.yml into POSTGRES_USER) —
    the same variable utils/postgres_helpers.py's PostgresConnection reads for
    its own connection. Validated against utils/migrations_dag.py's
    SCHEMA_OWNER_ROLES, the single source of truth for which role owns which
    schema, instead of introducing a second mapping here. Fails fast (before
    any DB connection is opened) on any other pairing.
    """
    from utils.migrations_dag import SCHEMA_OWNER_ROLES

    schema = os.environ["POSTGRES_SCHEMA"]
    runtime_role = os.environ["POSTGRES_USER"]
    expected_role = SCHEMA_OWNER_ROLES.get(schema)
    if expected_role is None or runtime_role != expected_role:
        allowed = ", ".join(f"{s}={r}" for s, r in SCHEMA_OWNER_ROLES.items())
        raise ValueError(
            f"Unsupported POSTGRES_SCHEMA/POSTGRES_USER combination: {schema!r}/{runtime_role!r}. "
            f"Allowed pairs (schema=runtime role): {allowed}."
        )
    return schema, expected_role, SQL_DIR / GRANT_SCRIPTS[schema]


def _set_role_password(cur, role, password):
    # ALTER ROLE ... PASSWORD does not accept bind parameters; sql.Literal quotes
    # the value safely without ever formatting it into a log line.
    cur.execute(sql.SQL("ALTER ROLE {} PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))


def _table_exists(cur, schema, table):
    cur.execute("SELECT to_regclass(%s) IS NOT NULL", (f"{schema}.{table}",))
    return cur.fetchone()[0]


def _bootstrap(conn, schema, owner_role, grant_script):
    """Schema, roles, passwords, base tables on a fresh DEV database, ownership handoff.

    BASE_SCHEMA_FILES only ever runs for `development`: congressional_videos_schema.sql
    and youtube_chapters_schema.sql hardcode `development.<table>` internally (they are
    not schema-parameterized SQL), so applying them against any other schema would create
    tables in `development` instead of the intended target. Every other schema (`production`)
    never runs them either — it always arrives already populated via pg_restore from the
    NAS, so its sentinel table is expected to exist before app-init ever runs; if it does
    not, that restore never happened and _bootstrap fails fast here (EmptyRestoredSchemaError)
    rather than let migrations run against an empty schema.
    """
    applied = 0
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        cur.execute(grant_script.read_text())
        _set_role_password(cur, owner_role, os.environ["POSTGRES_PASSWORD"])
        _set_role_password(cur, MIGRATION_ROLE, os.environ["MIGRATION_POSTGRES_PASSWORD"])
    conn.commit()
    if schema == "development":
        for filename, sentinel, migration_owned_views in BASE_SCHEMA_FILES:
            # One transaction per base file: a failure leaves the sentinel absent, so a retry re-applies it.
            with conn.cursor() as cur:
                if _table_exists(cur, schema, sentinel):
                    continue
                cur.execute((SQL_DIR / filename).read_text())
                for view in migration_owned_views:
                    cur.execute(
                        sql.SQL("DROP VIEW IF EXISTS {}.{}").format(sql.Identifier(schema), sql.Identifier(view))
                    )
            conn.commit()
            applied += 1
    else:
        with conn.cursor() as cur:
            restored = _table_exists(cur, schema, RESTORED_SCHEMA_SENTINEL)
        if not restored:
            raise EmptyRestoredSchemaError(
                f"schema {schema!r} is empty: {schema} data is bootstrapped by the infra "
                f"'import-db' playbook (pg_restore from the NAS); run it first, then re-run "
                f"app-init/apply"
            )
    with conn.cursor() as cur:
        cur.execute(OWNERSHIP_HANDOFF, {"schema": schema, "owner": owner_role})
    conn.commit()
    return applied


def _apply_migrations():
    """Run the same migration functions the run_migrations DAG uses, called directly."""
    from utils.migrations_dag import _apply_pending_migrations, _ensure_migrations_table

    _ensure_migrations_table()
    _apply_pending_migrations()


def _report(base_files_applied):
    """Print file and applied counts only; never row content or credentials."""
    from utils.postgres_helpers import PostgresConnection

    pg = PostgresConnection()
    file_count = len(list(DAGS_REPO_PATH.glob("*/sql/migrations/*.sql")))
    with pg.get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT count(*) AS count FROM {pg.schema}.schema_migrations")
        applied_count = cur.fetchone()["count"]
    print(
        f"application-db init: base_files_applied={base_files_applied} "
        f"migrations_found={file_count} migrations_applied={applied_count}"
    )


def main():
    sys.path.insert(0, str(DAGS_REPO_PATH))
    schema, owner_role, grant_script = _resolve_provisioning()
    conn = _superuser_connection()
    try:
        base_files_applied = _bootstrap(conn, schema, owner_role, grant_script)
    finally:
        conn.close()
    _apply_migrations()
    _report(base_files_applied)


if __name__ == "__main__":
    main()
