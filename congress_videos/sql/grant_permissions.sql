-- Least-privilege grants on the development schema (issue #203).
-- Run this with a superuser account (e.g., postgres user).
--
-- Two roles are provisioned:
--   airflow_dev         — runtime role, DML only (SELECT/INSERT/UPDATE/DELETE),
--                         scoped to `development`. No DDL.
--   airflow_migrations  — dedicated DDL role (CREATE/ALTER/DROP), scoped to
--                         `development`. Shared with grant_permissions_production.sql
--                         (idempotent re-creation — see below).
--
-- Additive-first: this script only creates new roles/grants. It does not
-- revoke anything from the legacy `airflow` role. The cutover REVOKE (schema-
-- scoped, congress schemas only — `airflow` keeps LOGIN for the Airflow
-- metadata DB) is a manual post-merge step documented in docs/DEPLOYMENT_NAS.md.
--
-- After running this script, set each role's password out-of-band:
--   ALTER ROLE airflow_dev PASSWORD '<vault-value>';
--   ALTER ROLE airflow_migrations PASSWORD '<vault-value>';

-- ---------------------------------------------------------------------------
-- Role creation — idempotent, passwordless (fails closed under scram/md5 auth
-- until a password is set via ALTER ROLE above).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_dev') THEN
        CREATE ROLE airflow_dev LOGIN;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_migrations') THEN
        CREATE ROLE airflow_migrations LOGIN;
    END IF;
END
$$;

-- airflow_migrations inherits airflow's ownership rights on pre-existing
-- objects via membership. Existing tables are owned by `airflow`; GRANT
-- CREATE ON SCHEMA alone cannot ALTER/DROP them. Membership is reversible
-- and touches no object-catalog rows (REASSIGN OWNED BY was rejected —
-- see design.md D2). Ownership of legacy objects stays on `airflow`.
GRANT airflow TO airflow_migrations;

-- ---------------------------------------------------------------------------
-- Schema-object creation (issue #310) — airflow_dev is the owner role that
-- `_assume_owner_role` (utils/migrations_dag.py) SETs ROLE to before running
-- migration DDL, so it must hold CREATE on its own schema, not just USAGE.
-- Without this grant every migration DDL statement fails with
-- InsufficientPrivilege once SET ROLE succeeds.
-- ---------------------------------------------------------------------------
GRANT CREATE ON SCHEMA development TO airflow_dev;

-- Membership (issue #291) — required for `SET ROLE "airflow_dev"` to succeed
-- from a connection authenticated as airflow_migrations.
GRANT airflow_dev TO airflow_migrations;

-- ---------------------------------------------------------------------------
-- Runtime role (airflow_dev): DML only, scoped to `development`
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA development TO airflow_dev;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA development TO airflow_dev;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA development TO airflow_dev;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA development TO airflow_dev;

-- ---------------------------------------------------------------------------
-- Migration role (airflow_migrations): DDL, scoped to `development`
-- ---------------------------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA development TO airflow_migrations;

-- ---------------------------------------------------------------------------
-- Default privileges (D3) — two blocks, so tables created by EITHER the new
-- migration role OR the legacy `airflow` role (which still creates tables
-- until cutover step 3) automatically hand off DML to airflow_dev.
-- ---------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA development
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow_dev;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA development
    GRANT USAGE, SELECT ON SEQUENCES TO airflow_dev;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA development
    GRANT EXECUTE ON FUNCTIONS TO airflow_dev;

ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA development
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow_dev;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA development
    GRANT USAGE, SELECT ON SEQUENCES TO airflow_dev;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA development
    GRANT EXECUTE ON FUNCTIONS TO airflow_dev;

-- ---------------------------------------------------------------------------
-- Verify grants
-- ---------------------------------------------------------------------------
SELECT
    grantee,
    privilege_type,
    table_schema,
    table_name
FROM information_schema.table_privileges
WHERE table_schema = 'development'
  AND grantee IN ('airflow_dev', 'airflow_migrations')
ORDER BY grantee, table_name, privilege_type;
