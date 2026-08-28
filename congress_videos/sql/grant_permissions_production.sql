-- Least-privilege grants on the production schema (issue #203).
-- Run this with a superuser account (e.g., postgres user).
--
-- Two roles are provisioned:
--   airflow_prod        — runtime role, DML only (SELECT/INSERT/UPDATE/DELETE),
--                         scoped to `production`. No DDL.
--   airflow_migrations  — dedicated DDL role (CREATE/ALTER/DROP), scoped to
--                         `production`. Role-creation block is duplicated
--                         deliberately from grant_permissions.sql — each
--                         script must be runnable standalone.
--
-- Additive-first: this script only creates new roles/grants. It does not
-- revoke anything from the legacy `airflow` role. The cutover REVOKE (schema-
-- scoped, congress schemas only — `airflow` keeps LOGIN for the Airflow
-- metadata DB) is a manual post-merge step documented in docs/DEPLOYMENT_NAS.md.
--
-- After running this script, set each role's password out-of-band:
--   ALTER ROLE airflow_prod PASSWORD '<vault-value>';
--   ALTER ROLE airflow_migrations PASSWORD '<vault-value>';

-- ---------------------------------------------------------------------------
-- Role creation — idempotent, passwordless (fails closed under scram/md5 auth
-- until a password is set via ALTER ROLE above).
-- ---------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_prod') THEN
        CREATE ROLE airflow_prod LOGIN;
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
-- Runtime role (airflow_prod): DML only, scoped to `production`
-- ---------------------------------------------------------------------------
GRANT USAGE ON SCHEMA production TO airflow_prod;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA production TO airflow_prod;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA production TO airflow_prod;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA production TO airflow_prod;

-- ---------------------------------------------------------------------------
-- Migration role (airflow_migrations): DDL, scoped to `production`
-- ---------------------------------------------------------------------------
GRANT USAGE, CREATE ON SCHEMA production TO airflow_migrations;

-- ---------------------------------------------------------------------------
-- Default privileges (D3) — two blocks, so tables created by EITHER the new
-- migration role OR the legacy `airflow` role (which still creates tables
-- until cutover step 4) automatically hand off DML to airflow_prod.
-- ---------------------------------------------------------------------------
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA production
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow_prod;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA production
    GRANT USAGE, SELECT ON SEQUENCES TO airflow_prod;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow_migrations IN SCHEMA production
    GRANT EXECUTE ON FUNCTIONS TO airflow_prod;

ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA production
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO airflow_prod;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA production
    GRANT USAGE, SELECT ON SEQUENCES TO airflow_prod;
ALTER DEFAULT PRIVILEGES FOR ROLE airflow IN SCHEMA production
    GRANT EXECUTE ON FUNCTIONS TO airflow_prod;

-- ---------------------------------------------------------------------------
-- Verify grants
-- ---------------------------------------------------------------------------
SELECT
    grantee,
    privilege_type,
    table_schema,
    table_name
FROM information_schema.table_privileges
WHERE table_schema = 'production'
  AND grantee IN ('airflow_prod', 'airflow_migrations')
ORDER BY grantee, table_name, privilege_type;
