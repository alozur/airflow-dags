-- Grant permissions on production schema to 'airflow' user
-- Run this with a superuser account (e.g., postgres user)

-- Grant usage on schema
GRANT USAGE ON SCHEMA production TO airflow;

-- Grant all privileges on schema
GRANT ALL PRIVILEGES ON SCHEMA production TO airflow;

-- Grant all privileges on all existing tables
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA production TO airflow;

-- Grant all privileges on all sequences (for auto-increment columns)
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA production TO airflow;

-- Grant all privileges on all functions in schema
GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA production TO airflow;

-- Grant privileges on future tables (so you don't have to grant again)
ALTER DEFAULT PRIVILEGES IN SCHEMA production
GRANT ALL PRIVILEGES ON TABLES TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA production
GRANT ALL PRIVILEGES ON SEQUENCES TO airflow;

ALTER DEFAULT PRIVILEGES IN SCHEMA production
GRANT ALL PRIVILEGES ON FUNCTIONS TO airflow;

-- Verify grants
SELECT
    grantee,
    privilege_type,
    table_schema,
    table_name
FROM information_schema.table_privileges
WHERE table_schema = 'production'
  AND grantee = 'airflow'
ORDER BY table_name, privilege_type;
