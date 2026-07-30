-- Migration: Add nullable nickname column to congress_participants
-- Created: 2026-07-30
-- Depends on: 013_create_congress_participants.sql
--
-- Adds a nullable TEXT column for display/short name (e.g. "Pedro" for Pedro Sánchez).
-- Population is out of scope for this change; the column is added now to unblock
-- the thumbnail feature (#18) without a blocking migration later.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS is safe to re-run.
-- Runner sets search_path before executing, so names are UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 014_add_nickname.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 014_add_nickname.sql

ALTER TABLE congress_participants
    ADD COLUMN IF NOT EXISTS nickname TEXT;

COMMENT ON COLUMN congress_participants.nickname IS
    'Optional short/display name for thumbnail generation; NULL until populated manually or via LLM';
