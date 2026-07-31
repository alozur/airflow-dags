-- Migration 018: Add deterministic URL-safe slug to congress_participants
-- Created: 2026-07-31
-- Depends on: 015_create_congress_participants.sql
--
-- slug = REPLACE(normalized_name, ' ', '-'). Since normalized_name is already
-- UNIQUE NOT NULL, the derived slug is unique too. Backfill existing rows before
-- enforcing NOT NULL, or the constraint alter fails on a populated table.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE UNIQUE INDEX IF NOT EXISTS.
-- Runner sets search_path before executing, so names are UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 018_add_participant_slug.sql

-- 1) Add nullable column (idempotent).
ALTER TABLE congress_participants
    ADD COLUMN IF NOT EXISTS slug TEXT;

-- 2) Backfill existing rows deterministically (only where unset).
UPDATE congress_participants
    SET slug = REPLACE(normalized_name, ' ', '-')
    WHERE slug IS NULL;

-- 3) Enforce NOT NULL now that every row has a value.
ALTER TABLE congress_participants
    ALTER COLUMN slug SET NOT NULL;

-- 4) Enforce uniqueness (idempotent; index form so re-runs are safe).
CREATE UNIQUE INDEX IF NOT EXISTS uq_congress_participants_slug
    ON congress_participants (slug);
