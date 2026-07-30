-- Migration: Create congress_participants table for daily deputies sync
-- Created: 2026-07-23
-- Depends on: none (additive, new table)
--
-- Backs congress_videos/modules/participants_{ingestion,enrichment,db}.py: stores the
-- canonical registry of active Spanish Congress of Deputies members, sourced from the
-- official congreso.es open-data feed and enriched with Wikidata photo URLs.
--
-- normalized_name is the idempotent upsert key (accents stripped, case-folded NOMBRE).
-- The migration runner runs `SET search_path TO {schema}, public` before executing, so
-- table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 013_create_congress_participants.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 013_create_congress_participants.sql

CREATE TABLE IF NOT EXISTS congress_participants (
    normalized_name                TEXT        UNIQUE NOT NULL,
    display_name                   TEXT        NOT NULL,
    party                          TEXT,
    parliamentary_group            TEXT,
    constituency                   TEXT,
    biography                      TEXT,
    full_membership_date           DATE,
    start_date                     DATE,
    group_entry_date               DATE,
    photo_url                      TEXT,
    created_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
