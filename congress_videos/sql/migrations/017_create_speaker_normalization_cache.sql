-- Migration: Create speaker_normalization_cache table
-- Created: 2026-07-31
-- Depends on: 004_create_video_shorts.sql (video_chapters table), 015_create_congress_participants.sql
--
-- Stores every fuzzy-lookup + AI-verification outcome for each dirty speaker
-- string found in a video chapter.  Provides idempotency for re-runs and an
-- audit trail for manual review.
--
-- FK targets video_chapters(chapter_id) — the actual PK column (SERIAL),
-- not the legacy `id` name used in the spec draft.
--
-- Runner executes `SET search_path TO {schema}, public` before this file,
-- so all table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 017_create_speaker_normalization_cache.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 017_create_speaker_normalization_cache.sql

CREATE TABLE IF NOT EXISTS speaker_normalization_cache (
    id                          SERIAL          PRIMARY KEY,
    chapter_id                  INT             NOT NULL REFERENCES video_chapters(chapter_id),
    dirty_speaker               TEXT            NOT NULL,
    canonical_speaker           TEXT,
    participant_normalized_name TEXT            REFERENCES congress_participants(normalized_name),
    status                      TEXT            NOT NULL CHECK (status IN ('matched', 'no_match', 'needs_manual')),
    confidence_score            NUMERIC(5,4),
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    UNIQUE (chapter_id, dirty_speaker)
);
