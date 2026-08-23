-- Migration: Add turn_type classification column (issue #176)
-- Created: 2026-08-24
-- Depends on: 032_add_upload_verification.sql
--
-- Adds turn_type TEXT NOT NULL DEFAULT 'monologue' to speaker_turn_videos.
-- Values: 'monologue' (single speaker or all-unknown) vs 'qa' (two or more
-- distinct non-placeholder resolved speakers in the same grouped output).
--
-- Existing rows receive 'monologue' via the column DEFAULT; re-classification
-- happens on the next materialization re-run (accepted behavior per spec).
--
-- Idempotent: ADD COLUMN IF NOT EXISTS.

-- UP

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS turn_type TEXT NOT NULL DEFAULT 'monologue';

-- DOWN
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS turn_type;
