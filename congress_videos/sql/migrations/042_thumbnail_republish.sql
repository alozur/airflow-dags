-- Migration 042: Thumbnail republish state on speaker_turn_videos (issue #331)
-- Created: 2026-09-02
-- Depends on: 025_create_speaker_turn_videos.sql, 032_add_upload_verification.sql
--
-- Durable, thumbnail-scoped state for the republish healer (issue #320 closed
-- detection only; the failure text used to expire with log retention).
--
-- POSITIVE marker semantics: thumbnail_republish_needed_at IS NULL = nothing to
-- heal. Every pre-existing row defaults to NULL, so backfill is a non-event.
--
-- Additive only. NO VIEW IS RECREATED: uploadable_turns gates on
-- is_uploaded_to_youtube = FALSE, and every healer candidate has it TRUE, so the
-- upload queue is structurally unaffected. Issue #251's CREATE OR REPLACE rule is
-- a VIEW rule, not an ADD COLUMN rule — precedent is migration 032, which added
-- the analogous upload_verified_at quadruple with plain ALTER TABLE.
--
-- No index: the candidate set is single-digit and the healer runs once daily.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS thumbnail_republish_needed_at  TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS thumbnail_republished_at       TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS thumbnail_republish_attempts   INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS thumbnail_republish_abandoned  BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_thumbnail_republish_error TEXT;

-- DOWN
-- Manual psql only -- the runner has no automatic rollback. Dropping these columns does
-- NOT un-publish anything on YouTube; it only discards the republish audit trail.
--
-- ALTER TABLE speaker_turn_videos
--     DROP COLUMN IF EXISTS last_thumbnail_republish_error,
--     DROP COLUMN IF EXISTS thumbnail_republish_abandoned,
--     DROP COLUMN IF EXISTS thumbnail_republish_attempts,
--     DROP COLUMN IF EXISTS thumbnail_republished_at,
--     DROP COLUMN IF EXISTS thumbnail_republish_needed_at;
