-- Migration 043: Persist art direction brief on video_thumbnails (issue #292)
-- Created: 2026-09-02
-- Depends on: 041_analytics_actions.sql (archetype column)
--
-- The archetype-aware thumbnail prompt already derives a structured art
-- direction brief (composition, subject framing, mood) per generation, but
-- it never survives past the in-memory generation call. Persisting it lets
-- downstream auditing and future re-generation reuse the same brief instead
-- of re-deriving it from scratch.
--
-- Nullable JSONB: every pre-existing row defaults to NULL, so backfill is a
-- non-event. This migration only adds the column; the write path (thumbnail
-- generation) is a separate, later change.
--
-- No index: the brief is read back per-row by thumbnail_id, never filtered
-- or sorted on.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE video_thumbnails
    ADD COLUMN IF NOT EXISTS art_direction_brief JSONB;

-- DOWN
-- Manual psql only -- the runner has no automatic rollback. Dropping this
-- column does NOT affect any published thumbnail; it only discards the art
-- direction brief audit data.
--
-- ALTER TABLE video_thumbnails
--     DROP COLUMN IF EXISTS art_direction_brief;
