-- Migration 052: bounded thumbnail-text regeneration audit trail (issue #545).
-- Additive/nullable: rows published before this change legitimately carry NULL.
-- speaker_turn_videos ONLY (design.md D1) — video_thumbnails is upserted
-- destructively via ON CONFLICT ... DO UPDATE on (chapter_id, label) by
-- generic_thumbnail_generator's persist_results, so a spend counter placed
-- there would be clobbered by the very operation it exists to bound.
-- video_shorts is deliberately untouched: this capability is long-form only.

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS thumbnail_regen_attempts    INTEGER     DEFAULT 0,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_exhausted   BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_outcome     TEXT,
    ADD COLUMN IF NOT EXISTS last_thumbnail_regen_error  TEXT,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_prior_brief JSONB,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_brief       JSONB;

-- DOWN (manual only; migration runner executes the whole file transactionally):
-- ALTER TABLE speaker_turn_videos
--     DROP COLUMN IF EXISTS thumbnail_regen_attempts,
--     DROP COLUMN IF EXISTS thumbnail_regen_exhausted,
--     DROP COLUMN IF EXISTS thumbnail_regen_at,
--     DROP COLUMN IF EXISTS thumbnail_regen_outcome,
--     DROP COLUMN IF EXISTS last_thumbnail_regen_error,
--     DROP COLUMN IF EXISTS thumbnail_regen_prior_brief,
--     DROP COLUMN IF EXISTS thumbnail_regen_brief;
