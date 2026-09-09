-- Migration 050: audit record of pre-publication copy verification (issue #512).
-- All columns nullable: rows published before this change, and rows whose
-- verification was inconclusive, legitimately carry NULL.

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS copy_verification_verdict   TEXT,
    ADD COLUMN IF NOT EXISTS copy_verification_findings  JSONB,
    ADD COLUMN IF NOT EXISTS copy_original_title         TEXT,
    ADD COLUMN IF NOT EXISTS copy_original_description   TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_title        TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_description  TEXT,
    ADD COLUMN IF NOT EXISTS copy_thumbnail_text         TEXT,
    ADD COLUMN IF NOT EXISTS copy_content_version        TEXT,
    ADD COLUMN IF NOT EXISTS copy_verified_at            TIMESTAMPTZ;

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS copy_verification_verdict   TEXT,
    ADD COLUMN IF NOT EXISTS copy_verification_findings  JSONB,
    ADD COLUMN IF NOT EXISTS copy_original_title         TEXT,
    ADD COLUMN IF NOT EXISTS copy_original_description   TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_title        TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_description  TEXT,
    ADD COLUMN IF NOT EXISTS copy_content_version        TEXT,
    ADD COLUMN IF NOT EXISTS copy_verified_at            TIMESTAMP;

-- DOWN (manual only; migration runner executes the whole file transactionally):
-- ALTER TABLE video_shorts
--     DROP COLUMN IF EXISTS copy_verification_verdict,
--     DROP COLUMN IF EXISTS copy_verification_findings,
--     DROP COLUMN IF EXISTS copy_original_title,
--     DROP COLUMN IF EXISTS copy_original_description,
--     DROP COLUMN IF EXISTS copy_corrected_title,
--     DROP COLUMN IF EXISTS copy_corrected_description,
--     DROP COLUMN IF EXISTS copy_content_version,
--     DROP COLUMN IF EXISTS copy_verified_at;
-- ALTER TABLE speaker_turn_videos
--     DROP COLUMN IF EXISTS copy_verification_verdict,
--     DROP COLUMN IF EXISTS copy_verification_findings,
--     DROP COLUMN IF EXISTS copy_original_title,
--     DROP COLUMN IF EXISTS copy_original_description,
--     DROP COLUMN IF EXISTS copy_corrected_title,
--     DROP COLUMN IF EXISTS copy_corrected_description,
--     DROP COLUMN IF EXISTS copy_thumbnail_text,
--     DROP COLUMN IF EXISTS copy_content_version,
--     DROP COLUMN IF EXISTS copy_verified_at;
