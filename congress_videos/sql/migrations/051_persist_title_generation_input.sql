-- Migration 051: serialized title-generator input payload (issue #549).
-- Nullable/additive: rows generated before this change legitimately carry NULL.

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS title_generation_input JSONB;

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS title_generation_input JSONB;

-- DOWN (manual only; migration runner executes the whole file transactionally):
-- ALTER TABLE video_shorts        DROP COLUMN IF EXISTS title_generation_input;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS title_generation_input;
