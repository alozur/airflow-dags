-- Migration 048: persist scheduled-quota attribution for issue #500.
-- Manual/forced recovery uploads are durable publication records, but must not
-- consume the slot reserved for a later scheduled run on the same date.
-- Existing rows default TRUE to preserve historical counting semantics.

ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS counts_toward_daily_quota BOOLEAN NOT NULL DEFAULT TRUE;

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS counts_toward_daily_quota BOOLEAN NOT NULL DEFAULT TRUE;

-- DOWN (manual only; migration runner executes the whole file transactionally):
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS counts_toward_daily_quota;
-- ALTER TABLE video_chapters DROP COLUMN IF EXISTS counts_toward_daily_quota;
