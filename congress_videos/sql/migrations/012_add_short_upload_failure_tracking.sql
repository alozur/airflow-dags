-- Migration: Add per-short upload failure tracking + exclude abandoned shorts
-- Created: 2026-07-29
-- Depends on: 004_create_video_shorts.sql (video_shorts table)
--
-- Records per-short YouTube upload failures (partial failures inside an otherwise
-- successful generic_youtube_uploader run). After the 3rd recorded failure a short is
-- soft-excluded from the upload queue via is_upload_abandoned; the row is never deleted.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + COMMENT ON COLUMN are safe to re-run.
-- Runner runs `SET search_path TO {schema}, public` first, so names are UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 012_add_short_upload_failure_tracking.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 012_add_short_upload_failure_tracking.sql

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS upload_attempts INTEGER DEFAULT 0;
ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS is_upload_abandoned BOOLEAN DEFAULT FALSE;
ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS last_upload_error TEXT DEFAULT NULL;

COMMENT ON COLUMN video_shorts.upload_attempts IS 'Cumulative count of recorded per-short upload FAILURES (successes never touch it)';
COMMENT ON COLUMN video_shorts.is_upload_abandoned IS 'TRUE once upload_attempts reaches the abandon threshold (3); excludes the short from get_pending_shorts';
COMMENT ON COLUMN video_shorts.last_upload_error IS 'Last recorded per-short upload failure message';
