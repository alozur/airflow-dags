-- Migration: Add source integrity deferred retry tracking
-- Created: 2026-07-30
-- Depends on: youtube_chapters_schema.sql (youtube_source_videos table)
--
-- Adds download_retry_after TIMESTAMP DEFAULT NULL to youtube_source_videos.
-- When record_source_integrity_failure() is called after a failed ffprobe probe,
-- this column is set to NOW()+12h so filter_unprocessed_videos skips the video
-- until the VOD has had time to finalise on YouTube.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS is safe to re-run.
-- Runner runs `SET search_path TO {schema}, public` first, so names are UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 013_add_source_integrity_tracking.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 013_add_source_integrity_tracking.sql

ALTER TABLE youtube_source_videos
    ADD COLUMN IF NOT EXISTS download_retry_after TIMESTAMP DEFAULT NULL;
