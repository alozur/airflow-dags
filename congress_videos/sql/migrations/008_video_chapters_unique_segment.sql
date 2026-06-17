-- Migration: Enforce uniqueness of video_chapters segments
-- Created: 2026-06-12
-- Depends on: 007_update_uploadable_chapters_order.sql
--
-- Deduplicates existing video_chapters rows and adds a unique index on
-- (video_id, start_time, end_time). This backs the ON CONFLICT upsert in
-- database.save_youtube_chapters so overlapping hourly runs of
-- congress_youtube_channel_monitor cannot produce duplicate chapters for the
-- same segment. NULL start/end times are treated as equal (IS NOT DISTINCT FROM).
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 008_video_chapters_unique_segment.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 008_video_chapters_unique_segment.sql

-- Step 1: De-duplicate existing rows, keeping the lowest chapter_id per segment.
-- IS NOT DISTINCT FROM treats NULL start/end times as equal.
DELETE FROM video_chapters a USING video_chapters b
WHERE a.chapter_id > b.chapter_id
  AND a.video_id = b.video_id
  AND a.start_time IS NOT DISTINCT FROM b.start_time
  AND a.end_time IS NOT DISTINCT FROM b.end_time;

-- Step 2: Create the unique index (idempotent).
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_chapters_segment
    ON video_chapters (video_id, start_time, end_time);
