-- Migration: Add per-chapter upload failure tracking + exclude abandoned chapters
-- Created: 2026-07-23
-- Depends on: 010_add_timeline_column.sql (uploadable_chapters view),
--             youtube_chapters_schema.sql (video_chapters table)
--
-- Records per-chapter YouTube upload failures (partial failures inside an otherwise
-- successful generic_youtube_uploader run). After the 3rd recorded failure a chapter is
-- soft-excluded from the upload queue via is_upload_abandoned; the row is never deleted.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + DROP VIEW IF EXISTS + CREATE VIEW are safe to
-- re-run. Runner runs `SET search_path TO {schema}, public` first, so names are UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 011_add_chapter_upload_failure_tracking.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 011_add_chapter_upload_failure_tracking.sql

-- Step 1: Add failure-tracking columns (additive, safe defaults).
ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS upload_attempts INTEGER DEFAULT 0;
ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS is_upload_abandoned BOOLEAN DEFAULT FALSE;
ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS last_upload_error TEXT;

-- Step 2: Recreate uploadable_chapters to also exclude abandoned chapters.
-- SELECT list is IDENTICAL to migration 010; only the WHERE clause gains the new predicate.
-- DROP + CREATE (not CREATE OR REPLACE) for consistency with 010 and unconditional safety.
DROP VIEW IF EXISTS uploadable_chapters;
CREATE VIEW uploadable_chapters AS
SELECT
    vc.chapter_id,
    vc.video_id,
    ysv.video_title AS source_video_title,
    ysv.session_number,
    ysv.session_date,
    vc.title AS chapter_title,
    vc.description,
    vc.duration_minutes,
    vc.speakers,
    vc.topics,
    vc.timeline,
    vc.start_time,
    vc.end_time,
    vc.relevance_score,
    vc.speaker_relevance_points,
    vc.topic_relevance_points,
    vc.public_interest_points,
    vc.scoring_reasoning,
    vc.key_speakers,
    vc.is_current_topic,
    vc.is_uploaded_to_youtube,
    vc.created_at,
    CURRENT_DATE - DATE(vc.created_at) AS days_since_created
FROM video_chapters vc
JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE
    vc.is_uploaded_to_youtube = FALSE
    AND vc.relevance_score >= 2
    AND vc.is_upload_abandoned = FALSE
ORDER BY
    ysv.session_date DESC NULLS LAST,
    vc.relevance_score DESC,
    vc.created_at DESC;
