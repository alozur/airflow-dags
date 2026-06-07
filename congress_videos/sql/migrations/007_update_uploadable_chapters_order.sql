-- Migration: Update uploadable_chapters ORDER BY — session_date first
-- Created: 2026-06-07
-- Depends on: youtube_chapters_schema.sql (uploadable_chapters view)
--
-- Changes the ordering of the uploadable_chapters view so that chapters from
-- the most recent plenary session surface first, then by relevance score.
-- Previously: relevance_score DESC, created_at DESC
-- Now:        session_date DESC NULLS LAST, relevance_score DESC, created_at DESC
--
-- Idempotent: CREATE OR REPLACE VIEW is safe to re-run.
-- Run against both development and production schemas as appropriate.
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 007_update_uploadable_chapters_order.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 007_update_uploadable_chapters_order.sql

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
ORDER BY
    ysv.session_date DESC NULLS LAST,
    vc.relevance_score DESC,
    vc.created_at DESC;
