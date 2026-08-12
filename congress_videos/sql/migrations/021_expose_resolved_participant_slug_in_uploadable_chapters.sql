-- Migration: Expose resolved_participant_slug in uploadable_chapters
-- Created: 2026-08-12
-- Depends on: 010_add_timeline_column.sql (uploadable_chapters view),
--             020_add_resolved_participant_slug.sql (video_chapters.resolved_participant_slug)
--
-- Surfaces the authoritative participant slug written by speaker normalization
-- (fuzzy match or institutional-role resolution) so the chapter uploader can
-- build the thumbnail with the resolved person instead of re-fuzzy-matching the
-- raw speaker string at thumbnail time.
--
-- Idempotent: CREATE OR REPLACE VIEW is safe to re-run. The new column is
-- appended at the END of the select list, which CREATE OR REPLACE VIEW allows
-- without a DROP (existing column names/order/types are unchanged).
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so table/view names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 021_expose_resolved_participant_slug_in_uploadable_chapters.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 021_expose_resolved_participant_slug_in_uploadable_chapters.sql

CREATE OR REPLACE VIEW uploadable_chapters AS
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
    CURRENT_DATE - DATE(vc.created_at) AS days_since_created,
    vc.resolved_participant_slug
FROM video_chapters vc
JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE
    vc.is_uploaded_to_youtube = FALSE
    AND vc.relevance_score >= 2
ORDER BY
    ysv.session_date DESC NULLS LAST,
    vc.relevance_score DESC,
    vc.created_at DESC;
