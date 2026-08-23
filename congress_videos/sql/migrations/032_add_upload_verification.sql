-- Migration: Add upload verification tracking (issue #141)
-- Created: 2026-08-23
-- Depends on: 031_add_turns_detected_at.sql
--
-- Adds upload_verified_at TIMESTAMPTZ (nullable, default NULL) to video_chapters
-- and speaker_turn_videos. NULL = not yet verified; set by post_upload_verification
-- DAG once the video is confirmed live (oembed 200 or Data API succeeded/private/unlisted).
--
-- Adds upload failure-tracking columns to speaker_turn_videos to mirror the
-- existing columns on video_chapters (upload_attempts, is_upload_abandoned,
-- last_upload_error). These are required by record_upload_verification_failure
-- so abandoned turns are excluded from the uploadable_turns view.
--
-- Recreates uploadable_turns view (copy of migration 030 body) with an added
-- AND NOT stv.is_upload_abandoned gate to prevent re-queuing of permanently
-- abandoned turns. This gate mirrors uploadable_chapters.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + DROP VIEW IF EXISTS + CREATE VIEW.

-- UP

ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS upload_verified_at TIMESTAMPTZ;

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS upload_verified_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS upload_attempts INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS is_upload_abandoned BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS last_upload_error TEXT;

DROP VIEW IF EXISTS uploadable_turns;
CREATE VIEW uploadable_turns AS
SELECT * FROM (
    SELECT DISTINCT ON (stv.output_path)
        stv.turn_id,
        stv.output_path,
        st.chapter_id,
        st.resolved_name,
        st.start_seconds,
        st.end_seconds,
        st.interest_score,
        vc.video_id,
        vc.title AS chapter_title,
        vc.description,
        vc.relevance_score,
        vc.key_speakers,
        ysv.session_number,
        ysv.session_date,
        stv.materialized_at,
        stv.prepared_at
    FROM speaker_turn_videos stv
    JOIN speaker_turns st ON stv.turn_id = st.turn_id
    JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
    JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
    WHERE stv.is_uploaded_to_youtube = FALSE
      AND stv.prepared_at IS NOT NULL             -- PREPARE readiness gate (issue #146)
      AND NOT stv.is_upload_abandoned              -- ABANDON gate (issue #141)
      AND vc.is_uploaded_to_youtube = FALSE
      AND vc.relevance_score >= 2
      AND COALESCE(st.interest_score, 1) >= 1    -- INTEREST_FILTER_THRESHOLD, soft-exclude score 0
    ORDER BY stv.output_path, stv.turn_id
) dedup
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore 031 view definition (no is_upload_abandoned gate), then drop the columns.
--
-- DROP VIEW IF EXISTS uploadable_turns;
-- CREATE VIEW uploadable_turns AS
-- SELECT * FROM (
--     SELECT DISTINCT ON (stv.output_path)
--         stv.turn_id,
--         stv.output_path,
--         st.chapter_id,
--         st.resolved_name,
--         st.start_seconds,
--         st.end_seconds,
--         st.interest_score,
--         vc.video_id,
--         vc.title AS chapter_title,
--         vc.description,
--         vc.relevance_score,
--         vc.key_speakers,
--         ysv.session_number,
--         ysv.session_date,
--         stv.materialized_at,
--         stv.prepared_at
--     FROM speaker_turn_videos stv
--     JOIN speaker_turns st ON stv.turn_id = st.turn_id
--     JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
--     JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
--     WHERE stv.is_uploaded_to_youtube = FALSE
--       AND stv.prepared_at IS NOT NULL
--       AND vc.is_uploaded_to_youtube = FALSE
--       AND vc.relevance_score >= 2
--       AND COALESCE(st.interest_score, 1) >= 1
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC;
--
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS last_upload_error;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS is_upload_abandoned;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS upload_attempts;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS upload_verified_at;
-- ALTER TABLE video_chapters DROP COLUMN IF EXISTS upload_verified_at;
