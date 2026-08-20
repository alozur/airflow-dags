-- Migration: Add per-turn interest score for upload prioritisation (issue #142)
-- Created: 2026-08-20
-- Depends on: 028_dedup_uploadable_turns_view.sql
--
-- Adds interest_score NUMERIC to speaker_turns so each materialized turn can
-- carry a 0–10 newsworthiness score computed from its windowed SRT transcript
-- via score_turn_interest() in congress_videos/srt_helpers.py.
--
-- Named constants (Python source of truth):
--   INTEREST_FILTER_THRESHOLD = 1   — turns scored 0 are soft-excluded
--   INTEREST_NEUTRAL          = 1   — COALESCE value for unscored turns
--     → NULL scores clear the filter (COALESCE(NULL,1) >= 1) but sort last
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + DROP/CREATE VIEW.

-- UP

ALTER TABLE speaker_turns ADD COLUMN IF NOT EXISTS interest_score NUMERIC;

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
        stv.materialized_at
    FROM speaker_turn_videos stv
    JOIN speaker_turns st ON stv.turn_id = st.turn_id
    JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
    JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
    WHERE stv.is_uploaded_to_youtube = FALSE
      AND vc.is_uploaded_to_youtube = FALSE
      AND vc.relevance_score >= 2
      AND COALESCE(st.interest_score, 1) >= 1    -- INTEREST_FILTER_THRESHOLD, soft-exclude score 0
    ORDER BY stv.output_path, stv.turn_id
) dedup
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1, sorts last)
         dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore 028 view definition (no interest_score), then drop the column.
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
--         vc.video_id,
--         vc.title AS chapter_title,
--         vc.description,
--         vc.relevance_score,
--         vc.key_speakers,
--         ysv.session_number,
--         ysv.session_date,
--         stv.materialized_at
--     FROM speaker_turn_videos stv
--     JOIN speaker_turns st ON stv.turn_id = st.turn_id
--     JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
--     JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
--     WHERE stv.is_uploaded_to_youtube = FALSE
--       AND vc.is_uploaded_to_youtube = FALSE
--       AND vc.relevance_score >= 2
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- ORDER BY dedup.relevance_score DESC, dedup.session_date DESC;
--
-- ALTER TABLE speaker_turns DROP COLUMN IF EXISTS interest_score;
