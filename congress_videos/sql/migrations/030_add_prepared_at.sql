-- Migration: Add prepared_at readiness gate to speaker_turn_videos (issue #146)
-- Created: 2026-08-21
-- Depends on: 029_add_turn_interest_score.sql
--
-- Adds prepared_at TIMESTAMPTZ (nullable, default NULL) to speaker_turn_videos.
-- NULL = unprepared; set ONLY by speaker_turn_prepare DAG after ALL sidecars
-- are on disk and ffprobe passes (atomic readiness handshake).
--
-- uploadable_turns view gains AND prepared_at IS NOT NULL so the upload DAG
-- only ever surfaces fully-prepared turns.  is_uploaded_to_youtube remains the
-- orthogonal upload gate.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + DROP VIEW IF EXISTS + CREATE VIEW.

-- UP

ALTER TABLE speaker_turn_videos ADD COLUMN IF NOT EXISTS prepared_at TIMESTAMPTZ;

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
      AND vc.is_uploaded_to_youtube = FALSE
      AND vc.relevance_score >= 2
      AND COALESCE(st.interest_score, 1) >= 1    -- INTEREST_FILTER_THRESHOLD, soft-exclude score 0
    ORDER BY stv.output_path, stv.turn_id
) dedup
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore 029 view definition (no prepared_at gate), then drop the column.
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
--         stv.materialized_at
--     FROM speaker_turn_videos stv
--     JOIN speaker_turns st ON stv.turn_id = st.turn_id
--     JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
--     JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
--     WHERE stv.is_uploaded_to_youtube = FALSE
--       AND vc.is_uploaded_to_youtube = FALSE
--       AND vc.relevance_score >= 2
--       AND COALESCE(st.interest_score, 1) >= 1
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC;
--
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS prepared_at;
