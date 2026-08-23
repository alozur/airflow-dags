-- Migration: Add speaker resolution columns (issue #177)
-- Created: 2026-08-24
-- Depends on: 033_add_turn_type.sql
--
-- Adds three nullable columns to speaker_turn_videos that persist the result
-- of the AI speaker resolution step (SRT intro-window + gpt-4o-mini).
--
-- resolved_participant_slug TEXT  — FK-by-convention to congress_participants.slug
-- speaker_resolution_confidence FLOAT  — model confidence in [0.0, 1.0]
-- speaker_resolution_method TEXT CHECK ('ai_srt_context'|'fuzzy'|'manual')
--
-- Recreates uploadable_turns view (032 baseline with 033 NO-OP) extending
-- the SELECT with the three new columns while preserving DISTINCT ON dedup
-- (migration 028), prepared_at gate (migration 030), is_upload_abandoned gate
-- (migration 032), and COALESCE interest ordering.
--
-- Idempotent: each column addition uses the IF NOT EXISTS guard, the constraint
-- is protected by a DO block checking pg_constraint, and the view uses DROP/CREATE.

-- UP

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS resolved_participant_slug TEXT,
    ADD COLUMN IF NOT EXISTS speaker_resolution_confidence FLOAT,
    ADD COLUMN IF NOT EXISTS speaker_resolution_method TEXT;

-- Guarded CHECK constraint — safe to re-run if already exists.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'chk_speaker_resolution_method'
    ) THEN
        ALTER TABLE speaker_turn_videos
            ADD CONSTRAINT chk_speaker_resolution_method
            CHECK (
                speaker_resolution_method IN ('ai_srt_context', 'fuzzy', 'manual')
                OR speaker_resolution_method IS NULL
            );
    END IF;
END $$;

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
        stv.prepared_at,
        stv.resolved_participant_slug,
        stv.speaker_resolution_confidence,
        stv.speaker_resolution_method
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
-- Drop the three new columns, remove the CHECK constraint, and restore the 032
-- view body (without resolved_participant_slug, speaker_resolution_confidence,
-- or speaker_resolution_method in SELECT).
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
--       AND NOT stv.is_upload_abandoned
--       AND vc.is_uploaded_to_youtube = FALSE
--       AND vc.relevance_score >= 2
--       AND COALESCE(st.interest_score, 1) >= 1
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC;
--
-- ALTER TABLE speaker_turn_videos DROP CONSTRAINT IF EXISTS chk_speaker_resolution_method;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS speaker_resolution_method;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS speaker_resolution_confidence;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS resolved_participant_slug;
