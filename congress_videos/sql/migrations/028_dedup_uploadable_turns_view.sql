-- Migration: Deduplicate uploadable_turns by output_path (issue #129)
-- Created: 2026-08-20
-- Depends on: 027_add_turn_upload_tracking.sql (uploadable_turns view)
--
-- Grouped short turns (<300s) share ONE materialized mp4 across N speaker_turn_videos
-- rows (distinct turn_id, same output_path). The 027 view emitted N rows per file,
-- re-offering an already-uploaded clip. DISTINCT ON (output_path) collapses each file
-- to one representative row so the queue + count_pending_uploadable_turns are
-- video-shaped. Pairs with the mark_turns_uploaded output_path fix (correctness).
--
-- Idempotent: DROP VIEW IF EXISTS + CREATE VIEW is safe to re-run.
-- Run against both dev and prod schemas via search_path.

-- UP
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
    ORDER BY stv.output_path, stv.turn_id
) dedup
ORDER BY dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore the 027 (non-deduplicated) definition:
-- DROP VIEW IF EXISTS uploadable_turns;
-- CREATE VIEW uploadable_turns AS
-- SELECT
--     stv.turn_id, stv.output_path, st.chapter_id, st.resolved_name,
--     st.start_seconds, st.end_seconds, vc.video_id, vc.title AS chapter_title,
--     vc.description, vc.relevance_score, vc.key_speakers,
--     ysv.session_number, ysv.session_date, stv.materialized_at
-- FROM speaker_turn_videos stv
-- JOIN speaker_turns st ON stv.turn_id = st.turn_id
-- JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
-- JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
-- WHERE stv.is_uploaded_to_youtube = FALSE
--   AND vc.is_uploaded_to_youtube = FALSE
--   AND vc.relevance_score >= 2
-- ORDER BY vc.relevance_score DESC, ysv.session_date DESC;
