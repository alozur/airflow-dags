-- Migration: Minimum grouped-video duration gate for uploadable_turns (issue #234)
-- Created: 2026-08-26
-- Depends on: 034_add_speaker_resolution.sql
--
-- The uploader publishes ONE video per day at 19:00 UTC. Without a length floor a
-- 60s closing remark can consume the slot a 10-minute intervention should have had.
-- Adds group_start_seconds/group_end_seconds and admits a turn video only when the
-- WHOLE grouped clip lasts >= 300s.
--
-- View-only migration: no table DDL, no data touched. Re-declares every gate of the
-- 034 body verbatim (028 DISTINCT ON dedup, 030 prepared_at, 032 is_upload_abandoned,
-- 029 interest ordering, 034 speaker-resolution columns).
--
-- Idempotent: DROP VIEW IF EXISTS + CREATE VIEW is safe to re-run.
-- Run against both dev and prod schemas via search_path (utils/migrations_dag.py).

-- UP

DROP VIEW IF EXISTS uploadable_turns;
CREATE VIEW uploadable_turns AS
WITH group_spans AS (
    -- Wall-clock span of every materialized turn video, over ALL speaker_turn_videos
    -- rows sharing an output_path. Grouped clips (issue #129) hold N rows per file.
    -- This aggregate is deliberately UNFILTERED: mark_turn_prepared sets prepared_at
    -- on a SINGLE turn_id, so the eligibility WHERE below keeps only one sibling.
    -- Computing the span after those gates (e.g. with MIN/MAX OVER (PARTITION BY ...),
    -- which Postgres evaluates AFTER WHERE) would collapse it to that one turn's
    -- narrow window and re-introduce the issue #151 bug class.
    SELECT stv.output_path,
           MIN(st.start_seconds) AS group_start_seconds,
           MAX(st.end_seconds)   AS group_end_seconds
    FROM speaker_turn_videos stv
    JOIN speaker_turns st ON stv.turn_id = st.turn_id
    GROUP BY stv.output_path
)
SELECT * FROM (
    SELECT DISTINCT ON (stv.output_path)
        stv.turn_id,
        stv.output_path,
        st.chapter_id,
        st.resolved_name,
        st.start_seconds,
        st.end_seconds,
        st.interest_score,
        gs.group_start_seconds,
        gs.group_end_seconds,
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
    JOIN group_spans gs ON gs.output_path = stv.output_path
    WHERE stv.is_uploaded_to_youtube = FALSE
      AND stv.prepared_at IS NOT NULL             -- PREPARE readiness gate (issue #146)
      AND NOT stv.is_upload_abandoned              -- ABANDON gate (issue #141)
      AND vc.is_uploaded_to_youtube = FALSE
      AND vc.relevance_score >= 2
      AND COALESCE(st.interest_score, 1) >= 1    -- INTEREST_FILTER_THRESHOLD, soft-exclude score 0
    ORDER BY stv.output_path, stv.turn_id
) dedup
-- MIN_TURN_UPLOAD_DURATION_SECONDS = 300 (issue #234): a turn video must last at least
-- 5 minutes to be worth the single daily 19:00 UTC slot. Documented literal, NOT
-- runtime-tunable — changing the floor requires a new migration.
WHERE dedup.group_end_seconds - dedup.group_start_seconds >= 300
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore the migration 034 view body (no group span columns, no duration filter).
-- No table DDL and no data are touched, so rollback is a pure view re-declaration.
--
-- DROP VIEW IF EXISTS uploadable_turns;
-- CREATE VIEW uploadable_turns AS
-- SELECT * FROM (
--     SELECT DISTINCT ON (stv.output_path)
--         stv.turn_id, stv.output_path, st.chapter_id, st.resolved_name,
--         st.start_seconds, st.end_seconds, st.interest_score,
--         vc.video_id, vc.title AS chapter_title, vc.description,
--         vc.relevance_score, vc.key_speakers,
--         ysv.session_number, ysv.session_date,
--         stv.materialized_at, stv.prepared_at,
--         stv.resolved_participant_slug, stv.speaker_resolution_confidence,
--         stv.speaker_resolution_method
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
