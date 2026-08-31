-- Migration: Procedural turn filter — flag+excise chair floor-handoffs (issue #143)
-- Created: 2026-08-31
-- Depends on: 035_add_min_turn_duration.sql
--
-- Adds a per-turn is_procedural/procedural_reason flag (set at diarization time by
-- congress_videos.modules.speaker_turns._flag_procedural) and a keep_intervals JSONB
-- column on speaker_turn_videos recording the executed cut boundaries when a
-- procedural member turn was excised from a grouped clip (NULL = legacy single
-- window, no excision).
--
-- Re-declares uploadable_turns from the 035 body verbatim — every prior gate
-- copied forward (028 DISTINCT ON dedup, 029 interest ordering, 030 prepared_at,
-- 032 is_upload_abandoned, 034 speaker-resolution columns, 035 group_spans CTE +
-- 300s floor) — plus two additions:
--   1. inner WHERE gains NOT COALESCE(st.is_procedural, FALSE);
--   2. group_spans gains procedural_seconds (SUM of excised member durations),
--      and the outer floor becomes group span MINUS procedural_seconds >= 300 —
--      the PUBLISHED clip, not the raw span, must clear 5 minutes.
--
-- group_spans stays otherwise UNFILTERED (issue #151 / MIN-MAX-after-WHERE trap,
-- documented in 035) — is_procedural is read here only to sum durations, never
-- to gate which rows enter the aggregate.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + DROP VIEW IF EXISTS/CREATE VIEW are
-- safe to re-run. Run against both dev and prod schemas via search_path
-- (utils/migrations_dag.py sets SET search_path TO {schema}, public first).

-- UP

ALTER TABLE speaker_turns
    ADD COLUMN IF NOT EXISTS is_procedural BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS procedural_reason TEXT;

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS keep_intervals JSONB;   -- NULL = legacy single window

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
           MAX(st.end_seconds)   AS group_end_seconds,
           SUM(CASE WHEN st.is_procedural THEN st.end_seconds - st.start_seconds ELSE 0 END)
               AS procedural_seconds
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
        gs.procedural_seconds,
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
      AND NOT COALESCE(st.is_procedural, FALSE)  -- procedural-turn exclusion (issue #143)
    ORDER BY stv.output_path, stv.turn_id
) dedup
-- MIN_TURN_UPLOAD_DURATION_SECONDS = 300 (issue #234): a turn video must last at
-- least 5 minutes to be worth the single daily 19:00 UTC slot. The floor measures
-- the PUBLISHED clip (group span minus excised procedural seconds), not the raw
-- span, so a group whose only substance is a 6-minute intervention plus a 15s
-- excised handoff still clears the floor on its real 6-minute content (issue #143).
WHERE dedup.group_end_seconds - dedup.group_start_seconds - dedup.procedural_seconds >= 300
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC, dedup.session_date DESC;

-- DOWN
-- Restore the migration 035 view body (no procedural columns/gate) and drop the
-- three new columns. Order matters: the view must be replaced BEFORE the columns
-- it references are dropped.
--
-- DROP VIEW IF EXISTS uploadable_turns;
-- CREATE VIEW uploadable_turns AS
-- WITH group_spans AS (
--     SELECT stv.output_path,
--            MIN(st.start_seconds) AS group_start_seconds,
--            MAX(st.end_seconds)   AS group_end_seconds
--     FROM speaker_turn_videos stv
--     JOIN speaker_turns st ON stv.turn_id = st.turn_id
--     GROUP BY stv.output_path
-- )
-- SELECT * FROM (
--     SELECT DISTINCT ON (stv.output_path)
--         stv.turn_id, stv.output_path, st.chapter_id, st.resolved_name,
--         st.start_seconds, st.end_seconds, st.interest_score,
--         gs.group_start_seconds, gs.group_end_seconds,
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
--     JOIN group_spans gs ON gs.output_path = stv.output_path
--     WHERE stv.is_uploaded_to_youtube = FALSE
--       AND stv.prepared_at IS NOT NULL
--       AND NOT stv.is_upload_abandoned
--       AND vc.is_uploaded_to_youtube = FALSE
--       AND vc.relevance_score >= 2
--       AND COALESCE(st.interest_score, 1) >= 1
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- WHERE dedup.group_end_seconds - dedup.group_start_seconds >= 300
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC;
--
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS keep_intervals;
-- ALTER TABLE speaker_turns DROP COLUMN IF EXISTS procedural_reason;
-- ALTER TABLE speaker_turns DROP COLUMN IF EXISTS is_procedural;
