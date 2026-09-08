-- Migration 049: freshness bucket as the leading publish-order key for
-- uploadable_turns (issue #513)
-- Created: 2026-09-08
-- Depends on: 044_deterministic_turn_publish_order.sql
--
-- uploadable_turns is the only view in this repo whose INTERNAL ORDER BY governs
-- runtime behaviour: get_uploadable_turns() runs `SELECT * FROM uploadable_turns
-- LIMIT %s` with no external ordering, and the single row it returns wins the one
-- daily long-form slot (DAILY_LONG_FORM_UPLOAD_LIMIT = 1).
--
-- Congressional content is news-shaped: its value decays. Ranking purely on the 044
-- editorial keys parked a 2026-09-04 turn behind three 2026-06-10 turns in the live
-- prod queue, so a clip about a session from days ago waited at least three days
-- while three-month-old material published first (measured 2026-09-08, issue #513).
--
-- Prepends ONE key and nothing else:
--   (dedup.session_date >= CURRENT_DATE - INTERVAL '14 days') DESC
-- Boolean DESC puts TRUE first in Postgres, so every turn from a session in the last
-- 14 days outranks every older turn. Within each bucket the 044 order is unchanged.
--
-- The 14-day cliff is deliberate: a 15-day turn ranks below a 13-day one regardless
-- of quality. That is the point — a hard bucket stays deterministic and exactly
-- testable, which a blended recency decay would not.
--
-- CURRENT_DATE is safe here: uploadable_turns is a PLAIN view, re-planned and
-- re-executed on every query, so a STABLE function carries no materialization or
-- expression-index immutability risk. session_date is a plain DATE column, so the
-- comparison is DATE vs DATE. Precedent: this repo already ships CURRENT_DATE inside
-- plain view definitions (migrations 007/010/011/021/036/038).
--
-- One-time effect on apply: backlog rows at the 14-day edge reshuffle at once and
-- recent turns jump ahead of the aged queue. Intended, bounded and self-resolving —
-- the aged queue resumes draining in its existing order once the fresh bucket empties.
--
-- ELIGIBILITY-NEUTRAL. The view body is carried forward from 044 VERBATIM — the
-- unfiltered group_spans CTE (issue #151 trap), the DISTINCT ON (stv.output_path)
-- dedup (028), every inner WHERE gate (030 prepared_at, 141 abandon, chapter
-- upload gate, relevance >= 2, interest >= 1, 143 procedural exclusion) and the
-- 300s published-duration floor (234/143) are unchanged. No row enters or leaves
-- the view; only the sequence changes.
--
-- Convention: DROP VIEW + CREATE VIEW, as every prior uploadable_turns migration
-- (025/028/029/030/032/040/044). CREATE OR REPLACE is uploadable_chapters'
-- convention (038) and cannot change a view's column list in Postgres.
--
-- Idempotent: DROP VIEW IF EXISTS + CREATE VIEW are safe to re-run.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

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
-- FRESHNESS BUCKET (issue #513). Congressional content is news-shaped and its value
-- decays: ranking purely on the editorial keys below parked a 2026-09-04 turn behind
-- three 2026-06-10 turns. TRUE sorts before FALSE under DESC, so any turn from a
-- session in the last 14 days outranks every older turn. The 14-day cliff is
-- deliberate, and within each bucket the 044 keys below are byte-for-byte unchanged.
ORDER BY (dedup.session_date >= CURRENT_DATE - INTERVAL '14 days') DESC,  -- freshness bucket (issue #513)
         COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC,
         dedup.session_date DESC,
         -- FIFO tie-break (issue #328). Once the three editorial keys are exhausted
         -- every remaining clip has already cleared the full eligibility gauntlet and
         -- is equally publishable, so the honest default is first-prepared-first-
         -- published: with N tied clips and a 1/day drain, every clip publishes within
         -- N days. This ordering is DELIBERATE, not accidental — do not drop it.
         dedup.materialized_at ASC,
         -- Total-order backstop: NOW() is transaction-scoped, so clips materialized in
         -- one batch share an identical materialized_at. turn_id is UNIQUE on
         -- speaker_turn_videos (025) and the outer result holds one row per
         -- output_path, so this key makes LIMIT 1 deterministic by contract.
         dedup.turn_id ASC;

-- DOWN
-- Manual psql only -- the runner has no automatic rollback, and it executes the
-- WHOLE file text in one transaction, so this block MUST stay commented out.
-- Restores the migration 044 view body (five-key ORDER BY, no freshness bucket).
-- Order-only rollback: no data is lost and no row's eligibility changes -- the queue
-- simply returns to stale-first editorial ranking.
--
-- DROP VIEW IF EXISTS uploadable_turns;
-- CREATE VIEW uploadable_turns AS
-- WITH group_spans AS (
--     SELECT stv.output_path,
--            MIN(st.start_seconds) AS group_start_seconds,
--            MAX(st.end_seconds)   AS group_end_seconds,
--            SUM(CASE WHEN st.is_procedural THEN st.end_seconds - st.start_seconds ELSE 0 END)
--                AS procedural_seconds
--     FROM speaker_turn_videos stv
--     JOIN speaker_turns st ON stv.turn_id = st.turn_id
--     GROUP BY stv.output_path
-- )
-- SELECT * FROM (
--     SELECT DISTINCT ON (stv.output_path)
--         stv.turn_id, stv.output_path, st.chapter_id, st.resolved_name,
--         st.start_seconds, st.end_seconds, st.interest_score,
--         gs.group_start_seconds, gs.group_end_seconds, gs.procedural_seconds,
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
--       AND NOT COALESCE(st.is_procedural, FALSE)
--     ORDER BY stv.output_path, stv.turn_id
-- ) dedup
-- WHERE dedup.group_end_seconds - dedup.group_start_seconds - dedup.procedural_seconds >= 300
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC,
--          dedup.materialized_at ASC, dedup.turn_id ASC;
