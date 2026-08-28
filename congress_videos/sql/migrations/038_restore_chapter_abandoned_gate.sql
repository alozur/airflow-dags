-- Migration: Restore is_upload_abandoned gate on uploadable_chapters (issue #251)
-- Created: 2026-08-28
-- Depends on: 021_expose_resolved_participant_slug_in_uploadable_chapters.sql (uploadable_chapters
--             SELECT list + resolved_participant_slug),
--             036_timestamptz_and_fk_hygiene.sql (uploadable_chapters, most recent recreation),
--             011_add_chapter_upload_failure_tracking.sql (video_chapters.is_upload_abandoned
--             column + the original gate this migration restores)
--
-- Migration 011 added `is_upload_abandoned` and gated uploadable_chapters on it after
-- 3 recorded upload failures. Migration 021 recreated the view to add
-- resolved_participant_slug and silently dropped the gate; migration 036 recreated the
-- view again (forced by an unrelated ALTER ... TYPE dependency) and carried the drop
-- forward, verbatim, still without the gate. Chapters that crossed the 3-failure
-- abandonment threshold have remained eligible for repeated upload attempts
-- indefinitely. This migration restores the gate as the third WHERE conjunct,
-- byte-identical to 011's original predicate, with no other change to the view.
--
-- CREATE OR REPLACE VIEW (not DROP + CREATE): only the WHERE clause changes here —
-- column names, order, and types are untouched, which is exactly CREATE OR REPLACE
-- VIEW's precondition. One statement means a manual psql apply (documented path
-- below, autocommit per statement) can never leave a stack with no view at all.
-- Idempotent: safe to re-run.
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so all table/view names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 038_restore_chapter_abandoned_gate.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 038_restore_chapter_abandoned_gate.sql

-- UP

-- SELECT list is IDENTICAL to migrations 021/036 (including resolved_participant_slug
-- as the last column). WHERE gains back the is_upload_abandoned = FALSE predicate
-- that 011 introduced and 021/036 silently dropped.
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
    AND vc.is_upload_abandoned = FALSE
ORDER BY
    ysv.session_date DESC NULLS LAST,
    vc.relevance_score DESC,
    vc.created_at DESC;

-- DOWN
-- Manual psql only — the runner has no automatic rollback. Rollback is
-- forward-only per design: never delete this migration's row from
-- schema_migrations and never DROP the view — ship a 039_* migration instead
-- that re-declares uploadable_chapters with 036's body (gate removed) if the
-- gate ever needs to be reverted. Deleting the schema_migrations row alone
-- would NOT undo the already-applied view definition.
