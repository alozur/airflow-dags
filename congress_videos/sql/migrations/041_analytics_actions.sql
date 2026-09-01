-- Migration 041: Analytics checkpoint actions (issues #102 + #185)
-- Created: 2026-09-01
-- Depends on: 019_create_video_thumbnails.sql, 026_create_video_analytics_snapshots.sql
--
-- Adds the write-side columns needed by the new video_analytics_actions DAG:
--   - video_thumbnails.archetype: persists the chosen thumbnail's archetype
--     so a later regeneration can read it back without parsing rendered
--     prompt text (anti-convergence steering).
--   - video_analytics_snapshots.action_detail: JSONB audit payload capturing
--     the video's prior brief/title/archetype BEFORE regeneration is
--     triggered (persist_results upserts in place and would destroy it
--     otherwise), plus the outcome of the action.
--   - A named CHECK constraint on video_analytics_snapshots.action_taken
--     (column already exists, reserved NULL placeholder since migration 026;
--     this is the first CHECK ever added to it). Mirrors 039's idempotent
--     shape (DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT) even though no
--     prior constraint exists, per the design-amendments binding shape.
--
-- Additive only: every existing action_taken value is NULL and satisfies
-- the CHECK (NULL IS NULL OR ... short-circuits true).
--
-- Rollout order is load-bearing: apply FIRST (dev then prod), then deploy —
-- code writing a new action_taken literal against the pre-040 table is fine
-- (no CHECK yet), but code writing action_detail before 040 fails outright.
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so all table names are intentionally UNQUALIFIED.

-- UP

ALTER TABLE video_thumbnails ADD COLUMN IF NOT EXISTS archetype TEXT;

ALTER TABLE video_analytics_snapshots ADD COLUMN IF NOT EXISTS action_detail JSONB;

ALTER TABLE video_analytics_snapshots DROP CONSTRAINT IF EXISTS video_analytics_snapshots_action_taken_check;
ALTER TABLE video_analytics_snapshots
    ADD CONSTRAINT video_analytics_snapshots_action_taken_check
    CHECK (action_taken IS NULL OR action_taken IN (
        'cold_start',
        'ok',
        'capped',
        'in_progress',
        'thumbnail_regenerated',
        'thumbnail_and_title_regenerated',
        'failed'
    ));

-- DOWN
-- Manual psql only — the runner has no automatic rollback. Published
-- YouTube changes are NOT auto-reverted by dropping these columns; the
-- action_detail audit trail is simply lost.
--
-- ALTER TABLE video_analytics_snapshots DROP CONSTRAINT IF EXISTS video_analytics_snapshots_action_taken_check;
-- ALTER TABLE video_analytics_snapshots DROP COLUMN IF EXISTS action_detail;
-- ALTER TABLE video_thumbnails DROP COLUMN IF EXISTS archetype;
