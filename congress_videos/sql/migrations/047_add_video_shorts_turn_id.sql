-- Migration 047: reference the source speaker turn from video_shorts (issue #467)
-- Created: 2026-09-08
-- Depends on: 025_create_speaker_turn_videos.sql, 004_create_video_shorts.sql
--
-- Additive and nullable: legacy rows keep turn_id = NULL and every existing
-- INSERT/UPDATE keeps working. No backfill. No view is recreated —
-- uploadable_turns does not touch video_shorts, so the 044 lockstep guard in
-- tests/congress_videos/sql/test_production_schema.py stays valid untouched.
-- Runner does `SET search_path TO {schema}, public` — names are UNQUALIFIED.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.

-- UP

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS turn_id INTEGER
        REFERENCES speaker_turn_videos(turn_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_video_shorts_turn_id ON video_shorts(turn_id);

COMMENT ON COLUMN video_shorts.turn_id IS
    'Representative speaker_turn_videos.turn_id this short was cut from (issue #467); NULL = legacy chapter-sourced row';

-- DOWN
-- Manual psql only -- the runner executes the WHOLE file in ONE transaction and
-- has no automatic rollback, so this block MUST stay commented (046 convention).
-- DROP INDEX IF EXISTS idx_video_shorts_turn_id;
-- ALTER TABLE video_shorts DROP COLUMN IF EXISTS turn_id;
