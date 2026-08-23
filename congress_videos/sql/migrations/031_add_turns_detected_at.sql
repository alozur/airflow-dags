-- Migration: Add turns_detected_at progress marker to video_chapters (issue #166)
-- Created: 2026-08-23
-- Depends on: 030_add_prepared_at.sql
--
-- Adds turns_detected_at TIMESTAMPTZ (nullable, default NULL) to video_chapters.
-- NULL = detection not yet attempted; set ONLY by speaker_turns DAG after a
-- successful diarization run (status == "ok", including 0-turn barren chapters).
--
-- select_chapters() cron branch gains two AND-conditions that exclude chapters
-- where turns_detected_at IS NOT NULL or speaker_turns rows already exist,
-- preventing barren chapters from looping indefinitely.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS.

-- UP

ALTER TABLE video_chapters ADD COLUMN IF NOT EXISTS turns_detected_at TIMESTAMPTZ;

-- DOWN
-- ALTER TABLE video_chapters DROP COLUMN IF EXISTS turns_detected_at;
