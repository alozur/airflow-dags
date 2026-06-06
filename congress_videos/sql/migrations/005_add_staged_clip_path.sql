-- Migration: Add staged_clip_path column to video_shorts
-- Created: 2026-06-06
-- Depends on: 004_create_video_shorts.sql
--
-- Adds the staged_clip_path column that stores the local path of the pre-trimmed
-- clip file produced by DAG 1 (reap_clip_preparer). This path is consumed by
-- DAG 2 (reap_processor) when uploading to Reap.
--
-- Run against both development and production schemas as appropriate.
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 005_add_staged_clip_path.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 005_add_staged_clip_path.sql

ALTER TABLE video_shorts ADD COLUMN IF NOT EXISTS staged_clip_path VARCHAR(2048);

COMMENT ON COLUMN video_shorts.staged_clip_path IS
    'Local filesystem path to the pre-trimmed clip file produced by reap_clip_preparer. NULL on legacy rows. Set when DAG 1 inserts a pending row; consumed by DAG 2 at upload time.';
