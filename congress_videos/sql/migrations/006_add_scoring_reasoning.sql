-- Migration: Add scoring_reasoning column to video_shorts
-- Created: 2026-06-06
-- Depends on: 005_add_staged_clip_path.sql
--
-- Adds scoring_reasoning to store the AI text that explains why a chapter
-- was selected as a candidate clip by reap_clip_preparer.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 006_add_scoring_reasoning.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 006_add_scoring_reasoning.sql

ALTER TABLE video_shorts ADD COLUMN IF NOT EXISTS scoring_reasoning TEXT;

COMMENT ON COLUMN video_shorts.scoring_reasoning IS
    'AI-generated explanation of why this chapter was selected as a clip candidate. NULL on legacy rows or when no reasoning was produced.';
