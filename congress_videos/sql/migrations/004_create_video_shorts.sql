-- Migration: Add video_shorts table for Reap Video Shorts pipeline
-- Created: 2026-06-05
-- Depends on: video_chapters table (youtube_chapters_schema.sql)
--
-- This migration creates the video_shorts table that tracks the full lifecycle
-- of each Reap-generated clip: from chapter selection through Reap processing,
-- local download, and YouTube Shorts upload.
--
-- Run against both development and production schemas as appropriate.
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 004_create_video_shorts.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 004_create_video_shorts.sql

-- ============================================================
-- TABLE: video_shorts
-- ============================================================
-- One row per Reap clip (a single chapter may produce N clips).
-- A placeholder row is inserted at job-creation time (reap_clip_id=NULL);
-- the sensor inserts one row per downloaded clip with reap_clip_id populated.

CREATE TABLE IF NOT EXISTS video_shorts (
    id                    SERIAL PRIMARY KEY,

    -- Source chapter (1→N relationship: one chapter → many clips)
    chapter_id            INTEGER NOT NULL REFERENCES video_chapters(id) ON DELETE CASCADE,

    -- Pre-trim window applied before sending to Reap (NULL = no pre-trim)
    pretrim_start_secs    FLOAT,
    pretrim_end_secs      FLOAT,
    pretrim_used_srt      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Reap job tracking
    reap_project_id       VARCHAR(255),
    reap_clip_id          VARCHAR(255) UNIQUE,  -- NULL on placeholder row, set when clip is downloaded

    -- Status lifecycle:
    -- pending → processing → downloaded (success)
    -- Terminal failures: failed | credits_exhausted | expired | below_threshold | invalid | error
    reap_status           VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Clip metadata (populated after Reap job completes)
    reap_virality_score   FLOAT,
    reap_clip_url         VARCHAR(2048),
    local_file_path       VARCHAR(2048),

    -- YouTube upload result
    youtube_video_id      VARCHAR(255),
    is_uploaded           BOOLEAN NOT NULL DEFAULT FALSE,

    -- Audit timestamps
    created_at            TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at            TIMESTAMP NOT NULL DEFAULT NOW()
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_video_shorts_chapter_id
    ON video_shorts(chapter_id);

CREATE INDEX IF NOT EXISTS idx_video_shorts_reap_project_id
    ON video_shorts(reap_project_id);

CREATE INDEX IF NOT EXISTS idx_video_shorts_reap_status
    ON video_shorts(reap_status);

CREATE INDEX IF NOT EXISTS idx_video_shorts_reap_clip_id
    ON video_shorts(reap_clip_id);

-- ============================================================
-- DOCUMENTATION COMMENTS
-- ============================================================

COMMENT ON TABLE video_shorts IS
    'Tracks Reap-generated clips per chapter. One row per clip; a placeholder row (reap_clip_id=NULL) is inserted at job-creation time and N clip rows are inserted when the sensor downloads all clips on job completion.';

COMMENT ON COLUMN video_shorts.reap_status IS
    'Lifecycle: pending → processing → downloaded. Terminal failures: failed | credits_exhausted | expired | below_threshold | invalid | error';

COMMENT ON COLUMN video_shorts.reap_clip_id IS
    'NULL on the placeholder row inserted at create_reap_job time. Populated (UNIQUE) when the clip is downloaded by ReapJobSensor.';

COMMENT ON COLUMN video_shorts.pretrim_used_srt IS
    'TRUE if an SRT file was used to select the AI pre-trim window before sending to Reap.';
