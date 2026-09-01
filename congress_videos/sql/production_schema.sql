-- PostgreSQL Database Schema for YouTube Video Chapters (Production)
-- This schema supports storing scored chapters from YouTube congressional videos
-- All tables are created in the 'production' schema

-- Create production schema
CREATE SCHEMA IF NOT EXISTS production;
SET search_path TO production, public;

-- ============================================================
-- SHARED TRIGGER FUNCTION
-- ============================================================

-- Function: update_timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- ============================================================
-- TABLES
-- ============================================================

-- Table: youtube_source_videos
-- Stores YouTube videos that are sources for chapter extraction
CREATE TABLE IF NOT EXISTS production.youtube_source_videos (
    video_id VARCHAR(50) PRIMARY KEY, -- YouTube video ID (e.g., 'ZBU0bVpYXM4')
    video_title VARCHAR(500),
    video_url VARCHAR(500),

    -- Session linkage (optional - for congressional videos)
    -- No foreign key constraint - stores session number for reference only
    session_number INTEGER,
    session_date DATE,

    -- Video metadata
    duration_seconds INTEGER,
    published_at TIMESTAMPTZ,
    channel_id VARCHAR(100),

    -- Processing status
    is_processed BOOLEAN DEFAULT FALSE,
    total_chapters INTEGER DEFAULT 0,

    -- Integrity gate: set to NOW()+12h when ffprobe detects a corrupt/incomplete download.
    -- filter_unprocessed_videos skips rows where download_retry_after > NOW() so the video
    -- is retried after the VOD has had time to finalise on YouTube.
    download_retry_after TIMESTAMPTZ DEFAULT NULL,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,

    -- Indexes
    CONSTRAINT unique_video_id UNIQUE(video_id)
);

-- Table: video_chapters
-- Stores individual chapters extracted from YouTube videos with AI relevance scoring
CREATE TABLE IF NOT EXISTS production.video_chapters (
    chapter_id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) REFERENCES production.youtube_source_videos(video_id) ON DELETE CASCADE,

    -- Chapter identification
    title TEXT NOT NULL,
    description TEXT,

    -- Timing information (SRT format timestamps)
    start_time VARCHAR(20), -- Format: "HH:MM:SS,mmm" (e.g., "00:10:15,500")
    end_time VARCHAR(20), -- Format: "HH:MM:SS,mmm"
    duration_minutes NUMERIC(10, 2),

    -- Content metadata
    speakers TEXT[], -- Array of speaker names
    topics TEXT[], -- Array of topic keywords
    timeline JSONB DEFAULT '[]'::jsonb, -- Key moments [{time, speaker, content}] with absolute source-video timestamps

    -- AI Relevance Scoring (0-5 scale, sum of 3 criteria)
    -- Score calculation: speaker_relevance_pts + topic_relevance_pts + public_interest_pts
    relevance_score INTEGER CHECK (relevance_score BETWEEN 0 AND 5),

    -- Individual scoring criteria breakdown
    speaker_relevance_points INTEGER CHECK (speaker_relevance_points BETWEEN 0 AND 2), -- Key political figures?
    topic_relevance_points INTEGER CHECK (topic_relevance_points BETWEEN 0 AND 2), -- Current/hot topic?
    public_interest_points INTEGER CHECK (public_interest_points BETWEEN 0 AND 1), -- Media interest potential?

    -- AI scoring details
    scoring_reasoning TEXT, -- AI justification for the score
    key_speakers TEXT[], -- Key speakers identified by AI
    is_current_topic BOOLEAN DEFAULT FALSE, -- Is this a current/hot topic?
    scoring_error TEXT, -- Error message if scoring failed
    scored_at TIMESTAMPTZ, -- When scoring was performed

    -- Upload tracking
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    youtube_video_id VARCHAR(50), -- YouTube video ID once uploaded as separate video
    youtube_upload_date TIMESTAMPTZ,

    -- Upload failure tracking (soft-delete after repeated failures)
    upload_attempts INTEGER DEFAULT 0,
    is_upload_abandoned BOOLEAN DEFAULT FALSE,
    last_upload_error TEXT,

    -- Timestamps
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

-- Table: video_shorts
-- Tracks Reap-generated clips per chapter (folds migrations 004+005+006+012)
CREATE TABLE IF NOT EXISTS production.video_shorts (
    id                    SERIAL PRIMARY KEY,
    chapter_id            INTEGER NOT NULL REFERENCES production.video_chapters(chapter_id) ON DELETE CASCADE,

    -- Pre-trim window applied before sending to Reap (NULL = no pre-trim)
    pretrim_start_secs    FLOAT,
    pretrim_end_secs      FLOAT,
    pretrim_used_srt      BOOLEAN NOT NULL DEFAULT FALSE,

    -- Reap job tracking
    reap_project_id       VARCHAR(255),
    reap_clip_id          VARCHAR(255) UNIQUE,
    reap_status           VARCHAR(50) NOT NULL DEFAULT 'pending',

    -- Clip metadata (populated after Reap job completes)
    reap_virality_score   FLOAT,
    reap_clip_url         VARCHAR(2048),
    local_file_path       VARCHAR(2048),

    -- YouTube upload result
    youtube_video_id      VARCHAR(255),
    is_uploaded            BOOLEAN NOT NULL DEFAULT FALSE,

    -- Audit timestamps
    created_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP NOT NULL DEFAULT NOW(),

    -- Added by migration 005
    staged_clip_path        VARCHAR(2048),

    -- Added by migration 006
    scoring_reasoning       TEXT,

    -- Added by migration 012 (upload failure tracking)
    upload_attempts         INTEGER DEFAULT 0,
    is_upload_abandoned     BOOLEAN DEFAULT FALSE,
    last_upload_error       TEXT
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_video_chapters_video_id ON production.video_chapters(video_id);
CREATE INDEX idx_video_chapters_relevance_score ON production.video_chapters(relevance_score DESC);
CREATE INDEX idx_video_chapters_uploaded ON production.video_chapters(is_uploaded_to_youtube);
CREATE INDEX idx_youtube_source_videos_session ON production.youtube_source_videos(session_number, session_date);

CREATE INDEX idx_video_shorts_chapter_id ON production.video_shorts(chapter_id);
CREATE INDEX idx_video_shorts_reap_project_id ON production.video_shorts(reap_project_id);
CREATE INDEX idx_video_shorts_reap_status ON production.video_shorts(reap_status);
CREATE INDEX idx_video_shorts_reap_clip_id ON production.video_shorts(reap_clip_id);
CREATE INDEX idx_video_shorts_uploaded_recent ON production.video_shorts(updated_at DESC) WHERE is_uploaded = TRUE;
CREATE INDEX idx_video_shorts_pending_downloaded ON production.video_shorts(reap_virality_score DESC NULLS LAST) WHERE is_uploaded = FALSE AND is_upload_abandoned = FALSE AND local_file_path IS NOT NULL AND reap_status = 'downloaded';

-- ============================================================
-- VIEWS
-- ============================================================

-- View: uploadable_chapters
-- Shows chapters that are eligible for YouTube upload based on relevance score
DROP VIEW IF EXISTS production.uploadable_chapters;
CREATE VIEW production.uploadable_chapters AS
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
    -- Calculate days since chapter was identified
    CURRENT_DATE - DATE(vc.created_at) AS days_since_created
FROM production.video_chapters vc
JOIN production.youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE
    vc.is_uploaded_to_youtube = FALSE
    AND vc.relevance_score >= 2  -- Only high-relevance chapters (score >= 2/5)
    AND vc.is_upload_abandoned = FALSE
ORDER BY
    vc.relevance_score DESC,  -- Higher relevance score first
    vc.created_at DESC;        -- Newer chapters first

-- View: chapter_statistics
-- Provides statistics about chapters by video
DROP VIEW IF EXISTS production.chapter_statistics;
CREATE VIEW production.chapter_statistics AS
SELECT
    ysv.video_id,
    ysv.video_title,
    ysv.session_number,
    ysv.session_date,
    COUNT(vc.chapter_id) AS total_chapters,
    COUNT(CASE WHEN vc.relevance_score >= 4 THEN 1 END) AS high_relevance_chapters,
    COUNT(CASE WHEN vc.relevance_score = 3 THEN 1 END) AS medium_relevance_chapters,
    COUNT(CASE WHEN vc.relevance_score <= 2 THEN 1 END) AS low_relevance_chapters,
    COUNT(CASE WHEN vc.is_uploaded_to_youtube = TRUE THEN 1 END) AS uploaded_chapters,
    ROUND(AVG(vc.relevance_score), 2) AS avg_relevance_score,
    ROUND(AVG(vc.duration_minutes), 2) AS avg_chapter_duration_minutes,
    MAX(vc.relevance_score) AS max_relevance_score,
    MIN(vc.relevance_score) AS min_relevance_score
FROM production.youtube_source_videos ysv
LEFT JOIN production.video_chapters vc ON ysv.video_id = vc.video_id
GROUP BY ysv.video_id, ysv.video_title, ysv.session_number, ysv.session_date
ORDER BY ysv.session_date DESC, ysv.video_id;

-- ============================================================
-- TRIGGERS
-- ============================================================

CREATE TRIGGER update_youtube_source_videos_updated_at
    BEFORE UPDATE ON production.youtube_source_videos
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_video_chapters_updated_at
    BEFORE UPDATE ON production.video_chapters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================
-- DOCUMENTATION COMMENTS
-- ============================================================

COMMENT ON TABLE production.youtube_source_videos IS 'Stores YouTube videos that are sources for chapter extraction (e.g., full plenary sessions)';
COMMENT ON TABLE production.video_chapters IS 'Stores individual chapters extracted from YouTube videos with AI relevance scoring (0-5 scale)';
COMMENT ON COLUMN production.video_chapters.relevance_score IS 'Total relevance score (0-5) = speaker_pts + topic_pts + interest_pts';
COMMENT ON COLUMN production.video_chapters.speaker_relevance_points IS 'Speaker relevance (0-2): Are key political figures involved?';
COMMENT ON COLUMN production.video_chapters.topic_relevance_points IS 'Topic relevance (0-2): Is it a current/hot topic in Spain?';
COMMENT ON COLUMN production.video_chapters.public_interest_points IS 'Public interest (0-1): Could it generate media interest?';
COMMENT ON COLUMN production.video_chapters.upload_attempts IS 'Cumulative count of recorded per-chapter upload FAILURES (successes never touch it)';
COMMENT ON COLUMN production.video_chapters.is_upload_abandoned IS 'TRUE once upload_attempts reaches the abandon threshold (3); excludes the chapter from uploadable_chapters';
COMMENT ON COLUMN production.video_chapters.last_upload_error IS 'Last recorded per-chapter upload failure message';
COMMENT ON VIEW production.uploadable_chapters IS 'Shows chapters eligible for YouTube upload (relevance_score >= 2)';
COMMENT ON VIEW production.chapter_statistics IS 'Provides aggregate statistics about chapters by source video';

-- View: uploadable_turns (migration 040)
-- Shows speaker_turn_videos rows that are PREPARED and not yet uploaded.
-- Cumulative lineage — this block must stay in lockstep with the LATEST view migration
-- under congress_videos/sql/migrations/ (guarded by tests/congress_videos/sql/test_production_schema.py):
--   028 DISTINCT ON (stv.output_path) grouped-turn dedup
--   029 interest_score column + priority ordering
--   030 stv.prepared_at TIMESTAMPTZ + IS NOT NULL readiness gate (issue #146)
--   032 NOT stv.is_upload_abandoned gate (issue #141)
--   034 resolved_participant_slug / speaker_resolution_confidence / speaker_resolution_method
--   035 group_spans CTE + 300s minimum grouped-clip duration floor (issue #234)
--   040 NOT COALESCE(st.is_procedural, FALSE) exclusion + procedural_seconds floor
--       adjustment — the published clip (span minus excised procedural spans),
--       not the raw span, must clear 300s (issue #143)

DROP VIEW IF EXISTS production.uploadable_turns;
CREATE VIEW production.uploadable_turns AS
WITH group_spans AS (
    -- Wall-clock span of every materialized turn video, over ALL speaker_turn_videos
    -- rows sharing an output_path. Grouped clips (issue #129) hold N rows per file.
    -- This aggregate is deliberately UNFILTERED: the eligibility WHERE below is applied
    -- after DISTINCT ON (stv.output_path), which keeps a single sibling row per clip.
    -- Computing the span after those gates (e.g. with MIN/MAX OVER (PARTITION BY ...),
    -- which Postgres evaluates AFTER WHERE) would collapse it to that one turn's
    -- narrow window and re-introduce the issue #151 bug class. is_procedural is read
    -- here only to sum excised durations, never to gate which rows enter the aggregate.
    SELECT stv.output_path,
           MIN(st.start_seconds) AS group_start_seconds,
           MAX(st.end_seconds)   AS group_end_seconds,
           SUM(CASE WHEN st.is_procedural THEN st.end_seconds - st.start_seconds ELSE 0 END)
               AS procedural_seconds
    FROM production.speaker_turn_videos stv
    JOIN production.speaker_turns st ON stv.turn_id = st.turn_id
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
    FROM production.speaker_turn_videos stv
    JOIN production.speaker_turns st ON stv.turn_id = st.turn_id
    JOIN production.video_chapters vc ON st.chapter_id = vc.chapter_id
    JOIN production.youtube_source_videos ysv ON vc.video_id = ysv.video_id
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
-- MIN_TURN_UPLOAD_DURATION_SECONDS = 300 (issue #234): a turn video must last at least
-- 5 minutes to be worth the single daily 19:00 UTC slot. Documented literal, NOT
-- runtime-tunable — changing the floor requires a new migration. The floor measures
-- the PUBLISHED clip (group span minus excised procedural seconds), not the raw
-- span (issue #143).
WHERE dedup.group_end_seconds - dedup.group_start_seconds - dedup.procedural_seconds >= 300
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC, dedup.session_date DESC;

COMMENT ON VIEW production.uploadable_turns IS 'Speaker turn videos eligible for YouTube upload — prepared_at IS NOT NULL (issue #146), NOT is_upload_abandoned (issue #141), NOT is_procedural (issue #143), and published clip duration (span minus excised procedural seconds) >= 300s (issue #234/#143)';
