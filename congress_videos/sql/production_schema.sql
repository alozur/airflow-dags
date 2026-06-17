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
    published_at TIMESTAMP,
    channel_id VARCHAR(100),

    -- Processing status
    is_processed BOOLEAN DEFAULT FALSE,
    total_chapters INTEGER DEFAULT 0,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

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
    scored_at TIMESTAMP, -- When scoring was performed

    -- Upload tracking
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    youtube_video_id VARCHAR(50), -- YouTube video ID once uploaded as separate video
    youtube_upload_date TIMESTAMP,

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_video_chapters_video_id ON production.video_chapters(video_id);
CREATE INDEX idx_video_chapters_relevance_score ON production.video_chapters(relevance_score DESC);
CREATE INDEX idx_video_chapters_uploaded ON production.video_chapters(is_uploaded_to_youtube);
CREATE INDEX idx_youtube_source_videos_session ON production.youtube_source_videos(session_number, session_date);

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
COMMENT ON VIEW production.uploadable_chapters IS 'Shows chapters eligible for YouTube upload (relevance_score >= 2)';
COMMENT ON VIEW production.chapter_statistics IS 'Provides aggregate statistics about chapters by source video';
