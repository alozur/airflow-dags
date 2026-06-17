-- PostgreSQL Database Schema for YouTube Video Chapters
-- This schema supports storing scored chapters from YouTube congressional videos
-- All tables are created in the 'development' schema
--
-- Migration: Add chapter tracking for YouTube videos
-- Created: 2025-10-31

-- Create development schema if not exists
CREATE SCHEMA IF NOT EXISTS development;
SET search_path TO development, public;

-- Table: youtube_source_videos
-- Stores YouTube videos that are sources for chapter extraction
CREATE TABLE IF NOT EXISTS development.youtube_source_videos (
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
CREATE TABLE IF NOT EXISTS development.video_chapters (
    chapter_id SERIAL PRIMARY KEY,
    video_id VARCHAR(50) REFERENCES development.youtube_source_videos(video_id) ON DELETE CASCADE,

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

-- Indexes for performance
CREATE INDEX idx_video_chapters_video_id ON development.video_chapters(video_id);
CREATE INDEX idx_video_chapters_relevance_score ON development.video_chapters(relevance_score DESC);
CREATE INDEX idx_video_chapters_uploaded ON development.video_chapters(is_uploaded_to_youtube);
CREATE INDEX idx_youtube_source_videos_session ON development.youtube_source_videos(session_number, session_date);

-- Unique segment per video — backs the ON CONFLICT (video_id, start_time, end_time)
-- upsert in database.save_youtube_chapters_to_db. Mirrors migration 008.
CREATE UNIQUE INDEX IF NOT EXISTS uq_video_chapters_segment
    ON development.video_chapters (video_id, start_time, end_time);

-- View: uploadable_chapters
-- Shows chapters that are eligible for YouTube upload based on relevance score
DROP VIEW IF EXISTS development.uploadable_chapters;
CREATE VIEW development.uploadable_chapters AS
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
FROM development.video_chapters vc
JOIN development.youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE
    vc.is_uploaded_to_youtube = FALSE
    AND vc.relevance_score >= 2  -- Only high-relevance chapters (score >= 4/5)
ORDER BY
    ysv.session_date DESC NULLS LAST,  -- Most recent session first
    vc.relevance_score DESC,           -- Highest score within same session
    vc.created_at DESC;                -- Newest ingestion as tiebreaker

-- View: chapter_statistics
-- Provides statistics about chapters by video
DROP VIEW IF EXISTS development.chapter_statistics;
CREATE VIEW development.chapter_statistics AS
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
FROM development.youtube_source_videos ysv
LEFT JOIN development.video_chapters vc ON ysv.video_id = vc.video_id
GROUP BY ysv.video_id, ysv.video_title, ysv.session_number, ysv.session_date
ORDER BY ysv.session_date DESC, ysv.video_id;

-- Triggers for automatic timestamp updates
CREATE TRIGGER update_youtube_source_videos_updated_at
    BEFORE UPDATE ON development.youtube_source_videos
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_video_chapters_updated_at
    BEFORE UPDATE ON development.video_chapters
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Comments for documentation
COMMENT ON TABLE development.youtube_source_videos IS 'Stores YouTube videos that are sources for chapter extraction (e.g., full plenary sessions)';
COMMENT ON TABLE development.video_chapters IS 'Stores individual chapters extracted from YouTube videos with AI relevance scoring (0-5 scale)';
COMMENT ON COLUMN development.video_chapters.relevance_score IS 'Total relevance score (0-5) = speaker_pts + topic_pts + interest_pts';
COMMENT ON COLUMN development.video_chapters.speaker_relevance_points IS 'Speaker relevance (0-2): Are key political figures involved?';
COMMENT ON COLUMN development.video_chapters.topic_relevance_points IS 'Topic relevance (0-2): Is it a current/hot topic in Spain?';
COMMENT ON COLUMN development.video_chapters.public_interest_points IS 'Public interest (0-1): Could it generate media interest?';
COMMENT ON VIEW development.uploadable_chapters IS 'Shows chapters eligible for YouTube upload (relevance_score >= 4)';
COMMENT ON VIEW development.chapter_statistics IS 'Provides aggregate statistics about chapters by source video';
