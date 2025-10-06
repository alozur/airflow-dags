-- PostgreSQL Database Schema for Congressional Video Management
-- This schema supports OpenAI classification, upload tracking, and age-based filtering
-- All tables are created in the 'development' schema

-- Create development schema
CREATE SCHEMA IF NOT EXISTS development;
SET search_path TO development, public;

-- Table: congressional_sessions
-- Stores session metadata
CREATE TABLE IF NOT EXISTS development.congressional_sessions (
    session_number INTEGER PRIMARY KEY, -- Now the primary key instead of id
    session_date DATE NOT NULL,
    target_date DATE NOT NULL, -- Original target date used for processing
    session_url VARCHAR(500),
    total_topics INTEGER DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Indexes for performance
    CONSTRAINT unique_session_date UNIQUE(session_date)
);

-- Table: video_topics
-- Stores individual topic videos with metadata and classification
CREATE TABLE IF NOT EXISTS development.video_topics (
    entry_id VARCHAR(100) PRIMARY KEY, -- from congreso website - now the primary key
    session_number INTEGER REFERENCES development.congressional_sessions(session_number) ON DELETE CASCADE,

    -- Video identification
    topic_title TEXT,
    video_url VARCHAR(500),
    video_file_path VARCHAR(500), -- local download path

    -- Speaker information
    speaker_name VARCHAR(200),
    role VARCHAR(200),
    profile_link VARCHAR(500),
    main_topic_entry_id VARCHAR(100) REFERENCES development.video_topics(entry_id) ON DELETE CASCADE, -- Links interventions to their main topic

    -- AI Interest Evaluation for YouTube Upload
    ai_interest_score INTEGER CHECK (ai_interest_score BETWEEN 1 AND 10), -- 1-10 interest score for YouTube
    ai_interest_reasoning TEXT, -- AI reasoning for the interest score
    ai_interest_evaluated_at TIMESTAMP, -- when the interest evaluation was performed

    -- YouTube Upload Management
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    youtube_video_id VARCHAR(50), -- YouTube video ID once uploaded
    youtube_upload_date TIMESTAMP,
    upload_eligible BOOLEAN DEFAULT FALSE, -- only TRUE for main topics eligible for upload
    is_main_topic BOOLEAN DEFAULT FALSE, -- indicates if this is a main topic

    -- YouTube Metadata (generated content for upload)
    youtube_title TEXT, -- AI-generated YouTube title
    youtube_description TEXT, -- AI-generated YouTube description
    youtube_metadata_generated_at TIMESTAMP, -- when metadata was generated

    -- Note: Age-based filtering handled in views and queries
    -- Cannot use generated column with subqueries in PostgreSQL

    -- Metadata
    file_size_bytes BIGINT,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP

    -- Note: entry_id is now the primary key, so no need for unique constraint
);

-- Table: upload_queue
-- Manages the queue of videos ready for YouTube upload
CREATE TABLE IF NOT EXISTS development.upload_queue (
    video_topic_entry_id VARCHAR(100) PRIMARY KEY REFERENCES development.video_topics(entry_id) ON DELETE CASCADE,
    queue_priority INTEGER DEFAULT 5, -- 1 (highest) to 10 (lowest)
    queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    attempted_uploads INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP,
    upload_status VARCHAR(20) DEFAULT 'pending' CHECK (upload_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    error_message TEXT
);

-- Indexes for performance
-- Note: Session date index handled by JOIN queries, no functional index needed
CREATE INDEX idx_video_topics_upload_status ON development.video_topics(is_uploaded_to_youtube, upload_eligible);
CREATE INDEX idx_upload_queue_status ON development.upload_queue(upload_status, queue_priority);

-- View: uploadable_videos
-- Shows videos that are eligible for YouTube upload (from upload_queue)
DROP VIEW IF EXISTS development.uploadable_videos;
CREATE VIEW development.uploadable_videos AS
SELECT
    vt.entry_id,
    vt.video_file_path,
    vt.ai_interest_score,
    vt.ai_interest_reasoning,
    vt.youtube_title,
    vt.youtube_description,
    uq.queue_priority,
    uq.upload_status,
    uq.queued_at,
    uq.attempted_uploads,
    uq.last_attempt_at,
    CURRENT_DATE - cs.session_date AS days_old,
    -- Effective priority: queue_priority adjusted by video age
    -- Newer videos get higher priority, older videos get downgraded
    -- Loses 0.2 priority points per day (5 days = -1 point, 10 days = -2 points)
    CAST(uq.queue_priority - ((CURRENT_DATE - cs.session_date) * 0.2) AS NUMERIC(5,2)) AS effective_priority
FROM development.upload_queue uq
JOIN development.video_topics vt ON uq.video_topic_entry_id = vt.entry_id
JOIN development.congressional_sessions cs ON vt.session_number = cs.session_number
WHERE
    uq.upload_status IN ('pending', 'failed')  -- Only pending or failed (for retry)
    AND vt.is_uploaded_to_youtube = FALSE
    AND vt.upload_eligible = TRUE
    AND vt.is_main_topic = TRUE  -- Only main topics, not interventions
ORDER BY effective_priority DESC, uq.queued_at ASC; 

-- Function: update_timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for automatic timestamp updates
CREATE TRIGGER update_congressional_sessions_updated_at
    BEFORE UPDATE ON development.congressional_sessions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_video_topics_updated_at
    BEFORE UPDATE ON development.video_topics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();