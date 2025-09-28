-- PostgreSQL Database Schema for Congressional Video Management
-- This schema supports OpenAI classification, upload tracking, and age-based filtering

-- Table: congressional_sessions
-- Stores session metadata
CREATE TABLE IF NOT EXISTS congressional_sessions (
    id SERIAL PRIMARY KEY,
    session_number INTEGER NOT NULL UNIQUE,
    session_date DATE NOT NULL,
    target_date DATE NOT NULL, -- Original target date used for processing
    session_url VARCHAR(500),
    total_topics INTEGER DEFAULT 0,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Indexes for performance
    CONSTRAINT unique_session_date UNIQUE(session_date, session_number)
);

-- Table: video_topics
-- Stores individual topic videos with metadata and classification
CREATE TABLE IF NOT EXISTS video_topics (
    id SERIAL PRIMARY KEY,
    session_id INTEGER REFERENCES congressional_sessions(id) ON DELETE CASCADE,

    -- Video identification
    entry_id VARCHAR(100) NOT NULL, -- from congreso website
    topic_title TEXT,
    topic_content TEXT,
    video_url VARCHAR(500),
    video_file_path VARCHAR(500), -- local download path

    -- OpenAI Classification
    openai_category VARCHAR(100), -- classified category (e.g., "Economy", "Health", "Education")
    openai_summary TEXT, -- AI-generated summary
    openai_keywords TEXT[], -- array of keywords
    openai_priority_score INTEGER CHECK (openai_priority_score BETWEEN 1 AND 10), -- 1-10 priority
    openai_processed_at TIMESTAMP,

    -- YouTube Upload Management
    is_uploaded_to_youtube BOOLEAN DEFAULT FALSE,
    youtube_video_id VARCHAR(50), -- YouTube video ID once uploaded
    youtube_upload_date TIMESTAMP,
    upload_eligible BOOLEAN DEFAULT TRUE, -- can be set to false manually

    -- Note: Age-based filtering handled in views and queries
    -- Cannot use generated column with subqueries in PostgreSQL

    -- Metadata
    file_size_bytes BIGINT,
    duration_seconds INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Constraints
    CONSTRAINT unique_topic_per_session UNIQUE(session_id, entry_id)
);

-- Table: upload_queue
-- Manages the queue of videos ready for YouTube upload
CREATE TABLE IF NOT EXISTS upload_queue (
    id SERIAL PRIMARY KEY,
    video_topic_id INTEGER REFERENCES video_topics(id) ON DELETE CASCADE,
    queue_priority INTEGER DEFAULT 5, -- 1 (highest) to 10 (lowest)
    queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    attempted_uploads INTEGER DEFAULT 0,
    last_attempt_at TIMESTAMP,
    upload_status VARCHAR(20) DEFAULT 'pending' CHECK (upload_status IN ('pending', 'processing', 'completed', 'failed', 'skipped')),
    error_message TEXT
);

-- Indexes for performance
-- Note: Session date index handled by JOIN queries, no functional index needed
CREATE INDEX idx_video_topics_upload_status ON video_topics(is_uploaded_to_youtube, upload_eligible);
CREATE INDEX idx_video_topics_openai_category ON video_topics(openai_category);
CREATE INDEX idx_upload_queue_status ON upload_queue(upload_status, queue_priority);

-- View: uploadable_videos
-- Shows videos that are eligible for YouTube upload
CREATE OR REPLACE VIEW uploadable_videos AS
SELECT
    vt.id,
    vt.entry_id,
    vt.topic_title,
    vt.openai_category,
    vt.openai_priority_score,
    cs.session_date,
    cs.session_number,
    vt.video_file_path,
    vt.file_size_bytes,
    CURRENT_DATE - cs.session_date AS days_old
FROM video_topics vt
JOIN congressional_sessions cs ON vt.session_id = cs.id
WHERE
    vt.is_uploaded_to_youtube = FALSE
    AND vt.upload_eligible = TRUE
    AND cs.session_date >= CURRENT_DATE - INTERVAL '14 days'
    AND vt.video_file_path IS NOT NULL
    AND vt.openai_processed_at IS NOT NULL
ORDER BY vt.openai_priority_score ASC, cs.session_date DESC;

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
    BEFORE UPDATE ON congressional_sessions
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_video_topics_updated_at
    BEFORE UPDATE ON video_topics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();