-- Migration 026: Create video_analytics_snapshots table
--
-- Stores YouTube Analytics metrics at fixed post-upload checkpoints
-- (24h, 48h, 7d, 30d, 90d) for each uploaded chapter video.
--
-- Exactly-once persistence is enforced by the UNIQUE constraint on
-- (youtube_video_id, checkpoint). The analytics DAG queries pending
-- pairs (NOT EXISTS) before calling the Analytics API.
--
-- action_taken is a reserved NULL placeholder for future threshold
-- evaluation (#102) and is never written by this change.
--
-- Migration is idempotent via CREATE TABLE IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS video_analytics_snapshots (
    snapshot_id       SERIAL PRIMARY KEY,
    chapter_id        INTEGER      NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    youtube_video_id  VARCHAR(50)  NOT NULL,
    checkpoint        TEXT         NOT NULL,          -- '24h'|'48h'|'7d'|'30d'|'90d'
    metrics           JSONB        NOT NULL,          -- config.analytics_config.METRIC_FIELDS: {views, estimatedMinutesWatched, averageViewDuration, averageViewPercentage, likes, dislikes, comments, shares, subscribersGained, subscribersLost}
    action_taken      TEXT,                           -- reserved NULL placeholder for #102; never written by this change
    collected_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_video_analytics_snapshot UNIQUE (youtube_video_id, checkpoint)
);

CREATE INDEX IF NOT EXISTS idx_video_analytics_chapter
    ON video_analytics_snapshots (chapter_id);

-- Down migration:
-- DROP TABLE IF EXISTS video_analytics_snapshots;
