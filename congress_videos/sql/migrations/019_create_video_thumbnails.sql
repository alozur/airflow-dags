-- Migration 019: Create video_thumbnails table
--
-- Stores Pikzels-generated thumbnail options for each chapter, including
-- the chosen option's OpenAI-generated title.  One run produces exactly
-- 2 rows per chapter_id (one per option label).
--
-- Upsert on (chapter_id, label) makes re-runs idempotent.

CREATE TABLE IF NOT EXISTS video_thumbnails (
    thumbnail_id      SERIAL PRIMARY KEY,
    chapter_id        INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    youtube_video_id  VARCHAR(50),
    label             TEXT NOT NULL,
    style             TEXT,
    prompt            TEXT,
    main_score        NUMERIC(6,3),
    local_path        TEXT NOT NULL,
    output_url        TEXT,
    openai_title      TEXT,
    is_chosen         BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_video_thumbnails_chapter_label UNIQUE (chapter_id, label)
);

CREATE INDEX IF NOT EXISTS idx_video_thumbnails_chapter
    ON video_thumbnails (chapter_id);

CREATE INDEX IF NOT EXISTS idx_video_thumbnails_chosen
    ON video_thumbnails (chapter_id, is_chosen);
