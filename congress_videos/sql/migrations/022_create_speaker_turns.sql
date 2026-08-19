-- Migration 022: Create speaker_turns table
--
-- Stores speaker-turn boundaries detected within video_chapters.
-- Each row represents one resolved speaker turn with source signal (acoustic,
-- text_confirmed, text_named), confidence, and optional resolved participant name.
--
-- Upsert on (chapter_id, start_seconds) makes re-runs idempotent.

CREATE TABLE IF NOT EXISTS speaker_turns (
    turn_id       SERIAL PRIMARY KEY,
    chapter_id    INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    start_seconds NUMERIC NOT NULL,
    end_seconds   NUMERIC NOT NULL,
    speaker_label TEXT    NOT NULL,
    resolved_name TEXT,
    confidence    NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    source        TEXT    NOT NULL CHECK (source IN ('acoustic','text_confirmed','text_named')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chapter_id, start_seconds)
);

CREATE INDEX IF NOT EXISTS idx_speaker_turns_chapter ON speaker_turns(chapter_id);
CREATE INDEX IF NOT EXISTS idx_speaker_turns_name    ON speaker_turns(resolved_name);
