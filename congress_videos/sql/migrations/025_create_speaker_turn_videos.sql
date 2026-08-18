-- Migration 025: Create speaker_turn_videos idempotency table
--
-- Records each successfully materialized speaker-turn video.
-- Acts as an idempotency guard: turns already present are skipped on re-run.
--
-- One row per speaker turn (UNIQUE on turn_id). Grouped short-turn plans
-- insert one row per constituent turn_id (each referencing the same output_path).
--
-- Migration is idempotent via CREATE TABLE IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS speaker_turn_videos (
    video_id         SERIAL PRIMARY KEY,
    turn_id          INTEGER     NOT NULL REFERENCES speaker_turns(turn_id) ON DELETE CASCADE,
    output_path      TEXT        NOT NULL,
    materialized_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_speaker_turn_videos_turn UNIQUE (turn_id)
);

CREATE INDEX IF NOT EXISTS idx_speaker_turn_videos_turn
    ON speaker_turn_videos (turn_id);

-- Down migration:
-- DROP TABLE IF EXISTS speaker_turn_videos;
