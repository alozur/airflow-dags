-- Migration 023: Create speaker_turn_trim_proposals table
--
-- Stores non-destructive, auditable trim proposals for silence and applause
-- zones detected within speaker turns. Each row represents a candidate interval
-- that *may* be trimmed; nothing is ever cut automatically.
--
-- Proposals carry:
--   - tipo:        'silence' | 'applause'
--   - score:       detector confidence (nullable; silence proposals omit it)
--   - source:      detection backend identifier (e.g. 'vad_webrtc', 'yamnet_tflite')
--   - is_voice_free: voice-gate result set before persistence; proposals where
--                    this is FALSE are kept for auditability but never executed
--
-- Upsert on (turn_id, start_seconds, tipo) makes re-runs idempotent.

CREATE TABLE IF NOT EXISTS speaker_turn_trim_proposals (
    proposal_id   SERIAL PRIMARY KEY,
    turn_id       INTEGER NOT NULL REFERENCES speaker_turns(turn_id) ON DELETE CASCADE,
    start_seconds NUMERIC NOT NULL,
    end_seconds   NUMERIC NOT NULL,
    tipo          TEXT    NOT NULL CHECK (tipo IN ('silence', 'applause')),
    score         NUMERIC,
    source        TEXT    NOT NULL,
    is_voice_free BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (turn_id, start_seconds, tipo)
);

CREATE INDEX IF NOT EXISTS idx_trim_proposals_turn ON speaker_turn_trim_proposals (turn_id);
CREATE INDEX IF NOT EXISTS idx_trim_proposals_kind ON speaker_turn_trim_proposals (tipo);

-- Down migration:
-- DROP TABLE IF EXISTS speaker_turn_trim_proposals;
