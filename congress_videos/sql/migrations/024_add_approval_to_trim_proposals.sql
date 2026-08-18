-- Migration 024: Add approval columns to speaker_turn_trim_proposals
--
-- Adds operator-approval tracking to existing trim proposals.
-- is_approved: the operator has reviewed and approved this proposal for execution.
-- approved_at: timestamp when the approval was set (nullable; NULL = not yet approved).
--
-- All existing rows default to is_approved = FALSE (no proposal is implicitly approved).
-- Migration is idempotent via ADD COLUMN IF NOT EXISTS.

ALTER TABLE speaker_turn_trim_proposals
    ADD COLUMN IF NOT EXISTS is_approved BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS approved_at TIMESTAMPTZ;

-- Down migration:
-- ALTER TABLE speaker_turn_trim_proposals
--     DROP COLUMN IF EXISTS is_approved,
--     DROP COLUMN IF EXISTS approved_at;
