-- Migration 046: persist the accepted speaker-resolution evidence (issue #430)
-- Created: 2026-09-04
-- Depends on: 034_add_speaker_resolution.sql
--
-- Adds ONE nullable column to speaker_turn_videos:
--   speaker_resolution_evidence TEXT -- the monologue two-step audit JSON
--   (method "monologue_window_v1") or, for the qa path, the verbatim
--   announcement quote the model reported. NULL = resolved before this
--   migration, or never resolved. No backfill.
--
-- NO view is recreated: uploadable_turns does not select this column, so the
-- 044 snapshot/lockstep guard in tests/congress_videos/sql/test_production_schema.py
-- stays valid untouched. The column is additive and nullable, so every existing
-- INSERT and UPDATE keeps working unchanged.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS speaker_resolution_evidence TEXT;

-- DOWN
-- Manual psql only -- the runner has no automatic rollback and executes the WHOLE
-- file text in ONE transaction, so this block MUST stay commented out (044 convention):
-- a live DOWN would revert its own UP and still register as applied.
--
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS speaker_resolution_evidence;
