-- Migration: Extend speaker_turns.source CHECK to allow 'llm_resolved' (issue #131)
-- Created: 2026-08-28
-- Depends on: 022_create_speaker_turns.sql (declares the source CHECK inline,
--             which Postgres auto-names speaker_turns_source_check)
--
-- Adds an LLM-fallback resolution tier ('llm_resolved', confidence 0.85) to
-- the speaker_turns.source ladder, ranked below text_named (0.95) and above
-- text_confirmed (0.80). The turn-name-resolution pipeline (module
-- speaker_turns._llm_resolve_name) writes this value only after the raw LLM
-- candidate passes roster fuzzy validation (lookup_participant_fuzzy).
--
-- Migration 022 declared the CHECK inline on CREATE TABLE, so Postgres
-- auto-assigned it the name speaker_turns_source_check. Because the
-- constraint is already named and already exists, the correct idempotent
-- mechanism is DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT (constraint
-- replacement) — NOT the pg_constraint-guarded DO-block used by migration
-- 034 for a *new* constraint. Design-amendments W2 (orchestrator-ratified)
-- makes this shape binding for this migration.
--
-- Additive-only value: every existing row already satisfies the wider
-- CHECK, so the re-add cannot fail on live data.
--
-- Rollout order is load-bearing: apply this migration FIRST (dev then
-- prod), then deploy code that writes 'llm_resolved'. Code deployed against
-- the narrow (pre-039) CHECK would raise at upsert and fail the chapter.
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so all table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 039_extend_speaker_turns_source.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 039_extend_speaker_turns_source.sql

-- UP

ALTER TABLE speaker_turns DROP CONSTRAINT IF EXISTS speaker_turns_source_check;
ALTER TABLE speaker_turns
    ADD CONSTRAINT speaker_turns_source_check
    CHECK (source IN ('acoustic', 'text_confirmed', 'text_named', 'llm_resolved'));

-- DOWN
-- Manual psql only — the runner has no automatic rollback. The DOWN block
-- MUST repair any llm_resolved rows before narrowing the CHECK, or the
-- ADD CONSTRAINT below fails against live data.
--
-- UPDATE speaker_turns SET source = 'acoustic', confidence = 0.50, resolved_name = NULL
--     WHERE source = 'llm_resolved';
-- ALTER TABLE speaker_turns DROP CONSTRAINT IF EXISTS speaker_turns_source_check;
-- ALTER TABLE speaker_turns ADD CONSTRAINT speaker_turns_source_check
--     CHECK (source IN ('acoustic', 'text_confirmed', 'text_named'));
