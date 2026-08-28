-- Migration: Extend speaker_turns.source CHECK to allow 'llm_resolved' (issue #131)
-- Created: 2026-08-28
-- Depends on: 022_create_speaker_turns.sql (declares the source CHECK inline,
--             which Postgres auto-names it speaker_turns_source_check)
--
-- Adds an LLM-fallback resolution tier ('llm_resolved', confidence 0.85),
-- ranked below text_named (0.95) and above text_confirmed (0.80). Since
-- 022's CHECK is already named, the idempotent mechanism here is
-- DROP CONSTRAINT IF EXISTS + ADD CONSTRAINT (constraint replacement) —
-- NOT the pg_constraint-guarded DO-block 034 uses for a *new* constraint.
-- Design-amendments W2 (orchestrator-ratified) makes this shape binding.
-- Additive-only: every existing row already satisfies the wider CHECK.
--
-- Rollout order is load-bearing: apply FIRST (dev then prod), then deploy
-- code that writes 'llm_resolved' — code against the narrow pre-039 CHECK
-- raises at upsert and fails the chapter.
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so all table names are intentionally UNQUALIFIED.

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
