-- Migration: Create llm_cache table for idempotent LLM responses
-- Created: 2026-06-12
-- Depends on: 008_video_chapters_unique_segment.sql
--
-- Backs utils/llm_cache.py: caches LLM JSON completions keyed by a
-- sha256(model + system_prompt + user_prompt + params) hash so repeated
-- task runs / retries reuse a stored response instead of re-calling the model.
-- The cache key embeds the prompt content, so any prompt change auto-invalidates.
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 009_create_llm_cache.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 009_create_llm_cache.sql

CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key   CHAR(64)    PRIMARY KEY,   -- sha256 hex digest
    model       VARCHAR(64) NOT NULL,
    response    JSONB       NOT NULL,
    created_at  TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_llm_cache_created_at ON llm_cache (created_at);
