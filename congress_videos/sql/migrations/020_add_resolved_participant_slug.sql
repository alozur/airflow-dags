-- Migration 020: Add resolved_participant_slug column to video_chapters
-- Created: 2026-08-11
-- Depends on: 018_add_participant_slug.sql (congress_participants.slug must exist)
--             017_create_speaker_normalization_cache.sql
--
-- Adds a nullable FK column linking a chapter to a verified congress_participants row.
-- Fill is LLM-free and async: normalize_chapter_speakers writes the slug after
-- confirming a 'matched' cache entry. NULL is the honest unresolved state.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS.
-- Runner sets search_path before executing, so table names are UNQUALIFIED.
--
-- One-time backfill: fills rows whose chapter_id appears in speaker_normalization_cache
-- with status='matched' and whose resolved_participant_slug is currently NULL.
-- The JOIN through congress_participants guarantees FK integrity: only slugs that
-- exist in congress_participants are written; rows with no matching slug are skipped.
--
-- Rollback: DROP COLUMN IF EXISTS resolved_participant_slug (additive/nullable, safe).

-- 1) Add nullable column (idempotent).
ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS resolved_participant_slug TEXT
        REFERENCES congress_participants(slug);

-- 2) Create an index for FK lookups / downstream JOINs (idempotent).
CREATE INDEX IF NOT EXISTS idx_video_chapters_resolved_participant_slug
    ON video_chapters (resolved_participant_slug);

-- 3) One-time idempotent backfill: set slug for matched cache rows where not yet set.
--    JOIN congress_participants guarantees only valid slugs are written (FK integrity).
--    WHERE IS NULL makes this a no-op for already-filled rows.
UPDATE video_chapters vc
    SET resolved_participant_slug = cp.slug
    FROM speaker_normalization_cache snc
    JOIN congress_participants cp
        ON cp.normalized_name = snc.participant_normalized_name
    WHERE snc.chapter_id = vc.chapter_id
      AND snc.status = 'matched'
      AND vc.resolved_participant_slug IS NULL;
