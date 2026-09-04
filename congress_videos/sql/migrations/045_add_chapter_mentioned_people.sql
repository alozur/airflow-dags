-- Migration 045: Record who is MENTIONED in a chapter (issue #432)
-- Created: 2026-09-04
-- Depends on: 020_add_resolved_participant_slug.sql (the speaker column this one is NOT)
--
-- resolved_participant_slug answers "who is SPEAKING in this chapter" (single-valued).
-- mentioned_participant_slugs answers "who is TALKED ABOUT" (many-valued). They are
-- different concepts and must never be conflated.
--
-- Bare TEXT[] with no FK: Postgres cannot express a per-element FK on an array, so
-- integrity is write-time only, enforced by the roster gate in
-- congress_videos/modules/mentioned_people_resolution.py. This matches the existing
-- speakers[] / key_speakers[] columns on the same table.
--
-- NULL = never analysed. '{}' = analysed, nobody mentioned. The distinction is load-bearing.
-- No index: the column is read back per-row by chapter_id, never filtered or sorted on.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS mentioned_participant_slugs TEXT[];

COMMENT ON COLUMN video_chapters.mentioned_participant_slugs IS
    'congress_participants.slug values for people MENTIONED in the chapter transcript '
    '(issue #432). Distinct from resolved_participant_slug, which is the chapter SPEAKER. '
    'Roster-gated at write time; unresolved or ambiguous mentions are dropped, never invented. '
    'NULL = never analysed; empty array = analysed, nobody mentioned.';

COMMENT ON COLUMN video_chapters.topics IS
    'Normalized topic labels (lowercase, trimmed, deduplicated, first-seen order, max 8). '
    'Source of truth moved in issue #432 from the chapter-identification by-product '
    '(utils/ai_chapter_analyzer.py) to the dedicated extract_topics call made at upload time '
    '(congress_videos/modules/topic_extraction.py). A successful extraction that yields zero '
    'topics does NOT overwrite a pre-existing value.';

-- DOWN
-- Manual psql only -- the runner executes the WHOLE file in ONE transaction, so a live
-- DOWN block would revert its own UP and still be recorded as applied.
--
-- ALTER TABLE video_chapters
--     DROP COLUMN IF EXISTS mentioned_participant_slugs;
