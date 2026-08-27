-- Migration: Upload-path partial indexes (issue #204)
-- Created: 2026-08-27
-- Depends on: 021_expose_resolved_participant_slug_in_uploadable_chapters.sql (uploadable_chapters),
--             035_add_min_turn_duration.sql (uploadable_turns), 004_create_video_shorts.sql
--
-- The 5 hottest upload-path predicates (uploadable views, select_unprepared_turns,
-- get_pending_shorts) run unindexed sequential scans. Adds one partial index per
-- predicate, matching the current view/query text verbatim so each index is
-- actually usable by the planner.
--
-- Plain CREATE INDEX IF NOT EXISTS — no CONCURRENTLY. The migration runner has
-- no autocommit mode: every file executes inside the implicit transaction opened
-- by its own connection (see utils/migrations_dag.py), and CONCURRENTLY requires
-- running outside any transaction block. Matches migration 020's precedent;
-- 004-036 never used CONCURRENTLY either. Run during the 14:00-20:00 UTC NAS
-- quiet window for large tables (brief exclusive locks during index build).
--
-- The migration runner runs `SET search_path TO {schema}, public` before
-- executing, so all table names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 037_upload_path_indexes.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 037_upload_path_indexes.sql

-- UP

-- uploadable_chapters (021): ORDER BY relevance_score DESC, created_at DESC
-- WHERE is_uploaded_to_youtube = FALSE.
CREATE INDEX IF NOT EXISTS idx_video_chapters_pending_priority
    ON video_chapters (relevance_score DESC, created_at DESC)
    WHERE is_uploaded_to_youtube = FALSE;

-- uploadable_turns (035): dedup + upload-eligibility gate on speaker_turn_videos.
CREATE INDEX IF NOT EXISTS idx_speaker_turn_videos_uploadable
    ON speaker_turn_videos (output_path, turn_id)
    WHERE is_uploaded_to_youtube = FALSE AND prepared_at IS NOT NULL AND NOT is_upload_abandoned;

-- select_unprepared_turns (database.py): nightly speaker_turn_prepare candidate scan.
CREATE INDEX IF NOT EXISTS idx_speaker_turn_videos_unprepared
    ON speaker_turn_videos (output_path, turn_id)
    WHERE prepared_at IS NULL AND is_uploaded_to_youtube = FALSE;

-- get_pending_shorts (database.py): upload-history query, ORDER BY updated_at DESC.
CREATE INDEX IF NOT EXISTS idx_video_shorts_uploaded_recent
    ON video_shorts (updated_at DESC)
    WHERE is_uploaded = TRUE;

-- get_pending_shorts (database.py): pending-candidate scan, ORDER BY virality score.
CREATE INDEX IF NOT EXISTS idx_video_shorts_pending_downloaded
    ON video_shorts (reap_virality_score DESC NULLS LAST)
    WHERE is_uploaded = FALSE AND is_upload_abandoned = FALSE AND local_file_path IS NOT NULL AND reap_status = 'downloaded';

-- DOWN
-- Cheap and safe — manual psql only, no automatic rollback in the runner.
--
-- DROP INDEX IF EXISTS idx_video_chapters_pending_priority;
-- DROP INDEX IF EXISTS idx_speaker_turn_videos_uploadable;
-- DROP INDEX IF EXISTS idx_speaker_turn_videos_unprepared;
-- DROP INDEX IF EXISTS idx_video_shorts_uploaded_recent;
-- DROP INDEX IF EXISTS idx_video_shorts_pending_downloaded;
-- DELETE FROM schema_migrations WHERE migration LIKE '%037_upload_path_indexes.sql';
