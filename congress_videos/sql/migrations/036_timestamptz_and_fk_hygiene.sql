-- Migration: TIMESTAMPTZ unification + FK hygiene (issue #209)
-- Created: 2026-08-27
-- Depends on: 021_expose_resolved_participant_slug_in_uploadable_chapters.sql (uploadable_chapters),
--             congressional_videos_schema.sql (uploadable_videos base body),
--             017_create_speaker_normalization_cache.sql, 034_add_speaker_resolution.sql
--
-- The system is UTC-only in practice but 18 columns across 5 tables are still
-- naive TIMESTAMP, and two FKs are incomplete: speaker_normalization_cache.chapter_id
-- has no ON DELETE CASCADE, and speaker_turn_videos.resolved_participant_slug has no
-- FK at all (unlike its video_chapters twin). This migration:
--   1. Drops the two views that select a converted column (Postgres refuses
--      ALTER ... TYPE on a column a view depends on) and recreates them verbatim
--      afterward — uploadable_turns is untouched (references no converted column).
--   2. Converts each of the 18 columns to TIMESTAMPTZ via `col AT TIME ZONE 'UTC'`,
--      guarded on information_schema so a second run is a no-op (idempotent even
--      across fresh installs where the column may already be TIMESTAMPTZ).
--   3. Adds ON DELETE CASCADE to speaker_normalization_cache.chapter_id's FK.
--   4. Nulls orphaned speaker_turn_videos.resolved_participant_slug values (logging
--      the count), then adds the missing FK to congress_participants(slug).
--
-- The migration runner runs `SET search_path TO {schema}, public` before executing,
-- so all table/view names are intentionally UNQUALIFIED.
--
-- Example (development): psql ... -c "SET search_path TO development, public;" -f 036_timestamptz_and_fk_hygiene.sql
-- Example (production):  psql ... -c "SET search_path TO production, public;"  -f 036_timestamptz_and_fk_hygiene.sql

-- UP

-- Step 1: drop the two views that depend on columns about to change type.
-- uploadable_turns (035) is deliberately NOT touched — it references no
-- column converted below.
DROP VIEW IF EXISTS uploadable_chapters;
DROP VIEW IF EXISTS uploadable_videos;

-- Step 2: guarded, idempotent TIMESTAMPTZ conversion of the 18 naive columns.
-- A missing table (schemas that never created it) simply yields no row from
-- information_schema, so the loop no-ops for that pair instead of erroring.
DO $$
DECLARE
    pair RECORD;
BEGIN
    FOR pair IN
        SELECT * FROM (VALUES
            ('congressional_sessions', 'processed_at'),
            ('congressional_sessions', 'updated_at'),
            ('video_topics', 'ai_interest_evaluated_at'),
            ('video_topics', 'youtube_upload_date'),
            ('video_topics', 'youtube_metadata_generated_at'),
            ('video_topics', 'thumbnail_generated_at'),
            ('video_topics', 'created_at'),
            ('video_topics', 'updated_at'),
            ('upload_queue', 'queued_at'),
            ('upload_queue', 'last_attempt_at'),
            ('youtube_source_videos', 'published_at'),
            ('youtube_source_videos', 'download_retry_after'),
            ('youtube_source_videos', 'created_at'),
            ('youtube_source_videos', 'updated_at'),
            ('video_chapters', 'scored_at'),
            ('video_chapters', 'youtube_upload_date'),
            ('video_chapters', 'created_at'),
            ('video_chapters', 'updated_at')
        ) AS t(table_name, column_name)
    LOOP
        IF EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = current_schema()
              AND table_name = pair.table_name
              AND column_name = pair.column_name
              AND data_type = 'timestamp without time zone'
        ) THEN
            EXECUTE format(
                'ALTER TABLE %I ALTER COLUMN %I TYPE TIMESTAMPTZ USING %I AT TIME ZONE ''UTC''',
                pair.table_name, pair.column_name, pair.column_name
            );
            RAISE NOTICE 'Converted %.% to TIMESTAMPTZ', pair.table_name, pair.column_name;
        END IF;
    END LOOP;
END $$;

-- Step 3: speaker_normalization_cache.chapter_id — add ON DELETE CASCADE.
-- Locate the existing FK dynamically (its auto-generated name is stable in
-- practice, but this is robust to a differently-named constraint too).
DO $$
DECLARE
    fk_name TEXT;
    fk_delete_type CHAR;
BEGIN
    SELECT conname, confdeltype INTO fk_name, fk_delete_type
    FROM pg_constraint
    WHERE conrelid = to_regclass('speaker_normalization_cache')
      AND contype = 'f'
      AND confrelid = to_regclass('video_chapters')
    LIMIT 1;

    IF fk_name IS NOT NULL AND fk_delete_type IS DISTINCT FROM 'c' THEN
        EXECUTE format('ALTER TABLE speaker_normalization_cache DROP CONSTRAINT %I', fk_name);
        ALTER TABLE speaker_normalization_cache
            ADD CONSTRAINT speaker_normalization_cache_chapter_id_fkey
            FOREIGN KEY (chapter_id) REFERENCES video_chapters(chapter_id) ON DELETE CASCADE;
        RAISE NOTICE 'speaker_normalization_cache.chapter_id FK now ON DELETE CASCADE (was %)', fk_name;
    END IF;
END $$;

-- Step 4: speaker_turn_videos.resolved_participant_slug — null orphans, then add FK.
DO $$
DECLARE
    orphan_count INT;
BEGIN
    UPDATE speaker_turn_videos
    SET resolved_participant_slug = NULL
    WHERE resolved_participant_slug IS NOT NULL
      AND NOT EXISTS (
          SELECT 1 FROM congress_participants cp
          WHERE cp.slug = speaker_turn_videos.resolved_participant_slug
      );
    GET DIAGNOSTICS orphan_count = ROW_COUNT;
    RAISE NOTICE '% orphaned resolved_participant_slug value(s) nulled on speaker_turn_videos', orphan_count;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = to_regclass('speaker_turn_videos')
          AND contype = 'f'
          AND confrelid = to_regclass('congress_participants')
    ) THEN
        ALTER TABLE speaker_turn_videos
            ADD CONSTRAINT speaker_turn_videos_resolved_participant_slug_fkey
            FOREIGN KEY (resolved_participant_slug) REFERENCES congress_participants(slug);
    END IF;
END $$;

-- Step 5: recreate the two dropped views verbatim.

-- uploadable_chapters — verbatim body from migration 021 (do NOT restore the
-- is_upload_abandoned gate that 011 introduced and 021 silently dropped; that
-- is a separate, out-of-scope follow-up).
CREATE VIEW uploadable_chapters AS
SELECT
    vc.chapter_id,
    vc.video_id,
    ysv.video_title AS source_video_title,
    ysv.session_number,
    ysv.session_date,
    vc.title AS chapter_title,
    vc.description,
    vc.duration_minutes,
    vc.speakers,
    vc.topics,
    vc.timeline,
    vc.start_time,
    vc.end_time,
    vc.relevance_score,
    vc.speaker_relevance_points,
    vc.topic_relevance_points,
    vc.public_interest_points,
    vc.scoring_reasoning,
    vc.key_speakers,
    vc.is_current_topic,
    vc.is_uploaded_to_youtube,
    vc.created_at,
    CURRENT_DATE - DATE(vc.created_at) AS days_since_created,
    vc.resolved_participant_slug
FROM video_chapters vc
JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE
    vc.is_uploaded_to_youtube = FALSE
    AND vc.relevance_score >= 2
ORDER BY
    ysv.session_date DESC NULLS LAST,
    vc.relevance_score DESC,
    vc.created_at DESC;

-- uploadable_videos — verbatim body from congressional_videos_schema.sql, unqualified.
-- Guarded (design D4): the legacy video_topic pipeline tables (upload_queue,
-- video_topics, congressional_sessions) exist only in installs created from the
-- base schema files — the NAS development/production schemas never created them.
-- to_regclass resolves through the same search_path the CREATE VIEW would use,
-- so the view is recreated exactly where its base tables are actually visible.
DO $guard$
BEGIN
    IF to_regclass('upload_queue') IS NOT NULL
       AND to_regclass('video_topics') IS NOT NULL
       AND to_regclass('congressional_sessions') IS NOT NULL THEN
        EXECUTE $view$
CREATE VIEW uploadable_videos AS
SELECT
    vt.entry_id,
    vt.session_number,
    vt.video_url,
    vt.video_file_path,
    vt.topic_title,
    vt.ai_interest_score,
    vt.ai_interest_reasoning,
    vt.youtube_title,
    vt.youtube_description,
    vt.thumbnail_text,
    vt.thumbnail_path,
    uq.queue_priority,
    uq.upload_status,
    uq.queued_at,
    uq.attempted_uploads,
    uq.last_attempt_at,
    CURRENT_DATE - cs.session_date AS days_old,
    CAST(uq.queue_priority - ((CURRENT_DATE - cs.session_date) * 0.2) AS NUMERIC(5,2)) AS effective_priority
FROM upload_queue uq
JOIN video_topics vt ON uq.video_topic_entry_id = vt.entry_id
JOIN congressional_sessions cs ON vt.session_number = cs.session_number
WHERE
    uq.upload_status IN ('pending', 'failed')
    AND vt.is_uploaded_to_youtube = FALSE
    AND vt.upload_eligible = TRUE
    AND vt.is_main_topic = TRUE
ORDER BY effective_priority DESC, uq.queued_at ASC
        $view$;
    ELSE
        RAISE NOTICE 'Skipping uploadable_videos recreation: legacy base tables absent (schema %)', current_schema();
    END IF;
END $guard$;

-- DOWN
-- Manual psql only — the runner has no automatic rollback.
--
-- Reverse all 18 conversions (lossless: UTC in, UTC out):
--   ALTER TABLE congressional_sessions ALTER COLUMN processed_at TYPE TIMESTAMP USING processed_at AT TIME ZONE 'UTC';
--   ALTER TABLE congressional_sessions ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN ai_interest_evaluated_at TYPE TIMESTAMP USING ai_interest_evaluated_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN youtube_upload_date TYPE TIMESTAMP USING youtube_upload_date AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN youtube_metadata_generated_at TYPE TIMESTAMP USING youtube_metadata_generated_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN thumbnail_generated_at TYPE TIMESTAMP USING thumbnail_generated_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_topics ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC';
--   ALTER TABLE upload_queue ALTER COLUMN queued_at TYPE TIMESTAMP USING queued_at AT TIME ZONE 'UTC';
--   ALTER TABLE upload_queue ALTER COLUMN last_attempt_at TYPE TIMESTAMP USING last_attempt_at AT TIME ZONE 'UTC';
--   ALTER TABLE youtube_source_videos ALTER COLUMN published_at TYPE TIMESTAMP USING published_at AT TIME ZONE 'UTC';
--   ALTER TABLE youtube_source_videos ALTER COLUMN download_retry_after TYPE TIMESTAMP USING download_retry_after AT TIME ZONE 'UTC';
--   ALTER TABLE youtube_source_videos ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC';
--   ALTER TABLE youtube_source_videos ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_chapters ALTER COLUMN scored_at TYPE TIMESTAMP USING scored_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_chapters ALTER COLUMN youtube_upload_date TYPE TIMESTAMP USING youtube_upload_date AT TIME ZONE 'UTC';
--   ALTER TABLE video_chapters ALTER COLUMN created_at TYPE TIMESTAMP USING created_at AT TIME ZONE 'UTC';
--   ALTER TABLE video_chapters ALTER COLUMN updated_at TYPE TIMESTAMP USING updated_at AT TIME ZONE 'UTC';
--
-- Drop the new FK (nulled orphan slugs are NOT restorable):
--   ALTER TABLE speaker_turn_videos DROP CONSTRAINT IF EXISTS speaker_turn_videos_resolved_participant_slug_fkey;
--
-- Re-add the chapter_id FK without CASCADE:
--   ALTER TABLE speaker_normalization_cache DROP CONSTRAINT IF EXISTS speaker_normalization_cache_chapter_id_fkey;
--   ALTER TABLE speaker_normalization_cache ADD CONSTRAINT speaker_normalization_cache_chapter_id_fkey
--       FOREIGN KEY (chapter_id) REFERENCES video_chapters(chapter_id);
--
-- Re-create both views (021 / base-schema bodies above), then:
--   DELETE FROM schema_migrations WHERE migration LIKE '%036_timestamptz_and_fk_hygiene.sql';
