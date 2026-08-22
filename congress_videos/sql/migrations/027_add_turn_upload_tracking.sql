-- Migration 027: Add upload tracking columns to speaker_turn_videos
-- and create uploadable_turns view for the turn upload queue.
--
-- UP
ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS is_uploaded_to_youtube BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS youtube_video_id VARCHAR(50),
    ADD COLUMN IF NOT EXISTS youtube_upload_date TIMESTAMPTZ;

DROP VIEW IF EXISTS uploadable_turns;
CREATE VIEW uploadable_turns AS
SELECT
    stv.turn_id,
    stv.output_path,
    st.chapter_id,
    st.resolved_name,
    st.start_seconds,
    st.end_seconds,
    vc.video_id,
    vc.title AS chapter_title,
    vc.description,
    vc.relevance_score,
    vc.key_speakers,
    ysv.session_number,
    ysv.session_date,
    stv.materialized_at
FROM speaker_turn_videos stv
JOIN speaker_turns st ON stv.turn_id = st.turn_id
JOIN video_chapters vc ON st.chapter_id = vc.chapter_id
JOIN youtube_source_videos ysv ON vc.video_id = ysv.video_id
WHERE stv.is_uploaded_to_youtube = FALSE
  AND vc.is_uploaded_to_youtube = FALSE
  AND vc.relevance_score >= 2
ORDER BY vc.relevance_score DESC, ysv.session_date DESC;

-- DOWN
-- DROP VIEW IF EXISTS uploadable_turns;
-- ALTER TABLE speaker_turn_videos
--     DROP COLUMN IF EXISTS is_uploaded_to_youtube,
--     DROP COLUMN IF EXISTS youtube_video_id,
--     DROP COLUMN IF EXISTS youtube_upload_date;
