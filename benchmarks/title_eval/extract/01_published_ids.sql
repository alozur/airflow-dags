\pset pager off
\pset format unaligned
\pset tuples_only on
\pset fieldsep ','
SET search_path TO production;

SELECT 'chapter', youtube_video_id, chapter_id::text, ''
FROM video_chapters WHERE is_uploaded_to_youtube AND youtube_video_id IS NOT NULL AND youtube_video_id <> ''
UNION ALL
SELECT 'turn', stv.youtube_video_id, st.chapter_id::text, COALESCE(stv.turn_type,'')
FROM speaker_turn_videos stv JOIN speaker_turns st ON st.turn_id = stv.turn_id
WHERE stv.is_uploaded_to_youtube AND stv.youtube_video_id IS NOT NULL AND stv.youtube_video_id <> ''
UNION ALL
SELECT 'short', youtube_video_id, chapter_id::text, ''
FROM video_shorts WHERE is_uploaded AND youtube_video_id IS NOT NULL AND youtube_video_id <> '';
