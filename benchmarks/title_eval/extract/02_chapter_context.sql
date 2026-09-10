\pset pager off
\pset format unaligned
\pset tuples_only on
SET search_path TO production;

SELECT jsonb_pretty(jsonb_agg(row_to_json(t)::jsonb))
FROM (
  SELECT
    vc.chapter_id,
    vc.video_id                    AS source_video_id,
    vc.youtube_video_id,
    vc.title                       AS chapter_title,
    vc.description                 AS chapter_description,
    vc.key_speakers,
    vc.speakers,
    vc.topics,
    vc.duration_minutes,
    vc.relevance_score,
    vc.resolved_participant_slug,
    vc.is_uploaded_to_youtube,
    ysv.session_date,
    ysv.session_number,
    -- Chosen thumbnail option: the `best` argument generate_title receives.
    (
      SELECT row_to_json(b)
      FROM (
        SELECT vt.style, vt.label, vt.prompt, vt.archetype, vt.main_score, vt.openai_title
        FROM video_thumbnails vt
        WHERE vt.chapter_id = vc.chapter_id AND vt.is_chosen = TRUE
        ORDER BY vt.created_at DESC
        LIMIT 1
      ) b
    ) AS chosen_option
  FROM video_chapters vc
  LEFT JOIN youtube_source_videos ysv ON ysv.video_id = vc.video_id
  WHERE vc.is_uploaded_to_youtube
    AND vc.youtube_video_id IS NOT NULL
    AND vc.youtube_video_id <> ''
) t;
