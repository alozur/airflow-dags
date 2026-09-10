\pset pager off
\pset format unaligned
\pset tuples_only on
SET search_path TO production;

\echo '@@TURNS@@'
SELECT jsonb_pretty(jsonb_agg(row_to_json(t)::jsonb))
FROM (
  SELECT
    stv.turn_id,
    stv.video_id,
    stv.youtube_video_id,
    stv.turn_type,
    stv.resolved_participant_slug,
    stv.speaker_resolution_confidence,
    stv.speaker_resolution_method,
    stv.output_path,
    stv.youtube_upload_date,
    st.chapter_id,
    st.speaker_label,
    st.resolved_name,
    st.start_seconds,
    st.end_seconds,
    st.interest_score,
    st.is_procedural,
    vc.title            AS chapter_title,
    vc.description      AS chapter_description,
    vc.key_speakers,
    vc.topics,
    vc.resolved_participant_slug AS chapter_participant_slug,
    vc.mentioned_participant_slugs
  FROM speaker_turn_videos stv
  JOIN speaker_turns st  ON st.turn_id = stv.turn_id
  JOIN video_chapters vc ON vc.chapter_id = st.chapter_id
  WHERE stv.is_uploaded_to_youtube
    AND stv.youtube_video_id IS NOT NULL AND stv.youtube_video_id <> ''
) t;

\echo '@@SHORTS@@'
SELECT jsonb_pretty(jsonb_agg(row_to_json(s)::jsonb))
FROM (
  SELECT
    vs.id AS short_id,
    vs.chapter_id,
    vs.turn_id,
    vs.youtube_video_id,
    vs.reap_virality_score,
    vs.pretrim_start_secs,
    vs.pretrim_end_secs,
    vs.pretrim_used_srt,
    vs.created_at,
    vc.title        AS chapter_title,
    vc.topics,
    vc.mentioned_participant_slugs,
    vc.scoring_reasoning,
    st.resolved_name AS turn_resolved_name
  FROM video_shorts vs
  JOIN video_chapters vc ON vc.chapter_id = vs.chapter_id
  LEFT JOIN speaker_turns st ON st.turn_id = vs.turn_id
  WHERE vs.is_uploaded
    AND vs.youtube_video_id IS NOT NULL AND vs.youtube_video_id <> ''
) s;
