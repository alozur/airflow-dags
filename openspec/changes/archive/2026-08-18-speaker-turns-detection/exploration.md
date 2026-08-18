# Exploration: Speaker-turn detection inside video_chapters

**Change:** `speaker-turns-detection`
**Source:** GitHub issue #86 (part of epic #16, split from #17)

## Goal

Detect speaker-turn boundaries **within** the existing thematic `video_chapters`
and persist named sub-turns, **without cutting the video**. Materialization
(1 speaker = 1 video) is issue #88; silence/applause trimming is #87.

## Established context (verified against code)

1. **Thematic chapters already exist.** `map_reduce_identify_chapters`
   (`congress_videos/modules/youtube/map_reduce_chapters.py`) produces 15–45 min
   chapters consumed in `youtube_upload_dag.py`. They carry `start_time`/
   `end_time`/`title` + a `speakers[]` list (issue #56). They are the PRE-CUT
   that bounds diarization to 30–60 min windows.
2. **Decided design.** pyannote diarization = the turn BOUNDARY signal; SRT
   president-announcement text ("Tiene la palabra el señor/la señora X") = the
   NAME **and** the confirm/deny gate per acoustic change. Diarization runs
   bounded per chapter, never on the full session.
3. **Fase 0 (ear audit) DONE.** Report at
   `benchmarks/pyannote_diarization/audit/run.SC6R8Dgl/AUDIT_REPORT.md`.
   32 detections → 6 noise, 7 redundant duplicates, 19 unique real changes;
   precision 81% crude / 59% useful. **Critical finding:**
   `confirmed_block_duration_seconds` is NOT a usable threshold (noise & real
   scores overlap 3–8.4 s vs 1.8–108.7 s). Noise = same-speaker pitch shifts.

## Current-state findings (file:line)

- **Chapter reads:** `video_chapters` table (`congress_videos/sql/youtube_chapters_schema.sql:48`);
  the `uploadable_chapters` view (`010_add_timeline_column.sql:28`) exposes
  `start_time`, `end_time`, `video_id`, `session_date`.
- **SRT access (reuse, no change):** `find_srt_for_chapter(video_id, chapter_id, session_date)`
  + `_parse_srt_blocks(path)` → `[{start_secs, end_secs, text}]`
  (`congress_videos/srt_helpers.py:21`). Filter to the chapter window exactly as
  `_prepare_thumbnail_config` does (`youtube_upload_dag.py:186`).
- **Audio slice (reuse, no change):** `vad_helpers._find_source_video(target_date, video_id)`
  + `extract_audio_wav(video_path, wav_path, start_secs=…, duration_secs=…)`
  (`vad_helpers.py:174`) already supports bounded `-ss`/`-t` extraction.
  NOTE: a second single-arg `_find_source_video` exists in
  `reap_clip_preparer_dag.py` — the `vad_helpers` one is the correct reuse.
- **Name resolution (reuse):** `lookup_participant_fuzzy(name)` + `normalize_member_name`
  (`participants_db.py:181`); idiomatic flow in `speaker_normalization.py:132`
  (fuzzy → AI verify → upsert).
- **No president-announcement phrase extractor exists** — this is new NLP work,
  the highest-risk new component.
- **Diarization is not importable** — the benchmark runs pyannote as an isolated
  Docker container; a production module must call it fresh (Docker subprocess).
- **DAG patterns:** `generic_thumbnail_generator_dag` (`schedule=None`, triggered
  by conf) is the precedent thin worker; `xcom_task` (`utils/airflow_helpers.py:6`).
- **Idempotency:** `ON CONFLICT … DO UPDATE SET … updated_at = NOW()` used across
  `speaker_normalization_cache`, `video_thumbnails`, `congress_participants`.
- **Migrations:** highest on `feat/issue-59…` branch is `019_create_video_thumbnails.sql`;
  `dev` may carry `020`. The spec phase MUST re-verify highest+1.

## Approaches

| Approach | Pros | Cons |
|----------|------|------|
| **A — standalone on-demand DAG** | no coupling to upload; chapters processed independently; matches thumbnail-DAG pattern; restartable per chapter; injectable diarize_fn | needs a trigger |
| B — tasks chained after `youtube_upload_dag` | turns fresh at upload | blocks upload on 4.6× realtime diarization; upload DAG already has a stale-tolerance timeout; couples slow audio to UI-critical flow |

**Recommendation: Approach A** + Docker-subprocess diarization + pure-module-first.

## Next

sdd-propose (done), then sdd-spec.
