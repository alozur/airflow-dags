# Proposal: Speaker-turn detection inside video_chapters

**Change:** `speaker-turns-detection`
**Source:** GitHub issue #86 (part of epic #16, split from #17)
**Status:** Proposed — Fase 0 (ear audit) complete; Fases 1–3 scoped here

## Intent

Publishable `video_chapters` bundle multiple speaker turns into one 15–45 min
segment. Downstream products (best-moment selection #17, per-speaker cuts #88)
need the turn boundaries **within** a chapter and **who** speaks each turn,
without cutting video. Fase 0 proved acoustic diarization alone is noisy;
fusing pyannote boundaries with SRT president-announcement text confirms and
names each turn. Deliver a standalone, on-demand pipeline that persists resolved
sub-turns for reuse.

## Scope

### In scope

- New standalone DAG `speaker_turns_dag` (`schedule=None`, triggered via conf
  like `generic_thumbnail_generator_dag`), processing a LIMIT of
  publication-intended chapters and re-queuing.
- Pure module `congress_videos/modules/speaker_turns.py`: injectable `diarize_fn`
  + president-announcement extractor + name resolution; the DAG is thin
  orchestration.
- Diarization backend = **Docker subprocess** (`docker run` isolated pyannote
  container, `--network none`, 4 GiB cap); torch/pyannote never enter the
  Airflow image.
- Signal fusion: pyannote = turn BOUNDARY; SRT president-announcement phrase =
  NAME + confirm/deny gate per acoustic change.
- Postprocessing: (1) merge adjacent same-speaker (gap < 1.0 s); (2) collapse
  A→B→A ping-pong in a short window (kills ~27 % duplicate redundancy);
  (3) TEXT GATE each acoustic change vs SRT blocks in `[t-30s, t+30s]`;
  (4) merge adjacent turns resolving to the same name.
- New table `speaker_turns(turn_id, chapter_id FK, start_seconds, end_seconds,
  speaker_label, resolved_name, confidence, source)`,
  `source ∈ {acoustic, text_confirmed, text_named}`. Migration = next sequential
  number (spec MUST re-verify highest+1; ≥ 020). Idempotent
  `UNIQUE(chapter_id, start_seconds)` + `ON CONFLICT DO UPDATE`.
- Reuse (no change): `extract_audio_wav` + `_find_source_video` (vad_helpers);
  `find_srt_for_chapter` + `_parse_srt_blocks` (srt_helpers); `lookup_participant_fuzzy`
  (participants_db).
- Graceful degradation: missing source video → log + skip chapter, never fail
  the DAG; missing SRT → acoustic-only, lower confidence, `source=acoustic`.

### Out of scope

- Video cutting / materialization (#88); silence/applause trimming (#87);
  recall measurement (Fase 0 is precision-only); the generic best-moment engine
  (#17).
- Chaining into `youtube_upload_dag` (4.6× realtime diarization must not block
  upload).
- Using `confirmed_block_duration_seconds` as a threshold — Fase 0 proved
  noise/real scores overlap; dead end, not implemented.

## Approach

Approach A (standalone on-demand DAG) + Docker-subprocess diarization +
pure-module-first. Per chapter: locate audio (`extract_audio_wav` over
`vad_helpers._find_source_video`), run `diarize_fn` (Docker subprocess, stubbed
in tests) → acoustic boundaries; parse SRT window blocks; for each acoustic
change gate/name against president-announcement phrases (new extractor; fallback
to `lookup_participant_fuzzy` when phrase absent); run
merge / ping-pong / text-gate / name-merge postprocessing; upsert into
`speaker_turns`.

## Affected areas

| Area | Impact |
|------|--------|
| `congress_videos/speaker_turns_dag.py` | New — thin on-demand DAG (LIMIT + requeue) |
| `congress_videos/modules/speaker_turns.py` | New — pure module (injectable diarize + extractor + postprocessing) |
| `congress_videos/sql/migrations/0NN_create_speaker_turns.sql` | New — table + `UNIQUE(chapter_id,start_seconds)`; verify number (≥ 020) |
| `congress_videos/modules/vad_helpers.py` | Reuse (no change) |
| `congress_videos/srt_helpers.py` | Reuse (no change) |
| `congress_videos/modules/participants_db.py` | Reuse (no change) |
| `tests/congress_videos/...` | New — module unit tests (stub `diarize_fn`) + DAG-load test |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| President-announcement extractor misses/mis-names turns | High | Fallback to fuzzy match; store `source`+`confidence`; precision-first |
| Docker pyannote image unavailable on worker | Med | Injectable `diarize_fn`; skip+log chapter, never fail DAG |
| Migration number collides with dev `020` | Med | Spec re-verifies highest migration, picks next |
| Diarization cost (4.6× realtime) | Med | On-demand only, LIMIT + requeue, publication-intended chapters only |

## Rollback

Purely additive: drop `speaker_turns_dag.py`, `modules/speaker_turns.py`, and
DROP the `speaker_turns` table. No existing DAG or table is touched.

## Resolved assumptions (defaults accepted by owner)

1. Persist EVERY confirmed acoustic change (`resolved_name` nullable), not only
   named turns.
2. Confidence = numeric 0–1.
3. One LIMIT batch per DAG run; external re-trigger (no self-trigger).

## Success criteria

- [ ] `speaker_turns_dag` loads with no import errors and runs on a triggered chapter.
- [ ] Named sub-turns persisted idempotently; re-run updates, never duplicates.
- [ ] Missing source video → chapter skipped, DAG succeeds.
- [ ] Missing SRT → acoustic-only rows with `source=acoustic`, lower confidence.
- [ ] Module unit tests pass with stubbed `diarize_fn` (no Docker in CI).
