# Archive Report: speaker-turns-detection (issue #86)

**Archived:** 2026-08-18
**Status:** DELIVERED to `dev` and closed.

## Delivery
- **PR #90** (PR1) — pure module `congress_videos/modules/speaker_turns.py` +
  migration `022_create_speaker_turns.sql`. Merged to `dev`.
- **PR #93** (PR2) — Docker diarization wrapper `speaker_turns_docker.py` +
  on-demand DAG `speaker_turns_dag.py`. Merged to `dev`.
- Issue **#86 CLOSED** (dev merge does not auto-close).

## What shipped
Detect speaker-turn boundaries **within** existing thematic `video_chapters`
and persist named sub-turns to the new `speaker_turns` table — **without cutting
video**. Diarization (pyannote, Docker-isolated, per-chapter) provides the
boundary; SRT president-announcement text provides the name **and** the
confirm/deny gate for each acoustic change.

## Fase 0 evidence (ear audit)
32 diarization candidates → 6 noise, 7 redundant duplicates, **19 unique real
changes** (precision 81% crude / 59% useful). **Key finding:**
`confirmed_block_duration_seconds` is NOT a usable threshold (noise & real
scores overlap) — so the text layer, not a score threshold, gates each change.
Report: `benchmarks/pyannote_diarization/audit/run.SC6R8Dgl/AUDIT_REPORT.md`.

## Verification
`sdd-verify` PASS (0 critical); 2083 tests green; e2e import-errors empty;
module coverage 94.89%. See `verify-report.md`.

## Follow-ups (not in this change)
- **#87** — trim no-voice / applause within each turn.
- **#88** — materialize 1 speaker = 1 video (ffmpeg).
- **#17** — generic best-moments engine can consume the `speaker_turns` table.

## Deviations from plan
- Migration numbered `022` (design said `021`; dev already carried 020+021 —
  caught by the Task-0 re-verify).
- `detect_turns` reads `_wav_path`/`_chapter_offset_seconds` from the chapter
  dict set by the DAG layer (accepted design deviation; verified wired).
