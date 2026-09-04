# Proposal: SRT artifacts for shorts + mentioned-people and topic analysis

## Problem Statement

Two gaps close together (issues #431, #432):

- **#431** asks for chapter *and* short-video SRTs in their canonical folders. The **chapter half is already delivered** by issue #340 (`write_chapter_srt_sidecar` -> `subtitles.srt` in `get_video_chapter_dir`); this change verifies and documents it. The **shorts half is missing**: `get_chapter_shorts_dir` holds only `{clip_id}.mp4`, so clips ship without subtitles.
- **#432**: `video_chapters.topics` is a by-product of the chapter-identification call, and no column records *who is mentioned* (`resolved_participant_slug` is the speaker, a different concept). Speaker / mentioned-people / topics are conflated.

## Intent

Give every chapter and generated short an SRT beside its media, and derive mentioned people (roster-gated slugs) and normalized topics from two independent LLM calls, without losing the speaker-vs-mentioned distinction.

## Scope

### In Scope
- `write_short_srt_sidecar` in `congress_videos/srt_helpers.py`, hooked in `ReapJobSensor.poke`; writes `{clip_id}.srt` beside `{clip_id}.mp4`, best-effort, never fails the sensor.
- Migration `045` adding `video_chapters.mentioned_participant_slugs TEXT[]` (+ `production_schema.sql` mirror, column comment, drift test) and a `COMMENT ON COLUMN` re-documenting `topics`.
- Two pure modules mirroring `chapter_speaker_resolution.py`: `resolve_mentioned_people` (roster-gated) and `extract_topics` (deterministic normalisation), hooked at upload time in `youtube_upload_dag.py`, independently try/excepted and persisted.

### Out of Scope
- Standalone SRT-download DAG; per-clip Reap timing; new analysis table; monitor-time hook; backfill of existing shorts/chapters beyond what future runs produce.

## Capabilities

### New Capabilities
- `short-video-srt-artifacts`: SRT sidecars for chapters and generated shorts in canonical directories, with reuse/skip and explicit failure outcomes.
- `chapter-mentioned-people`: roster-gated resolution of mentioned people into deduplicated `congress_participants` slugs.
- `chapter-topic-extraction`: independent LLM topic extraction with normalized, stable, deduplicated storage.

### Modified Capabilities
None (no `openspec/specs/` source-of-truth files exist yet).

## Approach

| Decision | Choice |
|---|---|
| Short SRT window | `[chapter_start + pretrim_start_secs, chapter_start + pretrim_end_secs]`, fallback full chapter span; documented approximation (Reap exposes no clip timing) |
| Reuse rule | Existing non-empty `{clip_id}.srt` reused, INFO-logged with path; missing/unreadable source or zero blocks -> WARNING + `None` |
| Slug integrity | Unresolved/ambiguous mentions dropped (never invented, never placeholders), raw names INFO-logged |
| Topics format | lowercase, trimmed, deduplicated first-seen order, documented cap |
| Idempotency | `cached_json_completion` (`llm_cache`); separate prompts -> separate cache keys |

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/srt_helpers.py` | Modified | `write_short_srt_sidecar` |
| `congress_videos/reap_processor_dag.py` | Modified | sidecar hook in `ReapJobSensor.poke` |
| `congress_videos/sql/migrations/045_*.sql`, `production_schema.sql` | New/Modified | `mentioned_participant_slugs`, column comments |
| `congress_videos/modules/mentioned_people_resolution.py`, `topic_extraction.py` | New | pure analysis modules |
| `congress_videos/config/ai_prompts.py` | Modified | two new prompts |
| `congress_videos/youtube_upload_dag.py` | Modified | both hooks on the shared path of `_prepare_thumbnail_config` (the turn-only queue never reaches `_resolve_chapter_speaker`) |
| `docs/ARCHITECTURE.md`, `docs/PIPELINE.md` | Modified | artifact paths + metadata semantics |

## Delivery Slices (auto-chain, 400-line budget)

| PR | Scope | Closes |
|---|---|---|
| PR1 | shorts SRT sidecar + hook + tests + docs | `Closes #431` |
| PR2 | migration 045 + mirror + drift test + `resolve_mentioned_people` + prompt + tests | — |
| PR3 | `extract_topics` + prompt + upload-DAG hooks + docs | `Closes #432` |

Then a release PR `dev` -> `main`.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Short SRT is a chapter/pre-trim approximation | High | Documented in module docstring and pipeline docs |
| Upload-time writes change `topics` source of truth | Med | Column comment + re-verify consumers (`reap_shorts_uploader_dag`, `uploadable_chapters`) at spec time |
| `mentioned_participant_slugs` integrity is write-time only | Med | Roster gate in the pure module + tests for unknown/ambiguous names |
| PR2 tightest against 400 lines | Med | Migration + one module only; docs deferred to PR3 |
| One analysis failing discards the other | Low | Independent try/except and independent persistence |

## Rollback Plan

- **PR1**: revert the commit. Sidecar files are inert artifacts; leaving them on disk is harmless.
- **PR2**: migration `045` is additive — rollback is `ALTER TABLE video_chapters DROP COLUMN mentioned_participant_slugs` plus reverting the mirror/drift test; no reads exist until PR3.
- **PR3**: revert the commit; chapters keep their previous `topics` values, columns stay unread.

## Dependencies

- Issue #340 (chapter SRT sidecar) already on `main`.
- `congress_participants` roster and `llm_cache` table already in production.

## Success Criteria

- [ ] #431: chapter SRT present in the canonical chapter dir (verified); generated short SRT present in the canonical shorts dir; re-runs idempotent without needless rewrite; missing subtitles / unavailable source / errors produce explicit logged outcomes; tests cover both artifact types, distinct destinations, already-present files, failures; paths documented.
- [ ] #432: two distinct LLM calls; mentioned people persisted as a deduplicated array of valid roster slugs with no invented slug; topics persisted normalized with stable ordering and dedup; both idempotent/cacheable per content revision; tests cover zero/one/multiple people, ambiguous matches, no topics, multiple topics, malformed output, one analysis failing while the other succeeds; prompts and storage preserve the speaker / mentioned-people / topics distinction.
- [ ] `uv run pytest` green on each slice; each PR under 400 changed lines.

## Open Questions

None. Product decisions 1-8 from the pre-proposal handoff are confirmed.
