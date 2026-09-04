# Exploration: srt-artifacts-people-topics (issues #431 + #432)

Full exploration text is mirrored in Engram (`sdd/srt-artifacts-people-topics/explore`, observation #2308).

## Current State

- **#431 chapter-SRT half is already shipped** by issue #340 (main `67ef035`): `write_chapter_srt_sidecar` in `congress_videos/srt_helpers.py` is called per chapter from `_run_save_chapters_to_db` (`youtube_channel_monitor_dag.py`) and writes an atomic `subtitles.srt` into `get_video_chapter_dir(...)` (`congress_videos/config/paths.py`). No DB column tracks it; readers re-derive the path via `find_srt_for_chapter(..., canonical_dir=...)`.
- **#431 short-video half is missing.** `video_shorts` has no subtitle tracking; `get_chapter_shorts_dir` / `get_chapter_short_file_path` exist but only the clip MP4 is written there, by `ReapJobSensor.poke` in `reap_processor_dag.py`. Reap exposes no per-clip start/end offsets; the only owned timing is the staged clip pre-trim window (`video_shorts.pretrim_start_secs` / `pretrim_end_secs`, chapter-relative, set by `reap_clip_preparer_dag.py`).
- **#432 groundwork:** `video_chapters` has `speakers TEXT[]`, `key_speakers TEXT[]`, `topics TEXT[]` (today a by-product of the chapter-identification LLM call in `utils/ai_chapter_analyzer.py`, consumed only by `reap_shorts_uploader_dag.py` post-upload and re-exposed by every `uploadable_chapters` view revision) and single-valued `resolved_participant_slug` (migration 020; the chapter's primary speaker, not "who is discussed"). No column exists for mentioned people.
- **LLM pattern to reuse:** `utils/llm_config.py` tiers -> `utils/llm_cache.py::cached_json_completion` (Postgres `llm_cache`, sha256 of model+prompts+params, never raises) -> `utils/ai_helpers.py::generate_json_completion`. Structural precedent: `congress_videos/modules/chapter_speaker_resolution.py::resolve_chapter_speakers` (#263): pure module, never raises, roster-gated (slug must be in roster), confidence-gated 0.80, injectable `completion_fn`, prompts in `congress_videos/config/ai_prompts.py`. Upload-time seam: `youtube_upload_dag.py::_resolve_chapter_speaker` with `get_participants_roster()`.
- Latest migration is `044`; next is `045`. `production_schema.sql` mirror + block-scoped drift test (`tests/congress_videos/sql/test_production_schema.py`) are mandatory for any schema change.

## Approaches

### #431
1. **Extend the #340 pattern to shorts (recommended):** `write_short_srt_sidecar(...)` in `srt_helpers.py` reusing `find_srt_for_chapter` + `chapter_window_blocks` + `_serialize_srt_blocks`, windowed to `[chapter_start + pretrim_start, chapter_start + pretrim_end]` (fallback: full chapter span), written atomically to `get_chapter_shorts_dir(...)/{clip_id}.srt` from `ReapJobSensor.poke`, best-effort, no DB column. Low effort, zero migration. Con: content is an approximation shared by all clips of one project.
2. DB column tracking short SRT path/status: explicit but needs a migration and diverges from the #340 precedent.
3. Standalone "download SRTs" DAG scanning both tables: matches the issue wording literally but duplicates logic and adds a DAG. Medium-high effort.

### #432
1. **Two pure resolver modules mirroring `chapter_speaker_resolution.py`, hooked at upload time (recommended):** `resolve_mentioned_people(srt_text, participants, completion_fn)` -> new column `video_chapters.mentioned_participant_slugs TEXT[]` (migration 045); `extract_topics(srt_text, completion_fn)` -> deterministic normalisation (lowercase/strip, dedup preserving first-seen order, cap) -> reuse `video_chapters.topics`. Hooked in `youtube_upload_dag.py` next to `_resolve_chapter_speaker`, independently try/excepted so one failure never discards the other. Separate prompts/schemas produce separate cache keys. Medium effort.
2. Monitor-time hook: metadata earlier but 2 LLM calls per scored chapter regardless of upload eligibility.
3. New `video_chapter_analysis` table: clean separation but JOINs everywhere and a larger migration.

## Chained-PR slicing (auto-chain, 400-line budget)

| Slice | Scope | Est. lines | Migration |
|---|---|---|---|
| PR1 (#431) | `write_short_srt_sidecar` + `ReapJobSensor` hook + tests + docs for the shorts path | ~140-180 | none |
| PR2 (#432a) | migration 045 `mentioned_participant_slugs` + schema mirror + drift test + `resolve_mentioned_people` module + prompt + tests | ~180-230 | 045 |
| PR3 (#432b) | `extract_topics` module + prompt + upload-DAG hook for both analyses + docs | ~150-200 | none |

Decision needed before apply: No. Chained PRs recommended: Yes. 400-line budget risk: Medium (PR2 is the tightest).

## Product decisions (resolved by the orchestrator with recommended defaults)

1. No new DB column for short SRTs (match #340).
2. Short SRT windowing falls back to the full chapter span when pre-trim offsets are NULL; zero blocks -> WARNING + `None`.
3. Reuse `video_chapters.topics`; document the source-of-truth change with a column comment.
4. Hook #432 at upload time (`youtube_upload_dag.py`).
5. Unresolved or ambiguous mentions are dropped from the slug array (never invented, never stored as placeholders) and logged so they stay visible.
6. Migration number 045.

## Risks

- Short SRTs are a chapter/pre-trim-window approximation, not per-clip precise (Reap exposes no clip timing); document it.
- Overwriting `topics` at upload time changes its semantic source; re-verify consumers against HEAD at spec time.
- `mentioned_participant_slugs` is a bare `TEXT[]`; integrity is write-time only (same as `speakers`/`key_speakers`).
- PR2 is the tightest against the 400-line budget.
- Issue #431 wording ("single explicit task for both artifact types") must be reframed: the chapter half is delivered by #340; this change closes the shorts gap and verifies the chapter path.
