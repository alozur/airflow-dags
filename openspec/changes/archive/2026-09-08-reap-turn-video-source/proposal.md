# Proposal: Source Reap shorts from diarized speaker-turn videos

## Intent

**Problem.** The Reap preparer selects `video_chapters` gated on
`is_uploaded_to_youtube = TRUE` — a flag now set only as a side effect of the
one turn/day published by `youtube_upload_dag`, so generation is throttled to
~1 chapter/day. `vc.youtube_upload_date IS NOT NULL` throttles publication
identically. The preparer also re-cuts every clip from the raw source although
diarization already materialized per-turn `output_path` files.

**Success.** Reap sources materialized turn videos, decoupled from the
long-form cadence: 28 eligible groups (120–900 s) instead of 11 gated
chapters, with no re-cut.

## Scope

### In Scope
- Migration `047`: `video_shorts.turn_id INTEGER REFERENCES
  speaker_turn_videos(turn_id) ON DELETE SET NULL` + index + snapshot/test.
- Turn-selection DB method replacing `get_chapters_for_shorts` (deleted);
  `insert_video_short(turn_id=...)`; dedup on `turn_id`.
- Preparer consumes `output_path`; pre-trim only above the Reap ceiling;
  zero-eligible WARNING with count.
- `claim_pending_clip` + `pending_shorts_candidate_sql` rebased to `turn_id`.
- Both parent-published gates dropped.

### Out of Scope
- Metrics infra, Reap scoring, `paths.py`, live-Postgres tests.
- Backfilling `turn_id` on legacy `video_shorts` rows.

## Capabilities

### New Capabilities
- `reap-turn-sourced-clips`: eligibility, deterministic selection, dedup key,
  Tier-1 partitioning, and pre-trim rules for turn-sourced shorts.

### Modified Capabilities
- `short-video-srt-artifacts`: its window requirement assumes chapter-relative
  pre-trim offsets (`srt_helpers.py:7,508`); turn-sourced offsets are
  file-relative and must be guarded.

## Approach

Mirror `uploadable_turns` (`production_schema.sql:529-625`): unfiltered
`group_spans` CTE (MIN/MAX per `output_path` — the #151 trap), then
`DISTINCT ON (output_path) ORDER BY output_path, turn_id`. Gates:
`output_path IS NOT NULL`, `NOT COALESCE(is_procedural,false)`, group span
within the 120–900 s floor/ceiling, no existing `video_shorts.turn_id`.
Explicit non-gates: `prepared_at`, relevance threshold, parent-published.
Order `COALESCE(interest_score,1) DESC, relevance_score DESC,
session_date DESC, turn_id ASC`. `chapter_id` stays NOT NULL and populated;
consumers reach turns via `LEFT JOIN`/`COALESCE` so legacy `turn_id IS NULL`
rows keep per-chapter behaviour, and the Tier-1 ranking CTE stays unfiltered
by upload state (#262). Pre-trim runs only when ffprobe duration exceeds
`pre_trim_threshold_secs` (600 → 900), on file-relative time.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `sql/migrations/047_*.sql`, `production_schema.sql:123-162,386-391` | New/Modified | Column + index + snapshot |
| `modules/database.py:507-554` | Removed | `get_chapters_for_shorts` |
| `modules/database.py:67-105,556-615,716-763,803-896` | Modified | Insert, claim, tiering, gate drop |
| `reap_clip_preparer_dag.py:59-331` | Modified | `output_path` + new pre-trim |
| `srt_helpers.py:500-580` | Modified | Guard turn-relative offsets |
| `tests/congress_videos/**` | Modified | Schema, DB, DAG tests |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Short publishes before its long-form parent | High (accepted) | `_format_own_channel_footer` null-safe; `NULLS LAST` ordering |
| Legacy `turn_id IS NULL` rows dropped from queues | Med | `LEFT JOIN`/`COALESCE`; legacy regression test |
| Tier-1 repartition repeats #262 | Med | Ranking CTE unfiltered; `test_rank_universe_includes_uploaded_clips` |
| Pre-trim/SRT window mis-timed on turn files | Med | File-relative rule settled in design; sidecar fallback |
| Volume jumps 11 → 28 groups | Med | Reap ceiling, Tier-1 cap, source cool-down unchanged |

## Rollback Plan

Revert the chained PRs in reverse merge order (4b → 4a → 3 → 2 → 1). Leave
`047` applied: `turn_id` is additive and nullable, ignored by reverted code
(repo convention keeps DOWN commented). Units 1–2 are inert without a caller,
so a preparer-only fault can revert unit 3 alone; 4a/4b degrade to per-chapter
behaviour through the same `COALESCE` fallback legacy rows already need.
Deploy-free stop-gap: pause `congress_reap_clip_preparer`.

## Dependencies

- Migration `046` applied in both schemas (confirmed 2026-09-07).
- Diarization populating `speaker_turn_videos.output_path`.

## Success Criteria

- [ ] Selection is independent of any parent-upload flag.
- [ ] One `video_shorts` row per `output_path` group.
- [ ] `get_chapters_for_shorts`, `_find_source_video`, and the
      `split_video_chapter` call removed.
- [ ] Legacy `turn_id IS NULL` rows still claim, tier, and upload.
- [ ] Zero-eligible runs log a WARNING naming the count.
- [ ] `uv run pytest` green; each of the 5 work units under 400 changed lines.
