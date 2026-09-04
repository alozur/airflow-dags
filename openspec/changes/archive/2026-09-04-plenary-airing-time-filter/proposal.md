# Proposal: Filter plenary sessions by airing time, not publish time

## Intent

**Problem**: `filter_plenary_session_videos` (`youtube_channel.py:137-141`) gates on
`snippet.publishedAt`. For a scheduled live broadcast that is the *creation* date, not the airing
date. Video `5RNELQ2W6co` was created 2026-08-27 and aired 2026-09-03, so with `lookback_days=1` the
2026-09-03 plenary was silently dropped. It is the pipeline's only date gate, and it runs before any
`liveStreamingDetails` exists in the flow.

**Success**: a plenary aired inside the window matches regardless of `publishedAt`, and a run that
sees a candidate but matches nothing is visible in the logs.

## Scope

### In Scope
- Re-key the date predicate to the airing time, enriching title-matched candidates with one batched
  `videos.list(part="liveStreamingDetails", id=...)`.
- WARNING observability for missing `liveStreamingDetails` and for zero-survivor runs.
- Extract `_fetch_video_items_by_id(youtube, ids, part)` shared with `filter_finished_streams`
  (decision 4: viable — that call sits outside the per-candidate `try`, uses a fixed `part`, and does
  no chunking; a pure lift preserves all three). On any observable divergence, keep the duplication.
- Add API mocking to the 8 `TestFilterPlenarySessionVideos` tests, per the `TestFilterFinishedStreams` idiom.

### Out of Scope
- `get_video_details`' `min_hours_since_end=12` guard; `filter_finished_streams`' readiness semantics
  and `guard_enabled=False` passthrough; DAG topology; `check_plenary_found`'s branch key.
- Any `dd/mm/yyyy` title parser; pypdf import failures; diarize-api OOM.

## Capabilities

### New Capabilities
- `plenary-session-matching`: which channel videos count as the plenary for a target date — title
  match, airing-time window, missing-data handling, match observability.

### Modified Capabilities
- None (`openspec/specs/` is empty).

## Approach

Exploration Approach 1 (enrich-before-filter), inside `t2`'s callable:

1. Title-match first; zero matches returns early with no API call.
2. One batched `videos.list` for matched ids (+1 quota unit vs. the existing 100-unit `search.list`).
3. Airing key `actualEndTime`; else `actualStartTime` with a WARNING naming the id; else WARNING and
   **exclude** — never raise (one malformed item must not kill the hourly run), never use `publishedAt`.
4. Window `[target_date - lookback_days, target_date]` on the airing timestamp's UTC date.
5. Title matches > 0 with zero survivors → WARNING naming ids, airing dates, and the window.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `youtube_channel.py:100-150` | Modified | Airing-time predicate + enrichment |
| `youtube_channel.py:249-252` | Modified | Batched call lifted to shared helper |
| `tests/.../test_youtube_channel.py:131-291` | Modified | Mocking + `5RNELQ2W6co` regression |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| A missed mock hits the network | Med | Fails loudly (`ValueError`), never a false pass |
| Lift alters `filter_finished_streams` | Low | Its tests must pass unmodified, or drop the lift |
| Window still excludes a real session | Low | Zero-survivor WARNING makes it observable |

## Rollback Plan

Revert the single commit: the predicate returns to `published_at` and the helper inlines back. No
schema, migration, DAG topology, or XCom contract changes.

## Dependencies

None. `videos.list` is already used twice in this module.

## Success Criteria

- [ ] `5RNELQ2W6co` shape (published 08-27, ended 09-03, target 09-03, lookback 1) matches.
- [ ] Airing time outside the window does not match.
- [ ] Missing `liveStreamingDetails` excludes with a WARNING naming the id.
- [ ] Zero survivors after title matches emits a WARNING.
- [ ] `uv run pytest` green; `get_video_details` and `filter_finished_streams` unchanged.
