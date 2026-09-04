# Exploration: `plenary-airing-time-filter` (issue #426)

Persisted by the orchestrator verbatim from the `sdd-explore` result (the phase agent had no file-write tool).

## Current State

The monitor DAG (`congress_youtube_channel_monitor`, `congress_videos/youtube_channel_monitor_dag.py:147-167`, `@hourly`, `catchup=False`) runs the production path as:

```
t1 fetch_youtube_channel_videos
 >> t2 filter_plenary_sessions      (title + DATE filter — the bug)
 >> t2b filter_unprocessed_videos   (DB idempotency dedup)
 >> t2_guard filter_finished_streams (readiness guard — HAS liveStreamingDetails)
 >> t2a check_if_plenary_found      (branch on total_matches)
```
(`youtube_channel_monitor_dag.py:783`, `t1 >> t2 >> t2b >> t2_guard >> t2a`)

1. **`fetch_youtube_channel_videos`** (`congress_videos/modules/youtube/youtube_channel.py:27-97`) calls `search.list(part="snippet", eventType="completed", ...)`. `search.list` never returns `liveStreamingDetails` — only `snippet.publishedAt`, which for a scheduled live broadcast is the *creation* date, not the airing date (confirmed by issue evidence: video `5RNELQ2W6co`, `publishedAt=2026-08-27`, `actualEndTime=2026-09-03`).
2. **`filter_plenary_session_videos`** (`youtube_channel.py:100-150`) is the buggy predicate. It title-matches (`youtube_channel.py:135`), then computes `published_date = datetime.fromisoformat(video["published_at"]...).date()` (`youtube_channel.py:137-138`) and keeps only `range_start <= published_date <= target_date_obj` (`youtube_channel.py:141`), where `range_start = target_date - lookback_days` (`youtube_channel.py:127`, default `lookback_days=1`, DAG param `youtube_channel_monitor_dag.py:158`). This is the ONLY date gate in the whole pipeline, and it runs before any `liveStreamingDetails` data exists anywhere in the flow.
3. **`filter_finished_streams`** (`youtube_channel.py:191-320`, task `t2_guard`) already does exactly the enrichment the fix needs: ONE batched `youtube.videos().list(part="snippet,contentDetails,liveStreamingDetails", id=",".join(ids)).execute()` (`youtube_channel.py:251`) for all surviving candidates, and already extracts `live_details.get("actualEndTime")` (`youtube_channel.py:283`). But it runs **after** `t2` — by the time this data is available, the video has already been dropped by the broken date predicate. Its purpose is readiness (drop live/upcoming/still-remuxing), not date matching, and `guard_enabled=False` makes it a pure passthrough (`youtube_channel.py:234-236`) — so folding date-matching into it would make the date filter itself disable-able, which is wrong.
4. **`get_video_details`** (`youtube_channel.py:323-434`, task `t3a`) does its own per-video `videos.list(part="snippet,contentDetails,liveStreamingDetails", id=video_id)` call (`youtube_channel.py:366-368`, one call per video, not batched) and applies the *separate, deliberate* `min_hours_since_end` VOD-freshness guard (`youtube_channel.py:379-390`) against `actualEndTime` — this must stay untouched per the issue's explicit "out of scope."
5. **`check_plenary_found`** (`youtube_channel_monitor_dag.py:224-238`) branches purely on `plenary_videos["total_matches"] > 0`; on zero it only logs `logging.info` (`youtube_channel_monitor_dag.py:232`) — no WARNING distinguishing "no candidate video at all" from "a plenary aired but got filtered out." This is the gap the last acceptance criterion targets.
6. **No title-date fallback exists anywhere in the codebase.** `TARGET_VIDEO_TITLE = "Sesión Plenaria (original)"` (`congress_videos/config/constants.py:26`) is a pure substring match; there is no `dd/mm/yyyy` parser applied to `video["title"]`. The only date-in-title parsing in the module is for PDF agenda text (`extract_session_date`, `youtube_channel.py:777-975`), an unrelated concern (Spanish month names, not `dd/mm/yyyy`).
7. **Quota shape**: `search.list` = 100 units/call (already paid once in `t1`); `videos.list` = 1 unit/call regardless of id-count up to 50 (already true for `t2_guard`'s batched call and each `t3a` per-video call). Adding one more batched `videos.list` call for date-matching costs 1 extra unit per run — negligible against the existing 100-unit `search.list` call.

## Affected Areas

- `congress_videos/modules/youtube/youtube_channel.py:100-150` — `filter_plenary_session_videos`, the function that must stop keying on `published_at`.
- `congress_videos/modules/youtube/youtube_channel.py:191-320` — `filter_finished_streams`, the precedent for the exact batched-`videos.list` + `actualEndTime` extraction pattern; must NOT be repurposed for date-matching (its `guard_enabled=False` passthrough is a deliberate independent toggle).
- `congress_videos/modules/youtube/youtube_channel.py:323-434` — `get_video_details`, whose `min_hours_since_end` freshness guard against `actualEndTime` (lines 379-390) is explicitly out of scope and must remain byte-identical in behavior.
- `congress_videos/youtube_channel_monitor_dag.py:209-238` — `t2` task definition (passes `lookback_days` through, `youtube_channel_monitor_dag.py:217`) and `check_plenary_found`/`t2a` (needs the new WARNING-or-louder observability path).
- `tests/congress_videos/modules/youtube/test_youtube_channel.py:131-291` — `TestFilterPlenarySessionVideos`, currently calls `filter_plenary_session_videos` with **no** `YOUTUBE_API_KEY` env and **no** mock of `build` (e.g. `test_empty_videos_returns_zero_matches`, line 145). If the fix adds a live API call inside this function, **every existing test in this class will need `monkeypatch.setenv("YOUTUBE_API_KEY", ...)` + `mocker.patch(".build", ...)`** added, following the exact pattern already used in `TestFilterFinishedStreams` (`test_youtube_channel.py:658-700`, the `_service(items_by_id)` helper mirroring `videos().list(part, id)`).
- `tests/congress_videos/modules/youtube/test_youtube_channel_extended.py` — uses a matching `_make_video`/`_make_plenary` helper pattern (lines 14-26); any shared enrichment helper extracted from `filter_finished_streams` should follow the same mocking idiom.
- `tests/congress_videos/test_youtube_channel_monitor_dag.py:45-81` — `TestFilterUnprocessedVideosTopology` asserts the exact `t2 >> t2b >> t2_guard >> t2a` edges; the DAG topology itself does not need to change under Approach 1 (enrichment happens inside `t2`'s function body, not as a new task), so these assertions should keep passing unmodified — this is a positive constraint confirming Approach 1 is the lower-blast-radius option vs. inserting a new DAG task.
- `utils/airflow_helpers.py:10-32` — `xcom_task` wrapper, unaffected; confirms the task-callable contract (`func()` or `func(value)`, then `ti.xcom_push`) that `filter_plenary_session_videos`'s new signature must still satisfy since it's invoked via `xcom_task` at `youtube_channel_monitor_dag.py:211-221`.

## Approaches

1. **Enrich-before-filter inside `filter_plenary_session_videos`** — title-match first (cheap, no API call), then one batched `videos.list(part="liveStreamingDetails", id=...)` call for title-matched candidates only, then apply the date-range check against `actualEndTime` (fallback `actualStartTime` per the AC wording), logging a WARNING (not silent drop) when a completed broadcast is missing `liveStreamingDetails` entirely, and a WARNING when title-matches exist but zero survive the date filter.
   - Pros: minimal blast radius — DAG topology (`t1 >> t2 >> t2b >> t2_guard >> t2a`) is untouched, `t2_guard`/`get_video_details` stay conceptually separate (readiness vs. matching), quota cost is +1 unit/run, directly reuses the exact mocking idiom already proven in `TestFilterFinishedStreams`.
   - Cons: touches every existing `TestFilterPlenarySessionVideos` test (8 tests, `test_youtube_channel.py:131-291`) to add API mocking; two near-duplicate "batched `videos.list` + extract `actualEndTime`" blocks now exist in the module (this one and `filter_finished_streams`) unless factored into a small shared helper.
   - Effort: Medium.

2. **Move the date filter after `t2_guard` (reorder title-filter vs. date-filter across two DAG tasks)** — keep `filter_plenary_session_videos` as a title-only filter (no API call, no test breakage), let `t2_guard`'s already-fetched `liveStreamingDetails` flow through unchanged, and add a *new* date-filtering step after `t2_guard` (or fold the date check into `t2_guard` behind a distinct, non-optional code path).
   - Pros: avoids touching the 8 existing title/date tests' mocking setup for `filter_plenary_session_videos` itself.
   - Cons: conflates two independent concerns inside `t2_guard` (readiness gate whose `guard_enabled=False` deliberately passthroughs everything, vs. a date predicate that must never be disable-able) — high risk of accidentally making the date filter skippable; `check_plenary_found`'s "total_matches" semantics get muddier because `t2_guard` currently recomputes `total_matches` from readiness-kept videos only (`youtube_channel.py:318-319`); requires either a new DAG task (topology change, breaks `TestFilterUnprocessedVideosTopology` assumptions) or entangling `filter_finished_streams`'s contract.
   - Effort: Medium-High, higher regression risk.

3. **Fetch `liveStreamingDetails` in `fetch_youtube_channel_videos` itself (t1)** — batch-enrich every candidate right after `search.list`, before any filtering.
   - Pros: single enrichment point, all downstream filters (title, date, readiness) see the same enriched shape.
   - Cons: `t1` currently returns up to `max_results` (default 20) raw candidates *before* any title filtering — enriching all of them costs the same 1 `videos.list` unit (batched, <=50 ids) but does unnecessary work for titles that will be dropped anyway; also changes `fetch_youtube_channel_videos`'s return contract, which is exercised by 5 dedicated tests (`test_youtube_channel.py:14-123`) plus test-mode's `create_test_video_data` (`youtube_channel_monitor_dag.py:184-194`, which bypasses `t1` entirely and must stay untouched) — wider blast radius than Approach 1 for no material benefit since quota cost is identical either way.
   - Effort: Medium, unnecessary scope increase.

## Recommendation

**Approach 1** (enrich-before-filter inside `filter_plenary_session_videos`). It is the only option that: (a) fixes the predicate at its actual source without changing DAG topology or task contracts elsewhere, (b) keeps `filter_finished_streams`'s `guard_enabled` toggle semantics intact (a non-optional date filter must never be gated behind an optional readiness guard), (c) keeps `get_video_details`'s `min_hours_since_end` freshness guard byte-identical as the issue explicitly requires, and (d) reuses a batched-`videos.list`-by-id mocking pattern the test suite already has a working idiom for (`TestFilterFinishedStreams`, `test_youtube_channel.py:682-700`). The spec/design phase should decide whether to extract a small shared `_fetch_live_streaming_details(youtube, ids)` helper to avoid duplicating the batched-call logic now present in both `filter_plenary_session_videos` and `filter_finished_streams`.

For the "found a plenary but matched nothing" observability AC, the simplest implementation with no new API calls: inside `filter_plenary_session_videos`, count `title_matches` separately from the final `matching_videos`, and `logging.warning(...)` when `title_matches > 0` and the final list is empty (distinguishing "channel had no candidate at all" from "channel had one, it fell outside the window or its liveStreamingDetails was missing"). No title-based `dd/mm/yyyy` fallback parser is needed or exists today — do not add one; it would be new unrequested scope on top of the airing-time fix.

## Risks

- Changing `filter_plenary_session_videos`'s internals to make a live API call requires `YOUTUBE_API_KEY` and `build` mocking in all 8 existing tests in `TestFilterPlenarySessionVideos` (`test_youtube_channel.py:131-291`); missing even one will make that test call the real network or raise `ValueError` for a missing key — both are visible failures, not silent false-passes, but the test count to update is non-trivial.
- `min_hours_since_end=12` in `get_video_details` (`youtube_channel.py:386`) is a *separate* freshness guard already keyed on `actualEndTime`; must verify the fix does not touch this function at all, only what feeds into it upstream.
- The AC "actualEndTime, or actualStartTime" fallback needs a design decision: should a still-in-progress-but-somehow-`eventType=completed` edge case (start present, end missing) match on start alone, or is that exactly the "missing liveStreamingDetails on a completed broadcast" failure case that must WARN instead? This needs to be resolved explicitly in `sdd-spec`, not left implicit.
- No `CONTEXT.md` or `docs/adr/` files exist yet in this worktree despite `CLAUDE.md` referencing that layout — nothing to cross-check against.

## Ready for Proposal

Yes.
