# Design: Filter plenary sessions by airing time

## Technical Approach

Approach 1 (enrich-before-filter) inside `filter_plenary_session_videos`. Order is load-bearing:
falsy-input guard → title match → **early return when zero title matches** → API key → `build` →
one batched `videos.list` → airing-date window → zero-survivor WARNING. The early return is what
satisfies "zero title matches, no `videos.list` call". DAG topology and XCom keys are unchanged.

## Architecture Decisions

| Decision | Choice | Alternative rejected | Rationale |
|---|---|---|---|
| Client construction | `os.getenv("YOUTUBE_API_KEY")` + `build(...)` **after** the title pass | Build at function top (`filter_finished_streams`' idiom) | Top-building makes a zero-title-match run demand a key, which contradicts the spec's "no API call" requirement |
| Missing key with title matches | `ValueError`, same message as `fetch_youtube_channel_videos` | Silent passthrough | The date gate must never degrade into "keep everything" |
| Airing key | `actualEndTime` → `actualStartTime` (WARNING) → WARNING + exclude | `publishedAt` fallback; raising | Spec precedence table; one malformed item must not kill an `@hourly` run |
| Non-string / unparseable timestamp | Treated as **missing**: WARNING + exclude | Propagate the exception | Spec: MUST NOT raise on a malformed item |
| Timezone | `.astimezone(UTC).date()` | Bare `.date()` (today's behavior) | Spec says *UTC calendar date*; a non-`Z` offset would otherwise shift the window a day |
| Per-video enrichment | Add `actual_end_time` / `actual_start_time` (may be `None`); every existing key untouched | Return dicts unmodified | Spec MAY-allows it; gives the run a forensic record of why it matched |
| Shared fetch | Pure lift of lines 250–252 into `_fetch_video_items_by_id` | Keep the duplication | One call, one `part` argument, no chunking, no empty-ids short-circuit *in the helper* |

## Interfaces

```python
def _fetch_video_items_by_id(youtube, ids: list[str], part: str) -> dict[str, dict]:
    resp = youtube.videos().list(part=part, id=",".join(ids)).execute()
    return {it["id"]: it for it in resp.get("items", [])}

def _airing_timestamp(item: dict | None) -> tuple[str | None, str | None]:
    """(iso_timestamp, "actualEndTime" | "actualStartTime") or (None, None).

    A value is returned only when `isinstance(value, str) and value` — a non-string
    or empty timestamp is reported as absent, so the caller never parses a non-str.
    """

def _airing_date(iso_timestamp: str) -> date:
    return datetime.fromisoformat(iso_timestamp.replace("Z", "+00:00")).astimezone(UTC).date()
```

**Exception discipline.** The `isinstance` guard in `_airing_timestamp` makes `TypeError` and
`AttributeError` unreachable at the parse site; the call is still wrapped in
`except (ValueError, TypeError, AttributeError)` → WARNING + exclude, so a shape the guard does not
anticipate still cannot raise out of the function. `date` joins the existing
`from datetime import UTC, datetime, timedelta`. Branching lives in the helpers so the predicate
stays under the C901 threshold of 10 — the file's existing `C901` per-file-ignore is REMOVE-ONLY and
must **not** be leaned on. Line length 120.

## Log shapes (assertable via `caplog`; f-strings, as in the surrounding function)

| Case | Message prefix |
|---|---|
| Start-time fallback | `Airing time for {video_id} falls back to actualStartTime ({ts}); actualEndTime absent` |
| No airing time | `No airing time for {video_id}: liveStreamingDetails has neither actualEndTime nor actualStartTime; excluding` |
| Unparseable | `Unparseable airing time for {video_id}: {ts!r}; excluding` |
| Zero survivors | `No plenary matched: {n} title match(es) {ids} with airing dates {dates} outside window [{range_start} .. {target_date_obj}]` |

`{dates}` renders the resolved UTC date per id and the literal string `"missing"` for a candidate
whose airing time could not be resolved (e.g. `['2026-09-01', 'missing']`), so one WARNING
distinguishes "aired outside the window" from "no data at all".

## Test plan (RED first, `strict_tdd: true`)

Seam: the public `filter_plenary_session_videos` plus the mocked `youtube_channel.build`. Private
helpers are exercised **through** it — no direct tests. `_service` / `_item` / `_iso_minutes_ago`
live at module scope (line 658+) and resolve fine from the earlier class.

**Every one of the 9 existing tests gains `monkeypatch.setenv("YOUTUBE_API_KEY", "fake")` and a
`build` mock** (the class count is 9, not the 8 the proposal states):

| Test | `build` mock |
|---|---|
| `test_empty_videos_returns_zero_matches` | `side_effect=AssertionError("build should not be called")` — no-call proof |
| `test_none_input_returns_zero_matches` | same no-call proof |
| `test_target_date_preserved_in_result` | same no-call proof |
| `test_title_mismatch_returns_zero_matches` | same no-call proof — this is the spec's "zero title matches skip the API" scenario |
| `test_title_match_and_date_match_returns_one` | `return_value=_service({...})`, item airing on target |
| `test_date_outside_lookback_window_returns_zero_matches` | `_service`, airing 3 days early |
| `test_title_match_is_case_insensitive` | `_service` |
| `test_multiple_matches_all_returned` | `_service`, two ids |
| `test_lookback_range_keeps_today_and_yesterday_drops_older` | `_service`; re-keyed to airing dates |

The four `AssertionError` rows use the existing idiom at `test_youtube_channel.py:992-995`: they
prove no call *and* prove the key is never required, which a bare `assert_not_called()` after a
plain mock would not.

Extend `_item` with `actual_start: str | None = None` (the default keeps `TestFilterFinishedStreams`
byte-identical).

**New tests**

| # | Test | Shape |
|---|---|---|
| 1 | `5RNELQ2W6co` regression | published `2026-08-27T08:05:32Z`, start `2026-09-03T06:55:01Z`, end `2026-09-03T13:09:16Z`, target `2026-09-03`, lookback 1 → matches |
| 2 | Lower boundary kept | airing `== target_date - lookback_days` → matches |
| 3 | **Upper boundary dropped** | airing `== target_date + 1 day` → does not match (spec scenario, previously uncovered) |
| 4 | Missing `liveStreamingDetails` | excluded + WARNING naming the id |
| 5 | `actualStartTime` fallback | matches + WARNING naming the id |
| 6 | Non-string / unparseable timestamp | excluded, no raise |
| 7 | Id absent from the API response | excluded, no raise |
| 8 | Exactly one call | title-matched `A,B,C` → one call, `id="A,B,C"`, `part="liveStreamingDetails"` |
| 9 | Zero-survivor WARNING | **GIVEN one candidate out of window and one with no `liveStreamingDetails`** → `total_matches == 0`, one WARNING naming both ids, `'missing'` for the second, and the window |
| 10 | Existing keys survive | a surviving candidate keeps `video_id`, `title`, `published_at` unchanged |
| 11 | **`filter_finished_streams` parity** (new test *in* `TestFilterFinishedStreams`) | asserts `youtube.videos().list` called **exactly once** with `part="snippet,contentDetails,liveStreamingDetails"` and `id="A,B"` |

Test 11 exists because no current test asserts `part` or call count, so "the existing tests pass
unmodified" does **not** prove the lift preserved the call shape. If test 11 cannot be made green
without touching `filter_finished_streams`' behavior, drop the lift and keep the duplication.

## Diff forecast

| File | ± | Note |
|---|---|---|
| `congress_videos/modules/youtube/youtube_channel.py` | ~+92 / −30 | 3 helpers, rewritten predicate, docstring, lift |
| `tests/.../test_youtube_channel.py` | ~+188 / −18 | 9 tests re-mocked, `_item` kwarg, 11 new tests |
| `congress_videos/youtube_channel_monitor_dag.py` | ~+1 / −1 | param comment |

**≈ 330 changed lines. A single PR still fits the 400-line budget, but the margin is ~70 lines —
budget risk Medium-High.** If it overruns: PR 1 = `_fetch_video_items_by_id` + parity test 11
(`filter_finished_streams` unchanged in behavior, ~40 lines); PR 2 = the airing-time predicate and
its tests. Do not shrink the diff by dropping tests or docs.

## Docs

`filter_plenary_session_videos`' docstring ("based on title and date", "its published date falls
in") **must change in the same commit** — docs drift is invisible to a green suite.
`youtube_channel_monitor_dag.py:158`'s param comment gains "airing". `docs/ARCHITECTURE.md`,
`docs/DAGS.md`, `congress_videos/SIMPLIFY_YOUTUBE_MONITOR.md` and
`congress_videos/IDEMPOTENT_MONITOR_PLAN.md` name the task but never the `publishedAt` keying — no
update required. (`docs/DAGS.md`' graph is already stale: it omits `filter_unprocessed_videos` and
`filter_finished_streams`. Pre-existing, **out of scope** — open a follow-up.) No `CONTEXT.md` or
`docs/adr/` exists.

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary. The one external boundary is a read-only YouTube Data API call already
made twice elsewhere in this module.

## Migration / Rollout

No migration. Revert the single commit: the predicate returns to `published_at` and the helper
inlines back. The added per-video keys have no consumer, so the revert cannot orphan data.

## Open Questions

None.
