# Make per-video snippet metadata authoritative

`get_video_details()` remains the sole enrichment boundary. It will preserve the selected video's input shape, always use the detailed API response for `title`, and replace `published_at` only when the detailed response supplies a truthy `snippet.publishedAt` value.

## Decision

| Area | Design |
| --- | --- |
| Authority | The per-video `videos().list(part='snippet,contentDetails,liveStreamingDetails')` response is authoritative for `snippet.title` and a supplied `snippet.publishedAt`. |
| Publication-time fallback | Start with `**video`; after building the enriched record, assign `published_at` only if `video_details['snippet'].get('publishedAt')` is truthy. Missing or `""` therefore leaves the incoming value untouched. |
| Test-mode boundary | `create_test_video_data()` remains unchanged and supplies the real placeholder record used by the regression test. Test mode selects the video only. |
| Out of scope | No persistence, schema, selection, migration, API-request, or unrelated metadata changes. |

## Data flow

1. Test mode creates a selected-video record through `create_test_video_data()`, including placeholder title and publication time.
2. `get_video_details()` requests the selected video using its existing per-video details request.
3. Once the existing VOD freshness guard accepts the response, the enrichment step spreads the selected record, sets `title` from `snippet.title`, and conditionally replaces `published_at` from non-empty `snippet.publishedAt`.
4. The enriched record is returned unchanged to existing downstream consumers; persistence receives the corrected values without any database-specific change.

## Implementation plan

### `congress_videos/modules/youtube/youtube_channel.py`

At the existing `enriched_video` construction in `get_video_details()`:

- Keep the current explicit API-title assignment intact.
- Read `snippet.publishedAt` from the already-fetched `video_details['snippet']`.
- If the value is truthy, set `enriched_video['published_at']` to it; otherwise do not write that key after the `**video` spread.
- Do not change the API `part` list, freshness guard, duration parsing, output envelope, or exception behavior.

This conditional assignment is deliberately preferred to assigning `None` or `""` in the dict literal: the incoming value stays intact for both omitted and empty API values.

## Strict-TDD regression strategy

Keep the focused test module at `tests/congress_videos/modules/youtube/test_youtube_channel_title.py` so the approved command remains stable; extend it from title-only coverage into the narrow metadata regression.

### RED

1. Change test setup to call `create_test_video_data(test_url)` from `congress_videos.modules.youtube.download`; do not hand-build the input record.
2. Mock the existing `youtube_channel.build` seam and return a historical `actualEndTime`, valid duration, a title distinct from the producer's placeholder, and a distinct non-empty `snippet.publishedAt`.
3. Call `get_video_details(..., min_hours_since_end=0)` and assert the returned record has the API title and API publication time, each differing from the producer's placeholders.
4. Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q`. Before the production change, the new publication-time assertion fails because the input placeholder survives; existing title assertions may remain green.

### GREEN

1. Apply only the conditional `published_at` enrichment described above.
2. Re-run the same focused command and expect all title and publication-time cases to pass.
3. Add or retain parameterized preservation coverage using a newly produced `create_test_video_data()` record for each API shape: omitted `publishedAt` and `publishedAt: ""`. Assert that each returned `published_at` equals the producer's original placeholder date.

The mock remains deterministic: it performs no network, Airflow, database, or clock-sensitive waiting; the historical end time and zero-hour margin satisfy the existing freshness guard.

## Contracts and acceptance mapping

| Requirement | Evidence |
| --- | --- |
| API title and supplied publication time win | Test-mode producer input plus a distinct detailed API title and `publishedAt`; assert both returned values. |
| Omitted or empty publication time preserves input | Parameterized response shapes; assert the original producer `published_at` remains. |
| Test mode only selects a video | The test uses the unmodified real placeholder producer and verifies enrichment solely through `get_video_details()`. |
| No persistence/API-contract changes | Implementation is restricted to the post-response enriched-record construction; request parameters and downstream interfaces are untouched. |

## Files to change during apply

- `congress_videos/modules/youtube/youtube_channel.py`
- `tests/congress_videos/modules/youtube/test_youtube_channel_title.py`

## Rollout and rollback

The change has no migration, configuration, or deployment sequencing. Deploy it with the ordinary application release. Reverting the two focused file changes restores prior propagation behavior; no stored-data rollback is required.

## Risks

| Risk | Mitigation |
| --- | --- |
| Empty API data overwrites a usable selected-record value | Assign only a truthy `snippet.publishedAt`. |
| Test no longer represents test mode | Obtain every regression input from `create_test_video_data()`. |
| Accidental scope expansion | Keep the change at the enrichment seam and retain the existing test file and focused command. |
