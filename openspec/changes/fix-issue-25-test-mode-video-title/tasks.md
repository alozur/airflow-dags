# Tasks: Make test-mode video metadata authoritative

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | 45–80 (two implementation files; SDD artifacts excluded) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR: focused metadata enrichment and regression coverage |
| Delivery strategy | auto-forecast |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

## Implementation work

### RED — reproduce API publication-time authority failure

- [x] In `tests/congress_videos/modules/youtube/test_youtube_channel_title.py`, replace the hand-built selected-video fixture path with `create_test_video_data(test_url)` from `congress_videos.modules.youtube.download`; extend `_make_api_response` (or its focused replacement) to supply a non-empty `snippet.publishedAt` distinct from the producer placeholder, retain a historical `actualEndTime`, and assert `get_video_details(..., min_hours_since_end=0)` returns both the API title and API publication time. Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q` and record the expected RED failure specifically at the new `published_at` assertion while title coverage remains valid. Rollback boundary: revert only this focused test edit. <!-- sdd-owner: implementation -->

### GREEN — enrich only supplied publication time

- [x] In `congress_videos/modules/youtube/youtube_channel.py` at `get_video_details()`'s `enriched_video` construction, preserve the existing API-title assignment and conditionally assign `enriched_video['published_at']` only when `video_details['snippet'].get('publishedAt')` is truthy; do not change the request `part` list, freshness guard, duration parsing, response shape, persistence, or exception behavior. Re-run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q` and record GREEN. Rollback boundary: remove only this conditional enrichment, restoring the prior selected-record value behavior. <!-- sdd-owner: implementation -->

### TRIANGULATE — prove omitted and empty fallback behavior

- [x] In `tests/congress_videos/modules/youtube/test_youtube_channel_title.py`, add focused parameterized coverage for an omitted `snippet.publishedAt` and `snippet.publishedAt: ""`, creating a fresh input through `create_test_video_data(test_url)` for each case and asserting the returned `published_at` equals that input's original placeholder value; keep API/network, Airflow, database, and clock-sensitive behavior mocked or bypassed through the existing build seam and `min_hours_since_end=0`. Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q` and record all authority and fallback cases passing. Rollback boundary: revert only these fallback cases without changing production behavior. <!-- sdd-owner: implementation -->

### REFACTOR — confirm narrow, readable regression coverage

- [x] Refactor only `tests/congress_videos/modules/youtube/test_youtube_channel_title.py` as needed to remove duplicated API-response or placeholder setup while preserving explicit assertions that API title and non-empty API publication time win, and that omitted/empty publication time preserves the input; keep the test module name and focused command unchanged. Run `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q` after the refactor and inspect the diff to confirm only `congress_videos/modules/youtube/youtube_channel.py` and this focused test changed. Rollback boundary: revert the focused production-and-test work unit; no schema, migration, API contract, or persistence rollback is required. <!-- sdd-owner: implementation -->

## Parent lifecycle actions

- [x] Review decision recorded: the user declined the candidate-specific bounded review; no review authority or receipt was created, and delivery follows ordinary repository policy. <!-- sdd-owner: parent -->
