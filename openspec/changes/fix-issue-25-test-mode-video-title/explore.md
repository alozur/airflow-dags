# Exploration: Test-mode video metadata authority

## Scope

Issue #25 says test mode should choose only a YouTube video ID/URL. Canonical persisted metadata must be refreshed from `youtube.videos().list(part='snippet,contentDetails,liveStreamingDetails')`, rather than retaining placeholder values from `create_test_video_data()`.

## Data flow

1. `congress_videos/youtube_channel_monitor_dag.py` branches on `isTesting`.
2. The test branch calls `download.create_test_video_data()` and writes `plenary_videos` to XCom. Its one video has placeholder `title` (`Test Video - Sesión Plenaria`) and `published_at` (`2025-01-01T10:00:00Z`).
3. Both test and production paths invoke `youtube_channel.get_video_details()` with that `plenary_videos` XCom. The production path instead originates in `fetch_youtube_channel_videos()`, which gets `snippet.title` and `snippet.publishedAt` from YouTube search results.
4. `get_video_details()` obtains the authoritative per-video API response and enriches the incoming dict. Downstream download/chapter transformations propagate `video['title']` into `video_title`.
5. `CongressionalVideoDB.save_youtube_chapters_to_db()` writes `video_title` to `youtube_source_videos`; its `ON CONFLICT (video_id) DO UPDATE` overwrites `video_title` with the submitted value.

## Current state and finding

The worktree already contains the title correction in `congress_videos/modules/youtube/youtube_channel.py`: after `**video`, it explicitly assigns `title: video_details['snippet']['title']`. It also contains `tests/congress_videos/modules/youtube/test_youtube_channel_title.py`, whose mocked API response has a title distinct from the placeholder and asserts that the API title wins.

Consequently, the existing title test is deterministic and tight, but it is currently expected to be green rather than RED. It is not evidence that the issue remains unfixed in this worktree. No code or tests were changed during exploration.

The API details request already includes `snippet`, so `snippet.publishedAt` is available when YouTube provides it. `get_video_details()` currently does **not** replace the incoming `published_at` with `video_details['snippet'].get('publishedAt')`; therefore test mode can still carry the placeholder date. This is the remaining metadata-authority gap.

## Recommended regression seam

Use a focused unit test for `get_video_details()` in `tests/congress_videos/modules/youtube/test_youtube_channel_title.py` (or a narrowly renamed metadata test):

- Arrange with `create_test_video_data(test_url)` to exercise the real test-mode placeholder producer, not a hand-built approximation.
- Patch `youtube_channel.build`; return one `videos().list(...).execute()` response with an old `actualEndTime`, valid `contentDetails.duration`, `snippet.title` different from the placeholder, and `snippet.publishedAt` different from the placeholder date.
- Call `get_video_details(..., min_hours_since_end=0)`.
- Assert one enriched video; assert its `title` equals the API title and differs from the placeholder; assert `published_at` equals the API `publishedAt` when supplied.

Command: `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q`.

This test is isolated from Airflow, database, network, and clock-sensitive freshness behavior (the end time is historical and the margin is zero). In the current worktree, its title assertion is green because the title fix exists; the `published_at` assertion is RED-capable against the currently missing propagation.

## Context and ADRs

`CONTEXT.md` and `docs/adr/` are absent in this worktree, so there is no applicable ADR conflict to report. `docs/agents/domain.md` directs this absence to be handled silently.

## Constraints

- Preserve production behavior and the original test-mode selection mechanism.
- Only make API metadata authoritative when the response provides the relevant value; do not replace a value with a missing/empty API field.
- The database upsert is correctly behaving as designed: it persists the metadata passed by upstream code; the correction belongs at enrichment.
