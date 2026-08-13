# Make test-mode video metadata authoritative

## Intent
Prevent test-mode placeholder metadata from overwriting persisted YouTube video records. Test mode must select a video only; supplied per-video YouTube metadata remains canonical.

## Goals
- In `get_video_details()`, retain the existing API-authoritative `title` behavior.
- Set `published_at` from `snippet.publishedAt` when the per-video YouTube response supplies a value.
- Preserve the incoming `published_at` when the API omits or provides an empty `publishedAt` value.
- Add focused regression coverage using the real test-mode placeholder producer and a distinct API `publishedAt` value.

## Scope and affected areas
- `congress_videos/modules/youtube/youtube_channel.py`: enrich `published_at` from supplied per-video snippet metadata.
- `tests/congress_videos/modules/youtube/test_youtube_channel_title.py` (or a narrowly renamed metadata test): prove API title and publication time supersede test-mode placeholders.

## Non-goals
- Database upsert behavior or persisted-schema changes.
- Test-mode video/channel selection behavior.
- YouTube API request contract changes.
- Migrations, backfills, or metadata fields beyond `title` and `published_at`.

## Acceptance criteria
- Given test-mode data from `create_test_video_data()` and a per-video API response with distinct `snippet.title` and `snippet.publishedAt`, `get_video_details()` returns those API values for `title` and `published_at`.
- When `snippet.publishedAt` is absent or empty, `get_video_details()` does not replace an existing `published_at` value with a missing value.
- Test mode continues to affect only the selected video; downstream consumers receive enriched metadata without database-specific changes.
- Focused regression proof passes:
  `uv run pytest tests/congress_videos/modules/youtube/test_youtube_channel_title.py -q`

## Risks and mitigations
| Risk | Mitigation |
| --- | --- |
| Missing API publication time erases an existing value | Assign only a supplied, non-empty `snippet.publishedAt`. |
| Regression test proves a synthetic path rather than the issue | Build input with `create_test_video_data()` and use API values distinct from placeholders. |
| Scope expands into persistence behavior | Limit implementation and assertions to enrichment before downstream persistence. |

## Rollback
Revert the focused enrichment and regression-test change. This restores the prior placeholder propagation behavior without schema, migration, API-contract, or database rollback work.

## Success criteria
Test-mode runs propagate API-supplied title and publication time into the enriched video record, so later persistence cannot overwrite production metadata with placeholders. The focused test remains deterministic, isolated from Airflow, database, network, and clock-sensitive freshness behavior.
