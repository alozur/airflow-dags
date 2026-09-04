# Plenary Session Matching Specification

## Purpose

Which channel videos count as "the plenary" for a target date: title match,
airing-time window (replacing publish-time), missing-data handling, and
match observability.

## Requirements

### Requirement: Title match gates the API call

MUST substring-match `target_title` (case-insensitive) against `title`
before any `videos.list` call. Zero matches, or empty/missing
`channel_videos`, MUST return `{"total_matches": 0, "videos": [], "target_date": target_date}`
with no `videos.list` call.

#### Scenario: Zero title matches skip the API
- GIVEN no candidate title contains `target_title`
- WHEN the function runs
- THEN `total_matches` is `0` and `videos.list` is never called

### Requirement: One batched airing-time lookup via a shared helper

Title-matched candidates MUST be enriched via exactly ONE call to
`videos.list(part="liveStreamingDetails", id=<comma-joined ids>)`, through a
shared helper `_fetch_video_items_by_id(youtube, ids, part)` that also backs
`filter_finished_streams`. The lift MUST NOT change `filter_finished_streams`'s
call shape: same single call, its existing `part` string, no chunking, no
new empty-ids short-circuit.

#### Scenario: One call for all matched ids
- GIVEN title-matched ids `A`, `B`, `C`
- WHEN the function runs
- THEN `videos.list` is called once with `id="A,B,C"`, `part="liveStreamingDetails"`

#### Scenario: filter_finished_streams call shape is unchanged
- GIVEN candidates from `filter_unprocessed_videos`, `guard_enabled=True`
- WHEN `filter_finished_streams` runs
- THEN `videos.list` is still called once with its existing `part` string and ids

### Requirement: Airing key precedence, never publish time

Per candidate: use `actualEndTime` if present; else `actualStartTime` with a
WARNING naming the id; else (no `liveStreamingDetails`, or neither
timestamp) WARNING naming the id and EXCLUDE. MUST NOT raise on a
malformed/missing item. MUST NOT use `published_at`/`publishedAt` as a
fallback.

#### Scenario: Precedence table decides the key and the WARNING

| liveStreamingDetails state | Key used | WARNING? |
|---|---|---|
| `actualEndTime` present | `actualEndTime` | no |
| only `actualStartTime` present | `actualStartTime` | yes, names id |
| absent, or neither timestamp | none — excluded | yes, names id |

- GIVEN any of the three states above
- WHEN the key is resolved
- THEN the table's outcome holds and no exception is ever raised

### Requirement: Airing-time window replaces publish-time window

Kept only when the airing timestamp's UTC calendar date falls within
`[target_date - lookback_days, target_date]` inclusive; `lookback_days`
keeps its default `1` and meaning. (Previously: compared `publishedAt`'s
date instead.)

#### Scenario: Regression — 5RNELQ2W6co matches
- GIVEN id `5RNELQ2W6co`, `published_at=2026-08-27T08:05:32Z`,
  `actualStartTime=2026-09-03T06:55:01Z`, `actualEndTime=2026-09-03T13:09:16Z`
- WHEN called with `target_date="2026-09-03"`, `lookback_days=1`
- THEN the candidate matches, `total_matches` is `1`

#### Scenario: Outside the window does not match; boundaries do/don't
- GIVEN an airing date on `2026-09-01` (same target/lookback) it does not match
- GIVEN an airing date equal to `target_date - lookback_days` it matches
- GIVEN an airing date equal to `target_date + 1 day` it does not match

### Requirement: Zero survivors are observable; existing keys are preserved

When title matches exist but none survive the window, log exactly one
WARNING naming every title-matched id, each resolved airing date (or
`"missing"`), and the window bounds. Return shape stays
`{"total_matches": int, "videos": [...], "target_date": str}` with
`total_matches` counting window survivors; every existing video dict key
(`video_id`, `title`, `published_at`, ...) MUST remain unchanged, and the
function MAY add `actual_end_time`/`actual_start_time`.

#### Scenario: Title matches, zero survivors, one WARNING
- GIVEN one candidate outside the window and one with no `liveStreamingDetails`
- WHEN the function runs
- THEN `total_matches` is `0` and one WARNING names both ids, their dates
  (or `"missing"`), and the window

#### Scenario: Existing keys survive enrichment
- GIVEN a candidate that survives the window
- WHEN the function runs
- THEN its `video_id`, `title`, `published_at` are unchanged in the output

### Requirement: Downstream guards stay unmodified

`get_video_details`'s `min_hours_since_end` guard and
`filter_finished_streams`'s readiness semantics (incl. `guard_enabled=False`
passthrough) MUST NOT change; their existing tests MUST pass unmodified.

#### Scenario: guard_enabled=False remains a passthrough
- GIVEN `guard_enabled=False`
- WHEN `filter_finished_streams` runs
- THEN it returns input unchanged with no Data API call

### Requirement: Tests mock the API and assert call behavior

Every `TestFilterPlenarySessionVideos` test MUST set `YOUTUBE_API_KEY` and
mock `build`, per the `TestFilterFinishedStreams` idiom. At least one test
MUST assert `videos.list` is called once with matched ids joined by `","`;
at least one MUST assert it is not called when there are no title matches.

#### Scenario: Test asserts call shape and no-match skip
- GIVEN a mocked `build` and title-matched ids `["A", "B"]`
- WHEN the test invokes the function
- THEN it asserts one call with `id="A,B"`; a separate no-title-match test
  asserts zero calls
