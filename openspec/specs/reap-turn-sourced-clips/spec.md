# Reap Turn-Sourced Clips Specification

## Purpose

Reap short-clip generation and publication source from diarized, already
materialized speaker-turn video files (`speaker_turn_videos.output_path`)
instead of raw long-form chapters, decoupling clip throughput from the
long-form YouTube upload cadence.

## Requirements

### Requirement: `video_shorts.turn_id` reference

`video_shorts` MUST have a nullable `turn_id INTEGER REFERENCES
speaker_turn_videos(turn_id) ON DELETE SET NULL` column with a supporting
index. `chapter_id` MUST remain `NOT NULL` and populated on every new row.

#### Scenario: Referenced turn video deleted

- GIVEN a row references `turn_id`
- WHEN the referenced `speaker_turn_videos` row is deleted
- THEN `turn_id` becomes `NULL` and the `video_shorts` row is not deleted

### Requirement: Turn-video candidate selection

The system MUST compute each `output_path` group's span as
`MIN(start_seconds)`/`MAX(end_seconds)` over ALL its turns (unfiltered by
procedural status), pick one representative row per group via `DISTINCT ON
(output_path) ORDER BY output_path, turn_id`, and order candidates by
`COALESCE(interest_score,1) DESC, relevance_score DESC, session_date DESC,
turn_id ASC`.

| Gate | Excludes candidate when |
|---|---|
| Source | `output_path IS NULL` |
| Procedural | representative `is_procedural = TRUE` |
| Floor | effective span (group span minus the summed procedural seconds of its turns, mirroring `uploadable_turns`) `< 120` seconds |
| Dedup | a `video_shorts` row already has that `turn_id` |

Span `> 900`s, `prepared_at IS NULL`, any relevance threshold, and a missing
parent YouTube upload date MUST NOT exclude a candidate.

#### Scenario: Eligible group selected

- GIVEN a 300s group with a non-procedural representative and no existing
  `video_shorts.turn_id` match
- WHEN selection runs
- THEN the representative `turn_id` is returned

#### Scenario: Span under floor excluded

- GIVEN a group spans 90 seconds
- WHEN selection runs
- THEN it is excluded

#### Scenario: Over-ceiling group stays eligible

- GIVEN a group spans 950 seconds
- WHEN selection runs
- THEN it is still returned

#### Scenario: Unpublished parent still eligible

- GIVEN the candidate's chapter has no YouTube upload date
- WHEN selection runs
- THEN it is not excluded on that basis

### Requirement: Preparer consumes materialized turn output directly

The preparer MUST stage each candidate's `output_path` file directly and
MUST NOT look up a raw source video or re-cut a chapter. When ffprobe
duration exceeds `pre_trim_threshold_secs` (default 900), it MUST pre-trim
using file-relative offsets; otherwise it MUST stage the full file. Every
inserted row MUST carry both `chapter_id` and `turn_id`.

#### Scenario: Under threshold used unmodified

- GIVEN ffprobe reports 600 seconds
- WHEN the preparer runs
- THEN no pre-trim occurs; the full file is staged

#### Scenario: Over threshold is pre-trimmed

- GIVEN ffprobe reports 950 seconds
- WHEN the preparer runs
- THEN it pre-trims on file-relative timestamps before staging

#### Scenario: Inserted row carries both keys

- GIVEN a candidate is prepared successfully
- WHEN `insert_video_short` runs
- THEN the row has both `chapter_id` and `turn_id` set

### Requirement: Zero-eligible run is logged

When selection returns zero eligible groups, the preparer MUST log a
WARNING naming the count before short-circuiting.

#### Scenario: No eligible candidates

- GIVEN selection returns an empty list
- WHEN the gate task runs
- THEN it logs a WARNING citing zero eligible candidates and still
  short-circuits

### Requirement: Claim ordering with legacy compatibility

`claim_pending_clip` MUST keep ordering by `session_date DESC NULLS LAST,
relevance_score DESC NULLS LAST` via a `LEFT JOIN` path that resolves
whether a row has `turn_id` or only `chapter_id`.

#### Scenario: Legacy chapter-only row still claimable

- GIVEN a pending row has `turn_id NULL`, `chapter_id` set
- WHEN `claim_pending_clip` runs
- THEN the row is still claimable in priority order

### Requirement: Tier-1 partitioning by turn with legacy fallback

`pending_shorts_candidate_sql` MUST partition Tier-1 ranking by `turn_id`
when present, falling back to `chapter_id` when `turn_id IS NULL`. The
ranking universe MUST stay unfiltered by `is_uploaded`, `local_file_path`,
or virality (issue #262). The outer query MUST NOT require a parent
YouTube upload date, and its ordering MUST stay NULL-safe (`NULLS LAST`)
now that a missing parent upload date is expected, not exceptional.

#### Scenario: Turn-keyed rows partition by turn_id

- GIVEN three downloaded rows share one `turn_id`
- WHEN the candidate query runs
- THEN they rank together and cap at the Tier-1 limit as one group

#### Scenario: Legacy rows partition by chapter_id

- GIVEN a row has `turn_id NULL`, `chapter_id` set
- WHEN the candidate query runs
- THEN it ranks within its `chapter_id` partition only

#### Scenario: Uploaded rows still consume ranking slots

- GIVEN a partition has one uploaded row and two pending rows
- WHEN the candidate query runs
- THEN the uploaded row still occupies a rank position

#### Scenario: Unpublished parent no longer blocks upload

- GIVEN a candidate's parent has no YouTube upload date
- WHEN `get_pending_shorts` runs
- THEN it is still returned as eligible

### Requirement: Chapter-only candidate selection is removed

`get_chapters_for_shorts` MUST be removed with no successor caller; all
Reap generation eligibility flows through turn-video candidate selection.

#### Scenario: No remaining caller

- GIVEN the codebase after this change
- WHEN searching for callers of `get_chapters_for_shorts`
- THEN none exist; the gate task calls turn-video selection instead
