# Chapter Mentioned-People Resolution Specification

## Purpose

Identify which known `congress_participants` are mentioned/discussed within
a chapter's subtitle text — a concept distinct from the chapter's *speaker*
(`resolved_participant_slug`) — and persist that as a roster-validated,
deduplicated slug array.

## Requirements

### Requirement: Dedicated LLM call over the persisted chapter SRT

The system MUST resolve mentioned people via a dedicated LLM call, separate
from speaker resolution and separate from topic extraction, using the
persisted chapter SRT sidecar text and the current `congress_participants`
roster as input.

#### Scenario: Call invoked with chapter SRT and roster

- GIVEN a chapter with a persisted `subtitles.srt` sidecar and a non-empty
  participant roster
- WHEN `resolve_mentioned_people` runs at upload time
- THEN a single dedicated completion call is made with the SRT text and
  roster, using a prompt/cache key distinct from topic extraction

### Requirement: Roster-gated slug validity

Every slug in the result MUST exist in the supplied roster. Unresolved or
ambiguous name mentions MUST be dropped — never invented and never stored as
placeholders — and the raw mention MUST be logged for visibility.

#### Scenario: Unknown name dropped and logged

- GIVEN the model returns a name with no matching roster slug
- WHEN the result is validated against the roster
- THEN the name is dropped from the result and logged (INFO), not persisted

#### Scenario: Ambiguous match dropped and logged

- GIVEN the model returns a mention with a slug that does not uniquely or
  confidently match one roster entry
- WHEN the result is validated
- THEN the mention is dropped and logged, and no slug is invented

### Requirement: Deduplicated slug array persisted per cardinality

Accepted slugs MUST be deduplicated and persisted to
`video_chapters.mentioned_participant_slugs` (`TEXT[]`, migration 045),
covering zero, one, and multiple mentioned people.

#### Scenario: Zero mentioned people

- GIVEN no valid roster matches are found
- WHEN persistence runs
- THEN `mentioned_participant_slugs` is stored as an empty array

#### Scenario: One mentioned person

- GIVEN exactly one valid roster match
- WHEN persistence runs
- THEN `mentioned_participant_slugs` contains that single slug

#### Scenario: Multiple mentioned people, deduplicated

- GIVEN the model mentions the same participant twice under different
  phrasing plus a second distinct participant
- WHEN persistence runs
- THEN `mentioned_participant_slugs` contains each distinct slug exactly once

### Requirement: Distinct from speaker resolution

`mentioned_participant_slugs` MUST be resolved and stored independently of
`resolved_participant_slug`; a participant appearing as speaker MAY also
appear as mentioned, and the two MUST NOT be conflated in code or prompts.

#### Scenario: Speaker and mentioned slugs both persist independently

- GIVEN a chapter whose speaker resolves to slug A and whose mentioned-people
  call resolves slugs A and B
- WHEN both writes complete
- THEN `resolved_participant_slug = A` and `mentioned_participant_slugs =
  [A, B]` (or dedup order), each written by its own code path

### Requirement: Never raise on malformed output

On malformed/unparseable model output or any internal exception, resolution
MUST degrade to an empty result and MUST NOT raise.

#### Scenario: Malformed model response

- GIVEN the completion call returns unparseable JSON or an error
- WHEN `resolve_mentioned_people` runs
- THEN it returns an empty result and no exception propagates

### Requirement: Cacheable per content revision

The call MUST use `cached_json_completion` so repeated resolution for
unchanged chapter SRT text and roster is served from cache.

#### Scenario: Repeated call is a cache hit

- GIVEN the same chapter SRT text and roster as a prior successful call
- WHEN `resolve_mentioned_people` runs again
- THEN the cached result is returned without a new model call

### Requirement: Failure isolation from topic extraction

A failure in mentioned-people resolution MUST NOT prevent topic extraction
from running or persisting, and MUST NOT roll back an already-persisted
topics result.

#### Scenario: Mentioned-people call fails, topics still persist

- GIVEN `resolve_mentioned_people` raises or returns an empty result
- WHEN the upload-time hook runs both analyses
- THEN `topics` is still extracted and persisted independently

### Requirement: Schema migration and drift guard

Migration `045` MUST add `mentioned_participant_slugs TEXT[]` to
`production.video_chapters`, mirrored in `production_schema.sql` with a
column comment, and MUST be covered by the block-scoped schema drift test.

#### Scenario: Schema mirror matches the live migration

- GIVEN migration `045` has been applied
- WHEN the schema drift test runs
- THEN `production_schema.sql`'s `video_chapters` definition matches the
  live table, including `mentioned_participant_slugs`
