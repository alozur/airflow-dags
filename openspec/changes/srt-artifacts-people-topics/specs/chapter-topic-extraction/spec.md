# Chapter Topic Extraction Specification

## Purpose

Derive normalized subject-matter topics for a chapter from a dedicated LLM
call, independent of mentioned-people resolution and of speaker resolution,
and persist them with stable, deduplicated ordering.

## Requirements

### Requirement: Dedicated LLM call independent of mentioned-people

The system MUST extract topics via a second, dedicated LLM call — distinct
prompt and cache key from `resolve_mentioned_people` — using the chapter's
persisted SRT text as input. The two analyses MUST run and fail
independently.

#### Scenario: Call invoked with its own prompt and cache key

- GIVEN a chapter with a persisted `subtitles.srt` sidecar
- WHEN `extract_topics` runs at upload time
- THEN a completion call is made with a prompt/cache key distinct from
  `resolve_mentioned_people`'s call

### Requirement: Normalized, deduplicated, capped output

Extracted topics MUST be lowercased, trimmed, deduplicated preserving
first-seen order, and truncated to a documented maximum count.

#### Scenario: Mixed-case and duplicate topics normalized

- GIVEN the model returns `["Sanidad", "sanidad ", "Educación"]`
- WHEN normalization runs
- THEN the result is `["sanidad", "educación"]` in first-seen order

#### Scenario: Topic count exceeds the documented cap

- GIVEN the model returns more topics than the documented maximum
- WHEN normalization runs
- THEN the result is truncated to the cap, preserving first-seen order

### Requirement: Persisted per cardinality with stable ordering

Results MUST be persisted to `video_chapters.topics`, covering zero and
multiple topics, replacing the prior chapter-identification-call value as
the source of truth for that column.

#### Scenario: No topics extracted

- GIVEN the model returns no topics, or extraction fails or is malformed
- WHEN persistence runs
- THEN `topics` is left untouched (the pre-existing value is preserved); this
  path never writes `NULL` and never overwrites a non-empty value with an
  empty one

#### Scenario: Multiple topics extracted

- GIVEN the model returns several distinct topics
- WHEN persistence runs
- THEN `topics` stores the normalized, deduplicated, ordered array

### Requirement: Never raise on malformed output

On malformed/unparseable model output or any internal exception, extraction
MUST degrade to an empty result and MUST NOT raise.

#### Scenario: Malformed model response

- GIVEN the completion call returns unparseable JSON or an error
- WHEN `extract_topics` runs
- THEN it returns an empty result and no exception propagates

### Requirement: Upload-time hook on the shared preparation path

`extract_topics` MUST be invoked in `youtube_upload_dag.py` on the shared
path of `_prepare_thumbnail_config`, after the SRT blocks are parsed and
outside the turn-only branch, beside `resolve_mentioned_people`,
independently try/excepted so its failure MUST NOT discard or block the
mentioned-people result. The chapter bounds MUST come from a chapter lookup
(`db.get_chapter_srt_context(chapter_id)`), never from the turn row, because
`uploadable_turns` does not expose `start_time`/`end_time`.

#### Scenario: Topic extraction fails while other analyses succeed

- GIVEN `extract_topics` raises or errors
- WHEN the upload-time hook runs both analyses
- THEN `mentioned_participant_slugs` still persists and `topics` is left
  untouched

### Requirement: Cacheable per content revision

The call MUST use `cached_json_completion` so repeated extraction for
unchanged chapter SRT text is served from cache, independent of the
mentioned-people cache key.

#### Scenario: Repeated call is a cache hit

- GIVEN the same chapter SRT text as a prior successful call
- WHEN `extract_topics` runs again
- THEN the cached result is returned without a new model call

### Requirement: Documented column source-of-truth and metric distinction

`video_chapters.topics`'s `COMMENT ON COLUMN` MUST document the upload-time
write as the revised source of truth (previously a chapter-identification
by-product), and docs MUST preserve the three-way distinction between
speaker (`resolved_participant_slug`), mentioned people
(`mentioned_participant_slugs`), and topics (`topics`).

#### Scenario: Column comment documents the source-of-truth change

- GIVEN migration `045` adds the re-documenting `COMMENT ON COLUMN`
- WHEN a developer inspects `production.video_chapters.topics`
- THEN the comment states topics are refreshed at upload time, distinct from
  speaker and mentioned-people columns
