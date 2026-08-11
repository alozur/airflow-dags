# YouTube Video Metadata Specification

## Purpose

Ensure that per-video YouTube snippet metadata is authoritative after test-mode video selection, preventing test-mode placeholders from reaching downstream consumers as video metadata.

## Requirements

### Requirement: API-supplied video metadata is authoritative

The system MUST return the per-video API response's `snippet.title` as the video `title` and a non-empty `snippet.publishedAt` as the video `published_at` when those values are supplied for the selected video.

#### Scenario: Test-mode placeholder metadata is enriched from the per-video response

- GIVEN a video record produced by test mode with placeholder `title` and `published_at` values
- AND a per-video API response for the selected video supplies a title and a non-empty publication time distinct from those placeholders
- WHEN video details are retrieved
- THEN the returned video `title` MUST equal the API-supplied `snippet.title`
- AND the returned video `published_at` MUST equal the API-supplied `snippet.publishedAt`

### Requirement: Missing API publication time preserves existing metadata

The system MUST retain the selected video's existing `published_at` value when the per-video API response omits `snippet.publishedAt` or provides it as an empty value.

#### Scenario: Publication time is absent from the per-video response

- GIVEN a selected video with an existing `published_at` value
- AND its per-video API response has no `snippet.publishedAt`
- WHEN video details are retrieved
- THEN the returned video `published_at` MUST equal its existing value

#### Scenario: Publication time is empty in the per-video response

- GIVEN a selected video with an existing `published_at` value
- AND its per-video API response provides an empty `snippet.publishedAt`
- WHEN video details are retrieved
- THEN the returned video `published_at` MUST equal its existing value

### Requirement: Test mode is limited to video selection

Test mode MUST affect selection of the video to retrieve and MUST NOT make test-mode placeholder metadata authoritative over supplied per-video API snippet metadata.

#### Scenario: Downstream consumers receive enriched metadata without persistence changes

- GIVEN test mode selects a video
- AND the selected video's per-video API response supplies snippet metadata
- WHEN video details are retrieved for downstream processing
- THEN downstream processing MUST receive the enriched video metadata
- AND no database-specific behavior, schema, migration, or API request contract change SHALL be required
