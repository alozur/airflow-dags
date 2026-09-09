# Title Generation Provenance Specification

## Purpose

Persist each live title generator's input and resulting title on the entity's own row at generation
time, so a run is replayable and auditable without live-worktree, reaped-clip, or sibling-window
re-reads.

## Requirements

### Requirement 1: Turn-path payload persistence

`speaker_turn_videos.title_generation_input` MUST store `generate_title`'s input (`summary`, `best`
as `{label, style, prompt}`, `sibling_titles`, `key_speakers`, `forbidden_title`,
`participant_slug`) plus the generated title, whenever a turn title is generated.

#### Scenario 1.1: Title generated for a turn

- GIVEN a turn run reaches `_task_generate_title` with all six inputs present
- WHEN `generate_title` returns a title
- THEN the payload holds all six inputs plus the title, written when the parent receives the
  thumbnail result — before and independent of the later upload outcome

### Requirement 2: Shorts-path payload persistence

`video_shorts.title_generation_input` MUST store the shorts metadata generator's input
(`transcript[:2000]`, `transcript_truncated`, `transcript_full_length`, `chapter_title`,
`primary_speaker`, `secondary_speakers`, `topics`, `scoring_reasoning[:500]`,
`mentioned_display_names`) plus the accepted title, whenever a shorts title is LLM-generated.

#### Scenario 2.1: Shorts LLM generation

- GIVEN `_generate_metadata` builds a prompt from a non-empty transcript
- WHEN the LLM returns a title
- THEN the payload holds the exact `transcript[:2000]` slice, truncation flag, full length, and title

#### Scenario 2.2: Fallback branch persists nothing

- GIVEN the transcript is empty and the LLM branch is skipped
- WHEN metadata falls back to the non-LLM path
- THEN no write occurs and the column stays NULL

### Requirement 3: Joinable, collision-free keying with an unambiguous zero-row signal

The turn write MUST key by `output_path` so every sibling row of a grouped publish gets the same
payload without touching other rows; the shorts write MUST key by `video_shorts.id`.

The turn `UPDATE` MUST NOT carry a content guard (no `IS DISTINCT FROM` predicate). This deliberately
diverges from `record_copy_verification_turn`: without a content guard, `rowcount == 0` has exactly
one meaning — **the key matched no row** — which is the loud signal that the write was keyed on the
wrong path (for example the child thumbnail DAG's `thumbnail.png` instead of the turn's `video.mp4`).
A zero-row result therefore MUST NOT be reported as success; it MUST be recorded and logged as a
distinct `no_row` outcome. A re-run with the same key overwrites the row and reports `rowcount >= 1`,
which is the intended behaviour.

#### Scenario 3.1: Grouped-turn write touches only its own siblings

- GIVEN a grouped publish inserts N rows with distinct `turn_id`s sharing one `output_path`
- WHEN the turn write executes once, keyed by that `output_path`
- THEN all N rows carry the identical, non-NULL payload, and rows of other `output_path`s are untouched

#### Scenario 3.2: Re-run overwrites, and a non-matching key is loud

- GIVEN a row's payload was already written for a given `output_path`
- WHEN the write runs again with the same key
- THEN the row is overwritten, `rowcount >= 1` is returned, and no exception is raised
- AND GIVEN a key that matches no row (such as a thumbnail path instead of the video path)
- WHEN the write executes
- THEN `rowcount == 0` is returned and the call site records and logs a `no_row` outcome rather than
  treating it as success

### Requirement 4: Replay without live-state dependency

A stored payload MUST carry every field its generator needs to reproduce the run from stored data
alone — no live-worktree read, no clip re-transcription, no sibling-window re-query.

#### Scenario 4.1: Turn and shorts payload round-trip

- GIVEN a persisted turn payload and a persisted shorts payload
- WHEN each is read back and passed to its own generator
- THEN each call succeeds using only the stored fields, with no extra DB query or re-transcription

### Requirement 5: Failure isolation

A persistence failure at either write site MUST be caught and logged at the call site, never raised
past it or allowed to block or crash publication.

#### Scenario 5.1: Forced DB failure during turn write

- GIVEN the turn write raises an exception
- WHEN the upload DAG's call-site wrapper executes
- THEN the exception is caught and logged, and the upload task still completes successfully

#### Scenario 5.2: Forced DB failure during shorts write

- GIVEN the shorts write raises an exception
- WHEN `_generate_metadata`'s call-site wrapper executes
- THEN the exception is caught and logged, and metadata assembly still completes

### Requirement 6: No credentials in the persisted payload

Serialized payloads MUST contain no credentials, access tokens, or private environment values, and no
key outside the declared schema.

#### Scenario 6.1: Schema-only, credential-free serialization

- GIVEN a fully assembled turn or shorts payload
- WHEN it is serialized to jsonb for the write
- THEN every key belongs to the declared schema, with no URL, filesystem path, token, or credential

### Requirement 7: Existing title safeguards remain intact

The empty-title guard, correct speaker attribution, and the #512 final-copy verification seam MUST
keep working unmodified.

#### Scenario 7.1: Empty-title guard still raises

- GIVEN `thumbnail_result["title"]` is missing or blank
- WHEN `_prepare_upload_config` processes the turn
- THEN it still raises `ValueError`, unaffected by the new write

#### Scenario 7.2: #512 verification seam still runs

- GIVEN a turn or short completes generation and publish
- WHEN the pipeline proceeds
- THEN `record_copy_verification_turn` / `_short` is still invoked unchanged
