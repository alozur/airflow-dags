# Thumbnail Text Regeneration Specification

## Purpose

When the final-copy verifier flags a `thumbnail_text` finding on the long-form
upload path, the system attempts one bounded, non-blocking regeneration of the
thumbnail before publication, and never allows that finding — or the
regeneration itself — to prevent or indefinitely delay publishing.

## Requirements

### Requirement: Regeneration Is Triggered Only By A Long-Form Thumbnail-Text Finding

The system MUST trigger a regeneration attempt through
`generic_thumbnail_generator` when, and only when, `verify_final_copy`'s
findings for the long-form upload path (`t6b`, before `t7` publishes) include
an entry with `field == "thumbnail_text"`. A verdict with no such finding
MUST NOT trigger any regeneration.

#### Scenario: A thumbnail-text finding triggers a regeneration attempt

- GIVEN a long-form turn's verification returns findings including
  `field == "thumbnail_text"`
- WHEN `t6b` evaluates the verdict
- THEN a regeneration attempt is triggered before `t7` publishes

#### Scenario: No thumbnail-text finding, no regeneration

- GIVEN a long-form turn's verification returns no `thumbnail_text` finding
- WHEN `t6b` evaluates the verdict
- THEN no regeneration attempt is triggered

### Requirement: Attempts Are Claimed Against A Bounded Per-Video Spend Ceiling

Because no Pikzels/OpenAI throttle exists elsewhere in the codebase, the
attempt counter MUST act as the only spend ceiling, not merely a loop guard.
An attempt MUST be claimed atomically, per video, before regeneration is
triggered. Once the per-video attempt budget is exhausted, further
`thumbnail_text` findings for that same video MUST NOT trigger additional
attempts. Re-running the same step for a video that already claimed or
completed an attempt MUST NOT increase the recorded attempt count or trigger
a duplicate regeneration.

#### Scenario: Exhausted budget refuses a further attempt

- GIVEN a video has already exhausted its regeneration attempt budget
- WHEN a further `thumbnail_text` finding occurs for that same video
- THEN no additional regeneration attempt is claimed or triggered

#### Scenario: Retrying the step does not accumulate attempts

- GIVEN a video's regeneration attempt has already been claimed
- WHEN the upload task is re-run for the same video
- THEN the attempt count does not increase and no duplicate regeneration runs

### Requirement: The Wait Is Bounded And Its Timeout Path Publishes As-Is

The regeneration wait MUST be bounded by an explicit time budget. On timeout,
on any regeneration failure, or when `generic_thumbnail_generator` is
unavailable, the task MUST proceed to publish using the thumbnail that
existed before the attempt, and MUST record that the regeneration did not
land. This is a routinely-exercised path, not an edge case: measured
production regenerations reach up to 3989s, which exceeds bounds tolerated by
comparable post-publication polling elsewhere in the codebase.

#### Scenario: Regeneration completes within the bound

- GIVEN a triggered regeneration completes before the wait bound elapses
- WHEN the poll observes completion
- THEN the newly generated thumbnail is used for publication

#### Scenario: Regeneration exceeds the bound

- GIVEN a triggered regeneration has not completed when the wait bound
  elapses
- WHEN the bound is reached
- THEN the task publishes with the pre-attempt thumbnail and records that the
  regeneration did not land

### Requirement: No Code Path May Block Or Indefinitely Delay Publication

A `thumbnail_text` finding, a claimed attempt, a timeout, or a regeneration
failure MUST NOT raise, block, or indefinitely delay publication under any
circumstance. This MUST hold by construction, independent of finding
frequency. The only condition that raises to block a long-form publication is
the pre-existing title hard-rejection asymmetry, which this capability MUST
NOT alter.

#### Scenario: Regeneration failure never raises

- GIVEN a regeneration attempt fails for any reason
- WHEN the upload task evaluates the outcome
- THEN the task proceeds to publish with the existing thumbnail and no
  exception propagates from the regeneration path

#### Scenario: Title hard-rejection remains the only blocking path

- GIVEN a verdict carries both a `thumbnail_text` finding and a `reject` on
  `title`
- WHEN the upload task evaluates the verdict
- THEN publication is aborted by the existing title hard-rejection path, not
  by anything in the regeneration path

### Requirement: Both The Prior And Regenerated Briefs Are Retained For Audit

Because `video_thumbnails` is upserted destructively via
`ON CONFLICT ... DO UPDATE` on `(chapter_id, label)`, the system MUST
snapshot the prior brief before triggering a regeneration. After the attempt
concludes, both the prior brief and any regenerated brief MUST remain
retrievable for audit, regardless of whether the regeneration lands.

#### Scenario: Prior brief is snapshotted before triggering

- GIVEN a regeneration attempt is about to be triggered
- WHEN the attempt is claimed
- THEN the existing brief is snapshotted before the trigger call is made

#### Scenario: Both briefs remain retrievable after a landed regeneration

- GIVEN a regeneration attempt completes and lands within the bound
- WHEN the outcome is recorded
- THEN both the prior brief and the regenerated brief are retrievable for
  audit

### Requirement: Regeneration Effect Is Scoped To The Triggering Turn Only

`video_thumbnails` rows keyed `(chapter_id, label)` are shared across every
sibling turn of a chapter. A regeneration triggered by one turn's
verification MUST change only that turn's own canonical `thumbnail.png` used
for its own publication. The shared `video_thumbnails` write MUST be
treated as best-effort/audit-only and MUST NOT be authoritative for what any
sibling turn publishes.

#### Scenario: A sibling turn is unaffected by another turn's regeneration

- GIVEN two sibling turns share the same `chapter_id`
- WHEN one turn's verification triggers a regeneration that lands
- THEN the sibling turn still publishes its own unchanged canonical
  thumbnail, independent of the shared `video_thumbnails` row

### Requirement: Scope Is Long-Form Only

This capability applies only to the long-form upload path
(`youtube_upload_dag.py`). The shorts path (`reap_shorts_uploader_dag.py`)
does not pass `thumbnail_text` to the verifier and `video_shorts` carries no
such column; this capability MUST NOT extend regeneration to shorts.

#### Scenario: Shorts verification never triggers regeneration

- GIVEN a short's copy verification runs
- WHEN its verdict is evaluated
- THEN no regeneration attempt is triggered, because no `thumbnail_text`
  finding can exist on the shorts path
