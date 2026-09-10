# Session Intro-Card Overlay Specification

## Purpose

Every long-form video published by `youtube_upload_dag` MUST carry a
5-second on-screen session-identification card burned in before upload,
produced in-process on the mandatory daily upload path, never silently
skipped and never blocking indefinitely.

## Requirements

### Requirement: Intro Card Style Definition

The system MUST define an `intro_sesion` tipo in the `congreso` domain of
`video_editor_config.py`, style attributes only (fonts, colors, layout),
matching the schema of the domain's other tipos.

#### Scenario: intro_sesion matches domain schema

- GIVEN the `congreso` tipo registry
- WHEN `intro_sesion` is loaded
- THEN it declares the same style keys as the other tipos and no per-call
  timing state

### Requirement: Intro Card Renderer Registration

A Pillow renderer for `intro_sesion` MUST be registered in
`_PILLOW_RENDERERS`; a config entry without a registered renderer MUST
fail loudly, never silently skip.

#### Scenario: Unregistered tipo fails loudly

- GIVEN `intro_sesion` exists in config but no renderer is registered
- WHEN `apply_overlays` processes it
- THEN the call raises an explicit error, not a partial or missing overlay

#### Scenario: Registered renderer produces the card

- GIVEN `intro_sesion` is defined and registered
- WHEN `apply_overlays` processes it
- THEN the card is composited onto the source within its time window

### Requirement: Pure Overlap-Resolution Helper

A pure function MUST accept a desired `(start, end)` window and the other
windows already placed in the same call, and return an adjusted window:
unchanged if free, otherwise shifted forward only (never before 0, never
backward) to the earliest free slot, preserving the requested duration.

#### Scenario: No overlap leaves the window unchanged

- GIVEN a desired window that overlaps nothing
- WHEN resolved
- THEN it is returned unchanged

#### Scenario: Full overlap shifts past the conflict

- GIVEN a desired window fully inside an existing window
- WHEN resolved
- THEN it starts at the existing window's end, same duration

#### Scenario: Partial overlap shifts to the earliest free slot

- GIVEN a desired window partially overlapping one existing window
- WHEN resolved
- THEN it starts at the earliest non-overlapping point at or after the
  original start, same duration

#### Scenario: Single overlay is a no-op

- GIVEN no other windows are placed yet
- WHEN resolved
- THEN the desired window is returned unchanged

### Requirement: Default Intro Window

The intro card MUST occupy `[0, 5)` seconds by default, defined as one
named, configurable constant.

#### Scenario: Default window applies without override

- GIVEN a caller supplies no custom window
- WHEN the intro overlay is built
- THEN it occupies exactly `[0, 5)` seconds, read from the named constant

### Requirement: Card Text From Session Metadata

Card text MUST be built by the caller from `session_number` and
`session_date` on the `uploadable_item` XCom, in Spanish, matching the
`titulo`/`descripcion` convention.

#### Scenario: Card text reflects session metadata

- GIVEN an `uploadable_item` with `session_number` and `session_date`
- WHEN the new task builds the overlay
- THEN the rendered text derives from those two fields, in Spanish

### Requirement: In-Process Task Overwrites Output Path In-Memory Only

A new task MUST run between `t5 extract_chapter_videos` and
`t6 prepare_upload_config`, calling `apply_overlays()` in-process and
overwriting `output_path` on the `chapter_extraction_results` XCom for
the current run, requiring no code change in `t6`.

#### Scenario: t6 consumes the overlaid video transparently

- GIVEN the task updated `chapter_extraction_results.output_path` for
  this run
- WHEN `t6` reads `output_path`
- THEN it resolves to the overlaid file with no change to `t6`'s code

### Requirement: Database Output Path Never Written Back

The `_edited` overlaid path MUST NEVER be written to
`speaker_turn_videos.output_path` in the database. It exists only as a
sibling file on disk plus the current run's in-memory XCom value.

#### Scenario: DB output_path is unchanged after the task runs

- GIVEN a turn video was overlaid and uploaded
- WHEN `speaker_turn_videos.output_path` is read afterward
- THEN it still points at the original, unedited source path

### Requirement: Source File Immutability

The source video file MUST NOT be mutated in place at any step of
overlay production.

#### Scenario: Source bytes are unchanged after overlay

- GIVEN a source video file before the task runs
- WHEN the task produces the overlaid output
- THEN the source file's bytes and path are unchanged

### Requirement: Edited File Is a Same-Directory Sibling

The `_edited` file MUST be written to the same directory as the source,
so `title.txt`, `description.txt`, `thumbnail.png` and `subtitles.srt`
still resolve for `prepare_orador_upload_config`.

#### Scenario: Sidecars resolve after the overlay task

- GIVEN the 4 sidecars exist alongside the source video
- WHEN the task writes `_edited` into that same directory
- THEN `t6`'s sidecar resolution succeeds unchanged

### Requirement: Idempotent Retry

Retrying MUST be idempotent: the `_edited` path MUST be deterministic per
source, and a retry MUST overwrite that path rather than accumulate
files.

#### Scenario: Retry overwrites the same deterministic path

- GIVEN the task already produced `_edited` for a source video
- WHEN the task is retried in the same run
- THEN it writes the same path, leaving exactly one edited file

### Requirement: Bounded, Fail-Loud Failure

The overlay step MUST run under a bounded time budget. If it cannot
finish within budget, or ffmpeg/rendering fails, the task MUST fail
explicitly and diagnosably. The feature MUST NEVER silently drop, skip,
or publish an un-overlaid video in place of a failed overlay.

#### Scenario: Overlay failure aborts publication

- GIVEN `apply_overlays` raises while producing the card
- WHEN the task evaluates the result
- THEN the task fails and `t6`/`t7` do not run for that video

#### Scenario: Excessive source duration fails explicitly

- GIVEN a source whose length would exceed the overlay step's time budget
- WHEN the task attempts the overlay
- THEN it fails with a diagnosable error before publishing anything,
  rather than hanging or publishing the un-overlaid source
