# Short-Video SRT Artifacts Specification

## Purpose

Every persisted chapter and every downloaded Reap short clip gets an SRT
subtitle sidecar beside its media, in a predictable canonical directory, so
downstream consumers can locate captions without re-deriving timing.

## Requirements

### Requirement: Chapter SRT sidecar in the canonical chapter directory

The system MUST write `subtitles.srt` under `get_video_chapter_dir(video_id,
chapter_id, channel_slug)`, windowed to the chapter span padded by
`CHAPTER_SRT_PAD_SECS` on both edges, with absolute (not re-timed)
timestamps. This path and behavior (issue #340) MUST remain the
verified source of truth for chapter subtitles.

#### Scenario: Chapter sidecar persisted at canonical path

- GIVEN a chapter with a resolvable source SRT and a valid `[start_time,
  end_time]` span
- WHEN the sidecar writer runs
- THEN `subtitles.srt` exists at `get_video_chapter_dir(...)/subtitles.srt`
  with absolute timestamps

### Requirement: Short clip SRT sidecar in the canonical shorts directory

The system MUST write `{clip_id}.srt` beside `{clip_id}.mp4` under
`get_chapter_shorts_dir(source_video_id, chapter_id, channel_slug)` for every
clip downloaded by `ReapJobSensor.poke`. The write MUST be best-effort: any
failure MUST be caught, logged, and MUST NOT raise or fail the sensor's
`poke`. Short-sidecar timestamps MUST be re-timed to the clip origin (first
block at or after `00:00:00,000`), unlike the chapter sidecar's absolute
timestamps.

#### Scenario: Short sidecar written after clip download

- GIVEN a completed Reap project with a downloaded clip `clip_id`
- WHEN `ReapJobSensor.poke` processes the clip
- THEN `{clip_id}.srt` exists at `get_chapter_shorts_dir(...)/{clip_id}.srt`

#### Scenario: Sidecar failure never fails the sensor

- GIVEN the sidecar write raises an internal error
- WHEN `ReapJobSensor.poke` processes the clip
- THEN `poke` still returns `True` and the clip row is still inserted

### Requirement: Short SRT window derivation with fallback

The system MUST derive the short's window as `[chapter_start +
pretrim_start_secs, chapter_start + pretrim_end_secs]` when both
`video_shorts.pretrim_start_secs` and `pretrim_end_secs` are present. When
either is `NULL`, it MUST fall back to the full chapter `[start_time,
end_time]` span. This approximation (Reap exposes no per-clip timing) MUST
be documented in the module docstring and pipeline docs.

#### Scenario: Pre-trim offsets present

- GIVEN `pretrim_start_secs=30` and `pretrim_end_secs=90` for a clip's chapter
- WHEN the short sidecar is written
- THEN the SRT covers `[chapter_start+30, chapter_start+90]` only

#### Scenario: Pre-trim offsets absent

- GIVEN `pretrim_start_secs` and `pretrim_end_secs` are both `NULL`
- WHEN the short sidecar is written
- THEN the SRT covers the full chapter span

### Requirement: Reuse of an existing non-empty sidecar

The system MUST reuse an existing non-empty `{clip_id}.srt` without
rewriting it, and MUST log an INFO message including the reused path.
Re-running the writer for an already-sidecar'd clip MUST be a no-op on disk.

#### Scenario: Existing non-empty sidecar is reused

- GIVEN `{clip_id}.srt` already exists with content
- WHEN the short sidecar writer runs again for the same `clip_id`
- THEN the file is left unmodified and an INFO log names its path

#### Scenario: Idempotent re-run

- GIVEN a short sidecar was already written for a clip
- WHEN the writer runs a second time
- THEN no additional write, temp file, or rewrite occurs

### Requirement: Explicit failure outcomes, never silent

Missing source subtitle, an unreadable/corrupt source SRT, or a windowed
block count of zero MUST each produce a WARNING log and a `None` return.
None of these cases MUST create or truncate a destination file, and none
MUST raise.

#### Scenario: No source subtitle found

- GIVEN no SRT source resolves for the clip's video
- WHEN the short sidecar writer runs
- THEN it logs WARNING, returns `None`, and writes no file

#### Scenario: Unreadable source SRT

- GIVEN the source SRT exists but cannot be read (I/O error)
- WHEN the short sidecar writer runs
- THEN it logs WARNING, returns `None`, and writes no file

#### Scenario: Zero blocks in the derived window

- GIVEN the derived window overlaps no subtitle blocks
- WHEN the short sidecar writer runs
- THEN it logs WARNING, returns `None`, and writes no file

### Requirement: Distinct canonical destinations per artifact type

Chapter sidecars (`get_video_chapter_dir(...)/subtitles.srt`) and short
sidecars (`get_chapter_shorts_dir(...)/{clip_id}.srt`) MUST resolve to
distinct paths and MUST NOT overwrite each other for the same chapter.

#### Scenario: Chapter and short sidecars coexist

- GIVEN a chapter with both a chapter sidecar and one or more short clips
- WHEN both writers have run
- THEN `subtitles.srt` and every `{clip_id}.srt` exist independently under
  their own canonical directories
