# Delta for Short-Video SRT Artifacts

## MODIFIED Requirements

### Requirement: Short SRT window derivation with fallback

For a chapter-sourced clip (`video_shorts.turn_id IS NULL`), the system MUST
derive the short's window as `[chapter_start + pretrim_start_secs,
chapter_start + pretrim_end_secs]` when both `pretrim_start_secs` and
`pretrim_end_secs` are present, falling back to the full chapter `[start_time,
end_time]` span when either is `NULL`.

For a turn-sourced clip (`video_shorts.turn_id IS NOT NULL`), the system
MUST NOT apply the chapter-relative `pretrim_start_secs`/`pretrim_end_secs`
formula, because those offsets are file-relative on a turn's `output_path`
media, not chapter-relative. It MUST instead fall back to the full chapter
`[start_time, end_time]` span, regardless of whether `pretrim_start_secs`/
`pretrim_end_secs` are present.

This approximation (Reap exposes no per-clip timing) MUST be documented in
the module docstring and pipeline docs.
(Previously: the chapter-relative pretrim formula applied unconditionally
whenever both offsets were present, with no turn-sourced guard.)

#### Scenario: Pre-trim offsets present, chapter-sourced clip

- GIVEN `turn_id IS NULL`, `pretrim_start_secs=30`, `pretrim_end_secs=90`
- WHEN the short sidecar is written
- THEN the SRT covers `[chapter_start+30, chapter_start+90]` only

#### Scenario: Pre-trim offsets absent, chapter-sourced clip

- GIVEN `turn_id IS NULL`, `pretrim_start_secs` and `pretrim_end_secs` both
  `NULL`
- WHEN the short sidecar is written
- THEN the SRT covers the full chapter span

#### Scenario: Turn-sourced clip ignores chapter-relative pretrim offsets

- GIVEN `turn_id IS NOT NULL` and `pretrim_start_secs=30`,
  `pretrim_end_secs=90` are present
- WHEN the short sidecar is written
- THEN the SRT covers the full chapter span, not
  `[chapter_start+30, chapter_start+90]`

#### Scenario: Turn-sourced clip with no pretrim offsets

- GIVEN `turn_id IS NOT NULL` and both pretrim offsets are `NULL`
- WHEN the short sidecar is written
- THEN the SRT covers the full chapter span
