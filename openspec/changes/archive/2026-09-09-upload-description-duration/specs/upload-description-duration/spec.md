# Upload Description Duration Specification

## Purpose

Defines how the published YouTube description's duration line is derived for a long-form
video row passed to `generate_youtube_metadata_for_selected_videos`. Turn rows (the only live
long-form upload path, issue #171) carry no `duration_minutes` field; this capability derives a
real duration from fields the row already carries, instead of silently defaulting to zero.

## Requirements

### Requirement: Turn row duration derivation from grouped span

For a video row where `"turn_id" in video`, the system MUST derive `published_seconds` as
`group_end_seconds - group_start_seconds - procedural_seconds` when `group_start_seconds`,
`group_end_seconds`, and `procedural_seconds` are all present and not `None`. This MUST be the
same formula the `uploadable_turns` view uses for its 300-second eligibility gate, so the
published duration never disagrees with the gate that admitted the clip.

#### Scenario: Complete group fields produce the eligibility-gate duration

- GIVEN a turn row with `group_start_seconds=100`, `group_end_seconds=700`,
  `procedural_seconds=0`
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `published_seconds` is `600`
- AND the resulting description contains `⏱️ Duración: 10 minutos`

### Requirement: Fallback to individual turn span

For a video row where `"turn_id" in video`, if any of `group_start_seconds`,
`group_end_seconds`, or `procedural_seconds` is missing or `None`, the system MUST fall back to
`published_seconds = end_seconds - start_seconds` when both `start_seconds` and `end_seconds`
are present and not `None`.

#### Scenario: Missing group fields fall back to the individual span

- GIVEN a turn row with no `group_start_seconds`, no `group_end_seconds`, no
  `procedural_seconds`, and `start_seconds=200`, `end_seconds=800`
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `published_seconds` is `600`
- AND the resulting description contains `⏱️ Duración: 10 minutos`

### Requirement: Non-derivable duration omits the line

For a video row where `"turn_id" in video`, if `published_seconds` cannot be derived (both the
group formula and the individual-span fallback are unavailable due to missing/`None` fields), or
the derived `published_seconds` is `<= 0`, the system MUST set `duration_seconds = 0` and
`duration_estimated = "N/A"`. The existing `duration != "N/A"` guard in
`generate_youtube_description` MUST then omit the `⏱️ Duración:` line entirely. The system MUST
NOT publish a duration line that reads `0 minutos`.

#### Scenario: All duration fields missing omits the line

- GIVEN a turn row with no `group_start_seconds`, no `group_end_seconds`, no
  `procedural_seconds`, no `start_seconds`, no `end_seconds`
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `duration_estimated` is `"N/A"`
- AND the resulting description contains no `⏱️ Duración:` line

#### Scenario: Non-positive derivable span omits the line

- GIVEN a turn row with `group_start_seconds=500`, `group_end_seconds=500`,
  `procedural_seconds=0` (derived span `0`)
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `published_seconds` is `0`
- AND `duration_estimated` is `"N/A"`
- AND the resulting description contains no `⏱️ Duración:` line

### Requirement: Minute rounding never renders zero

When `published_seconds` is derivable and `> 0`, the system MUST compute
`duration_estimated` as `f"{max(1, round(published_seconds / 60))} minutos"`, rounding to the
nearest whole minute (half away from zero) and clamping the result to a minimum of `1`. A real,
derivable clip span MUST NOT ever render `⏱️ Duración: 0 minutos`.

#### Scenario: Sub-60-second derivable span still renders at least one minute

- GIVEN a turn row with `group_start_seconds=0`, `group_end_seconds=45`,
  `procedural_seconds=0` (derived span `45` seconds)
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `published_seconds` is `45`
- AND the resulting description contains `⏱️ Duración: 1 minutos`
- AND the resulting description never contains `0 minutos`

### Requirement: Chapter row duration behaviour is unchanged

For a video row where `"turn_id" in video` is `False` (a chapter-shaped row), the system MUST
continue to compute `duration_minutes = video.get("duration_minutes", 0)` exactly as before this
change. This capability MUST NOT alter the chapter branch's formatting, derivation, or guard
behaviour; the deliberate asymmetry between the turn and chapter branches is out of scope for
unification here.

#### Scenario: Chapter row keeps its existing duration_minutes read

- GIVEN a chapter-shaped video row with `duration_minutes=10` and no `turn_id` key
- WHEN `generate_youtube_metadata_for_selected_videos` builds metadata for that row
- THEN `duration_minutes` is read via `video.get("duration_minutes", 0)` unchanged
- AND the resulting description contains `⏱️ Duración: 10 minutos`
