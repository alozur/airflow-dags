# Delta for Monologue Speaker Resolution

## ADDED Requirements

### Requirement: Chapter First-Substantive-Turn Signal

`select_unprepared_turns` MUST expose, per turn, whether it is its chapter's first substantive turn: true iff no other turn in the same chapter has both an earlier `start_seconds` and a duration >= 30.0 seconds. The signal MUST be computed over every turn belonging to the chapter, regardless of that turn's prepared, procedural, or unprepared-turns filter status.

#### Scenario: Signal true when only short blips precede the turn
- GIVEN turn 335 (chapter 522, duration >= 30.0s) has no earlier same-chapter turn >= 30.0s
- WHEN `select_unprepared_turns` computes the signal for turn 335
- THEN the signal is true

#### Scenario: Signal considers all chapter turns, not only unprepared/non-procedural ones
- GIVEN an earlier same-chapter turn has duration >= 30.0s but is already prepared or procedural
- WHEN the signal is computed for a later same-chapter turn
- THEN the signal is false, because the earlier substantive turn still counts

## MODIFIED Requirements

### Requirement: Preceding Window Selection

`anchor` MUST be `group_start_seconds` when not None, else `start_seconds`. When the turn's chapter-first-substantive-turn signal is true, `chapter_start_seconds` is parseable, and the gap `anchor - chapter_start_seconds` is greater than 0 and at most 300 seconds, `window_start` MUST be `max(0, min(anchor, chapter_start_seconds) - 120)`; otherwise `window_start` MUST be `max(0, anchor - 120)`. A block MUST be selected iff `window_start <= block.start < anchor`.
(Previously: `window_start` was always `max(0, anchor - 120)`, with no chapter-start exception.)

#### Scenario: Block at window-start boundary is included
- GIVEN a block starting at exactly `window_start`
- WHEN selecting the preceding window
- THEN the block is included

#### Scenario: Block just before window-start is excluded
- GIVEN a block starting at `window_start - 0.001`
- WHEN selecting the preceding window
- THEN the block is excluded

#### Scenario: Block at the anchor is excluded
- GIVEN a block starting exactly at `anchor`
- WHEN selecting the preceding window
- THEN the block is excluded

#### Scenario: Gap above the cap keeps the standard window
- GIVEN a chapter-first-substantive turn whose anchor is more than 300s after `chapter_start_seconds`
- WHEN selecting the preceding window
- THEN `window_start` is `max(0, anchor - 120)`

#### Scenario: Block overlapping the anchor is selected by start time
- GIVEN a block starting at `anchor - 1` and ending after `anchor`
- WHEN selecting the preceding window
- THEN the block is included

#### Scenario: Anchor near session start clamps window_start to zero
- GIVEN an anchor less than 120 and the signal is false
- WHEN selecting the preceding window
- THEN `window_start` is 0

#### Scenario: group_start_seconds overrides the turn's own start
- GIVEN a turn where `group_start_seconds` and `start_seconds` differ
- WHEN selecting the preceding window
- THEN the anchor is `group_start_seconds`

#### Scenario: Chapter's first substantive turn reaches the pre-chapter announcement
- GIVEN turn 335, chapter_start_seconds 14235.84, anchor 14416.84, signal true, announcement near 14212-14224s
- WHEN selecting the preceding window
- THEN `window_start` is `max(0, min(14416.84, 14235.84) - 120)` = 14115.84, and the announcement falls inside `[window_start, anchor)`

#### Scenario: Mid-chapter monologue window is unchanged
- GIVEN a monologue turn whose chapter-first-substantive-turn signal is false
- WHEN selecting the preceding window
- THEN `window_start` is exactly `max(0, anchor - 120)`, byte-identical to current behavior

#### Scenario: Unparseable or missing chapter start falls back to the standard window
- GIVEN the signal is true but `chapter_start_seconds` is unparseable or missing
- WHEN selecting the preceding window
- THEN `window_start` is `max(0, anchor - 120)`

#### Scenario: Extended window still clamps at zero
- GIVEN the signal is true and `min(anchor, chapter_start_seconds) - 120` is negative
- WHEN selecting the preceding window
- THEN `window_start` is 0
