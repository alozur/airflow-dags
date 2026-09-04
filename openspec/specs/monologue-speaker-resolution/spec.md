# Monologue Speaker Resolution Specification

## Purpose

Two-step LLM speaker identification for monologue (non-qa) turns, using only the pre-turn
announcement window (max 120s before the turn anchor) and never the turn's own transcript, so
attribution cannot be won by a person merely addressed or mentioned. Applies at
`speaker_turn_prepare_dag.py`'s speaker-resolution step.

## Requirements

### Requirement: Preceding Window Selection

`anchor` MUST be `group_start_seconds` when not None, else `start_seconds`. `window_start` MUST
be `max(0, anchor - 120)`. A block MUST be selected iff `window_start <= block.start < anchor`.

#### Scenario: Block at window-start boundary is included
- GIVEN a block starting at exactly `anchor - 120`
- WHEN selecting the preceding window
- THEN the block is included

#### Scenario: Block just before window-start is excluded
- GIVEN a block starting at `anchor - 120 - 0.001`
- WHEN selecting the preceding window
- THEN the block is excluded

#### Scenario: Block at the anchor is excluded
- GIVEN a block starting exactly at `anchor`
- WHEN selecting the preceding window
- THEN the block is excluded

#### Scenario: Block overlapping the anchor is selected by start time
- GIVEN a block starting at `anchor - 1` and ending after `anchor`
- WHEN selecting the preceding window
- THEN the block is included

#### Scenario: Anchor near session start clamps window_start to zero
- GIVEN an anchor less than 120
- WHEN selecting the preceding window
- THEN `window_start` is 0

#### Scenario: group_start_seconds overrides the turn's own start
- GIVEN a turn where `group_start_seconds` and `start_seconds` differ
- WHEN selecting the preceding window
- THEN the anchor is `group_start_seconds`

### Requirement: Announcement Pre-Gate

`has_announcement_phrase` MUST run on the concatenated window text before any LLM call; a false
result MUST skip both steps.

#### Scenario: No announcement phrase skips both LLM calls
- GIVEN window text without an announcement phrase
- WHEN resolving a monologue turn
- THEN no LLM call is made and the result is unresolved (`None`, matching `resolve_speaker`'s unresolved return)

### Requirement: Step-1 Prompt Payload Scope

Step 1 MUST receive only the concatenated selected-window blocks; it MUST NOT include text from
blocks before `window_start` or at/after `anchor`.

#### Scenario: Payload excludes text outside the window
- GIVEN blocks before `window_start` and blocks at/after `anchor`
- WHEN Step 1 is invoked
- THEN the captured request payload contains none of that text

### Requirement: Step-1 Floor-Holder Identification

Step 1 MUST return `{announced_name_or_role, evidence, found}`, MUST identify the person
announced to hold the floor (not one merely addressed or mentioned), and its `evidence` MUST be
locatable in the window blocks or the result is unresolved.

#### Scenario: Full name announcement resolves the floor holder
- GIVEN window text "Tiene la palabra la señora García"
- WHEN Step 1 runs
- THEN `announced_name_or_role` is "García" and `found` is true

#### Scenario: Addressee is not conflated with the responder
- GIVEN window text "Señor López, le contesta el ministro X"
- WHEN Step 1 runs
- THEN `announced_name_or_role` is "X", not "López"

#### Scenario: Role announcement after a courtesy phrase resolves correctly
- GIVEN window text "gracias señora presidenta, tiene la palabra el señor Ruiz"
- WHEN Step 1 runs
- THEN `announced_name_or_role` is "Ruiz"

#### Scenario: No announcement found stops before Step 2
- GIVEN Step 1 returns `found=false`
- WHEN resolving the turn
- THEN Step 2 is never called and the result is unresolved

#### Scenario: Unlocatable evidence is rejected
- GIVEN Step 1's `evidence` cannot be located in the window blocks
- WHEN validating the Step-1 result
- THEN the result is unresolved

### Requirement: Step-2 Prompt Payload Scope

Step 2 MUST receive only `{announced_name_or_role, evidence}` plus the roster (slugs and names);
it MUST NOT receive SRT block text beyond the evidence quote.

#### Scenario: Payload contains no window text beyond the evidence quote
- GIVEN a Step-1 result and the participant roster
- WHEN Step 2 is invoked
- THEN the captured request payload contains no window text other than the evidence string

### Requirement: Step-2 Roster-Backed Resolution

Step 2 MUST return `{full_name, participant_slug, confidence}`. The slug MUST exist in the
roster; a slug outside the roster, or confidence below `SPEAKER_RESOLUTION_MIN_CONFIDENCE`
(0.80), MUST yield an unresolved result, never a guessed slug.

#### Scenario: High-confidence roster match resolves
- GIVEN Step 2 returns a roster slug with confidence >= 0.80
- WHEN validating the result
- THEN the turn resolves to that slug

#### Scenario: Low confidence yields unresolved
- GIVEN Step 2 returns confidence < 0.80
- WHEN validating the result
- THEN the result is unresolved

#### Scenario: Slug outside the roster yields unresolved
- GIVEN Step 2 returns a slug not present in the roster
- WHEN validating the result
- THEN the result is unresolved

### Requirement: Result Shape and Evidence Audit

`resolve_monologue_speaker` MUST return `{participant_slug, confidence, evidence}` matching
`resolve_speaker`'s shape, plus a compact JSON audit string carrying `announced_name_or_role`,
`evidence`, `step1_found`, `step2_confidence`, `window_start_seconds`, `anchor_seconds`, and
`method: "monologue_window_v1"`. Unresolved MUST be returned as `None`, exactly as
`resolve_speaker` does; the audit string is carried as a fourth key `audit` of the resolved dict.

#### Scenario: Successful resolution produces both the result and the audit string
- GIVEN a resolvable turn
- WHEN resolution completes
- THEN the returned dict matches `resolve_speaker`'s shape and the audit JSON has all seven keys

### Requirement: Evidence Persistence

Migration `046_add_speaker_resolution_evidence.sql` MUST add
`speaker_turn_videos.speaker_resolution_evidence TEXT NULL` via `ADD COLUMN IF NOT EXISTS`, with
its DOWN block commented per the `044` convention. `production_schema.sql` and its column-tuple
test MUST be updated in the same change. `mark_turn_resolved` MUST accept an optional `evidence`
parameter (default None) and MUST write the column only when provided.

#### Scenario: Migration is idempotent
- GIVEN the migration has already run
- WHEN it runs again
- THEN it does not error and the column is unchanged

#### Scenario: Existing callers are unaffected
- GIVEN a caller of `mark_turn_resolved` without an `evidence` argument
- WHEN the call executes
- THEN `speaker_resolution_evidence` is left untouched

### Requirement: Routing by Turn Type

The caller MUST invoke `resolve_monologue_speaker` when `turn_type != 'qa'` and MUST invoke
`resolve_speaker` when `turn_type == 'qa'`, including the qa-promotion wide re-resolve.
Idempotency skip (existing slug at confidence >= 0.80) and the Gate B roster crosscheck MUST
remain unchanged for either path.

#### Scenario: Non-qa turn routes to the new resolver
- GIVEN a turn with `turn_type != 'qa'`
- WHEN the prepare step resolves speakers
- THEN `resolve_monologue_speaker` is called, not `resolve_speaker`

#### Scenario: qa turns and qa-promotion re-resolves keep using resolve_speaker
- GIVEN a turn with `turn_type == 'qa'`, or the qa-promotion signal fires on a monologue turn
- WHEN the prepare step resolves speakers
- THEN `resolve_speaker` is called

### Requirement: Non-Regression of the Existing Resolver

`resolve_speaker`, its prompts, and `tests/congress_videos/modules/test_speaker_resolution.py`
MUST remain unmodified and passing.

#### Scenario: Existing qa suite stays green
- GIVEN the full existing `test_speaker_resolution.py` suite
- WHEN it runs unmodified
- THEN it passes

### Requirement: Never-Raise Contract

`resolve_monologue_speaker` MUST catch any LLM or parsing error from either step, log a WARNING,
and return an unresolved result — mirroring `resolve_speaker`'s never-raises contract.

#### Scenario: Step-1 or Step-2 exception does not propagate
- GIVEN `completion_fn` raises during Step 1 or Step 2
- WHEN `resolve_monologue_speaker` is called
- THEN it returns unresolved and logs a WARNING, without raising

#### Scenario: A completion error response is handled without an exception
- GIVEN `completion_fn` returns a result whose `error` is set, or whose `data` is missing or not a dict
- WHEN `resolve_monologue_speaker` is called
- THEN it returns `None` and logs a WARNING, no exception is raised or caught, and a Step-1 error stops before Step 2
