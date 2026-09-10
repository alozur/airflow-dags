# Proposal: Monologue Intro Anchor (issue #613)

## Problem Statement

A chapter's first substantive monologue turn stays unresolved when a short diarization blip
(<30s, another label) precedes it. Long turns (>= 300s) always materialize solo, so
`group_start_seconds == start_seconds` and the 120s window `[anchor-120, anchor)` starts
minutes after the chapter start and can miss the pre-chapter announcement, so
`has_announcement_phrase` short-circuits. Prod evidence (exploration, not re-queried here):
turn 335 (chapter 522) chapter start 14235.84, turn start 14416.84, announcement
~14212-14224; 3/204 monologue turns match (196 +135s resolved; 321 +198.6s, 335 +181s
unresolved). The SRT sidecar (padded ±180s) holds the text.

## Intent

Let the first substantive turn of a chapter reach the pre-chapter announcement. Leave every
other monologue turn byte-identical.

## Scope

### In Scope
- Additive boolean column in `select_unprepared_turns`: the turn is its chapter's first
  substantive turn (no earlier turn in the same chapter with duration >= 30.0s).
- When true: `window_start = max(0, min(anchor, chapter_start) - 120)`. Otherwise unchanged.
- Regression test with turn 335's geometry; SQL test for the new column.
- MODIFIED delta: "Preceding Window Selection".

### Out of Scope
- Widening `MONOLOGUE_WINDOW_SECS` globally.
- `materialization.py` 300s solo rule; qa path (`speaker_resolution.py`).
- DB migration; `uploadable_turns` view (the resolver reads `select_unprepared_turns`, not the view).
- Backfill/re-resolution of turns 321/335.

## Capabilities

### New Capabilities
None

### Modified Capabilities
- `monologue-speaker-resolution`: "Preceding Window Selection" gains a chapter-first-substantive
  exception extending `window_start` to `chapter_start - 120`.

## Approach

Exploration Approach 1. SQL flags the chapter's first substantive turn. The module parses
`vc.start_time` and extends only the lower bound. The upper bound stays `anchor`. An unparseable
or missing chapter start falls back to today's window. `window_start_seconds` in the audit
records the extended value.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/modules/database.py` | Modified | Additive column in `select_unprepared_turns` |
| `congress_videos/modules/monologue_speaker_window.py` | Modified | Conditional `window_start` |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Modified | Turn-335 regression + non-regression |
| `tests/congress_videos/modules/test_database*.py` | Modified | New column coverage |
| `openspec/specs/monologue-speaker-resolution/spec.md` | Delta | Preceding Window Selection |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Signal computed after WHERE/dedup misses earlier prepared/procedural turns | Med | Design: correlated subquery over all chapter `speaker_turns` |
| Row cardinality or existing columns change | Low | Purely additive; existing SQL tests unchanged |
| Large chapter-start-to-turn gap grows the window | Low | Design decides whether to cap it |
| Fixtures assume all such turns fail today | Low | Design explains why turn 196 resolved |

## Rollback Plan

Revert the single PR. No schema, migration or persisted-state change.

## Dependencies

None.

## Success Criteria

- [ ] Turn-335 fixture: announcement selected, pre-gate passes.
- [ ] Mid-chapter monologue fixtures: window byte-identical.
- [ ] `uv run pytest` green; `dags list-import-errors` empty.
- [ ] Single PR to `dev`, <= 400 changed lines including SDD docs.
