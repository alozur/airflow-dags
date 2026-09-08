# Proposal: Publish the real clip duration in YouTube descriptions

## Intent

Every long-form turn clip published to YouTube carries `⏱️ Duración: 0 minutos` in its
description (issue #514). Root cause is a **field-shape mismatch**, not a unit or ordering bug:
`generate_youtube_metadata_for_selected_videos` reads `video.get("duration_minutes", 0)`
(`congress_videos/modules/youtube/youtube_ai.py:295`), but the `uploadable_turns` view
(migration `044`, lines 62-98) has no `duration_minutes` column, so the default fires on every
turn row. `youtube_ai.py:95` then sees `"0 minutos"` (not `"N/A"`), so the guard at line 125 does
not skip the line and the false figure is published. Turn uploads are the only live long-form
path (issue #171); shorts use a separate description builder and are unaffected.

## Scope

### In Scope
- Derive the published duration for turn rows inside `generate_youtube_metadata_for_selected_videos`.
- Discriminate row shape with `"turn_id" in video`, the convention already used at
  `congress_videos/youtube_upload_dag.py:384`.
- RED-first regression test with a new turn-shaped fixture.

### Out of Scope
- **Retroactive correction of already-published descriptions.** Clips already on YouTube keep
  saying "0 minutos"; a `videos.update` backfill is a separate follow-up issue, not this change.
- Any DB migration, view change, or `production_schema.sql` snapshot update.
- The chapter branch's `video.get("duration_minutes", 0)` behaviour (production-dead, unchanged).
- Singular/plural grammar of the `"{n} minutos"` string.

## Capabilities

### New Capabilities
- `upload-description-duration`: the duration figure emitted into a published long-form YouTube description.

### Modified Capabilities
- None.

## Approach

For turn rows, compute `published_seconds`:

1. `group_end_seconds - group_start_seconds - procedural_seconds` when all three are present and
   non-null — the exact formula of the view's own 300s eligibility gate (migration `044`,
   lines 100-105), so the displayed duration agrees with the gate that admitted the clip.
2. Fallback `end_seconds - start_seconds` when the group fields are absent.

Formatting contract (no ambiguity left for spec/design to invent):

| `published_seconds` | `duration_seconds` | `duration_estimated` | Published line |
|---|---|---|---|
| derivable, `> 0` | `int(published_seconds)` | `f"{max(1, round(s/60))} minutos"` | rendered |
| `<= 0`, or any required field missing/`None` | `0` | `"N/A"` | **omitted** (existing guard) |

Rounding is to the nearest whole minute, half away from zero, clamped to a minimum of `1` so a
real clip never renders `0 minutos`. Sub-60s spans cannot pass the 300s gate in production; the
clamp is defensive only.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/modules/youtube/youtube_ai.py` | Modified | Turn-aware duration branch in `generate_youtube_metadata_for_selected_videos` |
| `tests/congress_videos/modules/youtube/test_youtube_ai.py` | Modified | New turn-shaped fixture alongside the existing chapter-shaped `_make_top_video()` |

## Rejected Alternatives

| Alternative | Reason rejected |
|---|---|
| Add a computed `duration_minutes` column to `uploadable_turns` via a new migration | Duplicates data already derivable from selected columns; drags in schema-snapshot + drift-test churn for no behavioural gain |
| `ffprobe` the materialized clip at description-build time | I/O and subprocess cost per description; can disagree with the eligibility gate's own span |

## Test Strategy (Strict TDD)

File: `tests/congress_videos/modules/youtube/test_youtube_ai.py`.

RED first: add a turn-shaped fixture (`turn_id`, `start_seconds`, `end_seconds`,
`group_start_seconds`, `group_end_seconds`, `procedural_seconds`, no `duration_minutes`) and
assert the produced description contains the expected minutes figure and never `0 minutos`. The
existing chapter-shaped `_make_top_video()` stays and must keep passing. One vertical slice per
behaviour: derived duration, group-field fallback, non-derivable → line omitted.

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Turn rows with `NULL` group fields render no duration line at all | Low | Explicit fallback to `end_seconds - start_seconds`; omission is preferable to a false figure |
| Chapter branch keeps the same fragile `.get(..., 0)` pattern | Low | Unreachable in production; noted for a future unification |

## Size Estimate

~25 lines in `youtube_ai.py` plus ~60 test lines; with the SDD artifacts the change is expected
to land near 300 changed lines, inside the 400-line review budget. Single PR; budget risk **Medium**.

## Rollback Plan

Single-commit revert of the `youtube_ai.py` hunk (and its tests). No schema, no data, no
persisted state is touched — descriptions are computed fresh at build time, so reverting
immediately restores the previous behaviour with no cleanup.

## Dependencies

- None. Internal-codebase-only change; `sdd-research` not selected.

## Success Criteria

- [ ] A turn row from `uploadable_turns` produces `⏱️ Duración: <n> minutos` with `n >= 5`.
- [ ] No published description generated from a turn row contains `0 minutos`.
- [ ] Non-derivable duration omits the duration line rather than printing a false one.
- [ ] `uv run pytest` passes; existing chapter-shaped fixture behaviour unchanged.
