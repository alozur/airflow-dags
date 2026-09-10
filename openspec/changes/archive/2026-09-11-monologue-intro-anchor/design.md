# Design: Monologue Intro Anchor (issue #613)

## Technical Approach

`select_unprepared_turns` adds one boolean column: `is_chapter_first_substantive`.
`_resolve_monologue_inner` computes `window_start` through a new pure helper,
`monologue_window_start(turn, anchor)`. When the flag is true and the chapter start parses, the
lower bound moves back to `chapter_start - 120`. The upper bound stays `anchor`. Every other
turn keeps `max(0, anchor - 120)`. No migration and no view change. `uploadable_turns` is
upload-side only: the prepare DAG reads rows from `select_unprepared_turns` (call at `speaker_turn_prepare_dag.py:406`; routing at 425-456).

## Architecture Decisions

| # | Decision | Rejected alternatives | Rationale |
|---|----------|-----------------------|-----------|
| D1 | SQL signal: `BOOL_OR(<pred>) OVER (PARTITION BY stv.output_path) AS is_chapter_first_substantive`. `<pred>` = `(st.end_seconds - st.start_seconds) >= 30.0 AND NOT EXISTS (SELECT 1 FROM {st_table} st2 WHERE st2.chapter_id = st.chapter_id AND st2.turn_id <> st.turn_id AND (st2.end_seconds - st2.start_seconds) >= 30.0 AND st2.start_seconds < st.start_seconds)` | Window function over the filtered `dedup` rows; a per-row flag without the group OR | `st2` reads the raw table, so earlier turns that are prepared, procedural, or never materialized still count. This matches the "ALL chapter turns" rule. The OR is scoped to `output_path`, like `group_start_seconds`, so the flag holds even when the `DISTINCT ON` representative is a blip. The flag is additive: row count and existing columns do not change |
| D2 | Threshold `SUBSTANTIVE_TURN_MIN_SECS = 30.0` is a module constant in `database.py`, f-string-interpolated like the table names (no new bind param) | A bind param; importing the constant from the resolver | A trusted constant is not an injection vector. The existing `params` assertions stay valid. The database module does not depend on the resolver |
| D3 | Window rule: `window_start = max(0, min(anchor, chapter_start) - 120)` when the flag is true, the span parses, and `0 < anchor - chapter_start <= MONOLOGUE_INTRO_MAX_GAP_SECS (300.0)`. Otherwise it keeps the legacy formula | No cap; a cap tied to `CHAPTER_SRT_PAD_SECS` (180); a global `MONOLOGUE_WINDOW_SECS` bump | The data range is always safe: `chapter_start - 120 >= chapter_start - 180`, so the lower bound stays inside the padded sidecar for any gap. The cap exists for attribution and cost reasons. A gap over 300 s is itself long enough to hold an undetected intervention, so the handover at chapter start stops being evidence for this turn. 300 s covers all observed gaps (135, 181, 198.6) with 50% headroom and limits Step 1 input to 420 s of text (3.5x today). A 180 s cap would already reject turn 321 (198.6 s) |
| D4 | Chapter start comes from `speaker_resolution._chapter_span(turn)`, imported next to the private `_evidence_supported_in_blocks` that is already imported | A new parser; `srt_helpers._srt_timestamp_to_seconds` directly | It is already the parser `select_unprepared_turns` columns are built for, and it never raises. `None` (missing, unparseable, or `end <= start`) falls back to the legacy window |
| D5 | `select_preceding_window(blocks, anchor, window_seconds=120, *, window_start=None)` takes an optional keyword-only `window_start`. `None` keeps the legacy formula byte-identical | Passing `window_seconds = anchor - ws` | Float round-trip `anchor - (anchor - ws)` can drop a block that starts exactly at the boundary |
| D6 | Audit `window_start_seconds` records the effective (possibly extended) value. One INFO log line when an extension applies: turn_id, gap | A new audit key | The spec requires exactly seven audit keys |

**Other handovers inside the extended window.** Code never picks an announcement. Step 1
receives the whole window, and its prompt asks who gets the floor NEXT in a transcript that
ends where the turn starts. The extension only adds text EARLIER than the legacy window, and
the upper bound does not move. So a handover already inside the legacy window stays the one
nearest the turn, and the evidence gate still checks the extended blocks. The residual risk
(two handovers only in the extended text) is bounded by the 300 s cap.

**Turn 196 (+135 s, resolved).** Assumption, not re-queried: its legacy window
`[cs+15, anchor)` already contained a phrase that passes `has_announcement_phrase`, most likely
the broad `gracias, señoría` pattern or a repeated `tiene la palabra` handover, and Step 1 found
a handover there. Test fixtures therefore do not assume every first-substantive turn fails
today. The idempotency skip means already-resolved turns never re-enter the resolver.

**Small gaps.** A first-substantive turn at, for example, +5 s gets 5 s of extra lead-in. This
is intended; turns that are not first-substantive stay byte-identical.

## Data Flow

    speaker_turns (all rows) ──NOT EXISTS──┐
    select_unprepared_turns ──row{is_chapter_first_substantive, start_time}──> prepare DAG
      ──> resolve_monologue_speaker ──> monologue_window_start(turn, anchor)
      ──> select_preceding_window(blocks, anchor, window_start=ws) ──> pre-gate ──> Step 1/2

## File Changes

| File | Action | Est. lines |
|------|--------|-----------|
| `congress_videos/modules/database.py` | Constant + `BOOL_OR ... NOT EXISTS` column + docstring | +14 |
| `congress_videos/modules/monologue_speaker_window.py` | `MONOLOGUE_INTRO_MAX_GAP_SECS`, `monologue_window_start`, `window_start` kwarg, call site, `_chapter_span` import | +35 / -3 |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | New section: helper + resolver tests | +95 |
| `tests/congress_videos/modules/test_database.py` | `TestSelectUnpreparedTurnsChapterFirstSubstantive` (SQL-shape) | +25 |
| `tests/congress_videos/modules/test_mark_turn_resolved_live.py` | One live row-level test with a local explicit-span seeder | +40 |
| `openspec/changes/monologue-intro-anchor/*` | proposal, design, spec delta, tasks | ~240 authored |

## Testing Strategy (strict TDD, `uv run pytest`)

| Scenario | Test |
|----------|------|
| Turn-335 regression | Turn 14416.84 (group_start equal), `start_time="03:57:15,840"`, `end_time="04:14:07,700"`, flag true. Announcement block at 14212, blip block at 14411.79. Step 1 receives the announcement, result resolves, audit `window_start_seconds == 14115.84`. The same fixture with the flag false returns `None` and makes zero LLM calls |
| Mid-chapter non-regression | Flag false or absent: `monologue_window_start == max(0, anchor-120)` exactly; audit value unchanged |
| Fallbacks | Flag true with missing or unparseable `start_time`, gap > 300, or `chapter_start >= anchor`: legacy value |
| Clamp | `chapter_start < 120`: `window_start == 0` |
| Upper bound | Extended window still excludes blocks starting at or after `anchor`; a boundary block at exactly `window_start` is included |
| SQL shape | Alias `is_chapter_first_substantive`, `NOT EXISTS`, `st2.chapter_id = st.chapter_id`, `>= 30.0`, `BOOL_OR` over `stv.output_path`; existing shape tests untouched |
| Live row-level (skips without Postgres) | Chapter A: 10 s blip plus a 400 s turn, so the flag is true. Chapter B: an earlier 40 s procedural turn (filtered from the result) plus a long turn, so the flag is false |

The prepare-DAG mocked rows lack the key, so `turn.get` is falsy and those rows keep the legacy
behavior. No DAG test changes.

## Threat Matrix

N/A: no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary.

## Migration / Rollout

No migration required. Single PR to `dev`. Revert restores the legacy window. Turns 321 and 335
are not re-resolved automatically; that is out of scope.

## Open Questions

- [ ] PR budget: authored code and tests are about 210 lines, but the committed `explore.md`
  (153) plus the proposal (83) push the total past 400. Condense or drop `explore.md` from the
  PR (Engram keeps it), or accept a docs-only overage.
