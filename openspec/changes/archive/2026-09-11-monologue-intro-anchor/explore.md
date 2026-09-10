## Exploration: monologue-intro-anchor (issue #613)

### Current State — grounded on worktree airflow-dags-wt-613, branch wt/613-sdd, based on origin/dev 7f4cd5a

Monologue (non-qa) turns route through `resolve_monologue_speaker` in
`congress_videos/modules/monologue_speaker_window.py` (issue #430, live since main 68e02b6,
2026-09-04) — NOT through `resolve_speaker` in `speaker_resolution.py`. That module's docstring
says so explicitly: "Monologue turns are resolved by monologue_speaker_window.resolve_monologue_speaker,
not by this module." `speaker_turn_prepare_dag.py:453-456` branches `turn_type != 'qa'` ->
`resolve_monologue_speaker`, else `resolve_speaker` (qa + qa-promotion re-resolve, #322/#342 paths,
byte-identical and out of scope here).

`turn_anchor_seconds(turn)` (monologue_speaker_window.py:60-71) is the #283-rule-3-derived anchor:
`group_start_seconds` wins when not None (including 0.0), else `start_seconds`. Window:
`window_start = max(0, anchor - MONOLOGUE_WINDOW_SECS)`, `MONOLOGUE_WINDOW_SECS = 120` (fixed
per design.md, "not a parameter"). `select_preceding_window` keeps blocks with
`window_start <= start_secs < anchor`. `has_announcement_phrase` gates BOTH LLM calls — no
announcement phrase in the window text short-circuits to `None` before any completion_fn call.

`group_start_seconds` comes from `database.py::select_unprepared_turns` (~line 1807):
`MIN(st.start_seconds) OVER (PARTITION BY stv.output_path)` — scoped to whichever
`speaker_turn_videos` rows share one materialized `output_path`. Grouping into one `output_path`
happens earlier, in `materialization.py::plan_turn_materialization` (not touched by #430): any turn
whose duration `>= MIN_LONG_INTERVENTION_SECS` (300.0s) unconditionally flushes any accumulated
short-turn group and gets `turn_ids=(its own turn_id,)` — always solo, regardless of what
immediately precedes it, even a 0-gap short turn. **Consequence proven by prod evidence (folded in
below): for every long/solo monologue turn, `group_start_seconds` mathematically equals the turn's
own `start_seconds` — the #283 fallback is a structural no-op for this entire turn class**, not
just an edge case triggered by a missing group.

Chapter bounds (`vc.start_time`/`vc.end_time`, SRT-format strings) ARE already present in the same
`select_unprepared_turns` row (same columns `speaker_resolution.py::_chapter_span` parses), so
`chapter_start_seconds` is reachable at the monologue call site without new SQL — it's just never
read there today. The per-chapter `subtitles.srt` sidecar consumed by
`monologue_speaker_window._load_turn_blocks` is padded ±180s around the chapter boundary
(`srt_helpers.py:395`, `CHAPTER_SRT_PAD_SECS = 180.0`, "reach verified against ... INTRO_WINDOW_SECS=120
... QA_EVIDENCE_LOOKBACK_SECS=600"), so the announcement text is already present on disk whenever the
gap from chapter start is <=180s — the bug is purely in the window filter, not in data availability.

### Prod evidence (parallel agent, run 2026-09-10 17:18, folded in per coordinator instruction)

1. Turn 335 confirms the code trace above: `group_start_seconds = 14416.84` (turn 335's own start,
   NOT None) because turns 334/335/336 each materialize into their own `output_path` (groups of
   size 1). Window `[14296.84, 14416.84)` misses the announcement at ~14212-14224s;
   `has_announcement_phrase` returns false, both LLM calls are skipped, `resolved_participant_slug`
   stays NULL.
2. Chapter 522 span is `14235.84-15247.70s`. Merging blip turn 334 into turn 335's group would
   NOT fix this: turn 334 starts at 14411.79, itself well AFTER the announcement — the real gap is
   chapter-start-to-first-substantive-turn, not blip-to-turn.
3. Prevalence: 3 of 204 materialized monologue turns match "chapter's first substantive (>=30s)
   turn, preceded only by <30s other-label turns, starts >60s after chapter start": turn 196
   (ch 263, +135s, resolved anyway), turn 321 (ch 519, +198.6s, unresolved), turn 335 (ch 522,
   +181s, unresolved). Rare (~1.5%) but not a one-off.
4. Evidence-preferred direction: anchor the intro window to chapter start when the turn is the
   chapter's first substantive turn; naive window widening is explicitly framed as the "blunter
   alternative" (needs 135-199s, no forward-proof ceiling).

### Affected Areas
- `congress_videos/modules/monologue_speaker_window.py` — `turn_anchor_seconds`,
  `select_preceding_window` call site in `_resolve_monologue_inner` (window_start computation is
  where the fix lands).
- `congress_videos/modules/database.py::select_unprepared_turns` (~line 1781-1840) — only touched if
  Approach 1 (chapter-first-substantial SQL signal) is selected; no schema/migration needed, pure
  query-string change.
- `tests/congress_videos/modules/test_monologue_speaker_window.py` (603 lines today) — new test
  class for the widened-anchor path.
- `tests/congress_videos/modules/test_database.py` — only if Approach 1's new column is added.
- `openspec/specs/monologue-speaker-resolution/spec.md` — "Preceding Window Selection" requirement
  needs a MODIFIED/ADDED delta; it currently states `window_start` MUST be exactly
  `max(0, anchor-120)` with no exception.
- NOT affected: `congress_videos/modules/speaker_resolution.py` (frozen, qa path unchanged per
  spec's "Non-Regression of the Existing Resolver" requirement) and `materialization.py` (its
  300s solo-group rule is a deliberate, correct video-splicing decision — conflating it with the
  resolution anchor is the root cause, not something to "fix" there).

### Approaches

1. **Chapter-first-substantial SQL signal + bounded chapter-start anchor extension** (recommended)
   — add a window function to `select_unprepared_turns` computing whether this turn's start equals
   the chapter's earliest turn with duration >= a SUBSTANTIAL_TURN_MIN_SECONDS threshold (evidence
   suggests 30.0s); when true AND `group_start_seconds == start_seconds` (no real materialization
   grouping) AND `chapter_start_seconds < window_start`, extend `window_start` down to
   `chapter_start_seconds` (safe: SRT sidecar already padded 180s before chapter start).
   - Pros: precisely scoped to the exact failure mode prod evidence isolated; zero risk to
     mid-chapter monologues (whose "chapter-first" signal is false); no schema/migration; reuses
     `_chapter_span` (already duplicated/imported convention in this module).
   - Cons: touches `database.py` SQL (new window function ~8-12 lines) in addition to the pure
     module; adds one new implicit threshold constant (30.0s) needing an explicit design decision.
   - Effort: Medium. Estimated ~130-200 changed lines (SQL + module + 2 test files) — comfortably
     under the 400-line PR budget, single PR.

2. **Pure-code bounded chapter-start heuristic, no SQL** — reuse `chapter_start_seconds` (already
   in the row via `vc.start_time`) directly in `monologue_speaker_window.py`; when
   `group_start_seconds` is absent/self AND `anchor - chapter_start_seconds <= CAP` (propose e.g.
   300.0s, matching the existing `MIN_LONG_INTERVENTION_SECS` domain constant), extend
   `window_start` to `chapter_start_seconds`.
   - Pros: smallest diff, single file + tests, no SQL touch, no migration; ~70-115 changed lines.
   - Cons: less precise than Approach 1 — the cap-distance proxy cannot distinguish "true
     chapter-first turn" from "a long monologue that merely starts within CAP seconds of chapter
     start but was legitimately preceded by real (non-blip) content"; could occasionally widen for
     turns that don't need it (no confirmed instance in the 204-turn sample, but not provably
     excluded either).
   - Effort: Low-Medium.

3. **Widen `MONOLOGUE_WINDOW_SECS` globally** (e.g. 120 -> 220s) — one-constant change.
   - Pros: trivial diff.
   - Cons: explicitly the "blunter alternative" per prod evidence; no forward-proof ceiling (198.6s
     observed, nothing bounds a worse future case); increases LLM context/cost and false-positive
     risk for EVERY monologue turn, not just the ~1.5% actually broken; contradicts design.md's
     "MONOLOGUE_WINDOW_SECS ... not a parameter" framing and the existing byte-identical-window
     spec requirement without narrowing scope to the actual failure class.
   - Effort: Low, but rejected — least robust.

### Recommendation

Approach 1. It is the only option that matches the prod-evidence-preferred fix ("anchor to chapter
start when the turn is the chapter's first substantive turn") with a precise, provably-scoped
signal rather than a distance-based proxy, stays within the 400-line PR budget as a single change,
requires no migration, and leaves the qa path (`resolve_speaker`) and materialization's 300s
solo-group rule completely untouched — both are correct for their own concerns and must not be
conflated with the resolution anchor again. Approach 2 is a reasonable smaller fallback if
sdd-design prefers to avoid a `database.py` SQL touch, with the explicit tradeoff of some
unproven over-widening risk documented for propose/design to accept or reject.

Since issue #613 is labeled "investigate" but explicitly requires code, not just a report, this
exploration recommends the SDD change proceed straight through propose/design carrying Approach 1
as the default technical approach, with the SUBSTANTIAL_TURN_MIN_SECONDS threshold (30.0 proposed)
and the CAP fallback value (300.0 proposed, Approach 2 only) as the two open numeric decisions for
sdd-propose to confirm.

### Risks
- `MONOLOGUE_WINDOW_SECS`/window_start is documented in design.md and the current spec's "Preceding
  Window Selection" requirement as an exact `max(0, anchor-120)` formula with no exception — any
  approach here is an intentional, spec-breaking narrowing of that invariant, not a silent
  extension; needs an explicit MODIFIED requirement delta, and the existing boundary-condition
  tests in `test_monologue_speaker_window.py` (window-start-clamped-to-zero, etc.) must be reread
  for compatibility, not assumed unaffected.
- Approach 1's new SQL touches `select_unprepared_turns`, a query already carrying several
  historical gates (#146/#141/#234-adjacent columns) — needs care not to change row cardinality or
  existing column values, purely additive.
- Prevalence is low (3/204 in the sampled data) — sizing/priority is a product decision for
  sdd-propose, not this exploration; the fix is technically small regardless of prevalence.
- Turn 196 (ch 263, +135s gap) resolved anyway under the CURRENT code — worth confirming in
  propose/design why that one already worked (possibly a different announcement phrasing placed
  partially inside the current 120s window) so the fix's test fixtures don't assume all
  first-substantive-turn cases are uniformly and totally blocked today.

### Ready for Proposal
Yes — the failure mechanism is fully traced end-to-end in code (materialization -> SQL
group_start_seconds -> anchor -> window filter -> announcement pre-gate short-circuit) and
corroborated by prod evidence with exact turn IDs and gap measurements. sdd-propose can proceed
directly to Approach 1 as the default technical approach.
