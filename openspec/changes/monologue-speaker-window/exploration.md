# Exploration: monologue-speaker-window (issue #430)

Persisted by the orchestrator verbatim from the `sdd-explore` result (the phase agent had no file-write tool). Mirrored in Engram as `sdd/monologue-speaker-window/explore` (obs #2337).

## Current State

The prepare-time speaker resolution pipeline (issue #177, extended by #263/#282/#284/#321/#322/#342, all already shipped and present in this worktree) is currently a **single combined LLM call** per turn, not the two-step contract issue #430 wants:

- `congress_videos/modules/speaker_resolution.py::resolve_speaker(turn, participants, completion_fn=None)` (238-275) wraps `_resolve_speaker_inner` (283-495), never raises.
- Intro anchor (330-341): `intro_anchor = float(turn.get("group_start_seconds"))` if present else `start_secs` — a deliberate #283 rule-3 fix, **not** strictly the representative turn's own `start_seconds` as issue #430's literal wording implies. `intro_start = max(0.0, intro_anchor - INTRO_WINDOW_SECS)`, `INTRO_WINDOW_SECS = 120` (line 46).
- Turn window (343-345): `turn_end = start_secs + TURN_CONTEXT_SECS` (`TURN_CONTEXT_SECS = 60`, line 49) — **this is turn content sent to the model TODAY that #430 explicitly forbids** ("never pass ... post-turn transcript").
- For non-qa turns: `prompt_text_for_gate = combined_text = intro_text + "\n" + turn_text` (370-393); pre-gate `has_announcement_phrase` (`announcement_patterns.py:44-53`) runs on this combined text; **ONE** `completion_fn` call (429-433) sends `SPEAKER_RESOLUTION_USER_TEMPLATE` (`ai_prompts.py:336-346`), which interpolates `intro_text` + `turn_text` + the FULL participant roster **in the same prompt** — extraction and roster-resolution are not separated today.
- Evidence verification uses `region_blocks`, wider than the prompt (`QA_EVIDENCE_LOOKBACK_SECS = 600`, lines 68-72, 355-359) — this only post-hoc-checks the model's self-reported evidence; it's never itself sent to the model.
- `turn_type == 'qa'` (explicitly out of scope for #430) additionally builds a full chapter-wide `chapter_text` (#322, D1-D8) — this path must stay untouched.

## monologue vs qa representation

`speaker_turn_videos.turn_type` (migration `033_add_turn_type.sql`, `TEXT NOT NULL DEFAULT 'monologue'`, **no CHECK constraint**) is the sole discriminator, selected as `stv.turn_type` in `database.py::select_unprepared_turns` (line 1284). The narrow (non-widened) call path is implicitly "monologue" (whatever `turn_type` the row carries, usually `'monologue'`); the wide qa retry explicitly forces `turn_type='qa'` only on a promotion signal (`speaker_turn_prepare_dag.py:327`, issue #342).

## Persistence / migrations

`speaker_turn_videos` columns (migration `034_add_speaker_resolution.sql`): `resolved_participant_slug`, `speaker_resolution_confidence`, `speaker_resolution_method TEXT CHECK IN ('ai_srt_context','fuzzy','manual')` — `'ai_srt_context'` already covers the LLM path, no CHECK extension needed. **No `evidence` column exists anywhere** — confirmed via grep of `production_schema.sql`; the model's evidence string is used only transiently for the evidence-locatability gate (`speaker_resolution.py:482`) then discarded, visible only via log lines. Issue #430's "auditable source and evidence" AC is currently satisfied by logs only. Latest migration on disk: `044_deterministic_turn_publish_order.sql` (a concurrent change for issue #432, PR #449, adds `045_...mentioned_participant_slugs`; the next free number for this change is therefore 046).

## Existing tests

`tests/congress_videos/modules/test_speaker_resolution.py` (~20 classes) exhaustively covers the CURRENT single-call contract, including three classes that explicitly assert the narrow prompt stays **byte-identical**: `TestNonQaPromptUnchanged`, `TestPreGateUnchangedSlice1`, `TestWideUserTemplate`. These are intentional regression guards from #322. `tests/congress_videos/test_speaker_turn_prepare_dag.py` covers caller wiring: `TestSpeakerResolutionStep`, `TestPromotionHook`, `TestRosterCrosscheckGate`, `TestQaPromotionReresolution`, `TestQaReresolutionNoSignalRegression` — none test a genuinely separate Step-1/Step-2 call pair.

## Reusable infrastructure

`utils.llm_cache.cached_json_completion` + `utils.llm_config.LLM_CHEAP`; `announcement_patterns.has_announcement_phrase` (Step-1 pre-gate); `speaker_roster_crosscheck.chapter_roster_mentions`/`crosscheck_slug` (Gate B, deterministic, reusable unchanged after Step 2 produces a slug); `database.py::mark_turn_resolved` (label-scoped Gate A, 1358-1423) and `select_unprepared_turns` (1259-1318) — unchanged, since both operate on the `{slug, confidence, method}` shape regardless of resolver. `chapter_speaker_resolution.py` (#263) is a different chapter-level batched resolver — a plausible Step-2 prompt-shape template, not directly reusable. `TURN_NAME_RESOLUTION_SYSTEM_PROMPT` (#131, `ai_prompts.py:368-396`) is a **different, detect-time** pipeline stage (`speaker_turns.py`) — must not be conflated with #430's prepare-time two-step.

## Approaches

1. **Refactor `resolve_speaker`'s narrow branch in place** (same public signature) — Pros: smallest diff, zero caller change. Cons: grows an already-branching function's complexity further, and deliberately breaks the three byte-identical-narrow-prompt tests, which need real rewriting. Effort: Medium-High.

2. **New standalone module** `congress_videos/modules/monologue_speaker_window.py` with three pure pieces: `select_preceding_window(blocks, turn_start_seconds, window_seconds=120)` (trivially provable strict window), `identify_floor_holder(window_blocks, completion_fn=None)` → `{announced_name_or_role, evidence, found}` (Step 1), `resolve_announced_identity(announced_name_or_role, evidence, participants, completion_fn=None)` (Step 2, receives only Step 1's small JSON, never SRT blocks). Thin orchestrator `resolve_monologue_speaker`. Caller branches on `turn_type != 'qa'`. Pros: matches the issue's literal contract exactly, zero risk to the existing qa/#322 regression suite, follows the repo's established "one pure module per gate" convention (`speaker_roster_crosscheck.py`, `announcement_patterns.py`). Cons: some SRT/evidence-helper duplication unless explicitly shared imports; caller needs a `turn_type` branch. Effort: Medium.

3. **Hybrid dispatch inside `resolve_speaker`** — combines the risks of both above with none of the isolation benefits. Effort: High.

## Recommendation

Approach 2. Isolates blast radius to the monologue path, keeps the exhaustively-tested qa/#322/#342 machinery byte-identical, and makes the "at most 120s sent to the LLM" claim directly provable via a small pure function. Reuse (import, don't duplicate): `has_announcement_phrase`, `find_srt_for_chapter`/`_parse_srt_blocks`/`_srt_timestamp_to_seconds`, `_normalize_for_evidence`/`_evidence_supported`, `cached_json_completion`/`LLM_CHEAP`, `chapter_roster_mentions`/`crosscheck_slug`, `mark_turn_resolved`/`promote_turn_type_to_qa`.

Two open questions for sdd-propose: (1) anchor semantics — reuse the existing `group_start_seconds`-fallback anchor (recommended, avoids reintroducing the #283 diarization-blip bug) vs. the issue's literal turn-own-`start_seconds` wording; (2) evidence persistence — new migration for a DB column vs. accepting the existing log-based audit trail.

## Risks

- Turn content sent today for every non-qa turn must be excluded from Step 1 — deliberate, breaking contract change.
- Step-1 system prompt needs fresh authoring (addressee-vs-floor-holder distinction doesn't exist in any current prompt).
- Evidence-verification region (600s) is wider than any 120s prompt window — needs an explicit propose-phase decision for monologue turns.
- `turn_type` has no CHECK constraint; caller should treat non-`'qa'` as monologue.
- Verify worktree is still current at design/apply time — this pipeline area has had rapid successive PRs.

## Ready for Proposal

Yes.
