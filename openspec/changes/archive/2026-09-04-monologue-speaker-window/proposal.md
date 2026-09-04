# Proposal: Monologue Speaker Window (issue #430)

## Intent

Monologue resolution today makes ONE LLM call carrying the 120 s intro window, 60 s of the turn's own transcript, and the roster (`speaker_resolution.py:343-421`), so a person merely *addressed* early in the turn can win the attribution. #430 forbids post-turn text and requires two separated steps. Success: attribution derives only from the pre-turn announcement window, or stays unresolved, with evidence auditable in the DB.

## Scope

### In Scope
- New `congress_videos/modules/monologue_speaker_window.py`: `select_preceding_window`, `identify_floor_holder` (Step 1), `resolve_announced_identity` (Step 2), `resolve_monologue_speaker`.
- Two prompts in `config/ai_prompts.py`: Step 1 sees window text only; Step 2 sees only `{announced_name_or_role, evidence}` + roster, never SRT.
- Window: `anchor = group_start_seconds if not None else start_seconds` (#283); keep blocks with `start >= max(0, anchor-120)` and `start < anchor` — an overlapping block is in, nothing at/after the anchor ever is.
- Gates reused: `has_announcement_phrase` on window text only (absent → skip both LLM calls); `_evidence_supported` on window blocks only (no 600 s lookback); `SPEAKER_RESOLUTION_MIN_CONFIDENCE = 0.80`.
- Routing at `speaker_turn_prepare_dag.py:306`: `turn_type != 'qa'` → new resolver; qa → `resolve_speaker`.
- Migration `046_add_speaker_resolution_evidence.sql` (`speaker_turn_videos.speaker_resolution_evidence TEXT NULL`, DOWN commented per 044), `production_schema.sql` snapshot + column test, `mark_turn_resolved(..., evidence=None)`.

### Out of Scope
- qa / multi-speaker groups; chapter-level attribution.
- Editing `resolve_speaker` or its byte-identical-prompt tests. Its narrow branch is **not** dead — it still serves qa turns with an unparseable chapter span (`:384-390`); a follow-up issue reassesses it.
- Backfill of already-resolved turns.

## Capabilities

### New Capabilities
- `monologue-speaker-resolution`: window selection, two-step identification, roster-backed resolution, evidence persistence.

### Modified Capabilities
- None (`openspec/specs/` does not exist here).

## Approach

Exploration Approach 2. `resolve_monologue_speaker` returns the shape the caller already consumes — `{participant_slug, confidence, evidence}` — so Gate B (`crosscheck_slug`) and idempotency are reused untouched; Gate A (`mark_turn_resolved`) keeps its WHERE byte-identical and only gains the optional `evidence` parameter. The qa-promotion signal is computed by the **caller** (`_is_qa_promotion_signal`, `:316`), not returned by the resolver, so #342 needs no new field. Idempotency is unchanged: the caller skips turns already resolved at >= 0.80 (`:301-304`).

## Affected Areas

| Area | Impact |
|------|--------|
| `modules/monologue_speaker_window.py` | New |
| `config/ai_prompts.py`, `speaker_turn_prepare_dag.py`, `modules/database.py` | Modified |
| `sql/migrations/046_*.sql`, `sql/production_schema.sql`, `tests/congress_videos/**` | New/Modified |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Recall drops without turn text | Med | Deliberate per #430; pre-gate prefers unresolved over guessed |
| Number collision with PR #449 (045) | Med | 046 is next free; renumber on rebase |
| qa-path regression | Low | `resolve_speaker` untouched; its suite stays green |

## Rollback Plan

Revert the routing commit: monologue returns to `resolve_speaker` and the new module goes inert. Migration 046 is additive and nullable — leave it; a full revert drops the column manually.

## Dependencies

- PR #449 (migration 045) lands first; re-verify worktree currency at apply time.

## Delivery (auto-chain)

Over 400 lines. **A1** window selection + prompts + tests (inert); **A2a** dataclasses + two LLM steps + tests (inert); **A2b** audit + loader + orchestrator + tests (inert). **B** migration + snapshot + schema tests + `mark_turn_resolved(evidence=)`. **C** routing + wiring tests + docs + follow-up issue (needs A2b, B).

## Success Criteria

- [ ] Step-1 payload excludes text at/after the anchor and before `anchor-120`.
- [ ] Step 2 never receives SRT text.
- [ ] Tests cover full name, role-only, addressee-vs-speaker, no announcement, both boundaries.
- [ ] Unresolved leaves the slug NULL, never guessed.
- [ ] `speaker_resolution_evidence` populated on every new monologue resolution.
- [ ] `tests/congress_videos/modules/test_speaker_resolution.py` (qa/#322 module suite) green with a zero-line diff; the caller suite `tests/congress_videos/test_speaker_turn_prepare_dag.py` (#342 promotion tests) is rewired for the new routing with its assertions' semantics preserved.
