# Proposal: Correct ASR errors in the lapidary thumbnail quote

Issue #611 · base `origin/dev` @ 7f4cd5a · chapter and turn thumbnails (shared path).

## Intent

**Problem.** `extract_lapidary_quote` returns the LLM-ranked SRT clause **verbatim**. Whisper mishears proper
nouns and drops accents, so "Aan Curdi tenía 3 años, una camisa roja" (should be "Aylan Kurdi") is baked
into the Pikzels image. #512 only *flags* `thumbnail_text` after the image already exists, and #545
(regeneration) is long-form-only and heavy. The error has to be caught before the image is generated.

**Success.** A misheard name is either corrected or dropped to the existing `art_direct` fallback. It never
ships unchanged. Quotes without risky entities cost no extra LLM call and produce identical output.

## Scope

### In Scope
- A pure-Python risky-entity gate (proper-noun-shaped tokens, digit sequences) on the winning candidate.
- One narrow JSON-schema correction call (`LLM_CHEAP`, fail-soft) that returns the corrected text plus a confidence.
- An enforced structural guard (word count and order preserved; non-risky tokens unchanged). On low
  confidence, malformed output, or a guard violation the function returns `None`.
- A shared diacritics-preservation line in all three Pikzels templates, following the `_SAFE_ZONE_LINE` pattern.
- Tests: an "Aylan Kurdi" regression covering both the corrected path and the fallback path, plus a test that the happy path is unchanged.

### Out of Scope
- `final_copy_verification.py` (#512), thumbnail regeneration (#545), and DB migrations.
- Correcting misheard common words that are not proper nouns or figures.
- `.agents/skills/congress-thumbnail/SKILL.md` gets an optional note only.

## Capabilities

### New Capabilities
- `lapidary-quote-correction`: gated ASR correction or rejection of the extracted quote, and diacritics
  preservation in the rendered Pikzels prompt.

### Modified Capabilities
- None. No existing spec covers quote extraction or Pikzels prompt rendering.

## Approach

This is Approach 2 from the exploration. `extract_lapidary_quote` first runs the existing candidate
extraction, ranking and index parsing, all unchanged. The selected quote then goes to `_is_risky(quote)`.
If the gate finds nothing, the quote is returned exactly as today. If it finds something, the quote goes to
the correction call and then through the guard, and the result is either the corrected text or `None`.

The guard reuses the idea behind `is_contained`: it enforces the rule structurally and does not rely on the
prompt. That function cannot be reused as it stands, because a name correction adds a token that the
original text does not contain. `completion_fn` injection stays the test seam.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/modules/thumbnail_generation.py` | Modified | Gate, correction helper, guard, wiring |
| `congress_videos/config/ai_prompts.py` | Modified | Correction system/user prompts |
| `congress_videos/modules/thumbnail_prompt.py` | Modified | Shared diacritics line in `_TEMPLATE_A/B/C` |
| `tests/congress_videos/modules/test_thumbnail_*.py` | Modified | Regression, gate, guard, prompt tests |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| The gate is too broad because Spanish text uses capitalized words mid-sentence | Med | Design fixes the heuristic; tests pin negative cases |
| The model rewrites content instead of fixing spelling | Med | Structural guard rejects the correction → `None` |
| More quotes fall back to invented `art_direct` text | Low | This is acceptable because it beats shipping a misheard name |
| Two LLM calls on the risky path | Low | Only the minority path pays; `LLM_CHEAP` tier |

## Rollback Plan

Revert the single PR. There is no schema or state change. Behaviour returns to verbatim quotes, and the
prompt-line edit reverts independently.

## Dependencies

- None. This composes with #545 but does not require it.

## Success Criteria

- [ ] The "Aan Curdi" quote comes out as "Aylan Kurdi ..." when the correction passes the guard, and `None` otherwise.
- [ ] Non-risky quotes make exactly one LLM call; the 8 existing `TestExtractLapidaryQuote` tests pass unchanged.
- [ ] All three Pikzels layouts render the diacritics instruction.
- [ ] `uv run pytest` passes; the single PR stays within the 400 changed-line budget.
