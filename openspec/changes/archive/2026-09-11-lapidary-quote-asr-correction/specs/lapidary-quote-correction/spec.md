# Lapidary Quote Correction Specification

## Purpose

The lapidary thumbnail quote is extracted verbatim from Whisper-transcribed
SRT text and can carry ASR errors (misheard proper nouns, dropped
diacritics) straight into the rendered thumbnail image. This capability
gates the LLM-ranked winning quote through a cheap risky-entity check,
corrects it via a narrow, structurally-guarded LLM call only when needed,
and falls back to the existing `art_direct` invented-text path whenever the
correction cannot be trusted. It also ensures the rendered Pikzels prompt
instructs the model to preserve diacritics.

## Requirements

### Requirement: Risky-Entity Gate Runs Before Any Correction Call

The system MUST evaluate the ranking-selected winning quote against a
risky-entity gate (proper-noun-shaped tokens and digit sequences) before
considering correction. The system MUST NOT invoke the correction LLM call
when the gate finds no risky token in the quote.

#### Scenario: Happy-path quote makes no extra LLM call

- GIVEN the LLM-ranked winning candidate contains no risky token
- WHEN `extract_lapidary_quote` evaluates the candidate
- THEN no correction LLM call is made
- AND the returned quote is byte-identical to what the function returned
  before this capability existed

#### Scenario: A risky token enters the correction path

- GIVEN the winning candidate contains at least one token the gate flags as
  risky
- WHEN `extract_lapidary_quote` evaluates the candidate
- THEN the quote is passed to the correction call before being returned

### Requirement: Gated Correction Fixes a Flagged Quote

When the gate flags a quote, the system MUST send it to a bounded LLM
correction call that returns corrected text plus a confidence signal, and
MUST return the corrected text only after it passes the structural guard
defined below.

#### Scenario: Misheard proper noun is corrected

- GIVEN the winning candidate is "Aan Curdi tenía 3 años, una camisa roja"
  and the gate flags it as risky
- WHEN the correction call returns "Aylan Kurdi tenía 3 años, una camisa
  roja" with a high-confidence result that passes the structural guard
- THEN `extract_lapidary_quote` returns the corrected quote containing
  "Aylan Kurdi"

### Requirement: Fail-Soft Fallback to None

The system MUST return `None` from the correction path — never the
uncorrected risky quote — whenever the correction call's confidence is too
low, its output is malformed, the call itself errors, or the corrected text
fails a structural guard that checks word count, word order, and that every
non-flagged word is unchanged from the original quote except for restoring
lost diacritics (accent or tilde marks added or swapped on the same base
letters, never removed). A `None` result MUST continue to route through the
existing `art_direct` fallback.

#### Scenario: Unusable correction output is dropped

- GIVEN the correction call returns text below the confidence threshold, or
  returns malformed output, or errors outright — consistent with the
  module's existing fail-soft convention
- WHEN `extract_lapidary_quote` evaluates the result
- THEN it returns `None` instead of the risky or corrected quote

#### Scenario: Structural guard rejects a content rewrite

- GIVEN the correction call returns text whose word count or word order
  differs from the original quote, or that alters a word the gate did not
  flag as risky beyond a diacritic-only restoration (e.g. `tenia` → `tenía`
  is allowed; `tenia` → `tuvo` is not)
- WHEN the structural guard checks the corrected text
- THEN `extract_lapidary_quote` returns `None` instead of the rewritten text

### Requirement: Diacritics Preservation Instruction in Pikzels Templates

The system MUST include a shared diacritics-preservation instruction in
every Pikzels thumbnail template used to render quote text, so accented
characters in a corrected or verbatim quote are not silently dropped by
image generation.

#### Scenario: All three layouts carry the instruction

- GIVEN `build_pikzels_prompt` renders a prompt using any of the three
  thumbnail templates
- WHEN the rendered prompt is inspected
- THEN it contains the shared diacritics-preservation instruction line
  regardless of which of the three templates was selected
