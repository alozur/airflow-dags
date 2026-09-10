# Tasks: Correct ASR errors in the lapidary thumbnail quote

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ≈273 (code + tests only; openspec docs excluded from this PR) |
| 400-line budget risk | Low |
| Chained PRs recommended | No |
| Suggested split | Single PR (code+tests) to `dev`; openspec folder ships later via a separate archive PR (precedent #555, #608, #610) |
| Delivery strategy | auto-chain |
| Chain strategy | n/a |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Gate + correction + guard + wiring + prompts, all tests green | Code PR → `dev` | `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py tests/congress_videos/modules/test_thumbnail_prompt.py` | `uv run python congress_videos/modules/thumbnail_generation.py` (module import) + local DagBag import check; Docker e2e conditional on touched paths | Single revert; no schema/state change |

## Phase 1: Gate (RED → GREEN)

- [x] 1.1 RED: add parametrized `TestRiskyTokenIndices` to `tests/congress_videos/modules/test_thumbnail_generation.py` for R1/R2/R3 positives (index sets from design table) and the 4 sentence-case negatives → `frozenset()`.
- [x] 1.2 GREEN: add `_bare`, `_RISKY_EXEMPT_WORDS`, `_risky_token_indices` to `congress_videos/modules/thumbnail_generation.py`.

## Phase 2: Structural Guard (RED → GREEN)

- [x] 2.1 RED: add `TestPassesCorrectionGuard` — accepted (`o==c`), diacritic-only restoration on **any** token incl. non-flagged (`tenia→tenía`), flagged replacement (`Aan→Aylan`, `Curdi→Kurdi`), and rejections: extra word, reorder, non-flagged non-diacritic change (`meses`), changed digit, implausible name (`Sánchez`), accent removed, over `max_chars`.
- [x] 2.2 GREEN: add `_strip_accents`, `_is_allowed_token_change`, `_passes_correction_guard` to `congress_videos/modules/thumbnail_generation.py` (guard rule 2 applies to any token; rule 3 only to flagged tokens).

## Phase 3: Correction Call + Prompts (RED → GREEN)

- [x] 3.1 RED: add `LAPIDARY_CORRECTION_SYSTEM_PROMPT` / `_USER_TEMPLATE` string-content assertions to a new `TestLapidaryCorrectionPrompts` in `tests/congress_videos/modules/test_thumbnail_generation.py`.
- [x] 3.2 GREEN: add both prompt constants to `congress_videos/config/ai_prompts.py`.
- [x] 3.3 RED: add `TestRequestQuoteCorrection` (parametrized malformed-output cases → `None`; valid high-confidence case → corrected text) using the injected `completion_fn` fake.
- [x] 3.4 GREEN: add `_request_quote_correction` (`LLM_CHEAP`, `parse_json_response`, `_LAPIDARY_CORRECTION_MIN_CONFIDENCE=0.8`, `_LAPIDARY_CONTEXT_RADIUS=800`) to `congress_videos/modules/thumbnail_generation.py`.

## Phase 4: Wiring (RED → GREEN)

- [x] 4.1 RED: add "Aylan corrected" (2 calls, corrected string returned) and "Aylan fallback" (confidence 0.4 → `None`) cases to `extract_lapidary_quote` tests; assert the 8 existing `TestExtractLapidaryQuote` tests are untouched and make exactly 1 `completion_fn` call.
- [x] 4.2 GREEN: wire `_risky_token_indices` → `_request_quote_correction` → `_passes_correction_guard` into `extract_lapidary_quote`; update its docstring.

## Phase 5: Diacritics Prompt Line (RED → GREEN)

- [x] 5.1 RED: add `TestDiacriticsLine` to `tests/congress_videos/modules/test_thumbnail_prompt.py` — line present in A/B/C output, `TENÍA` survives `.upper()`, no `http` in the line.
- [x] 5.2 GREEN: add `_DIACRITICS_LINE` and the `diacritics_line` format kwarg to `congress_videos/modules/thumbnail_prompt.py`; insert into `_TEMPLATE_A/B/C`.

## Phase 6: Verification

- [x] 6.1 Run `uv run pytest -n auto` (full suite). Result: 5621 passed, 34 skipped (pre-existing Postgres-unavailable/env skips, unrelated to this change).
- [x] 6.2 Run `uv run ruff check` and `uv run ruff format --check` on the four touched files; confirm every new helper's mccabe complexity ≤ 10 (no C901 ignore added). Result: all pass; `--select C901` explicit run also passes.
- [x] 6.3 Local DagBag import check (`python congress_videos/modules/thumbnail_generation.py` or DAG-level import smoke) since Docker e2e may be unavailable in this environment. Result: `DagBag('congress_videos')` loads 18 DAGs, 0 import errors.
- [x] 6.4 Confirm openspec change folder stays untracked/unstaged in the code PR diff. Result: confirmed — `openspec/` remains untracked in `git status`.
