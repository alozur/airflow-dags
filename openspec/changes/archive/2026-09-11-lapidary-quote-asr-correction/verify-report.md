```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:cc966fbba60984920d83095c661644e27b1f8ebbc75124ebbf86b439dc3d67d9
verdict: pass
blockers: 0
critical_findings: 0
requirements: 4/4
scenarios: 6/6
test_command: uv run pytest -n auto -q
test_exit_code: 0
test_output_hash: sha256:306dd54d1a124b200b5dbab6d44b22f808a786812c159d0c6df72074e7a23ea2
build_command: uv run python -c "from airflow.models import DagBag; db = DagBag(dag_folder='congress_videos', include_examples=False); print('DAGs loaded:', len(db.dags)); print('Import errors:', db.import_errors)"
build_exit_code: 0
build_output_hash: sha256:4127547489cc2ebd9c9d850322b9f366b7f16a6e18001fcad95ce08ffc9e2f5f
```

## Verification Report

**Change**: lapidary-quote-asr-correction
**Version**: N/A (single-version spec)
**Mode**: Strict TDD

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total | 18 |
| Tasks complete | 18 |
| Tasks incomplete | 0 |

### Build & Tests Execution
**Build** (DagBag import check): ✅ Passed
```text
$ uv run python -c "from airflow.models import DagBag; db = DagBag(dag_folder='congress_videos', include_examples=False); print('DAGs loaded:', len(db.dags)); print('Import errors:', db.import_errors)"
DAGs loaded: 18
Import errors: {}
```

**Tests**: ✅ 5621 passed / ❌ 0 failed / ⚠️ 34 skipped
```text
$ uv run pytest -n auto -q
5621 passed, 34 skipped in 98.82s (0:01:38)
```
All 34 skips are pre-existing Postgres-unavailable / environment skips (`test_speaker_turns_chapter_order_live.py`, `sql/test_migration_029.py`, one SRT-size guard skip in `test_srt_helpers.py`), unrelated to this change and present on the base commit's test environment.

**Coverage**: aggregate suite run enforces `--cov-fail-under=80` (pyproject.toml addopts) and exited 0 with no coverage failure banner → ✅ Above threshold.

**Static analysis** (all pass, exit 0):
```text
$ uv run ruff check congress_videos tests utils
All checks passed!

$ uv run ruff format --check congress_videos/config/ai_prompts.py congress_videos/modules/thumbnail_generation.py \
  congress_videos/modules/thumbnail_prompt.py tests/congress_videos/modules/test_thumbnail_generation.py \
  tests/congress_videos/modules/test_thumbnail_prompt.py
5 files already formatted

$ uv run ruff check --select C901 congress_videos/config/ai_prompts.py congress_videos/modules/thumbnail_generation.py \
  congress_videos/modules/thumbnail_prompt.py
All checks passed!
```

**Docker e2e** (`scripts/test-airflow-e2e.sh`): ➖ unavailable — `docker info` failed in this environment. Not run; not a failure per project convention. Run manually before merge.

### Spec Compliance Matrix
| Requirement | Scenario | Test | Result |
|-------------|----------|------|--------|
| Risky-Entity Gate Runs Before Any Correction Call | Happy-path quote makes no extra LLM call | `test_thumbnail_generation.py::TestExtractLapidaryQuote::test_stop_word_filtered_only_valid_candidate_passed_to_llm` (asserts `len(received_calls) == 1`) + `::test_happy_path_returns_verbatim_candidate` (byte-identical verbatim return) | ✅ COMPLIANT |
| Risky-Entity Gate Runs Before Any Correction Call | A risky token enters the correction path | `test_thumbnail_generation.py::TestExtractLapidaryQuote::test_risky_quote_gets_corrected_when_guard_passes` (asserts `len(calls) == 2`, ranking then correction) | ✅ COMPLIANT |
| Gated Correction Fixes a Flagged Quote | Misheard proper noun is corrected | `test_thumbnail_generation.py::TestExtractLapidaryQuote::test_risky_quote_gets_corrected_when_guard_passes` ("Aan Curdi" → "Aylan Kurdi", corrected-path regression) | ✅ COMPLIANT |
| Fail-Soft Fallback to None | Unusable correction output is dropped | `test_thumbnail_generation.py::TestRequestQuoteCorrection::test_malformed_or_missing_output_returns_none` (8 parametrized malformed/empty/error cases) + `TestExtractLapidaryQuote::test_risky_quote_falls_back_to_none_on_low_confidence` (Aylan fallback-path regression, confidence 0.4) | ✅ COMPLIANT |
| Fail-Soft Fallback to None | Structural guard rejects a content rewrite | `test_thumbnail_generation.py::TestPassesCorrectionGuard::{test_extra_word_rejected, test_reordered_tokens_rejected, test_non_flagged_non_diacritic_change_rejected, test_changed_digit_rejected, test_implausible_name_rejected, test_accent_removed_rejected, test_over_max_chars_rejected}` | ✅ COMPLIANT |
| Diacritics Preservation Instruction in Pikzels Templates | All three layouts carry the instruction | `test_thumbnail_prompt.py::TestDiacriticsLine::{test_diacritics_line_present_layout_a, _b, _c, test_diacritics_line_mentions_accented_letters, test_accented_text_survives_uppercasing, test_diacritics_line_contains_no_http}` | ✅ COMPLIANT |

**Compliance summary**: 6/6 scenarios compliant

### Aylan Kurdi Regression Coverage
Both branches of the pinned regression are covered by dedicated tests, both green:
- **Corrected path**: `test_risky_quote_gets_corrected_when_guard_passes` — fragment `"Aan Curdi tenía 3 años, una camisa roja"`, correction call returns `{"corrected": "Aylan Kurdi tenía 3 años", "confidence": 0.95}`, asserts the returned string equals the corrected quote and exactly 2 `completion_fn` calls (ranking, correction).
- **Fallback path**: `test_risky_quote_falls_back_to_none_on_low_confidence` — same fragment, correction call returns confidence `0.4` (below the `0.8` threshold), asserts the function returns `None` (never the risky uncorrected quote).

### Happy-Path Byte-Identity and Zero-Extra-Call Verification
- `extract_lapidary_quote` source (`congress_videos/modules/thumbnail_generation.py:470-473`): when `_risky_token_indices(quote)` returns an empty `frozenset`, the function returns `quote` immediately — no call to `_request_quote_correction`, hence no second `completion_fn` invocation.
- `git diff 7f4cd5a c3cbce7 -- tests/congress_videos/modules/test_thumbnail_generation.py` contains **zero deletions** (267 insertions, 0 deletions) — all 8 pre-existing `TestExtractLapidaryQuote` tests are byte-identical/untouched and still pass, confirming the happy path is unchanged.
- `test_stop_word_filtered_only_valid_candidate_passed_to_llm` explicitly asserts `len(received_calls) == 1` for a non-risky quote, directly proving zero extra LLM calls on the happy path.

### Correctness (Static Evidence)
| Requirement | Status | Notes |
|------------|--------|-------|
| Risky-entity gate (R1/R2/R3) | ✅ Implemented | `_risky_token_indices` (thumbnail_generation.py:142-167) matches design.md rules exactly: R1 digit token, R2 mid-clause capitalized non-exempt, R3 first-token name-run opener. `_RISKY_EXEMPT_WORDS` matches the design's exempt set verbatim. |
| Gated correction call (LLM_CHEAP) | ✅ Implemented | `_request_quote_correction` (thumbnail_generation.py:306-342) calls `completion_fn` with `model=LLM_CHEAP`; uses `parse_json_response`; `_LAPIDARY_CORRECTION_MIN_CONFIDENCE = 0.8` (line 261); `_LAPIDARY_CONTEXT_RADIUS = 800` (line 264). |
| Confidence/schema validation | ✅ Implemented | `_valid_correction_text` (thumbnail_generation.py:282-303) checks non-blank `corrected` str, numeric non-bool `confidence` in `[0,1]`, and the `>= 0.8` gate — every failure path logs `logger.warning` and returns `None`. |
| Structural guard (3 rules) | ✅ Implemented | `_passes_correction_guard`/`_is_allowed_token_change`/`_is_plausible_flagged_replacement` (thumbnail_generation.py:203-257): identical tokens, diacritic-only restoration on ANY token (marks never removed, `_count_combining_marks` comparison), and SequenceMatcher-similarity replacement restricted to flagged tokens only. Matches design.md exactly, including the digit-immutability rule. |
| Fail-soft, never raises | ✅ Implemented | `generate_chat_completion` (utils/ai_helpers.py) and `parse_json_response` both catch all exceptions internally and return `{content: None, error: ...}` / `{data: None, error: ...}` — never propagate. `extract_lapidary_quote`'s new gate/correction/guard chain has no unguarded raise path; every failure resolves to `None` and falls through to the existing `art_direct` invented-text path (`_finalize_brief`, unchanged). |
| Diacritics-preservation line in all 3 templates | ✅ Implemented | `_DIACRITICS_LINE` (thumbnail_prompt.py) inserted via `{diacritics_line}` kwarg in `_TEMPLATE_A/B/C` and passed in `build_pikzels_prompt`; contains no `http` substring (defensive strip elsewhere would otherwise corrupt it). |
| Public signature unchanged | ✅ Implemented | `extract_lapidary_quote(srt_fragment, max_chars=40, min_words=3, max_words=8, completion_fn=None)` — identical signature to the pre-change function; docstring updated to describe the new gate/correction/guard chain. |

### Coherence (Design)
| Decision | Followed? | Notes |
|----------|-----------|-------|
| Reuse `completion_fn` seam (no new param) | ✅ Yes | `_request_quote_correction` takes the same injected `completion_fn` used by the ranking call; no new DI parameter added to `extract_lapidary_quote`. |
| Tier = `LLM_CHEAP` (not `LLM_DEFAULT`) | ✅ Yes | Confirmed at call site and via `test_uses_llm_cheap_tier_and_sends_flagged_words_and_context`. |
| Digit tokens immutable (gate-only, never corrected) | ✅ Yes | `_is_plausible_flagged_replacement` rejects any pair where either token contains a digit; `test_changed_digit_rejected` confirms a changed digit on a flagged index is rejected. |
| No caching (`cached_json_completion` rejected) | ✅ Yes | `_request_quote_correction` calls `completion_fn` directly each time; no cache wrapper imported or used. |
| Context = quote ± 800 chars of `srt_fragment` | ✅ Yes | `_context_window` implements the exact fallback-to-full-fragment behavior described in design.md; `_LAPIDARY_CONTEXT_RADIUS = 800`. |
| mccabe complexity ≤ 10, no per-file C901 ignore | ✅ Yes | `uv run ruff check --select C901` on all 3 touched production files passes with zero findings; no ignore comment added (`git diff` shows no `# noqa` or ruff config change). |
| 400-line PR budget — openspec docs excluded from code PR | ⚠️ Delivery concern, not a spec failure | Code+tests diff (`git diff --stat 7f4cd5a c3cbce7`) is **614 changed lines** (605 insertions + 9 deletions), 2.25× the tasks.md estimate of ≈273 and over the 400-line budget on its own — before the openspec folder is even added. tasks.md declares this a single indivisible work unit (one row in "Suggested Work Units") with `Chained PRs recommended: No`. Per the orchestrator's stated plan, delivery will be split into two stacked PRs to fit the review budget. This is a delivery-strategy decision, not a spec or design compliance failure, and does not affect the verdict below. |

### TDD Compliance
| Check | Result | Details |
|-------|--------|---------|
| TDD Evidence reported | ✅ | Found in Engram `sdd/lapidary-quote-asr-correction/apply-progress` (obs #2889) — 18/18 tasks, RED→GREEN per phase, no REFACTOR needed. |
| All tasks have tests | ✅ | 18/18 tasks map to test additions (Phases 1-5) or verification commands (Phase 6). |
| RED confirmed (tests exist) | ✅ | All test classes named in tasks.md (`TestRiskyTokenIndices`, `TestPassesCorrectionGuard`, `TestLapidaryCorrectionPrompts`, `TestRequestQuoteCorrection`, 2 new `TestExtractLapidaryQuote` cases, `TestDiacriticsLine`) exist in the diff. |
| GREEN confirmed (tests pass) | ✅ | 5621/5621 runnable tests pass on this run, including every test named above. |
| Triangulation adequate | ✅ | Gate: 4 positive + 4 negative parametrized cases. Guard: 9 cases (1 accept-identical, 1 accept-mixed, 7 rejection variants). Correction call: 8 malformed-input cases + 1 valid + 1 tier/context-capture case. |
| Safety Net for modified files | ✅ | `thumbnail_generation.py`, `ai_prompts.py`, `thumbnail_prompt.py` are all pre-existing modified files; the full pre-existing suite (5621 tests minus new ones) passes alongside the new tests, confirming no regression. |

**TDD Compliance**: 6/6 checks passed

### Assertion Quality
No tautologies, ghost loops, unguarded-empty-loop assertions, or production-code-free assertions found in the new/modified test code (`TestRiskyTokenIndices`, `TestPassesCorrectionGuard`, `TestLapidaryCorrectionPrompts`, `TestRequestQuoteCorrection`, `TestExtractLapidaryQuote` additions, `TestDiacriticsLine`). Every parametrized case asserts a distinct expected value (different index sets, different `None`/text outcomes, different rejection reasons) rather than repeating the same trivial expectation. Fakes are hand-written closures/functions, not `mocker.patch`-heavy — mock-to-assertion ratio is not a concern in the new tests.

**Assertion quality**: ✅ All assertions verify real behavior

### Issues Found
**CRITICAL**: None
**WARNING**: None
**SUGGESTION**:
- The 614-line diff (605+/9-) is a `size:exception` candidate against the 400-line review budget; the orchestrator's plan to split delivery into two stacked PRs addresses this at the delivery layer. No code change is required for this to close cleanly.

### Verdict
PASS
