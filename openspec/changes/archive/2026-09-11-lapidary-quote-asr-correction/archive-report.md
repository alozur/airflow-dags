# Archive Report: lapidary-quote-asr-correction

**Change**: lapidary-quote-asr-correction  
**Issue**: #611  
**Archive date**: 2026-09-11  
**Archive location**: `openspec/changes/archive/2026-09-11-lapidary-quote-asr-correction/`  
**Artifact store mode**: hybrid (openspec + Engram)  

## Cycle Completion

This SDD cycle has been fully planned, implemented, verified, and archived. All phases are complete.

### Phase Completion Summary

| Phase | Artifact | Status | Notes |
|-------|----------|--------|-------|
| sdd-explore | explore.md | ✅ Complete | Scope and dependencies mapped |
| sdd-propose | proposal.md | ✅ Complete | Business case and approach approved |
| sdd-spec | specs/lapidary-quote-correction/spec.md | ✅ Complete | 4 requirements, 6 scenarios defined |
| sdd-design | design.md | ✅ Complete | Architectural decisions and implementation plan |
| sdd-tasks | tasks.md | ✅ Complete | 18 implementation tasks, all checked ✅ |
| sdd-apply | (implemented, PRs merged) | ✅ Complete | Work delivered in stacked PRs #616 + #617 to dev |
| sdd-verify | verify-report.md | ✅ PASS | 4/4 requirements, 6/6 scenarios, 0 blockers, 0 CRITICAL findings |
| sdd-archive | (this report) | ✅ Complete | Specs synced, change archived, cycle closed |

## Task Completion Gate

**Result**: ✅ PASS

All 18 implementation tasks in `tasks.md` are marked complete (`[x]`):
- Phase 1 (Gate): 2 tasks ✅
- Phase 2 (Structural Guard): 2 tasks ✅
- Phase 3 (Correction Call + Prompts): 4 tasks ✅
- Phase 4 (Wiring): 2 tasks ✅
- Phase 5 (Diacritics Prompt Line): 2 tasks ✅
- Phase 6 (Verification): 4 tasks ✅

No stale unchecked implementation tasks remain in the archived artifact.

## Verification Status

**Result**: ✅ PASS (per verify-report.md)

| Metric | Value |
|--------|-------|
| Verdict | PASS |
| Blockers | 0 |
| CRITICAL findings | 0 |
| Requirements | 4/4 compliant |
| Scenarios | 6/6 compliant |
| Test suite | 5621 passed, 34 skipped (pre-existing), 0 failed |
| Static analysis | All checks passed (ruff format, ruff check, C901 complexity ≤ 10) |
| Build verification | DagBag import successful, 18 DAGs loaded, 0 import errors |
| Docker e2e | Unavailable in this environment (not a failure per project convention) |

### Coverage of Regression Cases

Both branches of the pinned Aylan Kurdi regression are fully covered by dedicated regression tests:
- **Corrected path**: `test_risky_quote_gets_corrected_when_guard_passes` — verifies correction LLM call returns corrected text with confidence ≥0.8
- **Fallback path**: `test_risky_quote_falls_back_to_none_on_low_confidence` — verifies that low-confidence responses return `None` (never the risky uncorrected quote)

Both tests are green on the verified commit.

## Implementation Summary

**What was delivered**: A risky-entity gate, LLM-backed correction call, and structural guard that corrects ASR errors (misheard proper nouns, dropped diacritics) in lapidary thumbnail quotes. The system gates the correction behind a cheap risky-entity check, applies a narrow LLM correction only when needed, and falls back to the existing `art_direct` invented-text path on any validation failure. A diacritics-preservation instruction was added to all three Pikzels prompt templates.

**Files modified**:
- `congress_videos/modules/thumbnail_generation.py` — risky-entity gate, correction call, structural guard, wiring into `extract_lapidary_quote`
- `congress_videos/modules/thumbnail_prompt.py` — diacritics-preservation instruction in all 3 Pikzels templates
- `congress_videos/config/ai_prompts.py` — `LAPIDARY_CORRECTION_SYSTEM_PROMPT` and `_USER_TEMPLATE` constants

**Tests added**:
- `tests/congress_videos/modules/test_thumbnail_generation.py`:
  - `TestRiskyTokenIndices` — parametrized gate positives and negatives
  - `TestPassesCorrectionGuard` — 9 guard validation cases (1 accept-identical, 1 accept-mixed, 7 rejection variants)
  - `TestLapidaryCorrectionPrompts` — prompt content assertions
  - `TestRequestQuoteCorrection` — malformed output cases and valid high-confidence case
  - Two regression test cases in `TestExtractLapidaryQuote` (corrected path and fallback path)
- `tests/congress_videos/modules/test_thumbnail_prompt.py`:
  - `TestDiacriticsLine` — diacritics instruction presence, accent survival, defensive http-string check

**Total change size**: 614 changed lines (605 insertions + 9 deletions) in code and tests. This exceeded the 400-line review budget; per the orchestrator's plan, delivery was split into two stacked PRs (#616: gate + guard + diacritics, 327 lines; #617: correction call + wiring, 287 lines). Both PRs were merged into dev with ruff CI green.

## Delivery Status

**Code delivery**: ✅ Complete  
- **PR #616**: Gate + guard + diacritics line (327 changed lines), merged to dev
- **PR #617**: Correction call + wiring (287 changed lines), merged to dev
- **Combined tree**: Byte-identical to verified commit c3cbce7
- **Ruff CI**: Green (all checks passed)
- **Target branch**: dev
- **Status on main**: Not yet merged to main. Issue #611 remains open pending next dev→main release.

**Artifact delivery**: ✅ Complete  
- Main spec created: `openspec/specs/lapidary-quote-correction/spec.md`
- Change folder archived: `openspec/changes/archive/2026-09-11-lapidary-quote-asr-correction/`

## Design Decisions Implemented

The orchestrator amended the spec after design validation to clarify:
- **Diacritic-only restoration** is allowed on **any** word in the quote (not just flagged words)
- **Replacements** (word substitutions) are only allowed on tokens flagged by the risky-entity gate (e.g., "Aan" → "Aylan" where both are on the risky index)

This is reflected in:
- `_is_allowed_token_change` function (line 222-244): grants two paths — identical tokens (always ok) or diacritic-only restoration on ANY token via combining-mark analysis
- `_is_plausible_flagged_replacement` function (line 246-257): restricts word substitutions to flagged indices only; rejects digit mutations and implausible names

## Sources of Truth Updated

The following spec now reflects the new behavior:
- **`openspec/specs/lapidary-quote-correction/spec.md`**: 4 requirements, 6 scenarios covering the risky-entity gate, gated correction call, fail-soft fallback, and diacritics-preservation instruction in Pikzels templates

## Potential Risks

**None**: All 4 requirements and 6 scenarios are compliant. No CRITICAL findings. No blockers.

**Delivery note**: The code change size (614 lines) exceeded the 400-line review budget and was split into two stacked PRs for reviewer manageability. This is a delivery strategy concern, not a code quality issue. Both PRs are on dev; merging to main is pending the next scheduled dev→main release.

## Archive Artifacts

The archived change folder contains:
- ✅ `proposal.md` — business case and approach
- ✅ `explore.md` — scope and dependencies
- ✅ `specs/lapidary-quote-correction/spec.md` — 4 requirements, 6 scenarios
- ✅ `design.md` — implementation plan and design decisions
- ✅ `tasks.md` — 18 implementation tasks, all checked
- ✅ `verify-report.md` — verification evidence and verdict (PASS)
- ✅ `archive-report.md` — this archive report

## Traceability

**Engram observations** (hybrid mode):
- Archive report persisted to Engram topic `sdd/lapidary-quote-asr-correction/archive-report`

**Verification evidence** (from final-state facts):
- Evidence SHA256: `cc966fbba60984920d83095c661644e27b1f8ebbc75124ebbf86b439dc3d67d9`
- Implementation commit (verified): `c3cbce7`
- PR #616 squash commit: `e3c5a8c`
- PR #617 squash commit (merged to dev): `d49068f`

**Task completion**: 18/18 tasks marked complete in tasks.md, verified against implementation.

**Test results** (from verify-report.md):
- 5621 tests passed
- 34 pre-existing skips (Postgres/environment unavailable)
- 0 test failures
- Coverage: ✅ Above 80% threshold

## Next Steps

The change is complete and archived. No further SDD work is needed. The code is on dev, ready for the next scheduled dev→main release. Issue #611 will close when the code merges to main and a release is published.
