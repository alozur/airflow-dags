# Archive Report: copy-evidence-name-split-coverage

**Change**: copy-evidence-name-split-coverage (issue #544)  
**Archive Date**: 2026-09-10  
**Archive Path**: `openspec/changes/archive/2026-09-10-copy-evidence-name-split-coverage/`  
**Base**: `main` @ 56a8180 (2026-09-09)  
**Branch**: `feat/544-copy-evidence-name-split` @ 423eb63  

## Executive Summary

This test-only SDD cycle established full direct-call coverage of the canonical/raw display-name split in `_copy_verification_evidence` — a documented contract that lived only in an archived design doc with zero passing tests. The cycle added 8 passing tests (all green on first execution with falsifiability proven via mutation checks), merged the new contract into the main spec, and confirmed zero production file changes. All 13 tasks complete; verify report issued PASS WITH WARNINGS (one documentation-completeness gap, not a functional defect).

## Cycle Completion Status

| Metric | Value | Notes |
|--------|-------|-------|
| **Tasks Complete** | 13/13 | All phases (1–5) executed; every task marked [x] |
| **Tests Added** | 8 | Passing; repo-wide baseline 5200 → 5208 (exactly +8) |
| **Production Files Changed** | 0 | Hard invariant: `git diff --name-only origin/main...HEAD` shows only `tests/` and `openspec/` |
| **Spec Sync** | 2 reqs, 5 scenarios merged | Delta appended to main spec at `openspec/specs/final-copy-verification/spec.md` |
| **Verify Verdict** | PASS WITH WARNINGS | Final finding counts: 0 CRITICAL, 0 BLOCKERS, 1 SUGGESTION |

## Final-State Facts (Per Orchestrator Launch Prompt)

The following facts override any stale claims in `apply-progress.md` or `verify-report.md`:

1. **The single `sdd-verify` WARNING is CLOSED**: Task 4.2's mutation-check row was missing from `apply-progress.md`, so its falsifiability was asserted rather than demonstrated. The orchestrator closed this gap in commit 423eb63 with an isolating mutation — making the `"party"` key conditional on a resolved participant in `congress_videos/youtube_upload_dag.py`, which can only manifest on the unresolved-speaker path. Result: `test_both_helpers_emit_identical_bundle_shape` **passed** while `test_unresolved_speaker_still_yields_matching_bundle_shape` **failed**, proving 4.2 catches a divergence 4.1 cannot see. The production file was restored and the tree verified clean before commit.

2. **Final numbers (orchestrator-verified, 2026-09-10 02:45 UTC)**:
   - `uv run pytest -q` → **5208 passed, 34 skipped**, exit 0
   - `uv run ruff check .` → All checks passed
   - Baseline on `main`: 5200 passed, 34 skipped; delta = exactly +8 tests
   - No regressions

3. **Hard invariant holds**: `git diff --name-only origin/main...HEAD` shows ONLY:
   - `openspec/changes/copy-evidence-name-split-coverage/` (now archived)
   - `tests/congress_videos/test_youtube_upload_dag.py`
   - `tests/congress_videos/test_reap_uploader_dag.py`
   
   **Zero `congress_videos/**` production files changed.** Independently confirmed by the orchestrator after the mutation checks.

4. **13/13 tasks complete**; 2 requirements (5 scenarios) traced to named passing tests.

5. **Single PR, no chaining** — ~299 added test lines, comfortably inside the 400-line review budget. The branch has NOT yet been merged to `dev`/`main`; the orchestrator handles delivery after archive.

6. **Framing correction recorded** (critical learning from exploration):
   - Issue #544 claimed a swapped `display_name`/`short_name` would make the verifier "silently reason over the wrong ground truth"
   - **Actual failure mode is stronger than the code supports**: `_flatten_evidence_strings` is key-agnostic, so a swap yields an identical token allowlist and would very likely not change today's verdicts
   - **Real failure mode is documented-contract drift with no test to catch it**, and the contract lived ONLY in an archived design doc (`2026-09-09-verify-final-copy-before-publication/design.md` D5)
   - **Promotion into the main spec is the point** of this cycle — moving the contract from ephemeral archive into durable specification

7. **Two traps caught by design** (documented in design.md D1–D4, independently verified by verify agent):
   - **Trap 1 (D1)**: Sentinel vs. real catalogue. Tests patch `canonical_display_name` with unmistakable, non-substring sentinels (`"RAW Foo"` vs. `"CANON-X"`), not real curated values. Real catalogue has `"Pedro Sánchez Pérez-Castejón"` → `"Sánchez"`, where canonical IS a substring of raw; key-placement proofs need sentinel values.
   - **Trap 2 (D4)**: `mencionados` tri-valued fixture. Parity/mentioned-entry fixtures use **two non-empty** mentioned slugs, never `None`/`[]`. Both the None and `[]` forms of `mencionados` compare equal trivially (the first a leaf → `None`, the second an empty list → `[]` → `None` via key-shape), so the populated dict-list form is mandatory to prove anything.

## Spec Merge Summary

**Main Spec Updated**: `openspec/specs/final-copy-verification/spec.md`

**Merge Action**: APPEND (delta has no MODIFIED/REMOVED/RENAMED sections, only ADDED)

**Sections Merged**:
- Requirement: Speaker Evidence Keeps Raw and Canonical Names Distinct (3 scenarios)
- Requirement: Evidence Bundle Shape Parity Across Upload Paths (2 scenarios)

**Pre-Merge Requirements Count: 8 (Independent Verification Call, Verdict Schema, Bounded Correction, Party Mismatch Detection, Thumbnail Text Flagged, Hard-Rejection Asymmetry, Fallback on Unavailable Verification, Audit Persistence)

**Post-Merge Requirements Count: 10 (8 pre-existing + 2 new)

**Pre-Merge Scenarios Count**: 15

**Post-Merge Scenarios Count**: 20

**Merge Verification**: Appended sections are verbatim from delta spec; no truncation or alteration. Main spec now contains all pre-existing requirements plus the two new requirements covering the canonical/raw split.

## Artifacts Archived

**Archive Location**: `openspec/changes/archive/2026-09-10-copy-evidence-name-split-coverage/`

**Archived Files**:
- ✅ `proposal.md` — intent, scope, approach, rollback
- ✅ `exploration.md` — exploration context (if present in source)
- ✅ `specs/final-copy-verification/spec.md` — delta spec (ADDED Requirements only)
- ✅ `design.md` — architecture decisions (D1–D5), testing strategy, threat matrix
- ✅ `tasks.md` — 13 tasks, all [x], all complete, traceability matrix
- ✅ `apply-progress.md` — implementation record, mutation-check evidence (7 of 8 groups)
- ✅ `verify-report.md` — verification envelope, spec compliance, trap avoidance matrix
- ✅ `archive-report.md` — this file (additive, written post-move)

**Directory Structure Preserved**: Move was mechanical (`git mv`); directory layout unchanged.

## Verify Report Summary (Per `verify-report.md`)

- **Verdict**: PASS WITH WARNINGS
- **Evidence Revision**: sha256:c275dabeb71d21577690bc92cfb91a9e4e463613f898dbb4beb3be8949edfc38
- **Blockers**: 0
- **Critical Findings**: 0
- **Requirements Covered**: 2/2
- **Scenarios Covered**: 5/5
- **Test Execution**: 5208 passed, 34 skipped (baseline +8)
- **Build**: ruff check and format pass

**Findings Detail**:
- **WARNING** (as recorded by `sdd-verify`, since **CLOSED** — see "Final state" above): Task 4.2 mutation-check row missing from `apply-progress.md` (documentation gap, not functional). Closed in commit `423eb63` by an isolating mutation that demonstrated 4.2's falsifiability. **Final counts at close: 0 CRITICAL, 0 WARNING, 1 SUGGESTION.**
- **SUGGESTION**: Line number citations in `tasks.md` (1152 → 1350 after insertions) will drift; cite by symbol name in future docs

## Traceability: Spec to Tests

| Requirement | Scenario | Test | File | Status |
|---|---|---|---|---|
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Resolvable slug | `test_resolvable_slug_splits_raw_and_canonical` | youtube_upload_dag.py, reap_uploader_dag.py | ✅ PASS |
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Unmapped slug | `test_unmapped_slug_keeps_raw_and_nulls_canonical` | youtube_upload_dag.py, reap_uploader_dag.py | ✅ PASS |
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Mentioned entries | `test_mentioned_entries_split_raw_and_canonical` | youtube_upload_dag.py, reap_uploader_dag.py | ✅ PASS |
| Evidence Bundle Shape Parity Across Upload Paths | Equivalent inputs | `test_both_helpers_emit_identical_bundle_shape` | reap_uploader_dag.py | ✅ PASS |
| Evidence Bundle Shape Parity Across Upload Paths | Unresolved speaker | `test_unresolved_speaker_still_yields_matching_bundle_shape` | reap_uploader_dag.py | ✅ PASS |

## Archive Verification

**Mechanical Move Validation**:
- Source directory removed: ✅
- Destination created with all files: ✅
- `diff -r` snapshot vs. archived: ✅ EMPTY (no byte differences)
- Main spec merged and verified: ✅ PASS (new sections present, pre-existing preserved)

**Readback Output**:
```
(empty — indicating perfect byte-identity between snapshot and archived tree)
```

## Key Learnings

1. Documented contracts that live only in archived design docs can drift unnoticed, even with zero functional impact today; promoting them into the main spec anchors them durably.

2. Sentinel values in test doubles (non-substring pairs) prevent accidental swap failures that real-catalogue bindings might not catch.

3. Tri-valued data structures (None vs. `[]` vs. populated) require explicit fixture shapes in parity tests; collapsing equivalences silently pass trivial comparisons.

4. Falsifiability via mutation checks (RED-then-mutation-prove-fail) is a legitimate substitute for RED-first TDD when the implementation pre-exists and is known to be correct, provided mutation evidence is real and complete.

5. Cross-seam parity testing with key-shape comparison (leaves discarded, structure only) avoids value coupling while catching architecture drift.

## Handoff Notes

- **No migration required**: test-only change, zero production surface.
- **Rollback**: delete the two new test classes in `test_youtube_upload_dag.py` and `test_reap_uploader_dag.py`.
- **Next phase**: orchestrator merges branch to `dev`/`main` per delivery strategy.
- **Future work**: #545, #546, etc. can rely on the now-specified contract to avoid drift in their own evidence bundles.

