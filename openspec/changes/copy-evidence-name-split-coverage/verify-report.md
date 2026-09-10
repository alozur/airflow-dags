```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:c275dabeb71d21577690bc92cfb91a9e4e463613f898dbb4beb3be8949edfc38
verdict: pass_with_warnings
blockers: 0
critical_findings: 0
requirements: 2/2
scenarios: 5/5
test_command: uv run pytest --no-cov -q
test_exit_code: 0
test_output_hash: sha256:2b6c8fea8eb974d5793805ada8186611db293792d7bd2bfba306291873d5cae4
build_command: uv run ruff check . && uv run ruff format --check .
build_exit_code: 0
build_output_hash: sha256:fe0e951d4e350981fb98b9cea5b8aa730374bae5d019d585c85640e7fc6809d1
```

## Verification Report

**Change**: copy-evidence-name-split-coverage (issue #544)
**Version**: N/A (single-version spec, `final-copy-verification` capability)
**Mode**: Strict TDD (test-only change; zero production edits in the final diff)

### Completeness

| Metric | Value |
|--------|-------|
| Tasks total | 13 |
| Tasks complete | 13 |
| Tasks incomplete | 0 |

All 13 tasks across Phases 1-5 verified as checked in `tasks.md` and confirmed against the actual diff.

### Hard Invariant: Diff Scope

`git diff --name-only origin/main...HEAD` shows exactly:

```
openspec/changes/copy-evidence-name-split-coverage/apply-progress.md
openspec/changes/copy-evidence-name-split-coverage/design.md
openspec/changes/copy-evidence-name-split-coverage/exploration.md
openspec/changes/copy-evidence-name-split-coverage/proposal.md
openspec/changes/copy-evidence-name-split-coverage/specs/final-copy-verification/spec.md
openspec/changes/copy-evidence-name-split-coverage/tasks.md
tests/congress_videos/test_reap_uploader_dag.py
tests/congress_videos/test_youtube_upload_dag.py
```

Zero `congress_videos/**` paths. `git status --short` is clean (no uncommitted mutation leftovers). The
apply agent's claim that its mutation-checking edits to production files were fully reverted is confirmed
independently — the working tree was clean before this verification began and remained clean after this
verify agent independently re-ran three of those same mutations (see below) and reverted each via
`git checkout --`.

### Build & Tests Execution

**Build**: PASSED
```text
$ uv run ruff check .
All checks passed!
$ uv run ruff format --check .
320 files already formatted
```

**Tests**: 5208 passed / 0 failed / 34 skipped (repo-wide)
```text
$ uv run pytest --no-cov -q
5208 passed, 34 skipped in 40.20s
```

Baseline on `main` (per orchestrator brief): 5200 passed, 34 skipped. Delta: +8 passed, matching exactly the
8 new tests this change adds. No regressions, no new skips.

Focused run (the 8 new tests only, independently executed by this verify agent):
```text
$ uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/test_reap_uploader_dag.py -k "CopyVerificationEvidence" -v --no-cov
8 passed, 315 deselected in 4.31s
```

**Coverage**: not separately measured for this focused change (repo-wide coverage gate not part of this
verification's scope); all new tests execute real, non-mocked-away code paths in the target functions.

### Spec Compliance Matrix

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Resolvable slug keeps raw and canonical names apart | `test_youtube_upload_dag.py::TestCopyVerificationEvidenceNameSplit::test_resolvable_slug_splits_raw_and_canonical`; `test_reap_uploader_dag.py::TestCopyVerificationEvidenceNameSplit::test_resolvable_slug_splits_raw_and_canonical` | COMPLIANT |
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Unmapped slug does not conflate the two fields | `test_youtube_upload_dag.py::TestCopyVerificationEvidenceNameSplit::test_unmapped_slug_keeps_raw_and_nulls_canonical`; `test_reap_uploader_dag.py::TestCopyVerificationEvidenceNameSplit::test_unmapped_slug_keeps_raw_and_nulls_canonical` | COMPLIANT |
| Speaker Evidence Keeps Raw and Canonical Names Distinct | Mentioned-person entries follow the same pairing | `test_youtube_upload_dag.py::TestCopyVerificationEvidenceNameSplit::test_mentioned_entries_split_raw_and_canonical`; `test_reap_uploader_dag.py::TestCopyVerificationEvidenceNameSplit::test_mentioned_entries_split_raw_and_canonical` | COMPLIANT |
| Evidence Bundle Shape Parity Across Upload Paths | Equivalent inputs yield matching bundle shape | `test_reap_uploader_dag.py::TestCopyVerificationEvidenceShapeParity::test_both_helpers_emit_identical_bundle_shape` | COMPLIANT |
| Evidence Bundle Shape Parity Across Upload Paths | Unresolved speaker still yields matching bundle shape | `test_reap_uploader_dag.py::TestCopyVerificationEvidenceShapeParity::test_unresolved_speaker_still_yields_matching_bundle_shape` | COMPLIANT |

**Compliance summary**: 5/5 scenarios compliant, 2/2 requirements fully covered.

### Trap Avoidance — Independently Re-Verified From Source, Not From apply-progress.md's Word

| # | Trap | Claim | Independent finding |
|---|------|-------|----------------------|
| 1 | Sentinel vs. real catalogue | Tests patch `canonical_display_name` with an unmistakable, non-substring sentinel (`"RAW Foo"` / `"CANON-X"`) instead of real curated-catalogue values | **Confirmed** by reading `test_youtube_upload_dag.py:1041-1049` and the mirrored reap block — both use `"RAW Foo"`/`"CANON-X"`, never a real roster string. No substring relationship. |
| 2 | `mencionados` tri-valued fixture | Parity/mentioned-entry fixtures use **two non-empty** mentioned slugs, never `None`/`[]` | **Confirmed**. Every `mentioned_entries_split_raw_and_canonical` test and both `TestCopyVerificationEvidenceShapeParity` cases that need a populated list use `["mentioned-a", "mentioned-b"]`. The one case using `None` (`test_unresolved_speaker_still_yields_matching_bundle_shape`) is explicitly the tri-valued "no analizado" leaf case the spec itself names as a distinct scenario, not a stand-in for the populated-list assertion. |
| 3 | Roster-keyed `side_effect`, not flat `return_value` | `lookup_participant_by_slug` is patched with a roster-keyed callable so distinct slugs resolve to distinct participants | **Confirmed** by source (`_lookup_stub(roster)` used as `side_effect=` in every mentioned-entry and parity test). **Independently re-verified by mutation**: temporarily replaced the roster-keyed `side_effect` with a flat `return_value=roster["speaker-slug"]` / `canonical["speaker-slug"]` in `test_mentioned_entries_split_raw_and_canonical` (youtube_upload_dag) — test failed exactly as apply-progress claims (`assert 'RAW Speaker' == 'RAW Mentioned A'`). Reverted via `git checkout --`; `git status --short` clean afterward. |
| 4 | Key-shape-only parity, no value/`inspect.getsource()` coupling | `_key_shape` recursively collapses leaves to `None` and compares only key structure | **Confirmed** by reading `_key_shape` (`test_reap_uploader_dag.py:1157-1168`) — leaves collapse to `None`, dict keys sorted, list length preserved, no value comparison anywhere, no `inspect` import in either test file. **Independently re-verified by mutation**: dropped `long_form["speaker"]["slug"]` before the `_key_shape` comparison in `test_both_helpers_emit_identical_bundle_shape` — test failed exactly as claimed (dict mismatch on the `speaker` sub-shape). Reverted via `git checkout --`. |

All four traps are genuinely avoided, not merely claimed. Traps #3 and #4 were independently re-mutated by
this verify agent (not just re-read from apply-progress.md) and reproduced the exact failure apply-progress
reported. Trap #2's production-file mutation (`short_name` falling back to `display_name`) was also
independently re-run against `congress_videos/youtube_upload_dag.py:668` and reproduced apply-progress's
exact failure (`assert 'RAW Foo' is None`) before being reverted.

### Falsifiability / Mutation-Check Assessment

Apply-progress claims "all 8 tests passed on first execution" with falsifiability established via mutation
checks rather than a conventional RED-first cycle, on the grounds that no implementation gap existed (both
target functions pre-existed, plain and already correct). This is a legitimate substitute for RED-first TDD
**only if every assertion group's mutation evidence is real and complete**. Assessment per assertion group:

| Task | Mutation-check row present in apply-progress.md? | Independently re-verified by this agent? |
|------|-----------------------------------------------------|--------------------------------------------|
| 2.1 / 3.1 (sentinel swap) | Yes | No (mechanism is a trivial literal-value swap; logically self-evident, low risk) |
| 2.2 / 3.2 (production fallback-conflation) | Yes | **Yes** — reproduced independently against `youtube_upload_dag.py` |
| 2.3 / 3.3 (roster collapse) | Yes | **Yes** — reproduced independently against `test_youtube_upload_dag.py` |
| 4.1 (dropped key) | Yes | **Yes** — reproduced independently against `test_reap_uploader_dag.py` |
| 4.2 (unresolved-speaker parity) | **No — missing row** | Not independently mutated (see WARNING below) |

**Finding (WARNING)**: `tasks.md` did not require an explicit mutation check for task 4.2 (only 4.1 carries
a "Mutation check:" instruction in the task text), so apply-progress's omission is not a violation of the
task's own contract. However, apply-progress's summary sentence ("Each mutation was applied … before
accepting each assertion group") slightly overstates the evidence: 7 of 8 assertion groups have a mutation
row, one (4.2) does not. This is low-risk in practice — 4.2 reuses the identical `_key_shape` comparison
mechanism already proven falsifiable by 4.1, and this verify agent confirmed by direct source reading that
both `_copy_verification_evidence` implementations are structurally byte-identical in their unresolved-slug
branch (same key names, same tri-valued `mencionados` handling) — but it is a real, if narrow, gap in the
"falsifiability proven for each assertion group" claim as literally stated.

### Correctness (Static Evidence)

| Requirement | Status | Notes |
|---|---|---|
| Raw/canonical distinct fields (`display_name`/`short_name`) | Implemented | Confirmed identical in both `congress_videos/youtube_upload_dag.py:664-673` and `congress_videos/reap_shorts_uploader_dag.py:261-270` — pre-existing, unchanged by this test-only PR. |
| `mencionados` per-entry raw/canonical split | Implemented | `youtube_upload_dag.py:655-662` / `reap_shorts_uploader_dag.py:252-259`, both unchanged. |
| Bundle shape parity between the two builders | Implemented | Both functions produce byte-identical key sets (`speaker` 7 keys, `chapter` 8 keys, `mencionados` entry 4 keys) by direct source inspection; now covered by an explicit test that would fail on future drift. |

### Coherence (Design)

| Decision | Followed? | Notes |
|---|---|---|
| D1 — distinguishable sentinels, not real catalogue | Yes | `"RAW Foo"`/`"CANON-X"` used throughout, confirmed by source read. |
| D2 — roster-keyed stub defined per test file, no cross-module import | Yes, with documented deviation | `test_reap_uploader_dag.py` reused the pre-existing `_lookup_stub` (originally at line 1152 on `main`, now at line 1350 after this PR's insertions) instead of redefining it — deviation explicitly logged in both `tasks.md` and `apply-progress.md`, and it satisfies D2's actual intent (no cross-test-module import, no shared conftest roster). |
| D3 — parity by input equivalence (shared dict pair + `MagicMock` db) | Yes | Confirmed in `test_both_helpers_emit_identical_bundle_shape`. |
| D4 — recursive key-shape, leaves discarded, two-non-empty-slug fixture rule | Yes | Confirmed by source read of `_key_shape` and the fixture data used. |
| D5 — parity test lives in `test_reap_uploader_dag.py` | Yes | `TestCopyVerificationEvidenceShapeParity` is in that file. |

### Issues Found

**CRITICAL**: None.

**WARNING**:
1. Task 4.2 (`test_unresolved_speaker_still_yields_matching_bundle_shape`) has no explicit mutation-check
   evidence row in `apply-progress.md`, unlike its 7 sibling assertion groups — apply-progress's blanket
   claim that falsifiability was proven "for each assertion group" is not literally true for this one case,
   though the risk is low because it reuses the already-proven `_key_shape` mechanism (task 4.1) against a
   production code path this verify agent confirmed by inspection is structurally symmetric.

**SUGGESTION**:
1. `tasks.md`/`apply-progress.md` cite `_lookup_stub` at "line 1152" — that line number has already shifted
   to 1350 as a direct effect of this PR's own insertions above it. Future task/progress documents that cite
   exact line numbers in a growing shared test file will drift; consider citing by symbol name only, or
   accept the drift as expected and harmless (as it is here).

### Verdict

**PASS WITH WARNINGS** — All 13 tasks complete, all 5 spec scenarios have real passing covering tests
(independently executed and, for the higher-risk trap-avoidance claims, independently re-mutated by this
verify agent), the hard diff-scope invariant holds with a clean working tree, `ruff check`/`ruff format
--check` pass, and the full suite is green at 5208 passed / 34 skipped (exactly baseline + 8). The single
WARNING (missing mutation-check row for task 4.2) does not block archive; it is a documentation-completeness
gap in the evidence trail, not a functional or spec-compliance defect.
