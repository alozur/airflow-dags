# Apply Progress: copy-evidence-name-split-coverage

**Status**: All 5 phases complete (13/13 tasks). Ready for `sdd-verify`.
**Mode**: Strict TDD (test-only change; zero production edits in the final diff).

## Completed Tasks

- [x] 1.1 — `_lookup_stub` helper added to `test_youtube_upload_dag.py` (new file, no prior helper existed).
- [x] 1.2 — Reused the existing `_lookup_stub` in `test_reap_uploader_dag.py` (module-level, line 1152) instead of redefining it. Deviation from the literal task text ("add the same ... helper"), justified: the helper already exists in-file for `TestBuildShortsMetadataContext`; redefining under the same name in the same module would shadow it. D2's actual intent (no cross-test-module import, no shared conftest roster) is fully satisfied by reuse.
- [x] 1.3 — `_key_shape(value)` recursive helper added to `test_reap_uploader_dag.py`.
- [x] 2.1, 2.2, 2.3 — `TestCopyVerificationEvidenceNameSplit` added to `test_youtube_upload_dag.py` (3 tests against `congress_videos.youtube_upload_dag._copy_verification_evidence`).
- [x] 3.1, 3.2, 3.3 — `TestCopyVerificationEvidenceNameSplit` added to `test_reap_uploader_dag.py` (3 tests against `congress_videos.reap_shorts_uploader_dag._copy_verification_evidence`).
- [x] 4.1, 4.2 — `TestCopyVerificationEvidenceShapeParity` added to `test_reap_uploader_dag.py` (2 tests: equivalent-inputs parity, unresolved-speaker parity).
- [x] 5.1, 5.2, 5.3 — Verification run (see below).

## Files Changed

| File | Action | What Was Done |
|------|--------|----------------|
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Added `_lookup_stub` helper + `TestCopyVerificationEvidenceNameSplit` (3 tests) |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modified | Added `_key_shape` helper + `TestCopyVerificationEvidenceNameSplit` (3 tests) + `TestCopyVerificationEvidenceShapeParity` (2 tests) |
| `congress_videos/**` | None | Zero production lines changed (confirmed via `git diff --stat origin/main`) |

## TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 2.1 | `test_youtube_upload_dag.py` | Unit | N/A (new tests, no existing coverage of this fn) | ✅ Written before running | ✅ Passed on first run | ➖ Single scenario (resolvable-slug split) | ➖ None needed |
| 2.2 | `test_youtube_upload_dag.py` | Unit | Same as above | ✅ Written | ✅ Passed | ➖ Single (unmapped-slug) | ➖ None needed |
| 2.3 | `test_youtube_upload_dag.py` | Unit | Same as above | ✅ Written | ✅ Passed | ✅ 2 mentioned entries with distinct sentinels + distinct speaker sentinel | ➖ None needed |
| 3.1 | `test_reap_uploader_dag.py` | Unit | Same as above | ✅ Written | ✅ Passed | ➖ Single | ➖ None needed |
| 3.2 | `test_reap_uploader_dag.py` | Unit | Same as above | ✅ Written | ✅ Passed | ➖ Single | ➖ None needed |
| 3.3 | `test_reap_uploader_dag.py` | Unit | Same as above | ✅ Written | ✅ Passed | ✅ 2 mentioned entries + distinct speaker sentinel | ➖ None needed |
| 4.1 | `test_reap_uploader_dag.py` | Unit (cross-module) | N/A (new) | ✅ Written | ✅ Passed | ➖ Paired with 4.2 | ➖ None needed |
| 4.2 | `test_reap_uploader_dag.py` | Unit (cross-module) | N/A (new) | ✅ Written | ✅ Passed | ✅ Unresolved-speaker case (distinct code path: `mentioned_participant_slugs=None`, `resolved_participant_slug=None`) | ➖ None needed |

All 8 tests passed on their first execution after being written (called real, already-existing production functions with correctly-shaped mocks — no implementation gap to close, per exploration.md §8: both functions were already plain, importable, and untouched by this change). "RED" for a test-only change against existing code means: written to call a real code path with assertions that would fail under a plausible bug, then execution proved they pass against the real (correct) implementation. Genuine RED (test literally failing) was produced and confirmed via the mutation-check discipline below, not via a missing implementation — there is no implementation gap in this change.

### Test Summary
- **Total tests written**: 8
- **Total tests passing**: 8
- **Layers used**: Unit (8)
- **Approval tests** (refactoring): None — no refactoring tasks, both target functions are unchanged
- **Pure functions created**: 2 test-only helpers (`_lookup_stub` in `test_youtube_upload_dag.py`, `_key_shape` in `test_reap_uploader_dag.py`), zero production functions

## Mutation-Check Evidence (per task instruction — required before accepting each assertion group)

Each mutation was applied, the affected test was run and observed to FAIL, then the mutation was reverted and the test re-confirmed GREEN. Where the task specified mutating production code (2.2, 3.2 — "make the raw name fall back to the canonical value"), the mutation was applied to the tracked production file, the test was run, and the production file was reverted via `Edit` back to its original content before any commit; `git diff --stat` was re-verified empty for `congress_videos/**` after every one of these checks.

| Task | Mutation applied | Where | Test run | Result before revert | Reverted? |
|------|-------------------|-------|----------|------------------------|-----------|
| 2.1 | Swapped the two sentinel return values in the test's own mocks (roster's `display_name` ↔ `canonical_display_name` return_value) | `test_youtube_upload_dag.py` (test-only) | `test_resolvable_slug_splits_raw_and_canonical` | FAILED — `assert 'CANON-X' == 'RAW Foo'` | ✅ Yes |
| 2.2 | Changed `"short_name": canonical_display_name(slug)` to `canonical_display_name(slug) or participant.get("display_name")` (simulates short_name silently falling back to the raw name when canonical is unmapped) | `congress_videos/youtube_upload_dag.py:668` (temporary) | `test_unmapped_slug_keeps_raw_and_nulls_canonical` | FAILED — `assert 'RAW Foo' is None` | ✅ Yes |
| 2.3 | Collapsed the roster-keyed `side_effect=` to a flat `return_value=roster["speaker-slug"]` / `canonical["speaker-slug"]` (trap #3: every mentioned slug gets the speaker's name) | `test_youtube_upload_dag.py` (test-only) | `test_mentioned_entries_split_raw_and_canonical` | FAILED — `assert 'RAW Speaker' == 'RAW Mentioned A'` | ✅ Yes |
| 3.1 | Same swap as 2.1, against the reap-side sentinels | `test_reap_uploader_dag.py` (test-only) | `test_resolvable_slug_splits_raw_and_canonical` (reap class) | FAILED — `assert 'CANON-X' == 'RAW Foo'` | ✅ Yes |
| 3.2 | Same fallback-conflation edit as 2.2, applied to the reap module's `_copy_verification_evidence` | `congress_videos/reap_shorts_uploader_dag.py:265` (temporary) | `test_unmapped_slug_keeps_raw_and_nulls_canonical` (reap class) | FAILED — `assert 'RAW Foo' is None` | ✅ Yes |
| 3.3 | Same roster-collapse as 2.3, against the reap-side fixture | `test_reap_uploader_dag.py` (test-only) | `test_mentioned_entries_split_raw_and_canonical` (reap class) | FAILED — `assert 'RAW Speaker' == 'RAW Mentioned A'` | ✅ Yes |
| 4.1 | Dropped the `"slug"` key from `long_form["speaker"]` before comparing `_key_shape` | `test_reap_uploader_dag.py` (test-only) | `test_both_helpers_emit_identical_bundle_shape` | FAILED — dict equality mismatch on `speaker` sub-shape | ✅ Yes |
| 4.2 | Made the `"party"` key conditional on a resolved participant in `congress_videos/youtube_upload_dag.py` (`**({"party": ...} if participant else {})`) — an asymmetry that appears ONLY on the unresolved-speaker path | `congress_videos/youtube_upload_dag.py` (production, reverted before commit) | `test_unresolved_speaker_still_yields_matching_bundle_shape` | FAILED — dict equality mismatch on `speaker` sub-shape, while `test_both_helpers_emit_identical_bundle_shape` still PASSED | ✅ Yes |

Note on 2.2/3.2 interpretation: the task text says "temporarily make the raw name fall back to the canonical value," but the concrete conflation risk the spec scenario guards against in the *unmapped* case (canonical returns `None`) is `short_name` silently inheriting the raw `display_name` — the reverse direction is a no-op under this fixture (`X or None == X`). The mutation applied is the one that is actually falsifiable against this fixture and matches the spec's stated failure mode ("proving neither field falls back to the other"). Both directions of the swap are already covered together by the 2.1/3.1 mutation (full swap under a resolvable canonical value).

## Deviations from Design
None — implementation matches design.md D1–D5 exactly (sentinel doubles, roster-keyed `side_effect`, parity-by-input-equivalence, recursive key-shape leaves-discarded, parity test located in `test_reap_uploader_dag.py`).

## Issues Found
None.

## Verification (Phase 5)

- **5.1** `uv run pytest tests/congress_videos/test_youtube_upload_dag.py tests/congress_videos/test_reap_uploader_dag.py --no-cov -q` → `323 passed in 5.27s`.
- **5.2** `git diff --stat origin/main` → only `openspec/changes/copy-evidence-name-split-coverage/**` and `tests/congress_videos/test_reap_uploader_dag.py` (+198) / `tests/congress_videos/test_youtube_upload_dag.py` (+101) appear. Zero `congress_videos/**` lines.
- **5.3** `uv run pytest` (full suite) → `5208 passed, 34 skipped in 131.65s`, exit code 0. Baseline on `main` was `5200 passed, 34 skipped` — the delta is exactly the 8 new tests added by this change, confirming zero regressions.
- `uv run ruff check .` → `All checks passed!`
- `uv run ruff format --check .` → `320 files already formatted`.

## Workload / PR Boundary
- Mode: single PR (forecast: Low risk, ~230 added lines, no chaining)
- Current work unit: Unit 1 (the only unit) — direct-call coverage + parity, both DAG test files, one commit
- Boundary: starts from `c9c15c8` (planning-only commit), ends with all 8 tests + helpers added, zero production changes
- Estimated review budget impact: ~299 added lines (test files only) — comfortably under the 400-line budget

## Rollback Boundary
Delete the two new test classes (`TestCopyVerificationEvidenceNameSplit` in each file) and `TestCopyVerificationEvidenceShapeParity` plus the `_key_shape`/`_lookup_stub` helpers added by this change. No production file requires any change to roll back — `congress_videos/**` was never touched.

## Status
13/13 tasks complete. Ready for verify.

## Post-verify addendum — task 4.2 mutation check (added by the orchestrator)

`sdd-verify` raised one WARNING: task 4.2 had no mutation-check row, so its falsifiability was asserted
rather than demonstrated. Closed with an **isolating** mutation, chosen so that it can only manifest on the
unresolved-speaker path:

```python
# congress_videos/youtube_upload_dag.py — temporary, reverted
**({"party": participant.get("party")} if participant else {}),
```

Result:

- `test_both_helpers_emit_identical_bundle_shape` (4.1) → **1 passed**
- `test_unresolved_speaker_still_yields_matching_bundle_shape` (4.2) → **1 failed**

That asymmetry is exactly what distinguishes the two tests: 4.2 catches a divergence 4.1 cannot see, so it
earns its place rather than duplicating 4.1. Production file restored and `git status` confirmed clean
before commit.
