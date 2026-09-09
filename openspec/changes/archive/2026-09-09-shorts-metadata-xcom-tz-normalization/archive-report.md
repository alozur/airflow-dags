# Archive Report: shorts-metadata-xcom-tz-normalization

**Change**: shorts-metadata-xcom-tz-normalization (GitHub issue #546)
**Date Archived**: 2026-09-09
**Artifact Store Mode**: hybrid (filesystem + Engram)
**Status**: COMPLETE

---

## Executive Summary

The `reap_shorts_uploader_dag` production crash (zero shorts publishing due to TIMESTAMPTZ deserialization failure in XCom) has been fixed by normalizing database rows to UTC at the XCom append site. All 20 core implementation tasks completed; verification passed with 5089/5089 tests passing, 0 failures, 0 blockers, all 5/5 requirements and 6/6 scenarios compliant. Two legitimately deferred tasks (Docker e2e unavailable in this environment, GitHub issues blocked by missing YAML issue forms) recorded for follow-up. Change is ready for delivery to production after `main` merge and NAS `git_sync`.

---

## Change Summary

**Issue**: #546 — `reap_shorts_uploader` broken in production: zero shorts publish due to `ValueError: ZoneInfo keys must be normalized relative paths` when XCom payloads from `_generate_metadata` (task t2) carry raw psycopg2-returned datetime objects with non-zero fixed UTC offsets from `video_chapters.updated_at` (TIMESTAMPTZ).

**Root Cause**: Fourth recurrence of the same defect class (#163, #303, #309). #309's convergence onto the `utc_normalize_row` convention was not enforced; #512's new `shorts_metadata` XCom hop omitted the TIMESTAMPTZ field in its fixtures, so it stayed green without the normalization guard.

**Solution**: Apply `utc_normalize_row` to `chapter` and `turn_speaker_row` dicts before appending to `shorts_metadata` in `_generate_metadata` (task t2). Durable guard: fixtures carry non-zero offsets by default; any future raw-TIMESTAMPTZ field in that payload MUST survive a real `XComEncoder`/`XComDecoder` round trip, or tests fail.

---

## Cycle Completion

### SDD Phases Executed

- **sdd-explore** → identified the crash site, fourth recurrence evidence, candidate solutions
- **sdd-propose** → Option A (normalize at append) selected; Option B (projection) rejected; scope/risks/rollback defined
- **sdd-spec** → 5 requirements + 6 scenarios defined; XCom serializer round-trip compliance explicit
- **sdd-design** → architecture decisions, fixture asymmetry rationale, "why this keeps happening" analysis, threat matrix (all N/A)
- **sdd-tasks** → 24 tasks structured in 6 phases (foundation, RED, GREEN, t2b regression, verification, commits/follow-ups)
- **sdd-apply** → all 20 core implementation tasks completed; code + tests shipped; 2 docs commits (code fix + SDD planning artifacts)
- **sdd-verify** → independently re-run all tests; 5089 passed, 34 skipped, 0 failures, 0 blockers; all 5/5 requirements and 6/6 scenarios verified compliant
- **sdd-archive** → this phase; all artifacts synced, change folder moved to archive, this report persisted

### Task Completion Status

**Core Implementation Tasks (20/20 COMPLETE)**:
- Phase 1 (Foundation): 1.1–1.4 ✅ (fixture imports + helpers + metadata fields)
- Phase 2 (RED): 2.1–2.4 ✅ (regression tests proving the defect)
- Phase 3 (GREEN): 3.1–3.3 ✅ (production fix + import + wrapping)
- Phase 4 (t2b Regression): 4.1–4.2 ✅ (downstream consumption verification)
- Phase 5 (Verification): 5.1–5.2 ✅ (full suite + linting + formatting)
- Phase 6 (Commits): 6.1–6.2 ✅ (code commit + SDD docs commit)

**Deferred Tasks (4 items, all legitimate and recorded):**
1. **Task 5.3 — Docker e2e smoke test** (`bash scripts/test-airflow-e2e.sh`)
   - Status: `unavailable` (not `failed`)
   - Reason: This archive environment lacks Docker API access (`permission denied on /var/run/docker.sock`)
   - Verified in: both apply and verify phases independently confirmed
   - Mitigation: Must be run manually before merge per project convention (`CLAUDE.md`)
   - Does NOT block archive per skill rules (environment unavailable ≠ test failure)

2. **Task 6.3 — File follow-up issue A** (repo-wide xcom_push serialization guard)
   - Status: Blocked (not deferred by implementation)
   - Reason: `alozur/airflow-dags` repository publishes no YAML Issue Form on `main` branch; only a local uncommitted `feature.yml` draft exists; the repository's issue-creation contract forbids markdown-body fallback
   - Rationale: Explicitly marked out-of-scope in proposal ("Follow-up issue"); orchestrator owns delivery decisions
   - Details: Should reference #546, design's "Why this keeps happening" section, ~61 xcom_push sites across 12 DAGs
   - Does NOT block archive per skill rules (orchestrator-owned, not apply-owned)

3. **Task 6.4 — File follow-up issue B** (latent pending_shorts risk)
   - Status: Blocked (same reason as 6.3)
   - Reason: Same repository issue-form constraint
   - Rationale: Explicitly marked out-of-scope in proposal; latent risk only if future migration adds TIMESTAMPTZ to `video_shorts.created_at`/`updated_at`/`copy_verified_at` (currently naive `TIMESTAMP`, safe today)
   - Does NOT block archive per skill rules (orchestrator-owned, not apply-owned)

**Task Completion Gate Result**: PASS
- All 20 core implementation tasks checked `[x]` in persisted `tasks.md` artifact
- All unchecked tasks legitimately deferred/blocked per skill rules and final-state facts
- No stale checkboxes for incomplete work
- Exceptional reconciliation NOT required

---

## Verification Results

**Verdict**: PASS (independent re-run by verify phase)

### Test Execution

```
Build: PASS
$ uv run ruff check .
All checks passed!

$ uv run ruff format --check .
313 files already formatted

Test: PASS (5089 passed / 0 failed / 34 skipped)
$ uv run pytest
================= 5089 passed, 34 skipped in 85.48s (0:01:25) ==================

Focused re-run (independent):
$ uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k "TestShortsMetadataXComNormalization or TestVerifyFinalCopyShorts" -v --no-cov
======================= 9 passed, 77 deselected in 3.44s =======================
```

### Spec Compliance

All 5/5 requirements and 6/6 scenarios verified compliant:

| # | Requirement | Scenario | Test | Result |
|---|-------------|----------|------|--------|
| REQ-1 | DB row normalization at append site | Chapter row with tz-aware updated_at normalized before push | `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` | ✅ PASS |
| REQ-1 | DB row normalization at append site | Missing turn stays None through normalization | `test_generate_metadata_missing_turn_stays_none_after_round_trip` | ✅ PASS |
| REQ-2 | Pushed payload survives real serializer | Normalized payload round-trips without raising | `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` (real XComEncoder/XComDecoder) | ✅ PASS |
| REQ-3 | Normalized offset survives round trip as UTC | Round-tripped updated_at is UTC | `test_generate_metadata_shorts_metadata_survives_real_xcom_round_trip` (asserts .utcoffset() == timedelta(0)) | ✅ PASS |
| REQ-4 | Un-normalized payload pinned as failing | Raw payload raises ZoneInfo ValueError | `test_raw_chapter_row_breaks_real_xcom_round_trip` (pytest.raises ValueError match) | ✅ PASS |
| REQ-5 | Downstream consumption unaffected by normalization | Verification evidence unchanged | `test_verify_final_copy_repush_survives_real_xcom_round_trip` + pre-existing TestVerifyFinalCopyShorts tests | ✅ PASS |

### Bug-Pin Regression Test

The permanent regression guard (`test_raw_chapter_row_breaks_real_xcom_round_trip`) deliberately never touches production code and stays red-raising forever, proving the fix has teeth. Verified by reversible revert during apply phase: removing both `utc_normalize_row` wraps reproduced the exact `ValueError: ZoneInfo keys must be normalized relative paths` failure; restored via `git checkout`, tree left clean.

---

## Artifacts Synced

### Delta Specs → Main Specs (Openspec)

**New Spec Created**: `openspec/specs/xcom-row-serialization/spec.md`
- Action: Mechanical shell copy (cp → diff → mv verification)
- Source: `openspec/changes/shorts-metadata-xcom-tz-normalization/specs/xcom-row-serialization/spec.md`
- Destination: `openspec/specs/xcom-row-serialization/spec.md`
- Diff Verification: ✅ PASS (empty diff, byte-identity confirmed)
- Size: 105 lines
- Content: 5 requirements, 6 scenarios, XCom serializer compliance explicit

### Change Folder Archived (Openspec)

**Source**: `openspec/changes/shorts-metadata-xcom-tz-normalization/`
**Destination**: `openspec/changes/archive/2026-09-09-shorts-metadata-xcom-tz-normalization/`
**Method**: `git mv` (tracked files)
**Diff Verification**: ✅ PASS (empty diff vs. pre-move snapshot)

**Archive Contents**:
- proposal.md ✅
- design.md ✅
- tasks.md ✅ (all 20 core implementation tasks marked [x])
- verify-report.md ✅
- explore.md ✅
- specs/xcom-row-serialization/ ✅
- archive-report.md (this file, created post-move) ✅

---

## Key Finding: Why This Keeps Happening

Recorded from design document per archive authority hierarchy (explicit final-state facts + post-verify evidence > intermediate snapshots):

**Problem**: #309 converged on the `utc_normalize_row` convention at XCom boundaries but left it unenforced. When #512 introduced a new `shorts_metadata` XCom hop from `_generate_metadata`, the test fixtures omitted the `TIMESTAMPTZ` field entirely, so the code stayed green without the normalization guard. Convention plus green tests is not a guard when the fixture cannot express the failure.

**Solution Architecture**: The durable guard is now the fixture carrying a non-zero offset (`timezone(timedelta(hours=2))`) by default in `_make_chapter_metadata`. Any future payload built from this fixture MUST survive a real `XComEncoder`/`XComDecoder` round trip, or the test fails immediately.

**Repo-Wide Scope**: A complete repo-wide check over all 61 `xcom_push` sites across 12 DAGs is the definitive answer to prevent recurrence and is deliberately deferred to a follow-up issue (Task 6.3, blocked).

**Current Coverage**: This change guards only the `shorts_metadata` payload from `_generate_metadata`; the other 60 push sites remain unenforced. A future TIMESTAMPTZ column landing in any of those payloads will repeat the cycle silently unless they too adopt the same fixture pattern or the follow-up guard is implemented.

---

## Code & Test Changes (Summary)

### Production Code

**File**: `congress_videos/reap_shorts_uploader_dag.py`
- Lines changed: ~7 (import + 2 wraps)
- Change: Apply `utc_normalize_row` to `chapter` and `turn_speaker_row` dicts before appending to `metadata_list` in `_generate_metadata`
- Preserved: All function signatures, all field selections in `_copy_verification_evidence`, all downstream consumption

### Tests

**File**: `tests/congress_videos/test_reap_uploader_dag.py`
- Lines changed: ~110 (fixture extensions + 3 new test methods)
- Changes:
  - Extended `_make_chapter_metadata` with `"updated_at": datetime(2024, 3, 1, 10, 0, tzinfo=timezone(timedelta(hours=2)))`
  - Extended `_make_short_meta`'s chapter dict with `"updated_at": datetime(2024, 3, 1, 8, 0, tzinfo=UTC)` (normalized state)
  - Added `_xcom_round_trip` helper (real XComEncoder/XComDecoder)
  - Added `TestShortsMetadataXComNormalization` class with 3 tests (T1 bug-pin, T2 primary, T3 t2b regression)
  - All pre-existing tests continue passing unchanged

### Utilities (Unchanged)

**File**: `utils/airflow_helpers.py`
- Reused `utc_normalize_row` function (no modifications)
- Existing call sites: `youtube_upload_dag.py:536`, `video_analytics_dag.py:211`

---

## Delivered Artifacts

### Code Commits (Delivered)

1. **71c789a** `docs(sdd): plan shorts_metadata xcom tz normalization for issue #546`
   - SDD planning: proposal, spec, design, tasks
   - Artifact count: ~300-380 lines markdown
   - Phase: proposal → spec → design → tasks

2. **fe4faf5** `fix(congress-videos): normalize shorts_metadata rows at the xcom append site`
   - Code fix: `congress_videos/reap_shorts_uploader_dag.py` + `tests/congress_videos/test_reap_uploader_dag.py`
   - Lines: ~7 production + ~110 tests = ~117 total (matches proposal estimate of ~116)
   - Scope: Full implementation per proposal scope

3. **06a096c** `docs(sdd): record apply progress for shorts-metadata-xcom-tz-normalization`
   - SDD artifact: apply-progress.md with task checkmarks

4. **e79e095** `docs(sdd): record verify report for shorts-metadata-xcom-tz-normalization`
   - SDD artifact: verify-report.md with 5089/5089 test results, compliance matrix

**Branch**: `fix/546-shorts-metadata-xcom-tz` (based on `origin/dev` = 786de7e)
**Working tree**: Clean (verified before archive)

### Artifacts NOT Yet Delivered (Out of Scope for This Change)

1. **PR to main** — Orchestrator owns delivery; branch not yet pushed
2. **GitHub issues #546 follow-ups** — Blocked (Task 6.3, 6.4); no GitHub write attempted
3. **Manual Docker e2e test** — Must run before merge per `CLAUDE.md`

---

## Compliance & Readiness

### Archive Readiness Checklist

- [x] Task Completion Gate: PASS (20/20 core tasks complete, 4 deferred legitimately)
- [x] Verification Verdict: PASS (5089/5089 tests, all 5/5 reqs + 6/6 scenarios compliant)
- [x] Spec Synced: PASS (delta copied, diff verified, main specs updated)
- [x] Change Archived: PASS (git mv, diff verified, source removed)
- [x] Archive Report Written: PASS (this document)
- [x] Final State Authority Applied: Explicit final-state facts from launch prompt used to resolve contradictions; no stale snapshot claims elevated
- [x] Mechanical Copy Contract: All copies/moves via shell (`cp`, `git mv`); diff readback mandatory; verbatim output included

### Known Limitations & Mitigations

| Limitation | Mitigation | Owner |
|-----------|-----------|-------|
| Docker unavailable in sandbox | Run `bash scripts/test-airflow-e2e.sh` manually before merge | User (pre-merge) |
| GitHub issue creation blocked (no YAML forms) | File follow-up via orchestrator or manual GitHub UI | Orchestrator / User |
| One payload guarded; 60 xcom_push sites unprotected | Follow-up issue (Task 6.3) deferred, orchestrator-owned | Orchestrator |
| Latent risk in pending_shorts if future migration | Documented in follow-up (Task 6.4); safe today | Orchestrator / User |

### Production Readiness

**Effective Date**: After `main` merge + NAS `git_sync` completes (typically within 24h of merge)
**Rollback Path**: Revert single commit `fe4faf5`; no schema/migration/state changes
**Manual Validation**: First `reap_shorts_uploader` run post-sync should reach task t3 and publish shorts without XCom serialization errors

---

## Observation IDs (Traceability)

Engram hybrid-mode artifacts read and persisted:

| Artifact Type | Topic Key | Purpose | Status |
|---|---|---|---|
| proposal | `sdd/shorts-metadata-xcom-tz-normalization/proposal` | Stored for archive traceability | Read from openspec |
| spec | `sdd/shorts-metadata-xcom-tz-normalization/spec` | Stored for archive traceability | Read from openspec |
| design | `sdd/shorts-metadata-xcom-tz-normalization/design` | Stored for archive traceability | Read from openspec |
| tasks | `sdd/shorts-metadata-xcom-tz-normalization/tasks` | Completion gate validation | Read from openspec (Task Completion Gate PASS) |
| verify-report | `sdd/shorts-metadata-xcom-tz-normalization/verify-report` | Verdict validation | Read from openspec (PASS) |
| archive-report | `sdd/shorts-metadata-xcom-tz-normalization/archive-report` | This document, persisted to Engram | To be saved via mem_save |

**Filesystem Path for OpenSpec**: `openspec/changes/archive/2026-09-09-shorts-metadata-xcom-tz-normalization/`

---

## Next Steps

1. **User Action (Pre-Merge)**:
   - Run `bash scripts/test-airflow-e2e.sh` to validate Docker smoke test (Task 5.3, currently unavailable)
   - Review branch code one final time if desired (all tests green, no blockers)

2. **Orchestrator Action (Delivery)**:
   - Push branch `fix/546-shorts-metadata-xcom-tz` to `origin`
   - Create PR targeting `origin/dev` (or follow repo's stacked-PR convention if multiple PRs pending)
   - Merge to `dev`, then to `main` per release workflow
   - Confirm NAS `git_sync` runs; monitor first `reap_shorts_uploader` execution post-sync

3. **Follow-Up Issues** (Orchestrator-owned, Task 6.3 & 6.4):
   - File issue A: "Repo-wide xcom_push serialization guard across ~61 push sites (12 DAGs)" with reference to #546, design findings, and proposed solution (repo-wide fixture pattern or centralized check)
   - File issue B: "Latent pending_shorts raw-row XCom risk if video_shorts timestamps migrate to TIMESTAMPTZ" with note that risk is future-only if migration happens

---

## Closing Notes

This change closes a production incident (zero shorts publishing in `reap_shorts_uploader`) introduced by #512's omission of a TIMESTAMPTZ normalization guard. The fix is minimal, focused, and durable: a guard-by-fixture pattern that forces any future raw-TIMESTAMPTZ field in the `shorts_metadata` payload to prove it survives a real serializer round trip, or tests fail immediately.

The deeper lesson — recorded in design's "Why this keeps happening" section — is that convention plus green tests is not a guard when the fixture cannot express the failure. The repo-wide solution (guarding all 61 xcom_push sites) is deferred to a follow-up issue and remains orchestrator-owned.

**SDD Cycle Status**: ✅ COMPLETE. Ready for delivery.

