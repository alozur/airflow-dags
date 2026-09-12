# Archive Report: vps-ephemeral-nas-inputs

**Change**: vps-ephemeral-nas-inputs  
**Archive date**: 2026-09-12  
**Archived to**: `openspec/changes/archive/2026-09-12-vps-ephemeral-nas-inputs/`  
**Artifact store**: hybrid (openspec + engram)  

## Executive Summary

Change `vps-ephemeral-nas-inputs` is now closed and archived. Three delta specifications (nas-fetch-outcome, nas-auto-fetch, nas-reclaim) have been merged into the main spec store. All 70 implementation tasks (phases 1–7, slices 1a–4c) are complete and shipped via 11 merged PRs (#626–#636) targeting `dev`. Phases 8.1–8.2 (verification via this sdd-verify run) are satisfied by evidence; phases 8.3–8.5 (manual production checks on the VPS) remain pending and tracked in issue #625.

## Specs Synced to Main Store

| Domain | File | Action | Details |
|--------|------|--------|---------|
| nas-fetch-outcome | `openspec/specs/nas-fetch-outcome/spec.md` | Created | Full spec copied from delta (no existing main spec) |
| nas-auto-fetch | `openspec/specs/nas-auto-fetch/spec.md` | Created | Full spec copied from delta (no existing main spec) |
| nas-reclaim | `openspec/specs/nas-reclaim/spec.md` | Created | Full spec copied from delta (no existing main spec) |

All three specs were copied mechanically with `cp -R` and verified with `diff -r` to ensure byte-identity. Empty diff confirms successful copy with zero truncation or alteration.

## Corrections Applied at Archive Time

Per verify-report warnings D-1 and D-2:

1. **spec.md (nas-auto-fetch)** — verified correct: "Video absent everywhere" scenario already states outcome as `skipped_no_video` per design D6a. No correction needed.

2. **design.md (D3 clarification)** — corrected: Design D3 previously stated "Gates 1–4 are re-evaluated inside the lock" but the shipped `reclaim_one_video` implementation only re-checks gates 1 (lock), 2 (grace), and 4 (NAS verify) inside the lock. Gate 3 (DB completeness) is selection-time only per `modules/nas_reclaim.py` module docstring and task 6.9 deviation. Design D3 updated to explicitly document gate 3's selection-time-only status and note the accepted-risk rationale (mirrors D4's reasoning: worst case is re-fetch churn, not data loss, since gate 4 always re-verifies before deletion).

3. **tasks.md (Phase 8 checkboxes)** — updated: Tasks 8.1 and 8.2 marked `[x]` as functionally satisfied by sdd-verify run evidence:
   - 8.1: `deploy/vps-dev/test_contract.py` full suite (25 passed) — confirmed via this verify pass.
   - 8.2: DAG import check (all 8 modules import cleanly) — confirmed via this verify pass; e2e Docker leg correctly reported `unavailable` (not a failure) on this controller host.

Tasks 8.3–8.5 remain unchecked and pending-live (manual VPS steps requiring live NAS/Airflow).

## Implementation Delivery Summary

**Merged PRs**: 11 squash commits to `dev` (tracking issue #625)
- PR #626–#627: Slice 1a (fetch-outcome + verify returncode hardening)
- PR #628–#629: Slice 1b (fetch_lock + ensure_local_video)  [split due to 400-line budget]
- PR #630: Slice 2 (wire 3 NAS-aware consumers)
- PR #631: Slice 3 (wire 2 unaware consumers) [includes HIGH finding remediation: uncaught ValueError in speaker_turn_videos]
- PR #632–#633: Slice 4a (reclaim settings + completeness extraction) [includes verifier-directed per-candidate isolation in nas_archive_dag]
- PR #634: Slice 4b (nas_reclaim module)
- PR #635–#636: Slice 4c (nas_reclaim DAG) [split due to 400-line budget]

**Release PR**: #637 merged to `main` as merge commit dcf1c65 (includes VPS foundation work).

**Verification evidence** (sdd-verify, Engram obs #2946):
- Build: PASS (ruff clean)
- Full test suite: 5723 passed / 36 skipped (pre-existing environmental/Postgres live tests)
- Targeted nas_* suite: 831 passed / 7 skipped
- Deploy contract: 25 passed
- DAG imports: all 8 touched modules import cleanly
- E2E gate: correctly reported `unavailable` (Docker unreachable on controller; not a failure)
- Coverage: all changed modules above 80% threshold (nas_fetch.py 95.31%, nas_archive.py 94.79%, nas_reclaim.py 94.85%)
- Verdict: **PASS WITH WARNINGS** — all 23 scenarios across 3 specs compliant; two documented deviations both evidence-backed and non-blocking.

**Independent reviewer findings**:
- Two HIGH findings found during slice review (path-injection in lock/marker code, uncaught ValueError in speaker_turn_videos) — both caught, remediated with RED→GREEN tests, confirmed present in merged tree.

## Spec Compliance & Coherence

### Behavioral Compliance (all 23 scenarios green)

**nas-fetch-outcome domain** (5 scenarios):
- All-failed run fails the task ✅
- Partial failure stays visible ✅
- Empty request is a no-op success ✅
- Every video already fetched (idempotent) ✅

**nas-auto-fetch domain** (8 scenarios):
- Inline fetch on missing local source (NAS-aware and previously unaware) ✅
- NAS-missing preserves existing behavior ✅
- Idempotent, concurrency-safe fetch ✅
- Retention refresh on auto-fetch ✅

**nas-reclaim domain** (10 scenarios):
- Three-gate deletion (happy path, DB-blocked, grace-blocked, NAS-verify-blocked) ✅
- Protected/non-media content never touched ✅
- In-flight fetch never reclaimed ✅
- Bounded run size ✅
- Schedule and environment contract ✅

### Documented Deviations (non-blocking, evidence-backed)

1. **D-1 (spec/tasks vs. code/design)** — **Already mitigated at archive time**. Spec scenario and tasks text now confirmed correct; no additional correction needed beyond archive-time documentation pass.

2. **D-2 (design D3 gate 3 re-evaluation)** — **Corrected at archive time** (see "Corrections Applied" section above). Design.md D3 prose updated to explicitly document gate 3 selection-time-only status.

3. **Review-budget overages resolved via commit splits** — slices 1b, 2, and 4c each initially exceeded the 400-line budget; coordinator-directed, verified-byte-identical commit splits resolved (no `size:exception` available). Final merged state byte-identical to pre-split single commits: zero behavior change.

4. **Phase 8.1–8.2 hygiene** — tasks marked as complete at archive time per verify run evidence. Low-risk cosmetic state sync.

## Task Completion Gate Validation

All 70 implementation tasks (phases 1–7) are checked `[x]` in the persisted `tasks.md` artifact. Phases 8.1–8.2 are functionally satisfied (see Phase 8 completion below) and marked `[x]` at archive time per verify-report evidence. Phases 8.3–8.5 remain pending-live (manual VPS/production steps).

**Task completion evidence**:
- All slice 1–7 work units merged and verified green.
- Phase 8.1 (deploy/vps-dev/test_contract.py): 25 passed ✅
- Phase 8.2 (DAG import check): all 8 modules import cleanly ✅
- Phase 8.3–8.5: pending-live (manual VPS steps, tracked in issue #625)

## Post-Archive Delivery Verification Pending

The homeserver-config repository contains a dependent change that pins `prepare.py` SOURCE to main commit dcf1c65 and renders `NAS_RECLAIM_GRACE_HOURS` from `dev_nas_reclaim_grace_hours`:

- **Homeserver-config branch**: `feat/625-nas-reclaim-pin` (commit bd0b53f)
- **Status**: Native review + production apply + live check (phases 8.3–8.5) pending at archive time.
- **Tracking**: Issue #625 and Engram topic `sdd/vps-ephemeral-nas-inputs/state` (project airflow-dags).

These post-archive steps are outside the SDD cycle scope but are documented here for complete lineage.

## Archive Contents

- `proposal.md` — change proposal (Approach 2, grounded against vps-dev-foundation)
- `specs/` — three delta specs (nas-fetch-outcome, nas-auto-fetch, nas-reclaim)
- `design.md` — technical design (D1–D7 decision drivers; corrected D3 at archive time)
- `tasks.md` — task breakdown (70 tasks; phases 1–7 complete, 8.1–8.2 verified, 8.3–8.5 pending-live)
- `exploration.md` — pre-proposal exploration
- `apply-progress.md` — implementation progress per slice
- `verify-report.md` — verification report (PASS WITH WARNINGS)
- `archive-report.md` — this report

## Source of Truth Update

The following main specs now reflect the shipped behavior:

- `openspec/specs/nas-fetch-outcome/spec.md` — handles fetch success/failure semantics
- `openspec/specs/nas-auto-fetch/spec.md` — inline fetch for 5 consumer DAGs
- `openspec/specs/nas-reclaim/spec.md` — 4-hourly reclaim with grace window and verification gates

These are the authoritative specifications for future changes to these capabilities. The change folder's delta specs are now archived and serve as historical record only.

## Lineage and Traceability

**OpenSpec artifacts** (filesystem):
- Archive folder: `openspec/changes/archive/2026-09-12-vps-ephemeral-nas-inputs/`
- Main specs: `openspec/specs/{nas-fetch-outcome,nas-auto-fetch,nas-reclaim}/spec.md`

**SDD workflow lineage**:
- Proposal phase: approved by user after research
- Spec phase: three delta specs generated
- Design phase: seven architecture decisions (D1–D7)
- Tasks phase: 70 implementation tasks across 7 slices
- Apply phase: 11 merged PRs (#626–#636) to `dev`; release PR #637 to `main` (dcf1c65)
- Verify phase: PASS WITH WARNINGS (sdd-verify obs #2946)
- Archive phase: this report; specs synced to main store; corrections applied

## Status

**Status**: ARCHIVED ✓  
**Merged**: `origin/dev` @ 3ce1da5; `origin/main` @ dcf1c65  
**Delivery**: Post-archive phases (8.3–8.5) pending per issue #625

SDD cycle complete. Change is ready for operator handoff per issue #625.
