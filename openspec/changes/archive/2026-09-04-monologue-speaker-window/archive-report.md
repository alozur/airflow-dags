# Archive Report: Monologue Speaker Window (issue #430)

**Date archived**: 2026-09-04  
**Change name**: monologue-speaker-window  
**GitHub issue**: #430  
**Artifact store**: openspec (repo-local)  
**Spec domain**: monologue-speaker-resolution

## Executive Summary

The monologue speaker window change has been fully planned, implemented, verified, and archived. All 49 tasks completed. Two-step LLM-based speaker identification for non-qa turns, using only the pre-turn announcement window (max 120s before turn anchor), with evidence audit persistence via migration 046. Five stacked PRs (#454–#462) merged to `dev` on 2026-09-04; both dev and prod schemas confirmed to have the new `speaker_resolution_evidence TEXT NULL` column live and idempotent.

## Specs Synced

| Domain | Action | Details |
|--------|--------|---------|
| monologue-speaker-resolution | Created | Delta spec merged to `openspec/specs/monologue-speaker-resolution/spec.md` (11 requirements, 25 scenarios, all COMPLIANT) |

**Spec summary**: New public API in `congress_videos/modules/monologue_speaker_window.py`:
- Window selection: `turn_anchor_seconds`, `select_preceding_window` (anchor from `group_start_seconds` or `start_seconds`, window 120s before anchor, exclusive of anchor and before)
- Step 1 (floor-holder identification): `identify_floor_holder` (announcement window only, no turn transcript)
- Step 2 (identity resolution): `resolve_announced_identity` (name + evidence quote + roster only, no window text)
- Orchestrator: `resolve_monologue_speaker` (never-raises wrapper, returns `{participant_slug, confidence, evidence, audit}` or `None`)
- Routing: `speaker_turn_prepare_dag.py:312-315` gates on `turn_type != 'qa'`; qa turns stay with `resolve_speaker`
- Evidence persistence: Migration 046 adds `speaker_turn_videos.speaker_resolution_evidence TEXT NULL`
- Caller contract: `mark_turn_resolved(..., evidence=None)` unchanged for 5-positional calls, extended for evidence

## Archive Contents

✓ proposal.md — Change scope, intent, approach (Exploration Approach 2), risks, dependencies, success criteria  
✓ specs/monologue-speaker-resolution/spec.md — 11 requirements, 25 scenarios (all delta, all COMPLIANT)  
✓ design.md — Technical approach, architecture decisions (D1–D8), data flow, file changes, interfaces, prompts, migration, testing strategy, threat matrix  
✓ tasks.md — 49 tasks, all marked `[x]` complete (5 phases: A1 window, A2a LLM steps, A2b orchestrator, B migration, C routing)  
✓ verify-report.md — PASS WITH WARNINGS (49/49 tasks, 4576 tests passed / 29 skipped, migration 046 live on dev and prod, 2 non-critical WARNINGs)  
✓ apply-progress.md — Phase-by-phase progress notes from implementation  
✓ exploration.md — Initial exploration of two approaches

## Task Completion

| Metric | Count |
|--------|-------|
| **Total tasks** | 49 |
| **Completed** | 49 |
| **Unchecked** | 0 |

All implementation tasks marked complete. Task 4.11 (apply migration 046 to dev, then prod) was initially CRITICAL-blocked at first verify pass but remediated by orchestrator on 2026-09-04 (07:57Z) on the NAS. Re-verify confirmed live via `pg_attribute`: `speaker_resolution_evidence TEXT NULL` exists in both `development.speaker_turn_videos` and `production.speaker_turn_videos`. Migration applied to dev via `run_migrations` after `git_sync`; production was pre-applied idempotently with same `ADD COLUMN IF NOT EXISTS` logic.

## Verification Status

**Verdict**: PASS WITH WARNINGS (from `verify-report.md`, 2026-09-04)

| Metric | Value |
|--------|-------|
| Requirements | 11/11 COMPLIANT |
| Scenarios | 25/25 COMPLIANT |
| Unit tests | 4576 passed, 0 failed, 29 skipped (opt-in Postgres/LLM, pre-existing) |
| Build | ✓ PASSED (ruff check, ruff format, import check) |
| Migration 046 | ✓ Confirmed LIVE on both dev and prod schemas |
| `resolve_speaker` regression | ✓ 0-line diff, 71 tests green (frozen module + suite) |

**Non-critical WARNINGs** (do not block archive):
1. Opt-in live-LLM test uses `LIVE_LLM_TESTS` env var instead of design-spec name `MONOLOGUE_LIVE_LLM_TESTS` (deliberate deviation per C slice, does not break any spec requirement)
2. Two of five "Step-1 Floor-Holder Identification" scenarios (addressee-vs-responder, courtesy-then-handover) have only mocked pass-through tests, not real-model proof; only full-name case has live-model test (opt-in, skipped in CI/default)

## Delivery Artifacts

**Merged PRs** (all to `dev`, stacked-to-main chain):
- PR #454 `feat/430-a1-window-selection` — window selection, 4 prompts, 6 boundary tests (342 authored lines)
- PR #455 `feat/430-a2a-llm-steps` — `FloorHolder`/`AnnouncedIdentity`, two LLM steps, parametrized tests (395 authored lines)
- PR #456 `feat/430-a2b-orchestrator` — audit builder, loader, orchestrator, never-raise wrapper (320 authored lines)
- PR #458 `feat/430-b-evidence-migration` — migration 046, schema snapshot, `mark_turn_resolved(evidence=)` (145 authored lines)
- PR #462 `feat/430-c-routing` — routing logic, caller-suite rewiring, documentation update (336 authored lines)

**Total delivered**: ~1538 authored lines (vs. ~1090 forecast); review risk mitigated via 5 chained slices < 400 lines each.

**Follow-up issue**: GitHub #463 "reassess resolve_speaker's narrow intro+turn branch after monologue routing" filed for post-merge review of dead-code elimination opportunity.

## Final State Authority

**Source ranking** (per sdd-archive SKILL.md Final-State Authority):

1. **Persisted tasks artifact** (highest authority): `tasks.md` shows 49/49 `[x]` complete, including task 4.11 remediated
2. **Explicit final-state facts from launch prompt**: All 5 PRs merged to dev (b504254), migration 046 confirmed live on both dev and prod schemas, verify PASS WITH WARNINGS, follow-up #463 filed
3. **Intermediate snapshots** (`verify-report.md`, `apply-progress.md`, lowest authority): Reflect state at verify time; stale for any work completed after they were written

This report describes the state AT ARCHIVE CLOSE, not at intermediate snapshots. Work after `verify-report.md` timestamp (migration 046 live application on prod, task 4.11 remediation) is recorded here with evidence (pg_attribute confirmation, tasks.md checkbox update).

## SDD Cycle Status

**Phase 1 — sdd-explore**: ✓ Completed (two approaches evaluated; Exploration Approach 2 selected)  
**Phase 2 — sdd-propose**: ✓ Completed (proposal approved, scope/approach/rollback confirmed)  
**Phase 3 — sdd-spec**: ✓ Completed (11 requirements, 25 scenarios, all specified)  
**Phase 4 — sdd-design**: ✓ Completed (8 architecture decisions, data flow, interfaces, testing strategy)  
**Phase 5 — sdd-tasks**: ✓ Completed (49 tasks, auto-chain delivery, 5 slices forecast ~1090 lines)  
**Phase 6 — sdd-apply**: ✓ Completed (all slices delivered, 5 PRs merged to dev, 1538 authored lines)  
**Phase 7 — sdd-verify**: ✓ Completed (PASS WITH WARNINGS, 49/49 tasks, 4576 tests, migration 046 live)  
**Phase 8 — sdd-archive**: ✓ Completed (this report)

## Not Yet Complete (Delivery Path)

These steps are workflow routing, not SDD closure — the SDD cycle is complete at archive:
- Release dev → main (ordinary repository policy, user decision)
- `git_sync_dag` trigger on NAS prod stack (routine pipeline, not SDD)
- `run_migrations` verify migration 046 recorded as applied on prod (routine pipeline; already pre-applied idempotently)
- Close GitHub issue #430 (user action after release)

---

**Archive verified**: ✓ All artifacts in place, no unchecked tasks, migration 046 confirmed live, spec merged  
**Archived to**: `openspec/changes/archive/2026-09-04-monologue-speaker-window/`  
**Spec source of truth**: `openspec/specs/monologue-speaker-resolution/spec.md`
