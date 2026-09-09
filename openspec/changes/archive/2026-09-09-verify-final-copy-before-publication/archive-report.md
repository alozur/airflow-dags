# Archive Report: Verify Final Copy Before Publication

**Change**: verify-final-copy-before-publication (GitHub issue #512)
**Archive Date**: 2026-09-09
**Archived Location**: `openspec/changes/archive/2026-09-09-verify-final-copy-before-publication/`

## Executive Summary

Issue #512 (`verify-final-copy-before-publication`) has completed the SDD apply and verify phases. All four implementation slices merged into `origin/dev` (PRs #523–#541). Verification returned **PASS WITH WARNINGS** (0 CRITICAL, 8/8 requirements, 12/12 scenarios, 5085 tests passed). The change has been archived with delta specs synced to main and a record of final state persisted.

**Status at archive**: Implementation complete on `feat/512-verify-final-copy`; code NOT yet in `main`, NOT yet in production. Phase 5 (deployment verification) remains intentionally OPEN and is post-merge scope per tasks.md.

---

## Change Scope

### Intent
Add an independent verification layer that runs before publication at both long-form (turn) and shorts upload seams. Verify title, description, and thumbnail text for politician-name correctness, party accuracy, spelling, grammar, and unsupported claims. Persist audit records with original, corrected, and evidence values. Flag (but never auto-correct) thumbnail text.

### Affected Components
- **New module**: `congress_videos/modules/final_copy_verification.py` (verdict schema, bounded correction, defensive parsing)
- **Modified DAGs**: `congress_videos/youtube_upload_dag.py` (long-form seam), `congress_videos/reap_shorts_uploader_dag.py` (shorts seam)
- **Database**: Migration 050 adds 9 columns to `speaker_turn_videos` and 8 to `video_shorts` (audit columns: verdict, original/corrected title and description, findings JSON, verified timestamp)
- **Prompts**: Two new constants in `congress_videos/config/ai_prompts.py` (system prompt and user template, Spanish, neutral register)

### Scope Explicitly Out-of-Bounds (documented)
1. **Thumbnail-text auto-correction** — architecturally impossible: text is baked into PNG before verifier runs. Flagged only. Follow-up needed: automatic thumbnail regeneration on rejected thumbnail text.
2. **Dead chapter path** `prepare_chapter_upload_config` (`modules/youtube/youtube_upload.py:140-297`) — unused in production since issue #171. No verification, schema changes, or tests added.

---

## Implementation Delivery

### Slices 1–4: All Complete and Merged
| Slice | Component | PRs | Status | Lines | Note |
|-------|-----------|-----|--------|-------|------|
| 1 | Migration 050 + schema snapshot + drift test | #523 | ✅ Merged to dev | 140 | Schema-only, no consumer yet; clean rollback boundary |
| 2a | Verifier module core (verdict, parsing, correction, versioning) | #529 | ✅ Merged to dev | 743 | `size:exception` (exceeds 400-line budget); ledger reset approved |
| 2b | Prompts + public wrapper | #533 | ✅ Merged to dev | 308 | Within 400-line budget; completes module definition |
| 3 | Long-form seam wiring (t6b task, audit writes, accumulator findings) | #540 | ✅ Merged to dev | 921 | `size:exception`; ledger reset approved; DagBag check passed |
| 4 | Shorts seam wiring (t2b task, audit writes, no accumulator) | #541 | ✅ Merged to dev | 562 | `size:exception`; ledger reset approved; DagBag check passed |

**Total changed lines**: ~2674 across four slices.

**Budget context**: Each slice designed for ≤400 lines (Medium review risk, auto-chain delivery strategy per tasks.md). Three slices (2a, 3, 4) exceeded budget and were accepted as `size:exception`, each requiring a maintainer `sdd-attempt reset` before the next slice could proceed. This pattern is documented and distinct from uncontrolled scope creep — each slice has a clear scope, completion, and rollback boundary.

---

## Verification Results

### Build & Tests (per verify-report obs #2646)
- **Unit tests**: 5085 passed / 0 failed / 34 skipped
- **Lint (`ruff check`)**: All checks passed
- **Format (`ruff format --check`)**: 313 files already formatted
- **Spec compliance**: 8/8 requirements traced to passing tests, 12/12 scenarios passing
- **E2E smoke test**: Docker unavailable (permission denied), substituted `DagBag(safe_mode=True)` check → zero import errors, both DAGs' task graphs verified correct (15 tasks in long-form with `verify_final_copy` between `t6`/`t7`, 6 tasks in shorts with `verify_final_copy` between `t2`/`t3`)

### Verdict
**PASS WITH WARNINGS** (0 CRITICAL issues, 0 blockers)

### Warnings (Non-Blocking)
1. No dedicated unit test exercises `_copy_verification_evidence` internals to assert the canonical-vs-raw display-name split (`short_name = canonical_display_name(slug)`, `display_name` = raw) across both DAG files. Behavior confirmed correct by code reading; nothing would catch a future accidental field swap. Recommend adding a parametrized pair of lightweight tests per DAG file.
2. Three of four apply slices (2a, 3, 4) exceeded the 400-line review-workload budget and required `size:exception` + ledger reset each. Not a defect but a documented pattern for this change's shape. Worth noting for future similar designs.
3. `scripts/test-airflow-e2e.sh` unavailable in this environment (Docker socket permission denied); reported `unavailable`, not a failure. A `DagBag(safe_mode=True)` substitute confirms clean imports and correct task ordering on both DAGs, but the authoritative e2e script (`airflow dags list-import-errors` inside the real container stack) has not run and should run manually before merge per tasks.md 5.2.

---

## Verification Traceability

Per the Final-State Authority hierarchy, findings are sourced from:

- **Highest**: explicit final-state facts in orchestrator launch prompt (this document)
- **Mid**: verify-report (obs #2646, at verification time)
- **Lowest**: apply-progress (intermediate snapshots, per submit)

When sources disagree, higher-ranked sources govern this report. All stale claims from apply-progress are noted as such with their timestamps.

### Key Facts vs. Verify-Report Claims

| Claim | Source | Status |
|-------|--------|--------|
| All 4 apply slices merged to origin/dev | Launch prompt | ✅ CONFIRMED (PRs #523, #529, #533, #540, #541) |
| Verification returned PASS WITH WARNINGS | Verify-report obs #2646 | ✅ CONFIRMED (0 CRITICAL, 8/8 reqs, 12/12 scenarios) |
| 5085 tests passed, 34 skipped | Verify-report obs #2646 | ✅ CONFIRMED |
| Phase 5 (post-merge) intentionally OPEN | Launch prompt | ✅ CONFIRMED (tasks 5.1–5.6 marked [ ] per design) |
| Three slices with size:exception | Launch prompt | ✅ CONFIRMED (2a: 743 lines, 3: 921 lines, 4: 562 lines per ledger) |
| DagBag(safe_mode=True) import check passed | Launch prompt | ✅ CONFIRMED (congress_youtube_chapter_uploader 15 tasks, reap_shorts_uploader 6 tasks, verify_final_copy wired correctly at both seams) |

---

## Migration 050: Pre-Deployment Requirement

**CRITICAL ALERT**: Migration 050 must be applied in BOTH schemas (`development` and `production`) before any of slices 2, 3, or 4 reach production.

**Per design.md** (Migration / Rollout section) and **final-state facts**: 
- Slice 1 (migration only) may land in production without slices 2–4.
- Slices 3 and 4 will FAIL LOUDLY (`ValueError` in verifier task) if migration 050 columns are missing at publish time.
- Slice 2 (module only, no DAG consumers) is inert until slices 3/4 wire it.

**Deployment verification tasks (Phase 5, post-merge)**:
- [ ] 5.3 Apply migration 050 to NAS `development` schema via `migrations_dag`. Verify by querying both tables' columns, NOT by trusting DAG-run status (precedent: issue #467 / migration 047, which failed with permission denied in BOTH schemas after infra-security cutover; fix was `GRANT REFERENCES ON ALL TABLES` to the owner).
- [ ] 5.4 Apply migration 050 to NAS `production` schema via `migrations_dag`. Verify identically.
- [ ] 5.5 Confirm migration success in BOTH schemas BEFORE slices 3 and 4 reach production.
- [ ] 5.6 `git_sync` both stacks after each slice deploys; confirm `airflow dags list-import-errors` empty.

---

## Documented Deviations from Design (Intentional)

All documented in apply-progress and verified by code reading:

1. **Evidence tri-valued `"mencionados"` field**: keyed as Spanish `"mencionados"` per design.md D5's explicit JSON text, while all other evidence keys remain English. Verified by direct code inspection.

2. **`_copy_verification_evidence` duplicated across DAGs**: Functions in `youtube_upload_dag.py` and `reap_shorts_uploader_dag.py` are identical in structure and keying, deliberately NOT shared via cross-DAG-file import (no precedent for that in the codebase). Shorts version takes pre-fetched `chapter` and `turn_speaker_row` dicts instead of querying; no `thumbnail_text` key (no thumbnail step exists on shorts seam). Duplication intentional, not divergent. Confirmed identical by code reading; both exhibit the same canonical/raw display-name split.

3. **Test shape for `run_correction_round`**: Targets the bounded-correction contract internals (the seam), not the prompt content (which is LLM-owned). Party-variant false-positive avoidance rule is locked by prompt content assertion (`TestPinnedPromptContent::test_system_prompt_states_the_party_variant_rule`), not behavioral proof, which is appropriate for LLM-owned semantic judgments. Documented in verify-report.

---

## Task Completion Status

### Implementation Scope (Phases 1–4)
All 32 tasks complete and marked [x] in persisted tasks.md:
- Phase 1: 6/6 ✅
- Phase 2: 11/11 ✅
- Phase 3: 13/13 ✅
- Phase 4: 8/8 ✅

### Deployment Verification Scope (Phase 5, Post-Merge)
- 0/6 tasks complete, marked [ ] — intentionally OPEN per design.md and tasks.md
- **Not sdd-apply scope**: deployment verification, migration application, and production queries are post-merge obligations
- **Blocking conditions**: migration 050 must be successfully applied in both schemas before slices 3/4 go live; failure to apply → writes fail loudly at publish time (no silent data loss)

---

## Git State & Commit

**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-512` (branch `feat/512-verify-final-copy`)
**Archived from**: `openspec/changes/verify-final-copy-before-publication/`
**Archived to**: `openspec/changes/archive/2026-09-09-verify-final-copy-before-publication/`
**Spec synced to**: `openspec/specs/final-copy-verification/spec.md` (new, created from delta)

**Artifacts in archive**:
- proposal.md ✅
- design.md ✅
- tasks.md ✅
- specs/final-copy-verification/spec.md ✅
- verify-report.md ✅
- archive-report.md (this file) ✅

---

## Success Criteria (from proposal.md)

All met:

- [x] Verifier is a distinct LLM call, receiving title/description/thumbnail_text as separate named inputs plus trusted evidence
- [x] Checks politician names, party names, spelling, grammar, language use; returns `pass|correctable|reject` with field-level findings
- [x] At most one correction round, followed by recheck; corrections never invent identities, parties, or claims
- [x] Turn-title `reject` blocks publication; every other field/path persists verdict and publishes existing fallback
- [x] Verdict, original, corrected, and findings persisted idempotently; nothing written when inconclusive/unsupported
- [x] Verifier unavailable/inconclusive preserves safe fallback, observable via accumulator
- [x] Tests cover: consistent copy, politician-name correction, party correction, spelling/grammar correction, speaker/person mismatch, unsupported claims, malformed verifier output, verifier failure, thumbnail-text flag-without-correction, audit-write idempotency
- [x] `uv run pytest`, `ruff check`, and `ruff format --check` pass on every slice

---

## Rollback Capability

**Slice 1 (migration 050)**: Revert PR #523. Migration 050 is fully reversible with the commented `DOWN` block (drops all 17 added columns). No consumers yet, no data loss.

**Slices 2–4**: Revert PRs #533, #540, #541 in reverse order. Verifier module is purely additive; with the DAG tasks removed, both seams behave exactly as today. All guard logic is defensive (inconclusive → publish existing fallback, never raise unless it's the documented turn-title `reject`).

**Complete rollback**: `git revert` all four PRs in reverse order (4, 3, 2, 1). No data loss. Both seams revert to pre-verification behavior.

---

## Key Learnings

1. **Size-exception pattern**: When core bounded-correction flow cannot be split without coupling cost (validation/parsing/containment/correction), accept the oversized slice rather than fragment it artificially — document it, require ledger reset, and monitor for similar future changes.

2. **Display-name canonicalization**: A cross-DAG helper would reduce duplication but breaks the codebase's DAG-independence convention (verified: no other DAG file imports another DAG file). Lightweight parity test between the two `_copy_verification_evidence` copies mitigates drift risk cheaply.

3. **Migration pre-deployment dependency**: Schema changes blocking DAG behavior (writes fail loudly if columns missing) must be applied before the wiring code goes live. Query columns to confirm, never trust DAG-run status alone (precedent: infra-security cutover broke ownership).

4. **Inconclusive verdict strategy**: Verifier failure is not a publish failure — it is an observable soft-skip with full fallback. This preserves the distinction between a correctness concern (worth raising on title) and an observation (worth logging and flagging on description/thumbnail).

5. **Prompt-owned semantic rules**: Party-variant false-positive avoidance is entirely prompt-resident logic (no code implements party matching). Test governance via pinned-prompt-content assertions, not behavioral proof.

---

## Archive Completion

✅ **Spec merged**: Delta spec from `openspec/changes/verify-final-copy-before-publication/specs/final-copy-verification/spec.md` copied to `openspec/specs/final-copy-verification/spec.md` via mechanical shell copy with diff verification.

✅ **Change folder archived**: Source `openspec/changes/verify-final-copy-before-publication` moved to `openspec/changes/archive/2026-09-09-verify-final-copy-before-publication` via `git mv` with full recursive snapshot and diff readback.

✅ **Archive report persisted**: This report saved to Engram topic `sdd/verify-final-copy-before-publication/archive-report`.

✅ **Commit pending**: Phase output ready for commit on `feat/512-verify-final-copy` with conventional message (not pushed, per instructions).

---

**Archive Phase Complete** — SDD cycle closed. Change ready for orchestrator's post-merge deployment verification (Phase 5, per tasks.md).
