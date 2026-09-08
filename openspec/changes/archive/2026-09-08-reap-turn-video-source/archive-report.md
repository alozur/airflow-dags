# Archive Report: Reap Turn-Video Source Shorts (#467)

**Date**: 2026-09-08  
**Change**: reap-turn-video-source  
**Issue**: #467  
**Status**: ARCHIVED — Ready for release to main  

---

## Executive Summary

Issue #467 completes the decoupling of short-clip generation from long-form upload cadence by sourcing Reap shorts from diarized speaker-turn video files (`speaker_turn_videos.output_path`) instead of raw chapters. Six stacked PRs merged into `dev` on 2026-09-08, introducing a nullable `turn_id` FK in `video_shorts`, deterministic per-turn candidate selection, leading pre-trim for large turns, and turn-sourced SRT sidecar handling. Verification passed with three WARNING-level findings (stale docs, design supersession, live-DB integration tests); all warnings resolved in post-verify commit d6313be.

---

## Final State (Authoritative)

### Merged Implementation

| PR # | Commit | Content | Status |
|------|--------|---------|--------|
| #468 | a2ab9fd | Migration 047 (nullable `turn_id` FK) + schema snapshot + 218 schema tests | ✅ Merged dev 2026-09-08 |
| #469 | 32412e5 | `get_turn_videos_for_shorts` + `insert_video_short(turn_id=)` surface swap | ✅ Merged dev 2026-09-08 |
| #470 | b339a12 | Preparer rewrite (output_path staging, leading pre-trim, 569 obsolete test deletions) [size:exception 920 lines] | ✅ Merged dev 2026-09-08 |
| #471 | 41abc1c | `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` + processor/sidecar wiring | ✅ Merged dev 2026-09-08 |
| #472 | 0570027 | SRT sidecar full-chapter-span guard per spec | ✅ Merged dev 2026-09-08 |
| #473 | 8e4e104 | Per-turn Tier-1 partition + parent-published gate drop | ✅ Merged dev 2026-09-08 |
| (post-verify) | d6313be | Docs fixes: PIPELINE.md turn-based preparer description + design.md §7 supersession note | ✅ Applied 2026-09-08 |

**Dev head**: 7b29b45 (includes all six PRs)

### Verification

**Verdict**: PASS WITH WARNINGS (per verify-report d4ad3a1)

| Metric | Result |
|--------|--------|
| CRITICAL blockers | 0 |
| Requirements | 8/8 covered |
| Scenarios | 19/19 compliant |
| Test suite | 4613 passed, 32 skipped (Postgres-dependent), 0 failed |
| Coverage | 90.59% (threshold 80%) ✅ |
| Ruff (lint/format) | ✅ All checks passed |
| DagBag import | 16 DAGs, 0 errors ✅ |
| E2E gate | Applicable (congress_videos touched) → Docker unavailable (sandbox), reported per repo policy |

### Warnings Resolved

Per launch prompt final-state facts and post-verify commit d6313be:

1. **Docs drift (PIPELINE.md, lines 110–111)** — stale "capítulos elegibles" + "IA + contexto SRT" clauses
   - **Fixed in d6313be** ✅ Rewritten to describe turn-video selection + deterministic leading-window pre-trim

2. **Design supersession (design.md §7, turn-sourced sidecar window math)**
   - **Context**: Design §7 sketched a group-span window formula. The spec (short-video-srt-artifacts/spec.md lines 13–18) mandates unconditional full-chapter-span fallback for turn-sourced clips (override).
   - **Resolution**: Spec has higher precedence per SDD hierarchy. Implementation correctly follows spec. 
   - **Fixed in d6313be** ✅ Design.md now documents the supersession explicitly

3. **Live-Postgres behavioral tests (test_get_pending_shorts_sql.py)** — four Tier-1 partitioning scenarios skip in sandbox (no reachable Postgres)
   - **Context**: `test_two_turn_groups_in_one_chapter_get_independent_tier1_caps`, `test_mixed_legacy_and_turn_rows_partition_independently_in_one_chapter`, `test_uploaded_top_clips_consume_chapter_tier1_slots`, `test_unpublished_parent_chapter_is_still_returned` all skip cleanly.
   - **Resolution**: Spec-matching unit-level SQL-shape tests (`test_reap_db_methods.py`) pass; live-Postgres suite must run on NAS before merge per repo convention for this test file.
   - **Outstanding** ⚠️ Orchestrator Phase 5 will exercise these via `congress_reap_clip_preparer` manual trigger on prod (tasks.md Phase 5.5)

### Spec Merges

#### Added: `openspec/specs/reap-turn-sourced-clips/spec.md`

Copied from delta (no main spec existed prior). Defines:
- `video_shorts.turn_id` nullable FK with `ON DELETE SET NULL`
- Turn-video candidate selection with 4 gates (Source, Procedural, Floor 120s, Dedup)
- Preparer consuming materialized turn output + leading pre-trim logic
- Zero-eligible logging
- Claim ordering with legacy compatibility via LEFT JOIN
- Tier-1 partitioning by `turn_id` with `chapter_id` fallback
- `get_chapters_for_shorts` removal requirement

**8 requirements, 15 scenarios** — all compliant per spec compliance matrix in verify-report.

#### Modified: `openspec/specs/short-video-srt-artifacts/spec.md`

Replaced "Short SRT window derivation with fallback" requirement (lines 49–68):
- **Before**: Chapter-relative formula when offsets present; fallback to full chapter when absent
- **After**: Distinguishes chapter-sourced (`turn_id IS NULL`) from turn-sourced (`turn_id IS NOT NULL`)
  - Chapter-sourced: unchanged (chapter-relative when offsets present, full span when absent)
  - Turn-sourced: **always** full chapter span (offsets are file-relative, not chapter-relative)
- Scenario count: 2 → 4 (split pre-existing chapter scenarios, added 2 turn-sourced scenarios)

**4/4 new scenarios** — all compliant per spec compliance matrix in verify-report.

### Product Decisions Confirmed

Per launch prompt:
- Both parent-published gates dropped (fully committed to per-turn throughput decoupling) ✅
- `turn_id` FK dedup instead of `COALESCE` + `DISTINCT ON` ✅
- Per-turn Tier-1 partition with per-chapter fallback for legacy rows ✅
- `get_chapters_for_shorts` deleted (zero production callers) ✅
- No `prepared_at` or relevance thresholds added to gate (keep floor simple) ✅

### Production Validation (Read-Only)

Per launch prompt: orchestrator ran design.md read-only validation query on production schema (2026-09-08):
- 32 eligible turn groups (28 in 120–900 s window, 4 to pre-trim, 11 with parent already uploaded)
- Baseline: ~1 chapter/day via old selection
- **Outcome**: ~30× throughput increase expected (32 turns vs. 1 chapter per run cycle)

### Tasks Status

**Phases 1–4b (27 implementation tasks)**: All `[x]` complete per `tasks.md` and git history.

**Phase 5 (5 orchestrator-run ops)**: Intentionally `[ ]` — post-merge NAS deployment and migration application, explicitly outside `sdd-apply` scope.
- 5.1 Apply migration 047 on NAS development schema
- 5.2 Apply migration 047 on NAS production schema
- 5.3 `git_sync` both stacks; confirm `airflow dags list-import-errors` empty
- 5.4 Run read-only prod validation query
- 5.5 Manually trigger `congress_reap_clip_preparer` on prod; confirm staged turn-sourced clips

---

## Artifact Traceability

### Source Artifacts (from change folder)

- **exploration.md** — Initial feasibility and scope (2026-09-07)
- **proposal.md** — Change framing, approach, rollback plan (2026-09-07)
- **specs/**
  - `reap-turn-sourced-clips/spec.md` (ADDED) — new domain
  - `short-video-srt-artifacts/spec.md` (delta) — updated requirement
- **design.md** — Architectural decisions and data flow (2026-09-07, updated 2026-09-08 post-verify)
- **tasks.md** — 5 phases, 32 work items, chain strategy stacked-to-main (2026-09-07, updated 2026-09-08 for Phase 5 note)
- **apply-progress.md** — Implementation snapshot as PRs merged (2026-09-08)
- **verify-report.md** — Full verification with spec compliance matrix (2026-09-08, observation d4ad3a1)

### Main Specs (Updated)

- **openspec/specs/reap-turn-sourced-clips/spec.md** (NEW) — 8 requirements, 15 scenarios
- **openspec/specs/short-video-srt-artifacts/spec.md** (MODIFIED) — 1 requirement updated, 4 scenarios (was 2)

---

## Outstanding Follow-Ups

Per launch prompt and verify-report:

1. **Within-tier ordering decision** (design.md follow-up note): Within a Tier-1 partition, `ORDER BY vc.youtube_upload_date DESC NULLS LAST` places turn-sourced shorts whose long-form parent is unpublished last. Decide whether to order by a turn-side recency key instead (e.g., `speaker_turn_videos.session_date DESC`).

2. **Live-Postgres suite** (pre-merge gate): Four behavioral Tier-1 partitioning scenarios in `test_get_pending_shorts_sql.py` must be exercised on the NAS or CI-with-Postgres before merge is considered fully verified (repo convention for this test file).

---

## Issue Closure

**Issue #467** closes when the release PR reaches `main`. The archive contains the complete implementation, verified state, and handoff for the orchestrator's Phase 5 NAS deployment.

---

## Deployment

Pending: orchestrator appends NAS evidence after release.
