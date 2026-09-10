# Archive Report: Session Intro-Card Overlay for Long-Form Uploads

**Change**: session-intro-card-overlay (GitHub issue #558)  
**Archive Date**: 2026-09-10  
**Archived Path**: `openspec/changes/archive/2026-09-10-session-intro-card-overlay/`  
**Tracker Branch**: `feat/558-session-intro-card` @ `019f7d8` (merged, ready for base repo delivery)

## Change Summary

A new task `apply_intro_overlay` (t5b) was added to the `youtube_upload_dag` to burn a 5-second session-identification card onto every long-form video before upload. The feature:
- Defines an `intro_sesion` tipo in `video_editor_config.py` and registers a Pillow renderer
- Implements a pure overlap-resolution helper (`resolve_overlay_slot`) for future collision detection
- Adds proportional timeout (`max_timeout` kwarg) and a hard duration guard to prevent infinite-duration stalls
- Wires t5b between t5 (`extract_chapter_videos`) and t6 (`prepare_upload_config`), updating the `chapter_extraction_results` XCom in-memory only
- Builds card text from session metadata (Spanish titulo/descripcion per D6 precedent)
- Guarantees source file immutability, same-directory sibling output, and DB invariant preservation

Scope: 141 production lines, 587 test lines across 3 files + test files, organized as 4 chained PRs delivering 41 atomic work units.

## Final State Authority

The following facts are the authoritative terminal record per the SDD Archive Final-State Authority hierarchy:

**Rank 1 — Persisted tasks artifact**: All 41 implementation tasks in `tasks.md` are marked `[x]` complete.

**Rank 2 — Orchestrator's explicit final-state facts** (supersede any stale snapshot claims):
1. The single `sdd-verify` WARNING is CLOSED. Commit 416afd1 added `TestApplyIntroOverlaySuccess::test_card_text_from_build_intro_card_text_reaches_the_overlay_conf`, which was mutation-checked (replacing the `titulo` wiring with a constant makes it fail). Final findings: **0 CRITICAL, 0 WARNING, 3 SUGGESTION**.
2. Final test numbers: `uv run pytest -q` → **5200 passed, 34 skipped**, exit 0. (`verify-report.md` recorded 5199; the one additional test is the WARNING-closure test added in 416afd1.)
3. All 41 tasks complete. All 11 requirements and 16 scenarios traced to named passing tests.
4. All 4 PRs merged on 2026-09-10: #562 (tipo + renderer), #563 (overlap helper), #564 (timeout guard), #565 (DAG wiring). Base was `origin/main` @ `96b4d8f`.
5. `size:exception` accepted: PR #565 was 728 changed lines (141 production + 587 test); attempt ledger was reset with `--objective-relation independent`.
6. Docker e2e unavailable (exit 4, `EXIT_DOCKER_UNAVAILABLE`). Documented substitute (`airflow dags list-import-errors` on prod NAS after `git_sync`) will be run by orchestrator, not by this archive phase. Honestly reported as unavailable, not fabricated as a pass.

**Rank 3 — Intermediate snapshots** (`verify-report.md` at verification time):
- `verify-report.md` (evidence_revision sha256:9c1426b3aa1efcd166ba84f49d1b35ccc91d90df62c936854e63ed5e7c348816) recorded 5199 passed, 1 WARNING, 3 SUGGESTIONS. The WARNING-closure test (416afd1) was added after that snapshot, narrowing findings to 0 CRITICAL, 0 WARNING, 3 SUGGESTION.
- All 41 tasks were already marked complete at verification time.
- TDD compliance and assertion quality confirmed by independent re-run in verify pass.

## Spec Sync

| Domain | Action | Status |
|--------|--------|--------|
| `session-intro-card-overlay` | New main spec created | ✅ Complete |

**Details**: Delta spec `openspec/changes/session-intro-card-overlay/specs/session-intro-card-overlay/spec.md` was the first and only spec for this domain. No existing main spec existed; the delta spec was copied mechanically to `openspec/specs/session-intro-card-overlay/spec.md` with `diff -r` verification. No merge required.

**Spec content**: 11 requirements with 16 scenarios covering intro-card style, renderer registration, overlap resolution, default window, card text, in-memory output-path overwrite, DB invariant, source immutability, same-directory sibling, idempotent retry, and bounded fail-loud behavior.

## Archive Contents

- ✅ `proposal.md` — Original proposal with scope, approach, rollback plan
- ✅ `exploration.md` — Research phase findings
- ✅ `evidence-encode-benchmark.md` — Performance benchmark evidence
- ✅ `design.md` — Technical decisions (D1–D5), data flow, interfaces
- ✅ `specs/session-intro-card-overlay/spec.md` — 11 requirements, 16 scenarios (now also copied to main specs)
- ✅ `tasks.md` — 41 atomic tasks (4 PRs, 7+8+7+19 tasks per slice), all marked `[x]`
- ✅ `apply-progress.md` — Implementation progress, TDD cycle evidence for all 4 slices
- ✅ `verify-report.md` — Verification report: 5199 tests passed initially, 1 WARNING, 3 SUGGESTIONS
- ✅ `archive-report.md` — This file

**Task completion**: 41/41 tasks marked complete. No stale checkboxes. Spot-checked implementations confirm all tasks delivered:
- PR 1: `intro_sesion` tipo, `_render_intro_sesion` renderer, registration (7 tasks)
- PR 2: `resolve_overlay_slot` pure helper, `INTRO_WINDOW_SECONDS` constant (8 tasks)
- PR 3: `max_timeout` kwarg, duration guard, constants (7 tasks)
- PR 4: t5b wiring, `_build_intro_card_text`, `_apply_intro_overlay`, DAG task-count fixes, dependency chain insertion (19 tasks)

## Verification Summary

**Build & Tests** (independently re-run per orchestrator's final-state facts):
- `uv run ruff check .` → All checks passed
- `uv run ruff format --check .` → 313 files already formatted
- `uv run pytest -q` → **5200 passed, 34 skipped** (34 skips are pre-existing, environment-gated, unrelated to this change)
- Coverage gate (`--cov-fail-under=80`) passed with exit 0

**Findings** (per orchestrator's final-state facts):
- **CRITICAL**: 0
- **WARNING**: 0 (closure test added in commit 416afd1)
- **SUGGESTION**: 3 (resolve_overlay_slot unused/dead-code note, PR #565 size:exception size-exception already accepted, Docker e2e honestly unavailable)

**Compliance Matrix** (16/16 scenarios):
- All 11 requirements verified against named, real, passing tests
- All 16 scenarios have covering test cases with triangulation (e.g., 5+ cases for renderer, 8 for overlap helper, 4+4 for timeout guard, 5 for card text, 5 for fail-loud)
- TDD cycle evidence confirms RED→GREEN→REFACTOR progression for all 41 tasks
- Regression tests pin the DB landmine (`turn_id` always set by t6, keeping the `mark_turns_uploaded_by_output_path` fallback unreachable)

**Scope boundary verification**:
- `git diff --stat origin/main...HEAD` confirms ZERO changes to `reap_clip_preparer_dag.py`, `reap_processor_dag.py`, `reap_shorts_uploader_dag.py`, `speaker_turn_videos_dag.py`, or any DB schema
- Only 3 production files touched: `congress_videos/config/video_editor_config.py`, `congress_videos/modules/video_editor.py`, `congress_videos/youtube_upload_dag.py`
- Changes are cohesive, unrelated DAGs remain untouched

## Delivery Chain

All 4 PRs merged on 2026-09-10 in the tracker branch:

| PR | Head | Base | Content | Author |
|---|---|---|---|---|
| #562 | `feat/558-slice1-tipo-renderer` | tracker | `intro_sesion` tipo + renderer + registration | Claude Opus 5, Alonso Zurera |
| #563 | `feat/558-slice2-overlap-helper` | PR #562 | pure `resolve_overlay_slot` + `INTRO_WINDOW_SECONDS` | Claude Opus 5, Alonso Zurera |
| #564 | `feat/558-slice3-timeout-guard` | PR #563 | `max_timeout` kwarg + duration guard | Claude Opus 5, Alonso Zurera |
| #565 | `feat/558-slice4-dag-wiring` | PR #564 | `apply_intro_overlay` (t5b) wiring, 15→16 tasks | Claude Opus 5, Alonso Zurera |

Tracker branch: `feat/558-session-intro-card` @ `019f7d8`  
Base (starting point): `origin/main` @ `96b4d8f` (2026-09-10, post #556)

**Review workload**:
- Forecast: High risk (555 total lines estimated; actual PR #565 was 728 changed lines)
- Strategy: auto-chain / feature-branch-chain (4 slices)
- Acceptance: `size:exception` used; attempt ledger reset with `--objective-relation independent`
- Risk management: Each slice autonomous, testable, rollbackable; no unbounded work in a single PR

## Follow-Up Items

### Named Follow-Up: Disk Growth from Permanent `_edited` Copies

**Issue**: Every long-form upload now leaves a full re-encoded sibling file (`_edited` path) alongside the original. No cleanup mechanism exists anywhere in the codebase to remove these copies.

**Scope Decision**: This was an **explicit, deliberate scope exclusion for #558**, not an oversight. The overlay feature itself ships complete, working, and tested. Automatic cleanup was deferred as a separate concern suitable for a follow-up issue.

**Recommendation**: The orchestrator should file a new GitHub issue to track cleanup of permanent `_edited` files (e.g., scheduled purge of `_edited` copies older than N days, or a manual maintenance task).

**Impact**: Storage growth is linear with upload volume; long-running instances should monitor disk usage. This does not block publication or correctness.

## Archive Verification

**Mechanical copy contract**: All artifacts copied with shell only (`cp -R`, `git mv`), never via model Read/Write.

**Diff verification**: 
```
$ diff -r /tmp/snapshot-source openspec/changes/archive/2026-09-10-session-intro-card-overlay/
(no output — exact match)
```
Empty diff output confirms byte-identity; no truncation or alteration during move.

**Contents verified**:
- [ ✅ ] Main specs updated correctly (`openspec/specs/session-intro-card-overlay/spec.md` created)
- [ ✅ ] Change folder moved to archive with ISO date prefix
- [ ✅ ] Archive contains all artifacts (proposal, specs, design, tasks, apply-progress, verify-report)
- [ ✅ ] Archived `tasks.md` has all 41/41 implementation tasks marked `[x]`, no stale checkboxes
- [ ✅ ] Active changes directory no longer has this change (source gone after `git mv`)
- [ ✅ ] Verbatim `diff -r` readback output confirms empty diff (no differences)

## SDD Cycle Complete

The change has been fully planned, implemented, verified, and archived. All artifacts are now in:
- **Archived folder**: `openspec/changes/archive/2026-09-10-session-intro-card-overlay/`
- **Main specs**: `openspec/specs/session-intro-card-overlay/spec.md`
- **Active code**: Merged to tracker branch `feat/558-session-intro-card` @ `019f7d8`, ready for base-repo delivery (merge to `dev`/`main` per ordinary repository policy)

No further SDD phase work is required. Delivery, merging to main, and production deployment follow ordinary repository policy.
