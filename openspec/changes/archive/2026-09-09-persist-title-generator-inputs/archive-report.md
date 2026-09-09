# Archive Report: Persist Title Generator Input Payloads (Issue #549)

## Executive Summary

Change `persist-title-generator-inputs` is complete, verified (PASS), and archived. All three slices implemented, unit-tested, and integrated successfully. The change delivers title-generation-provenance capability: serialized input payloads for both live title generators (turn path and shorts path) are now persisted at generation time, enabling #510's title-eval corpus. Deployment to dev, main, and production NAS is pending; migration 051 must run on both dev and prod schemas via the migrations DAG.

## Delivery Summary

### Scope Delivered

**New Capability**: `title-generation-provenance`
- Turn path: serialized payload (summary, best, sibling_titles, key_speakers, forbidden_title, participant_slug, title) persisted on `speaker_turn_videos.title_generation_input` (JSONB, nullable).
- Shorts path: serialized payload (transcript slice, truncation flags, chapter/speaker/topic metadata, generated title) persisted on `video_shorts.title_generation_input` (JSONB, nullable).
- Both payloads include the resulting title alongside input, written atomically on same statement.
- Credential exclusion enforced via allowlisted builder functions (no URLs, paths, or tokens reach storage).
- Failure isolation: DB write failures log and continue; never block publication.

### Three-Slice Implementation

**Slice 1** — `feat/549-slice1-migration-db` (commit 12f3728)
- Migration 051: two nullable JSONB columns added to speaker_turn_videos and video_shorts
- production_schema.sql: both ADD COLUMN lines mirrored for drift detection
- CongressionalVideoDB: record_title_generation_input_turn and record_title_generation_input_short methods
- Unit tests: SQL text, bind format (::jsonb), rowcount passthrough, ValueError guards, Scenario 3.1 (grouped sibling update), Scenario 3.2a (re-run overwrite)
- **PR #551** (base: dev)

**Slice 2a** — `feat/549-slice2a-turn-payload` (commit bc26c29)
- build_turn_title_payload pure builder: explicit literal keys, reduced best dict (label/style/prompt), key_speakers normalized (str kept, dict → name, anything else dropped)
- _task_thumbnail_result XCom hook: pulls fetch_recent_history, builds payload, returns as title_generation_input alongside 4 legacy keys; _task_generate_title return type (str) unchanged
- Tests: declared-keys-only assertion, recursive credential/URL/path scanner, best reduction verification, sibling_titles forwarding
- **PR #552** (base: dev)

**Slice 2b** — `feat/549-slice2b-parent-hook` (commit 868f0a4)
- trigger_thumbnail_generation integration: optional payload key in strict result validation (excluded from conjunction to avoid degrading valid titles), payload extracted with .get(), missing/non-dict logged as WARNING with status="skipped"
- record_title_generation_input_turn call site: try/except wrapper following upload_marking.py convention, uses thumbnail_config["output_path"] as key (not child's returned thumbnail.png path), logs no_row when rowcount==0, XCom pushes title_provenance status
- Tests: Scenario 3.2b (non-matching-key-is-loud), Scenario 5.1 (optional payload validation), Scenario 7.1 (_prepare_upload_config unchanged)
- **PR #553** (base: dev)

**Slice 3** — `feat/549-slice3-shorts-path` (commit c9d0e31 → edb557f after minor formatting)
- build_shorts_title_payload pure builder: transcript capped at 2000 chars (exact slice fed to template), transcript_truncated bool, transcript_full_length, scoring_reasoning capped at 500 chars, mentioned_display_names conditional
- _generate_metadata integration: assembles payload, calls record_title_generation_input_short(short_id), try/except wrapper pushes status to metadata["title_provenance"]
- Tests: Scenario 4.1 (transcript truncation flags), Scenario 7.2 (shorts metadata unchanged except for title_provenance field), Scenario 6.1 (shorts path builder credential exclusion)
- Contract tests: exact dict keys, recursive scanners, LLM-branch condition (no payload on fallback)
- **PR #554** (base: dev; labelled size:exception)

## Design Corrections Applied

### C1 — Test Structure vs. Task Wording (Deviation Documented)

**Issue**: Task 1.3 directed adding `title_generation_input` to the `TABLE_COLUMNS` tuple entries for *both* speaker_turn_videos and video_shorts. Actual discovery: video_shorts uses a separate constant, `VIDEO_SHORTS_COLUMNS`, inside TestVideoShortsTableSnapshot (not TABLE_COLUMNS dict).

**Resolution**: Updated both structures. Intent (make drift check catch missing column) fully satisfied; only task's named-constant wording was imprecise. Confirmed RED→GREEN: both `test_column_present_in_block[title_generation_input]` tests (speaker_turn_videos and video_shorts) failed before schema mirror and passed after. Recorded as Deviation 1 in apply-progress.

### C2 — Payload Key Placement: XCom Rebuild vs. Signature Widening (Design D1)

**Issue**: Proposal suggested updating both consumers of _task_generate_title (currently returns str; proposal had to widen it to dict to carry payload). This breaks _task_persist_results:291 and _task_thumbnail_result:332 which both pull task_ids="generate_title" as str | None. An in-flight run resumed across deploy would crash.

**Resolution**: Keep _task_generate_title's return type (str) untouched. Build the payload **inside _task_thumbnail_result**, which already pulls validate_input, choose_best_option, and generate_title. Payload is a pure function of immutable XCom values, so rebuilding is byte-exact with what the prompt consumed. Risk 1 (breaking existing consumers) dissolved. Limitation: manual clear+rerun of fetch_recent_history between the two tasks would desync sibling_titles; accepted and not reachable in normal scheduling.

### C3 — Optional Payload in Strict Result Validation (Design D2)

**Issue**: trigger_thumbnail_generation's conjunction validates exactly [success, chapter_id, output_path, title] (line 808-816). Adding title_generation_input to that conjunction would mean a missing or malformed payload degrades a good title into _thumbnail_failure (line 818), violating the acceptance criterion "persist when generated, not gated behind upload success."

**Resolution**: title_generation_input is NOT added to the conjunction. It is read after the conjunction passes via result.get("title_generation_input"). Missing or non-dict payload logs WARNING and records status="skipped" in the title_provenance XCom. Valid title never degrades to thumbnail failure due to payload issues. Risk 2 resolved.

### C4 — Write Key and Loud No-Match (Design D3)

**Issue**: Child DAG returns the reconciled thumbnail.png path as output_path (generic_thumbnail_generator_dag.py:340). If the turn write naively used child's output_path as the key, it would write to the wrong row (thumbnail path ≠ turn video path). Additionally, record_copy_verification_turn uses an IS DISTINCT FROM guard for idempotency; without such a guard, rowcount==0 is ambiguous: did the key not match, or did the row already have that payload?

**Resolution**: Write key is thumbnail_config["output_path"] (the turn video.mp4, set at youtube_upload_dag.py:427), read from local variable scope at line 755. No IS DISTINCT FROM guard (unlike record_copy_verification_turn) because the design explicitly rejects idempotency: re-running overwrites unconditionally (last generation is the published one), which is correct. rowcount==0 is unambiguous: no row matched the key. The call site logs WARNING and records status="no_row" instead of raising. Risk 3 resolved without a raise.

## Verification Evidence

**Verdict**: PASS (0 CRITICAL, 0 WARNING, 3 SUGGESTION)

**Specification Coverage**:
- Requirements: 7/7 (all met)
- Scenarios: 11/11 (note: spec has 11 heading instances; Scenario 3.2 holds two distinct behaviors—3.2a: re-run overwrite, 3.2b: non-matching-key-is-loud—reflected in tasks as 3.2a/3.2b)

**Test Execution**:
```
$ uv run pytest -q
5147 passed, 34 skipped, exit 0 (105-111s runtime)
```
34 skips: live-Postgres-dependent tests correctly skipped in sandbox (connection refused to localhost:5432); one SRT pathological fixture too small in this environment; both pre-existing skip conditions unrelated to this change.

**Lint**:
```
$ uv run ruff check .
All checks passed!
$ uv run ruff format --check .
313 files already formatted
```

**E2E (Docker-dependent harness)**:
```
$ bash scripts/test-airflow-e2e.sh
Result: unavailable (Docker daemon not reachable in this sandbox)
```
Per CLAUDE.md convention: unavailable is not a failure. Task 3.10 flagged this in apply-progress; harness must be run manually against real Docker host before merge to confirm `airflow dags list-import-errors` stays empty for three modified DAGs (generic_thumbnail_generator_dag.py, youtube_upload_dag.py, reap_shorts_uploader_dag.py).

**Diff Summary**:
```
congress_videos/generic_thumbnail_generator_dag.py             |  21 ++
congress_videos/modules/database.py                            |  97 +++++
congress_videos/modules/thumbnail_generation.py                |  70 ++++
congress_videos/reap_shorts_uploader_dag.py                    | 126 +++++++
congress_videos/sql/migrations/051_persist_title_generation_input.sql | 12 +
congress_videos/sql/production_schema.sql                      |   8 +-
congress_videos/youtube_upload_dag.py                          |  64 +++-
tests/congress_videos/modules/test_database.py                 | 158 ++++++++
tests/congress_videos/modules/test_generic_thumbnail_dag.py    |  68 ++++
tests/congress_videos/modules/test_thumbnail_generation.py     | 176 ++++++++++
tests/congress_videos/sql/test_production_schema.py            |   7 +-
tests/congress_videos/test_reap_uploader_dag.py                | 389 +++++++++++++++++++
tests/congress_videos/test_youtube_upload_dag.py               | 144 ++++++++
19 files changed, 2817 insertions(+), 4 deletions (openspec/ + production/test code)
```
Excluding openspec planning artifacts: 13 files, 1336 insertions, 4 deletions of production+test code. All 4 deletions are internal (docstring text, comments); zero pre-existing production logic removed. Change is purely additive.

**Evidence Revision**: sha256:81f4d6f669303e28d98c6d4b61d8cd3f2b8b41687a56f99a74995d3a8e47007f

## Delivery Chain

**Feature Branch Chain** (auto-chain, 400-line budget per PR):
1. `feat/549-generator-input-payloads` (planning commit 0d9d11c) — base for all slices
2. `feat/549-slice1-migration-db` (commit 12f3728) → **PR #551** (base: dev) — 307 changed lines
3. `feat/549-slice2a-turn-payload` (commit bc26c29, off #551) → **PR #552** (base: dev) — 335 changed lines
4. `feat/549-slice2b-parent-hook` (commit 868f0a4, off #552) → **PR #553** (base: dev) — 206 changed lines
5. `feat/549-slice3-shorts-path` (commits c9d0e31 → edb557f, off #553) → **PR #554** (base: dev) — 515 changed lines, labelled **size:exception**

**Ledger Incident and Resolution**:
The attempt ledger BLOCKED on slice 2 after sdd-apply reported the work overran the declared 500-line ceiling (569 changed lines total for slice 2a+2b combined). The orchestrator then split that single unit into 2a (335 lines) and 2b (206 lines) to respect the 400-line review budget, and re-verified both independently. The ledger was cleared with a maintainer `gentle-ai sdd-attempt reset` (request-id `549-reset-slice2-1`). Slice 3 was approved with an accepted `size:exception` label (515 lines = 126 production + 389 tests).

**Current Status** (as of archive time, 2026-09-09):
- All three slices: implemented, tested green, and committed.
- PRs #551-#554: opened and pending review/merge.
- Deployment: **PENDING**. Code branches are ready; they have not been merged to dev/main or deployed to NAS.
- Migration 051: **PENDING** execution on both dev and prod schemas via the migrations DAG once code lands.

## Archive Contents

```
openspec/changes/archive/2026-09-09-persist-title-generator-inputs/
├── proposal.md                                           (9 KB, original proposal with scope, approach, acceptance criteria)
├── design.md                                             (~10 KB, technical approach and architecture decisions D1-D6, contracts)
├── spec.md                                               (spec moved to openspec/specs/title-generation-provenance/spec.md)
├── tasks.md                                              (33/33 tasks completed)
├── explore.md                                            (discovery notes)
├── apply-progress.md                                     (slice-by-slice implementation, deviations, status)
├── verify-report.md                                      (verification verdict: PASS, 5147 tests pass, 0 CRITICAL)
└── archive-report.md                                     (this file)
```

**Main Specs Updated**:
- `openspec/specs/title-generation-provenance/spec.md` — NEW (copied from delta spec)

## What Remains

**Before Merge**:
- Manual E2E harness: `bash scripts/test-airflow-e2e.sh` must be run against a real Docker host to confirm `airflow dags list-import-errors` is clean for the three modified DAGs.

**After Merge to dev/main**:
1. Migration 051 execution on dev schema (via migrations DAG) — must complete before first publish via modified DAGs.
2. Migration 051 execution on prod schema (via migrations DAG on NAS, scheduled after dev validation).
3. Parallel publication tests on both dev and prod to confirm payloads are recorded correctly on live turns and shorts.

**Not Blocked By**:
- Issue #510 (title-eval corpus) — this change unblocks it by persisting the inputs; #510 needs its own separate harness to consume these payloads.
- Any other open issues.

## Task Completion Summary

| Task ID | Status | Description |
|---------|--------|-------------|
| 1.1-1.9 | ✅ Complete | Migration 051, schema mirror, write methods, unit tests |
| 2.1-2.13 | ✅ Complete | Turn path: builder, XCom contract, parent hook, tests |
| 3.1-3.10 | ✅ Complete | Shorts path: builder, _generate_metadata hook, tests |

## Key Learnings

1. Keeping _task_generate_title's return type (str) untouched while building payloads inside _task_thumbnail_result avoided breaking two existing XCom consumers and prevented in-flight resume crashes.

2. Excluding title_generation_input from the strict result validation conjunction allowed optional payload delivery without degrading valid titles, fully decoupling DB persistence from upload success.

3. An ambiguity-free write-key strategy (unguarded UPDATE, rowcount==0 means no match) is simpler and more expressive than idempotency guards when re-runs are intentional (last generation wins).

4. Recordable design deviations (task wording vs. actual test structure, e.g., VIDEO_SHORTS_COLUMNS vs. TABLE_COLUMNS) should be documented transparently in apply-progress rather than silently resolved, maintaining audit trail clarity.

5. Splitting an oversized slice (569 lines → 335+206) at a clean architectural boundary (turn vs. parent integration) and re-verifying both pieces independently preserved the 400-line review budget and improved bisectability without re-implementing.

---

**Archive Date**: 2026-09-09  
**Archived By**: sdd-archive (haiku)  
**Spec Sync**: openspec/specs/title-generation-provenance/spec.md created (NEW)  
**Folder Move**: openspec/changes/persist-title-generator-inputs → openspec/changes/archive/2026-09-09-persist-title-generator-inputs (git mv, verified)
