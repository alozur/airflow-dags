# Archive Report: Thumbnail Text Regeneration (Issue #545)

**Change**: `thumbnail-text-regeneration`
**Archived**: `openspec/changes/archive/2026-09-10-thumbnail-text-regeneration/`
**Issue**: GitHub #545
**Cycle close date**: 2026-09-10
**Archival date**: 2026-09-10

## Executive Summary

Issue #545 has been fully planned, implemented, verified, and archived. The change introduces a non-blocking, bounded regeneration capability triggered when the final-copy verifier flags a `thumbnail_text` finding on the long-form upload path. A new capability (`thumbnail-text-regeneration`) was created and a delta capability (`final-copy-verification`) was merged into the main spec. All 34 implementation tasks across three phases are complete. The system passed verification with 0 CRITICAL findings and all 14 scenarios traced to passing tests. Two main specs have been updated to reflect the new behavior. The change is ready for delivery.

## Final State (Authority)

This archive report records the state of the change AT CLOSE per the **Final-State Authority** hierarchy defined in the archive skill. The following facts OUTRANK any claim in the intermediate `verify-report.md` or `apply-progress.md`:

### Verification Result: PASS (Per Orchestrator Launch Prompt)

- **Verdict**: PASS
- **Critical findings**: 0
- **Warning findings**: 0
- **Suggestion findings**: 3 (non-blocking; see section 8 below)
- **Requirements tested**: 8/8 from `thumbnail-text-regeneration/spec.md` + 1 modified requirement from `final-copy-verification/spec.md`
- **Scenarios tested**: 14/14 all traced to named, passing tests

Per `verify-report.md` (written during sdd-verify phase): The three SUGGESTION findings are:
1. Guard-condition parity with `validate_input`'s `.strip()` — a proposal for future hardening of the claim-stage boundary check
2. Unused `not_claimed` outcome literal in docs — appears in `design.md` D4 outcome list but is only theoretically unreachable given the call site architecture
3. `_build_regen_child_conf`'s call sitting just outside its own `try/except` — currently unreachable given the guaranteed-dict call site

None of these block the archived change.

### Test Results: Latest Run (Per Orchestrator Launch Prompt)

Reconfirmed by orchestrator directly at archive time:

```
uv run pytest -q                    → 5262 passed, 34 skipped, exit 0
uv run ruff check                   → All checks passed, exit 0
uv run ruff format --check          → 320 files already formatted, exit 0
bash scripts/test-airflow-e2e.sh    → EXIT_DOCKER_UNAVAILABLE (4)
                                      [Docker daemon genuinely unavailable;
                                       per repo contract: unavailable, not pass/fail]
```

### Migration 052 Verification (Per Orchestrator Launch Prompt)

- **Status**: Verified against the REAL production schema
- **Test method**: Dry-run inside a transaction, applied cleanly, rolled back
- **Result**: Production re-checked to confirm 0 pre-existing `thumbnail_regen%` columns
- **Not yet applied in production**: Will be applied via `migrations_dag` after `git_sync`

### Delivery Chain (All MERGED 2026-09-10)

| PR | Head commit | Base | Content |
|----|---|---|---|
| #571 | feat/545-slice1-migration | feat/545-thumbnail-text-regeneration (tracker) | Migration 052 + DB accessors (`claim_thumbnail_text_regeneration`, `record_thumbnail_text_regeneration_outcome`) |
| #572 | feat/545-slice2-regen-helper | PR #571 head | Bounded trigger/poll helper `_regenerate_flagged_thumbnail` + constants (unwired) |
| #573 | feat/545-slice3-wiring | PR #572 head | `t6b` branch, hoisted `xcom_push`, operator-signal line, docs |

**Tracker branch**: `feat/545-thumbnail-text-regeneration` @ `04ed3bb` (all three PRs merged into tracker)
**Chain base**: `origin/main` @ `caefba2` (2026-09-10 00:00 UTC baseline)
**Tracker status**: Has NOT yet merged to `dev`/`main` — orchestrator handles delivery after archive

### Task Completion Status

**All 34 implementation tasks complete** ✓

- **Phase 1 (Migration + DB Accessors)**: 12/12 tasks checked `[x]`
- **Phase 2 (Bounded Helper)**: 8/8 tasks checked `[x]`
- **Phase 3 (Wiring + Docs)**: 14/14 tasks checked `[x]`

No stale checkboxes detected. Task completion gate: **PASS**.

### Size & Review Budget

Two PRs exceeded the 400-line review budget and accepted `size:exception`:

| PR | Authored changed lines | Budget | Status |
|----|---|---|---|
| #572 | 434 | 400 | Accepted `size:exception` per attempt-ledger `maintainer_decision` |
| #573 | 759 | 400 | Accepted `size:exception` per attempt-ledger `maintainer_decision` + reset with `--objective-relation independent` |

Both slices honored the `work-unit-commits` "budget is not code-golf" rule — no comments, docs, or tests were trimmed to fit the budget. Each slice delivers a complete, autonomous, independently-verifiable work unit.

## Spec Synchronization

### New Capability: `thumbnail-text-regeneration`

**Status**: ✓ Created
**Location**: `openspec/specs/thumbnail-text-regeneration/spec.md`
**Action**: Mechanical copy from change folder to main specs
**Diff verification**: ✓ No differences (empty `diff -r` output)

**Spec contents**:
- **Purpose**: Bounded, non-blocking regeneration triggered by a `thumbnail_text` finding on the long-form upload path
- **Requirements**: 6 distinct requirements covering trigger scope, attempt ceiling, wait bounds, blocking guarantees, brief retention, and sibling isolation
- **Scenarios**: 8 scenarios across the 6 requirements, all traced to passing tests

**Example requirement**: "Attempts Are Claimed Against A Bounded Per-Video Spend Ceiling" — the attempt counter is the ONLY spend ceiling, not merely a loop guard, because no Pikzels/OpenAI throttle exists elsewhere in the codebase.

### Modified Capability: `final-copy-verification`

**Status**: ✓ Delta merged into main spec
**Location**: `openspec/specs/final-copy-verification/spec.md`
**Modification**: "Requirement: Thumbnail Text Flagged Without Correction"
**Action**: Replaced single scenario with two scenarios; updated requirement prose to document downstream regeneration behavior
**Critical requirement preserved**: ✓ "Hard-Rejection Asymmetry" (lines 95-116 of the main spec) remains 100% unchanged

**Before merge**:
```markdown
### Requirement: Thumbnail Text Flagged Without Correction

Thumbnail text MUST be verified but never auto-corrected, because it is
already baked into the chosen thumbnail image.

#### Scenario: Thumbnail text finding is flagged only
```

**After merge**:
```markdown
### Requirement: Thumbnail Text Flagged Without Correction

Thumbnail text MUST be verified but never auto-corrected, because it is
already baked into the chosen thumbnail image. On the long-form upload path,
a `thumbnail_text` finding MUST also drive at most one bounded, non-blocking
regeneration attempt, governed entirely by the `thumbnail-text-regeneration`
capability. The verifier itself MUST NOT be changed to correct the field, and
triggering a regeneration MUST NOT block or delay publication.
(Previously: a `thumbnail_text` finding was recorded and persisted with no
downstream effect beyond the audit row.)

#### Scenario: Thumbnail text finding is flagged only
[unchanged verifier behavior]

#### Scenario: Long-form finding drives a bounded downstream regeneration
[new downstream behavior: claims attempt, triggers regeneration, handles timeout]
```

**Diff verification**: ✓ No differences in merged spec (verified by successful `Edit` operation)

## Spec Contents Summary

### `thumbnail-text-regeneration/spec.md` — 8 scenarios, 6 requirements

1. Regeneration triggered **only** by a thumbnail-text finding (2 scenarios: trigger, no-trigger)
2. Attempts claimed atomically against a bounded per-video ceiling, exhausted = refuse further attempts (2 scenarios: exhausted, idempotent retry)
3. Wait is bounded; timeout publishes as-is (2 scenarios: completes within bound, timeout)
4. No code path may block/delay publication (2 scenarios: failure never raises, title hard-rejection remains only blocking path)
5. Both prior and regenerated briefs retained for audit (implicit across scenarios 1–4; explicit test coverage per verify-report traceability table)
6. Regeneration effect scoped to triggering turn only (1 scenario: sibling unaffected)
7. Scope is long-form only (verified structurally: `reap_shorts_uploader_dag.py` untouched)

### `final-copy-verification/spec.md` — delta applied

**Modified requirement** (1 of the 11 total requirements in this spec):
- "Thumbnail Text Flagged Without Correction": now documents the downstream regeneration behavior
- **Unchanged**: "Hard-Rejection Asymmetry", "Independent Verification Call", "Verdict and Findings Schema", "Bounded Correction", "Party Mismatch Detection", "Fallback on Unavailable Verification", "Audit Persistence", "Speaker Evidence Keeps Raw and Canonical Names Distinct", "Evidence Bundle Shape Parity"

## Traceability: All 14 Scenarios Trace to Tests

Per `verify-report.md`, all 14 scenarios (8 from new spec + 1 modified + 5 from unmodified `final-copy-verification` requirements) are traced to real, named, passing tests:

### New Spec: `thumbnail-text-regeneration` (8 scenarios)

| Scenario | Test(s) | Result |
|---|---|---|
| A thumbnail-text finding triggers a regeneration attempt | `test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger` | PASS |
| No thumbnail-text finding, no regeneration | `test_verify_final_copy_no_thumbnail_text_finding_zero_claims` + mutation check | PASS |
| Exhausted budget refuses a further attempt | `test_refuses_at_ceiling` | PASS |
| Retrying the step does not accumulate attempts | `test_idempotent_on_rerun` | PASS |
| Regeneration completes within the bound | `test_completes_within_bound_returns_regenerated_result` | PASS |
| Regeneration exceeds the bound | `test_times_out_after_exactly_max_polls` (exact-iteration: `sleep.call_count == 100`) | PASS |
| Regeneration failure never raises | `test_no_path_ever_raises` (5-way parametrized) + `test_verify_final_copy_every_failure_mode_returns_none_never_raises` (6-way parametrized) | PASS |
| Title hard-rejection remains the only blocking path | `test_verify_final_copy_title_reject_still_raises_before_any_claim` | PASS |

### Modified Spec: `final-copy-verification` — "Thumbnail Text Flagged Without Correction"

| Scenario | Test(s) | Result |
|---|---|---|
| Thumbnail text finding is flagged only | `test_final_copy_verification.py::test_thumbnail_finding_recorded_corrected_cannot_carry_it` + `test_system_prompt_never_corrects_thumbnail_text` (pre-existing, unmodified, still passing) | PASS |
| Long-form finding drives a bounded downstream regeneration | `test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger` | PASS |

## Archive Contents Verified

**Artifact inventory** (all present in `openspec/changes/archive/2026-09-10-thumbnail-text-regeneration/`):

- ✓ `proposal.md` — problem statement, scope, capabilities, approach, risks, rollback, success criteria
- ✓ `design.md` — detailed design decisions (D1–D6), requirements rationale, rejected alternatives, non-negotiables
- ✓ `specs/thumbnail-text-regeneration/spec.md` — new full spec (8 scenarios)
- ✓ `specs/final-copy-verification/spec.md` — modified existing spec (delta applied)
- ✓ `tasks.md` — 34/34 tasks complete, traceability table
- ✓ `apply-progress.md` — implementation snapshots per phase
- ✓ `verify-report.md` — verification evidence, traceability, six suspicious-point validation
- ✓ `evidence-regeneration-cost.md` — measured poll-time distribution, cost analysis
- ✓ `exploration.md` — initial research, related issues, design space alternatives

**Mechanical move verification**: ✓ Source folder removed, destination folder created, `diff -r` returned empty (no byte alterations, no truncation)

## Key Technical Decisions Recorded

### Non-Negotiable Facts (all verified during apply and verify)

1. **Migration 052 DOWN block MUST be commented out** — `migrations_dag` executes the whole file in one transaction; live DOWN would silently revert the migration. Confirmed present at lines 18–26 of migration 052. ✓

2. **Hoisted `xcom_push`** — The `ti.xcom_push(key="upload_config", ...)` MUST fire when a landed thumbnail regeneration occurs, even without a correction. Pre-#545 this line lived inside `if verdict.correction_applied:` and would silently drop a regen-only case. Hoisted to line 1784, outside the correction conditional. Test `test_verify_final_copy_hoisted_xcom_push_fires_without_correction` enforces this; live mutation check performed during apply (reverted hoist, confirmed test fails, restored). ✓

3. **Claim-before-act, threshold 2** — Attempt is claimed atomically BEFORE `_regenerate_flagged_thumbnail` makes the paid trigger call. Ceiling is 2 attempts per video (`WHERE attempts < 2`). Enforced by database.py's single `UPDATE` statement with exact `WHERE` guard. Tests: `test_charges_before_second_call`, `test_refuses_at_ceiling`, `test_idempotent_on_rerun` (mutation-sensitive). ✓

4. **No `raise` on any regeneration path** — Neither the trigger call, the poll loop, the claim, nor the top-level `_apply_thumbnail_regeneration_if_flagged` in `_verify_final_copy` may raise into `t7`. Enforced by explicit `try/except` at `_regenerate_flagged_thumbnail` level and call-site level; no re-raise anywhere. The title-reject `raise` at youtube_upload_dag.py:1747–1752 is strictly before the regeneration branch (line 1754). Test `test_verify_final_copy_every_failure_mode_returns_none_never_raises` wraps the call and `pytest.fail()`'s if anything escapes. ✓

5. **Bounded poll: 1000s (100 × 10s)** — `_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10`, `_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv(..., "100"))`. Measured over 75 production runs: p50 214s, p95 888s, **max 3989s**. The max exceeds comparable DAG bounds, so timeout is load-bearing and regularly exercised. Test `test_times_out_after_exactly_max_polls` pins `sleep.call_count == 100` (exact iteration, not ≥). On timeout, function returns `{"outcome": "timeout", ...}` and `t7` publishes the pre-attempt thumbnail. ✓

6. **Sibling isolation by file** — `_build_regen_child_conf` unconditionally overrides `child_conf["output_path"] = output_path` (the triggering turn's own file), never trusting the shared `thumbnail_config`'s value. Test `test_verify_final_copy_sibling_isolation_by_output_path` exercises the real (unmocked) regenerate chain with two turns sharing `chapter_id` and asserts the triggered conf's `output_path` matches turn A's file, not the shared key, not a sibling's file. ✓

### Design Decisions Rationale

1. **Migration columns on `speaker_turn_videos` only** — `video_shorts` is untouched because shorts verification never passes `thumbnail_text` and regeneration is out of scope for the shorts path.

2. **Counter is the ONLY spend ceiling** — No Pikzels/OpenAI throttle exists elsewhere in the codebase, so this counter is not just a loop guard; it controls the actual spend. Threshold is 2 attempts per video, matching the #331 `thumbnail_republish_attempts` shape.

3. **Shared `video_thumbnails` row is audit-only** — The row is keyed `(chapter_id, label)` and shared across every sibling turn. The regeneration's effect scopes to the triggering turn's own canonical `thumbnail.png` file. The `video_thumbnails` write is best-effort/audit-only, not authoritative for what siblings publish. This is documented in `docs/DAGS.md` and flagged as a residual risk (pre-existing architecture limitation, not new to #545).

4. **Regeneration never blocks publication** — Timeout, failure, exhausted attempts, or unavailability all publish with the pre-attempt thumbnail. The only blocking path for long-form uploads remains the title hard-rejection (pre-existing asymmetry, unchanged by #545).

5. **No Airflow `execution_timeout`** — Explicitly rejected as the backstop. A task timeout on `t6b` fails the task, skips `t7`, and converts a finding into a publication block — the opposite of the non-blocking guarantee. Instead, the poll bound and the failure-return pattern enforce the guarantee. Regression comment present at youtube_upload_dag.py:1880–1887.

## Follow-Up Risks Documented

The launch prompt identified two follow-ups for the orchestrator to file (not archived, not resolved here):

1. **The unbounded `while True` poll in `trigger_thumbnail_generation`** (pre-existing risk, untouched by #545)
   - `youtube_upload_dag.py::trigger_thumbnail_generation` contains an unbounded `while True` that is not constrained by any timeout
   - This is orthogonal to #545's bounded polling in `_regenerate_flagged_thumbnail`
   - Recommended as a separate follow-up issue

2. **Sibling residual hazard** (pre-existing architecture, diagnosable via #545, not resolved here)
   - A sibling turn verified after turn A's regeneration may read the regenerated shared brief via `get_chosen_thumbnail(chapter_id)` while its own `thumbnail.png` still carries the old text
   - This is a pre-existing consequence of the `(chapter_id, label)` shared-row architecture, not introduced by #545
   - Only made diagnosable by the audit trail #545 creates; fixing it requires architectural change to per-turn rows
   - Documented in `docs/DAGS.md` and flagged as accepted pre-existing limitation

## Source of Truth Updated

The following specs are now authoritative for the system's behavior:

- `openspec/specs/thumbnail-text-regeneration/spec.md` — defines the new regeneration capability
- `openspec/specs/final-copy-verification/spec.md` — updated to reflect the downstream regeneration behavior

Both specs fully supersede any prior inline documentation or ticket descriptions.

## Delivery Readiness

**Status**: ✓ **READY FOR DELIVERY**

The change is complete, verified, and archived. The orchestrator will:
1. Merge the tracker branch to `dev` and then `main`
2. Apply migration 052 via `migrations_dag` in production
3. Confirm `airflow dags list-import-errors` on the NAS after `git_sync`
4. Close issue #545

No additional work is required from the archive executor.

## Scope Boundaries

**In scope** (all delivered):
- Migration 052 (7 new columns on `speaker_turn_videos` only, DOWN commented out)
- `claim_thumbnail_text_regeneration` and `record_thumbnail_text_regeneration_outcome` DB accessors
- `_regenerate_flagged_thumbnail` bounded trigger/poll helper
- `t6b _verify_final_copy` regeneration branch and hoisted `xcom_push`
- Docs in `docs/DAGS.md`
- Full test coverage across all three phases

**Out of scope** (intentionally not touched):
- Shorts path (`reap_shorts_uploader_dag.py`, `video_shorts` schema) — verified 0 changes
- Unbounded `while True` in `trigger_thumbnail_generation` — filed as follow-up
- LLM prompt modifications to the verifier itself
- Per-turn rows for `video_thumbnails` (architectural limitation noted, not addressed)

**Verified by filename diff**:
```
Changed files: congress_videos/modules/database.py
               congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql
               congress_videos/youtube_upload_dag.py
               docs/DAGS.md
               openspec/changes/thumbnail-text-regeneration/*
               tests/congress_videos/...
Zero changes:  reap_shorts_uploader_dag.py ✓
               video_shorts ✓
```

## Lessons for Future Changes

1. **Shared audit rows with per-turn scope** — When a shared DB row (keyed by higher-level entity like `chapter_id`) is used to track per-lower-level changes (per-turn), the audit row is best-effort only. The source of truth for each turn's behavior must remain in the turn's own artifacts (file paths, canonical values), not in the shared row. This pattern worked well here and should be explicitly designed in future similar changes.

2. **Claim-before-act for spend ceilings** — When no throttle exists elsewhere, the attempt counter is the ONLY spend control, not a loop guard. Atomicity matters: claim in one `UPDATE` statement before making the paid call. Tests must verify the exact `WHERE` guard (mutation-sensitive).

3. **Hoisting conditional XCom pushes** — When a seam has two independent mutations (correction + file swap), a single guarded push covering both is clearer than nested conditionals. The `mutated = mutated or regen_mutated` fold pattern made it easy to reason about. A test that explicitly disables one mutation branch and verifies the push still fires catches the regression.

4. **Bounded retries with regularly-exercised timeout** — The design correctly treated the timeout as a regularly-loaded path (measured max 3989s in production), not an edge case. This meant the no-timeout behavior needed careful verification, not assumed. PR feedback that requests "but what if it times out?" should be welcomed — it's often the most load-bearing path.

## Conclusion

Issue #545 "Regenerate the thumbnail when the verifier flags its text" has been closed. A non-blocking, bounded regeneration capability has been introduced on the long-form upload path. Two specs have been created/modified in the source of truth. All 34 tasks are complete. The system passed verification with 0 CRITICAL findings and full scenario traceability. The change is archived and ready for delivery.

---

**Archive Report Generated**: 2026-09-10
**Archive Phase**: sdd-archive
**Phase Executor**: Claude Haiku 4.5 (sdd-archive worker)
**Verification Source**: orchestrator launch prompt (authoritative final-state facts), verify-report.md (snapshot), apply-progress.md (snapshot)
