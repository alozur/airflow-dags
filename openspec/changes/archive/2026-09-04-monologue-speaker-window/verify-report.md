```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:67dcc718a780f6d45b55f7ae2e47d363e142818281bf6c88817f1e970bf48bce
verdict: pass_with_warnings
blockers: 0
critical_findings: 0
requirements: 11/11
scenarios: 25/25
test_command: uv run pytest -n auto -q
test_exit_code: 0
test_output_hash: sha256:fb9e0f6044ed6c46f8c00b9c9fdede20837eb29b5565982f182ee44765be82fe
build_command: uv run ruff check && uv run ruff format --check && PYTHONPATH=. uv run python congress_videos/speaker_turn_prepare_dag.py
build_exit_code: 0
build_output_hash: sha256:fc630f5cc4076b1979a5f868a0ffac3bd457ff3499e2d81c71139bc1cc15b06e
```

## Verification Report

**Change**: monologue-speaker-window (issue #430)
**Version**: N/A (single-version spec)
**Mode**: Strict TDD

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total | 49 |
| Tasks complete | 49 |
| Tasks incomplete | 0 |

Task 4.11 ("Apply migration 046 to dev, then to prod, BEFORE Phase 5 merges to main") is now
marked `[x]` in `tasks.md`, remediated by the orchestrator on the NAS (2026-09-04, outside this
sandboxed worktree). Verified via `pg_attribute`: `speaker_resolution_evidence` exists in BOTH
`development.speaker_turn_videos` and `production.speaker_turn_videos` (query returned
development|1, production|1; type text, nullable). `development` was applied through
`run_migrations` after `git_sync`; `production` was pre-applied idempotently with the same
`ADD COLUMN IF NOT EXISTS`, so `run_migrations` will record 046 as a no-op there after release.
This was the sole CRITICAL finding of the prior verify pass; it is now resolved.

### Build & Tests Execution
**Build**: PASSED
```text
uv run ruff check                                              -> All checks passed! (exit 0)
uv run ruff format --check                                     -> 301 files already formatted (exit 0)
PYTHONPATH=. uv run python congress_videos/speaker_turn_prepare_dag.py -> imports cleanly, only pre-existing
                                                                     Airflow RemovedInAirflow3Warning notices (exit 0)
```

**Tests**: 4576 passed / 0 failed / 29 skipped
```text
uv run pytest -n auto -q
4576 passed, 29 skipped in 72.95s
```
All 29 skips are pre-existing opt-in live-Postgres/live-LLM tests (no local Postgres, no
Tailscale/NAS access, no OPENAI_API_KEY+LIVE_LLM_TESTS=1 in this sandbox) — same skip count as the
prior verify pass. The passed count rose from 4532 to 4576 because this branch has been rebased
onto the now-merged `dev` (PRs #454/#455/#456/#458/#462, dev head `b504254`), which folds in tests
from the other four slices that were previously only reachable via their own branches.

Targeted re-confirmation:
- `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -q -o addopts= -rs`
  -> 47 passed, 1 skipped (the opt-in `live_llm` test, skip reason: "Opt-in: requires
  OPENAI_API_KEY and LIVE_LLM_TESTS=1"), unchanged from the prior pass.
- `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` -> 71 passed (frozen
  suite, unmodified).
- `git diff origin/dev...HEAD -- congress_videos/modules/speaker_resolution.py
  tests/congress_videos/modules/test_speaker_resolution.py` -> 0 lines (both files byte-identical
  to `dev`), unchanged.

**Coverage**: Not available — no coverage tool configured/run in this pass (project convention:
`uv run pytest` without `--cov`); assessed by test-count and scenario-mapping instead. ➖ Not
available

### Spec Compliance Matrix

**Requirement: Preceding Window Selection** (6/6 scenarios)
| Scenario | Test | Result |
|---|---|---|
| Block at window-start boundary is included | `test_monologue_speaker_window.py::test_block_at_window_start_boundary_is_included` | COMPLIANT |
| Block just before window-start is excluded | `::test_block_just_before_window_start_is_excluded` | COMPLIANT |
| Block at the anchor is excluded | `::test_block_at_anchor_is_excluded` | COMPLIANT |
| Block overlapping the anchor is selected by start time | `::test_block_overlapping_anchor_is_selected_by_start_time` | COMPLIANT |
| Anchor near session start clamps window_start to zero | `::test_anchor_near_session_start_clamps_window_start_to_zero` | COMPLIANT |
| group_start_seconds overrides the turn's own start | `::test_group_start_seconds_overrides_start_seconds_for_the_window` (+ `test_anchor_honours_group_start_seconds_zero` for the `0.0` case) | COMPLIANT |

**Requirement: Announcement Pre-Gate** (1/1)
| Scenario | Test | Result |
|---|---|---|
| No announcement phrase skips both LLM calls | `::test_resolve_monologue_speaker_pre_gate_no_call_when_no_announcement_phrase` (asserts `completion_fn` call count 0, result `None`) | COMPLIANT |

**Requirement: Step-1 Prompt Payload Scope** (1/1)
| Scenario | Test | Result |
|---|---|---|
| Payload excludes text outside the window | `::test_resolve_monologue_speaker_payload_excludes_text_outside_the_window` (captures every `user` prompt across BOTH Step 1 and Step 2 calls; asserts `SENTINEL_BEFORE_WINDOW` and `SENTINEL_AFTER_ANCHOR` absent from `captured` after each) | COMPLIANT — genuine e2e proof, not a mock tautology; would fail if the whole SRT were sent |

**Requirement: Step-1 Floor-Holder Identification** (5/5, with a caveat — see WARNING below)
| Scenario | Test | Result |
|---|---|---|
| Full name announcement resolves the floor holder | `::test_identify_floor_holder_is_a_mock_echo[full-name]` (mock) + `::test_identify_floor_holder_live_model_resolves_full_name_announcement` (opt-in `live_llm`, skipped here) | COMPLIANT (contract-level; real-model proof is opt-in and skipped in this run) |
| Addressee is not conflated with the responder | `::test_identify_floor_holder_is_a_mock_echo[addressee-vs-responder]` | COMPLIANT (contract-level only — see WARNING) |
| Role announcement after a courtesy phrase resolves correctly | `::test_identify_floor_holder_is_a_mock_echo[courtesy-then-handover]` | COMPLIANT (contract-level only — see WARNING) |
| No announcement found stops before Step 2 | `::test_resolve_monologue_speaker_found_false_stops_before_step_2` (call count == 1) | COMPLIANT |
| Unlocatable evidence is rejected | `::test_resolve_monologue_speaker_unlocatable_evidence_returns_none` | COMPLIANT |

**Requirement: Step-2 Prompt Payload Scope** (1/1)
| Scenario | Test | Result |
|---|---|---|
| Payload contains no window text beyond the evidence quote | `::test_resolve_announced_identity_payload_excludes_window_text_beyond_evidence` + the orchestrator e2e test above (Step 2's captured prompt also checked) | COMPLIANT — `resolve_announced_identity`'s signature structurally never receives SRT blocks, only `{floor_holder, participants}` |

**Requirement: Step-2 Roster-Backed Resolution** (3/3)
| Scenario | Test | Result |
|---|---|---|
| High-confidence roster match resolves | `::test_resolve_announced_identity_confidence_boundary[at-threshold-accepts]` (0.80) | COMPLIANT |
| Low confidence yields unresolved | `::test_resolve_announced_identity_confidence_boundary[just-below-threshold-rejects]` (0.79) | COMPLIANT |
| Slug outside the roster yields unresolved | `::test_resolve_announced_identity_rejects_slug_outside_roster` | COMPLIANT |

**Requirement: Result Shape and Evidence Audit** (1/1)
| Scenario | Test | Result |
|---|---|---|
| Successful resolution produces both the result and the audit string | `::test_resolve_monologue_speaker_successful_resolution_shape_and_audit` (asserts dict shape + audit key set == the 7 required keys + `method == "monologue_window_v1"`) | COMPLIANT |

**Requirement: Evidence Persistence** (2/2)
| Scenario | Test | Result |
|---|---|---|
| Migration is idempotent | `ADD COLUMN IF NOT EXISTS` (self-evidently idempotent SQL); schema-snapshot proof via `test_production_schema.py::test_column_present_in_block[speaker_turn_videos-speaker_resolution_evidence]`; NOW ALSO confirmed live on both `development` and `production` via `pg_attribute` (development|1, production|1; text, nullable) — `production` was applied idempotently against an already-partially-migrated cluster with no error | COMPLIANT |
| Existing callers are unaffected | `test_database_speaker_resolution.py::test_five_positional_arg_call_leaves_sql_byte_identical` (golden/approval test — byte-identical SQL for the 5-positional call) | COMPLIANT |

**Requirement: Routing by Turn Type** (2/2)
| Scenario | Test | Result |
|---|---|---|
| Non-qa turn routes to the new resolver | `test_speaker_turn_prepare_dag.py::TestSpeakerResolutionRouting::test_monologue_turn_routes_to_monologue_resolver` | COMPLIANT |
| qa turns and qa-promotion re-resolves keep using resolve_speaker | `::test_qa_turn_routes_to_resolve_speaker` + `::test_qa_promotion_wide_repass_still_uses_resolve_speaker` | COMPLIANT |

**Requirement: Non-Regression of the Existing Resolver** (1/1)
| Scenario | Test | Result |
|---|---|---|
| Existing qa suite stays green | `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` -> 71 passed; `git diff origin/dev...HEAD` on both the module and its test file -> 0 lines | COMPLIANT |

**Requirement: Never-Raise Contract** (2/2)
| Scenario | Test | Result |
|---|---|---|
| Step-1 or Step-2 exception does not propagate | `::test_resolve_monologue_speaker_never_raises_end_to_end[1]` / `[2]` (parametrized `raising_step`) | COMPLIANT |
| A completion error response is handled without an exception, Step-1 error stops before Step 2 | `identify_floor_holder`/`resolve_announced_identity` error-response tests (isolated seam level) + `::test_resolve_monologue_speaker_found_false_stops_before_step_2` (orchestrator-level, same `FloorHolder()` sentinel code path an error response also produces) | COMPLIANT — proven via the shared sentinel path rather than a literal `{"error": ...}` orchestrator-level test (SUGGESTION below) |

**Compliance summary**: 25/25 scenarios compliant (11/11 requirements), 2 scenarios flagged with a documented proof-strength caveat (see WARNING/SUGGESTION below) — unchanged from the prior pass.

### Correctness (Static Evidence)
| Requirement | Status | Notes |
|------------|--------|-------|
| `select_preceding_window` window rule | Implemented | `window_start = max(0.0, anchor_seconds - window_seconds)`; selection `window_start <= block["start_secs"] < anchor_seconds` — exact spec match, `congress_videos/modules/monologue_speaker_window.py:74-88` |
| `turn_anchor_seconds` | Implemented | `group_start_seconds` wins when not `None` (incl. `0.0`), else `start_seconds` — exact spec match, lines 60-71 |
| Migration 046 | Implemented and LIVE | `ADD COLUMN IF NOT EXISTS`, DOWN fully commented per the 044 convention; now confirmed applied on both `development` and `production` via `pg_attribute` |
| `production_schema.sql` + column-tuple test | Implemented | Column added in lockstep with the schema-snapshot test in the same change |
| `mark_turn_resolved(evidence=)` | Implemented | Optional kwarg, default `None`; SET clause and params extended only when provided; WHERE clause and `logger.info` call byte-identical to pre-#430 |
| Routing (`turn_type != 'qa'` -> `resolve_monologue_speaker`) | Implemented | `speaker_turn_prepare_dag.py:312-315`; qa-promotion wide re-pass still forces `turn_type='qa'`, unchanged |
| `resolve_speaker` non-regression | Implemented | 0-line diff against `origin/dev` for both the module and its full test suite |
| Never-raise contract | Implemented | `resolve_monologue_speaker` wraps `_resolve_monologue_inner` in `try/except Exception`, logs one WARNING, returns `None` |

### Coherence (Design)
| Decision | Followed? | Notes |
|----------|-----------|-------|
| D1: `resolve_speaker` and its suite stay frozen | Yes | 0-line diff confirmed |
| D5: `_load_turn_blocks` duplicated, not extracted from the frozen module | Yes | Matches design.md's stated rationale |
| Stacked-to-main chain (A1 -> A2a -> A2b -> B -> C), each slice < 400 authored lines | Yes | Historical per-slice measurements from the prior verify pass stand unchanged (A2a=395, A2b=320, B=145, C=336); the chain is now fully merged into `dev` (PRs #454/#455/#456/#458/#462), so re-measuring branch-to-branch diffs is no longer meaningful — confirmed optional by the orchestrator for this pass and not re-run |
| `live_llm` marker + opt-in test added | Yes | Registered in `pyproject.toml`; skips by default and in this run |
| Opt-in live-test env var name | **Deviation** | design.md specifies `MONOLOGUE_LIVE_LLM_TESTS`; the shipped test uses `LIVE_LLM_TESTS` — see WARNING below, unresolved from the prior pass |
| Docs updated in the same slice (`docs/PIPELINE.md`) | Yes | One paragraph added, verified accurate against the shipped routing logic |
| Migration 046 applied to dev and prod before merge to main | Yes (remediated) | Confirmed via `pg_attribute` on both schemas — this closes the sole CRITICAL from the prior verify pass |

### Issues Found

**CRITICAL**: None. The prior CRITICAL (task 4.11 — migration 046 not yet applied to dev/prod) is
resolved: `pg_attribute` confirms `speaker_resolution_evidence TEXT NULL` exists in both
`development.speaker_turn_videos` and `production.speaker_turn_videos`, and `tasks.md` now marks
4.11 `[x]` with that evidence.

**WARNING** (unchanged from the prior pass):
1. design.md's Testing Strategy names the opt-in live-test gate `MONOLOGUE_LIVE_LLM_TESTS`; the
   shipped test (`test_identify_floor_holder_live_model_resolves_full_name_announcement`) uses
   `LIVE_LLM_TESTS` instead (a documented deliberate deviation for slice C). Does not break any
   spec requirement — the spec text never names this env var — but it is a design-vs-implementation
   naming mismatch the maintainer should confirm is intentional.
2. Three of the five "Step-1 Floor-Holder Identification" scenarios (full-name, addressee-vs-responder,
   courtesy-then-handover) are proven only by pass-through mock-echo tests
   (`test_identify_floor_holder_is_a_mock_echo`), which assert that whatever `completion_fn` returns
   is relayed unchanged — not that the real model performs the semantic disambiguation the scenario
   describes. Only the "full-name" case has a corresponding real-model proof, and it is an opt-in
   test skipped by default and in this run. The "addressee-vs-responder" and "courtesy-then-handover"
   cases currently have NO real-model check at all, opt-in or otherwise.

**SUGGESTION** (unchanged from the prior pass):
1. `test_identify_floor_holder_payload_excludes_text_outside_the_window` (the A2a unit-level payload
   test) never actually includes any "outside window" text among its inputs — it asserts a sentinel
   string that was never passed anywhere is absent, which is trivially true regardless of
   implementation. Does not weaken the change's actual proof (the orchestrator-level e2e test is the
   genuine proof), but this specific test adds no discriminating signal.
2. No dedicated live-Postgres test runs migration 046 twice to directly prove idempotency at
   runtime (unlike some earlier migrations that have a `test_migration_0NN.py` file); coverage
   relies on the self-evident `ADD COLUMN IF NOT EXISTS` syntax plus the schema-snapshot test. Now
   additionally corroborated by the live re-application on `production` succeeding as a genuine
   idempotent no-op against an already-migrated column — closing most of the practical risk this
   suggestion originally flagged.
3. The "A completion error response is handled without an exception" scenario is proven indirectly
   via the shared `FloorHolder()` sentinel code path rather than a literal orchestrator-level
   `{"error": "..."}` test. A literal test would make this scenario's coverage explicit rather than
   inferred.
4. `apply-progress.md`'s Phase 4 status line ("11/12 tasks done, 1 explicitly deferred") and Phase
   5's "Outstanding blocker carried from slice B" paragraph are now stale relative to the remediated
   `tasks.md` 4.11 entry; the orchestrator may want to append a remediation note there for
   consistency, though this was out of this verify pass's explicit instruction scope (only `tasks.md`
   was to be updated).

### Verdict
**PASS WITH WARNINGS** — all code, tests (4576 passed/0 failed/29 pre-existing skips), lint,
format, and import checks are green; 49/49 tasks complete (migration 046 now confirmed live on both
dev and prod); 25/25 spec scenarios have a passing covering test. Two non-blocking WARNINGs remain
(an opt-in test's env-var name deviates from design.md; 2 of 3 LLM-reasoning sub-scenarios have no
real-model test, only mocked contract proof) — ready for archive once the maintainer has visibility
into those.
