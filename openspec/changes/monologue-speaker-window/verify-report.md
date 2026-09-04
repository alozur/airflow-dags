```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:67dcc718a780f6d45b55f7ae2e47d363e142818281bf6c88817f1e970bf48bce
verdict: fail
blockers: 1
critical_findings: 1
requirements: 11/11
scenarios: 25/25
test_command: uv run pytest -n auto -q
test_exit_code: 0
test_output_hash: sha256:621292d4439fb5e8ae85924b323275cd87ec474de9d199b22f193f2e6d9bacdc
build_command: uv run ruff check && uv run ruff format --check && PYTHONPATH=. uv run python congress_videos/speaker_turn_prepare_dag.py
build_exit_code: 0
build_output_hash: sha256:2abbc7dfc5a301feeb67eac32119b4c4e1d8ec8347be5694a6519bb1eab21566
```

## Verification Report

**Change**: monologue-speaker-window (issue #430)
**Version**: N/A (single-version spec)
**Mode**: Strict TDD

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total | 49 |
| Tasks complete | 48 |
| Tasks incomplete | 1 (task 4.11 — apply migration 046 to dev then prod; deployment-ops action, not code) |

### Build & Tests Execution
**Build**: PASSED
```text
uv run ruff check                                              -> All checks passed! (exit 0)
uv run ruff format --check                                     -> 299 files already formatted (exit 0)
PYTHONPATH=. uv run python congress_videos/speaker_turn_prepare_dag.py -> imports cleanly, only pre-existing
                                                                     Airflow RemovedInAirflow3Warning notices (exit 0)
```

**Tests**: 4532 passed / 0 failed / 29 skipped
```text
uv run pytest -n auto -q
4532 passed, 29 skipped in 67.71s
All 29 skips are pre-existing opt-in live-Postgres/live-LLM tests (no local Postgres, no
Tailscale/NAS access, no OPENAI_API_KEY+LIVE_LLM_TESTS=1 in this sandbox) -- none are new
failures caused by this change.
```

Targeted re-confirmation:
- `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -q -o addopts= -rs` -> 47 passed, 1 skipped (the opt-in `live_llm` test, skip reason: "Opt-in: requires OPENAI_API_KEY and LIVE_LLM_TESTS=1").
- `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` -> 71 passed (frozen suite, unmodified).
- `git diff origin/dev...HEAD -- congress_videos/modules/speaker_resolution.py tests/congress_videos/modules/test_speaker_resolution.py` -> 0 lines (both files byte-identical to `dev`).

**Coverage**: Not available — no coverage tool configured/run in this pass (project convention: `uv run pytest` without `--cov`); assessed by test-count and scenario-mapping instead. ➖ Not available

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
| Migration is idempotent | `ADD COLUMN IF NOT EXISTS` (self-evidently idempotent SQL); schema-snapshot proof via `test_production_schema.py::test_column_present_in_block[speaker_turn_videos-speaker_resolution_evidence]` | COMPLIANT — no dedicated live-Postgres run-twice test exists (SUGGESTION below) |
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

**Compliance summary**: 25/25 scenarios compliant (11/11 requirements), 2 scenarios flagged with a documented proof-strength caveat (see WARNING/SUGGESTION below).

### Correctness (Static Evidence)
| Requirement | Status | Notes |
|------------|--------|-------|
| `select_preceding_window` window rule | Implemented | `window_start = max(0.0, anchor_seconds - window_seconds)`; selection `window_start <= block["start_secs"] < anchor_seconds` — exact spec match, `congress_videos/modules/monologue_speaker_window.py:74-88` |
| `turn_anchor_seconds` | Implemented | `group_start_seconds` wins when not `None` (incl. `0.0`), else `start_seconds` — exact spec match, lines 60-71 |
| Migration 046 | Implemented | `ADD COLUMN IF NOT EXISTS`, DOWN fully commented per the 044 convention, header explains the runner's single-transaction risk |
| `production_schema.sql` + column-tuple test | Implemented | Column added in lockstep with the schema-snapshot test in the same change |
| `mark_turn_resolved(evidence=)` | Implemented | Optional kwarg, default `None`; SET clause and params extended only when provided; WHERE clause and `logger.info` call byte-identical to pre-#430 |
| Routing (`turn_type != 'qa'` -> `resolve_monologue_speaker`) | Implemented | `speaker_turn_prepare_dag.py:312-315`; qa-promotion wide re-pass still forces `turn_type='qa'` at line 336, unchanged |
| `resolve_speaker` non-regression | Implemented | 0-line diff against `origin/dev` for both the module and its full test suite |
| Never-raise contract | Implemented | `resolve_monologue_speaker` wraps `_resolve_monologue_inner` in `try/except Exception`, logs one WARNING, returns `None` |

### Coherence (Design)
| Decision | Followed? | Notes |
|----------|-----------|-------|
| D1: `resolve_speaker` and its suite stay frozen | Yes | 0-line diff confirmed |
| D5: `_load_turn_blocks` duplicated, not extracted from the frozen module | Yes | Matches design.md's stated rationale (avoid modifying the frozen module) |
| Stacked-to-main chain (A1 -> A2a -> A2b -> B -> C), each slice < 400 authored lines | Yes, with a methodology caveat | See "Per-Slice Budget Verification" below — measured cleanly against each slice's actual local parent commit; A2a=395, A2b=320, B=145, C=336 lines, all under 400 |
| `live_llm` marker + opt-in test added | Yes | Registered in `pyproject.toml`; skips by default and in this run |
| Opt-in live-test env var name | **Deviation** | design.md specifies `MONOLOGUE_LIVE_LLM_TESTS`; the shipped test uses `LIVE_LLM_TESTS` — see WARNING below |
| Docs updated in the same slice (`docs/PIPELINE.md`) | Yes | One paragraph added, verified accurate against the shipped routing logic |

### Per-Slice Budget Verification

The literal `git diff --stat <parent-branch>...<branch>` command specified for this check is
currently unreliable for A2a/B/C: the orchestrator is actively rebasing the lower slices
(`feat/430-a1-window-selection`, `feat/430-a2a-llm-steps`, `feat/430-b-evidence-migration`) onto
`dev` in a separate worktree, so those `origin/*` branch refs have moved past the commits this
worktree's `feat/430-c-routing` branch was actually built on (confirmed: `origin/feat/430-b-evidence-migration`
now points to `23b3d26`, while this worktree's `feat/430-c-routing` still descends from the
pre-rebase `6700695`; `git diff origin/feat/430-b-evidence-migration...origin/feat/430-c-routing`
pulls in already-landed B-slice files as noise). Measuring against each slice's own actual local
ancestor commit (as recorded in `apply-progress.md` and independently re-verified here) gives clean,
single-slice diffs:
- A2a: `845cd03..` (A1 tip) -> **395** authored lines (388 ins + 7 del) — confirmed via the single `feat` commit `9c5bcf9` in isolation (`388(+)/7(-)`, 2 files)
- A2b: `origin/feat/430-a2a-llm-steps...origin/feat/430-a2b-orchestrator` -> **320** authored lines (308 ins + 12 del), clean, not yet polluted by a rebase
- B: `origin/feat/430-a2b-orchestrator...origin/feat/430-b-evidence-migration` -> **145** authored lines (142 ins + 3 del), clean
- C (this worktree): `6700695..HEAD` -> **336** authored lines (270 ins + 66 del), clean

All four are under the 400-line budget. This is a verification-methodology note, not a code defect.

### Issues Found

**CRITICAL**:
1. Task 4.11 ("Apply migration 046 to dev, then to prod, BEFORE Phase 5 merges to main") is
   unchecked and unexecuted. This is a deployment-ops action (no code defect — the migration file
   itself is correct, idempotent, and schema-snapshot-tested), but it is a hard prerequisite: until
   migration 046 runs on dev and prod, `mark_turn_resolved`'s new `evidence=` writes will fail
   against those live databases once slice C's routing goes live, because
   `speaker_resolution_evidence` will not exist as a column there. apply-progress.md flags this as
   an "outstanding blocker carried from slice B" for the orchestrator/maintainer. This blocks a
   clean archive of the change until resolved.

**WARNING**:
1. design.md's Testing Strategy names the opt-in live-test gate `MONOLOGUE_LIVE_LLM_TESTS`; the
   shipped test (`test_identify_floor_holder_live_model_resolves_full_name_announcement`) uses
   `LIVE_LLM_TESTS` instead (per an explicit launch-time instruction for slice C, documented in
   apply-progress.md as a deliberate deviation). Does not break any spec requirement — the spec
   text never names this env var — but it is a design-vs-implementation naming mismatch the
   maintainer should confirm is intentional before archiving.
2. Three of the five "Step-1 Floor-Holder Identification" scenarios (full-name, addressee-vs-responder,
   courtesy-then-handover) are proven only by pass-through mock-echo tests
   (`test_identify_floor_holder_is_a_mock_echo`), which assert that whatever `completion_fn` returns
   is relayed unchanged — not that the real model performs the semantic disambiguation the scenario
   describes. Only the "full-name" case has a corresponding real-model proof
   (`test_identify_floor_holder_live_model_resolves_full_name_announcement`), and it is an opt-in
   test skipped by default and in this run (no `OPENAI_API_KEY`/`LIVE_LLM_TESTS=1`). This is an
   explicitly acknowledged, reasonable limitation for LLM-calling code (mocked tests prove the
   contract; only a live call can prove model reasoning) — flagged for visibility, not as a defect.
   The "addressee-vs-responder" and "courtesy-then-handover" cases currently have NO real-model
   check at all, opt-in or otherwise.

**SUGGESTION**:
1. `test_identify_floor_holder_payload_excludes_text_outside_the_window` (the A2a unit-level payload
   test, not the A2b orchestrator-level one) never actually includes any "outside window" text among
   its inputs — it asserts a sentinel string that was never passed anywhere is absent, which is
   trivially true regardless of implementation. It does not weaken the change's actual proof (the
   orchestrator-level `test_resolve_monologue_speaker_payload_excludes_text_outside_the_window` is
   the genuine end-to-end proof and would fail if the whole SRT were sent), but this specific test
   adds no discriminating signal and could be removed or strengthened.
2. No dedicated live-Postgres test runs migration 046 twice to directly prove idempotency at
   runtime (unlike some earlier migrations that have a `test_migration_0NN.py` file); coverage
   relies on the self-evident `ADD COLUMN IF NOT EXISTS` syntax plus the schema-snapshot test. Low
   risk given the migration's simplicity, but would close the gap for parity with the project's
   established per-migration test convention.
3. The "A completion error response is handled without an exception" scenario is proven indirectly:
   the orchestrator-level test that stops before Step 2 uses a `found=false` JSON body, not a
   literal `{"error": "..."}` response, even though both produce the same `FloorHolder()` sentinel
   internally. A literal orchestrator-level error-response test would make this scenario's coverage
   explicit rather than inferred from the shared code path.

### Verdict
**FAIL** — all code, tests (4532 passed/0 failed/29 pre-existing skips), lint, format, and import
checks are green, and 25/25 spec scenarios have a passing covering test, but task 4.11 (applying
migration 046 to dev and prod) remains unchecked and is a hard deployment prerequisite before this
change can be archived; resolve that blocker, then re-verify or acquire a settle to archive.
