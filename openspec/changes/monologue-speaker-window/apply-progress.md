# Apply Progress: Monologue Speaker Window (issue #430)

## Phase 1 — A1: Window Selection + Prompts

**Status**: DONE (8/8 tasks). Branch `feat/430-a1-window-selection`, base `dev`
(`0b693d4`). Commit `e5fd38d`.

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 1.1/1.2 | `tests/congress_videos/modules/test_monologue_speaker_window.py` | Unit | N/A (new file) | Written — `ImportError: cannot import name 'MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT'` | Passed — 20/20 | 7 cases for `select_preceding_window`, 4 for `turn_anchor_seconds` (boundary, exclusion, overlap, clamp, override incl. `0.0`) | Clean — no duplication, both functions < 15 lines |
| 1.3/1.4 | same file | Unit | N/A (new file) | Same collection failure as above (prompts + module both missing) | Passed — 20/20 | 9 prompt-contract assertions (addressee/found-false/verbatim rules, `string.Formatter` placeholder-set checks per template) | Clean — prompt text copied verbatim from design.md |

### Test Summary
- Total tests written: 20
- Total tests passing: 20
- Layers used: Unit (20)
- Approval tests: None — no refactoring of existing code, only additive
- Pure functions created: 2 (`turn_anchor_seconds`, `select_preceding_window`)

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v -o addopts=` → 20 passed |
| Runtime harness command/scenario and exact result | N/A — no routing, no caller wiring yet (A1 is inert: nothing imports the module or the new prompt constants). `bash scripts/test-airflow-e2e.sh` deferred to a later slice once the module is actually wired; import-safety is proven here by the full-suite DagBag-adjacent collection succeeding (`uv run pytest -n auto` collects and runs cleanly with the new module present) |
| Rollback boundary | Revert commit `e5fd38d` (or delete `congress_videos/modules/monologue_speaker_window.py` + the 4 prompt constants in `congress_videos/config/ai_prompts.py`). Nothing else references either. |

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/monologue_speaker_window.py` | Created | `MONOLOGUE_WINDOW_SECS`, `MONOLOGUE_RESOLUTION_METHOD`, `turn_anchor_seconds`, `select_preceding_window` |
| `congress_videos/config/ai_prompts.py` | Modified | Added `MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT`, `MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE`, `MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT`, `MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE`, verbatim per design.md, after `SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE` |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Created | 20 unit tests: window selection (7), anchor resolution (4), prompt contracts (9) |

### Deviations from Design
None — implementation matches design.md's Interfaces/Contracts and Prompts sections verbatim
for the A1-scoped subset (constants, `turn_anchor_seconds`, `select_preceding_window`, the 4
prompt constants). `FloorHolder`/`AnnouncedIdentity` dataclasses and the LLM-calling functions
are explicitly out of scope for A1 per the tasks.md slice boundary.

### Issues Found
None.

### Quality Gate
`uv run ruff check congress_videos/modules/monologue_speaker_window.py congress_videos/config/ai_prompts.py tests/congress_videos/modules/test_monologue_speaker_window.py` → All checks passed.
`uv run ruff format --check` (same paths) → 3 files already formatted.

### Full Suite
`uv run pytest -n auto -q` → 4494 passed, 27 skipped (all 27 skips are pre-existing live-Postgres
opt-in tests unrelated to this change — Postgres is not running in this environment).

### Measured Diff
`git diff --stat origin/dev...HEAD -- . ':!openspec'` → 3 files changed, 342 insertions(+), 0
deletions(-). Forecast was ~280; actual is 342 — over forecast but under the 400-line PR budget
and under the ~350 resplit threshold from design.md, so no resplit needed.

### Remaining Tasks (later slices — NOT started, per launch scope)
- [ ] Phase 2 — A2a: `FloorHolder`/`AnnouncedIdentity` dataclasses, `identify_floor_holder`,
      `resolve_announced_identity` (branch `feat/430-a2a-llm-steps`, base this A1 branch).
- [ ] Phase 3 — A2b: `build_resolution_audit`, `_load_turn_blocks`, `_resolve_monologue_inner`,
      `resolve_monologue_speaker` orchestrator.
- [ ] Phase 4 — B: Migration 046 + `mark_turn_resolved(evidence=)`.
- [ ] Phase 5 — C: Routing + caller-suite rewiring + docs.

### Status
8/8 Phase-1 (A1) tasks complete. Ready for `sdd-verify` on this slice, then PR against `dev`.
Do NOT start A2a in this session — orchestrator launches it separately per the chain plan.


## Phase 2 — A2a: LLM Steps

**Status**: DONE (8/8 tasks). Branch `feat/430-a2a-llm-steps`, base A1 branch
(`845cd03`). Commit `765d814` (rewritten from an initial `a27e326` + `c5686ef` pair after a
budget-driven test collapse — see *Budget Correction* below; the earlier SHAs no longer exist on
this branch).

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 2.1/2.2 | `tests/congress_videos/modules/test_monologue_speaker_window.py` | Unit | ✅ 20/20 (A1 tests, run before touching the module) | Written — `ImportError: cannot import name 'AnnouncedIdentity'` | Passed — 40/40 | 4 mock-echo pass-through cases (full name, role-only, addressee-vs-responder, courtesy+handover) as ONE `@pytest.mark.parametrize` test, `found=false`, payload-scope exclusion, 3 parametrized error-response cases, 1 raise-propagation case | Clean — `identify_floor_holder` stays 1 branch, < 20 lines |
| 2.3/2.4 | same file | Unit | ✅ 40/40 (A1 + Step-1 tests, run before adding Step 2) | Written — same `ImportError` until 2.2/2.4 land | Passed — 40/40 | confidence-at-threshold/just-below as ONE `@pytest.mark.parametrize` test, slug-outside-roster reject, non-numeric-confidence reject, payload-scope exclusion, roster-presence, 3 parametrized error-response cases, 1 raise-propagation case | Clean — extracted `_validate_announced_identity` helper to keep `resolve_announced_identity` and its own complexity under the C901=10 / 50-line limits |

### Test Summary
- Total tests written this slice: 20 test functions (40 parametrized cases; 40 cumulative with A1)
- Total tests passing: 40/40 (targeted), 4514/4514 non-skipped (full suite)
- Layers used: Unit (20 new test functions)
- Approval tests: None — no refactoring of existing code, only additive
- Pure functions created: 0 new pure functions (both steps call the injected `completion_fn`
  seam by design — not pure); 1 pure helper extracted (`_validate_announced_identity`)

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v -o addopts=` → 40 passed |
| Runtime harness command/scenario and exact result | N/A — still inert (nothing imports `identify_floor_holder`/`resolve_announced_identity` from a caller or the orchestrator; A2b wires them together). Import-safety proven by the full-suite run collecting and passing cleanly with the extended module present: `uv run pytest -n auto -q` → 4514 passed, 27 skipped |
| Rollback boundary | Revert commit `765d814` (or delete the two dataclasses + two functions + `_validate_announced_identity` added to `monologue_speaker_window.py`, and their tests). A1's window-selection primitives and prompts stay usable/tested untouched. |

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/monologue_speaker_window.py` | Modified | Added `FloorHolder`, `AnnouncedIdentity` frozen dataclasses; `identify_floor_holder` (Step 1 seam); `resolve_announced_identity` (Step 2 seam) + `_validate_announced_identity` helper; imports `SPEAKER_RESOLUTION_MIN_CONFIDENCE` from `speaker_resolution.py` and `LLM_CHEAP` from `utils.llm_config` |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Modified | +20 test functions covering 40 parametrized cases: 1 parametrized mock-echo test (4 cases), found=false, 2 payload-scope-exclusion cases, roster-presence, 1 parametrized confidence-boundary test (2 cases), slug-outside-roster, non-numeric-confidence, 2 parametrized error-response tests (3 cases each), 2 raise-propagation cases |
| `openspec/changes/monologue-speaker-window/tasks.md` | Modified | Phase 2 (8 tasks) marked `[x]`; task 2.8 note records the budget-driven collapse and final measured diff |

### Deviations from Design
One documented, deliberate deviation (unchanged by the budget correction): design.md's
"Additional assertable WARNING/INFO lines" list gives log-message templates carrying
`turn_id=%s` (e.g. `"...: step 2 returned slug %r not in roster for turn_id=%s — returning
None"`). The A2a interface signatures (`identify_floor_holder(window_blocks, completion_fn)`,
`resolve_announced_identity(floor_holder, participants, completion_fn)`) do not receive
`turn_id` — only `_resolve_monologue_inner`/`resolve_monologue_speaker` (A2b) do. The step
functions here log the same semantic WARNING/INFO messages (roster miss, invalid confidence, low
confidence, completion error) without `turn_id`, since it is not available at this seam. Tests
assert log level and call-count, not the literal turn_id-bearing text, so no spec requirement is
unsatisfied — the never-raise / error-response / roster / confidence CONTRACTS design.md
requires are all implemented exactly. `turn_id`-qualified logging will be added at the
orchestrator level in A2b, where the design's exact log text applies.

Otherwise implementation matches design.md's Interfaces/Contracts section verbatim for the
A2a-scoped subset.

### Issues Found
None.

### Quality Gate
`uv run ruff check congress_videos/modules/monologue_speaker_window.py tests/congress_videos/modules/test_monologue_speaker_window.py` → All checks passed.
`uv run ruff format --check` (same paths) → clean.

### Full Suite
`uv run pytest -n auto -q` → 4514 passed, 27 skipped (same 27 pre-existing live-Postgres opt-in
skips as A1 — Postgres not running in this environment). `test_speaker_resolution.py` and
`speaker_resolution.py` have a byte-identical (0-line) diff against A1's HEAD (`845cd03`),
confirming the frozen module and its suite are untouched.

### Budget Correction

The first pass (commits `a27e326` + `c5686ef`, now superseded) measured 423 authored lines — 23
over the 400-line budget with no `size:exception` available this run. The orchestrator's
two-step plan was applied:

**Step 1 — collapse near-duplicate tests (sufficient; no split needed).** The 4 separate
`identify_floor_holder` mock-echo tests (`García` / role-only / addressee-vs-`X` / `Ruiz`) were
collapsed into ONE `@pytest.mark.parametrize` test carrying the same 4 cases and the same
assertion shape per case. The 2 separate `resolve_announced_identity` confidence-boundary tests
(0.80 accept / 0.79 reject) were collapsed into ONE `@pytest.mark.parametrize` test carrying the
same 2 cases. No scenario was dropped — `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v -o addopts=` still collects and passes all 40 individual cases
(now as parametrized IDs, e.g. `test_identify_floor_holder_is_a_mock_echo[addressee-vs-responder]`),
proving identical coverage. No test moved away from the code it proves — everything stayed in
`test_monologue_speaker_window.py` next to `identify_floor_holder`/`resolve_announced_identity`.

**Step 2 — re-measure.** `git diff --stat 845cd03..HEAD -- . ':!openspec'` after the collapse →
2 files changed, 388 insertions(+), 7 deletions(-) = **395 authored lines**, under the 400-line
budget. The split into a second `feat/430-a2a2-identity-step` branch was NOT needed.

**Commit rewrite.** Because both original commits were unpushed, `git reset --soft 845cd03` plus
a fresh commit replaced `a27e326`+`c5686ef` with a single up-to-date `feat` commit
(`765d814`) carrying the collapsed tests, followed by this `chore(sdd)` bookkeeping commit. The
branch stayed `feat/430-a2a-llm-steps` throughout; no second branch was created.

### Measured Diff (final)
`git diff --stat 845cd03..HEAD -- . ':!openspec'` → 2 files changed, 388 insertions(+), 7
deletions(-) = 395 authored lines. Forecast was ~250; actual is 395 — under the 400-line budget,
no `size:exception` needed.

### Remaining Tasks (later slices — NOT started, per launch scope)
- [ ] Phase 3 — A2b: `build_resolution_audit`, `_load_turn_blocks`, `_resolve_monologue_inner`,
      `resolve_monologue_speaker` orchestrator (branch `feat/430-a2b-orchestrator`, base this A2a
      branch).
- [ ] Phase 4 — B: Migration 046 + `mark_turn_resolved(evidence=)`.
- [ ] Phase 5 — C: Routing + caller-suite rewiring + docs.

### Status
8/8 Phase-2 (A2a) tasks complete (16 cumulative across A1+A2a). Ready for `sdd-verify` on this
slice, then PR against the A1 branch. Do NOT start A2b in this session — orchestrator launches it
separately per the chain plan.

## Phase 3 — A2b: Orchestrator

**Status**: DONE (8/8 tasks — 3.1 through 3.9, numbered 3.1-3.9 in tasks.md). Branch
`feat/430-a2b-orchestrator`, base A2a HEAD (`e80affb`). Commit `328ecfb`.

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 3.1-3.4/3.2/3.5 | `tests/congress_videos/modules/test_monologue_speaker_window.py` | Unit | ✅ 40/40 (A1+A2a tests, run before touching the module) | Written — `ImportError: cannot import name 'resolve_monologue_speaker'` | Passed — 47/47 on first implementation pass | 7 end-to-end cases: pre-gate no-call, payload-exclusion (sentinels before window_start and at/after anchor absent from every captured prompt across both steps), found=false stops before step 2, unlocatable evidence → None, successful resolution shape + 7-key audit, never-raise parametrized over `raising_step` in {1, 2} | Clean — `_resolve_monologue_inner` stays one straight-line function under 50 lines / C901=10; `resolve_monologue_speaker` is a 2-line try/except wrapper matching `resolve_speaker`'s shape exactly |

### Test Summary
- Total tests written this slice: 7 test functions (9 parametrized cases with the 2-case
  never-raise parametrize; 47 cumulative with A1+A2a)
- Total tests passing: 47/47 (targeted), 4521/4521 non-skipped (full suite)
- Layers used: Unit (7 new test functions)
- Approval tests: None — no refactoring of existing code, only additive
- Pure functions created: `build_resolution_audit` (pure — no I/O, deterministic JSON from its
  four inputs). `_load_turn_blocks` is I/O-bound by design (duplicated from the frozen module per
  D5) and is exercised only through the patched `find_srt_for_chapter`/`_parse_srt_blocks` seam.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -v -o addopts=` → 47 passed |
| Runtime harness command/scenario and exact result | N/A — still inert (no caller imports `resolve_monologue_speaker`; slice C wires routing). Import-safety proven by the full-suite run collecting and passing cleanly with the extended module present: `uv run pytest -n auto -q` → 4521 passed, 27 skipped |
| Rollback boundary | Revert commit `328ecfb` (or delete `build_resolution_audit`, `_load_turn_blocks`, `_resolve_monologue_inner`, `resolve_monologue_speaker` from `monologue_speaker_window.py`, and their tests). A1's window-selection primitives and A2a's two LLM steps stay usable/tested untouched. |

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/monologue_speaker_window.py` | Modified | Added `build_resolution_audit`, `_load_turn_blocks`, `_resolve_monologue_inner`, `resolve_monologue_speaker`; imports `get_video_chapter_dir`, `has_announcement_phrase`, `_evidence_supported_in_blocks` (from the frozen `speaker_resolution.py`, unmodified), `find_srt_for_chapter`/`_parse_srt_blocks` (module-level, so tests patch this module's own namespace) |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Modified | +7 test functions: pre-gate no-call, payload-exclusion end-to-end, found=false stops before step 2, unlocatable evidence, successful-resolution shape+audit, 1 parametrized never-raise test (2 cases: raise on step 1, raise on step 2) |
| `openspec/changes/monologue-speaker-window/tasks.md` | Modified | Phase 3 (9 tasks) marked `[x]`, with a note on the deferred `live_llm` marker and the first-pass 320-line measurement |

### Deviations from Design
Two documented deviations, both consistent with A2a's already-recorded pattern:

1. **`live_llm` marker not added.** design.md's Testing Strategy describes an opt-in
   `@pytest.mark.live_llm` test proving the addressee/floor-holder distinction against the real
   model, but this test is not assigned to any task in tasks.md Phase 1, 2, or 3 — only mentioned
   in design.md's prose. Per the launch instruction ("if the opt-in live test lands in this phase
   per tasks.md"), it does NOT land in A2b (tasks.md never schedules it), so no `live_llm` marker
   was registered in `[tool.pytest.ini_options].markers`. **Risk flagged for the orchestrator**:
   this test may need to be added retroactively to A2a or A2b, or explicitly scoped into a later
   phase, since no phase currently owns it.
2. **Roster-miss/confidence-below-threshold WARNING/INFO still lack `turn_id`** at the
   `resolve_announced_identity` level (A2a's documented deviation, unchanged — that function's
   signature has no `turn_id` parameter). A2b's OWN log lines (pre-gate skip, step-1 found=false
   skip, evidence-not-locatable, and the top-level never-raise catch) all carry `turn_id`, exactly
   as design.md specifies, since the orchestrator has it. This satisfies the launch instruction's
   "turn_id-qualified WARNING shapes (now that turn_id is available)" for every log line the
   orchestrator itself emits.

Otherwise implementation matches design.md's Interfaces/Contracts and Data Flow sections verbatim
for the A2b-scoped subset.

### Issues Found
None.

### Quality Gate
`uv run ruff check congress_videos/modules/monologue_speaker_window.py tests/congress_videos/modules/test_monologue_speaker_window.py` → All checks passed.
`uv run ruff format --check` (same paths) → clean, no reformatting needed.

### Full Suite
`uv run pytest -n auto -q` → 4521 passed, 27 skipped (same 27 pre-existing live-Postgres opt-in
skips as A1/A2a — Postgres not running in this environment). `test_speaker_resolution.py` and
`speaker_resolution.py` have a byte-identical (0-line) diff against A2a's HEAD (`e80affb`),
confirming the frozen module and its suite are untouched.

### Measured Diff
`git diff --stat e80affb..HEAD -- . ':!openspec'` → 2 files changed, 308 insertions(+), 12
deletions(-) = **320 authored lines**. Forecast was ~230; actual is 320 — over forecast but
comfortably under the 400-line budget, landed on the FIRST pass with no collapsing needed. This
validates the coordinator's directive to parametrize near-duplicate tests from the start: the
never-raise test was written as one `@pytest.mark.parametrize("raising_step", [1, 2])` test
rather than two separate functions from the outset, rather than being collapsed after the fact as
in A2a.

### Remaining Tasks (later slices — NOT started, per launch scope)
- [ ] Phase 4 — B: Migration 046 + `mark_turn_resolved(evidence=)` (branch
      `feat/430-b-evidence-migration`, base this A2b branch).
- [ ] Phase 5 — C: Routing + caller-suite rewiring + docs (needs A2b and B).

### Status
9/9 Phase-3 (A2b) tasks complete (33 cumulative across A1+A2a+A2b). All four public module
functions (`select_preceding_window`, `identify_floor_holder`, `resolve_announced_identity`,
`resolve_monologue_speaker`) now exist and are fully tested; the module remains INERT — no
caller imports it. Ready for `sdd-verify` on this slice, then PR against the A2a branch. Do NOT
start Phase 4 (B) in this session — orchestrator launches it separately per the chain plan.
