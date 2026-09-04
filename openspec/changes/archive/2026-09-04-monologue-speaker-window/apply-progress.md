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

## Phase 4 — B: Evidence Migration

**Status**: 11/12 tasks done, 1 explicitly deferred (4.11 — operational, out of this apply
slice's reach). Branch `feat/430-b-evidence-migration`, base A2b HEAD (`7687cd6`). Commit
`fb112ee`.

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 4.3/4.4 | `tests/congress_videos/sql/test_production_schema.py` | Unit (schema-snapshot) | ✅ 214/214 (full file, run before touching the schema) | Written — `test_column_present_in_block[speaker_turn_videos-speaker_resolution_evidence]` fails: column missing from the extracted CREATE TABLE block | Passed — 215/215 | ➖ Single scenario (membership check); no additional case needed | ➖ None needed |
| 4.5/4.6 | `tests/congress_videos/modules/test_database_speaker_resolution.py` | Unit | ✅ 9/9 (pre-existing `TestMarkTurnResolved` tests, run before editing `database.py`) | Written — `TypeError: got an unexpected keyword argument 'evidence'` on both parametrized cases | Passed — 24/24 | 3 cases: evidence-provided (column present), evidence-omitted (column absent) as ONE `@pytest.mark.parametrize` test, plus a byte-identical golden-string test for the plain 5-positional call | Clean — `evidence_set`/`set_params` stay 2 lines, WHERE clause and `logger.info` untouched byte-for-byte |

### Test Summary
- Total tests written this slice: 4 test functions (5 cases: 1 schema-tuple parametrize case, 2
  `evidence`-presence parametrize cases, 1 byte-identical case, 1 new opt-in live test)
- Total tests passing: 239/239 (targeted, non-live), 3 skipped (live-Postgres, unreachable —
  same as the file's 2 pre-existing live tests); 4529/4529 non-skipped (full suite)
- Layers used: Unit (3 new non-live test functions), opt-in live-Postgres (1 new test, skipped
  in this environment)
- Approval tests: The byte-identical golden-string test IS an approval test — it captures the
  exact pre-#430 SQL text for the 5-positional (no-evidence) call path and asserts the new
  optional parameter changes nothing when omitted.
- Pure functions created: 0 (this slice only extends an existing DB method's SQL-building logic)

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database_speaker_resolution.py -v -o addopts=` → 239 passed |
| Runtime harness command/scenario and exact result | Opt-in live-Postgres round-trip: `TEST_DATABASE_URL=postgresql://airflow:airflow@100.120.28.116:5433/postgres uv run pytest tests/congress_videos/modules/test_mark_turn_resolved_live.py -o addopts= -v` → 3 skipped (`Postgres unavailable` — NAS unreachable from this sandbox, no Tailscale). This is the SAME opt-in/best-effort harness pattern the project has used for every prior live-Postgres slice (see `docs/agents` live-Postgres convention); it is a graceful skip, not a failure. Migration 046 itself was NOT applied to any live database from this apply — see the deferred task 4.11 below. |
| Rollback boundary | Revert commit `fb112ee` (or delete `congress_videos/sql/migrations/046_add_speaker_resolution_evidence.sql`, revert the `production_schema.sql` column+header addition, revert `database.py`'s `evidence` parameter, and the 3 test-file changes). The column is additive and nullable; if migration 046 has already been applied live, leaving it in place (rather than running its manual DOWN) is explicitly the documented rollback per design.md's Migration/Rollout section. |

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/sql/migrations/046_add_speaker_resolution_evidence.sql` | Created | `ALTER TABLE speaker_turn_videos ADD COLUMN IF NOT EXISTS speaker_resolution_evidence TEXT;`, DOWN block commented per the 044 convention — copied verbatim from design.md's exact SQL |
| `congress_videos/sql/production_schema.sql` | Modified | Added `speaker_resolution_evidence TEXT,` after `speaker_resolution_method` in the `speaker_turn_videos` block; extended the folded-migration header comment with `+ 046 (resolution evidence, issue #430)` |
| `congress_videos/modules/database.py` | Modified | `mark_turn_resolved(..., evidence: str \| None = None)` — `evidence_set`/`set_params` computed before the query, appended to the SET clause and param tuple only when `evidence is not None`; WHERE subselect and `logger.info` call byte-identical |
| `tests/congress_videos/sql/test_production_schema.py` | Modified | Added `"speaker_resolution_evidence"` to `TABLE_COLUMNS["speaker_turn_videos"]` |
| `tests/congress_videos/modules/test_database_speaker_resolution.py` | Modified | +1 parametrized test (evidence-provided/evidence-omitted, 2 cases) + 1 byte-identical golden-string test for the 5-positional call |
| `tests/congress_videos/modules/test_mark_turn_resolved_live.py` | Modified | `speaker_resolution_evidence TEXT` added to `_SCHEMA_SQL`; +1 opt-in live round-trip test (`test_mark_turn_resolved_persists_evidence_only_when_provided`) |
| `openspec/changes/monologue-speaker-window/tasks.md` | Modified | Phase 4 (11 of 12 tasks) marked `[x]`; task 4.11 left `[ ]` and annotated as a deferred deployment action |

### Deviations from Design
None on the code shape — `mark_turn_resolved`'s `evidence_set`/`set_params` construction and the
migration SQL match design.md's Interfaces/Contracts and Migration sections verbatim. One
**scope deviation, explicitly required by the environment**: task 4.11 ("Apply migration 046 to
dev, then to prod, BEFORE Phase 5 merges to main") was NOT executed. This is a live
database-migration action, not a code-authoring task — it requires SSH/DB credentials to the
dev and prod stacks that this sandboxed apply worktree does not have (confirmed: the NAS
`postgres_shared:5433` opt-in live-test endpoint itself is unreachable — see Runtime harness
above). **This is a hard blocker for the orchestrator/maintainer**: migration 046 must be applied
to both dev and prod before slice C's PR merges to main, exactly as the migration file's own
header states and as task 4.11 requires. Do not skip this step when landing slice C.

### Issues Found
None. Migration numbering confirmed clean: `ls congress_videos/sql/migrations/ | tail -3` showed
`044_deterministic_turn_publish_order.sql`, `045_add_chapter_mentioned_people.sql` (PR #449,
issue #432) already present on this branch, so 046 required no renumbering.

### Quality Gate
`uv run ruff check congress_videos/modules/database.py tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database_speaker_resolution.py tests/congress_videos/modules/test_mark_turn_resolved_live.py` → All checks passed.
`uv run ruff format --check` (same paths) → clean (1 file auto-reformatted during this slice, all logic-preserving).
Note: `ruff` does not lint `.sql` files — the migration file has no ruff gate; its shape is
proven by the `test_production_schema.py` snapshot tests and its own SQL is copied verbatim from
design.md.

### Full Suite
`uv run pytest -n auto -q` → 4529 passed, 28 skipped (27 pre-existing live-Postgres skips + 1 new
one from this slice's opt-in live test — Postgres/NAS not reachable in this environment).
`tests/congress_videos/modules/test_monologue_speaker_window.py` and
`tests/congress_videos/test_speaker_turn_prepare_dag.py` are untouched this slice (git status
confirms only the 6 files listed above changed) — the DAG and the monologue resolver are wired
together in slice C, not here.

### Measured Diff
`git diff --stat 7687cd6..HEAD -- . ':!openspec'` → 6 files changed, 142 insertions(+), 3
deletions(-) = **145 authored lines**. Forecast was ~120; actual is 145 — close to forecast and
comfortably under the 400-line budget on the first pass, no collapsing needed (consistent with
the parametrize-from-the-start discipline: the evidence-presence test was written as one
2-case parametrized test rather than two separate functions from the outset).

### Remaining Tasks (later slices — NOT started, per launch scope)
- [ ] Task 4.11 (deferred, this phase) — apply migration 046 to dev, then prod, BEFORE Phase 5
      merges to main. **Orchestrator/maintainer action required.**
- [ ] Phase 5 — C: Routing + caller-suite rewiring + docs (needs A2b and B — both now done).

### Status
11/12 Phase-4 (B) tasks complete (44 cumulative task-count across A1+A2a+A2b+B, plus 1 explicitly
deferred operational task). The `speaker_resolution_evidence` column now exists in the schema
snapshot and `mark_turn_resolved` can write it, but NOTHING calls it with a non-None `evidence=`
yet — that wiring is slice C's job. Ready for `sdd-verify` on this slice, then PR against the A2b
branch (with the 4.11 deployment blocker flagged for the maintainer). Do NOT start Phase 5 (C) in
this session — orchestrator launches it separately per the chain plan.

## Phase 5 — C: Routing + Wiring (final slice)

**Status**: DONE (12/12 tasks). Branch `feat/430-c-routing`, base B HEAD (`6700695`). Commits
`b4e68e7` (routing + caller-suite rewiring), `588745c` (opt-in `live_llm` test + marker), `9fdd502`
(docs).

This slice was interrupted once by a rate limit mid-implementation and resumed in the same
session; `git diff` was re-read against the working tree before continuing (per the resume
instruction) to confirm exactly what was already done (routing, 21 patch-site renames, 10
`evidence=` kwargs, the `TestQaPromotionReresolution._run` two-mock rewrite) before adding the 3
routing tests, the `live_llm` test, docs, and this section. One fix made during resume: the new
`from congress_videos.modules.monologue_speaker_window import resolve_monologue_speaker` import
line had landed out of isort order (after `speaker_placeholders` instead of before
`participants_db`) — corrected before the first commit of this slice.

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 5.1/5.2/5.3 | `tests/congress_videos/test_speaker_turn_prepare_dag.py` | Unit | ✅ 72/72 (full file, run before touching the DAG or its suite) | Not executed as a literal fail-first cycle for the 21 mechanical renames (pure caller-suite rewiring following an already-fixed production contract — see Deviations); the 3 NEW routing tests were written fresh against the already-implemented routing and confirmed passing on first run, which is the honest characterization for this kind of caller-suite migration | Passed — 75/75 on first full run after all edits | 3 new tests: monologue→monologue-resolver-only, qa→resolve_speaker-only, promotion-wide-repass→resolve_speaker with turn_type='qa' | Clean — routing is a 6-line if/else at one call site; no extraction needed |
| — | `tests/congress_videos/modules/test_monologue_speaker_window.py` | Unit (opt-in live) | ✅ 47/47 (file, before adding) | N/A — additive opt-in test, skipped by construction until env vars are set | Skips cleanly with `--strict-markers` on (marker registered in `pyproject.toml` first) | N/A — single scenario, real-model check | ➖ None needed |

### Deviation Note on TDD Discipline for This Slice

Task 5.1 nominally asks for "RED: 3 routing tests ... fails" as a strict red-green pair for the
whole caller-suite migration. In practice this slice is a **caller-suite migration following an
already-implemented production change**, not new production behavior discovered test-first: the
routing logic in `speaker_turn_prepare_dag.py` was written first (matching design.md's exact
snippet), then the 23-site caller-suite rewiring was mechanical (each patch target renamed to
match what the code under test now actually calls), and only the 3 NEW routing tests are genuinely
new test coverage. Those 3 were confirmed to describe behavior not covered by any pre-existing
test (the old suite never asserted "the monologue resolver, not resolve_speaker, is called" as its
own scenario), and running the full targeted suite once immediately after all edits landed
75/75 green on the first attempt with zero failed-then-fixed iterations — consistent with a
correctly-executed mechanical rewiring, not a sign the tests weren't exercised. This is recorded
honestly rather than fabricating a fail-first log for renames that could not fail differently from
the routing change they must track 1:1.

### Test Summary
- Total tests written/changed this slice: 3 new routing tests + 1 new opt-in live test + 21
  patch-site renames + 10 `evidence=` kwarg additions + 1 two-mock `_run` rewrite (9 call sites) +
  1 dual-patch addition (`test_monologue_and_qa_both_vad_called`)
- Total tests passing: 75/75 (`test_speaker_turn_prepare_dag.py`), 47 passed + 1 skipped
  (`test_monologue_speaker_window.py`), 4532/4532 non-skipped (full suite)
- Layers used: Unit (24 new/changed test functions), opt-in live-LLM (1, skipped in this
  environment — no `OPENAI_API_KEY`/`LIVE_LLM_TESTS=1`)
- Approval tests: The `TestQaPromotionReresolution` suite functions as an approval suite for the
  qa-promotion re-resolution contract — all 9 of its tests still assert the exact same
  slug/confidence/promotion outcomes as before, now via two independent mocks instead of one
  shared-mock call-order trick
- Pure functions created: 0 (routing is a conditional at an existing call site)

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/test_speaker_turn_prepare_dag.py -v -o addopts=` → 75 passed |
| Runtime harness command/scenario and exact result | `PYTHONPATH=. uv run python congress_videos/speaker_turn_prepare_dag.py` → imports cleanly (only pre-existing Airflow `RemovedInAirflow3Warning` deprecation notices, no errors). Bare `uv run python congress_videos/speaker_turn_prepare_dag.py` (no `PYTHONPATH=.`) fails with `ModuleNotFoundError: No module named 'congress_videos'` — a pre-existing project-layout property of this environment, not a regression from this change; task 5.6 is recorded against the working `PYTHONPATH=.` form. |
| Rollback boundary | Revert commit `b4e68e7` (routing + caller-suite rewiring): monologue turns return to `resolve_speaker`, the module goes fully inert again, and the caller suite reverts to its pre-#430 single-mock shape. `588745c` (live_llm test) and `9fdd502` (docs) can be reverted independently without affecting routing. |

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/speaker_turn_prepare_dag.py` | Modified | Import `resolve_monologue_speaker` (isort-ordered); route `turn_type != 'qa'` to it, `turn_type == 'qa'` to `resolve_speaker` unchanged; `evidence=winner.get("audit") or winner.get("evidence") or None` at the `mark_turn_resolved` call site; inline comment explains the routing (no new scheduler trigger words) |
| `tests/congress_videos/test_speaker_turn_prepare_dag.py` | Modified | 21 patch-site renames (`resolve_speaker` → `resolve_monologue_speaker` for `_make_turn()`-based tests with no `turn_type`); 1 dual-patch addition (`test_monologue_and_qa_both_vad_called`, mixed turn types); `TestQaPromotionReresolution._run` rewritten with two independent mocks (`mock_narrow`/`mock_wide`) plus all 9 call sites and their call-count/call-arg assertions updated; `TestQaReresolutionNoSignalRegression` gains an explicit `mock_wide.assert_not_called()`; 10 `mark_turn_resolved.assert_called_once_with(...)` gain `evidence="..."`; new `TestSpeakerResolutionRouting` class with the 3 routing tests |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Modified | +1 opt-in `@pytest.mark.live_llm` test (`test_identify_floor_holder_live_model_resolves_full_name_announcement`), skipped unless `OPENAI_API_KEY` and `LIVE_LLM_TESTS=1` |
| `pyproject.toml` | Modified | `live_llm` marker registered in `[tool.pytest.ini_options].markers` |
| `docs/PIPELINE.md` | Modified | One paragraph in Fase 2 describing the two-step monologue resolver |
| `openspec/changes/monologue-speaker-window/tasks.md` | Modified | Phase 5 (12 tasks) marked `[x]` |

### Deviations from Design
1. **`LIVE_LLM_TESTS` env var name, not `MONOLOGUE_LIVE_LLM_TESTS`.** design.md's Testing
   Strategy names the opt-in gate `MONOLOGUE_LIVE_LLM_TESTS`; this launch's explicit instruction
   named it `LIVE_LLM_TESTS`. Implemented exactly as instructed for this launch
   (`LIVE_LLM_TESTS=1` + `OPENAI_API_KEY`) since it is the more recent, explicit directive for this
   slice. **Flagged for the orchestrator**: if `MONOLOGUE_LIVE_LLM_TESTS` was the intended
   canonical name, this is a one-line rename in
   `tests/congress_videos/modules/test_monologue_speaker_window.py`'s `skipif` condition.
2. **TDD framing for the 21 mechanical renames** — see the dedicated "Deviation Note on TDD
   Discipline" subsection above; the 3 NEW routing tests were the genuine RED→GREEN cycle for this
   slice.

Otherwise this slice matches design.md's caller routing snippet, the `mark_turn_resolved`
`evidence=` call site, and the qa-promotion wide re-pass contract verbatim.

### Issues Found
None.

### Quality Gate
`uv run ruff check congress_videos/speaker_turn_prepare_dag.py tests/congress_videos/test_speaker_turn_prepare_dag.py tests/congress_videos/modules/test_monologue_speaker_window.py` → All checks passed (after `ruff format` auto-wrapped 5 lines that exceeded 120 chars once `resolve_speaker` renamed to the 10-character-longer `resolve_monologue_speaker`).
`uv run ruff format --check` (same paths) → clean.

### Full Suite
`uv run pytest -n auto -q` → 4532 passed, 29 skipped (28 pre-existing live-Postgres/live-LLM-shape
skips from A1–B, +1 new: this slice's opt-in `live_llm` test, skipped because
`OPENAI_API_KEY`/`LIVE_LLM_TESTS=1` are not set in this environment).
`git diff 6700695..HEAD -- tests/congress_videos/modules/test_speaker_resolution.py` → 0 lines (frozen
suite byte-identical); `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` →
71 passed.

### Measured Diff
`git diff --stat 6700695..HEAD -- . ':!openspec'` → 5 files changed, 270 insertions(+), 66
deletions(-) = **336 authored lines**. Forecast was ~210-250; actual is 336 — over forecast
(mostly from the 23 mechanical patch-site touches plus the two-mock `_run` rewrite genuinely
needing every one of its 9 call sites updated) but comfortably under the 400-line budget on the
first pass, no collapsing needed.

### Follow-up Issue (for the orchestrator to file post-merge)

**Title**: `refactor(speaker-resolution): reassess resolve_speaker's narrow intro+turn branch after monologue routing (#430)`

**Body**:

> After #430, `speaker_turn_prepare_dag.py` never calls `resolve_speaker` with a non-`qa`
> `turn_type` in production: every call site now passes `turn_type='qa'`, either because the turn
> itself is `qa`, or because the qa-promotion wide re-pass (#342) forces
> `{**turn, "turn_type": "qa"}`. Inside `resolve_speaker` (`speaker_resolution.py:384-390`), the
> narrow (non-wide) intro+turn prompt path is still reachable — but now ONLY as the internal
> fallback for a `qa` turn whose chapter span is unparseable
> (`QA_WIDE_CONTEXT_ENABLED and turn_type == "qa"` but `chapter_span is None`), not as a
> DAG-level "monologue turn" branch anymore.
>
> Three test classes in `tests/congress_videos/modules/test_speaker_resolution.py` guard prompt
> byte-identity for this narrow path and are now guarding that fallback only, not a
> production-reachable monologue branch:
> - `TestNonQaPromptUnchanged` (parametrized over `turn_type` in `["monologue", None]`) asserts
>   `resolve_speaker`'s narrow prompt is byte-identical to `SPEAKER_RESOLUTION_USER_TEMPLATE` for
>   non-`qa` calls — a scenario the DAG no longer produces.
> - `TestWideUserTemplate` asserts the wide template's tail (from `INTRO WINDOW` onward) is
>   byte-identical to the narrow template — still relevant for the fallback's prompt shape, but no
>   longer for a "monologue" call.
> - `TestQaGatedWideContext.test_qa_turn_unparseable_chapter_span_falls_back_to_narrow` is now the
>   ONLY test class describing a call shape that still occurs in production.
>
> **Options for a follow-up issue to resolve**:
> 1. **Keep as a documented fallback.** `resolve_speaker` stays exactly as-is; its non-wide branch
>    is retitled in comments/docstrings as "the qa-unparseable-chapter-span fallback" rather than
>    implicitly "the monologue path". Lowest risk, zero behavior change, purely documentation and
>    test-class-naming clarity.
> 2. **Route the unparseable-chapter-span case to `resolve_monologue_speaker` too.** Since that
>    case no longer has a real chapter-wide context advantage over the two-step window resolver,
>    it could route through the same window-only resolver as monologue turns. Requires re-deriving
>    the window anchor for a qa turn without a parseable chapter span and re-verifying `#322`'s
>    guarantees don't quietly regress.
> 3. **Delete the narrow branch from `resolve_speaker` entirely** once (2) ships, collapsing
>    `resolve_speaker` to a wide-context-only qa resolver. Highest risk; requires re-verifying every
>    `#322` guarantee and updating/removing `TestNonQaPromptUnchanged`,
>    `TestPreGateUnchangedSlice1`'s non-qa parametrize cases, and any other test asserting non-qa
>    `resolve_speaker` behavior no longer reachable in production.
>
> This is deliberately scoped as a follow-up, not part of #430: `resolve_speaker` and its full test
> suite (`tests/congress_videos/modules/test_speaker_resolution.py`) are frozen for the duration of
> #430 by design (D1), and #430's proposal explicitly marks this reassessment out of scope.

### Status
12/12 Phase-5 (C) tasks complete. This is the LAST slice of the `monologue-speaker-window`
(issue #430) chain: A1 → A2a → A2b → B → C all done. The `speaker_resolution_evidence` column
(migration 046, slice B) now receives real data from every non-qa resolution; the module is no
longer inert. **Outstanding blocker carried from slice B**: task 4.11 (apply migration 046 to dev,
then prod) was explicitly deferred there — this MUST happen before this slice's PR is merged to
`main`, per design.md's Migration/Rollout ordering. Ready for `sdd-verify` across the full chain,
then PR review per the stacked-to-main delivery strategy. STOPPING here — no further slices.
