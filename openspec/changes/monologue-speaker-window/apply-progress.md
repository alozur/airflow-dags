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
