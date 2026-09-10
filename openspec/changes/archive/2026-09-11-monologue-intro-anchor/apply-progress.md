# Apply Progress: Monologue Intro Anchor (issue #613)

**Mode**: Strict TDD
**Status**: 17/17 tasks complete. Ready for verify.

## Completed Tasks

- [x] 1.1 RED: `TestSelectUnpreparedTurnsChapterFirstSubstantive` (SQL-shape) in `test_database.py`
- [x] 1.2 GREEN: `SUBSTANTIVE_TURN_MIN_SECS` + `is_chapter_first_substantive` column in `database.py`
- [x] 1.3 RED: live row-level test in `test_mark_turn_resolved_live.py`
- [x] 1.4 GREEN: confirmed against a real disposable Postgres (NAS, Tailscale route)
- [x] 2.1 RED: `monologue_window_start` unit tests in `test_monologue_speaker_window.py`
- [x] 2.2 GREEN: `MONOLOGUE_INTRO_MAX_GAP_SECS` + `monologue_window_start` in `monologue_speaker_window.py`
- [x] 2.3 GREEN: `window_start` keyword-only param on `select_preceding_window`
- [x] 2.4 RED: boundary tests reusing the extended window (characterization, since 2.3 already implemented the kwarg)
- [x] 2.5 GREEN: wired `_resolve_monologue_inner` to `monologue_window_start` + audit + INFO log
- [x] 2.6 RED+GREEN: turn-335 end-to-end fixture (signal true resolves; signal false returns None, zero LLM calls)
- [x] 3.1 RED+GREEN: "gap above cap keeps standard window" scenario (parametrized with 2.1's fallback tests)
- [x] 3.2 RED+GREEN: "extended window still clamps at zero" scenario
- [x] 3.3 Confirmed `speaker_turn_prepare_dag` tests unchanged (85/85 pass, no drift)
- [x] 4.1 Full suite: `uv run pytest -n auto` — 5596 passed, 36 skipped (local Postgres unavailable)
- [x] 4.2 Lint: `ruff check` + `ruff format --check` clean, no C901 violation
- [x] 4.3 DagBag import check clean (18 dags, 0 import errors); Docker e2e reported `unavailable` (sandbox has no docker daemon)
- [x] 4.4 `openspec/changes/monologue-intro-anchor/` confirmed untracked in the code diff

## Files Changed

| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/database.py` | Modified | Added `SUBSTANTIVE_TURN_MIN_SECS` constant and `is_chapter_first_substantive` BOOL_OR/NOT EXISTS column to `select_unprepared_turns` |
| `congress_videos/modules/monologue_speaker_window.py` | Modified | Added `MONOLOGUE_INTRO_MAX_GAP_SECS`, pure helper `monologue_window_start`, `window_start` kwarg on `select_preceding_window`, wired `_resolve_monologue_inner` |
| `tests/congress_videos/modules/test_database.py` | Modified | `TestSelectUnpreparedTurnsChapterFirstSubstantive` (one SQL-shape test, all D1/D2 assertions) |
| `tests/congress_videos/modules/test_mark_turn_resolved_live.py` | Modified | `_seed_turn` extended (start/end/is_procedural), `TestSelectUnpreparedTurnsChapterFirstSubstantiveLive` (2 live tests) |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Modified | `monologue_window_start` tests (geometry, non-regression, parametrized fallbacks, clamp), `select_preceding_window` explicit-start boundary tests, turn-335 end-to-end tests |

## TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 1.1/1.2 | `test_database.py` | Unit (SQL-shape mock) | ✅ 86/86 pre-existing | ✅ Written (4 asserts failed on missing column/keywords) | ✅ Passed | ✅ 6 assertions covering D1/D2 in one scenario | ✅ Consolidated 4 tests → 1 (design.md lists 1 SQL-shape scenario) |
| 1.3/1.4 | `test_mark_turn_resolved_live.py` | Live-Postgres (opt-in) | ✅ 3/3 pre-existing | ✅ Written against live disposable DB (NAS) | ✅ Passed (5/5 incl. pre-existing) | ✅ 2 cases: blip-precedes (true) / procedural-counts (false) | ✅ `_seed_turn` end_seconds branch → single COALESCE INSERT |
| 2.1/2.2 | `test_monologue_speaker_window.py` | Unit (pure function) | ✅ 54/54 pre-existing | ✅ Written (ImportError before impl existed) | ✅ Passed | ✅ 7 cases: geometry, non-regression, 4 parametrized fallbacks, clamp | ✅ None needed |
| 2.3/2.4 | `test_monologue_speaker_window.py` | Unit | N/A (kwarg add, 2.3 GREEN precedes 2.4 per tasks.md ordering) | ➖ Characterization (kwarg implemented in 2.3, tests confirm in 2.4) | ✅ Passed | ✅ 4 parametrized boundary cases | ✅ None needed |
| 2.5/2.6 | `test_monologue_speaker_window.py` | Unit (orchestrator) | ✅ 60/60 pre-existing (post 2.1-2.4) | ✅ Written; signal-true case failed pre-wiring (`result is None`) | ✅ Passed after wiring | ✅ 2 cases: signal true (resolves) / signal false (None, zero calls) | ✅ None needed |
| 3.1/3.2 | `test_monologue_speaker_window.py` | Unit | ✅ (same suite) | ✅ Same parametrized/dedicated tests as 2.1 | ✅ Passed | ➖ Single each (named scenario tests) | ✅ None needed |

## Test Summary

- **Total tests added/modified**: 27 test functions (net, after consolidation) across 3 files
- **Total tests passing**: 5596 passed, 36 skipped (full suite, local Postgres unavailable) + 5 passing against a real disposable Postgres (NAS)
- **Layers used**: Unit (majority), SQL-shape mock (`test_database.py`), Live-Postgres opt-in (`test_mark_turn_resolved_live.py`, exercised live against a disposable NAS DB, then dropped)
- **Pure functions created**: 1 (`monologue_window_start`)

## Work Unit Evidence

| Work Unit | Focused test command / result | Runtime harness / result | Rollback boundary |
|-----------|-------------------------------|---------------------------|--------------------|
| 1: SQL signal | `uv run pytest tests/congress_videos/modules/test_database.py tests/congress_videos/modules/test_mark_turn_resolved_live.py -o addopts= -q` → 92 passed (87 + 5 live, live run required a disposable NAS Postgres) | Live-Postgres row-level test against disposable `test_airflow_dags_613`/`_613b` DB on NAS (Tailscale `100.75.246.38:5433`), created/dropped via `docker exec postgres_shared psql -U admin` (trust auth); DagBag import check clean | Revert commit `eea18c6`; purely additive SQL column, no migration, no persisted-state change |
| 2+3: window-start extension | `uv run pytest tests/congress_videos/modules/test_monologue_speaker_window.py -o addopts= -q -m "not live_llm"` → 60 passed | `resolve_monologue_speaker` end-to-end with turn-335 fixture (mocked `completion_fn`, real SRT-block selection/pre-gate/audit logic) | Revert commit `6c218b7`; `window_start` kwarg defaults to `None` (legacy formula), so any caller not yet passing it is unaffected |
| test consolidation | Same commands as above, re-run post-consolidation, same pass counts | N/A — test-only change | Revert commit `db3b60f`; no production code touched |

## Deviations from Design

- Task ordering: implemented 2.5 (wiring) only after writing the turn-335 driving test (2.6) first, to keep the RED→GREEN discipline strict, even though tasks.md lists 2.5 before 2.6. Functionally identical to the plan.
- Test consolidation (not in original task list): to stay within the 400-line hard review budget (initial honest implementation measured 453 authored lines vs 7f4cd5a), consolidated four near-duplicate SQL-shape tests into one (matching design.md's own Testing Strategy table, which lists exactly one "SQL shape" scenario), and parametrized two sets of near-identical `monologue_window_start`/`select_preceding_window` boundary tests. Zero coverage was removed — same assertions, same case count, re-verified green (including a second live-Postgres pass) after the change. Final measured diff: 373 authored lines (362 additions + 11 deletions) vs 7f4cd5a.
- design.md's "Open Questions" PR-budget concern about `explore.md`/proposal size is moot for the code PR: the orchestrator's delivery instructions already exclude `openspec/` entirely from this PR (stays untracked), so that concern does not apply here.

## Issues Found

None.

## Workload / PR Boundary

- Mode: single PR (delivery_strategy: auto-chain, but Chained PRs recommended: No / Low risk — no chaining needed)
- Current work unit: entire change (both suggested work units — SQL signal and window-start extension — landed as 3 sequential work-unit commits on one branch)
- Boundary: `eea18c6` (SQL signal) → `6c218b7` (window-start extension) → `db3b60f` (test consolidation to stay under budget), all on `fix/613-monologue-intro-anchor`, based on `origin/dev` @ `7f4cd5a`
- Estimated review budget impact: 373 authored changed lines (additions + deletions) vs the 400-line hard budget — under budget with margin. `openspec/changes/monologue-intro-anchor/` stays untracked, per delivery instructions.

## Status

17/17 tasks complete. Ready for verify.
