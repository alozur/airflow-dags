# Apply Progress: session-intro-card-overlay

## Batch 1 — PR 1: `intro_sesion` tipo + renderer + registration

**Mode**: Strict TDD
**Base branch**: tracker `feat/558-session-intro-card`
**Working branch**: `feat/558-slice1-tipo-renderer`

### Completed Tasks

- [x] 1.1 RED: `intro_sesion` tipo-shape test (schema match vs `extracto_sesion`, no timing state)
- [x] 1.2 GREEN: `intro_sesion` config entry added to `congress_videos/config/video_editor_config.py`
- [x] 1.3 RED: unregistered-renderer fails loudly (`apply_overlays` raises `KeyError` naming the tipo)
- [x] 1.4 GREEN: `_render_intro_sesion(overlay, style, W, H)` implemented in `video_editor.py`
- [x] 1.5 GREEN: `"intro_sesion": _render_intro_sesion` registered in `_PILLOW_RENDERERS`
- [x] 1.6 GREEN: composite-within-time-window test (`apply_overlays`, ffmpeg subprocess patched)
- [x] 1.7 REFACTOR: extracted shared `_render_horizontal_bar_card` helper, deduping
      `_render_extracto_sesion` and `_render_intro_sesion` (only the vertical anchor differs)

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 1.1 | `tests/congress_videos/modules/test_video_editor.py::TestIntroSesionConfig` | Unit | ✅ 110/110 (baseline) | ✅ Written — 4 tests failed (`intro_sesion` absent from config) | ✅ Passed after 1.2 | ✅ 4 cases (existence, schema-equality, required-keys, no-timing-state) | ➖ None needed — pure data literal |
| 1.2 | same as 1.1 | Unit | N/A (config edit) | — | ✅ 5/5 in `TestIntroSesionConfig` pass | — | ➖ None needed |
| 1.3 | `tests/congress_videos/modules/test_video_editor.py::TestIntroSesionRenderer::test_intro_sesion_without_renderer_fails_loudly` | Unit | ✅ 118/119 (only 1.4-dependent tests still red at this point) | ✅ Written — uses `monkeypatch.delitem(_PILLOW_RENDERERS, "intro_sesion", raising=False)` so the assertion stays meaningful even after 1.5 registers the real renderer | ✅ Passed (raised via the pre-existing generic `render_pillow_overlay` dispatch check — no new production code needed; this test pins that guarantee for `intro_sesion` specifically) | ➖ Single scenario per spec (unregistered tipo → `KeyError`) | ➖ None needed |
| 1.4 | `TestIntroSesionRenderer::test_intro_sesion_renderer_returns_rgba_image_of_correct_size`, `..._is_not_fully_transparent`, `..._renders_without_descripcion` | Unit | ✅ 119 total before this task (5 config + 1 fail-loud passing, 3 renderer tests red) | ✅ Written first (referenced `_render_intro_sesion`, which did not exist) | ✅ Passed after implementing `_render_intro_sesion` | ✅ 3 cases: size/mode, non-transparent pixel, missing `descripcion` | ✅ Clean — see 1.7 |
| 1.5 | `TestIntroSesionRenderer::test_apply_overlays_composites_intro_sesion_within_time_window` | Unit (dispatch-level) | ✅ (same run) | ✅ Written first (dispatch raised `KeyError` until registered) | ✅ Passed after adding `"intro_sesion": _render_intro_sesion` to `_PILLOW_RENDERERS` | ➖ Single — dispatch registration is binary | ➖ None needed |
| 1.6 | same as 1.5 | Unit (`apply_overlays`, ffmpeg `subprocess.run` patched, `render_pillow_overlay` wrapped/spied) | ✅ | ✅ Written first | ✅ Passed — asserts `result["success"] is True`, `render_spy.assert_called_once()`, and the built ffmpeg `-filter_complex` string contains `between(t,0.0,5.0)` (the overlay's own time window) | ➖ Single scenario (spec has one "registered renderer produces the card" scenario) | ➖ None needed |
| 1.7 | Full `tests/congress_videos/modules/test_video_editor.py` (approval-style: pre-refactor green run captured, then re-run after extraction) | Unit | ✅ 119/119 before refactor | N/A (refactor task) | ✅ 119/119 still passing after extracting `_render_horizontal_bar_card` | N/A | ✅ Clean — removed ~18 duplicated lines between the two renderers |

### Test Summary
- **Total tests written**: 9 (`TestIntroSesionConfig`: 4, `TestIntroSesionRenderer`: 5)
- **Total tests passing**: 9/9 new, 119/119 in `test_video_editor.py`, 5156/5156 in the full repo suite (34 pre-existing skips, unrelated to this change — Postgres-live tests and one env-specific SRT-size guard)
- **Layers used**: Unit (9)
- **Approval tests** (refactoring): 1 full-file regression run before/after the 1.7 extraction (119/119 both times)
- **Pure functions created**: `_render_intro_sesion` (thin wrapper) + `_render_horizontal_bar_card` (shared pure Pillow drawing helper)

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/config/video_editor_config.py` | Modified | Added `intro_sesion` tipo entry to the `congreso` domain — same 12 style keys as `extracto_sesion`, centered variant values |
| `congress_videos/modules/video_editor.py` | Modified | Added `_render_horizontal_bar_card` shared helper, `_render_intro_sesion`, registered it in `_PILLOW_RENDERERS`; refactored `_render_extracto_sesion` to reuse the shared helper (behavior-preserving) |
| `tests/congress_videos/modules/test_video_editor.py` | Modified | Added `TestIntroSesionConfig` (T-14, 4 tests) and `TestIntroSesionRenderer` (T-15, 5 tests); updated module docstring's test-group index |
| `openspec/changes/session-intro-card-overlay/tasks.md` | Modified | Marked tasks 1.1–1.7 `[x]` |

### Deviations from Design
None — implementation matches design D2's "centered rather than bottom-anchored" instruction and the interface contract (mirrors `extracto_sesion` keys exactly). The `margin_y` key is repurposed for `intro_sesion` as a fine-tuning vertical offset from true center (default `0`) rather than a margin from the bottom edge, since the schema requires the key to be present but a bottom-margin semantic does not apply to a centered card — this is a naming reuse, not a functional gap; noted here for traceability.

### Issues Found
None.

## Work Unit Evidence (PR 1)

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k intro_sesion` → **9 passed** |
| File-scoped regression command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -q --no-cov` → **119 passed** (110 pre-existing + 9 new, 0 regressions) |
| Full-suite command and exact result | `uv run pytest` → **5156 passed, 34 skipped**, exit code 0 (coverage gate `--cov-fail-under=80` satisfied — no failure output) |
| Lint / format | `uv run ruff check .` → All checks passed. `uv run ruff format --check .` → 313 files already formatted |
| Runtime harness command/scenario and exact result | N/A — pure Pillow/config unit per the tasks.md work-unit table; no DAG wiring, no ffmpeg subprocess actually spawned (patched in all tests), no DB or Airflow runtime boundary touched in this slice |
| Rollback boundary | Revert the 3 files above (config entry, renderer + registration, tests). Nothing else in the repo references `intro_sesion` yet — it is inert until PR 4 wires `apply_intro_overlay` (t5b) into `youtube_upload_dag.py` |

## Workload / PR Boundary
- Mode: chained PR slice (`auto-chain`, `feature-branch-chain`)
- Current work unit: PR 1 of 4 (`intro_sesion` tipo + renderer + registration)
- Boundary: starts from the tracker branch `feat/558-session-intro-card` (base, untouched by this slice) and ends with `intro_sesion` fully defined, registered, and covered by tests — no caller wires it yet
- Changed lines (`git diff --stat` across the 3 changed source/test files, authored, additions+deletions): **186 insertions + 3 deletions = 189**, vs. the ~140 estimate in tasks.md — within the 400-line budget, no `size:exception` needed
- Out of scope (untouched, confirmed via task scope, not yet re-verified via `git diff --stat` against `main` since PR 2/3/4 are still pending on this branch): `resolve_overlay_slot`, `max_timeout` kwarg, duration guard, t5b task, DAG wiring, `youtube_upload_dag.py`, all reap DAGs, `speaker_turn_videos_dag.py`, DB schema

## Batch 2 — PR 2: `resolve_overlay_slot` + default-window constant

**Mode**: Strict TDD
**Base branch**: PR 1 branch `feat/558-slice1-tipo-renderer` (merged-in as commit `a365382`)
**Working branch**: `feat/558-slice2-overlap-helper`

### Completed Tasks

- [x] 2.1 RED: `resolve_overlay_slot(existing=[], start, dur)` returns the window unchanged
- [x] 2.2 RED: full overlap `existing=[(0,10)]` shifts requested window to start at 10, same duration
- [x] 2.3 RED: partial overlap shifts to the earliest free slot at/after the original start
- [x] 2.4 RED: never shifts backward, never starts before `0.0`
- [x] 2.5 GREEN: implemented pure `resolve_overlay_slot(existing, requested_start, requested_duration) -> tuple[float, float]` in `video_editor.py`
- [x] 2.6 GREEN: added `INTRO_WINDOW_SECONDS = (0.0, 5.0)` module constant in `video_editor.py`
- [x] 2.7 RED→GREEN: caller with no custom window occupies exactly `[0, 5)`, read from the constant (verified via `resolve_overlay_slot(existing=[], *INTRO_WINDOW_SECONDS...)` with no other windows placed — `_apply_intro_overlay`, the real caller, does not exist until PR 4)
- [x] 2.8 REFACTOR: docstring on `resolve_overlay_slot` states the never-backward / never-negative / duration-preserving invariants; added to module's Public API index

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 2.1–2.4 | `tests/congress_videos/modules/test_video_editor.py::TestResolveOverlaySlot` | Unit | ✅ 119/119 (PR 1 baseline) | ✅ Written first — 8 tests failed with `ImportError: cannot import name 'resolve_overlay_slot'` | ✅ 8/8 passed after implementing `resolve_overlay_slot` | ✅ 8 cases: no-overlap, single-overlay no-op, full overlap, partial overlap, never-before-zero, never-backward-past-a-later-window (unsorted `existing`), duration preserved, purity (no mutation of `existing`) | ➖ None needed at this step — see 2.8 |
| 2.6–2.7 | `TestIntroWindowConstant` | Unit | ✅ (same run) | ✅ Written first — 2 tests failed with `ImportError: cannot import name 'INTRO_WINDOW_SECONDS'` | ✅ 2/2 passed after adding the constant | ✅ 2 cases: constant value `(0.0, 5.0)`, and default-window-via-`resolve_overlay_slot` composition | ➖ None needed |
| 2.8 | Full `tests/congress_videos/modules/test_video_editor.py` (approval-style: green before/after docstring + module-docstring update) | Unit | ✅ 129/129 before | N/A (refactor/docs task) | ✅ 129/129 still passing after adding the invariants docstring and the Public API index line | N/A | ✅ Clean — docstring-only change, no behavior touched |

### Test Summary
- **Total tests written**: 10 (`TestResolveOverlaySlot`: 8, `TestIntroWindowConstant`: 2)
- **Total tests passing**: 10/10 new, 129/129 in `test_video_editor.py`, 5166/5166 in the full repo suite (same 34 pre-existing skips as PR 1 — Postgres-live tests and one env-specific SRT-size guard)
- **Layers used**: Unit (10)
- **Pure functions created**: `resolve_overlay_slot` (no I/O, no globals, no mutation of `existing` — purity pinned by `test_does_not_mutate_existing_argument`)

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/video_editor.py` | Modified | Added `INTRO_WINDOW_SECONDS = (0.0, 5.0)` constant and pure `resolve_overlay_slot()` helper (half-open `[start, end)`, clamps to `max(requested_start, 0.0)`, sweeps sorted `existing`); added both to the module's Public API docstring index |
| `tests/congress_videos/modules/test_video_editor.py` | Modified | Added `TestResolveOverlaySlot` (T-16, 8 tests) and `TestIntroWindowConstant` (T-17, 2 tests); updated module docstring's test-group index |
| `openspec/changes/session-intro-card-overlay/tasks.md` | Modified | Marked tasks 2.1–2.8 `[x]` |

### Deviations from Design
None — implementation matches design D2 exactly (signature, half-open interval semantics, clamp-then-sweep algorithm, purity). Task 2.7's "caller with no custom window" scenario is verified at the pure-function level (`resolve_overlay_slot` composed with `INTRO_WINDOW_SECONDS` and an empty `existing` list) because the real caller (`_apply_intro_overlay`, t5b) is explicitly out of scope until PR 4 per the tasks.md work-unit boundary — `resolve_overlay_slot` and `INTRO_WINDOW_SECONDS` remain unused/unwired production code until then, which is expected and correct for this slice.

### Issues Found
None.

## Work Unit Evidence (PR 2)

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k resolve_overlay_slot` → **1 passed** (matches the DoD command's literal `-k` substring; full coverage of both new test classes verified separately via `-k "TestResolveOverlaySlot or TestIntroWindowConstant"` → **10 passed**) |
| File-scoped regression command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -q --no-cov` → **129 passed** (119 pre-existing + 10 new, 0 regressions) |
| Full-suite command and exact result | `uv run pytest` → **5166 passed, 34 skipped**, exit code 0 |
| Lint / format | `uv run ruff check .` → All checks passed. `uv run ruff format --check .` → 313 files already formatted (after one `ruff format` auto-fix on the new test file's line-length) |
| Runtime harness command/scenario and exact result | N/A — pure function, no I/O, no DAG/ffmpeg/DB boundary touched in this slice, per the tasks.md work-unit table |
| Rollback boundary | Revert the `INTRO_WINDOW_SECONDS` constant, `resolve_overlay_slot` function, and their tests in the 2 files above. Nothing in the repo calls `resolve_overlay_slot` yet — inert until PR 4 wires `_apply_intro_overlay` |

## Workload / PR Boundary
- Mode: chained PR slice (`auto-chain`, `feature-branch-chain`)
- Current work unit: PR 2 of 4 (`resolve_overlay_slot` + default-window constant)
- Boundary: starts from PR 1's merged branch state (`intro_sesion` tipo/renderer already registered) and ends with `resolve_overlay_slot` + `INTRO_WINDOW_SECONDS` fully implemented and tested — no caller wires it yet
- Changed lines (`git diff --stat` on the 2 changed source/test files, authored, additions+deletions): **151 insertions + 0 deletions = 151**, vs. the ~120 estimate in tasks.md — within the 400-line budget, no `size:exception` needed
- Out of scope (confirmed untouched): `max_timeout` kwarg, duration guard, t5b task, DAG wiring, `youtube_upload_dag.py`, all reap DAGs, `speaker_turn_videos_dag.py`, DB schema

## Batch 3 — PR 3: `max_timeout` kwarg + duration guard

**Mode**: Strict TDD
**Base branch**: PR 2 branch `feat/558-slice2-overlap-helper`
**Working branch**: `feat/558-slice3-timeout-guard`

### Completed Tasks

- [x] 3.1 RED: `apply_overlays(..., max_timeout=None)` preserves today's `compute_ffmpeg_timeout(duration)` behavior (backward compat for `generic_video_editor`)
- [x] 3.2 RED: `apply_overlays(..., max_timeout=N)` uses `N` instead of the capped `compute_ffmpeg_timeout` value
- [x] 3.3 GREEN: added keyword-only `max_timeout: int | None = None` to `apply_overlays` in `video_editor.py`
- [x] 3.4 RED: source duration > `MAX_OVERLAY_SOURCE_SECONDS` raises `ValueError` naming duration, limit and constant, before ffmpeg spawns (subprocess patched, asserted never called)
- [x] 3.5 GREEN: added `MAX_OVERLAY_SOURCE_SECONDS = 3600` and `OVERLAY_MAX_TIMEOUT_SECONDS = 5400` module constants in `video_editor.py`; guard check runs before ffmpeg invocation
- [x] 3.6 RED→GREEN: duration at/under the guard succeeds and an explicit `max_timeout=OVERLAY_MAX_TIMEOUT_SECONDS` is honored
- [x] 3.7 REFACTOR: guard message includes duration, limit and constant name for diagnosability (verified by a dedicated assertion test — no code change needed beyond 3.5's message, which already satisfied this)

### TDD Cycle Evidence

| Task | Test File | Layer | Safety Net | RED | GREEN | TRIANGULATE | REFACTOR |
|------|-----------|-------|------------|-----|-------|-------------|----------|
| 3.1–3.3 | `tests/congress_videos/modules/test_video_editor.py::TestApplyOverlaysMaxTimeout` | Unit | ✅ 129/129 (PR 2 baseline) | ✅ Written first — `test_max_timeout_none_preserves_compute_ffmpeg_timeout_behavior` and `test_max_timeout_explicit_overrides_capped_value` failed with `TypeError: apply_overlays() got an unexpected keyword argument 'max_timeout'` | ✅ Passed after adding the keyword-only `max_timeout` param and the `max_timeout if max_timeout is not None else compute_ffmpeg_timeout(...)` branch | ✅ 4 cases: `max_timeout=None` explicit, omitted entirely, explicit override, and keyword-only enforcement (`TypeError` on positional call) | ➖ None needed — branch is already minimal |
| 3.4–3.5 | `TestOverlayDurationGuard` | Unit | ✅ (same run) | ✅ Written first — `ImportError: cannot import name 'MAX_OVERLAY_SOURCE_SECONDS'`; once patched around, the over-guard test hit a **real ffmpeg subprocess failure** (`RuntimeError`, not `ValueError`) proving the guard did not yet exist and did not stop ffmpeg from spawning | ✅ Passed after adding both constants and the pre-ffmpeg duration check | ✅ 4 cases: duration over the guard raises + subprocess never called, message contains duration/limit/constant name, duration exactly at the guard limit succeeds, duration under the guard with explicit `max_timeout=OVERLAY_MAX_TIMEOUT_SECONDS` honored | ➖ None needed |
| 3.6 | same class, `test_duration_under_guard_with_explicit_max_timeout_is_honored` | Unit | ✅ | ✅ Written first (same batch as 3.4/3.5 — depends on both `OVERLAY_MAX_TIMEOUT_SECONDS` and the guard existing) | ✅ Passed — asserts `subprocess.run` receives `timeout=5400` via `call_args.kwargs["timeout"]` | ➖ Single scenario per spec | ➖ None needed |
| 3.7 | `test_guard_message_names_duration_limit_and_constant` | Unit | ✅ | ✅ Written first | ✅ Passed — asserts `"7200"`, `"3600"`, and `"MAX_OVERLAY_SOURCE_SECONDS"` are all present in `str(exc_info.value)` | ➖ Single — one message format | ➖ None needed — the 3.5 implementation's message already satisfied 3.7's requirement, confirmed by this dedicated assertion test rather than a separate code change |

### Test Summary
- **Total tests written**: 8 (`TestApplyOverlaysMaxTimeout`: 4, `TestOverlayDurationGuard`: 4)
- **Total tests passing**: 8/8 new, 137/137 in `test_video_editor.py`, 5174/5174 in the full repo suite (same 34 pre-existing skips as PR 1/PR 2)
- **Layers used**: Unit (8)
- **Pure functions created**: None new — `apply_overlays` gained a guard branch and a keyword-only override; both are still deterministic given `_get_source_duration`'s mocked/probed return value

### Files Changed
| File | Action | What Was Done |
|------|--------|---------------|
| `congress_videos/modules/video_editor.py` | Modified | Added `MAX_OVERLAY_SOURCE_SECONDS = 3600` and `OVERLAY_MAX_TIMEOUT_SECONDS = 5400` module constants; added keyword-only `max_timeout: int \| None = None` to `apply_overlays`; added the pre-ffmpeg duration guard (`ValueError` naming duration, limit, constant name); updated the module's Public API docstring index and `apply_overlays`' own docstring |
| `tests/congress_videos/modules/test_video_editor.py` | Modified | Added `TestApplyOverlaysMaxTimeout` (T-18, 4 tests) and `TestOverlayDurationGuard` (T-19, 4 tests); updated module docstring's test-group index |
| `openspec/changes/session-intro-card-overlay/tasks.md` | Modified | Marked tasks 3.1–3.7 `[x]` |

### Deviations from Design
None — implementation matches design D1 exactly: guard constant `MAX_OVERLAY_SOURCE_SECONDS = 3600`, override constant `OVERLAY_MAX_TIMEOUT_SECONDS = 5400`, keyword-only `max_timeout` defaulting to `None` (today's `compute_ffmpeg_timeout` behavior), guard raises before any ffmpeg subprocess spawns. `OVERLAY_MAX_TIMEOUT_SECONDS` remains an inert named constant until PR 4 wires `_apply_intro_overlay` to pass it explicitly — expected per the tasks.md work-unit boundary.

### Issues Found
None.

## Work Unit Evidence (PR 3)

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k "timeout or guard"` → **9 passed** (matches the DoD command exactly; the 9th is a PR 2 test, `test_compute_ffmpeg_timeout_called_with_duration`, whose name also matches `timeout` — no regression, still green) |
| File-scoped regression command and exact result | `uv run pytest tests/congress_videos/modules/test_video_editor.py -q --no-cov` → **137 passed** (129 pre-existing + 8 new, 0 regressions) |
| Full-suite command and exact result | `uv run pytest` → **5174 passed, 34 skipped**, exit code 0 |
| Lint / format | `uv run ruff check .` → All checks passed. `uv run ruff format --check .` → flagged 2 files (long test method signatures); `uv run ruff format .` applied the fix, re-run of both commands is clean |
| Runtime harness command/scenario and exact result | N/A — timeout arithmetic and a pre-flight guard only; ffmpeg subprocess is patched in every test per the tasks.md work-unit table, no DAG/DB/Airflow runtime boundary touched in this slice |
| Rollback boundary | Revert the `max_timeout` kwarg, the two new constants, the guard branch, and their tests in the 2 files above. `max_timeout=None` keeps `apply_overlays`'s default behavior byte-identical for `generic_video_editor` and every other existing caller; `OVERLAY_MAX_TIMEOUT_SECONDS` is unused/unwired until PR 4 |

## Workload / PR Boundary
- Mode: chained PR slice (`auto-chain`, `feature-branch-chain`)
- Current work unit: PR 3 of 4 (`max_timeout` kwarg + duration guard)
- Boundary: starts from PR 2's branch state (`resolve_overlay_slot` + `INTRO_WINDOW_SECONDS` already in place) and ends with `apply_overlays` accepting an optional `max_timeout` override and refusing oversized sources before ffmpeg spawns — no caller passes `max_timeout` yet
- Changed lines (`git diff --stat` on the 2 changed source/test files, authored, additions+deletions): **274 insertions + 9 deletions = 283**, vs. the ~95 estimate in tasks.md — within the 400-line budget, no `size:exception` needed (the estimate undercounted the docstring updates and the 4-case triangulation per RED test required by Strict TDD's minimum-2-cases rule)
- Out of scope (confirmed untouched): t5b task, DAG wiring, `youtube_upload_dag.py`, all reap DAGs, `speaker_turn_videos_dag.py`, DB schema

## Remaining Tasks (PR 4 — NOT started, out of scope for this batch)
- [ ] 4.1–4.19 t5b task, wiring, DAG task-count fixes

### Status
22/41 tasks complete (PR 1 + PR 2 + PR 3 fully done). Ready for verify on PR 3's scope; PR 4 is the next apply batch, based on this branch per `feature-branch-chain`.
