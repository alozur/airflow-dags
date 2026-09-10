# Tasks: Session Intro-Card Overlay for Long-Form Uploads

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~555 total (4 slices) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 (~140) → PR 2 (~120) → PR 3 (~95) → PR 4 (~200) |
| Delivery strategy | auto-chain |
| Chain strategy | feature-branch-chain |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: feature-branch-chain
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | Base | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|---|---|---|---|---|---|---|
| 1 | `intro_sesion` tipo + `_render_intro_sesion` renderer registered | tracker `feat/558-session-intro-card` | PR 1 | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k intro_sesion` | N/A — pure Pillow/config unit, no DAG/ffmpeg | Revert config entry + renderer + registration; no other file depends on it yet |
| 2 | `resolve_overlay_slot` pure helper + default-window constant | PR 1 branch | PR 2 | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k resolve_overlay_slot` | N/A — pure function, no I/O | Revert function + constant; unused until PR 4 wires it |
| 3 | `max_timeout` kwarg + duration guard constants | PR 2 branch | PR 3 | `uv run pytest tests/congress_videos/modules/test_video_editor.py -k "timeout or guard"` | N/A — timeout arithmetic only, ffmpeg subprocess patched | Revert kwarg/guard; `max_timeout=None` default keeps `generic_video_editor` behavior untouched |
| 4 | t5b task, wiring, DAG task-count/id fixes | PR 3 branch | PR 4 | `uv run pytest tests/congress_videos/test_youtube_upload_dag.py -k intro_overlay` | `bash scripts/test-airflow-e2e.sh` (16 tasks, clean DagBag import) | Revert t5b task + wiring line + count/id assertions; t6/t7 unaffected |

## Traceability

| Requirement / Scenario | Task(s) |
|---|---|
| Intro Card Style Definition — schema match | 1.1, 1.2 |
| Renderer Registration — unregistered fails loudly | 1.3 |
| Renderer Registration — registered produces card | 1.4, 1.5, 1.6 |
| Overlap Helper — no-overlap unchanged / single-overlay no-op | 2.1 |
| Overlap Helper — full overlap shifts past conflict | 2.2 |
| Overlap Helper — partial overlap shifts to earliest slot | 2.3 |
| Overlap Helper — never backward / never < 0 | 2.4 |
| Default Intro Window — applies without override | 2.6, 2.7 |
| Card Text From Session Metadata | 4.4, 4.5 |
| In-Process Overwrite — t6 reads overlaid path transparently | 4.9, 4.15, 4.16, 4.17 |
| DB Output Path Never Written Back | 4.11 |
| Source File Immutability | 4.12 |
| Edited File Same-Directory Sibling | 4.14 |
| Idempotent Retry | 4.13 |
| Bounded Fail-Loud — overlay failure aborts publication | 4.7 |
| Bounded Fail-Loud — excessive duration fails explicitly | 3.4, 3.5, 4.7 |

## PR 1: `intro_sesion` tipo + renderer + registration (base: tracker branch, ~140 lines)

- [x] 1.1 RED: `tests/congress_videos/modules/test_video_editor.py` — `intro_sesion` in `congreso` tipos declares the same keys as `extracto_sesion` (`renderer`, `fontfile`, `fontfile_sub`, `fontsize_title/_sub`, `bg_color`, `accent_color`, `title_color`, `sub_color`, `width_pct`, `height`, `margin_y`), no per-call timing state.
- [x] 1.2 GREEN: add `intro_sesion` entry (centered variant) to `congress_videos/config/video_editor_config.py` `congreso` domain.
- [x] 1.3 RED: `apply_overlays`/dispatch raises an explicit error when `intro_sesion` has no registered renderer — never a silent/partial overlay.
- [x] 1.4 GREEN: implement `_render_intro_sesion(overlay, style, W, H)` in `congress_videos/modules/video_editor.py` using `textbbox()`/`textlength()` (never `textsize()` — Pillow 12.3.0), matching `_draw_text_block` idiom of the 5 existing renderers.
- [x] 1.5 GREEN: register `"intro_sesion": _render_intro_sesion` in `_PILLOW_RENDERERS`.
- [x] 1.6 GREEN: test registered renderer composites the card within its time window (`apply_overlays` with ffmpeg subprocess patched, render call asserted).
- [x] 1.7 REFACTOR: check for duplication vs `_render_extracto_sesion`; extract only if trivial.

## PR 2: `resolve_overlay_slot` + default-window constant (base: PR 1 branch, ~120 lines)

- [x] 2.1 RED: `resolve_overlay_slot(existing=[], start, dur)` returns the window unchanged (no-overlap / single-overlay no-op).
- [x] 2.2 RED: full overlap `existing=[(0,10)]` shifts requested window to start at 10, same duration.
- [x] 2.3 RED: partial overlap shifts to the earliest free slot at/after the original start, same duration.
- [x] 2.4 RED: never shifts backward, never starts before `0.0`.
- [x] 2.5 GREEN: implement pure `resolve_overlay_slot(existing: list[tuple[float, float]], requested_start: float, requested_duration: float) -> tuple[float, float]` in `video_editor.py` — half-open `[start, end)`, clamp start to `max(requested_start, 0.0)`, sweep sorted `existing`.
- [x] 2.6 GREEN: add named default-window constant (`INTRO_WINDOW_SECONDS = (0.0, 5.0)`) in `video_editor.py`.
- [x] 2.7 RED→GREEN: caller with no custom window occupies exactly `[0, 5)`, read from the constant.
- [x] 2.8 REFACTOR: docstring on `resolve_overlay_slot` stating the never-backward/never-negative/duration-preserving invariants.

## PR 3: `max_timeout` kwarg + duration guard (base: PR 2 branch, ~95 lines)

- [x] 3.1 RED: `apply_overlays(..., max_timeout=None)` preserves today's `compute_ffmpeg_timeout(duration)` behavior (backward compat for `generic_video_editor`).
- [x] 3.2 RED: `apply_overlays(..., max_timeout=N)` uses `N` instead of the capped `compute_ffmpeg_timeout` value.
- [x] 3.3 GREEN: add keyword-only `max_timeout: int | None = None` to `apply_overlays` in `video_editor.py`.
- [x] 3.4 RED: source duration > `MAX_OVERLAY_SOURCE_SECONDS` raises `ValueError` naming duration, limit and constant, before ffmpeg spawns (subprocess patched, asserted never called).
- [x] 3.5 GREEN: add `MAX_OVERLAY_SOURCE_SECONDS = 3600` and `OVERLAY_MAX_TIMEOUT_SECONDS = 5400` in `video_editor.py`; guard check runs before ffmpeg invocation.
- [x] 3.6 RED→GREEN: duration at/under the guard succeeds and the intro call site's explicit `max_timeout=OVERLAY_MAX_TIMEOUT_SECONDS` is honored.
- [x] 3.7 REFACTOR: guard message includes duration, limit and constant name for diagnosability.

## PR 4: t5b task, wiring, DAG task-count fixes (base: PR 3 branch, ~200 lines)

- [x] 4.1 RED: `tests/congress_videos/test_youtube_upload_dag.py:50` `test_dag_has_fifteen_tasks` → 15→16.
- [x] 4.2 RED: `tests/congress_videos/test_youtube_upload_dag.py:2584` `test_dag_task_count_updated_for_wired_dual_queue` → 15→16, update docstring/message.
- [x] 4.3 RED: `test_expected_task_ids_present` → add `apply_intro_overlay` to the expected task-id list.
- [x] 4.4 RED: `_build_intro_card_text(session_number, session_date)` builds Spanish `titulo`/`descripcion` (`titulo = f"Sesión {n}"` else date; `descripcion = str(session_date)`; both absent raises), per D6 precedent at L398.
- [x] 4.5 GREEN: implement `_build_intro_card_text` in `congress_videos/youtube_upload_dag.py`.
- [x] 4.6 RED: `_apply_intro_overlay` pass-through — missing/failed/empty `chapter_extraction_results` logs and leaves the XCom untouched.
- [x] 4.7 RED: `_apply_intro_overlay` fail-loud — guard trip, missing font, ffmpeg non-zero, missing output, absent session fields all raise; XCom untouched; downstream tasks do not run.
- [x] 4.8 RED: `_apply_intro_overlay` calls `validate_editor_input(conf)` on the built conf BEFORE `apply_overlays` (mock call-order assertion) — a missing font must raise `FileNotFoundError` naming tipo/key/path before ffmpeg spawns.
- [x] 4.9 RED: `_apply_intro_overlay` overwrites `chapter_extraction_results[0]["output_path"]` to the `_edited` sibling path in-memory only, records `original_output_path`, imports no database module, calls no `db.*` write.
- [x] 4.10 RED (regression, pins the landmine): `turn_config["turn_id"]` is always set by t6, so `upload_marking.py`'s `mark_turns_uploaded_by_output_path` fallback (`WHERE output_path = %s`) is never exercised for turn uploads even with an `_edited` path — assert `turn_id` is present in the `mark_turn_uploads` call for the turn branch.
- [x] 4.11 RED: `speaker_turn_videos.output_path` is never updated by `_apply_intro_overlay` — no DB write issued (DB invariant).
- [x] 4.12 RED: source file bytes/path unchanged after `_apply_intro_overlay` runs (source immutability).
- [x] 4.13 RED: retrying `_apply_intro_overlay` in the same run writes the same deterministic `_edited` path, no accumulation.
- [x] 4.14 RED: `_edited` file lands in `os.path.dirname(source)` so the 4 sidecars still resolve for `prepare_orador_upload_config`.
- [x] 4.15 GREEN: implement `_apply_intro_overlay(ti)` in `congress_videos/youtube_upload_dag.py` — read `chapter_extraction_results`, build the `intro_sesion` overlay conf (default `[0,5)` window, `_build_intro_card_text`), call `validate_editor_input(conf)`, call `apply_overlays(..., max_timeout=OVERLAY_MAX_TIMEOUT_SECONDS)`, overwrite `output_path` in-memory, push XCom.
- [x] 4.16 GREEN: wire `PythonOperator(task_id="apply_intro_overlay")` (t5b) between t5 (`extract_chapter_videos`) and t6 (`prepare_upload_config`) in the dependency chain.
- [x] 4.17 REFACTOR: confirm t6 needs zero code changes (reads the overwritten XCom transparently).
- [x] 4.18 Verification: `bash scripts/test-airflow-e2e.sh` — 16 tasks, clean DagBag import.
- [x] 4.19 Verification: `git diff --stat` confirms `reap_clip_preparer_dag.py`, `reap_processor_dag.py`, `reap_shorts_uploader_dag.py`, `speaker_turn_videos_dag.py`, and DB schema are untouched.
