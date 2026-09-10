# Design: Session intro-card overlay for long-form uploads

## Technical Approach

A new task `apply_intro_overlay` (t5b) sits between t5 and t6. It builds a one-overlay conf, runs the
existing `validate_editor_input`, calls `apply_overlays()` in-process (the `_extract_chapter_videos`
precedent), and overwrites `output_path` on the `chapter_extraction_results` XCom. t6 is unchanged.
New `intro_sesion` tipo + `_render_intro_sesion` supply the card.

## Architecture Decisions

### D1 — Encode strategy (the escalated blocker)

**Choice: (a) full re-encode with a *proportional* timeout + (c) a loud pre-flight duration guard.**

| Option | Tradeoff | Verdict |
|---|---|---|
| (a) plain, keep 3600s cap | Zero new code, but budget is constant while cost is O(length) — headroom shrinks with duration | Partial |
| (b) segment-and-concat | O(intro) cost, but keyframe-alignment + A/V-sync risk at the boundary, and it forks or mutates `apply_overlays`, a shared API the on-demand DAG also calls | **Rejected** |
| (c) hard max-duration guard | Turns a mid-flight kill into a diagnosable failure; does not by itself size the budget | Adopted as a net |

Rationale. Measured: 720p30 source, `libx264 -preset veryfast -crf 20` at **~3.5x realtime**; worst
production video **38.3 min** (p50 5.6, p95 18.8). At 3.5x that is ~11 min against 3600s — 5.5x headroom.
(b) buys a constant-factor win the numbers do not need, and pays for it with A/V-sync risk in the
**mandatory daily publication path**. Rejected.

But plain (a) leaves the structural mismatch the exploration named: `min(120 + 8.0*d, 3600)` is constant
past 435s while cost is O(d). The **cap** causes that, not the formula — uncapped, `120 + 8.0*d` is ~28x
the measured cost at every duration. So this call site passes an explicit `max_timeout`, restoring a
budget that scales with cost and absorbing a 1080p move (~1.75x realtime → still ~14x) without re-tuning.

Bounds, derived not guessed: guard `MAX_OVERLAY_SOURCE_SECONDS = 3600` (1.57x worst observed);
`OVERLAY_MAX_TIMEOUT_SECONDS = 5400` = 1.5x the guard, so even a pessimistic **1.0x realtime** encode of
the largest allowed source finishes inside budget. Guard trips → `ValueError` naming duration, limit and
constant, **before ffmpeg spawns**. `apply_overlays` gains keyword-only `max_timeout: int | None = None`
defaulting to today's behavior — backward compatible for `generic_video_editor`.

### D2 — Overlap resolution

```python
def resolve_overlay_slot(
    existing: list[tuple[float, float]], requested_start: float, requested_duration: float
) -> tuple[float, float]:
```
Pure. Half-open `[start, end)` — touching endpoints do not overlap. Clamps start to `max(requested_start,
0.0)`, sweeps sorted `existing`, advancing to each intersecting interval's end. **Never** moves backward,
**never** starts before 0, preserves `requested_duration` exactly, returns the earliest free slot.
Today `existing` is empty on this path; it exists because `_validate_overlay` has zero collision
detection. Isolated in its own slice so it can be dropped if judged unwarranted surface.

### D3 — Fail-loud vs. pass-through

t6 already treats *upstream* extraction failure as a benign skip. t5b mirrors that: no results / not
`success` / empty `output_path` → log + leave the XCom untouched. Every failure *caused by t5b* (guard
trip, missing font, ffmpeg non-zero, missing output, absent session fields) raises. Never a silent skip,
never an un-overlaid publication.

### D4 — DB invariant

t5b imports **no** database module and performs no write. It mutates only the XCom dict and additionally
records `original_output_path` for diagnosis. Verified landmine: `mark_turn_uploads` falls back to
`mark_turns_uploaded_by_output_path(video_file)` — `WHERE output_path = %s` would match **0 rows** on an
`_edited` path, publishing without marking and re-publishing tomorrow. The fallback is unreachable
because t6 always sets `turn_config["turn_id"]`; a regression test **pins** that, keeping it unreachable.

### D5 — Fonts

`_validate_overlay`'s check alone is **not** sufficient: `apply_overlays` never calls
`validate_editor_input`, and `_load_font` silently falls back to `ImageFont.load_default()`, degrading a
missing font into a garbage card reported as success. t5b therefore calls `validate_editor_input` on the
conf it builds — reusing the existing check, no parallel code. Failure surface: `FileNotFoundError`
naming tipo, key and path, before any encode. Fonts and Pillow 12.3.0 are confirmed present in
`airflow-scheduler-prod`; renderer must use `textbbox()` (reuse `_draw_text_block`), never `textsize()`.

## Data Flow

    t5 extract ──→ t5b apply_intro_overlay ──→ t6 prepare_upload_config
       │                │  validate → guard → apply_overlays          │
       │                ↓                                             ↓
       └── XCom chapter_extraction_results[0].output_path = <..._edited.mp4>
           (DB speaker_turn_videos.output_path UNTOUCHED)

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/config/video_editor_config.py` | Modify | `intro_sesion` tipo, `extracto_sesion` shape |
| `congress_videos/modules/video_editor.py` | Modify | `_render_intro_sesion` + registration; `resolve_overlay_slot`; 4 constants; `max_timeout` kwarg |
| `congress_videos/youtube_upload_dag.py` | Modify | `_build_intro_card_text`, `_apply_intro_overlay`, t5b, wiring 15→16 |
| `tests/congress_videos/modules/test_video_editor.py` | Modify | Renderer, helper, timeout/guard tests |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modify | 15→16 at **both** L50 and L2584; t5b coverage |

## Interfaces / Contracts

`intro_sesion` mirrors `extracto_sesion` keys (`renderer`, `fontfile`, `fontfile_sub`,
`fontsize_title/_sub`, `bg_color`, `accent_color`, `title_color`, `sub_color`, `width_pct`, `height`,
`margin_y`), centered rather than bottom-anchored. Card text reuses the D6 precedent at L398:
`titulo = f"Sesión {n}"` else the date; `descripcion = str(session_date)`. Both absent → raise.

## Testing Strategy

| Layer | What | How |
|---|---|---|
| Unit | Renderer, `resolve_overlay_slot` edges, guard, timeout arithmetic, card text | pytest; no ffmpeg |
| Integration | t5b XCom rewrite, pass-through, fail-loud, no DB import, turn_id pinned | `_make_ti()` double, `apply_overlays` patched |
| E2E | DAG imports, 16 tasks | `bash scripts/test-airflow-e2e.sh` |

## Threat Matrix

`N/A` — no routing, VCS/PR automation, or executable-file classification boundary. The subprocess
boundary is unchanged: argv lists, no shell; session text renders into a PNG and never enters an ffmpeg
filter expression.

## Migration / Rollout

No migration. Revert t5b + wiring; `git_sync` redeploys. Stray `_edited` files are inert.

## Delivery Slices (auto-chain, feature-branch-chain, 400-line budget)

Forecast ~555 lines → chained. Tracker `feat/558-session-intro-card`; each child targets the previous.

1. tipo + renderer + registration + tests (~140)
2. `resolve_overlay_slot` + constants + tests (~120)
3. `max_timeout` kwarg + duration guard + tests (~95)
4. t5b task, wiring, task-count fixes, DAG tests (~200)

## Open Questions

- None blocking.
