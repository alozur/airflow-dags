# Proposal: Session intro-card overlay for long-form uploads

## Intent

**Problem**: long-form videos published by `youtube_upload_dag` carry no on-screen identification of the
plenary session they came from, so viewers cannot tell which session or date a clip belongs to. The
`generic_video_editor` machinery to burn such a card already exists, but is on-demand only and the
mandatory daily upload path never invokes it.

**Success**: every long-form upload ships a 5s session card burned in, produced on the upload path,
with no silent skip and no stalled publication.

## Scope

### In Scope
- `intro_sesion` tipo in `video_editor_config.py` (**style only**, like the 5 `congreso` tipos).
- `_render_intro_sesion` registered in `_PILLOW_RENDERERS`.
- New task between `t5 extract_chapter_videos` and `t6 prepare_upload_config` calling `apply_overlays()`
  in-process and overwriting `output_path` on the `chapter_extraction_results` XCom (t6 unchanged).
- Card text from caller-supplied `session_number` / `session_date` on `uploadable_item`.
- 5s window `[0, 5)` as a named constant.
- A bounded, fail-loud resolution to the timeout ceiling below.

### Out of Scope
- Disk growth from permanent `_edited` copies → follow-up GitHub issue at archive time.
- DB schema changes; reap DAGs; the standalone `generic_video_editor` DAG.

## Capabilities

### New Capabilities
- `session-intro-card-overlay`: burning a session-identifying card into long-form video before upload.

### Modified Capabilities
- None.

## Approach

In-process `apply_overlays()`, mirroring `_extract_chapter_videos`'s existing in-process call to
`video_splitter.extract_chapters_from_video()` (**not** the child-DAG thumbnail trigger). Sidecars still
resolve because `_edited` lands in the same directory.

**Invariant**: never write the `_edited` path back to `speaker_turn_videos.output_path`. That, not any
filename convention, is what keeps the reap pipeline decoupled.

## Blocker for design to resolve

`apply_overlays` reuses `compute_ffmpeg_timeout` (`120 + 8.0 x duration`, `max_timeout=3600`). The cap
binds at **435s (~7m15s)** of source, so every long-form video receives a *constant* 3600s budget while
the full-video `libx264 -preset veryfast -crf 20` re-encode cost grows with length. On the I/O-contended
NAS a long turn can plausibly be killed mid-flight, intermittently breaking the daily upload.

| Option | Pro | Con |
|---|---|---|
| (a) Duration-derived timeout, this call site only | `max_timeout` is already a kwarg | Cost stays O(length); only moves the cliff |
| (b) Segment-and-concat: re-encode first N s, stream-copy the rest | Cost becomes O(intro) | Keyframe alignment and A/V sync risk at the boundary |
| (c) Hard max-source-duration guard, fail loudly | Never publishes un-overlaid | Blocks publication instead of solving it |

**Strongest: (b)** — it removes the failure mode instead of deferring it, with (a) or (c) as a net.
Design owns the decision, with evidence.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/config/video_editor_config.py` | Modified | `intro_sesion` tipo |
| `congress_videos/modules/video_editor.py` | Modified | Renderer + registration; segment strategy |
| `congress_videos/youtube_upload_dag.py` | Modified | New task; 15 → 16 |
| `tests/congress_videos/modules/test_video_editor.py` | Modified | Renderer and timing tests |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modified | Task-count / task-id asserts break |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Timeout kills the daily upload | High | Blocker decision above; bounded and fail-loud |
| A/V desync at concat boundary | Med | Keyframe-aligned split; assert output duration |
| Fonts missing in the prod container | Med | Spot-check `FONT_BOLD` / `FONT_REGULAR` before apply |
| Disk doubling per upload | High | Accepted tradeoff; follow-up issue |
| Task-count assertions break | High | Explicit task in `tasks.md` |

## Rollback Plan

Revert the `youtube_upload_dag.py` task and its wiring; `git_sync` redeploys. Tipo and renderer are
inert without a caller and may stay. No DB state to unwind; stray `_edited` files are harmless.

## Dependencies

- ffmpeg + `libx264` in the Airflow image, and `session_number` / `session_date` on the
  `uploadable_turns` / `uploadable_chapters` views — both present.

## Success Criteria

- [ ] A long-form upload shows the session card for exactly 5s from t=0.
- [ ] The task fails loudly on error — never silently skips, never publishes un-overlaid.
- [ ] Worst-case realistic source duration completes inside the chosen timeout.
- [ ] `speaker_turn_videos.output_path` unchanged in the DB after a run.
- [ ] `uv run pytest` green; DAG exposes 16 tasks, no import errors.
