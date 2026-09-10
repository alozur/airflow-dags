# Exploration: Session intro-card overlay burn-in for long-form YouTube uploads

Issue: #558 — `feat(video-editor): burn session intro-card overlay into long-form videos before YouTube upload`
Date: 2026-09-10
Phase: explore (read-only; grounded against the worktree at `feat/558-session-intro-card` off `main` @ 96b4d8f)

## Current State

### `congress_videos/config/video_editor_config.py`

The `congreso` domain currently has 5 tipos, all Pillow-rendered (transparent PNG composited via
ffmpeg `overlay`, not ffmpeg `drawtext`): `extracto_sesion`, `speaker_id`, `cita_destacada`,
`urgente`, `dato_contexto`.

Common schema across all of them:

- `renderer: "pillow"`
- `fontfile` / `fontfile_sub` — always `FONT_BOLD` / `FONT_REGULAR` from `config/paths.py`
- `fontsize_title` / `fontsize_sub`
- `bg_color` / `accent_color` / `title_color` / `sub_color` as RGBA tuples
- layout keys (`width_pct`, `height`, `margin_y` / `margin_x`, or `padding` / `width` depending on shape)

A new `intro_sesion` tipo follows the same shape. Closest analog is `extracto_sesion` (bottom-centered bar).

**The config dict alone is not enough**: a matching `_render_intro_sesion` function must also be
registered in `_PILLOW_RENDERERS` in `congress_videos/modules/video_editor.py`. Issue #558 does not
mention this second half.

### `congress_videos/modules/video_editor.py`

> Path correction: the issue cites `congress_videos/video_editor.py`, which does not exist. The real
> module lives under `congress_videos/modules/`.

- `apply_overlays(source_path, output_path, overlays, domain_cfg)` always runs **one ffmpeg
  invocation over the entire source file** (`-i src` once, `-vf` / `-filter_complex` once,
  `libx264 -preset veryfast -crf 20`, `-c:a copy`). Cost scales with **total video length**, not with
  the overlay's own time window.
- **Source is never mutated** — confirmed. `_default_output_path()` appends `_edited` before the
  extension, in the same directory. ffmpeg always writes to that new path via `-y`.
- `validate_editor_input` / `_validate_overlay` (real lines ~613-697, not 613-668 as the issue says)
  check only: required top-level keys, source XOR, per-overlay required keys, time ordering
  (`tiempo_fin > tiempo_inicio`), tipo existence, font-file existence on disk.
  **Zero collision/overlap detection** — this confirms the gap the issue describes.
- `apply_overlays` reuses `compute_ffmpeg_timeout` from `video_splitter.py`:
  `base = 120s + factor 8.0 x duration`, **hard-capped at `max_timeout = 3600s`**.

### `congress_videos/youtube_upload_dag.py`

Real chain: `t5 extract_chapter_videos -> t6 prepare_upload_config -> t6b verify_final_copy ->
t7 trigger_youtube_upload` (15 tasks total). The issue's "between t5 and t6" placement is correct.

For the turn path (the only path reachable in production today):

- `t5` pushes the `chapter_extraction_results` XCom, with `results[0]["output_path"]` = the
  pre-materialized turn video (no ffmpeg call on this branch).
- `t6`'s turn branch reads that `output_path`, writes fresh `title.txt` / `description.txt` via
  `_write_orador_sidecars(output_path, ...)`, then calls `prepare_orador_upload_config(output_path=...)`,
  which sets `video_file = output_path` verbatim and hard-requires 4 sidecars (`title.txt`,
  `description.txt`, `thumbnail.png`, `subtitles.srt`) already present in `os.path.dirname(output_path)`.
- **Cleanest insertion point**: a new task between t5 and t6 that reads `chapter_extraction_results`,
  runs `apply_overlays()` to produce the `_edited` sibling file (same directory, so sidecars still
  resolve), and **overwrites the `output_path` field in that same XCom key** before t6 reads it.
  This needs zero code change inside t6.
- `session_number` / `session_date` are confirmed present on the turn's `uploadable_item` row — but
  via the SQL view definitions (`production_schema.sql` ~606-634 for `uploadable_turns`, ~475-512 for
  `uploadable_chapters`), **not** at the `database.py:1738-1752` / `1270-1292` line numbers the issue
  cites (those lines are unrelated `record_copy_verification_short` / `record_title_generation_input_turn`
  code). The substantive claim holds; the citation is wrong.

> Precedent correction: the issue justifies the in-process call by saying it matches "the pattern this
> DAG already uses for thumbnail generation". That is backwards — `trigger_thumbnail_generation` uses
> `trigger_dag_api` to run `generic_thumbnail_generator` as a **child DAG** and polls for completion.
> The real in-process precedent in this same DAG is `_extract_chapter_videos`'s chapter branch, which
> calls `video_splitter.extract_chapters_from_video()` directly.

### Reap-pipeline non-impact

The issue's citation is wrong but its conclusion holds, for a cleaner reason.

`reap_clip_preparer_dag.py` has **no** `_find_source_video()` function at all (zero grep matches). The
function the issue describes (`_find_source_video_any_date`, excluding the `chapter_video` substring,
scanning `DOWNLOADS_DIR/{date}/{video_id}/`) actually lives in `speaker_turn_videos_dag.py`, an
unrelated materialization-time DAG.

`reap_clip_preparer_dag.py` itself (`_stage_and_pretrim_clip`) reads `output_path` **directly as a DB
column value** from `db.get_turn_videos_for_shorts()` — no directory scan, no filename convention
involved at all.

So non-impact is real, but the actual invariant to preserve is:

> **The new overlay task must never write back to `speaker_turn_videos.output_path` in the DB.** It may
> only produce a sibling file and mutate the in-memory XCom for this run.

As long as that invariant holds, reap is structurally decoupled regardless of naming.

### Test layout

- `tests/congress_videos/modules/test_video_editor.py` — where the new tipo/renderer tests and the
  overlap-resolution helper tests belong.
- `tests/congress_videos/test_youtube_upload_dag.py` — has a `_make_ti()` XCom-store test double, plus
  `test_dag_has_fifteen_tasks` and `test_expected_task_ids_present`, both of which are hard assertions
  that **will fail immediately** once the new task is added (15 -> 16).

## Affected Areas

| Path | Change |
| --- | --- |
| `congress_videos/config/video_editor_config.py` | add `intro_sesion` tipo entry |
| `congress_videos/modules/video_editor.py` | add `_render_intro_sesion` + register in `_PILLOW_RENDERERS`; add pure overlap-resolution helper |
| `congress_videos/youtube_upload_dag.py` | new task between t5/t6; task count 15 -> 16 |
| `tests/congress_videos/modules/test_video_editor.py` | new tipo/renderer tests + overlap-helper unit tests |
| `tests/congress_videos/test_youtube_upload_dag.py` | task-count/task-id assertions; new task XCom coverage |

Verified **not** affected: `reap_clip_preparer_dag.py`, `reap_processor_dag.py`,
`reap_shorts_uploader_dag.py`, `speaker_turn_videos_dag.py` — none read from the new `_edited` path.

## Approaches

### 1. In-process `apply_overlays()` call in a new DAG task (the issue's proposal)

Matches the existing in-process chapter-extraction precedent (not the thumbnail one).

- **Pros**: no extra DAG-trigger latency/polling loop; simplest XCom threading (overwrite
  `chapter_extraction_results.output_path` in place); failures surface directly on the task.
- **Cons**: couples the daily upload DAG's runtime to a full-video re-encode; a slow or stuck ffmpeg
  blocks the single daily upload slot instead of being isolated in its own DAG run.
- **Effort**: Low-Medium.

### 2. Trigger `generic_video_editor` via `trigger_dag_api` (mirroring the actual thumbnail pattern)

- **Pros**: isolates the re-encode's resource/timeout blast radius from the upload DAG; reuses the
  existing standalone DAG unchanged.
- **Cons**: more moving parts (child-DAG polling, cross-DAG XCom retrieval), mirrors the fragile
  thumbnail-wait pattern already present; the issue explicitly scopes the standalone DAG to
  manual/ad-hoc use only.
- **Effort**: Medium.

## Recommendation

**Approach 1 (in-process)**, as the issue proposes — simpler, with a real precedent in the same DAG.

The **timeout ceiling is the risk to resolve before implementation**, not the call pattern.
`compute_ffmpeg_timeout`'s 3600s hard cap, combined with no documented or enforced upper bound on
long-form turn/grouped-turn video duration, is a real failure mode on the I/O-contended NAS. Design
must address it as a first-class decision.

## Risks

1. **ffmpeg timeout on long videos (biggest)** — `apply_overlays` reuses the 3600s-capped
   `compute_ffmpeg_timeout`; no code-level upper bound exists on the video length it will be asked to
   re-encode. Must be resolved in design before apply.
2. **Disk growth on the NAS** — the `_edited` file is a full permanent re-encoded copy kept forever
   alongside the untouched original. Every long-form upload now roughly doubles stored bytes for that
   turn, indefinitely. No cleanup mechanism for `_edited` outputs exists anywhere in this codebase.
3. **Font/runtime availability unverified** — `FONT_BOLD` / `FONT_REGULAR` are the same fonts the 5
   existing tipos reference, but `generic_video_editor` is "purely on-demand", meaning this code path
   may never have actually run inside the production Airflow container. Previously an optional side
   path with no blast radius; now it sits in the mandatory daily upload path. Spot-check, don't assume.
4. **Test-count coupling** — `test_dag_has_fifteen_tasks` / `test_expected_task_ids_present` will fail
   immediately. Trivial to fix, must not be forgotten in `tasks.md`.
5. **Idempotency** — verified safe. `apply_overlays` always writes via `ffmpeg -y`, so retries
   overwrite the same deterministic `_edited` path; the `uploadable_item` XCom is stable across retries
   within one DAG run.
6. **Citation drift in the issue** — several concrete file/line references in #558 are stale or wrong
   (module path, `_find_source_video` location/file, `database.py` line numbers, thumbnail in-process
   claim). Conclusions mostly still hold, but later phases must re-derive rationale from the verified
   code paths above, not quote the issue body verbatim.

## Ready for Proposal

Yes, with one explicit condition: `sdd-propose` must address the ffmpeg-timeout risk on long-form
videos as a first-class design decision, since it is the one risk that could make this feature
intermittently fail in production on the NAS.
