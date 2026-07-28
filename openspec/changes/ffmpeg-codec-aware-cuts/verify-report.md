# Verify Report: ffmpeg-codec-aware-cuts

Branch: `feat/ffmpeg-codec-aware-cuts` (not switched, no commits made during verify).
Inputs read: spec (engram obs 60 + openspec spec.md), design (engram obs 64), tasks (engram obs 66 + openspec tasks.md), apply-progress (engram obs 69 + openspec apply-progress.md). Full artifact set present — full verification performed (spec + design + tasks + implementation + tests + review workload).

## Overall status: CONDITIONAL PASS — implementation verified correct; archive blocked on 2 externally-deferred items + 4 parent-owned review gates

Independently confirmed (not taken on apply's self-report):

## 1. Spec requirement coverage vs. actual diff

| Requirement | Verified against source | Result |
|---|---|---|
| Codec Detection Function | Read `utils/codec_detection.py` in full. `detect_video_codec` wraps the entire `subprocess.run` call in try/except for `FileNotFoundError`, `subprocess.TimeoutExpired`, and generic `Exception`; also checks `returncode != 0` after the call. Every branch returns `"unknown"`, none re-raises. h264/avc1→h264, av1/av01→av1, else→unknown (vp9/hevc fold into the same unknown branch, not a new category). | PASS |
| Per-Source-Video Codec Cache | `get_cached_codec(source_path, cache)`: `cache is None` → probes directly, no memo; otherwise keyed by `os.path.abspath(source_path)`, populate-on-first-probe. Confirmed it is a **plain dict passed as a parameter**, not a module-level global — grep of `codec_detection.py` shows no module-level cache variable; `codec_cache: dict = {}` is created inside `extract_chapters_from_video`'s loop scope and inside `reap_clip_preparer_dag.py`'s DAG task scope, each a fresh local var. Satisfies "MUST NOT persist across DAG runs." | PASS |
| Codec-Based Reencode Decision Rule | `reencode_for_codec`: h264→True, else→False. Called before `build_ffmpeg_cut_cmd` in both sites (not a retry-after-failure pattern). | PASS |
| Codec-Aware Wiring at Both Cut Sites | `video_splitter.split_video_chapter` and `reap_clip_preparer_dag.py`'s `_extract_and_pretrim_clip` loop both call `get_cached_codec`/`reencode_for_codec` from the single `utils.codec_detection` module — no divergent second implementation found (grepped for `detect_video_codec` definitions: exactly one, in `utils/codec_detection.py`). `reap_shorts_uploader_dag.py` confirmed **zero diff** (`git diff --stat` and `git status --porcelain` both empty for that file) — untouched, out of scope respected. | PASS |
| Per-Cut Audit Fields | `split_video_chapter`'s result dict gains `source_codec`+`cut_mode` on the success path AND on both `except` blocks (source_codec initialized `"unknown"` before `try`, so a pre-probe failure still reports a value). Existing keys (`success`, `output_path`, `file_size_bytes`, etc.) all still present — additive only, confirmed via diff (no removed/renamed keys). Pre-trim site has no result dict (per spec) — audit surfaces via the extended log line `... srt_window=%s source_codec=%s cut_mode=%s`, confirmed in diff. | PASS |
| Downloader Codec-Mismatch Logging | `_warn_if_not_h264` in `utils/youtube_downloader.py` reuses `detect_video_codec` (imported, not reimplemented — grep confirms only one `def detect_video_codec`). Wired at both the yt-dlp success block (after "Ready for YouTube upload!") and the pytubefix early-success return. Logs only when codec != h264. No `format_map`/`ydl_opts` changes in the diff. | PASS |
| Backward Compatibility | `build_ffmpeg_cut_cmd(src, out, start, duration, reencode: bool = True)` — signature/default unchanged (confirmed via `grep -n "def build_ffmpeg_cut_cmd" -A 15`, no diff touches this function). `split_video_chapter`'s existing 4 positional params unchanged; `codec_cache=None` is a new **trailing optional** param. `_ffmpeg_extract_window` same pattern: `reencode=True` trailing optional, existing callers unaffected. | PASS |
| Test Coverage | See §2 below. | PASS |

## 2. Deliberate design points — explicitly verified in code, not inferred

- **Cache is a plain dict param, not a module global (CRITICAL design guard)**: confirmed — `utils/codec_detection.py` has no module-level cache variable. `get_cached_codec(source_path, cache)` takes the dict as its second parameter; callers (`extract_chapters_from_video`, the DAG task closure) each instantiate their own `codec_cache: dict = {}`. No violation found.
- **Pre-trim reuses the RAW SOURCE's cached codec, not a fresh probe of `clip_path`**: confirmed by reading the actual diff hunk in `reap_clip_preparer_dag.py` — `source_codec = get_cached_codec(source_video_path, codec_cache)` and `reencode = reencode_for_codec(source_codec)` are computed immediately after the chapter/source lookup, **before** `clip_path`/`chapter_folder` are even constructed, then the same `reencode` variable is threaded ~50 lines later into `_ffmpeg_extract_window(..., reencode=reencode)` for the pre-trim call. This is the correct, easy-to-get-wrong design point, and it is implemented correctly.
- **Single shared detection function, no divergent second implementation**: grepped the full diff + new file for `def detect_video_codec` — exactly one definition, in `utils/codec_detection.py`, imported by all three consuming modules (`video_splitter.py`, `reap_clip_preparer_dag.py`, `youtube_downloader.py`).
- **`reap_shorts_uploader_dag.py` untouched**: `git diff --stat -- congress_videos/reap_shorts_uploader_dag.py` and `git status --porcelain -- congress_videos/reap_shorts_uploader_dag.py` both returned empty output. Zero changes, confirmed independently (not just trusted from spec's scope-correction narrative).

## 3. Independent test suite re-execution (not just trusting apply's self-report)

Re-ran the exact same test command myself in this sandbox (reused the still-present throwaway venv at `/tmp/venv-airflow-test`, same approach apply used — no repo changes):

```
/tmp/venv-airflow-test/bin/python -m pytest tests/utils/test_codec_detection.py tests/utils/test_youtube_downloader.py \
  tests/utils/test_time_utils.py tests/congress_videos/modules/test_video_splitter.py \
  tests/congress_videos/test_reap_clip_preparer_dag.py -p no:cacheprovider --no-cov -q -m "not slow"
```

Result (fresh execution, this session): **194 passed, 1 deselected** — matches apply's reported count exactly, independently confirmed, not taken on faith.

## 4. Regression-proofing audit (5 updated existing tests in test_video_splitter.py)

Read the actual diff of `tests/congress_videos/modules/test_video_splitter.py`. Confirmed 5 pre-existing tests were updated to add `mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")` (lines 9, 17, 25, 33, 64 of the diff) rather than depending on the fixture file's real codec via `ffprobe`. This matches the task list's audited list of 5 tests (`test_ffmpeg_success`, `test_ffmpeg_nonzero_returncode_returns_error`, `test_ffmpeg_timeout_returns_specific_error`, `test_result_success_contains_all_keys`, `test_successful_extraction_increments_counter`) — count and mechanism confirmed by reading the code, not just the count claimed in apply-progress.

## 5. Assertion quality spot-check (strict TDD active)

Sampled `tests/utils/test_codec_detection.py`: tests mock `subprocess.run` at the module boundary and assert on the actual function's return value per codec-name input (h264/avc1/av1/av01/vp9/etc.) — behavioral assertions, not tautologies, not smoke-only, no CSS/implementation-detail assertions (not applicable to this Python codebase). No ghost loops or type-only assertions found in the sampled tests. No further quality issues found in the sampled sections of `test_video_splitter.py`'s cache-sharing tests (call-count assertions on the mock, appropriate for verifying "exactly one probe" cache behavior).

## 6. TDD Cycle Evidence

`apply-progress.md` contains a complete `TDD Cycle Evidence` table (RED/GREEN/TRIANGULATE/REFACTOR columns) for every implementation unit: `detect_video_codec`, `get_cached_codec`, `reencode_for_codec`/`cut_mode_for_reencode`, `split_video_chapter` result-dict fields, `split_video_chapter` codec_cache param, `_ffmpeg_extract_window` reencode param, pre-trim loop wiring, `_warn_if_not_h264` + wiring. Each row cites a real captured RED failure message (e.g. `ModuleNotFoundError`, `AssertionError: Missing key: source_codec`, `TypeError: ... unexpected keyword argument 'codec_cache'`) — these are real, specific, non-generic failure signatures consistent with genuine RED runs, not fabricated placeholders. Strict TDD compliance: **PASS**.

## 7. Task checkbox verification

Scanned `openspec/changes/ffmpeg-codec-aware-cuts/tasks.md` for `^\s*- \[ \]`. Unchecked lines found:

```
- [ ] Start or reuse bounded review for Slice 1 (new module + tests only). <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for Slice 2 (video_splitter.py wiring + regression-proofed existing tests). <!-- sdd-owner: parent -->
- [ ] Start or reuse bounded review for Slice 3 (reap_clip_preparer_dag.py + youtube_downloader.py wiring). <!-- sdd-owner: parent -->
- [ ] Run the full suite with coverage enforced: `conda run -n airflow python -m pytest` (or `python -m pytest` if env is active) from repo root and confirm `--cov-fail-under=80` still passes with all new/changed files included. <!-- sdd-owner: implementation -->
- [ ] Confirm success-criteria smoke check against the two known-bad AV1 videos (GMZ5TwfZJHw, sahjXSGn-Ak) referenced in the proposal, if NAS/fixture access is available in the apply environment; otherwise document that this was not runnable in this environment and defer to sdd-verify. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review as a final gate before archive, referencing all slice reviews. <!-- sdd-owner: parent -->
```

Classification:
- **4 items are `sdd-owner: parent`** (bounded review gates) — not implementation-owned, out of scope for this verify's PASS/FAIL of the code itself, but they ARE required before archive per the orchestrator's own Review Execution Contract.
- **2 items are `sdd-owner: implementation`** (full-coverage-gate run, NAS smoke check) — these are genuinely unchecked implementation tasks. Per this project's environment constraint (no conda/NAS access in this sandbox, explicitly accepted by the user ahead of this verify), these are **CRITICAL completeness gaps for archive purposes**, not silently passable, but are correctly deferred to the user's real environment rather than faked here.

**Per the verify contract: this is NOT a clean PASS and archive is NOT ready** while these 2 implementation-owned unchecked tasks and 4 parent-owned review gates remain outstanding. This is a completeness/process gate, not a code-defect finding — the code changes underlying those 2 tasks are otherwise fully verified correct in this environment (all 194 tests genuinely pass here; the gap is specifically the repo-wide 80% coverage gate and the NAS smoke check, both requiring resources unavailable in this sandbox).

## 8. Review Workload / PR boundary

Tasks.md forecasted chained PRs (400-line risk: High, ~480-650 lines). Apply-progress and the delegation record `size:exception` — explicit user/orchestrator decision to deliver as a single PR against `dev`, overriding the chained recommendation. This is documented (not silent scope creep) in both tasks.md's Review Workload Forecast section and apply-progress.md's "Workload / PR boundary" section. **No scope creep found**: `git diff --stat` for the working tree shows changes confined to exactly the files the design/tasks specified (`utils/codec_detection.py` new, `video_splitter.py`, `reap_clip_preparer_dag.py`, `youtube_downloader.py`, their respective test files) — no unrelated files touched by this change.

Note: the working tree also has unrelated **staged** changes (`congress_videos/config/constants.py`, `congress_videos/modules/participants_ingestion.py`, migration `012_create_congress_participants.sql`, fixtures) that are **pre-existing/unstaged from a different, unrelated piece of work** and were not touched or created by this change's diff — confirmed via `git diff --stat` scoped to only the ffmpeg-codec-aware-cuts files, and via the fact those staged files are entirely absent from this change's design/tasks/spec. Flagged as a **WARNING** for hygiene (working tree has mixed staged content from another feature) but NOT attributable to this change's scope.

## Blockers (exact)

1. **CRITICAL (completeness, archive blocker)**: 2 implementation-owned unchecked tasks in tasks.md — full-suite `--cov-fail-under=80` gate not run in the real conda/airflow env, and NAS smoke check against GMZ5TwfZJHw/sahjXSGn-Ak not run. Both are pending REAL verification outside this sandbox, per explicit user acceptance — not a code defect, but archive cannot proceed until done (or explicitly waived by the user/maintainer).
2. **WARNING (process, not this change's fault)**: 4 parent-owned "Start or reuse bounded review" gates (Slice 1, 2, 3, final) remain unchecked — required by the orchestrator's Review Execution Contract before archive.
3. **WARNING (hygiene, out of scope)**: working tree has unrelated staged changes (participants_ingestion feature) mixed in alongside this change's unstaged diff — recommend the user commit/stash them separately before proceeding to avoid an accidental combined commit.

## Items still pending REAL verification outside this sandbox (hand back to user)

- Full repo-wide test suite run in the real `conda run -n airflow python -m pytest` environment, confirming `--cov-fail-under=80` passes with all new/changed files included.
- NAS smoke check against the two known-bad AV1 videos (`GMZ5TwfZJHw`, `sahjXSGn-Ak`) referenced in the original proposal's success criteria.

## next_recommended

`resolve-blockers` — not `archive` yet. Once the user has run the real coverage gate + NAS smoke check (and the 4 parent-owned bounded reviews are completed/waived per the orchestrator's process), this change is otherwise fully verified and ready for archive.
