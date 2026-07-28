# Tasks: ffmpeg-codec-aware-cuts

Spec: `openspec/changes/ffmpeg-codec-aware-cuts/specs/congress-video-cutting/spec.md`
Design: `openspec/changes/ffmpeg-codec-aware-cuts/design.md` (approved, settled — do not reopen)

Test runner (from sdd-init, obs #7): `conda run -n airflow python -m pytest` (env-active: `python -m pytest` from repo root).
Coverage gate `--cov-fail-under=80` runs on every invocation via addopts — for fast/partial local runs during RED/GREEN cycles use `python -m pytest -p no:cacheprovider --no-cov <path>`, then run the full suite without `--no-cov` before closing out each slice.
Strict TDD Mode is ACTIVE for this project: every implementation unit below is sequenced RED → GREEN → TRIANGULATE → REFACTOR. Do not bundle "implement + test" into one step.

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~480–650 (new module ~90 + its tests ~130, video_splitter.py edit ~35 + tests ~80, reap_clip_preparer_dag.py edit ~55 + tests ~90, youtube_downloader.py edit ~25 + new tests ~70) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 → PR 2 → PR 3 (see Slice Boundary below) |
| Delivery strategy | ask-on-risk (session default) — parent must confirm with user before sdd-apply starts |
| Chain strategy | pending — parent/user to confirm (stacked-to-main vs feature-branch-chain) before apply |

```text
Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High
```

### Proposed Slice Boundary (for parent/user confirmation — not decided unilaterally here)

- **Slice 1 (PR 1)**: shared `utils/codec_detection.py` module + its own full test suite. Self-contained, no wiring into existing call sites yet. Lowest risk, easiest to review in isolation. Est. ~220 lines.
- **Slice 2 (PR 2)**: wire `video_splitter.split_video_chapter` (codec_cache param, probe, audit fields) + existing `test_video_splitter.py` audit/mock fixes + new wiring tests. Depends on Slice 1 merged (imports `utils.codec_detection`). Est. ~115 lines.
- **Slice 3 (PR 3)**: wire `reap_clip_preparer_dag.py` pre-trim (reuse raw-source decision, cache threading, log-line audit) + `youtube_downloader.py` post-download logging + their tests. Depends on Slice 1 merged; independent of Slice 2's internals (only depends on the shared module's public API). Est. ~215 lines.

If the user declines chaining, Slice 1+2+3 combined tasks below can run as a single PR — the task list is written so it works either way; only the PR boundary changes, not the task content.

---

## Slice 1 — Shared codec detection module (`utils/codec_detection.py`)

- [x] RED: Write `utils/test_codec_detection.py` covering `detect_video_codec()`: h264/avc1 → "h264"; av1/av01 → "av1"; unrecognized codec (vp9, hevc) → "unknown"; ffprobe binary missing (`FileNotFoundError`) → "unknown"; ffprobe timeout → "unknown"; ffprobe non-zero return code → "unknown"; empty stdout → "unknown". Assert the function never raises. Use `mocker`/`mock_subprocess_run` per existing `tests/conftest.py` conventions. Run with `python -m pytest utils/test_codec_detection.py -p no:cacheprovider --no-cov` and confirm all new tests FAIL (module doesn't exist yet). <!-- sdd-owner: implementation -->
- [x] RED: Add test cases to `utils/test_codec_detection.py` asserting distinct log messages/log calls for "unrecognized codec" (e.g. vp9/hevc) vs "ffprobe failure" (missing binary/timeout/non-zero) — per design Decision 3, these must not be masked as the same warning. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Implement `utils/codec_detection.py::detect_video_codec(source_path, *, timeout=30) -> str` per design Decision 3 exactly: `ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 <path>`; lowercase/strip stdout; h264/avc1→"h264", av1/av01→"av1", else→"unknown"; catch `FileNotFoundError`/`subprocess.TimeoutExpired`/non-zero returncode/generic `Exception`→"unknown" with distinct log messages for unrecognized-codec vs probe-failure. Run tests until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write tests for `get_cached_codec(source_path, cache)`: `cache=None` → probes directly every call (no memoization); `cache={}` dict → first call probes and stores under `os.path.abspath(source_path)`, second call with same path (even relative vs absolute variants) hits cache and does NOT call ffprobe again (assert mock call count == 1). Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Implement `get_cached_codec(source_path, cache)` per design Decision 2: key = `os.path.abspath(source_path)`; `cache is None` bypasses memoization; otherwise populate-on-first-probe, reuse thereafter. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write tests for `reencode_for_codec(codec) -> bool` (h264→True, av1→False, unknown→False) and `cut_mode_for_reencode(reencode) -> str` ("reencode"/"stream_copy"), including the 1:1 mapping invariant. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Implement `reencode_for_codec` and `cut_mode_for_reencode` in `utils/codec_detection.py`. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: Add at least 2 additional edge-case inputs per function (e.g. mixed-case codec strings like "H264"/"AV01", path with spaces, cache dict pre-populated with a stale/different codec value to confirm it is trusted as-is and not re-probed) to confirm the implementation generalizes rather than being hardcoded to the first fixture. <!-- sdd-owner: implementation -->
- [x] REFACTOR: Clean up `utils/codec_detection.py` (docstrings per Google style, import ordering stdlib/third-party/local, remove duplication between the two log-message branches) while keeping all tests GREEN. Run full `utils/test_codec_detection.py` file plus `python -m pytest -p no:cacheprovider --no-cov utils/` to confirm no collateral breakage in sibling `utils/` tests. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review for Slice 1 (new module + tests only). <!-- sdd-owner: parent -->

---

## Slice 2 — Wire `video_splitter.split_video_chapter` + audit existing tests (regression risk)

- [x] AUDIT (do first, before any wiring): Read `congress_videos/modules/test_video_splitter.py` in full and identify every test that exercises `split_video_chapter`'s re-encode vs stream-copy command shape (asserts on `libx264`/`-c copy`/`build_ffmpeg_cut_cmd` args). For each such test, either (a) confirm the fixture is verified/mocked to be h264 already, or (b) mark it for mocking in the next task. This directly addresses the design-flagged risk: unwired real fixture files will now hit real `ffprobe`, and a non-h264 fixture would silently flip to stream-copy and break existing re-encode-shape assertions. <!-- sdd-owner: implementation -->
- [x] RED/GREEN (regression-proofing): Update every existing `test_video_splitter.py` test identified in the audit task to explicitly mock `utils.codec_detection.detect_video_codec` (or `get_cached_codec`) to return a controlled value ("h264" for tests asserting re-encode shape, "av1" for tests asserting stream-copy shape if any exist) rather than relying on fixture file codec. Run the full existing `test_video_splitter.py` suite (`python -m pytest congress_videos/modules/test_video_splitter.py -p no:cacheprovider --no-cov`) and confirm it still passes with mocks in place BEFORE `split_video_chapter` is wired to call the new module (these tests should still pass at this point since the mock target doesn't exist in the call path yet — if they fail, fix the mock target path now, before wiring). <!-- sdd-owner: implementation -->
- [x] RED: Write new tests in `test_video_splitter.py` for the wired behavior: `split_video_chapter(..., codec_cache=None)` — h264 source → `reencode=True` passed to `build_ffmpeg_cut_cmd`; av1 source → `reencode=False`; ffprobe failure/unknown → `reencode=False` (fail-safe); result dict contains `source_codec` and `cut_mode` on the success path AND on both except-block failure paths (source_codec defaults to "unknown" before try, per design Decision 3). Confirm FAIL (function not yet wired). <!-- sdd-owner: implementation -->
- [x] RED: Write a test asserting `codec_cache` threading: calling `split_video_chapter` twice with the same `source_video_path` and a shared `codec_cache={}` dict results in exactly one `detect_video_codec`/ffprobe call (mock call count == 1). Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Wire `congress_videos/modules/video_splitter.py::split_video_chapter` per design Decision 3: add trailing optional `codec_cache=None` param; initialize `source_codec="unknown"` before the `try` block so failure paths report it; call `get_cached_codec(source_video_path, codec_cache)` then `reencode_for_codec(...)` before `build_ffmpeg_cut_cmd(reencode=...)`; add `source_codec`/`cut_mode` to the result dict on all paths (success + both except blocks). Import from `utils.codec_detection`. Run tests until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write a test for `extract_chapters_from_video` (the loop caller) asserting it creates one `codec_cache={}` before its chapter loop and threads it into every `split_video_chapter` call, so N chapters from the same source share one probe. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Update `extract_chapters_from_video` to create `codec_cache = {}` before the loop and pass it through to each `split_video_chapter` call. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: Add a test with 3+ chapters from the same source and assert ffprobe/detect_video_codec is called exactly once across all of them (not per-chapter), plus a test with 2 different source paths in sequence asserting 2 separate probes (cache doesn't cross-contaminate by source). <!-- sdd-owner: implementation -->
- [x] REFACTOR: Clean up `video_splitter.py` (docstring for the new param, import ordering) while keeping the full `test_video_splitter.py` suite GREEN, including all pre-existing tests updated in the audit step above. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review for Slice 2 (video_splitter.py wiring + regression-proofed existing tests). <!-- sdd-owner: parent -->

---

## Slice 3 — Wire `reap_clip_preparer_dag.py` pre-trim + `youtube_downloader.py` logging

- [x] RED: Write tests in `congress_videos/test_reap_clip_preparer_dag.py` for `_ffmpeg_extract_window(source_path, dest_path, start_secs, end_secs, reencode=True)`: assert the trailing optional `reencode` param controls the ffmpeg command shape (re-encode flags vs `-c copy`) and defaults to `True` for backward compatibility when omitted. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Add the trailing optional `reencode=True` param to `_ffmpeg_extract_window` and use it to select command shape, preserving the existing default so any caller that omits it keeps prior behavior. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write a test for the pre-trim call site's loop-scope logic asserting it (a) creates one `codec_cache = {}` before the chapter loop, (b) computes `source_codec`/`reencode` from the **raw source_video_path already in loop scope** — NOT a fresh probe of `clip_path` — per design Decision 1 (pre-trim reuses the raw source's cached decision because stream-copy preserves source codec), and (c) passes that `reencode` value into `_ffmpeg_extract_window`. Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Wire the pre-trim call site in `reap_clip_preparer_dag.py`: import `get_cached_codec`/`reencode_for_codec`/`cut_mode_for_reencode` from `utils.codec_detection`; create `codec_cache = {}` before the loop; compute `source_codec = get_cached_codec(source_video_path, codec_cache)` and `reencode = reencode_for_codec(source_codec)` per chapter using the raw source path (shared cache with `split_video_chapter`'s internal lookup for the same source within the same run); pass `reencode` into `_ffmpeg_extract_window`. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write a test asserting the existing info log line ("Chapter %s pre-trimmed: ... srt_window=%s") is extended to append " source_codec=%s cut_mode=%s" with correct values, and that no DB schema / result-dict field is added at this site (per spec Requirement 5 — pre-trim has no returned result dict, audit surfaces via log line only). Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Extend the log line in `reap_clip_preparer_dag.py` to append `source_codec`/`cut_mode`. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: Add a test confirming that when `split_video_chapter` and the pre-trim step process the SAME source video within the same DAG task run and share the same `codec_cache` instance, only one probe occurs total across both call sites (validates the cross-site cache sharing described in design Decision 2, not just within a single site). <!-- sdd-owner: implementation -->
- [x] RED: Write tests in `utils/test_youtube_downloader.py` (new file if absent) for a new `_warn_if_not_h264(file_path, *, context)` helper: h264 detected → no warning logged; av1/unknown detected → WARNING logged naming requested avc1/H264, the actual detected codec, and the `context` (video id or URL). Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Implement `_warn_if_not_h264` in `utils/youtube_downloader.py` reusing `detect_video_codec` from `utils/codec_detection.py` (no reimplementation). Run until GREEN. <!-- sdd-owner: implementation -->
- [x] RED: Write a test asserting `_warn_if_not_h264` is invoked post-download in the yt-dlp success path (after the "Ready for YouTube upload!" log, per design Decision 4) with `context=info.get("id") or youtube_url`, and that no `format_map`/`ydl_opts`/download-behavior code changed (structural diff check — same download call args as before). Confirm FAIL. <!-- sdd-owner: implementation -->
- [x] GREEN: Insert the `_warn_if_not_h264(...)` call at the yt-dlp success block insertion point identified in design Decision 4. Run until GREEN. <!-- sdd-owner: implementation -->
- [x] TRIANGULATE: Add a test for the secondary/recommended insertion point (before the pytubefix early `return result` in `download_youtube_video_for_upload`, per design Decision 4) if that code path exists and is reachable; if not applicable, add a comment/task note explaining why it was skipped rather than silently omitting coverage. <!-- sdd-owner: implementation -->
- [x] REFACTOR: Clean up `reap_clip_preparer_dag.py` and `utils/youtube_downloader.py` changes (docstrings, import ordering per project convention: stdlib/third-party/local) while keeping `test_reap_clip_preparer_dag.py` and `utils/test_youtube_downloader.py` fully GREEN. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review for Slice 3 (reap_clip_preparer_dag.py + youtube_downloader.py wiring). <!-- sdd-owner: parent -->

---

## Cross-slice final verification (run regardless of chaining decision)

- [ ] Run the full suite with coverage enforced: `conda run -n airflow python -m pytest` (or `python -m pytest` if env is active) from repo root and confirm `--cov-fail-under=80` still passes with all new/changed files included. <!-- sdd-owner: implementation -->
- [ ] Confirm success-criteria smoke check against the two known-bad AV1 videos (GMZ5TwfZJHw, sahjXSGn-Ak) referenced in the proposal, if NAS/fixture access is available in the apply environment; otherwise document that this was not runnable in this environment and defer to sdd-verify. <!-- sdd-owner: implementation -->
- [ ] Start or reuse bounded review as a final gate before archive, referencing all slice reviews. <!-- sdd-owner: parent -->

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~480–650 |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 → PR 2 → PR 3 |
| Delivery strategy | ask-on-risk |
| Chain strategy | pending |

```text
Decision needed before apply: Yes
Chained PRs recommended: Yes
Chain strategy: pending
400-line budget risk: High
```
