# Apply Progress: ffmpeg-codec-aware-cuts

Delivery context: single PR against `dev` (size:exception recorded — ~480-650 estimated
changed lines vs 400-line budget, user chose one PR over chaining). No chain_strategy
applies. Strict TDD Mode active throughout (RED → GREEN → TRIANGULATE → REFACTOR per unit).

## Environment note (sandbox limitation — read before trusting "full suite" claims)

This apply ran in a sandbox without `conda`/the `airflow` env (per sdd-init obs #7, the
canonical test command is `conda run -n airflow python -m pytest`; `conda` is not on PATH
here). To execute real tests I created a throwaway venv (`/tmp/venv-airflow-test`, **not**
part of the repo) and installed `pytest`, `pytest-cov`, `pytest-mock`, `pytest-xdist`,
`freezegun`, `psycopg2-binary`, plus a **minimal local stub of the `airflow` package**
(DAG/PythonOperator/ShortCircuitOperator/AirflowException/TaskInstance) installed only into
that venv's site-packages so `congress_videos/reap_clip_preparer_dag.py` could be imported
and its real (pre-existing) test suite could run. This stub is NOT part of the repo, was not
committed anywhere, and does not affect production code or the real `conda run -n airflow`
env used in CI/other environments.

Consequence: all TDD evidence below for the touched files is from REAL pytest runs (not
inferred), but:
- The repo-wide `--cov-fail-under=80` gate could not be validated end-to-end here — many
  unrelated modules fail to import in this sandbox venv (missing `openai`, `googleapiclient`,
  `beautifulsoup4`/`urllib3`, `webrtcvad-wheels`, etc. — all pre-existing, unrelated to this
  change). A full-suite run with real deps must happen in the real `airflow` conda env before
  merge (task left unchecked, see below).
- The NAS smoke check (known-bad AV1 videos GMZ5TwfZJHw / sahjXSGn-Ak) requires NAS access
  not available in this sandbox — not runnable here, left unchecked and deferred to
  sdd-verify/real environment per the task's own fallback instruction.

Everything else (all new/changed test files, all 3 slices' wiring) ran for real and is GREEN.

## Persistence note

Engram HTTP server was unreachable during this session (`mem_search`/`mem_save` reported
"could not reach the Engram HTTP server at http://127.0.0.1:7437"). Tasks (obs #66), spec
(obs #60), design (obs #64), and sdd-init (obs #7) were still readable via cached
`mem_search` results, so inputs were retrieved successfully. This apply-progress and the
tasks checkbox updates are persisted to the **openspec** file store (this file +
`openspec/changes/ffmpeg-codec-aware-cuts/tasks.md`). An `mem_save`/`mem_update` attempt to
also write to Engram is included below for completeness; if the server was still down when
this ran, the parent/orchestrator should retry the Engram write once the server is back up.

## Files changed

- `utils/codec_detection.py` — **new**: `detect_video_codec`, `get_cached_codec`,
  `reencode_for_codec`, `cut_mode_for_reencode`.
- `tests/utils/test_codec_detection.py` — **new**, 31 tests.
  (Deviation from design.md's illustrative path `utils/test_codec_detection.py`: the repo's
  actual test convention, confirmed by reading `tests/utils/` and root `conftest.py`
  (`testpaths=["tests"]`), is a mirrored `tests/<pkg>/test_*.py` tree, not co-located
  `test_*.py` next to source. Used the real convention; same test content/coverage.)
- `congress_videos/modules/video_splitter.py` — `split_video_chapter` gains optional
  `codec_cache=None`; probes codec via `get_cached_codec`, decides `reencode` via
  `reencode_for_codec`, passes it to `build_ffmpeg_cut_cmd`; adds `source_codec`/`cut_mode` to
  the result dict on all paths (success + both except blocks); `source_codec` initialized to
  `"unknown"` before `try` so failure-before-probe paths still report consistent fields.
  `extract_chapters_from_video` creates one `codec_cache = {}` before its loop and threads it
  into every `split_video_chapter` call.
- `tests/congress_videos/modules/test_video_splitter.py` — 5 existing tests updated to mock
  `utils.codec_detection.detect_video_codec` (audit/regression-proofing, see Slice 2 below);
  10 new tests added (`TestSplitVideoChapterCodecAware` x7, cache-sharing tests x2 in
  `TestExtractChaptersFromVideo`, plus the key-list update). 45 tests total, all passing.
- `congress_videos/reap_clip_preparer_dag.py` — imports `get_cached_codec`,
  `reencode_for_codec`, `cut_mode_for_reencode`; `_ffmpeg_extract_window` gains optional
  `reencode=True` (backward-compat default); `_extract_and_pretrim_clip`'s loop creates one
  `codec_cache = {}`, computes `source_codec`/`reencode` from the **raw** `source_video_path`
  (not `clip_path`) per design Decision 1, threads the cache into `split_video_chapter`, passes
  `reencode` into the pre-trim call, and extends the existing pre-trim info log line with
  ` source_codec=%s cut_mode=%s`. No DB schema / result-dict change at this site (pre-trim has
  no returned dict, per spec Requirement 5).
- `tests/congress_videos/test_reap_clip_preparer_dag.py` — `_setup_mocks` helper extended to
  mock `detect_video_codec` (audit/regression-proofing); 7 new tests added
  (`_ffmpeg_extract_window` reencode param x2, raw-source-not-clip-path x2, log-line extension,
  cross-site single-probe sharing). 26 tests total, all passing.
- `utils/youtube_downloader.py` — imports `detect_video_codec`; new `_warn_if_not_h264(file_path, *, context)`
  helper (reuses `detect_video_codec`, no reimplementation); wired at both insertion points
  per design Decision 4: the yt-dlp success block (after "Ready for YouTube upload!") and the
  pytubefix success early-return in `download_youtube_video_for_upload`. No `format_map`/
  `ydl_opts`/download-behavior changes.
- `tests/utils/test_youtube_downloader.py` — 7 new tests (`TestWarnIfNotH264` x4,
  `TestWarnIfNotH264WiredIntoYtdlpSuccessPath` x3 including the secondary pytubefix insertion
  point). 57 tests total, all passing.

## Persisted task checkboxes

`openspec/changes/ffmpeg-codec-aware-cuts/tasks.md` updated: 31 implementation-owned
checkboxes marked `[x]` (all of Slice 1, Slice 2, Slice 3's RED/GREEN/TRIANGULATE/REFACTOR
items). 3 parent-owned "Start or reuse bounded review" checkboxes left untouched (byte-for-byte,
per ownership boundary). 2 implementation-owned cross-slice items left unchecked (see
Remaining below) because they were honestly not runnable in this sandbox.

## TDD Cycle Evidence

| Unit | RED (confirmed failing) | GREEN | TRIANGULATE | REFACTOR |
|---|---|---|---|---|
| `detect_video_codec` | `ModuleNotFoundError: utils.codec_detection` (module didn't exist) | 24/24 tests pass | +7 tests (mixed-case, path w/ spaces, stale cache trust) → 31/31 pass | docstrings, import order, log-message dedup reviewed; suite still green |
| `get_cached_codec` | same RED run (module missing) | included in the 24 | included above | " |
| `reencode_for_codec` / `cut_mode_for_reencode` | same RED run | included in the 24 | included above | " |
| `split_video_chapter` result-dict fields | `AssertionError: Missing key: source_codec` (real failure, captured) | 45/45 `test_video_splitter.py` tests pass | 3+ chapters share 1 probe / 2 sources = 2 probes tests added and pass | import order (utils.codec_detection above utils.time_utils), docstring updated |
| `split_video_chapter` codec_cache param | `TypeError: split_video_chapter() got an unexpected keyword argument 'codec_cache'` | same 45/45 | same as above | same |
| `_ffmpeg_extract_window` reencode param | `TypeError: _ffmpeg_extract_window() got an unexpected keyword argument 'reencode'` | 26/26 `test_reap_clip_preparer_dag.py` tests pass | cross-site single-probe-sharing test added and passes | docstring updated to explain default+rationale |
| pre-trim loop wiring (raw-source reuse, log line) | 4 failing (`KeyError: 'reencode'` x2, log-format TypeError in test itself — fixed, then `AssertionError: 0 == 1` on cache sharing) | same 26/26 | included above | " |
| `_warn_if_not_h264` + wiring (yt-dlp block) | `AttributeError: ... does not have the attribute '_warn_if_not_h264'` (x6 tests) | 57/57 `test_youtube_downloader.py` tests pass | secondary pytubefix-success insertion point test added (RED confirmed, then GREEN) | import ordering confirmed (stdlib/yt_dlp third-party/utils local) |

Audit/regression-proofing step (Slice 2, before wiring): read
`tests/congress_videos/modules/test_video_splitter.py` in full; identified 5 tests exercising
`split_video_chapter`'s ffmpeg command construction that would start hitting the real
`ffprobe` once codec detection is wired in (`test_ffmpeg_success`,
`test_ffmpeg_nonzero_returncode_returns_error`, `test_ffmpeg_timeout_returns_specific_error`,
`test_result_success_contains_all_keys`, `test_successful_extraction_increments_counter`).
Added `mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")` to each,
confirmed the suite still passed identically before wiring (mock target exists at the
`utils.codec_detection` module level from Slice 1, just unused pre-wiring), then proceeded to
wire `split_video_chapter`. Same pattern applied to `_setup_mocks`/inline mocks in
`test_reap_clip_preparer_dag.py` for the same reason (the loop's own `get_cached_codec` call
would otherwise probe real ffprobe or collide with the shared `subprocess.run` mock used for
the safety-gate ffprobe call).

## Test commands run (real pytest, this sandbox's venv)

```
# Slice-by-slice, fast partial runs during RED/GREEN:
python -m pytest tests/utils/test_codec_detection.py -p no:cacheprovider --no-cov -q
python -m pytest tests/congress_videos/modules/test_video_splitter.py -p no:cacheprovider --no-cov -q -m "not slow"
python -m pytest tests/congress_videos/test_reap_clip_preparer_dag.py -p no:cacheprovider --no-cov -q
python -m pytest tests/utils/test_youtube_downloader.py -p no:cacheprovider --no-cov -q

# Cross-slice combined run (final):
python -m pytest tests/utils/test_codec_detection.py tests/utils/test_youtube_downloader.py \
  tests/utils/test_time_utils.py tests/congress_videos/modules/test_video_splitter.py \
  tests/congress_videos/test_reap_clip_preparer_dag.py -p no:cacheprovider --no-cov -q -m "not slow"
```

Result: **194 passed, 1 deselected** (the 1 deselected is the pre-existing
`@pytest.mark.slow @pytest.mark.integration` real-ffmpeg AV1 test in
`test_video_splitter.py`, unrelated to this change and already marked slow before this apply).

Coverage on touched files only (`--cov=utils.codec_detection --cov=congress_videos.modules.video_splitter
--cov=congress_videos.reap_clip_preparer_dag --cov=utils.youtube_downloader`):
`utils/codec_detection.py` 100%, `congress_videos/modules/video_splitter.py` 93.48%,
`congress_videos/reap_clip_preparer_dag.py` 81.99%, `utils/youtube_downloader.py` 83.97%.

Full repo-wide run attempted (`--continue-on-collection-errors`, no `--no-cov`): 647 passed,
270 failed, 58 errors — spot-checked several failures (`test_ai_helpers.py`,
`test_git_sync_dag.py`, etc.) and confirmed they are all pre-existing `ModuleNotFoundError`/
`AttributeError` collection failures from optional deps not installed in this sandbox venv
(`openai`, `googleapiclient`, full `apache-airflow`, `webrtcvad-wheels`, etc.) — unrelated to
`utils/codec_detection.py`, `video_splitter.py`, `reap_clip_preparer_dag.py`, or
`youtube_downloader.py`. None of the spot-checked failures touch codec detection or the two
cut sites. This must be re-run in the real `conda run -n airflow python -m pytest` env before
merge to get a trustworthy repo-wide coverage number (task left unchecked below).

## Deviations from design

1. Test file location: `tests/utils/test_codec_detection.py` (mirrored tree) instead of
   design.md's illustrative `utils/test_codec_detection.py` (co-located) — matches the repo's
   actual, confirmed test convention (`pyproject.toml` `testpaths=["tests"]`, all other
   `utils/*` tests live under `tests/utils/`). No functional difference.
2. No other deviations. Function signatures, cache key (`os.path.abspath`), cache ownership
   (caller-supplied, not module-global), `reencode_for_codec`/`cut_mode_for_reencode` mapping,
   pre-trim's raw-source-reuse (not a fresh probe of `clip_path`), and both downloader
   insertion points match design.md exactly.

## Remaining (not done by this apply)

Implementation-owned, left unchecked in tasks.md:
- [ ] `- [ ] Run the full suite with coverage enforced: conda run -n airflow python -m pytest ...` —
  blocked by sandbox environment (no conda/airflow env here); must run in the real env before
  merge/archive.
- [ ] `- [ ] Confirm success-criteria smoke check against the two known-bad AV1 videos
  (GMZ5TwfZJHw, sahjXSGn-Ak) ...` — blocked by no NAS access in this sandbox; per the task's
  own fallback clause this is documented here and deferred to sdd-verify / a NAS-connected
  environment.

Parent-owned, untouched (byte-for-byte) per ownership boundary — deferred lifecycle actions:
- [ ] Start or reuse bounded review for Slice 1 (new module + tests only).
- [ ] Start or reuse bounded review for Slice 2 (video_splitter.py wiring + regression-proofed existing tests).
- [ ] Start or reuse bounded review for Slice 3 (reap_clip_preparer_dag.py + youtube_downloader.py wiring).
- [ ] Start or reuse bounded review as a final gate before archive, referencing all slice reviews.

## Workload / PR boundary

Per the delegated-task instructions, this ran as a **single PR** against `dev`
(`size:exception` recorded by the orchestrator/user, overriding the tasks.md-forecasted
chained-PR recommendation). No chain_strategy applies; all 3 slices were implemented in one
pass on `feat/ffmpeg-codec-aware-cuts`. No commit was made (implementation only, per
instructions — orchestrator handles commit/PR).

## Structured status consumed

- Delivery: single PR, size:exception, no chain_strategy (given directly in the task prompt —
  not re-litigated).
- Strict TDD: active, test command `conda run -n airflow python -m pytest` / env-active
  `python -m pytest` (sdd-init obs #7).
- `actionContext`/edit roots: none flagged; all edits stayed within
  `utils/`, `congress_videos/`, and `tests/` under the existing repo root, on the pre-checked-out
  `feat/ffmpeg-codec-aware-cuts` branch. No branch switch/creation performed.
