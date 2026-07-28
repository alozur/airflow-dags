# Congress Video Cutting Specification

## Purpose

Congressional plenary session videos may arrive as H264 (`avc1`) or AV1 (`av01`)
depending on what YouTube serves for a given stream length/resolution. Re-encoding
a cut window on a corrupted AV1 source crashes ffmpeg with fatal OBU/AAC decode
errors. This spec defines codec-aware cut behavior — detect the source codec once,
cache it per source video, and choose the ffmpeg cut strategy (re-encode vs.
stream-copy) accordingly — applied consistently across the two ffmpeg cut sites
that operate on the raw source videos, plus downloader observability and per-cut
audit fields.

This is a new domain spec; no prior canonical spec exists for `congress-video-cutting`.
Domain name and boundary were inferred from the proposal's Affected Areas (no
`Capabilities` section was present in the proposal) — flagged as a risk below.

## Requirements

### Requirement: Codec Detection Function

The system MUST provide a single shared codec-detection function that determines
the video codec of a source file via `ffprobe`, used as the one source of truth for
codec detection across all cut sites and the downloader.

- Input: a source file path (`str`).
- Output: a normalized codec string, one of exactly `"h264"`, `"av1"`, or `"unknown"`.
- The function MUST NOT raise on ffprobe failure, timeout, or a missing `ffprobe`
  binary — it MUST catch those conditions internally and return `"unknown"`.
- Any codec name ffprobe reports that is not `h264`/`avc1` or `av1`/`av01` (e.g.
  `vp9`, `hevc`) MUST normalize to `"unknown"` — there is no additional named
  category beyond `"h264"`, `"av1"`, `"unknown"`; unrecognized codecs are treated
  identically to the fail-safe unknown case, not as a distinct branch.

#### Scenario: H264 source detected

- GIVEN a source file whose primary video stream codec is `h264`
- WHEN the codec-detection function is called with that file's path
- THEN it returns `"h264"`

#### Scenario: AV1 source detected

- GIVEN a source file whose primary video stream codec is `av1` (`av01`)
- WHEN the codec-detection function is called with that file's path
- THEN it returns `"av1"`

#### Scenario: ffprobe binary missing

- GIVEN the `ffprobe` executable is not available on the runtime `PATH`
- WHEN the codec-detection function is called with any source file path
- THEN it returns `"unknown"` and does not raise an exception

#### Scenario: ffprobe times out

- GIVEN `ffprobe` does not complete within the function's configured timeout
- WHEN the codec-detection function is called
- THEN it returns `"unknown"` and does not raise an exception

#### Scenario: ffprobe process failure

- GIVEN `ffprobe` exits with a non-zero return code or produces unparseable output
- WHEN the codec-detection function is called
- THEN it returns `"unknown"` and does not raise an exception

#### Scenario: unrecognized codec

- GIVEN a source file whose primary video stream codec is `vp9` or `hevc`
- WHEN the codec-detection function is called with that file's path
- THEN it returns `"unknown"` (not a new category)

### Requirement: Per-Source-Video Codec Cache

The system MUST memoize the detected codec per source video so that a source
video producing many chapters/shorts is probed by `ffprobe` at most once within
a single pipeline/task execution.

- Cache key MUST be derived from `video_id` and/or the resolved source file path
  (exact key strategy is a design decision, but it MUST NOT allow a stale codec
  from a different physical file to be reused against a re-downloaded/replaced
  source within the same run).
- Cache scope MUST be limited to a single pipeline/task execution (e.g. a
  process-local structure). The cache MUST NOT be persisted across separate
  Airflow DAG runs — every new run starts with an empty cache and re-probes on
  first use.
- On first lookup for a given source video, the cache MUST populate itself by
  calling the codec-detection function and storing the result.
- On every subsequent lookup for the same source video within the same
  run/process, the cache MUST return the previously stored result without
  invoking `ffprobe` again.

#### Scenario: First cut probes, later cuts reuse

- GIVEN a source video that has not yet been probed in the current pipeline run
- WHEN a cut site requests the codec for that source for the first cut
- THEN the codec-detection function is invoked exactly once, and the result is stored

#### Scenario: Subsequent cuts from the same source reuse the cache

- GIVEN a source video whose codec was already probed and cached earlier in the
  same pipeline run
- WHEN N additional cuts (chapters or shorts) are requested from that same source
- THEN `ffprobe` is invoked zero additional times for those N cuts, and each cut
  receives the cached codec value

#### Scenario: Cache does not survive across separate runs

- GIVEN a source video was probed and cached during pipeline run A
- WHEN pipeline run B later processes cuts from the same source video
- THEN run B performs its own fresh probe (the cache from run A is not visible to run B)

### Requirement: Codec-Based Reencode Decision Rule

The system MUST select the ffmpeg cut mode based on the cached/detected codec,
reusing the existing `reencode` parameter of `build_ffmpeg_cut_cmd` rather than
introducing a new command-building path.

- WHEN the detected codec is `"h264"`, THEN the cut site MUST call
  `build_ffmpeg_cut_cmd` (or the site's equivalent command builder) with
  `reencode=True` (frame-accurate re-encode path, unchanged from current behavior).
- WHEN the detected codec is `"av1"` or `"unknown"`, THEN the cut site MUST call
  `build_ffmpeg_cut_cmd` (or the site's equivalent command builder) with
  `reencode=False` (stream-copy path).
- This decision MUST be made once per cut, before subprocess invocation — not as
  a reactive retry after a failed re-encode attempt.

#### Scenario: H264 source re-encodes

- GIVEN a source video with cached/detected codec `"h264"`
- WHEN a cut is performed against that source
- THEN the ffmpeg command is built with `reencode=True`

#### Scenario: AV1 source stream-copies

- GIVEN a source video with cached/detected codec `"av1"`
- WHEN a cut is performed against that source
- THEN the ffmpeg command is built with `reencode=False`

#### Scenario: Unknown codec fails safe to stream-copy

- GIVEN a source video whose codec could not be determined (`"unknown"`)
- WHEN a cut is performed against that source
- THEN the ffmpeg command is built with `reencode=False` — the system never
  attempts a blind re-encode when the codec is not confidently known

### Requirement: Codec-Aware Wiring at Both Cut Sites

The system MUST wire codec detection, caching, and the reencode decision rule
into each of the two in-scope ffmpeg cut sites, replacing today's
try-reencode/catch/retry-stream-copy pattern with a decide-then-cut-once pattern,
without changing each site's existing signature/return-shape contract beyond the
additive audit fields defined below.

- `congress_videos/modules/video_splitter.py::split_video_chapter` MUST consult
  the shared cache for `source_video_path`'s codec before calling
  `build_ffmpeg_cut_cmd`, and MUST pass the resulting `reencode` value through.
- `congress_videos/reap_clip_preparer_dag.py`'s inline pre-trim cut (around the
  `"Pre-trim ffmpeg failed for chapter"` error path, via `_ffmpeg_extract_window`)
  MUST consult the shared cache for its source file's codec before calling
  `build_ffmpeg_cut_cmd`, and MUST pass the resulting `reencode` value through.
- Both sites MUST use the same shared codec-detection function and the same
  shared cache — no site may implement a second, divergent detection or caching
  mechanism.
- `congress_videos/reap_shorts_uploader_dag.py`'s ffmpeg call (around the
  `"ffmpeg failed for short"` error path) is explicitly OUT OF SCOPE for this
  wiring: it is an audio-only extraction (`ffmpeg -vn ...` to WAV, for Whisper
  transcription) against an already-produced/downloaded short clip
  (`short.get('local_file_path')`, sourced via `reap_client.download_clip`), not
  a cut of the raw AV1-risk source video. It MUST NOT be wired into the codec
  decision rule.

#### Scenario: video_splitter chooses reencode per codec

- GIVEN `split_video_chapter` is called with a source video of known codec
- WHEN it builds its ffmpeg command
- THEN the `reencode` value passed to `build_ffmpeg_cut_cmd` matches the codec
  decision rule for that source's cached codec

#### Scenario: clip preparer pre-trim chooses reencode per codec

- GIVEN the pre-trim step in `reap_clip_preparer_dag.py` runs against a source
  clip of known codec
- WHEN it builds its ffmpeg command via `_ffmpeg_extract_window` /
  `build_ffmpeg_cut_cmd`
- THEN the `reencode` value passed matches the codec decision rule for that
  source's cached codec

### Requirement: Per-Cut Audit Fields in Result Dicts

The system MUST add two fields — `source_codec` (`str`: `"h264"` | `"av1"` |
`"unknown"`) and `cut_mode` (`str`: `"reencode"` | `"stream_copy"`) — to the
result of every cut performed at both in-scope cut sites, without removing or
renaming any existing result-dict keys.

- `split_video_chapter`'s existing return dict (currently including `success`,
  `output_path`, `file_size_bytes`, `file_size_mb`, `duration_seconds`,
  `start_time`, `end_time`, `error`) MUST gain `source_codec` and `cut_mode` keys
  on both success and failure paths where a codec was determined before the
  failure occurred.
- `reap_clip_preparer_dag.py`'s pre-trim result/log context (the per-chapter
  processing outcome tracked around the pre-trim step) MUST record
  `source_codec` and `cut_mode` for that chapter's pre-trim cut.
- `cut_mode` MUST be `"reencode"` exactly when `reencode=True` was used to build
  the command, and `"stream_copy"` exactly when `reencode=False` was used —
  there is a 1:1 mapping, no independent state.

#### Scenario: Successful h264 cut carries audit fields

- GIVEN a successful cut against an `"h264"` source
- WHEN the cut site returns its result
- THEN the result includes `source_codec: "h264"` and `cut_mode: "reencode"`

#### Scenario: Successful av1 cut carries audit fields

- GIVEN a successful cut against an `"av1"` source
- WHEN the cut site returns its result
- THEN the result includes `source_codec: "av1"` and `cut_mode: "stream_copy"`

#### Scenario: Unknown-codec cut still carries audit fields

- GIVEN a cut against a source whose codec could not be determined
- WHEN the cut site returns its result
- THEN the result includes `source_codec: "unknown"` and `cut_mode: "stream_copy"`

### Requirement: Downloader Codec-Mismatch Logging

`utils/youtube_downloader.py` MUST log a warning after a download completes when
the actual downloaded file's codec differs from the requested `avc1`/H264
preference, using the same shared codec-detection function defined above (no
second, divergent detection implementation in the downloader).

- The check MUST run once, after the download completes and the final file path
  is known (post-download detection only — this requirement does not change
  `format_map` or any download/format-selection behavior).
- WHEN the detected codec of the downloaded file is not `"h264"` (i.e. it is
  `"av1"` or `"unknown"`), THEN the downloader MUST log at `WARNING` level (or
  the codebase's established equivalent) a message that identifies: (a) that the
  requested/preferred codec was `avc1`/H264, (b) the actual detected codec value,
  and (c) enough context (e.g. video id or file path) to correlate the warning
  with the specific download.
- WHEN the detected codec of the downloaded file is `"h264"` (the requested
  case), THEN no codec-mismatch warning MUST be logged.

#### Scenario: Downloaded file matches requested codec

- GIVEN a download completes and the final file's detected codec is `"h264"`
- WHEN the post-download codec check runs
- THEN no codec-mismatch warning is logged

#### Scenario: Downloaded file falls back to AV1

- GIVEN a download completes and the final file's detected codec is `"av1"`
- WHEN the post-download codec check runs
- THEN a `WARNING`-level log is emitted naming the requested codec (`avc1`/H264),
  the actual detected codec (`av1`), and identifying context for the download

#### Scenario: Downloaded file codec cannot be determined

- GIVEN a download completes and the post-download codec check returns `"unknown"`
- WHEN the check runs
- THEN a `WARNING`-level log is emitted naming the requested codec, the
  `"unknown"` result, and identifying context for the download

### Requirement: Backward Compatibility of Existing Interfaces

The system MUST preserve existing public interfaces used by current callers and
tests; codec-awareness is additive.

- `build_ffmpeg_cut_cmd`'s existing signature (including the `reencode: bool =
  True` default and both its re-encode and stream-copy command shapes) MUST be
  reused as-is by the codec-aware wiring — this change MUST NOT replace it with
  a new command-building function or alter its default behavior for callers that
  do not go through the new codec-detection/cache path.
- `split_video_chapter`'s existing positional/keyword call signature
  (`source_video_path`, `output_path`, `start_time`, `end_time`) MUST remain
  unchanged; codec-awareness must not require callers to pass new arguments.
- Existing successful-path result-dict keys at both in-scope cut sites MUST remain
  present with their existing meanings; `source_codec` and `cut_mode` are
  additions, not replacements.
- The DAG cut site (`reap_clip_preparer_dag.py`) MUST continue to produce a
  failure/skip outcome consistent with its current error-handling behavior (e.g.
  `"Pre-trim ffmpeg failed for chapter"` logging) when the underlying ffmpeg
  invocation itself fails, regardless of which `reencode` mode was selected.

#### Scenario: Existing h264-only caller behavior unchanged

- GIVEN an existing test or caller of `split_video_chapter` against a known
  H264 source, written before this change
- WHEN that test/caller runs after this change is applied
- THEN it still receives `reencode=True` behavior and all previously-asserted
  result keys, with `source_codec`/`cut_mode` present as additional keys

#### Scenario: build_ffmpeg_cut_cmd default is unchanged

- GIVEN a caller invokes `build_ffmpeg_cut_cmd` without specifying `reencode`
- WHEN the command is built
- THEN it defaults to `reencode=True` exactly as before this change

### Requirement: Test Coverage

The system MUST have unit-test coverage for the following areas before this
change is considered complete:

- Codec detection: `"h264"` case, `"av1"` case, `"unknown"` case for an
  unrecognized codec (e.g. `vp9`/`hevc`), and `"unknown"` case for a missing/
  failing `ffprobe` binary or timeout.
- Cache behavior: a source probed once and reused across N (N ≥ 2) subsequent
  cut requests within the same run results in exactly one `ffprobe` invocation.
- Decision wiring: for each of the two in-scope cut sites, an `"h264"` source
  selects `reencode=True` and an `"av1"`/`"unknown"` source selects
  `reencode=False`.
- Result-dict audit fields: for each of the two in-scope cut sites, a successful
  cut's result contains the correct `source_codec` and `cut_mode` values for each
  of the three codec cases.
- Downloader logging: a warning is logged only when the actual downloaded
  codec differs from the requested `avc1`/H264 preference, and is not logged on
  a successful `avc1`/H264 download.

#### Scenario: Test suite covers all codec-detection branches

- GIVEN the test suite for the shared codec-detection function
- WHEN it is run
- THEN it includes passing cases for `"h264"`, `"av1"`, unrecognized-codec
  `"unknown"`, and ffprobe-failure/timeout `"unknown"`

#### Scenario: Test suite covers cache reuse

- GIVEN the test suite for the per-source-video cache
- WHEN a source is probed once and 3 subsequent cuts are requested
- THEN the test asserts `ffprobe` (or the mocked detection call) was invoked
  exactly once across all 4 requests

## Risks / Open Questions Carried From Proposal

- **Domain inference**: the proposal has no `Capabilities` section; this spec's
  domain (`congress-video-cutting`) and requirement boundaries were inferred
  from the proposal's Affected Areas. Flagged for confirmation in design/tasks.
- **Scope correction — shorts uploader confirmed out of scope**: reading
  `reap_shorts_uploader_dag.py` around the `"ffmpeg failed for short"` log line
  confirmed this ffmpeg invocation extracts a `pcm_s16le` **audio-only** stream
  (`-vn`) from `short.get('local_file_path')` — a short clip already produced and
  downloaded from an external service (Reap, via `reap_client.download_clip` in
  `reap_processor_dag.py`), for Whisper transcription. It is not a cut of the raw
  7–10h AV1 source video and is not exposed to the AV1 corruption risk this change
  addresses. This site has been removed from scope entirely (was previously,
  incorrectly, listed as a third in-scope cut site); no further design resolution
  is needed.
- **Cache key definition** (video_id vs. resolved file path, and how to avoid
  stale reuse across re-downloads within the same run) is a design decision per
  the proposal; this spec states the constraint but not the concrete
  implementation.
- **ffprobe performance on 7–10h files**: expected to be fast (container/stream
  metadata only), but not verified here — flagged for design/tasks to confirm.
