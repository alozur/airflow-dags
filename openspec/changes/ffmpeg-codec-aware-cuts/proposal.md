# Proposal: ffmpeg-codec-aware-cuts

## Status
Draft — proposal phase (next: spec)

## Problem Statement

Congressional plenary session videos (7–10 hours) are frequently only available from
YouTube as AV1 (`av01`) at 720p+, because YouTube does not serve `avc1`/H264 for very
long streams at that resolution. `utils/youtube_downloader.py`'s format string already
prefers `avc1`, but its fallback chain (`bestvideo[height>=720]+bestaudio/best`) has no
codec filter, so it silently accepts AV1 when H264 isn't offered.

Two real production files confirm this is not a download/network defect:
- `GMZ5TwfZJHw` (7h10 session, 18/06): `av01`, 1280x720, 800kb/s — file size/duration/
  bitrate consistent with a complete, non-truncated download.
- `sahjXSGn-Ak` (10h05 session, 23/07): `av01`, 1280x720, 877kb/s — same, file complete.

When `congress_videos/modules/video_splitter.py`'s `split_video_chapter` re-encodes a
cut window with `libx264`, ffmpeg crashes with OBU/AAC decode errors whenever the cut
window lands on a corrupted AV1 segment. This is genuine dav1d-vs-YouTube-AV1-encode
bitstream incompatibility in specific segments of the source — not a tolerable decode
glitch — so `-err_detect ignore_err` (added in a prior commit) does not help.

Two prior fixes were tried and are insufficient (do not repeat):
- `87aeb8b` — forced H264 in the downloader: YouTube simply doesn't offer H264 for
  these long streams, so this cannot succeed.
- `1c0316b` — added input-seeking + `-err_detect ignore_err` tolerance: still fatal on
  real mid-stream corruption, only helps with minor glitches.

The same long AV1-risk source videos are cut in two places today
(`video_splitter.split_video_chapter` and the inline pre-trim cut in
`reap_clip_preparer_dag.py`), both of which currently assume a re-encode-first /
stream-copy-fallback strategy and can hit the same crash.

## Goals

1. Detect the source video's codec proactively (via `ffprobe`), before deciding the
   ffmpeg cut strategy, instead of reactively catching a re-encode failure.
2. If codec is `h264` → keep the current default: frame-accurate re-encode
   (`libx264`/`aac`).
3. If codec is `av1`, or ffprobe fails, or the codec is unrecognized → stream-copy
   (`-c copy`), which never decodes the corrupted bitstream. This trades frame accuracy
   for reliability: keyframe-snap drift of a few seconds is accepted.
4. Apply this decision consistently across the two ffmpeg cut sites that operate on
   these source videos: `video_splitter.split_video_chapter` and the inline pre-trim
   cut in `reap_clip_preparer_dag.py`.
5. Cache the ffprobe result per source video (keyed by video_id / source file path) so
   a single source video producing dozens of chapters/shorts is probed exactly once per
   pipeline run, not once per cut.
6. Add observability to the downloader: log a clear warning when the yt-dlp fallback
   actually returns AV1 instead of the requested H264, so future AV1 rate is visible
   without re-investigating from scratch.
7. Make the codec decision auditable per cut: each cut's result dict includes
   `source_codec` and `cut_mode` (`"reencode"` | `"stream_copy"`).

## Non-Goals

- Fixing or working around YouTube's AV1 encoding itself — out of anyone's control.
- Changing yt-dlp's format string / download quality preference or download strategy.
  The downloader keeps trying `avc1` first; this change only adds *logging* when the
  fallback yields AV1, it does not alter what gets downloaded.
- Any database schema changes. Auditability is result-dict only (in-memory / task
  return value), not persisted to a new column or table.
- Retrying failed re-encodes with a different backoff/queue strategy — replaced
  entirely by the proactive codec check.
- Transcoding existing AV1 source files to H264 ahead of time (a valid future
  optimization, but out of scope here).

## Proposed Approach

1. **Shared codec-detection helper**: a small function (likely in
   `congress_videos/modules/video_splitter.py` or a new shared module) that runs
   `ffprobe` against a source video path and returns a normalized codec string
   (`"h264"`, `"av1"`, or `"unknown"` on ffprobe failure / unrecognized codec).
2. **Per-source-video cache**: a cache (keyed by video_id or source file path) storing
   the detected codec, populated on first probe and reused for every subsequent cut
   drawn from the same source file within a pipeline run. Scope and lifetime of this
   cache (process-local dict vs. Airflow XCom/Variable) is a spec/design decision.
3. **Decision rule wired into both cut sites**:
   - `congress_videos/modules/video_splitter.py::split_video_chapter`
   - `congress_videos/reap_clip_preparer_dag.py` inline pre-trim cut (~line 208,
     "Pre-trim ffmpeg failed for chapter")
   Each site consults the cached codec for its source video and chooses `reencode` for
   `h264`, `stream_copy` for `av1` or `unknown` — replacing today's
   try-reencode/catch/retry-stream-copy pattern with a decide-then-cut-once pattern.

   **Excluded**: `congress_videos/reap_shorts_uploader_dag.py`'s ffmpeg call (~line
   170, "ffmpeg failed for short") was investigated and confirmed out of scope. It
   is an audio-only extraction (`ffmpeg -vn ...` to WAV, for Whisper transcription),
   not a video cut, and its input is `short.get('local_file_path')` — a short clip
   already produced and downloaded from an external service (Reap, via
   `reap_client.download_clip` in `reap_processor_dag.py`), not the raw 7–10h AV1
   source video. It is not exposed to the AV1 corruption risk this change addresses,
   so it must not be wired into the codec decision.
4. **Downloader logging**: after a download completes in
   `utils/youtube_downloader.py`, inspect the actual downloaded stream's codec and log
   a warning if it's not `avc1`/H264 despite the format string requesting it. No change
   to `format_map` or download behavior.
5. **Result dict auditability**: `split_video_chapter` and the equivalent result
   structure in the other cut site gain `source_codec` and `cut_mode` fields.

## Business / Product Framing

This is a solo-maintainer congressional video archival pipeline (no other stakeholders
to coordinate with). The business problem is operational reliability: long plenary
sessions are the most important content (full-day sessions) and are exactly the videos
most likely to be AV1-only, so today's crash disproportionately affects the highest-value
source material. The fix converts unpredictable pipeline failures (crash mid-run,
requiring manual re-trigger and investigation) into predictable, self-selecting behavior
(automatically the right cut strategy, with a visible audit trail per chapter/short). The
downloader logging closes the loop: it turns "AV1 happens sometimes" into a measurable,
observable trend so future capacity/prioritization decisions have data instead of
having to redo this incident investigation.

## Affected Areas

- `congress_videos/modules/video_splitter.py`
- `congress_videos/reap_clip_preparer_dag.py`
- `congress_videos/reap_shorts_uploader_dag.py`
- `utils/youtube_downloader.py`
- (possibly) a new shared helper module for codec detection + caching

## Risks

- **Frame accuracy loss on AV1 sources**: stream-copy cuts snap to the nearest
  keyframe, so AV1-sourced chapters/shorts may start/end a few seconds off the
  requested boundary. Accepted tradeoff per agreed fail-safe rule — reliability over
  precision when the source is AV1.
- **Cache key collisions**: if the cache key (video_id vs. file path) isn't chosen
  carefully, a stale codec could be reused across re-downloads or across DAG runs.
  Needs explicit key/lifetime definition in spec/design.
- **ffprobe availability/cost**: ffprobe must be present in the runtime environment;
  probing a 7–10 hour file should be fast (container/stream metadata only, no decode),
  but this should be verified in design/tasks.
- **Silent unknown-codec cases**: defaulting unknown codecs to stream-copy is
  fail-safe for reliability, but could mask a real ffprobe misconfiguration if not
  logged distinctly from the expected `av1` case.

## Rollback

All changes are additive/behavioral within existing functions (new helper, new
decision branch, new log lines, new result-dict fields) — no schema or interface
migration. Rollback is a straight revert of the commit(s); no data backfill or cleanup
required since no persisted state changes.

## Success Criteria

- Both known-bad production videos (`GMZ5TwfZJHw`, `sahjXSGn-Ak`) can be fully chaptered
  and/or clipped without ffmpeg crashing, using stream-copy mode.
- A known-good H264 source video continues to produce frame-accurate re-encoded cuts
  (no regression for the common case).
- ffprobe is called at most once per distinct source video per pipeline run, regardless
  of how many chapters/shorts are cut from it.
- Every cut's result dict includes `source_codec` and `cut_mode`.
- The downloader logs a warning (not silent) whenever it falls back to a non-H264
  download.

## Proposal Question Round (already conducted)

The following product questions were asked and answered earlier in this conversation;
recorded here for traceability, not for re-asking:

1. Which cut sites are in scope? → All three (chapter splitter, pre-trim, shorts).
2. Should codec detection be cached, and at what granularity? → Yes, per source video,
   reused across all cuts from that source within a run.
3. Should downloader behavior change? → No, only add observability logging for AV1
   fallback.
4. Should audit fields be persisted to the DB? → No, result-dict only.
5. What should happen when ffprobe fails or returns an unknown codec? → Fail-safe to
   stream-copy, never blind re-encode.

No further question round requested by the user for this change.
