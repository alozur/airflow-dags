# Design: ffmpeg-codec-aware-cuts

Domain: `congress-video-cutting` · Project: `airflow-dags` · Pipeline: `congress_videos`
Status: design phase (next: tasks). Proposal + spec approved and settled.

## Scope reaffirmation (settled, do not reopen)

Two cut sites are in scope, both consuming the raw 7–10h AV1-risk source video:

1. `congress_videos/modules/video_splitter.py::split_video_chapter`
2. `congress_videos/reap_clip_preparer_dag.py`'s inline pre-trim (`_ffmpeg_extract_window`,
   the `"Pre-trim ffmpeg failed for chapter"` path).

`reap_shorts_uploader_dag.py` is **confirmed OUT of scope**: it is an audio-only
(`-vn`, `pcm_s16le`) extraction from an externally-produced Reap clip, not a cut of the
raw AV1 source. Not touched by this design.

---

## 1. Codec-detection module placement (design decision)

**Decision: create a new module `utils/codec_detection.py`.**

### Why `utils/` and not `video_splitter.py`

The dependency direction in this repo is one-way:

- `congress_videos/modules/video_splitter.py` imports `from utils.time_utils import parse_timestamp`.
- `congress_videos/reap_clip_preparer_dag.py` imports `from utils.airflow_helpers …`,
  `from utils.env_loader …`, and `from congress_videos.modules.video_splitter …`.
- `utils/youtube_downloader.py` imports only stdlib + `yt_dlp` — it does **not** import
  from `congress_videos/`.

`utils/` is the low-level shared layer; `congress_videos/` is the higher-level consumer.
The downloader (Requirement 6) needs the same detection function. If detection lived in
`congress_videos/modules/video_splitter.py`, then `utils/youtube_downloader.py` would have
to import *up* into `congress_videos/`, inverting the dependency arrow and creating a
circular-import risk (`congress_videos → utils → congress_videos`).

Placing detection in `utils/codec_detection.py` lets all three consumers
(`video_splitter.py`, `reap_clip_preparer_dag.py`, `youtube_downloader.py`) import it
without inverting the layering. It also reads as screaming-architecture: the module name
states exactly what it does. Existing `utils/` modules (`time_utils`, `env_loader`,
`ai_helpers`) confirm the convention of small single-purpose utility modules.

`build_ffmpeg_cut_cmd` stays in `video_splitter.py` (unchanged) — detection and
command-building are separate concerns and live in separate layers.

---

## 2. Cache placement and lifetime (the main open question)

### Call-structure facts (read from code)

- `split_video_chapter` is called **once per chapter in a loop** from
  `extract_chapters_from_video` (`video_splitter.py`). Many chapters can share the same
  `video_id` / source file.
- In `reap_clip_preparer_dag.py::_extract_and_pretrim_clip`, the loop calls
  `split_video_chapter` once per chapter, then conditionally calls `_ffmpeg_extract_window`
  (pre-trim) once for that same chapter. Both run inside a **single Airflow task's Python
  process** (`t2 = PythonOperator(task_id='extract_and_pretrim_clip')`).
- **Critical subtlety:** the pre-trim's input is `clip_path` — the chapter clip that
  `split_video_chapter` just wrote — **not** the raw source. But stream-copy preserves the
  source codec: if the raw source was AV1 and `split_video_chapter` stream-copied,
  `clip_path` is still AV1, and re-encoding it in the pre-trim would crash identically.
  Therefore the pre-trim must reuse the **raw source's** codec decision, not re-probe the
  derived clip. The raw `source_video_path` is already in scope in the loop, so this is
  free.

### Decision: caller-supplied process-local `dict`, keyed by resolved absolute source path

**Structure:** a plain `dict[str, str]` mapping `os.path.abspath(source_video_path)` →
`"h264" | "av1" | "unknown"`.

**Key:** resolved absolute path of the **raw source video** (`os.path.abspath`). Rationale:
- Both cut sites already hold the raw source path (`source_video_path` in
  `split_video_chapter`; `_find_source_video(video_id)` result in the DAG loop). It is the
  shared context both sites have.
- `video_id` is rejected as the key: it is not consistently available at both sites in the
  same form, and a bare `video_id` could collide with a re-downloaded/replaced physical
  file within the same run (the spec's stale-reuse constraint). The absolute file path is
  the physical identity of the bytes being cut, so it cannot alias a different file.

**Ownership / threading — caller-supplied optional parameter, NOT a module-level global:**

- `split_video_chapter` gains an **optional** parameter `codec_cache: dict | None = None`.
- `extract_chapters_from_video` creates `codec_cache = {}` once before its loop and passes
  the same dict into every `split_video_chapter` call → one probe per distinct source across
  all chapters.
- `_extract_and_pretrim_clip` creates `codec_cache = {}` once before its loop, passes it into
  `split_video_chapter`, and also uses it directly to compute the pre-trim's `reencode` value
  → the chapter cut and the pre-trim share one cache entry, one probe.
- When `codec_cache is None` (e.g. an ad-hoc single call or existing test), the lookup helper
  probes once for that call without caching — behaviorally correct, just not memoized.

**Why caller-supplied over a module-level global:** an explicit parameter is testable and
has no hidden cross-test state — each test constructs its own dict (or passes `None`) and
asserts probe counts deterministically. A module-level global cache would need explicit
reset between tests and could leak state across DAG task invocations that reuse the same
worker process. Explicit dependency > hidden module state; no strong reason here to prefer
the global.

**Why this satisfies the "MUST NOT persist across DAG runs" constraint:** the dict is a
local variable created *inside* the task callable / module function body. It lives only for
that Python call's stack and is garbage-collected when the function returns. It never touches
XCom or Airflow Variables, so there is no serialization surface through which a codec value
could leak into a later DAG run. This is strictly simpler than XCom/Variable, which would
require explicit serialization, a cleanup step, and careful keying to avoid exactly the
cross-run leak the spec forbids.

---

## 3. Function signatures & contracts

### `utils/codec_detection.py` (new)

```python
import logging
import os
import subprocess

logger = logging.getLogger(__name__)

_H264_NAMES = frozenset({"h264", "avc1"})
_AV1_NAMES = frozenset({"av1", "av01"})
FFPROBE_TIMEOUT_SECS = 30


def detect_video_codec(source_path: str, *, timeout: int = FFPROBE_TIMEOUT_SECS) -> str:
    """Return 'h264' | 'av1' | 'unknown' for the primary video stream. Never raises."""
    cmd = [
        "ffprobe", "-v", "error",
        "-select_streams", "v:0",
        "-show_entries", "stream=codec_name",
        "-of", "csv=p=0",
        source_path,
    ]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        logger.warning("ffprobe binary not found; codec unknown for %s", source_path)
        return "unknown"
    except subprocess.TimeoutExpired:
        logger.warning("ffprobe timed out (%ss); codec unknown for %s", timeout, source_path)
        return "unknown"
    except Exception as exc:  # defensive catch-all — detection must never raise
        logger.warning("ffprobe error for %s: %s; codec unknown", source_path, exc)
        return "unknown"

    if proc.returncode != 0:
        logger.warning(
            "ffprobe exit %s for %s: %s; codec unknown",
            proc.returncode, source_path, (proc.stderr or "").strip(),
        )
        return "unknown"

    name = (proc.stdout or "").strip().lower()
    if name in _H264_NAMES:
        return "h264"
    if name in _AV1_NAMES:
        return "av1"
    logger.warning("ffprobe reported unrecognized codec %r for %s; treating as unknown",
                   name, source_path)
    return "unknown"


def get_cached_codec(source_path: str, cache: dict | None) -> str:
    """Cache-aware lookup keyed by resolved absolute path. Probes at most once per key."""
    if cache is None:
        return detect_video_codec(source_path)
    key = os.path.abspath(source_path)
    if key not in cache:
        cache[key] = detect_video_codec(source_path)
    return cache[key]


def reencode_for_codec(codec: str) -> bool:
    """Decision rule: h264 -> reencode; av1/unknown -> stream-copy (fail-safe)."""
    return codec == "h264"


def cut_mode_for_reencode(reencode: bool) -> str:
    """1:1 mapping with the reencode bool for the audit field."""
    return "reencode" if reencode else "stream_copy"
```

**ffprobe command → normalized output mapping.** The command
`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of csv=p=0 <path>`
prints the primary video stream's `codec_name` and nothing else, e.g. `h264\n`, `av1\n`,
`vp9\n`, `hevc\n`, or empty on no video stream. After `.strip().lower()`:
`h264`/`avc1` → `"h264"`; `av1`/`av01` → `"av1"`; anything else (incl. empty, `vp9`,
`hevc`) → `"unknown"`. ffprobe reads container/stream metadata only (no decode), so it is
fast even on a 10h file — the 30s timeout is a generous safety bound, not an expected wait.

### `video_splitter.py::split_video_chapter` (signature extended, additive)

```python
def split_video_chapter(source_video_path, output_path, start_time, end_time, codec_cache=None):
```

- New trailing **optional** `codec_cache=None` — existing positional/keyword calls
  (`source_video_path, output_path, start_time, end_time`) remain valid unchanged
  (Backward-Compat requirement satisfied).
- Flow: initialize `source_codec = "unknown"` **before** the `try` so failure paths can
  report it. Inside `try`, after the duration checks and before `build_ffmpeg_cut_cmd`:

  ```python
  source_codec = get_cached_codec(source_video_path, codec_cache)
  reencode = reencode_for_codec(source_codec)
  ffmpeg_command = build_ffmpeg_cut_cmd(
      src=source_video_path, out=output_path,
      start=start_seconds, duration=duration_seconds,
      reencode=reencode,
  )
  ```
- `build_ffmpeg_cut_cmd` is reused **as-is** via its existing `reencode` parameter — no new
  command builder.

### `reap_clip_preparer_dag.py::_ffmpeg_extract_window` (signature extended, additive)

```python
def _ffmpeg_extract_window(source_path, dest_path, start_secs, end_secs, reencode=True):
```

- New trailing optional `reencode=True` preserves the current default for any caller that
  doesn't pass it. Passed straight through to `build_ffmpeg_cut_cmd(..., reencode=reencode)`.
- The caller (`_extract_and_pretrim_clip`) computes `reencode` from the **raw source's**
  cached codec and passes it in (see §5), so the pre-trim mirrors the chapter cut's decision.

---

## 4. Downloader integration (`utils/youtube_downloader.py`)

**Reuse `detect_video_codec` — no reimplementation.** Add one small module-level helper and
call it at the post-download success point(s):

```python
from utils.codec_detection import detect_video_codec  # top-of-file import

def _warn_if_not_h264(file_path: str, *, context: str) -> str:
    codec = detect_video_codec(file_path)
    if codec != "h264":
        logger.warning(
            "Downloaded file for %s has codec=%r but avc1/H264 was requested "
            "(yt-dlp/pytubefix fallback accepted a non-H264 stream): %s",
            context, codec, file_path,
        )
    return codec
```

**Insertion point (required):** inside `download_youtube_video_for_upload`'s yt-dlp success
block — the `if file_path.exists():` branch (the ~line 380 area), immediately after
`result["success"] = True` and the existing `"✅ Download complete"` / `"Ready for YouTube
upload!"` info logs, before the closing `else`:

```python
            logger.info(f"   Ready for YouTube upload!")
            _warn_if_not_h264(str(file_path), context=info.get("id") or youtube_url)
```

**Secondary insertion (recommended, same helper):** the function tries pytubefix first and
`return result` early on its success, bypassing the yt-dlp block. To also observe AV1 from
the pytubefix fallback path (its Fallback 2 accepts any adaptive video), call the same helper
before that early return in `download_youtube_video_for_upload`:

```python
        result = download_with_pytubefix(youtube_url, output_dir, min_resolution)
        if result["success"]:
            _warn_if_not_h264(result["file_path"], context=youtube_url)
            return result
```

Both call the identical helper/detection function. The message names (a) requested `avc1`/
H264, (b) the actual detected codec, (c) the video id / URL and file path for correlation.
No `format_map`, `ydl_opts`, or download behavior changes — logging only. `"h264"` result →
no warning.

---

## 5. Result-dict / audit-field integration

### `split_video_chapter` return dict

Add `source_codec` and `cut_mode` on **all** paths. Existing keys unchanged (additive).

Success path:
```python
return {
    "success": True,
    "output_path": output_path,
    "file_size_bytes": file_size,
    "file_size_mb": file_size_mb,
    "duration_seconds": duration_seconds,
    "start_time": start_time,
    "end_time": end_time,
    "source_codec": source_codec,                    # NEW
    "cut_mode": cut_mode_for_reencode(reencode),     # NEW
    "error": None,
}
```

Failure paths (`TimeoutExpired` and generic `Exception`) — spec requires audit fields where
a codec was determined before the failure. Because `source_codec` is initialized to
`"unknown"` before the `try`, both except blocks can always emit consistent fields (a failure
before probing reports `source_codec="unknown"`, `cut_mode="stream_copy"`):
```python
    return {
        "success": False,
        "output_path": None,
        "source_codec": source_codec,                                    # NEW
        "cut_mode": cut_mode_for_reencode(reencode_for_codec(source_codec)),  # NEW
        "error": error_msg,
    }
```

### `reap_clip_preparer_dag.py` pre-trim site

This site does **not** build a returned per-chapter result dict (it logs + inserts to DB, and
the spec forbids DB schema changes). Match the **existing log shape** rather than invent a
result dict. The chapter cut's audit fields already arrive via `result['source_codec']` /
`result['cut_mode']` from `split_video_chapter`. For the pre-trim, extend the existing
success info log:

Current:
```python
logging.info(
    "Chapter %s pre-trimmed: %.1f–%.1fs (%.0fs) srt_window=%s",
    chapter_id, pretrim_start, pretrim_end, duration, pretrim_used_srt,
)
```
Becomes:
```python
logging.info(
    "Chapter %s pre-trimmed: %.1f–%.1fs (%.0fs) srt_window=%s source_codec=%s cut_mode=%s",
    chapter_id, pretrim_start, pretrim_end, duration, pretrim_used_srt,
    source_codec, cut_mode_for_reencode(reencode),
)
```
where `source_codec` / `reencode` are the loop-scoped values computed from the raw source
(below). Existing failure/skip logging (`"Pre-trim ffmpeg failed for chapter"`, safety-gate
blocks) is unchanged.

### Loop wiring in `_extract_and_pretrim_clip`

```python
from congress_videos.modules.video_splitter import (build_ffmpeg_cut_cmd,
    compute_ffmpeg_timeout, split_video_chapter)
from utils.codec_detection import get_cached_codec, reencode_for_codec, cut_mode_for_reencode
...
def _extract_and_pretrim_clip(ti, **context):
    ...
    codec_cache: dict = {}          # NEW — task-scoped, one entry per source
    for chapter in chapters:
        ...
        source_video_path = _find_source_video(video_id)
        if not source_video_path:
            ...; continue

        source_codec = get_cached_codec(source_video_path, codec_cache)   # NEW
        reencode = reencode_for_codec(source_codec)                       # NEW

        result = split_video_chapter(
            source_video_path=source_video_path,
            output_path=clip_path,
            start_time=_interval_to_srt(start_time),
            end_time=_interval_to_srt(end_time),
            codec_cache=codec_cache,     # NEW — shared cache → chapter cut hits, no re-probe
        )
        ...
        # pre-trim branch:
        _ffmpeg_extract_window(
            source_path=clip_path, dest_path=trimmed_path,
            start_secs=pretrim_start, end_secs=pretrim_end,
            reencode=reencode,           # NEW — mirror the raw source's decision
        )
```

`get_cached_codec(source_video_path, codec_cache)` in the loop and the same call inside
`split_video_chapter` share the dict + key → exactly **one** ffprobe per distinct source
across the chapter cut and its pre-trim, and across all chapters from that source.

### `extract_chapters_from_video` wiring

```python
codec_cache: dict = {}              # NEW — before the loop
for chapter in uploadable_chapters:
    ...
    result = split_video_chapter(
        source_video_path=source_video_path, output_path=output_path,
        start_time=start_time, end_time=end_time,
        codec_cache=codec_cache,     # NEW — one probe per distinct source across chapters
    )
```

---

## 6. Data flow (end to end)

```
raw source video (mp4/mkv/webm on NAS)
        │
        ▼
get_cached_codec(abspath(source), codec_cache)     ← task-scoped dict, 1 probe/source
        │  (miss → detect_video_codec → ffprobe -show_entries codec_name)
        ▼
"h264" | "av1" | "unknown"
        │
        ▼
reencode_for_codec()  →  h264 ? True : False
        │
        ├─► split_video_chapter → build_ffmpeg_cut_cmd(reencode=…) → ffmpeg cut
        │         └─ result dict += source_codec, cut_mode
        │
        └─► _extract_and_pretrim_clip pre-trim → _ffmpeg_extract_window(reencode=…)
                  └─ existing info log += source_codec, cut_mode

download path (separate):
yt-dlp / pytubefix download completes → _warn_if_not_h264(file, ctx)
        └─ detect_video_codec → WARNING iff codec != "h264"
```

---

## 7. File-change summary

| File | Change | Kind |
|------|--------|------|
| `utils/codec_detection.py` | **NEW** — `detect_video_codec`, `get_cached_codec`, `reencode_for_codec`, `cut_mode_for_reencode` | add |
| `congress_videos/modules/video_splitter.py` | `split_video_chapter` gains optional `codec_cache=None`; probe → decide `reencode`; add `source_codec`/`cut_mode` to all result dicts; `extract_chapters_from_video` creates + threads `codec_cache` | edit (additive) |
| `congress_videos/reap_clip_preparer_dag.py` | import detection helpers; `_ffmpeg_extract_window` gains optional `reencode=True`; loop creates `codec_cache`, computes `source_codec`/`reencode`, threads cache into `split_video_chapter`, passes `reencode` to pre-trim, extends pre-trim info log | edit (additive) |
| `utils/youtube_downloader.py` | import `detect_video_codec`; add `_warn_if_not_h264` helper; call at yt-dlp success block (+ pytubefix early-return) | edit (logging only) |
| `utils/test_codec_detection.py` | **NEW** tests | add |
| `congress_videos/modules/test_video_splitter.py` | new decision/audit tests | edit |
| `congress_videos/test_reap_clip_preparer_dag.py` | new pre-trim decision/audit tests | edit |
| `utils/test_youtube_downloader.py` | downloader warning tests (new file if absent) | add/edit |

No DB schema changes. No `build_ffmpeg_cut_cmd`, `format_map`, or download-behavior changes.

---

## 8. Test strategy (strict TDD — tests co-located `test_*.py`, pytest + mocker)

- **`utils/test_codec_detection.py`** (mock `subprocess.run`):
  - `codec_name` `h264`/`avc1` → `"h264"`; `av1`/`av01` → `"av1"`; `vp9`/`hevc`/empty →
    `"unknown"`.
  - `FileNotFoundError` (ffprobe missing), `TimeoutExpired`, non-zero returncode → `"unknown"`,
    never raises.
  - `get_cached_codec`: with a real dict, probing once then 3 more lookups for the same path →
    `detect_video_codec`/`subprocess.run` called **exactly once**; `cache is None` → probes per
    call (no memo); different paths → separate entries.
  - `reencode_for_codec` / `cut_mode_for_reencode` truth table (3 codecs).
- **`test_video_splitter.py`**: patch `utils.codec_detection.detect_video_codec` (or
  `get_cached_codec`) — h264 source → `build_ffmpeg_cut_cmd` receives `reencode=True`; av1 and
  unknown → `reencode=False`. Result dict carries correct `source_codec`/`cut_mode` on success
  and on the failure paths. Passing the same `codec_cache` across N calls → one probe.
- **`test_reap_clip_preparer_dag.py`**: patch detection — pre-trim's `_ffmpeg_extract_window`
  called with `reencode` matching the raw source codec; cache shared with `split_video_chapter`
  → one probe per source across chapter + pre-trim.
- **`test_youtube_downloader.py`**: patch `detect_video_codec` — `"av1"`/`"unknown"` → WARNING
  logged with requested-vs-actual + context; `"h264"` → no warning; download behavior asserted
  unchanged.

---

## 9. Rollout & rollback

Purely additive/behavioral within existing functions plus one new leaf module. No migration,
no persisted-state change. Rollback = straight revert of the commit(s); nothing to backfill.
Deploy is a standard DAG code deploy — `ffprobe` is already a runtime dependency (the DAG's
existing safety gate already calls `ffprobe`), so no new binary requirement.

---

## 10. Risks & mitigations

- **Existing `split_video_chapter` tests using real fixture files** will now run `ffprobe` on
  those fixtures; a non-h264/unknown fixture would flip the command to stream-copy and could
  break a re-encode-shape assertion. Mitigation: in the tasks phase, mock detection in those
  tests (patch `get_cached_codec`/`detect_video_codec`) or ensure fixtures probe as `h264`.
  Flagged for tasks — do not let this silently regress the existing suite.
- **Pre-trim reuses the raw-source decision, not a re-probe of `clip_path`.** This is correct
  because stream-copy preserves the source codec, but it is a deliberate coupling — documented
  here so a future refactor doesn't "fix" it into a redundant second probe.
- **`os.path.abspath` vs `realpath`**: `abspath` normalizes cwd-relative paths; both cut sites
  already build absolute paths, so `abspath` suffices. If symlinked download dirs ever appear,
  switch the key to `os.path.realpath` — noted, not needed now.
- **Unknown-codec masking**: `detect_video_codec` logs a **distinct** WARNING for the
  unrecognized-codec case vs the ffprobe-failure case, so a real ffprobe misconfiguration is
  not silently laundered into the expected-av1 path.

## 11. Next phase

`tasks` — break this into TDD task batches (new module + tests first, then the two cut-site
wirings, then downloader logging), respecting the backward-compat and probe-once acceptance
criteria.
