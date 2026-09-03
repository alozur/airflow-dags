"""Shared ffmpeg/ffprobe-based video codec detection.

Used by both cut sites (``congress_videos.modules.video_splitter`` and
``congress_videos.reap_clip_preparer_dag``) and the downloader
(``utils.youtube_downloader``) to decide whether a source video can be safely
re-encoded (H264) or must fall back to a stream-copy cut (AV1/unknown), and to
warn when a downloaded file is not H264. See design.md for the full rationale.
"""

import logging
import os
import subprocess

logger = logging.getLogger(__name__)

_H264_NAMES = frozenset({"h264", "avc1"})
_AV1_NAMES = frozenset({"av1", "av01"})
FFPROBE_TIMEOUT_SECS = 30


def detect_video_codec(source_path: str, *, timeout: int = FFPROBE_TIMEOUT_SECS) -> str:
    """Return ``"h264"`` | ``"av1"`` | ``"unknown"`` for the primary video stream.

    Runs ``ffprobe -show_entries stream=codec_name`` against the first video
    stream and normalizes the result. Never raises: a missing ffprobe binary,
    a timeout, a non-zero exit, or any other error is caught and reported as
    ``"unknown"`` (fail-safe — callers treat unknown as "do not re-encode").

    Args:
        source_path: Path to the video file to probe.
        timeout: Subprocess timeout in seconds (default 30s). ffprobe reads
            only container/stream metadata, not the full stream, so this is a
            generous safety bound rather than an expected wait.

    Returns:
        ``"h264"``, ``"av1"``, or ``"unknown"``.
    """
    cmd = [
        "ffprobe",
        "-v",
        "error",
        "-select_streams",
        "v:0",
        "-show_entries",
        "stream=codec_name",
        "-of",
        "csv=p=0",
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
            proc.returncode,
            source_path,
            (proc.stderr or "").strip(),
        )
        return "unknown"

    name = (proc.stdout or "").strip().lower()
    if name in _H264_NAMES:
        return "h264"
    if name in _AV1_NAMES:
        return "av1"
    logger.warning(
        "ffprobe reported unrecognized codec %r for %s; treating as unknown",
        name,
        source_path,
    )
    return "unknown"


def get_cached_codec(source_path: str, cache: dict | None) -> str:
    """Cache-aware codec lookup keyed by the resolved absolute source path.

    Probes at most once per distinct source path when a cache dict is
    supplied. Passing ``cache=None`` disables memoization (each call probes).

    Args:
        source_path: Path to the raw source video.
        cache: A caller-owned, process-local dict to memoize results in, or
            ``None`` to probe directly without caching.

    Returns:
        The detected codec string (``"h264"`` | ``"av1"`` | ``"unknown"``).
    """
    if cache is None:
        return detect_video_codec(source_path)
    key = os.path.abspath(source_path)
    if key not in cache:
        cache[key] = detect_video_codec(source_path)
    return cache[key]


def reencode_for_codec(codec: str) -> bool:
    """Decide whether to re-encode: only H264 sources re-encode.

    AV1 and unknown sources fall back to stream-copy (fail-safe): re-encoding
    a corrupt/misdetected AV1 source is what causes ffmpeg to crash.

    Args:
        codec: Detected codec string, as returned by :func:`detect_video_codec`.

    Returns:
        ``True`` for ``"h264"``, ``False`` otherwise.
    """
    return codec == "h264"


def cut_mode_for_reencode(reencode: bool) -> str:
    """Map the ``reencode`` decision to its audit-field string.

    Args:
        reencode: The boolean produced by :func:`reencode_for_codec`.

    Returns:
        ``"reencode"`` when ``reencode`` is ``True``, else ``"stream_copy"``.
    """
    return "reencode" if reencode else "stream_copy"
