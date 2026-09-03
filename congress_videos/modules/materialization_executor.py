"""Execution layer for speaker-turn video materialization.

Consumes a :class:`~congress_videos.modules.materialization.MaterializationPlan`
(from the pure planning module) together with a resolved source-video path and
executes the necessary ffmpeg commands to produce a single output MP4.

Three execution paths:

1. **Single keep-interval, non-AV1** — plain stream-copy cut via
   :func:`build_ffmpeg_cut_cmd` with ``reencode=False``.

2. **Single keep-interval, AV1 source** — re-encode via
   :func:`build_ffmpeg_cut_cmd` with ``reencode=True`` (AV1 + stream-copy
   produces an unplayable moov atom).

3. **Multiple keep-intervals (excision, ``needs_reencode=True``)** — concat-
   demuxer pipeline: each keeper segment is cut with re-encode (libx264/aac),
   written to a temporary file, a concat-list file is written, and ffmpeg
   joins all segments with a final ``-c copy`` pass into the output path.
   Temporary files are removed on both success and failure.

All subprocess calls use list-form arguments (no shell interpolation).
Timeouts are computed from the total kept duration via
:func:`compute_ffmpeg_timeout`.

Usage::

    from congress_videos.modules.materialization import plan_turn_materialization
    from congress_videos.modules.materialization_executor import execute_plan

    plans = plan_turn_materialization(turns, approved_trims)
    for plan in plans:
        execute_plan(plan, source_path, output_path, codec=codec)
"""

from __future__ import annotations

import logging
import os
import subprocess
import tempfile
from pathlib import Path

from congress_videos.modules.materialization import MaterializationPlan
from congress_videos.modules.video_splitter import (
    build_ffmpeg_cut_cmd,
    compute_ffmpeg_timeout,
)
from utils.codec_detection import get_cached_codec

__all__ = [
    "execute_plan",
]

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _write_concat_list(segment_paths: list[str], tmpdir: str) -> str:
    """Write an ffmpeg concat-demuxer list file and return its path.

    Each line is ``file '<absolute_path>'`` as required by the demuxer.
    ``-safe 0`` is set at call-site so absolute paths are accepted.

    Args:
        segment_paths: Ordered list of absolute paths to segment MP4 files.
        tmpdir:        Directory where the list file is written.

    Returns:
        Absolute path to the written ``concat_list.txt`` file.
    """
    list_path = os.path.join(tmpdir, "concat_list.txt")
    with open(list_path, "w", encoding="utf-8") as fh:
        for seg in segment_paths:
            fh.write(f"file '{seg}'\n")
    return list_path


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def execute_plan(
    plan: MaterializationPlan,
    source_path: str,
    output_path: str,
    *,
    codec: str | None = None,
    codec_cache: dict | None = None,
) -> None:
    """Execute a :class:`~congress_videos.modules.materialization.MaterializationPlan`.

    Creates the output directory if needed, then follows one of three ffmpeg
    strategies depending on the number of keep-intervals and the source codec:

    - **1 interval, non-AV1** → ``build_ffmpeg_cut_cmd(reencode=False)`` (stream copy).
    - **1 interval, AV1/unknown** → ``build_ffmpeg_cut_cmd(reencode=True)`` (re-encode).
    - **N > 1 intervals** → concat-demuxer: N segment cuts (always re-encode)
      written to temp files, joined into ``output_path`` via ``-c copy``.

    Temp files (multi-interval path only) are cleaned up in a ``finally`` block
    on both success and failure, so the NAS never accumulates orphan segments.

    Args:
        plan:         Materialization plan produced by
                      :func:`~congress_videos.modules.materialization.plan_turn_materialization`.
        source_path:  Absolute path to the downloaded source video.
        output_path:  Absolute path where the output MP4 is written.
        codec:        Pre-detected source codec string (``"h264"`` | ``"av1"`` |
                      ``"unknown"``). When ``None``, the codec is probed via
                      :func:`~utils.codec_detection.get_cached_codec` using
                      ``codec_cache``.
        codec_cache:  Optional shared dict for memoizing codec probes across
                      multiple :func:`execute_plan` calls within the same DAG run.

    Raises:
        RuntimeError: When any ffmpeg invocation exits with a non-zero code.
    """
    # Resolve codec if not supplied
    if codec is None:
        codec = get_cached_codec(source_path, codec_cache)

    keep = plan.keep_intervals

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if len(keep) == 1:
        _execute_single_interval(source_path, output_path, keep[0], codec)
    else:
        _execute_multi_interval(source_path, output_path, keep, codec)


def _execute_single_interval(
    source_path: str,
    output_path: str,
    interval,
    codec: str,
) -> None:
    """Run a single-interval cut (stream-copy or re-encode depending on codec).

    Design rule: only h264 sources use stream-copy (``-c copy``). AV1, unknown,
    and every other codec (h265, vp9, …) re-encode via libx264/aac. This is a
    deliberately conservative policy: stream-copy is enabled only for the codec
    proven safe in this pipeline, so a valid moov atom is always produced. The
    ``reencode`` flag passed to ``build_ffmpeg_cut_cmd`` is therefore:
    - h264, no trims → ``reencode=False`` → stream-copy (fast, keyframe-snapped).
    - anything else  → ``reencode=True``  → libx264/aac (safe, frame-accurate).
    """
    force_reencode = codec != "h264"

    duration = interval.end - interval.start
    cmd = build_ffmpeg_cut_cmd(
        src=source_path,
        out=output_path,
        start=interval.start,
        duration=duration,
        reencode=force_reencode,
    )
    timeout = compute_ffmpeg_timeout(duration)
    log.info(
        "executor.single_interval src=%s out=%s start=%.2f duration=%.2f codec=%s reencode=%s",
        source_path,
        output_path,
        interval.start,
        duration,
        codec,
        force_reencode,
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg single-interval cut failed (exit {result.returncode}): {result.stderr}")


def _execute_multi_interval(
    source_path: str,
    output_path: str,
    keep_intervals,
    codec: str,
) -> None:
    """Run the concat-demuxer pipeline for multiple keep intervals.

    Each keeper segment is re-encoded (libx264/aac) and written to a temp
    file. All temp files are removed in a ``finally`` block on both success
    and failure.
    """
    tmpdir = tempfile.mkdtemp(prefix="mat_exec_")
    segment_paths: list[str] = []

    try:
        total_kept = sum(ki.end - ki.start for ki in keep_intervals)
        seg_timeout = compute_ffmpeg_timeout(total_kept / len(keep_intervals))

        for idx, ki in enumerate(keep_intervals):
            seg_path = os.path.join(tmpdir, f"seg_{idx:03d}.mp4")
            segment_paths.append(seg_path)
            duration = ki.end - ki.start
            cmd = build_ffmpeg_cut_cmd(
                src=source_path,
                out=seg_path,
                start=ki.start,
                duration=duration,
                reencode=True,  # excision always re-encodes
            )
            log.info(
                "executor.segment idx=%d start=%.2f duration=%.2f -> %s",
                idx,
                ki.start,
                duration,
                seg_path,
            )
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=seg_timeout)
            if result.returncode != 0:
                raise RuntimeError(f"ffmpeg segment cut {idx} failed (exit {result.returncode}): {result.stderr}")

        # Write concat list and join
        list_path = _write_concat_list(segment_paths, tmpdir)
        concat_cmd = [
            "ffmpeg",
            "-y",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_path,
            "-c",
            "copy",
            output_path,
        ]
        concat_timeout = compute_ffmpeg_timeout(total_kept)
        log.info(
            "executor.concat n_segments=%d output=%s timeout=%ds",
            len(segment_paths),
            output_path,
            concat_timeout,
        )
        concat_result = subprocess.run(concat_cmd, capture_output=True, text=True, timeout=concat_timeout)
        if concat_result.returncode != 0:
            raise RuntimeError(f"ffmpeg concat failed (exit {concat_result.returncode}): {concat_result.stderr}")

    finally:
        # Clean up all temp segment files and the list file
        for seg in segment_paths:
            if os.path.exists(seg):
                os.remove(seg)
        list_file = os.path.join(tmpdir, "concat_list.txt")
        if os.path.exists(list_file):
            os.remove(list_file)
        try:
            os.rmdir(tmpdir)
        except OSError:
            pass  # non-empty directory; leave it (shouldn't happen)
