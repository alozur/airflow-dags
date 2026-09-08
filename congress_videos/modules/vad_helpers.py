"""Voice Activity Detection (VAD) helpers for chapter silence-trim.

This module detects the real start AND end of *sustained* speech in a chapter's
audio span and trims the silence on BOTH edges, so persisted chapters no longer
open in the silent "dead start" (people walking in, room noise) nor end in
trailing applause/silence. ``trim_chapter_silence_with_vad`` is the public entry
point wired into the monitor DAG.

Design notes:
- ``first_sustained_speech_start`` / ``last_sustained_speech_end`` are the pure,
  backend-agnostic cores. They take ``(start, end)`` voiced segments (merged into
  blocks by ``_merge_speech_blocks``) and never import any VAD library, so they
  are fully testable with synthetic data and are the coverage anchor.
- ``extract_audio_wav`` shells out to ffmpeg to produce a mono 16 kHz WAV.
- ``detect_speech_bounds`` wires a backend (webrtcvad by default, silero opt-in)
  to the cores, deriving BOTH edges from a SINGLE backend pass. The heavy
  ``silero_vad``/``torch`` deps are **lazy-imported** inside the silero branch
  only; the default webrtc path and CI never touch torch. Any failure returns
  ``None`` for an edge so VAD can only shrink a chapter, never block the pipeline.
"""

from __future__ import annotations

import copy
import logging
import math
import os
import subprocess
import tempfile
import wave
from collections.abc import Callable

import numpy as np

from congress_videos.config.constants import (
    CHAPTER_SPLIT_MIN_GAP_SECS,
    VAD_BACKEND,
    VAD_ENABLED,
    VAD_END_MARGIN_SECS,
    VAD_GAP_MERGE_SECS,
    VAD_MIN_CHAPTER_SECS,
    VAD_MIN_SUSTAINED_SECS,
    VAD_SAFETY_MARGIN_SECS,
    VAD_SAMPLE_RATE,
    VAD_TURN_TRIM_EPSILON_SECS,
)
from congress_videos.config.paths import get_download_video_path
from congress_videos.modules.video_editor import _get_source_duration
from congress_videos.modules.video_splitter import compute_ffmpeg_timeout
from utils.time_utils import format_timestamp, parse_timestamp

log = logging.getLogger(__name__)

_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm")


def _find_source_video(target_date: str, video_id: str) -> str | None:
    """Locate the downloaded source media for a video on disk.

    Looks in ``downloads/{target_date}/{video_id}/`` (via
    :func:`get_download_video_path`) and returns the first ``.mp4``/``.mkv``/
    ``.webm`` file that is not a ``chapter_video`` artifact, or ``None`` if the
    directory or a media file is missing.
    """
    video_dir = get_download_video_path(target_date, video_id)
    if not os.path.isdir(video_dir):
        return None
    for filename in sorted(os.listdir(video_dir)):
        if filename.endswith(_MEDIA_SUFFIXES) and "chapter_video" not in filename:
            return os.path.join(video_dir, filename)
    return None


def _merge_speech_blocks(
    segments: list[tuple[float, float]],
    gap_merge_secs: float,
) -> list[tuple[float, float, float]]:
    """Merge voiced segments into sustained blocks.

    Segments are sorted by start; adjacent segments separated by a gap shorter
    than ``gap_merge_secs`` are coalesced into one block. Backend-agnostic and
    pure — the shared core of :func:`first_sustained_speech_start` and
    :func:`last_sustained_speech_end`.

    Returns:
        A time-ordered list of ``(block_start, block_end, voiced_secs)`` where
        ``voiced_secs`` is the accumulated voiced time within the block (NOT the
        wall span). Empty when ``segments`` is empty.
    """
    if not segments:
        return []

    ordered = sorted(segments, key=lambda seg: seg[0])
    blocks: list[tuple[float, float, float]] = []
    block_start = ordered[0][0]
    block_end = ordered[0][1]
    block_voiced = 0.0
    prev_end = ordered[0][0]

    for start, end in ordered:
        if start - prev_end > gap_merge_secs:
            blocks.append((block_start, block_end, block_voiced))
            block_start = start
            block_end = end
            block_voiced = 0.0
        block_voiced += end - start
        block_end = max(block_end, end)
        prev_end = end

    blocks.append((block_start, block_end, block_voiced))
    return blocks


def first_sustained_speech_start(
    segments: list[tuple[float, float]],
    *,
    gap_merge_secs: float = 2.0,
    min_sustained_secs: float = 8.0,
    safety_margin_secs: float = 2.0,
) -> float | None:
    """Find the start of the first sustained voice block.

    Pure / backend-agnostic core (no audio, no VAD imports). Segments are sorted
    by start; adjacent segments separated by a gap shorter than ``gap_merge_secs``
    are merged into a single block. The first block whose accumulated voiced time
    reaches ``min_sustained_secs`` marks speech start.

    Args:
        segments: Voiced ``(start, end)`` intervals in seconds.
        gap_merge_secs: Merge segments whose inter-gap is smaller than this.
        min_sustained_secs: Minimum accumulated voiced time for a block to qualify.
        safety_margin_secs: Seconds subtracted from the block start.

    Returns:
        ``max(0.0, block_start - safety_margin_secs)`` for the first qualifying
        block, or ``None`` if no block accumulates enough voiced time.
    """
    for block_start, _block_end, block_voiced in _merge_speech_blocks(segments, gap_merge_secs):
        if block_voiced >= min_sustained_secs:
            return max(0.0, block_start - safety_margin_secs)
    return None


def last_sustained_speech_end(
    segments: list[tuple[float, float]],
    *,
    gap_merge_secs: float = VAD_GAP_MERGE_SECS,
    min_sustained_secs: float = VAD_MIN_SUSTAINED_SECS,
    end_margin_secs: float = VAD_END_MARGIN_SECS,
) -> float | None:
    """Find the end of the last sustained voice block.

    Pure / backend-agnostic core and the exact mirror of
    :func:`first_sustained_speech_start`. Segments are sorted by start; adjacent
    segments separated by a gap shorter than ``gap_merge_secs`` are merged into a
    block. Unlike the start core, this does NOT short-circuit on the first
    qualifying block — it walks every block and keeps the END of the LAST one whose
    accumulated voiced time reaches ``min_sustained_secs``.

    Args:
        segments: Voiced ``(start, end)`` intervals in seconds.
        gap_merge_secs: Merge segments whose inter-gap is smaller than this.
        min_sustained_secs: Minimum accumulated voiced time for a block to qualify.
        end_margin_secs: Seconds added after the block end (trailing breathing room).

    Returns:
        ``last_block_end + end_margin_secs`` for the last qualifying block (NOT
        clamped here — the caller clamps to the original chapter end), or ``None``
        if no block accumulates enough voiced time.
    """
    last_qual_end: float | None = None
    for _block_start, block_end, block_voiced in _merge_speech_blocks(segments, gap_merge_secs):
        if block_voiced >= min_sustained_secs:
            last_qual_end = block_end  # keep the LAST qualifying block's end

    if last_qual_end is None:
        return None
    return last_qual_end + end_margin_secs


def extract_audio_wav(
    video_path: str,
    wav_path: str,
    sample_rate: int = 16000,
    *,
    start_secs: float | None = None,
    duration_secs: float | None = None,
) -> str:
    """Extract a mono WAV from ``video_path`` via ffmpeg.

    Builds ``ffmpeg -i video -ac 1 -ar {sample_rate} -f wav wav_path`` and reuses
    :func:`compute_ffmpeg_timeout` for an adaptive subprocess timeout.

    When ``start_secs`` and/or ``duration_secs`` are given, only that slice is
    extracted via ``-ss {start}`` / ``-t {duration}`` placed BEFORE ``-i`` for
    fast input seeking. With both ``None`` the whole file is extracted
    (back-compatible behaviour).

    Args:
        video_path: Source video/clip path.
        wav_path: Destination WAV path.
        sample_rate: Output sample rate in Hz (default 16000).
        start_secs: Optional slice start offset in seconds (fast seek, before ``-i``).
        duration_secs: Optional slice duration in seconds.

    Returns:
        ``wav_path`` on success.

    Raises:
        RuntimeError: If ffmpeg exits non-zero.
    """
    cmd = ["ffmpeg", "-y"]
    if start_secs is not None:
        cmd += ["-ss", str(start_secs)]
    if duration_secs is not None:
        cmd += ["-t", str(duration_secs)]
    cmd += [
        "-i",
        video_path,
        "-ac",
        "1",
        "-ar",
        str(sample_rate),
        "-f",
        "wav",
        wav_path,
    ]
    timeout = compute_ffmpeg_timeout(duration_secs or 0)
    log.info(
        "vad.extract_audio_wav.start video_path=%s wav_path=%s start_secs=%s duration_secs=%s",
        video_path,
        wav_path,
        start_secs,
        duration_secs,
    )
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    if result.returncode != 0:
        raise RuntimeError(f"ffmpeg audio extract failed: {result.stderr}")
    log.info("vad.extract_audio_wav.done wav_path=%s", wav_path)
    return wav_path


def _webrtc_segments(audio_path: str, sample_rate: int) -> list[tuple[float, float]]:
    """Run webrtcvad over a mono WAV and return voiced ``(start, end)`` segments.

    Uses 30 ms frames at aggressiveness mode 2 (matching the validated prototype).
    Contiguous voiced frames are coalesced into segments. ``webrtcvad`` is imported
    at function scope (light C ext, no torch).
    """
    import webrtcvad  # light C extension, no torch in this path

    with wave.open(audio_path, "rb") as wav:
        if wav.getnchannels() != 1:
            log.warning("vad.webrtc.not_mono channels=%s", wav.getnchannels())
            return []
        if wav.getframerate() != sample_rate:
            log.warning(
                "vad.webrtc.sample_rate_mismatch expected=%s actual=%s",
                sample_rate,
                wav.getframerate(),
            )
            return []
        n_frames = wav.getnframes()
        pcm = wav.readframes(n_frames)

    samples = np.frombuffer(pcm, dtype=np.int16)
    vad = webrtcvad.Vad(2)
    frame_ms = 30
    frame_len = int(sample_rate * frame_ms / 1000)

    segments: list[tuple[float, float]] = []
    seg_start: float | None = None
    for i in range(0, len(samples) - frame_len, frame_len):
        frame = samples[i : i + frame_len].tobytes()
        t = i / sample_rate
        is_voiced = vad.is_speech(frame, sample_rate)
        if is_voiced and seg_start is None:
            seg_start = t
        elif not is_voiced and seg_start is not None:
            segments.append((seg_start, t))
            seg_start = None
    if seg_start is not None:
        segments.append((seg_start, len(samples) / sample_rate))
    return segments


def _silero_segments(audio_path: str, sample_rate: int) -> list[tuple[float, float]] | None:
    """Run silero-vad over a mono WAV and return voiced ``(start, end)`` segments.

    ``silero_vad`` and ``torch`` are lazy-imported here so the default webrtc path
    and CI never pull torch. Forces ``onnx=True`` and CPU execution (production NAS
    has no GPU). Returns ``None`` if the optional deps are missing.
    """
    try:
        import torch
        from silero_vad import get_speech_timestamps, load_silero_vad
    except ImportError as exc:
        log.warning("vad.silero.import_failed error=%s", exc)
        return None

    with wave.open(audio_path, "rb") as wav:
        n_frames = wav.getnframes()
        pcm = wav.readframes(n_frames)
    samples = np.frombuffer(pcm, dtype=np.int16).astype(np.float32) / 32768.0

    model = load_silero_vad(onnx=True)
    tensor = torch.from_numpy(samples).to(device="cpu")
    timestamps = get_speech_timestamps(
        tensor,
        model,
        sampling_rate=sample_rate,
        return_seconds=True,
        threshold=0.5,
        min_speech_duration_ms=250,
        min_silence_duration_ms=300,
    )
    return [(ts["start"], ts["end"]) for ts in timestamps]


def detect_speech_segments(
    audio_path: str,
    backend: str = VAD_BACKEND,
    *,
    sample_rate: int = VAD_SAMPLE_RATE,
) -> list[tuple[float, float]] | None:
    """Run the selected VAD backend once and return its voiced segments.

    Owns the single backend dispatch shared by :func:`detect_speech_bounds` (edge
    trim) and the chapter-split gap finder: both need the SAME raw voiced
    ``(start, end)`` segments, just consumed differently.

    Args:
        audio_path: Path to a mono WAV at ``sample_rate``.
        backend: ``"webrtc"`` (default, stdlib ``wave`` + numpy + ``webrtcvad``,
            no torch) or ``"silero"`` (opt-in, lazy-imports ``silero_vad``/``torch``).
        sample_rate: WAV sample rate in Hz.

    Returns:
        Voiced ``(start, end)`` segments relative to the start of ``audio_path``.
        An empty list means the backend ran and found no voice. ``None`` means the
        backend name is unknown or a backend dependency is unavailable.
    """
    if backend == "webrtc":
        return _webrtc_segments(audio_path, sample_rate)
    if backend == "silero":
        return _silero_segments(audio_path, sample_rate)
    log.warning("vad.unknown_backend backend=%s", backend)
    return None


def detect_speech_bounds(
    audio_path: str,
    backend: str = VAD_BACKEND,
    *,
    sample_rate: int = VAD_SAMPLE_RATE,
    gap_merge_secs: float = VAD_GAP_MERGE_SECS,
    min_sustained_secs: float = VAD_MIN_SUSTAINED_SECS,
    safety_margin_secs: float = VAD_SAFETY_MARGIN_SECS,
    end_margin_secs: float = VAD_END_MARGIN_SECS,
) -> tuple[float | None, float | None]:
    """Detect both sustained-speech edges from a SINGLE backend pass.

    Runs the selected backend VAD EXACTLY ONCE (via :func:`detect_speech_segments`)
    to obtain the voiced segments, then derives BOTH edges from those same
    segments: ``first_sustained_speech_start`` for the start offset and
    ``last_sustained_speech_end`` for the end offset. Both offsets are RELATIVE to
    the start of the analysed slice.

    Args:
        audio_path: Path to a mono WAV at ``sample_rate``.
        backend: ``"webrtc"`` (default) or ``"silero"``.
        sample_rate: WAV sample rate in Hz.
        gap_merge_secs: Forwarded to both cores.
        min_sustained_secs: Forwarded to both cores.
        safety_margin_secs: Forwarded to the start core (subtracted before the start).
        end_margin_secs: Forwarded to the end core (added after the end).

    Returns:
        ``(start_offset, end_offset)`` relative to the slice. Either element is
        ``None`` if no sustained speech defines that edge. ``(None, None)`` when
        the backend is unknown or a backend dependency is unavailable.
    """
    segments = detect_speech_segments(audio_path, backend, sample_rate=sample_rate)
    if segments is None:
        return None, None

    start = first_sustained_speech_start(
        segments,
        gap_merge_secs=gap_merge_secs,
        min_sustained_secs=min_sustained_secs,
        safety_margin_secs=safety_margin_secs,
    )
    end = last_sustained_speech_end(
        segments,
        gap_merge_secs=gap_merge_secs,
        min_sustained_secs=min_sustained_secs,
        end_margin_secs=end_margin_secs,
    )
    log.info(
        "vad.detect_speech_bounds.done backend=%s segments=%s speech_start=%s speech_end=%s",
        backend,
        len(segments),
        start,
        end,
    )
    return start, end


# ---------------------------------------------------------------------------
# Chapter-duration split (issue #466) — pure planning cores.
#
# These functions never touch audio or a VAD backend directly; they operate on
# already-detected ``(start, end)`` segments (or an injected ``segments_for``
# callable), which is what makes them fully unit-testable and the coverage
# anchor for the split feature, mirroring ``first_sustained_speech_start`` /
# ``last_sustained_speech_end`` above.
# ---------------------------------------------------------------------------


def find_split_gap(
    segments: list[tuple[float, float]],
    target_secs: float,
    *,
    lo_secs: float,
    hi_secs: float,
    gap_merge_secs: float = VAD_GAP_MERGE_SECS,
    min_gap_secs: float = CHAPTER_SPLIT_MIN_GAP_SECS,
) -> float | None:
    """Midpoint of the admissible inter-block gap nearest ``target_secs``.

    Walks the SAME sustained-speech blocks as :func:`_merge_speech_blocks` and
    takes the complement: for consecutive blocks, the gap is
    ``(prev_block_end, next_block_start)``. A gap is admissible when its width is
    ``>= min_gap_secs`` AND its midpoint falls inside ``[lo_secs, hi_secs]``.

    Tie-break (D1, total order so the result is deterministic): nearest midpoint
    to ``target_secs`` wins; ties broken by the WIDER gap, then by the EARLIER
    midpoint.

    Args:
        segments: Voiced ``(start, end)`` intervals, in the same time base as
            ``target_secs``/``lo_secs``/``hi_secs``.
        target_secs: The arithmetic target cut point.
        lo_secs: Lower bound of the admissible search interval (inclusive).
        hi_secs: Upper bound of the admissible search interval (inclusive).
        gap_merge_secs: Forwarded to :func:`_merge_speech_blocks`.
        min_gap_secs: Minimum gap width to count as a real boundary.

    Returns:
        The nearest admissible gap's midpoint, or ``None`` if none qualifies.
    """
    blocks = _merge_speech_blocks(segments, gap_merge_secs)
    if len(blocks) < 2:
        return None

    candidates: list[tuple[float, float]] = []  # (midpoint, width)
    for (_start_a, end_a, _voiced_a), (start_b, _end_b, _voiced_b) in zip(blocks, blocks[1:], strict=False):
        width = start_b - end_a
        if width < min_gap_secs:
            continue
        midpoint = (end_a + start_b) / 2.0
        if midpoint < lo_secs or midpoint > hi_secs:
            continue
        candidates.append((midpoint, width))

    if not candidates:
        return None

    midpoint, _width = min(candidates, key=lambda c: (abs(c[0] - target_secs), -c[1], c[0]))
    return midpoint


def plan_split_points(
    duration_secs: float,
    *,
    max_chapter_secs: float,
    min_child_secs: float,
) -> list[float]:
    """Equal-interval arithmetic targets for splitting a chapter.

    ``N = ceil(duration_secs / max_chapter_secs)`` children are needed; this
    returns their ``N - 1`` cut targets at ``duration_secs * i / N``.

    Args:
        duration_secs: The chapter's total span.
        max_chapter_secs: Maximum allowed span per child.
        min_child_secs: Minimum allowed span per child (accepted for interface
            symmetry with :func:`snap_split_points`; the equal-interval targets
            already respect it by construction — see D2).

    Returns:
        ``[]`` when ``duration_secs <= max_chapter_secs`` (the idempotence
        short-circuit — an already-compliant chapter is never touched).
    """
    del min_child_secs  # not needed for equal-interval targets (kept for symmetry)
    if duration_secs <= max_chapter_secs:
        return []
    n = math.ceil(duration_secs / max_chapter_secs)
    return [duration_secs * i / n for i in range(1, n)]


def _admissible_interval(
    prev_cut_secs: float,
    duration_secs: float,
    max_chapter_secs: float,
    min_child_secs: float,
    remaining_cuts: int,
) -> tuple[float, float]:
    """D2's per-cut admissible interval — bounds every child by MAX and MIN.

    For cut ``i`` (1-based) with ``remaining_cuts = N - i`` cuts still to place
    after this one:
        ``lo = max(prev_cut + MIN, duration - remaining_cuts * MAX)``
        ``hi = min(prev_cut + MAX, duration - remaining_cuts * MIN)``

    This guarantees, by construction, that no child ever exceeds
    ``max_chapter_secs`` and no child ever falls below ``min_child_secs`` —
    proven for both the current child (bounded by ``hi``) and the final child
    (bounded by the next cut's ``lo``).
    """
    lo = max(prev_cut_secs + min_child_secs, duration_secs - remaining_cuts * max_chapter_secs)
    hi = min(prev_cut_secs + max_chapter_secs, duration_secs - remaining_cuts * min_child_secs)
    return lo, hi


def _snap_one_cut(
    target_secs: float,
    prev_cut_secs: float,
    duration_secs: float,
    segments_for: Callable[[float, float], list[tuple[float, float]] | None],
    *,
    window_secs: float,
    widen_factor: float,
    max_chapter_secs: float,
    min_child_secs: float,
    gap_merge_secs: float,
    min_gap_secs: float,
    remaining_cuts: int,
) -> tuple[float, str]:
    """Snap ONE cut: try the base window, widen ONCE, else clamp to arithmetic."""
    lo, hi = _admissible_interval(prev_cut_secs, duration_secs, max_chapter_secs, min_child_secs, remaining_cuts)

    for half_window, reason in ((window_secs, "gap"), (window_secs * widen_factor, "gap_widened")):
        search_lo = max(lo, target_secs - half_window)
        search_hi = min(hi, target_secs + half_window)
        segments = segments_for(search_lo, search_hi)
        if segments is None:
            continue
        gap = find_split_gap(
            segments,
            target_secs,
            lo_secs=search_lo,
            hi_secs=search_hi,
            gap_merge_secs=gap_merge_secs,
            min_gap_secs=min_gap_secs,
        )
        if gap is not None:
            return gap, reason

    return min(max(target_secs, lo), hi), "arithmetic"


def snap_split_points(
    targets: list[float],
    segments_for: Callable[[float, float], list[tuple[float, float]] | None],
    duration_secs: float,
    *,
    window_secs: float,
    widen_factor: float,
    max_chapter_secs: float,
    min_child_secs: float,
    gap_merge_secs: float,
    min_gap_secs: float,
) -> list[tuple[float, str]]:
    """Sequentially snap every arithmetic target to a nearby speech gap (D2).

    Each cut is snapped in order (cut 1 first) so the admissible interval for
    cut ``i`` can depend on the PREVIOUS cut's actual (snapped) value, not its
    arithmetic target — this is what keeps the ``N`` exact and every child
    bounded by ``[min_child_secs, max_chapter_secs]`` by construction (D2).

    Args:
        targets: The ``N - 1`` arithmetic targets from :func:`plan_split_points`.
        segments_for: Injected ``(lo_secs, hi_secs) -> segments | None`` callable.
            Returns voiced segments for that slice, or ``None`` on failure
            (missing video, extraction error, VAD unavailable) — this keeps the
            planner itself pure/audio-free and fully unit-testable.
        duration_secs: The chapter's total span.
        window_secs: Initial half-window search radius around each target.
        widen_factor: Multiplier applied ONCE to ``window_secs`` when no gap is
            found in the base window.
        max_chapter_secs: Maximum allowed span per child.
        min_child_secs: Minimum allowed span per child.
        gap_merge_secs: Forwarded to :func:`find_split_gap`.
        min_gap_secs: Forwarded to :func:`find_split_gap`.

    Returns:
        One ``(cut_secs, reason)`` per target, in order. ``reason`` is one of
        ``"gap"`` (found in the base window), ``"gap_widened"`` (found after
        widening once), or ``"arithmetic"`` (fallback, clamped into the
        admissible interval).
    """
    n = len(targets) + 1
    results: list[tuple[float, str]] = []
    prev_cut = 0.0
    for i, target in enumerate(targets, start=1):
        cut, reason = _snap_one_cut(
            target,
            prev_cut,
            duration_secs,
            segments_for,
            window_secs=window_secs,
            widen_factor=widen_factor,
            max_chapter_secs=max_chapter_secs,
            min_child_secs=min_child_secs,
            gap_merge_secs=gap_merge_secs,
            min_gap_secs=min_gap_secs,
            remaining_cuts=n - i,
        )
        results.append((cut, reason))
        prev_cut = cut
    return results


def _chapter_span_ok(new_start: float, new_end: float, min_chapter_secs: float) -> bool:
    """True if a trimmed span stays correctly ordered and long enough.

    Shared guard for both edges: the end must sit after the start and the
    surviving duration must reach ``min_chapter_secs``.
    """
    return new_end > new_start and new_end - new_start >= min_chapter_secs


def _trim_one_chapter(
    chapter: dict,
    source_video: str,
    *,
    backend: str,
    gap_merge_secs: float,
    min_sustained_secs: float,
    safety_margin_secs: float,
    end_margin_secs: float,
    sample_rate: int,
    min_chapter_secs: float,
) -> None:
    """Trim a single chapter's silence on BOTH edges from one VAD pass.

    Best-effort and idempotent: extracts the chapter's audio slice ONCE, runs the
    backend ONCE via :func:`detect_speech_bounds`, and trims each edge
    independently:

    - start: ``new_start = chapter_start + start_offset`` (forward only; the
      margin and ``>= 0`` clamp live inside the start core).
    - end: ``new_end = min(chapter_start + end_offset, chapter_end_original)``
      (SUM + STRICT clamp — the end can only REDUCE, never extend past the LLM end).

    Each edge is applied only if VAD returned a value for it and the min-duration /
    ordering guards hold against the other edge's final value. On any failure /
    ``None`` offset / guard violation, that edge (or the whole chapter) is left
    unchanged. The chapter dict is mutated in place; VAD never raises out of here.
    """
    start_raw = chapter.get("start_time")
    end_raw = chapter.get("end_time")
    if not start_raw or not end_raw:
        return

    chapter_start = parse_timestamp(str(start_raw))
    chapter_end = parse_timestamp(str(end_raw))
    duration = chapter_end - chapter_start
    if duration <= 0:
        return

    wav_path: str | None = None
    try:
        fd, wav_path = tempfile.mkstemp(suffix=".wav", prefix="vad_chapter_")
        os.close(fd)
        extract_audio_wav(
            source_video,
            wav_path,
            sample_rate=sample_rate,
            start_secs=chapter_start,
            duration_secs=duration,
        )
        start_offset, end_offset = detect_speech_bounds(
            wav_path,
            backend=backend,
            sample_rate=sample_rate,
            gap_merge_secs=gap_merge_secs,
            min_sustained_secs=min_sustained_secs,
            safety_margin_secs=safety_margin_secs,
            end_margin_secs=end_margin_secs,
        )
    except Exception as exc:  # noqa: BLE001 — VAD is best-effort, never fail the task
        log.warning(
            "vad.trim_chapter.failed start_time=%s end_time=%s error=%s",
            start_raw,
            end_raw,
            exc,
        )
        return
    finally:
        if wav_path and os.path.exists(wav_path):
            os.unlink(wav_path)

    # Candidate boundaries: start can only rise, end can only fall (strict clamp).
    new_start = chapter_start + start_offset if start_offset is not None else chapter_start
    new_end = min(chapter_start + end_offset, chapter_end) if end_offset is not None else chapter_end

    # --- end edge: apply only if it REDUCES and the chapter stays usable ---
    if end_offset is not None:
        if new_end < chapter_end and _chapter_span_ok(new_start, new_end, min_chapter_secs):
            new_end_srt = format_timestamp(new_end, with_ms=True)
            log.info(
                "vad.trim_chapter.end_lowered old_end=%s new_end=%s end_offset=%s",
                end_raw,
                new_end_srt,
                end_offset,
            )
            chapter["end_time"] = new_end_srt
        else:
            new_end = chapter_end  # rejected → end stays original for the start guard
            log.warning(
                "vad.trim_chapter.end_kept end_time=%s candidate_end=%s new_start=%s min_chapter_secs=%s",
                end_raw,
                min(chapter_start + end_offset, chapter_end),
                new_start,
                min_chapter_secs,
            )

    # --- start edge: apply only if it RISES and the chapter stays usable ---
    if start_offset is not None:
        if new_start > chapter_start and _chapter_span_ok(new_start, new_end, min_chapter_secs):
            new_start_srt = format_timestamp(new_start, with_ms=True)
            log.info(
                "vad.trim_chapter.start_raised old_start=%s new_start=%s start_offset=%s",
                start_raw,
                new_start_srt,
                start_offset,
            )
            chapter["start_time"] = new_start_srt
        elif new_start > chapter_start:
            log.warning(
                "vad.trim_chapter.start_kept start_time=%s new_start=%s new_end=%s min_chapter_secs=%s",
                start_raw,
                new_start,
                new_end,
                min_chapter_secs,
            )


def trim_chapter_silence_with_vad(
    scored_chapters: dict,
    *,
    target_date: str,
    backend: str = VAD_BACKEND,
    gap_merge_secs: float = VAD_GAP_MERGE_SECS,
    min_sustained_secs: float = VAD_MIN_SUSTAINED_SECS,
    safety_margin_secs: float = VAD_SAFETY_MARGIN_SECS,
    end_margin_secs: float = VAD_END_MARGIN_SECS,
    sample_rate: int = VAD_SAMPLE_RATE,
    min_chapter_secs: float = VAD_MIN_CHAPTER_SECS,
) -> dict:
    """Trim every chapter's silence on BOTH edges via a single VAD pass.

    For each chapter in ``scored_chapters['videos'][*]['scored_chapters']`` the
    on-disk source video is located (``downloads/{target_date}/{video_id}/``), the
    audio slice ``[chapter_start, chapter_end]`` is extracted ONCE, and the backend
    is run ONCE via :func:`detect_speech_bounds` to obtain both offsets (relative to
    the slice). The new boundaries are:

    - ``new_start = chapter_start + start_offset`` (forward only — the VAD offset
      SUMS onto the start, it does not replace it).
    - ``new_end = min(chapter_start + end_offset, chapter_end_original)`` (SUM +
      STRICT clamp — the end can only REDUCE, never extend past the LLM end).

    Best-effort: a missing video, an ffmpeg failure, a VAD failure, ``None`` for an
    edge, or a result that would shrink the chapter below ``min_chapter_secs`` (or
    invert the ordering) leaves the corresponding timestamp(s) unchanged. The task
    NEVER raises on VAD errors. Deterministic and idempotent on both edges (a re-run
    over an already-trimmed chapter yields start_offset ≈ 0 and an end clamped to
    the current end → no drift).

    Args:
        scored_chapters: The scored-chapters dict (mutated in place and returned).
        target_date: ``YYYY-MM-DD`` used to locate the downloaded video.
        backend: VAD backend (``"webrtc"`` default, ``"silero"`` opt-in).
        gap_merge_secs: Forwarded to both sustained-block cores.
        min_sustained_secs: Forwarded to both sustained-block cores.
        safety_margin_secs: Forwarded to the start core (subtracted within the slice).
        end_margin_secs: Forwarded to the end core (added within the slice).
        sample_rate: WAV sample rate fed to the backend.
        min_chapter_secs: Minimum surviving chapter duration after trimming.

    Returns:
        The SAME ``scored_chapters`` dict with corrected ``start_time`` and
        ``end_time`` fields.
    """
    if not scored_chapters or not scored_chapters.get("videos"):
        return scored_chapters

    for video in scored_chapters["videos"]:
        video_id = video.get("video_id")
        chapters = video.get("scored_chapters") or []
        if not video_id or not chapters:
            continue

        source_video = _find_source_video(target_date, str(video_id))
        if not source_video:
            log.warning(
                "vad.trim.video_not_found video_id=%s target_date=%s chapters=%s",
                video_id,
                target_date,
                len(chapters),
            )
            continue  # best-effort: keep all chapters of this video unchanged

        for chapter in chapters:
            _trim_one_chapter(
                chapter,
                source_video,
                backend=backend,
                gap_merge_secs=gap_merge_secs,
                min_sustained_secs=min_sustained_secs,
                safety_margin_secs=safety_margin_secs,
                end_margin_secs=end_margin_secs,
                sample_rate=sample_rate,
                min_chapter_secs=min_chapter_secs,
            )

    return scored_chapters


def trim_turn_silence_with_vad(
    video_path: str,
    *,
    backend: str = VAD_BACKEND,
    sample_rate: int = VAD_SAMPLE_RATE,
    gap_merge_secs: float = VAD_GAP_MERGE_SECS,
    min_sustained_secs: float = VAD_MIN_SUSTAINED_SECS,
    safety_margin_secs: float = VAD_SAFETY_MARGIN_SECS,
    end_margin_secs: float = VAD_END_MARGIN_SECS,
    min_chapter_secs: float = VAD_MIN_CHAPTER_SECS,
) -> tuple[float, float]:
    """Trim leading and trailing silence from a speaker-turn MP4 in place.

    Detects sustained-speech boundaries via VAD, applies margins, and rewrites
    ``video_path`` atomically with ffmpeg stream-copy. Returns the trimmed
    offsets ``(trim_start_secs, trim_end_secs)`` so that the caller can adjust
    the SRT window accordingly.

    Args:
        video_path: Absolute path to the speaker-turn MP4. Modified in place on success.
        backend: VAD backend (``"webrtc"`` default).
        sample_rate: WAV sample rate for the VAD backend.
        gap_merge_secs: Merge voiced segments separated by gaps shorter than this.
        min_sustained_secs: Minimum accumulated voiced time for a block to qualify.
        safety_margin_secs: Seconds before the detected speech start (start margin).
        end_margin_secs: Seconds after the detected speech end (end margin).
        min_chapter_secs: Minimum surviving duration after trim.

    Returns:
        ``(trim_start_secs, trim_end_secs)`` — both 0.0 when the file is untouched.
        NEVER raises; any internal failure returns ``(0.0, 0.0)`` with the file intact.
    """
    if not VAD_ENABLED:
        log.info("vad.trim_turn.skipped reason=disabled video_path=%s", video_path)
        return (0.0, 0.0)

    wav_path: str | None = None
    tmp_video: str | None = None
    try:
        log.info("vad.trim_turn.start video_path=%s", video_path)

        # Step 1: Extract whole-file audio WAV for VAD analysis.
        fd, wav_path = tempfile.mkstemp(suffix=".wav", prefix="vad_turn_")
        os.close(fd)
        extract_audio_wav(video_path, wav_path, sample_rate=sample_rate)

        # Step 2: Detect speech bounds (relative offsets from file start).
        speech_start, speech_end = detect_speech_bounds(
            wav_path,
            backend=backend,
            sample_rate=sample_rate,
            gap_merge_secs=gap_merge_secs,
            min_sustained_secs=min_sustained_secs,
            safety_margin_secs=safety_margin_secs,
            end_margin_secs=end_margin_secs,
        )

        # Step 3: Probe file duration for tail-trim math.
        duration = _get_source_duration(video_path)

        # Derive trim offsets.
        # trim_start = seconds to cut from the start of the file.
        # trim_end = seconds to cut from the end of the file.
        trim_start = speech_start if speech_start is not None else 0.0
        if duration is not None and speech_end is not None:
            trim_end = max(0.0, duration - speech_end)
        else:
            trim_end = 0.0  # no tail trim when duration unknown

        # Step 4: Epsilon guard — skip rewrite for negligible trims.
        if trim_start < VAD_TURN_TRIM_EPSILON_SECS and trim_end < VAD_TURN_TRIM_EPSILON_SECS:
            log.info(
                "vad.trim_turn.skipped reason=below_eps trim_start=%.3f trim_end=%.3f video_path=%s",
                trim_start,
                trim_end,
                video_path,
            )
            return (0.0, 0.0)

        # Step 5: Span guard — ensure surviving duration meets minimum.
        if duration is not None:
            new_start_abs = trim_start
            new_end_abs = duration - trim_end
        else:
            # Duration unknown: only start trim applied; end is unbounded.
            # Use a synthetic pass for span guard (start-only trim is always OK if > eps).
            new_start_abs = trim_start
            new_end_abs = new_start_abs + min_chapter_secs + 1.0

        if not _chapter_span_ok(new_start_abs, new_end_abs, min_chapter_secs):
            log.info(
                "vad.trim_turn.skipped reason=span_guard trim_start=%.3f trim_end=%.3f"
                " new_start=%.3f new_end=%.3f video_path=%s",
                trim_start,
                trim_end,
                new_start_abs,
                new_end_abs,
                video_path,
            )
            return (0.0, 0.0)

        # Step 6: Compute ffmpeg -ss/-to values. Input-side seek: -ss/-to BEFORE -i.
        ss = trim_start
        to = (duration - trim_end) if duration is not None else None

        # Step 7: Build temp file path in same directory as video_path.
        video_dir = os.path.dirname(video_path)
        tmp_fd, tmp_video = tempfile.mkstemp(
            suffix=".vadtrim.tmp.mp4",
            dir=video_dir,
        )
        os.close(tmp_fd)

        # Step 8: Run ffmpeg with input-side seek (-ss/-to before -i) and stream copy.
        from congress_videos.modules.video_splitter import compute_ffmpeg_timeout

        timeout = compute_ffmpeg_timeout(0)
        cmd = ["ffmpeg", "-y", "-ss", str(ss)]
        if to is not None:
            cmd += ["-to", str(to)]
        cmd += ["-i", video_path, "-c", "copy", tmp_video]

        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            log.warning(
                "vad.trim_turn.ffmpeg_failed rc=%d video_path=%s stderr=%s",
                result.returncode,
                video_path,
                result.stderr[:200],
            )
            return (0.0, 0.0)

        # Step 9: Atomic replace.
        os.replace(tmp_video, video_path)
        tmp_video = None  # replaced — no cleanup needed

        log.info(
            "vad.trim_turn.applied trim_start=%.3f trim_end=%.3f new_duration=%.3f video_path=%s",
            trim_start,
            trim_end,
            (to - ss) if to is not None else 0.0,
            video_path,
        )
        return (trim_start, trim_end)

    except Exception as exc:  # noqa: BLE001 — VAD trim is best-effort, never block preparation
        log.warning(
            "vad.trim_turn.skipped reason=error video_path=%s error=%s",
            video_path,
            exc,
        )
        return (0.0, 0.0)
    finally:
        if wav_path and os.path.exists(wav_path):
            os.unlink(wav_path)
        if tmp_video and os.path.exists(tmp_video):
            try:
                os.unlink(tmp_video)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Chapter-duration split (issue #466) — audio wrapper + public entry point.
# ---------------------------------------------------------------------------


def _timeline_within(timeline: list[dict] | None, lo_secs: float, hi_secs: float) -> list[dict]:
    """Keep timeline moments whose ABSOLUTE ``time`` falls inside ``[lo_secs, hi_secs]``.

    Local counterpart to
    ``congress_videos.modules.youtube.download._filter_timeline_by_range`` (D5) —
    NOT an import of that private symbol: importing it would pull the heavy
    youtube package (yt-dlp/OpenAI) into a module the turn-prepare DAG also
    imports, and this caller already holds numeric bounds so it needs neither
    the SRT re-parse nor that helper's unparseable-bounds fallback.
    ``build_youtube_chapters_block`` (youtube_ai.py) short-circuits cleanly on
    an empty timeline, so an empty result here degrades safely.
    """
    if not timeline:
        return []
    kept: list[dict] = []
    for moment in timeline:
        if not isinstance(moment, dict):
            continue
        raw_time = moment.get("time")
        if not raw_time:
            continue
        try:
            secs = parse_timestamp(str(raw_time))
        except (ValueError, TypeError):
            continue
        if lo_secs <= secs <= hi_secs:
            kept.append(moment)
    return kept


def _segments_for_cut(
    source_video: str | None,
    chapter_start: float,
    *,
    backend: str,
    sample_rate: int,
) -> Callable[[float, float], list[tuple[float, float]] | None]:
    """Build the ``segments_for`` callable injected into :func:`snap_split_points`.

    Extracts ONE candidate-cut slice per call (not the whole chapter) and runs
    the VAD backend once over it. Never raises — any failure returns ``None``,
    driving that cut's D4 arithmetic fallback; sibling cuts may still snap.
    """

    def segments_for(lo_secs: float, hi_secs: float) -> list[tuple[float, float]] | None:
        if source_video is None:
            return None
        wav_path: str | None = None
        try:
            fd, wav_path = tempfile.mkstemp(suffix=".wav", prefix="vad_split_")
            os.close(fd)
            extract_audio_wav(
                source_video,
                wav_path,
                sample_rate=sample_rate,
                start_secs=chapter_start + lo_secs,
                duration_secs=hi_secs - lo_secs,
            )
            slice_segments = detect_speech_segments(wav_path, backend, sample_rate=sample_rate)
        except Exception as exc:  # noqa: BLE001 — best-effort per-cut, D4 falls back to arithmetic
            log.warning(
                "chapter_split.cut_extract_failed lo_secs=%.2f hi_secs=%.2f error=%s",
                lo_secs,
                hi_secs,
                exc,
            )
            return None
        finally:
            if wav_path and os.path.exists(wav_path):
                os.unlink(wav_path)
        if slice_segments is None:
            log.warning("chapter_split.cut_vad_unavailable lo_secs=%.2f hi_secs=%.2f", lo_secs, hi_secs)
            return None
        # Segments come back slice-relative; rebase to chapter-relative (D2/D1 share this basis).
        return [(start + lo_secs, end + lo_secs) for start, end in slice_segments]

    return segments_for


def _split_one_chapter(
    chapter: dict,
    source_video: str | None,
    *,
    max_chapter_secs: float,
    min_child_secs: float,
    window_secs: float,
    widen_factor: float,
    gap_merge_secs: float,
    min_gap_secs: float,
    backend: str = VAD_BACKEND,
    sample_rate: int = VAD_SAMPLE_RATE,
) -> list[dict]:
    """Split ONE chapter into contiguous children, or return it unchanged.

    ``source_video=None`` forces every cut to the arithmetic fallback (D4).
    Idempotence: :func:`plan_split_points` returns ``[]`` under threshold, so
    the chapter comes back as the SAME dict object — no copy, no re-suffix.
    """
    start_raw = chapter.get("start_time")
    end_raw = chapter.get("end_time")
    if not start_raw or not end_raw:
        return [chapter]

    chapter_start = parse_timestamp(str(start_raw))
    chapter_end = parse_timestamp(str(end_raw))
    duration = chapter_end - chapter_start
    if duration <= 0:
        return [chapter]

    targets = plan_split_points(duration, max_chapter_secs=max_chapter_secs, min_child_secs=min_child_secs)
    if not targets:
        return [chapter]

    cuts = snap_split_points(
        targets,
        _segments_for_cut(source_video, chapter_start, backend=backend, sample_rate=sample_rate),
        duration,
        window_secs=window_secs,
        widen_factor=widen_factor,
        max_chapter_secs=max_chapter_secs,
        min_child_secs=min_child_secs,
        gap_merge_secs=gap_merge_secs,
        min_gap_secs=min_gap_secs,
    )
    for cut_secs, reason in cuts:
        if reason == "arithmetic":
            log.warning(
                "chapter_split.cut_fallback reason=no_gap cut_secs=%.2f start_time=%s",
                cut_secs,
                start_raw,
            )
        else:
            log.info(
                "chapter_split.cut_snapped reason=%s cut_secs=%.2f start_time=%s",
                reason,
                cut_secs,
                start_raw,
            )

    boundaries = [0.0, *(cut_secs for cut_secs, _reason in cuts), duration]
    total_children = len(boundaries) - 1
    parent_title = chapter.get("title", "Untitled Chapter")

    children: list[dict] = []
    for idx in range(total_children):
        lo, hi = boundaries[idx], boundaries[idx + 1]
        child = copy.deepcopy(chapter)
        child["start_time"] = format_timestamp(chapter_start + lo, with_ms=True)
        child["end_time"] = format_timestamp(chapter_start + hi, with_ms=True)
        child["duration_minutes"] = round((hi - lo) / 60.0, 2)
        child["title"] = f"{parent_title} (Parte {idx + 1}/{total_children})"
        child["timeline"] = _timeline_within(chapter.get("timeline"), chapter_start + lo, chapter_start + hi)
        children.append(child)
    return children
