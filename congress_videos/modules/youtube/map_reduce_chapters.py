"""Map-reduce chapter identification for very long SRT transcripts (#8, #15).

When a per-chunk SRT exceeds the LLM's safe context window, sending it whole
forces the model to "decide blind" on a truncated view. This module instead:

  1. **Map** — split the SRT into overlapping windows (never mid-block) and run
     per-window chapter identification independently (:func:`map_chapters`).
  2. **Reduce** — consolidate the per-window chapter lists into a single sorted
     list (:func:`reduce_chapters`). Chapters that straddle a window seam are
     merged by a DETERMINISTIC overlap-union rule (#15, :func:`_resolve_seams`),
     then the existing >50%-overlap dedup pass (#6) removes the rest.

Below the windowing threshold the SRT is processed in a single pass, so behaviour
is identical to the original single-call path.

Design contract (sdd/chunking-v1-improvements/design):
  - ``window_chars`` default 80_000, ``overlap_pct`` default 0.12.
  - ``<= window_chars`` → single window, single LLM call.
  - Seam resolution is deterministic (NO extra LLM call) and runs BEFORE #6 dedup.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Callable

from utils.time_utils import parse_timestamp

logger = logging.getLogger(__name__)

# Default sliding-window parameters (chars). ~4 chars/token ⇒ 80k chars ≈ 20k
# tokens, comfortably inside gpt-4o-mini's 128k context with room for the prompt.
DEFAULT_WINDOW_CHARS = 80_000
DEFAULT_OVERLAP_PCT = 0.12

# Two chapters from adjacent windows whose ranges overlap by more than this
# fraction of the shorter chapter are treated as the SAME chapter and merged
# (union of bounds). Mirrors the #6 dedup threshold for consistency.
SEAM_OVERLAP_FRACTION = 0.5

# Split SRT text on blank-line block boundaries (never split mid-block).
_BLOCK_SEPARATOR = re.compile(r"\n\s*\n")


@dataclass(frozen=True)
class Window:
    """A contiguous slice of SRT text aligned to block boundaries.

    Attributes:
        index: Zero-based position of the window in the sequence.
        text: The SRT text for this window (whole blocks only).
        char_start: Offset of the window's first char in the original SRT.
        char_end: Offset one past the window's last char in the original SRT.
    """

    index: int
    text: str
    char_start: int
    char_end: int


def _split_into_blocks(srt_text: str) -> list[str]:
    """Split SRT text into non-empty blocks on blank-line boundaries."""
    return [b for b in _BLOCK_SEPARATOR.split(srt_text.strip()) if b.strip()]


def window_srt(
    srt_text: str,
    window_chars: int = DEFAULT_WINDOW_CHARS,
    overlap_pct: float = DEFAULT_OVERLAP_PCT,
) -> list[Window]:
    """Split an SRT transcript into overlapping windows on block boundaries.

    Windows never split an SRT block mid-way. Each window (after the first)
    starts ``overlap_pct * window_chars`` characters *before* the previous
    window ended, so a chapter that straddles a seam is visible to both
    adjacent windows and can be reconciled in :func:`reduce_chapters`.

    Args:
        srt_text: The full SRT transcript.
        window_chars: Target maximum window size in characters.
        overlap_pct: Fraction of ``window_chars`` to overlap between adjacent
            windows (0.0–1.0).

    Returns:
        A list of :class:`Window`. When ``len(srt_text) <= window_chars`` a
        single window covering the whole transcript is returned (single-pass).
    """
    if window_chars <= 0:
        raise ValueError("window_chars must be positive")
    if not 0.0 <= overlap_pct < 1.0:
        raise ValueError("overlap_pct must be in [0.0, 1.0)")

    stripped = srt_text.strip()
    if len(stripped) <= window_chars:
        # Single-pass: boundary `==` and "window larger than SRT" both land here.
        return [Window(index=0, text=stripped, char_start=0, char_end=len(stripped))]

    blocks = _split_into_blocks(stripped)
    if not blocks:
        return [Window(index=0, text=stripped, char_start=0, char_end=len(stripped))]

    separator = "\n\n"
    block_lengths = [len(b) for b in blocks]

    overlap_chars = int(window_chars * overlap_pct)
    windows: list[Window] = []
    start_block = 0
    win_index = 0
    n_blocks = len(blocks)

    while start_block < n_blocks:
        # Greedily accumulate whole blocks until adding the next would exceed
        # window_chars (but always include at least one block).
        end_block = start_block
        size = block_lengths[start_block]
        while end_block + 1 < n_blocks:
            next_size = size + len(separator) + block_lengths[end_block + 1]
            if next_size > window_chars:
                break
            end_block += 1
            size = next_size

        win_blocks = blocks[start_block:end_block + 1]
        text = separator.join(win_blocks)
        char_start = sum(block_lengths[:start_block]) + len(separator) * start_block
        windows.append(
            Window(
                index=win_index,
                text=text,
                char_start=char_start,
                char_end=char_start + len(text),
            )
        )
        win_index += 1

        if end_block + 1 >= n_blocks:
            break

        # Step the next window back by ~overlap_chars worth of whole blocks so
        # adjacent windows share content around the seam.
        next_start = end_block + 1
        if overlap_chars > 0:
            back = 0
            acc = 0
            while next_start - back - 1 > start_block:
                acc += len(separator) + block_lengths[next_start - back - 1]
                if acc >= overlap_chars:
                    break
                back += 1
            next_start = max(start_block + 1, next_start - back)
        start_block = next_start

    logger.info(
        "window_srt: split %d chars into %d windows (window_chars=%d, overlap_pct=%.2f)",
        len(stripped), len(windows), window_chars, overlap_pct,
    )
    return windows


def map_chapters(
    windows: list[Window],
    identify_fn: Callable[[str], list[dict]],
) -> list[list[dict]]:
    """Run per-window chapter identification (the *map* phase).

    Args:
        windows: Windows produced by :func:`window_srt`.
        identify_fn: Callable that takes a window's SRT text and returns a list
            of chapter dicts (each with ``start_time``/``end_time``). Injected so
            the LLM call can be mocked in tests.

    Returns:
        One chapter list per window, in window order. A window that yields no
        chapters contributes an empty list (never dropped, never raises).
    """
    results: list[list[dict]] = []
    for window in windows:
        try:
            chapters = identify_fn(window.text) or []
        except Exception:  # noqa: BLE001 — one bad window must not sink the rest
            logger.warning(
                "map_chapters: window %d failed identification; treating as empty",
                window.index, exc_info=True,
            )
            chapters = []
        results.append(list(chapters))
    return results


def _secs(value: str | float) -> float:
    """Parse a timestamp (str) or pass through a numeric value, in seconds."""
    if isinstance(value, (int, float)):
        return float(value)
    return parse_timestamp(value)


def _resolve_seams(
    window_results: list[list[dict]],
    seam_overlap_fraction: float = SEAM_OVERLAP_FRACTION,
) -> list[dict]:
    """Merge chapters that straddle window seams (#15) — deterministic union.

    Flattens all per-window chapters and greedily merges any pair whose time
    ranges overlap by more than ``seam_overlap_fraction`` of the shorter
    chapter. Merging takes the union of bounds (min start, max end) — the
    "wider boundary wins" rule — and keeps the longer title. No LLM call.

    This runs BEFORE the #6 overlap-dedup pass: it reconciles the SAME chapter
    seen twice across an overlap region, whereas #6 removes genuinely distinct
    chapters that happen to overlap heavily.

    Args:
        window_results: Per-window chapter lists from :func:`map_chapters`.
        seam_overlap_fraction: Overlap threshold (fraction of shorter chapter).

    Returns:
        A list of chapters sorted by start time, with seam duplicates merged.
    """
    chapters: list[dict] = [ch for window in window_results for ch in window]
    if len(chapters) <= 1:
        return list(chapters)

    chapters.sort(key=lambda ch: _secs(ch.get("start_time", 0.0)))

    merged: list[dict] = []
    for chapter in chapters:
        if not merged:
            merged.append(dict(chapter))
            continue

        prev = merged[-1]
        p_start, p_end = _secs(prev.get("start_time", 0.0)), _secs(prev.get("end_time", 0.0))
        c_start, c_end = _secs(chapter.get("start_time", 0.0)), _secs(chapter.get("end_time", 0.0))

        overlap = max(0.0, min(p_end, c_end) - max(p_start, c_start))
        shorter = min(p_end - p_start, c_end - c_start)

        if shorter > 0 and overlap / shorter > seam_overlap_fraction:
            # Same chapter seen across the seam → union of bounds, longer title.
            union_start = prev if p_start <= c_start else chapter
            union_end = prev if p_end >= c_end else chapter
            p_title = prev.get("title", "") or ""
            c_title = chapter.get("title", "") or ""
            merged[-1] = {
                **prev,
                "start_time": union_start.get("start_time"),
                "end_time": union_end.get("end_time"),
                "title": p_title if len(p_title) >= len(c_title) else c_title,
            }
        else:
            merged.append(dict(chapter))

    return merged


def reduce_chapters(window_results: list[list[dict]]) -> list[dict]:
    """Consolidate per-window chapter lists into a single sorted list.

    Pipeline: seam resolution (#15) → overlap dedup (#6) → sort by start.

    Args:
        window_results: Per-window chapter lists from :func:`map_chapters`.

    Returns:
        The final consolidated, de-duplicated, start-sorted chapter list.
    """
    # Import lazily to avoid a circular import (download imports this module).
    from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

    seam_resolved = _resolve_seams(window_results)
    deduped = _dedup_overlapping_chapters(seam_resolved)
    deduped.sort(key=lambda ch: _secs(ch.get("start_time", 0.0)))
    return deduped


def map_reduce_identify_chapters(
    srt_content: str,
    identify_fn: Callable[[str], list[dict]],
    window_chars: int = DEFAULT_WINDOW_CHARS,
    overlap_pct: float = DEFAULT_OVERLAP_PCT,
) -> list[dict]:
    """Identify interesting chapters across an arbitrarily long SRT (#8 + #15).

    When ``len(srt_content) <= window_chars`` this is a single ``identify_fn``
    call (no windowing) and the result is returned unchanged. Otherwise the SRT
    is windowed with overlap, identified per window (map), and consolidated with
    seam resolution + dedup (reduce).

    Args:
        srt_content: The full per-chunk SRT transcript.
        identify_fn: Callable taking SRT text → chapter list. Injected so the
            caller wires in the cached LLM call (and tests can mock it).
        window_chars: Target window size in characters.
        overlap_pct: Overlap fraction between adjacent windows.

    Returns:
        The consolidated list of interesting chapters.
    """
    windows = window_srt(srt_content, window_chars=window_chars, overlap_pct=overlap_pct)

    if len(windows) == 1:
        # Single-pass path: identical to the original single LLM call.
        return list(identify_fn(windows[0].text) or [])

    logger.info(
        "map_reduce_identify_chapters: %d chars → %d windows",
        len(srt_content), len(windows),
    )
    window_results = map_chapters(windows, identify_fn)
    return reduce_chapters(window_results)
