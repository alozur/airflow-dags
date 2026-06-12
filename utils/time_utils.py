"""Unified timestamp parsing and formatting utilities.

Provides a single source of truth for SRT/chapter timestamp handling,
replacing the three duplicate implementations that existed in
video_splitter.py, srt_helpers.py, and ai_chapter_analyzer.py.
"""

from __future__ import annotations


def parse_timestamp(ts: str) -> float:
    """Convert a timestamp string to total seconds as a float.

    Accepts the following formats:
    - ``HH:MM:SS``
    - ``HH:MM:SS,mmm``  (SRT comma separator)
    - ``HH:MM:SS.mmm``  (dot separator)

    Args:
        ts: Timestamp string in one of the supported formats.

    Returns:
        Total seconds as a float, preserving millisecond precision.

    Raises:
        ValueError: If the input cannot be parsed as a valid timestamp.

    Examples:
        >>> parse_timestamp("01:23:45")
        5025.0
        >>> parse_timestamp("00:01:30,500")
        90.5
        >>> parse_timestamp("00:00:05.250")
        5.25
    """
    if not isinstance(ts, str):
        raise ValueError(f"Expected str, got {type(ts).__name__}: {ts!r}")

    ts = ts.strip()

    # Normalise separator: replace comma with dot before splitting
    normalised = ts.replace(",", ".")

    # Split off optional milliseconds
    if "." in normalised:
        time_part, ms_str = normalised.rsplit(".", 1)
        try:
            ms = int(ms_str) / (10 ** len(ms_str))
        except ValueError:
            raise ValueError(f"Invalid milliseconds in timestamp: {ts!r}")
    else:
        time_part = normalised
        ms = 0.0

    parts = time_part.split(":")
    if len(parts) != 3:
        raise ValueError(
            f"Timestamp must be in HH:MM:SS[,mmm] format, got: {ts!r}"
        )

    try:
        hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
    except ValueError:
        raise ValueError(f"Non-integer component in timestamp: {ts!r}")

    if minutes >= 60 or seconds >= 60:
        raise ValueError(
            f"Minutes/seconds out of range (0-59) in timestamp: {ts!r}"
        )

    return float(hours * 3600 + minutes * 60 + seconds) + ms


def format_timestamp(seconds: float, *, with_ms: bool = True) -> str:
    """Format total seconds as a timestamp string.

    Args:
        seconds: Total seconds (non-negative float).
        with_ms: When True (default), include milliseconds as
            ``HH:MM:SS,mmm``.  When False, return ``HH:MM:SS``.

    Returns:
        Formatted timestamp string.

    Examples:
        >>> format_timestamp(5025.0)
        '01:23:45,000'
        >>> format_timestamp(90.5, with_ms=False)
        '00:01:30'
    """
    total_ms = round(seconds * 1000)
    ms = total_ms % 1000
    total_s = total_ms // 1000
    h = total_s // 3600
    m = (total_s % 3600) // 60
    s = total_s % 60

    if with_ms:
        return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"
    return f"{h:02d}:{m:02d}:{s:02d}"
