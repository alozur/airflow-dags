"""SRT file helpers for AI-assisted pre-trim window selection."""

import logging
import os
import re
from typing import Optional

from congress_videos.config.paths import DOWNLOADS_DIR, PROJECT_DATA_DIR
from utils.ai_helpers import generate_json_completion

logger = logging.getLogger(__name__)

_TIMESTAMP_RE = re.compile(r'\d{2}:\d{2}:\d{2}')
_TIMESTAMP_STRIP_RE = re.compile(r',\d+')


def find_srt_for_chapter(
    video_id: str,
    chapter_id: int,
    session_date: Optional[str] = None,
) -> Optional[str]:
    """
    Try common SRT path patterns, return first existing path or None.

    Probes:
      1. data/congress_videos/{video_id}/srt_files/
      2. downloads/{session_date}/{video_id}/srt_files/  (if session_date provided)
      3. downloads/ (date-agnostic search when session_date is None)
    """
    srt_filename = f"{video_id}.srt"

    candidates = [
        os.path.join(PROJECT_DATA_DIR, video_id, "srt_files", srt_filename),
    ]

    if session_date:
        candidates.append(
            os.path.join(DOWNLOADS_DIR, session_date, video_id, "srt_files", srt_filename)
        )
    else:
        # Search across all date folders when session_date is not known at call time
        if os.path.isdir(DOWNLOADS_DIR):
            for date_folder in os.listdir(DOWNLOADS_DIR):
                candidate = os.path.join(
                    DOWNLOADS_DIR, date_folder, video_id, "srt_files", srt_filename
                )
                candidates.append(candidate)

    for path in candidates:
        if os.path.exists(path):
            return path

    return None


def parse_srt_to_text(srt_path: str, max_chars: int = 8000) -> str:
    """
    Parse SRT file and return text with timestamps for AI analysis.

    Format: "00:01:23 --> 00:01:45\\nHello world\\n\\n00:01:46 --> 00:02:01\\nNext line\\n\\n..."
    Truncates to max_chars to stay within token limits.
    """
    try:
        with open(srt_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        logger.warning("Failed to read SRT file %s: %s", srt_path, e)
        return ""

    # SRT blocks are separated by blank lines; each block: index, timestamp, text
    blocks = re.split(r"\n\s*\n", content.strip())
    parts = []

    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue

        timestamp_line = None
        text_lines = []

        for line in lines:
            # Detect SRT timestamp line: HH:MM:SS,mmm --> HH:MM:SS,mmm
            if _TIMESTAMP_RE.match(line) and "-->" in line:
                # Drop milliseconds — keeps format readable for the AI prompt
                timestamp_line = _TIMESTAMP_STRIP_RE.sub("", line)
            elif timestamp_line is not None:
                text_lines.append(line)

        if timestamp_line and text_lines:
            parts.append(f"{timestamp_line}\n{' '.join(text_lines)}")

    result = "\n\n".join(parts)
    return result[:max_chars]


def select_pretrim_window(
    srt_text: str,
    target_secs: int = 360,
) -> Optional[dict]:
    """
    Use AI to identify the most engaging window in the SRT content.

    Returns {"start_seconds": float, "end_seconds": float} or None on failure.
    Caller falls back to the full clip when None is returned.
    """
    system_prompt = (
        "You are a video editor assistant. Given a transcript with timestamps from a "
        "Spanish parliamentary debate, identify the single most engaging and self-contained "
        "continuous window suitable for a YouTube Short. "
        "Return ONLY a JSON object with keys start_seconds and end_seconds (floats). "
        "No explanation, no markdown fences."
    )
    user_prompt = (
        f"{srt_text}\n\n"
        f"Find the most engaging continuous {target_secs}-second window. "
        f'Return JSON: {{"start_seconds": <float>, "end_seconds": <float>}}'
    )

    result = generate_json_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model="gpt-4o-mini",
        max_tokens=100,
    )

    if result["error"] or result["data"] is None:
        logger.warning(
            "select_pretrim_window: AI call failed or returned no data: %s", result["error"]
        )
        return None

    data = result["data"]

    try:
        start = float(data["start_seconds"])
        end = float(data["end_seconds"])
    except (KeyError, TypeError, ValueError) as e:
        logger.warning("select_pretrim_window: invalid JSON shape %s — %s", data, e)
        return None

    duration = end - start
    if start < 0 or end <= start or abs(duration - target_secs) > 30:
        logger.warning(
            "select_pretrim_window: window out of bounds (start=%.1f end=%.1f target=%ds)",
            start,
            end,
            target_secs,
        )
        return None

    return {"start_seconds": start, "end_seconds": end}
