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
_SRT_TIMESTAMP_ARROW_RE = re.compile(
    r'(\d{2}:\d{2}:\d{2},\d+)\s*-->\s*(\d{2}:\d{2}:\d{2},\d+)'
)


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
    srt_filenames = [f"{video_id}_merged.srt", f"{video_id}.srt"]

    candidates = []
    for name in srt_filenames:
        candidates.append(os.path.join(PROJECT_DATA_DIR, video_id, "srt_files", name))

    if session_date:
        for name in srt_filenames:
            candidates.append(
                os.path.join(DOWNLOADS_DIR, session_date, video_id, "srt_files", name)
            )
    else:
        if os.path.isdir(DOWNLOADS_DIR):
            for date_folder in os.listdir(DOWNLOADS_DIR):
                for name in srt_filenames:
                    candidates.append(
                        os.path.join(DOWNLOADS_DIR, date_folder, video_id, "srt_files", name)
                    )

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

    blocks = re.split(r"\n\s*\n", content.strip())
    parts = []

    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue

        timestamp_line = None
        text_lines = []

        for line in lines:
            if _TIMESTAMP_RE.match(line) and "-->" in line:
                timestamp_line = _TIMESTAMP_STRIP_RE.sub("", line)
            elif timestamp_line is not None:
                text_lines.append(line)

        if timestamp_line and text_lines:
            parts.append(f"{timestamp_line}\n{' '.join(text_lines)}")

    result = "\n\n".join(parts)
    return result[:max_chars]


def _srt_timestamp_to_seconds(ts: str) -> float:
    """Convert 'HH:MM:SS,mmm' to float seconds."""
    ts = ts.strip()
    time_part, ms_part = ts.split(",")
    h, m, s = time_part.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s) + int(ms_part) / 1000


def _parse_srt_blocks(srt_path: str) -> list[dict]:
    """Parse SRT into list of {start_secs, end_secs, text} dicts."""
    try:
        with open(srt_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()
    except OSError as e:
        logger.warning("Failed to read SRT for block parsing %s: %s", srt_path, e)
        return []

    blocks = re.split(r"\n\s*\n", content.strip())
    result = []

    for block in blocks:
        lines = block.strip().splitlines()
        if len(lines) < 2:
            continue

        timestamp_line = None
        text_lines = []

        for line in lines:
            if _SRT_TIMESTAMP_ARROW_RE.match(line.strip()):
                timestamp_line = line.strip()
            elif timestamp_line is not None:
                text_lines.append(line)

        if not timestamp_line or not text_lines:
            continue

        m = _SRT_TIMESTAMP_ARROW_RE.match(timestamp_line)
        if not m:
            continue

        try:
            start_secs = _srt_timestamp_to_seconds(m.group(1))
            end_secs = _srt_timestamp_to_seconds(m.group(2))
        except (ValueError, IndexError):
            continue

        result.append({
            "start_secs": start_secs,
            "end_secs": end_secs,
            "text": " ".join(text_lines),
        })

    return result


def _find_phrase_in_blocks(blocks: list[dict], phrase: str) -> Optional[dict]:
    """
    Find the first SRT block whose text contains the phrase.

    Tries progressively shorter prefixes (8 words → 4 words) to tolerate
    minor paraphrasing or truncation by the AI.
    Returns the matching block dict or None.
    """
    for n_words in (8, 4):
        words = phrase.lower().split()[:n_words]
        if len(words) < 2:
            continue
        search = " ".join(words)
        for block in blocks:
            normalized = " ".join(block["text"].lower().split())
            if search in normalized:
                return block
    return None


def select_pretrim_window(
    srt_path: str,
    target_secs: int = 360,
) -> Optional[dict]:
    """
    Use AI to identify the most engaging window in the SRT content.

    The AI returns start_phrase and end_phrase — exact text copied from the
    transcript. Actual timestamps are then resolved by searching the SRT blocks,
    so seconds always come from the source file, never from model arithmetic.

    Returns {"start_seconds": float, "end_seconds": float} or None on failure.
    Caller falls back to the full clip when None is returned.
    """
    srt_text = parse_srt_to_text(srt_path)
    if not srt_text:
        logger.warning("select_pretrim_window: empty SRT text from %s", srt_path)
        return None

    blocks = _parse_srt_blocks(srt_path)
    if not blocks:
        logger.warning("select_pretrim_window: no SRT blocks parsed from %s", srt_path)
        return None

    system_prompt = (
        "You are a video editor assistant. Given a transcript with timestamps from a "
        "Spanish parliamentary debate, identify the single most engaging and self-contained "
        "continuous window suitable for a YouTube Short. "
        "Return ONLY a JSON object with keys start_phrase and end_phrase — copy the exact "
        "words from the transcript where the window should start and end. "
        "No explanation, no markdown fences."
    )
    user_prompt = (
        f"{srt_text}\n\n"
        f"Find the most engaging continuous window of approximately {target_secs} seconds. "
        f"Return the opening words of the first sentence (start_phrase) and the opening words "
        f"of the last sentence (end_phrase), copied verbatim from the transcript above.\n"
        f'Return JSON: {{"start_phrase": "<exact text from transcript>", "end_phrase": "<exact text from transcript>"}}'
    )

    result = generate_json_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model="gpt-4o-mini",
        max_tokens=200,
    )

    if result["error"] or result["data"] is None:
        logger.warning("select_pretrim_window: AI call failed: %s", result["error"])
        return None

    data = result["data"]
    start_phrase = (data.get("start_phrase") or "").strip()
    end_phrase = (data.get("end_phrase") or "").strip()

    if not start_phrase or not end_phrase:
        logger.warning("select_pretrim_window: AI returned empty phrases: %s", data)
        return None

    start_block = _find_phrase_in_blocks(blocks, start_phrase)
    end_block = _find_phrase_in_blocks(blocks, end_phrase)

    if start_block is None or end_block is None:
        logger.warning(
            "select_pretrim_window: phrases not found in SRT "
            "(start_phrase=%r found=%s, end_phrase=%r found=%s)",
            start_phrase[:60], start_block is not None,
            end_phrase[:60], end_block is not None,
        )
        return None

    start_secs = start_block["start_secs"]
    end_secs = end_block["end_secs"]

    if end_secs <= start_secs:
        logger.warning(
            "select_pretrim_window: end (%.1f) <= start (%.1f) — invalid window",
            end_secs, start_secs,
        )
        return None

    duration = end_secs - start_secs
    if duration < 60:
        logger.warning("select_pretrim_window: window too short (%.1fs)", duration)
        return None

    logger.info(
        "select_pretrim_window: window %.1f–%.1f (%.0fs) resolved from SRT phrases",
        start_secs, end_secs, duration,
    )
    return {"start_seconds": start_secs, "end_seconds": end_secs}
