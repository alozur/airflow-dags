"""
AI-powered chapter analysis for video transcriptions.

This module uses OpenAI to analyze video transcriptions and agendas
to identify interesting chapters/segments for content extraction.
"""

import logging
import os
from typing import Dict, List

import openai

from congress_videos.config.ai_prompts import (
    CHAPTER_IDENTIFICATION_SYSTEM_PROMPT,
    CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE,
)
from utils.ai_helpers import generate_json_completion
from utils.time_utils import parse_timestamp

logger = logging.getLogger(__name__)

# OpenAI API configuration
openai.api_key = os.getenv('OPENAI_API_KEY')


def parse_timestamp_to_seconds(timestamp: str) -> float:
    """Convert SRT timestamp to total seconds.

    Delegates to the unified ``utils.time_utils.parse_timestamp`` which
    preserves millisecond precision and returns a float.

    Supports:
    - ``HH:MM:SS`` (simplified format)
    - ``HH:MM:SS,mmm`` (SRT comma-separated milliseconds)
    - ``HH:MM:SS.mmm`` (dot-separated milliseconds)

    Args:
        timestamp: Time string in one of the supported formats.

    Returns:
        Total seconds as float (millisecond-precise).
    """
    try:
        return parse_timestamp(timestamp)
    except (ValueError, AttributeError):
        return 0.0


def _adaptive_silence_threshold(
    gaps_secs: list[float],
    percentile: float = 75.0,
    floor: int = 8,
) -> float:
    """Compute a percentile-based silence threshold from observed gap durations.

    Uses pure-Python sorted-index percentile (no numpy).  The result is clamped
    to a minimum of ``floor`` seconds so very sparse transcripts don't produce a
    threshold of zero.

    Args:
        gaps_secs: All inter-entry gap durations in seconds (may be empty).
        percentile: Desired percentile in [0, 100] (default: 75).
        floor: Hard minimum threshold in seconds (default: 8).

    Returns:
        Computed threshold in seconds (float), always >= floor.
    """
    if not gaps_secs:
        return float(floor)
    sorted_gaps = sorted(gaps_secs)
    n = len(sorted_gaps)
    # Linear interpolation index
    idx = (percentile / 100.0) * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    frac = idx - lower
    value = sorted_gaps[lower] + frac * (sorted_gaps[upper] - sorted_gaps[lower])
    return max(float(floor), value)


def detect_silence_gaps(
    srt_content: str,
    min_silence_seconds: int = 15,
    use_adaptive: bool = False,
    adaptive_percentile: float = 75.0,
) -> List[Dict]:
    """
    Detect silence gaps in SRT content by finding time gaps between subtitle entries.

    This function analyzes the SRT transcript to find natural breaks (silence periods)
    that can be used as chunk boundaries for better content segmentation.

    Args:
        srt_content: Full SRT transcription content
        min_silence_seconds: Minimum silence duration to consider as a gap (default: 15)
        use_adaptive: When True, derive the threshold from the Nth percentile of all
            observed inter-entry gaps instead of using ``min_silence_seconds`` directly.
            Falls back to the fixed threshold if no gaps pass the adaptive one.
        adaptive_percentile: Percentile to use for adaptive threshold (default: 75).

    Returns:
        List of silence gaps:
        [
            {
                "gap_start": "HH:MM:SS",
                "gap_end": "HH:MM:SS",
                "gap_duration_seconds": int,
                "previous_text": str,  # Last text before gap
                "next_text": str       # First text after gap
            }
        ]
    """
    import re

    # Parse SRT format to extract timestamps and text
    # SRT format: timestamp1 --> timestamp2 followed by text
    # Supports both HH:MM:SS and HH:MM:SS,mmm formats
    pattern = r'(\d{1,2}:\d{2}:\d{2}(?:,\d{3})?)\s*-->\s*(\d{1,2}:\d{2}:\d{2}(?:,\d{3})?)\s*\n(.+?)(?=\n\d{1,2}:\d{2}:\d{2}(?:,\d{3})?\s*-->|\Z)'

    entries = re.findall(pattern, srt_content, re.DOTALL)

    if not entries:
        logger.warning("No SRT entries found in content")
        return []

    # Collect ALL inter-entry gap durations first (needed for adaptive mode).
    all_gaps: list[float] = []
    for i in range(len(entries) - 1):
        current_end_seconds = parse_timestamp_to_seconds(entries[i][1].strip())
        next_start_seconds = parse_timestamp_to_seconds(entries[i + 1][0].strip())
        gap = next_start_seconds - current_end_seconds
        all_gaps.append(gap)

    # Determine effective threshold.
    if use_adaptive:
        effective_threshold = _adaptive_silence_threshold(
            all_gaps, percentile=adaptive_percentile
        )
        logger.info(
            "Adaptive silence threshold: %.2fs (p%.0f of %d gaps); "
            "fixed fallback: %ds",
            effective_threshold, adaptive_percentile, len(all_gaps), min_silence_seconds,
        )
    else:
        effective_threshold = float(min_silence_seconds)

    silence_gaps = []

    for i in range(len(entries) - 1):
        current_entry = entries[i]
        next_entry = entries[i + 1]

        current_end_time = current_entry[1].strip()
        next_start_time = next_entry[0].strip()

        gap_duration = all_gaps[i]

        if gap_duration >= effective_threshold:
            current_end_seconds = parse_timestamp_to_seconds(current_end_time)
            next_start_seconds = parse_timestamp_to_seconds(next_start_time)
            silence_gaps.append({
                "gap_start": current_end_time,
                "gap_end": next_start_time,
                "gap_duration_seconds": gap_duration,
                "gap_midpoint_seconds": (current_end_seconds + next_start_seconds) // 2,
                "previous_text": current_entry[2].strip()[:100],
                "next_text": next_entry[2].strip()[:100],
            })

    # Adaptive fallback: if no gaps passed the adaptive threshold, retry with fixed.
    if use_adaptive and not silence_gaps:
        logger.warning(
            "Adaptive threshold %.2fs produced 0 gaps; "
            "falling back to fixed threshold %ds",
            effective_threshold, min_silence_seconds,
        )
        for i in range(len(entries) - 1):
            current_entry = entries[i]
            next_entry = entries[i + 1]
            current_end_time = current_entry[1].strip()
            next_start_time = next_entry[0].strip()
            gap_duration = all_gaps[i]
            if gap_duration >= min_silence_seconds:
                current_end_seconds = parse_timestamp_to_seconds(current_end_time)
                next_start_seconds = parse_timestamp_to_seconds(next_start_time)
                silence_gaps.append({
                    "gap_start": current_end_time,
                    "gap_end": next_start_time,
                    "gap_duration_seconds": gap_duration,
                    "gap_midpoint_seconds": (current_end_seconds + next_start_seconds) // 2,
                    "previous_text": current_entry[2].strip()[:100],
                    "next_text": next_entry[2].strip()[:100],
                })

    threshold_used = effective_threshold if not (use_adaptive and not silence_gaps) else min_silence_seconds
    logger.info(f"Found {len(silence_gaps)} silence gaps of {threshold_used}+ seconds")
    return silence_gaps


def chunk_by_silence(
    srt_content: str,
    min_silence_seconds: int = 15,
    min_chunk_duration_minutes: int = 20,
    max_chunk_duration_minutes: int = 30,
    use_adaptive: bool = False,
    adaptive_percentile: float = 75.0,
) -> List[Dict]:
    """
    Split SRT content into chunks based on silence gaps.

    Creates natural chunks by splitting at silence gaps. Ensures chunks are at least
    min_chunk_duration_minutes long by merging small chunks with adjacent ones.

    Args:
        srt_content: Full SRT transcription content
        min_silence_seconds: Minimum silence duration to use as split point (default: 15)
        min_chunk_duration_minutes: Minimum duration for each chunk (default: 20)
        max_chunk_duration_minutes: Maximum duration for each chunk (default: 30)
        use_adaptive: When True, derive the silence threshold from the Nth percentile
            of observed inter-entry gaps instead of using ``min_silence_seconds`` directly.
            Falls back to the fixed threshold if the adaptive one produces no gaps (default: False).
        adaptive_percentile: Percentile to use for adaptive threshold (default: 75).

    Returns:
        List of chunks:
        [
            {
                "chunk_number": int,
                "start_time": "HH:MM:SS",
                "end_time": "HH:MM:SS",
                "duration_seconds": int,
                "duration_minutes": float,
                "content": str  # SRT content for this chunk
            }
        ]
    """
    import re

    # Debug: Log SRT content info
    logger.info(f"Chunking SRT content: {len(srt_content)} characters")
    if len(srt_content) > 0:
        logger.info(f"First 500 chars: {srt_content[:500]}")

    # Detect all silence gaps
    silence_gaps = detect_silence_gaps(
        srt_content,
        min_silence_seconds,
        use_adaptive=use_adaptive,
        adaptive_percentile=adaptive_percentile,
    )

    if not silence_gaps:
        logger.warning("No silence gaps found, returning entire content as single chunk")
        # Parse first and last timestamps (with or without milliseconds)
        timestamps = re.findall(r'\d{1,2}:\d{2}:\d{2}(?:,\d{3})?', srt_content)
        if len(timestamps) >= 2:
            start_time = timestamps[0]
            end_time = timestamps[-1]
            duration = parse_timestamp_to_seconds(end_time) - parse_timestamp_to_seconds(start_time)
        else:
            start_time = "00:00:00"
            end_time = "00:00:00"
            duration = 0

        return [{
            "chunk_number": 1,
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "duration_minutes": round(duration / 60, 1),
            "content": srt_content
        }]

    # Extract all SRT entries with timestamps
    # Supports both HH:MM:SS and HH:MM:SS,mmm formats
    pattern = r'(\d{1,2}:\d{2}:\d{2}(?:,\d{3})?)\s*-->\s*(\d{1,2}:\d{2}:\d{2}(?:,\d{3})?)\s*\n(.+?)(?=\n\d{1,2}:\d{2}:\d{2}(?:,\d{3})?\s*-->|\Z)'
    entries = re.findall(pattern, srt_content, re.DOTALL)

    chunks = []
    chunk_start_idx = 0
    chunk_start_time = entries[0][0] if entries else "00:00:00"
    max_chunk_seconds = max_chunk_duration_minutes * 60
    min_chunk_seconds = min_chunk_duration_minutes * 60

    for gap in silence_gaps:
        gap_midpoint = gap['gap_midpoint_seconds']

        # Find the entry index at this gap
        gap_entry_idx = None
        for idx, entry in enumerate(entries):
            if parse_timestamp_to_seconds(entry[1]) >= gap_midpoint:
                gap_entry_idx = idx
                break

        if gap_entry_idx is None:
            continue

        # Check if creating a chunk here would exceed max duration
        chunk_end_time = entries[gap_entry_idx - 1][1] if gap_entry_idx > 0 else entries[0][1]
        chunk_duration = parse_timestamp_to_seconds(chunk_end_time) - parse_timestamp_to_seconds(chunk_start_time)

        # Only create chunk if it meets duration criteria
        if chunk_duration >= max_chunk_seconds or gap_entry_idx == len(entries) - 1:
            # Build chunk content
            chunk_entries = entries[chunk_start_idx:gap_entry_idx]
            chunk_content = ""
            for entry in chunk_entries:
                chunk_content += f"{entry[0]} --> {entry[1]}\n{entry[2]}\n\n"

            chunks.append({
                "chunk_number": len(chunks) + 1,
                "start_time": chunk_start_time,
                "end_time": chunk_end_time,
                "duration_seconds": chunk_duration,
                "duration_minutes": round(chunk_duration / 60, 1),
                "content": chunk_content.strip()
            })

            # Start new chunk
            chunk_start_idx = gap_entry_idx
            chunk_start_time = entries[gap_entry_idx][0] if gap_entry_idx < len(entries) else chunk_end_time

    # Add final chunk if there are remaining entries
    if chunk_start_idx < len(entries):
        chunk_entries = entries[chunk_start_idx:]
        chunk_end_time = entries[-1][1]
        chunk_duration = parse_timestamp_to_seconds(chunk_end_time) - parse_timestamp_to_seconds(chunk_start_time)

        chunk_content = ""
        for entry in chunk_entries:
            chunk_content += f"{entry[0]} --> {entry[1]}\n{entry[2]}\n\n"

        chunks.append({
            "chunk_number": len(chunks) + 1,
            "start_time": chunk_start_time,
            "end_time": chunk_end_time,
            "duration_seconds": chunk_duration,
            "duration_minutes": round(chunk_duration / 60, 1),
            "content": chunk_content.strip()
        })

    # Post-process: Merge chunks that are too small (less than min_chunk_duration_minutes)
    merged_chunks = []
    i = 0
    while i < len(chunks):
        current_chunk = chunks[i]

        # If chunk is too small and not the last one, merge with next
        while (current_chunk['duration_seconds'] < min_chunk_seconds and
               i < len(chunks) - 1):
            next_chunk = chunks[i + 1]

            # Merge current and next chunk
            current_chunk = {
                "chunk_number": len(merged_chunks) + 1,
                "start_time": current_chunk['start_time'],
                "end_time": next_chunk['end_time'],
                "duration_seconds": (parse_timestamp_to_seconds(next_chunk['end_time']) -
                                   parse_timestamp_to_seconds(current_chunk['start_time'])),
                "content": current_chunk['content'] + "\n\n" + next_chunk['content']
            }
            current_chunk['duration_minutes'] = round(current_chunk['duration_seconds'] / 60, 1)
            i += 1

        # Renumber chunk
        current_chunk['chunk_number'] = len(merged_chunks) + 1
        merged_chunks.append(current_chunk)
        i += 1

    logger.info(f"Created {len(merged_chunks)} chunks based on silence gaps (min: {min_chunk_duration_minutes} min)")
    for chunk in merged_chunks:
        logger.info(f"  Chunk {chunk['chunk_number']}: {chunk['start_time']} - {chunk['end_time']} ({chunk['duration_minutes']} min)")

    return merged_chunks


def format_seconds_to_timestamp(seconds: float) -> str:
    """Convert seconds to HH:MM:SS format.

    Args:
        seconds: Total seconds (int or float; fractional part is truncated).

    Returns:
        Formatted timestamp string in ``HH:MM:SS`` format.
    """
    total = int(seconds)
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def analyze_chapters_with_ai(
    srt_content: str,
    agenda_content: str,
    min_duration_minutes: int = 15,
    max_duration_minutes: int = 30,
    model: str = "gpt-4o-mini"
) -> Dict:
    """
    Use AI to identify topic changes in transcription based on content similarity.

    Simplified approach:
    1. Read the full transcription (SRT format with timestamps)
    2. Read the session agenda for context
    3. Identify when topics change based on content similarity
    4. Create chapter boundaries at topic changes
    5. Return structured chapter data with timestamps

    Args:
        srt_content: Full transcription in SRT format with timestamps
        agenda_content: Session agenda text (used as chunk summary for context)
        min_duration_minutes: Minimum chapter duration (default: 15)
        max_duration_minutes: Maximum chapter duration (default: 30)
        model: OpenAI model to use (default: "gpt-4o-mini")

    Returns:
        Dict with chapter analysis results:
        {
            "success": bool,
            "total_chapters": int,
            "total_duration_seconds": int,
            "chapters": [
                {
                    "chapter_number": int,
                    "title": str,
                    "start_time": str (HH:MM:SS),
                    "end_time": str (HH:MM:SS),
                    "duration_seconds": int,
                    "topics": [str]
                }
            ],
            "error": str (if failed)
        }
    """
    result = {
        "success": False,
        "total_chapters": 0,
        "total_duration_seconds": 0,
        "chapters": [],
        "error": None
    }

    if not srt_content:
        result["error"] = "Empty SRT content provided"
        logger.warning(result["error"])
        return result

    try:
        logger.info("Identifying topic changes in transcription using AI...")
        logger.info(f"Target chapter duration: {min_duration_minutes}-{max_duration_minutes} minutes")

        user_prompt = CHAPTER_IDENTIFICATION_USER_PROMPT_TEMPLATE.format(
            chunk_summary=agenda_content,
            srt_content=srt_content,
        )

        logger.info(f"Calling OpenAI API with model: {model}")

        ai_result = generate_json_completion(
            system_prompt=CHAPTER_IDENTIFICATION_SYSTEM_PROMPT,
            user_prompt=user_prompt,
            model=model,
            temperature=0.2,
            max_tokens=2000,
        )

        if ai_result["error"]:
            result["error"] = ai_result["error"]
            logger.error(f"AI completion failed: {ai_result['error']}")
            return result

        chapters_data = ai_result["data"] or {}
        raw_chapters = chapters_data.get("interesting_chapters", [])

        # Normalise chapters to the expected output structure
        chapters = []
        for idx, chapter in enumerate(raw_chapters, start=1):
            start_seconds = parse_timestamp_to_seconds(chapter.get("start_time", "00:00:00"))
            end_seconds = parse_timestamp_to_seconds(chapter.get("end_time", "00:00:00"))
            duration_seconds = max(0, end_seconds - start_seconds)

            chapters.append({
                "chapter_number": idx,
                "title": chapter.get("title", ""),
                "start_time": chapter.get("start_time", "00:00:00"),
                "end_time": chapter.get("end_time", "00:00:00"),
                "duration_seconds": duration_seconds,
                "duration_minutes": round(duration_seconds / 60, 1),
                "topics": chapter.get("topics", []),
            })

        # Sort chapters by start time
        chapters.sort(key=lambda x: parse_timestamp_to_seconds(x["start_time"]))

        # Calculate total duration
        if chapters:
            first_start = parse_timestamp_to_seconds(chapters[0]["start_time"])
            last_end = parse_timestamp_to_seconds(chapters[-1]["end_time"])
            total_duration = last_end - first_start
        else:
            total_duration = 0

        result["success"] = True
        result["total_chapters"] = len(chapters)
        result["total_duration_seconds"] = total_duration
        result["chapters"] = chapters

        logger.info(f"Identified {len(chapters)} topic changes")
        for chapter in chapters:
            logger.info(
                f"  Chapter {chapter['chapter_number']}: {chapter['title']} "
                f"({chapter['start_time']} - {chapter['end_time']}, {chapter.get('duration_minutes', 0):.1f} min)"
            )

    except Exception as e:
        result["error"] = f"Unexpected error: {str(e)}"
        logger.error(result["error"], exc_info=True)

    return result
