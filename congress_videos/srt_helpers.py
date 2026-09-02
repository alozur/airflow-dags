"""SRT file helpers for AI-assisted pre-trim window selection."""

import logging
import os
import re
from typing import Optional

from congress_videos.config.paths import DOWNLOADS_DIR, PROJECT_DATA_DIR
from utils.ai_helpers import generate_json_completion
from utils.llm_config import LLM_CHEAP
from utils.time_utils import parse_timestamp

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Interest scoring constants
# ---------------------------------------------------------------------------

INTEREST_SCALE_MIN: int = 0
INTEREST_SCALE_MAX: int = 10
# Threshold below which a turn is soft-excluded from the upload queue.
# Matches the COALESCE neutral value so unscored turns clear the filter
# but sort last among genuinely interesting turns.
# Also documented in migration 029 SQL header comment.
INTEREST_FILTER_THRESHOLD: int = 1
# Neutral COALESCE value: passes the >= INTEREST_FILTER_THRESHOLD filter
# but sorts last among all turns with a real score.
INTEREST_NEUTRAL: int = 1

_TIMESTAMP_RE = re.compile(r'\d{2}:\d{2}:\d{2}')
_TIMESTAMP_STRIP_RE = re.compile(r',\d+')
_SRT_TIMESTAMP_ARROW_RE = re.compile(
    r'(\d{2}:\d{2}:\d{2},\d+)\s*-->\s*(\d{2}:\d{2}:\d{2},\d+)'
)


def _secs_to_srt_ts(secs: float) -> str:
    """Convert float seconds to SRT timestamp string ``HH:MM:SS,mmm``."""
    ms = int(round((secs % 1) * 1000))
    s = int(secs)
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{sec:02d},{ms:03d}"


def _serialize_srt_blocks(blocks: list[dict]) -> str:
    """Rebuild a valid SRT string from ``{start_secs, end_secs, text}`` blocks.

    Produces sequential 1-based indices, ``HH:MM:SS,mmm`` timestamps, and
    blank-line-separated entries.  Empty list returns ``""``.  Pure; no I/O.
    """
    out = []
    for i, b in enumerate(blocks, start=1):
        out.append(
            f"{i}\n{_secs_to_srt_ts(b['start_secs'])} --> "
            f"{_secs_to_srt_ts(b['end_secs'])}\n{b['text']}\n"
        )
    return "\n".join(out)


def find_srt_for_chapter(
    video_id: str,
    chapter_id: int,
    session_date: Optional[str] = None,
    canonical_dir: Optional[str] = None,
) -> Optional[str]:
    """
    Try common SRT path patterns, return first existing path or None.

    When *canonical_dir* is supplied, probes ``{canonical_dir}/subtitles.srt``
    first.  Falls through to the legacy probe list when that file is absent or
    when *canonical_dir* is ``None`` (default, preserving existing behavior).

    Legacy probes:
      1. data/congress_videos/{video_id}/srt_files/
      2. downloads/{session_date}/{video_id}/srt_files/  (if session_date provided)
      3. downloads/ (date-agnostic search when session_date is None)
    """
    # video_id becomes a path component below; reject anything outside the
    # YouTube-id charset to block path traversal from a tampered DB row.
    if not video_id or not re.fullmatch(r"[A-Za-z0-9_-]+", video_id):
        logger.warning("find_srt_for_chapter: unsafe video_id %r — refusing path probe", video_id)
        return None

    srt_filenames = [f"{video_id}_merged.srt", f"{video_id}.srt"]

    candidates = []
    if canonical_dir:
        candidates.append(os.path.join(canonical_dir, "subtitles.srt"))
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


# When None is passed as max_chars, this cap guards against pathologically large
# pre-trim SRTs (>300k chars) that would blow the model's context.
PRETRIM_MAX_CHARS = 120_000
_PRETRIM_PATHOLOGICAL_THRESHOLD = 300_000


def parse_srt_to_text(srt_path: str, max_chars: int | None = None) -> str:
    """
    Parse SRT file and return text with timestamps for AI analysis.

    Format: "00:01:23 --> 00:01:45\\nHello world\\n\\n00:01:46 --> 00:02:01\\nNext line\\n\\n..."

    Args:
        srt_path: Path to the SRT file.
        max_chars: Maximum characters to return.  When ``None`` (default) the
            full parsed text is returned, capped only when the raw file exceeds
            ``_PRETRIM_PATHOLOGICAL_THRESHOLD`` (300k chars).  Pass an explicit
            integer to enforce a hard limit (e.g. for legacy callers).
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

    if max_chars is not None:
        return result[:max_chars]

    # Pathological guard: warn and cap at PRETRIM_MAX_CHARS when unbounded.
    if len(result) > _PRETRIM_PATHOLOGICAL_THRESHOLD:
        logger.warning(
            "parse_srt_to_text: SRT text is %d chars (> %d); "
            "capping at PRETRIM_MAX_CHARS=%d to avoid context overflow",
            len(result),
            _PRETRIM_PATHOLOGICAL_THRESHOLD,
            PRETRIM_MAX_CHARS,
        )
        return result[:PRETRIM_MAX_CHARS]

    return result


def _srt_timestamp_to_seconds(ts: str) -> float:
    """Convert an SRT timestamp string to float seconds.

    Delegates to ``utils.time_utils.parse_timestamp`` which accepts
    ``HH:MM:SS``, ``HH:MM:SS,mmm``, and ``HH:MM:SS.mmm``.

    Args:
        ts: Timestamp string.

    Returns:
        Total seconds as float.
    """
    return parse_timestamp(ts)


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


def _window_srt_blocks(
    blocks: list[dict],
    window_start: float,
    window_end: float,
) -> list[dict]:
    """Filter and re-time SRT blocks to a clip window.

    Returns a new list of ``{start_secs, end_secs, text}`` dicts that overlap
    ``[window_start, window_end)`` with timestamps re-timed to clip origin
    (i.e. ``window_start`` is subtracted from each surviving block's
    ``start_secs`` / ``end_secs``).  Timestamps that would go negative are
    clamped to 0.0 (boundary-straddling blocks whose start precedes the window).

    Overlap predicate: ``block.start_secs < window_end AND block.end_secs > window_start``
    (identical to the predicate used in ``_window_srt_text``).

    Pure; no I/O; never raises.

    Args:
        blocks:       Parsed SRT blocks, each a ``{start_secs, end_secs, text}`` dict.
        window_start: Clip window start in seconds.
        window_end:   Clip window end in seconds.

    Returns:
        Re-timed blocks whose original interval overlaps the window.
    """
    out = []
    for b in blocks:
        if b["start_secs"] < window_end and b["end_secs"] > window_start:
            out.append({
                "start_secs": max(0.0, b["start_secs"] - window_start),
                "end_secs": max(0.0, b["end_secs"] - window_start),
                "text": b["text"],
            })
    return out


def chapter_window_blocks(blocks: list[dict], start_time, end_time) -> list[dict]:
    """Filter *blocks* to a chapter's own ``[start_time, end_time)`` span
    (issue #322). Unlike ``_window_srt_blocks``, timestamps are NOT
    re-timed — absolute coordinates are preserved (D9: needed by the
    qa-gated wide prompt context and the deferred thumbnail adopter).
    ``start_time``/``end_time`` accept an SRT ``str`` or numeric seconds.
    Overlap predicate matches ``_window_srt_blocks``: ``start < end AND
    end > start``. Fails safe (``[]`` + WARNING) on any parse failure or
    ``end_secs <= start_secs``. Never raises.
    """
    try:
        start_secs = start_time if isinstance(start_time, (int, float)) else _srt_timestamp_to_seconds(start_time)
        end_secs = end_time if isinstance(end_time, (int, float)) else _srt_timestamp_to_seconds(end_time)
        start_secs, end_secs = float(start_secs), float(end_secs)
    except (ValueError, TypeError):
        start_secs = end_secs = None

    if start_secs is None or end_secs <= start_secs:
        logger.warning(
            "chapter_window_blocks: invalid chapter span start_time=%r end_time=%r — returning []",
            start_time, end_time,
        )
        return []

    return [b for b in blocks if b["start_secs"] < end_secs and b["end_secs"] > start_secs]


def _window_srt_blocks_multi(
    blocks: list[dict],
    intervals: list[tuple[float, float]],
) -> list[dict]:
    """Retime SRT blocks across N kept windows (issue #143).

    Unlike ``_window_srt_blocks`` (a single window), this walks an ORDERED
    list of kept intervals and offsets each surviving block by the
    cumulative duration of the PREVIOUSLY KEPT windows, so the output SRT
    lines up with the concatenated, gap-free cut video (excised spans
    between intervals never appear in the output timeline at all).

    A block whose original span straddles a cut boundary is emitted in BOTH
    surviving windows it overlaps, each clamped to that window's edge — its
    words are physically split by the cut, so the caption follows the audio
    on each side.

    With exactly one interval, output is identical to
    ``_window_srt_blocks(blocks, intervals[0][0], intervals[0][1])``.

    Defensive (validate at system boundaries — intervals may originate from
    a JSONB column read back from the DB): intervals are coerced to float,
    sorted by start, and any non-positive-duration (``start >= end``)
    interval is silently dropped rather than raising.

    Pure; no I/O; never raises.

    Args:
        blocks:    Parsed SRT blocks, each a ``{start_secs, end_secs, text}`` dict.
        intervals: Kept (start, end) second pairs, in source-time coordinates.

    Returns:
        Re-timed blocks in chronological output order.
    """
    valid_intervals = sorted(
        (float(start), float(end)) for start, end in intervals if float(end) > float(start)
    )

    out: list[dict] = []
    elapsed = 0.0
    for window_start, window_end in valid_intervals:
        for b in blocks:
            if b["start_secs"] < window_end and b["end_secs"] > window_start:
                clamped_start = max(b["start_secs"], window_start)
                clamped_end = min(b["end_secs"], window_end)
                out.append({
                    "start_secs": clamped_start - window_start + elapsed,
                    "end_secs": clamped_end - window_start + elapsed,
                    "text": b["text"],
                })
        elapsed += window_end - window_start
    return out


def _window_srt_text(video_id: str, start_seconds: float, end_seconds: float) -> str:
    """Return joined text of SRT blocks overlapping [start_seconds, end_seconds].

    Uses ``find_srt_for_chapter`` (date-agnostic probe) to locate the merged
    SRT file, then filters ``_parse_srt_blocks`` output to only blocks that
    overlap the given window.  Blocks are included when their interval intersects
    [start_seconds, end_seconds] (i.e. block.start < end and block.end > start).

    Returns ``""`` when the SRT file is absent, the window yields no blocks,
    or any I/O error occurs.  Never raises.

    Args:
        video_id:      Source video identifier.
        start_seconds: Window start in seconds (inclusive).
        end_seconds:   Window end in seconds (exclusive).

    Returns:
        Space-joined text of all matching blocks, or ``""``.
    """
    srt_path = find_srt_for_chapter(video_id, 0)  # chapter_id unused in probe
    if srt_path is None:
        return ""

    blocks = _parse_srt_blocks(srt_path)
    texts = [
        b["text"]
        for b in blocks
        if b["start_secs"] < end_seconds and b["end_secs"] > start_seconds
    ]
    return " ".join(texts)


def score_turn_interest(window_text: str, completion_fn=None) -> Optional[int]:
    """Score a turn's SRT window 0–10 for YouTube newsworthiness.

    Mirrors the robustness pattern of ``extract_lapidary_quote``:
    - Returns ``None`` on empty/whitespace input (no LLM call).
    - Returns ``None`` on any LLM error, non-parseable output, or missing API key.
    - Clamps the returned integer to [INTEREST_SCALE_MIN, INTEREST_SCALE_MAX].
    - NEVER raises; all exceptions are caught and logged at WARNING.

    Args:
        window_text:   Windowed SRT text for the turn (joined subtitle blocks).
        completion_fn: Injectable callable with the same signature as
            ``generate_chat_completion`` (for unit-test isolation).  Defaults to
            the real ``generate_chat_completion`` when ``None``.

    Returns:
        Integer 0–10, or ``None`` on any failure.
    """
    if not window_text.strip():
        return None

    if completion_fn is None:
        from utils.ai_helpers import generate_chat_completion as completion_fn  # type: ignore[assignment]

    from congress_videos.config.ai_prompts import INTEREST_SCORING_SYSTEM_PROMPT

    try:
        resp = completion_fn(
            system_prompt=INTEREST_SCORING_SYSTEM_PROMPT,
            user_prompt=window_text[:8000],
            model=LLM_CHEAP,
            temperature=0.0,
            max_tokens=3,
        )
        content = ((resp or {}).get("content") or "").strip()
        m = re.search(r"-?\d+", content)
        if not m:
            return None
        val = int(m.group())
        return max(INTEREST_SCALE_MIN, min(INTEREST_SCALE_MAX, val))
    except Exception:
        logger.warning(
            "score_turn_interest: unexpected error scoring window (len=%d)",
            len(window_text),
            exc_info=True,
        )
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
    srt_text = parse_srt_to_text(srt_path, max_chars=PRETRIM_MAX_CHARS)
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
        model=LLM_CHEAP,
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
    end_secs = min(end_block["end_secs"], start_block["start_secs"] + target_secs)

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
