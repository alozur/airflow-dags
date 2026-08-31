"""Pure speaker-resolution module for the turn prepare pipeline (issue #177).

Identifies the speaking participant from the president's intro announcement
(SRT window before the turn starts) plus the first seconds of the turn itself.
Persisted via mark_turn_resolved; consumed in-memory by thumbnail/sidecar steps.

Design constraints:
- resolve_speaker MUST never raise. Any exception returns None.
- All DB I/O and SRT I/O is injected or happens outside this module.
- completion_fn defaults to cached_json_completion (Postgres-backed LLM cache).

NOTE: imports from congress_videos.srt_helpers and utils.llm_cache are done at
module level to enable clean patching in tests. Neither import pulls in task or
scheduler constructs, so this module is safe to parse under the scheduler.
"""
from __future__ import annotations

import logging
from typing import Callable

from congress_videos.config.ai_prompts import (
    SPEAKER_RESOLUTION_SYSTEM_PROMPT,
    SPEAKER_RESOLUTION_USER_TEMPLATE,
)
from congress_videos.srt_helpers import _parse_srt_blocks, find_srt_for_chapter

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants (tested directly by spec)
# ---------------------------------------------------------------------------

INTRO_WINDOW_SECS: int = 120
"""Seconds before turn start to include as the president's intro window."""

TURN_CONTEXT_SECS: int = 60
"""Seconds of the turn itself to include as context."""

SPEAKER_RESOLUTION_MIN_CONFIDENCE: float = 0.80
"""Minimum model confidence required to accept a resolution result."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_speaker(
    turn: dict,
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> dict | None:
    """Identify the participant speaking in a turn using SRT context windows.

    Returns a dict ``{participant_slug, confidence, evidence}`` when the model
    attributes the turn with confidence >= SPEAKER_RESOLUTION_MIN_CONFIDENCE
    and the returned slug exists in *participants*. Returns None on any failure,
    missing SRT, low confidence, or hallucinated slug.

    This function NEVER raises. All internal exceptions are caught and logged.

    Args:
        turn: Row dict from select_unprepared_turns. Must contain at least
            ``start_seconds``, ``video_id``, ``chapter_id``, ``session_date``.
        participants: List of participant dicts, each with at minimum a ``slug``
            and ``display_name`` key. Fetched once per prepare loop by the caller.
        completion_fn: Optional override for the LLM completion call. Defaults
            to ``utils.llm_cache.cached_json_completion``. Must accept
            (system_prompt, user_prompt, **kwargs) and return a dict with
            ``data`` and ``error`` keys (same contract as cached_json_completion).

    Returns:
        ``{"participant_slug": str, "confidence": float, "evidence": str}``
        or ``None``.
    """
    try:
        return _resolve_speaker_inner(turn, participants, completion_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "resolve_speaker: unexpected exception for turn_id=%s — returning None (%s: %s)",
            turn.get("turn_id"),
            type(exc).__name__,
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------

def _resolve_speaker_inner(
    turn: dict,
    participants: list[dict],
    completion_fn: Callable | None,
) -> dict | None:
    """Internal resolver (may raise; wrapped by resolve_speaker)."""
    if not participants:
        logger.debug("resolve_speaker: empty participants list — returning None")
        return None

    # Build the valid slug set for validation
    valid_slugs: set[str] = {p["slug"] for p in participants}

    # Locate SRT file for this turn's source video
    video_id = turn.get("video_id")
    chapter_id = turn.get("chapter_id")
    session_date = turn.get("session_date")

    srt_path = find_srt_for_chapter(
        str(video_id) if video_id is not None else "",
        chapter_id or 0,
        str(session_date) if session_date else None,
    )

    if srt_path is None:
        logger.debug(
            "resolve_speaker: no SRT found for turn_id=%s video_id=%s — returning None",
            turn.get("turn_id"),
            video_id,
        )
        return None

    # Parse all SRT blocks
    all_blocks = _parse_srt_blocks(srt_path)

    start_secs = float(turn.get("start_seconds", 0))

    # Intro window anchor (issue #283 rule 3): use the group's real start
    # when available (MIN(start_seconds) OVER the materialized group), not
    # the representative turn's own start_seconds — the representative may
    # be a diarization blip mid-group. Explicit `is not None` so a
    # legitimate group_start_seconds == 0.0 is honoured rather than falling
    # through to start_secs.
    group_start = turn.get("group_start_seconds")
    intro_anchor = float(group_start) if group_start is not None else start_secs

    # Intro window: [intro_anchor - INTRO_WINDOW_SECS, intro_anchor)
    intro_start = max(0.0, intro_anchor - INTRO_WINDOW_SECS)
    intro_blocks = [
        b for b in all_blocks
        if b["start_secs"] >= intro_start and b["end_secs"] <= intro_anchor
    ]

    # Turn context window: [start, start + TURN_CONTEXT_SECS)
    turn_end = start_secs + TURN_CONTEXT_SECS
    turn_blocks = [
        b for b in all_blocks
        if b["start_secs"] >= start_secs and b["start_secs"] < turn_end
    ]

    # Both windows may be empty at the start of a session — still attempt resolution
    # if we have at least some turn-window content.
    if not intro_blocks and not turn_blocks:
        logger.debug(
            "resolve_speaker: no SRT blocks in either window for turn_id=%s — returning None",
            turn.get("turn_id"),
        )
        return None

    intro_text = "\n".join(b["text"] for b in intro_blocks) if intro_blocks else "(no intro)"
    turn_text = "\n".join(b["text"] for b in turn_blocks) if turn_blocks else "(no turn context)"

    # Serialize participant roster: slug | display_name | party
    roster_lines = [
        f"{p['slug']} | {p.get('display_name', '')} | {p.get('party', '')}"
        for p in participants
    ]
    participant_roster = "\n".join(roster_lines)

    user_prompt = SPEAKER_RESOLUTION_USER_TEMPLATE.format(
        intro_text=intro_text,
        turn_text=turn_text,
        participant_roster=participant_roster,
    )

    # Call the LLM (or injected stub)
    if completion_fn is None:
        from utils.llm_cache import cached_json_completion
        completion_fn = cached_json_completion

    response = completion_fn(
        SPEAKER_RESOLUTION_SYSTEM_PROMPT,
        user_prompt,
        model="gpt-4o-mini",
        temperature=0.0,
        max_tokens=300,
    )

    if response.get("error") or not response.get("data"):
        logger.debug(
            "resolve_speaker: completion error for turn_id=%s: %s",
            turn.get("turn_id"),
            response.get("error"),
        )
        return None

    data = response["data"]

    slug = data.get("participant_slug")
    confidence = data.get("confidence")
    evidence = data.get("evidence", "")

    # Validate slug: must be in participants, confidence must be >= threshold
    if not slug or slug not in valid_slugs:
        logger.debug(
            "resolve_speaker: hallucinated or null slug %r for turn_id=%s — returning None",
            slug,
            turn.get("turn_id"),
        )
        return None

    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        logger.debug(
            "resolve_speaker: invalid confidence %r for turn_id=%s — returning None",
            confidence,
            turn.get("turn_id"),
        )
        return None

    if confidence < SPEAKER_RESOLUTION_MIN_CONFIDENCE:
        logger.debug(
            "resolve_speaker: confidence %.2f < %.2f for turn_id=%s — returning None",
            confidence,
            SPEAKER_RESOLUTION_MIN_CONFIDENCE,
            turn.get("turn_id"),
        )
        return None

    logger.info(
        "resolve_speaker: resolved turn_id=%s → slug=%r confidence=%.2f",
        turn.get("turn_id"),
        slug,
        confidence,
    )
    return {"participant_slug": slug, "confidence": confidence, "evidence": evidence}
