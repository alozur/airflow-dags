"""Pure chapter speaker resolution module (issue #263).

Identifies which known participant a chapter's dirty speaker mentions refer
to, using a single batched LLM call validated against a supplied participant
roster. Used identically by the monitor-time normalization step and the
upload-time thumbnail-config step, so both seams derive the same canonical
name and slug from one resolver contract.

Design constraints:
- resolve_chapter_speakers MUST never raise. Any exception yields an empty
  ChapterSpeakerResolution.
- Every returned participant_slug is validated against the supplied roster;
  a slug absent from the roster is treated as no match.
- display_name always comes from the roster row, never from the model.
- completion_fn defaults to cached_json_completion (Postgres-backed cache).

NOTE: imports from congress_videos.config.ai_prompts and utils.llm_cache are
done at module level (ai_prompts) or lazily (llm_cache) to enable clean
patching in tests, mirroring congress_videos.modules.speaker_resolution.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from congress_videos.config.ai_prompts import (
    CHAPTER_SPEAKER_RESOLUTION_SYSTEM_PROMPT,
    CHAPTER_SPEAKER_RESOLUTION_USER_TEMPLATE,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants (tested directly by spec)
# ---------------------------------------------------------------------------

CHAPTER_SPEAKER_MIN_CONFIDENCE: float = 0.80
"""Minimum model confidence required to accept a resolved mention."""

MAX_MENTIONS_PER_CALL: int = 8
"""Upper bound on mentions resolved in a single batched call."""


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SpeakerMatch:
    """A single resolved speaker mention."""

    mention: str
    participant_slug: str
    display_name: str
    confidence: float
    evidence: str = ""


@dataclass(frozen=True)
class ChapterSpeakerResolution:
    """Return value of :func:`resolve_chapter_speakers`."""

    matches: tuple[SpeakerMatch, ...] = ()
    """Accepted matches, in the SAME order as the input mentions list."""

    by_mention: dict[str, SpeakerMatch] = field(default_factory=dict)
    """Lookup of accepted matches keyed by the original mention string."""

    @property
    def primary(self) -> SpeakerMatch | None:
        """The first accepted match, in input order, or None when empty."""
        return self.matches[0] if self.matches else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_chapter_speakers(
    mentions: list[str],
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> ChapterSpeakerResolution:
    """Resolve *mentions* against *participants* with one batched LLM call.

    This function NEVER raises. All internal exceptions are caught and
    logged, degrading to an empty :class:`ChapterSpeakerResolution`.

    Args:
        mentions: Ordered list of dirty speaker strings, already
            placeholder-filtered and deduplicated by the caller.
        participants: Roster of known participants, each a dict with at
            minimum ``slug`` and ``display_name`` keys (and optionally
            ``party``).
        completion_fn: Optional override for the LLM completion call.
            Defaults to ``utils.llm_cache.cached_json_completion``. Must
            accept ``(system_prompt, user_prompt, **kwargs)`` and return a
            dict with ``data`` and ``error`` keys.

    Returns:
        A :class:`ChapterSpeakerResolution`. Matches preserve input order
        (never the model's response order); ``primary`` is the first
        accepted match.
    """
    try:
        return _resolve_inner(mentions, participants, completion_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "resolve_chapter_speakers: unexpected exception — returning empty result (%s: %s)",
            type(exc).__name__,
            exc,
        )
        return ChapterSpeakerResolution()


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------

def _resolve_inner(
    mentions: list[str],
    participants: list[dict],
    completion_fn: Callable | None,
) -> ChapterSpeakerResolution:
    """Internal resolver (may raise; wrapped by resolve_chapter_speakers)."""
    if not mentions or not participants:
        logger.debug(
            "resolve_chapter_speakers: empty mentions or empty roster — skipping call"
        )
        return ChapterSpeakerResolution()

    capped_mentions = mentions[:MAX_MENTIONS_PER_CALL]
    roster_by_slug: dict[str, dict] = {p["slug"]: p for p in participants}

    mention_block = "\n".join(capped_mentions)
    roster_lines = [
        f"{p['slug']} | {p.get('display_name', '')} | {p.get('party', '')}"
        for p in participants
    ]
    participant_roster = "\n".join(roster_lines)

    user_prompt = CHAPTER_SPEAKER_RESOLUTION_USER_TEMPLATE.format(
        mention_block=mention_block,
        participant_roster=participant_roster,
    )

    if completion_fn is None:
        from utils.llm_cache import cached_json_completion
        completion_fn = cached_json_completion

    response = completion_fn(
        CHAPTER_SPEAKER_RESOLUTION_SYSTEM_PROMPT,
        user_prompt,
        model="gpt-4o-mini",
        temperature=0.0,
        max_tokens=600,
    )

    if response.get("error") or not response.get("data"):
        logger.debug(
            "resolve_chapter_speakers: completion error: %s", response.get("error")
        )
        return ChapterSpeakerResolution()

    raw_matches = response["data"].get("matches") or []

    # Keep the FIRST raw entry per mention; ignore entries for mentions we
    # did not ask about (hallucinated or duplicated echoes).
    raw_by_mention: dict[str, dict] = {}
    for entry in raw_matches:
        mention = entry.get("mention")
        if mention in capped_mentions and mention not in raw_by_mention:
            raw_by_mention[mention] = entry

    matches: list[SpeakerMatch] = []
    by_mention: dict[str, SpeakerMatch] = {}
    for mention in capped_mentions:
        entry = raw_by_mention.get(mention)
        if entry is None:
            continue

        slug = entry.get("participant_slug")
        if not slug or slug not in roster_by_slug:
            logger.debug(
                "resolve_chapter_speakers: rejecting mention %r — slug %r not in roster",
                mention, slug,
            )
            continue

        try:
            confidence = float(entry.get("confidence"))
        except (TypeError, ValueError):
            logger.debug(
                "resolve_chapter_speakers: rejecting mention %r — invalid confidence %r",
                mention, entry.get("confidence"),
            )
            continue

        if confidence < CHAPTER_SPEAKER_MIN_CONFIDENCE:
            logger.debug(
                "resolve_chapter_speakers: rejecting mention %r — confidence %.2f < %.2f",
                mention, confidence, CHAPTER_SPEAKER_MIN_CONFIDENCE,
            )
            continue

        match = SpeakerMatch(
            mention=mention,
            participant_slug=slug,
            display_name=roster_by_slug[slug].get("display_name", ""),
            confidence=confidence,
            evidence=entry.get("evidence", ""),
        )
        matches.append(match)
        by_mention[mention] = match

    if matches:
        logger.info(
            "resolve_chapter_speakers: resolved %d/%d mention(s)",
            len(matches), len(capped_mentions),
        )

    return ChapterSpeakerResolution(matches=tuple(matches), by_mention=by_mention)
