"""Pure mentioned-people resolution module (issue #432).

Identifies known `congress_participants` MENTIONED within a chapter's SRT
text — distinct from the chapter *speaker* (issue #263) and from topic
extraction. Structurally cloned from `chapter_speaker_resolution`: never
raises, roster- and confidence-gated (0.80), display_name from the roster
only, completion_fn defaults to cached_json_completion.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from congress_videos.config.ai_prompts import (
    MENTIONED_PEOPLE_SYSTEM_PROMPT,
    MENTIONED_PEOPLE_USER_TEMPLATE,
)
from utils.llm_config import LLM_CHEAP

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants (tested directly by spec)
# ---------------------------------------------------------------------------

MENTIONED_PEOPLE_MIN_CONFIDENCE: float = 0.80
"""Minimum model confidence required to accept a resolved mention."""

MAX_MENTIONED_PEOPLE: int = 12
"""Upper bound on distinct mentioned people persisted per chapter."""

MENTIONED_PEOPLE_MAX_CHARS: int = 20_000
"""Chapter SRT text is truncated to this many characters before the call."""


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MentionedPerson:
    """A single resolved mentioned-person match."""

    mention: str
    participant_slug: str
    display_name: str
    confidence: float
    evidence: str = ""


@dataclass(frozen=True)
class MentionedPeopleResult:
    """Return value of :func:`resolve_mentioned_people`."""

    ok: bool = False
    """True only for a parsed, well-formed completion response."""

    people: tuple[MentionedPerson, ...] = ()
    """Accepted mentions, deduplicated by slug, first-seen order."""

    dropped_mentions: tuple[str, ...] = ()
    """Raw mention strings dropped by the roster or confidence gate."""

    @property
    def slugs(self) -> tuple[str, ...]:
        """Deduplicated participant_slug values, first-seen order."""
        return tuple(person.participant_slug for person in self.people)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def resolve_mentioned_people(
    srt_text: str,
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> MentionedPeopleResult:
    """Resolve people MENTIONED in *srt_text* against *participants*.

    This function NEVER raises. All internal exceptions are caught and
    logged, degrading to an empty :class:`MentionedPeopleResult` (ok=False).

    Args:
        srt_text: Persisted chapter SRT transcript text.
        participants: Roster of known participants, each a dict with at
            minimum ``slug`` and ``display_name`` keys (and optionally
            ``party``).
        completion_fn: Optional override for the LLM completion call.
            Defaults to ``utils.llm_cache.cached_json_completion``. Must
            accept ``(system_prompt, user_prompt, **kwargs)`` and return a
            dict with ``data`` and ``error`` keys.

    Returns:
        A :class:`MentionedPeopleResult`. ``ok`` distinguishes a successful
        call that found nobody mentioned from a failed/malformed call.
    """
    try:
        return _resolve_inner(srt_text, participants, completion_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "resolve_mentioned_people: unexpected exception — returning empty result (%s: %s)",
            type(exc).__name__,
            exc,
        )
        return MentionedPeopleResult()


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _resolve_inner(
    srt_text: str,
    participants: list[dict],
    completion_fn: Callable | None,
) -> MentionedPeopleResult:
    """Internal resolver (may raise; wrapped by resolve_mentioned_people)."""
    if not srt_text or not participants:
        logger.debug("resolve_mentioned_people: empty text or empty roster — skipping call")
        return MentionedPeopleResult()

    truncated_text = srt_text[:MENTIONED_PEOPLE_MAX_CHARS]
    roster_by_slug: dict[str, dict] = {p["slug"]: p for p in participants}

    roster_lines = [f"{p['slug']} | {p.get('display_name', '')} | {p.get('party', '')}" for p in participants]
    participant_roster = "\n".join(roster_lines)

    user_prompt = MENTIONED_PEOPLE_USER_TEMPLATE.format(
        srt_text=truncated_text,
        participant_roster=participant_roster,
    )

    if completion_fn is None:
        from utils.llm_cache import cached_json_completion

        completion_fn = cached_json_completion

    response = completion_fn(MENTIONED_PEOPLE_SYSTEM_PROMPT, user_prompt, model=LLM_CHEAP)

    if response.get("error") or response.get("data") is None:
        logger.debug("resolve_mentioned_people: completion error: %s", response.get("error"))
        return MentionedPeopleResult()

    raw_mentions = response["data"].get("mentions") or []

    people: list[MentionedPerson] = []
    seen_slugs: set[str] = set()
    dropped: list[str] = []
    for entry in raw_mentions:
        name = entry.get("name", "")
        slug = entry.get("participant_slug")
        if not slug or slug not in roster_by_slug:
            logger.info(
                "resolve_mentioned_people: dropping mention %r — slug %r not in roster",
                name,
                slug,
            )
            dropped.append(name)
            continue

        try:
            confidence = float(entry.get("confidence"))
        except (TypeError, ValueError):
            logger.info(
                "resolve_mentioned_people: dropping mention %r — invalid confidence %r",
                name,
                entry.get("confidence"),
            )
            dropped.append(name)
            continue

        if confidence < MENTIONED_PEOPLE_MIN_CONFIDENCE:
            logger.info(
                "resolve_mentioned_people: dropping mention %r — confidence %.2f < %.2f",
                name,
                confidence,
                MENTIONED_PEOPLE_MIN_CONFIDENCE,
            )
            dropped.append(name)
            continue

        if slug in seen_slugs:
            continue
        seen_slugs.add(slug)

        people.append(
            MentionedPerson(
                mention=name,
                participant_slug=slug,
                display_name=roster_by_slug[slug].get("display_name", ""),
                confidence=confidence,
                evidence=entry.get("evidence", ""),
            )
        )

        if len(people) >= MAX_MENTIONED_PEOPLE:
            break

    return MentionedPeopleResult(ok=True, people=tuple(people), dropped_mentions=tuple(dropped))
