"""Gate B: deterministic roster cross-check for turn speaker resolution (issue #321).

Before a resolved slug is persisted, cross-check its canonical roster
``display_name`` against the chapter's own ``key_speakers``/``speakers``
rosters. This is a pure, never-raising module — no I/O, no LLM.

Design constraints (issue #321):
- ``chapter_roster_mentions`` filters both arrays to non-placeholder mentions
  (reusing ``is_placeholder`` from #263) and dedups case-insensitively.
- ``crosscheck_slug`` classifies the outcome as ``"accept"``, ``"reject"``, or
  ``"no_opinion"`` (fail-open when there is nothing to compare against).
- Neither function raises on malformed input.
"""

from __future__ import annotations

import unicodedata

from rapidfuzz import fuzz

from congress_videos.modules.speaker_placeholders import is_placeholder

ROSTER_MATCH_MIN_RATIO: int = 85
"""Minimum rapidfuzz token_set_ratio (on significant tokens) to accept a match."""

COURTESY_TOKENS: frozenset[str] = frozenset(
    {
        "senor",
        "senora",
        "don",
        "dona",
        "de",
        "del",
        "la",
        "el",
        "los",
        "las",
        "y",
    }
)
"""Normalized (casefold, accent-stripped) Spanish courtesy/particle tokens that
must never, on their own, satisfy a roster match."""

_MIN_TOKEN_LEN: int = 3


def _normalize(text: str) -> str:
    """casefold -> NFD -> drop combining marks -> collapse whitespace."""
    folded = text.casefold()
    decomposed = unicodedata.normalize("NFD", folded)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.split())


def _significant_tokens(normalized_text: str) -> set[str]:
    """Tokens of length >= _MIN_TOKEN_LEN that are not courtesy tokens."""
    return {token for token in normalized_text.split() if len(token) >= _MIN_TOKEN_LEN and token not in COURTESY_TOKENS}


def _mention_name_from_entry(entry) -> str | None:
    """Extract a real mention name from one roster entry, or ``None``.

    Accepts a dict with a ``name`` key, or a plain ``str``. Any other type,
    a blank/whitespace-only name, or a known placeholder name yields
    ``None``. Never raises.
    """
    try:
        if isinstance(entry, dict):
            name = str(entry.get("name", ""))
        elif isinstance(entry, str):
            name = entry
        else:
            return None
    except Exception:
        return None
    name = name.strip()
    if not name:
        return None
    if is_placeholder(name):
        return None
    return name


def _dedupe_case_insensitive(names: list[str]) -> list[str]:
    """Order-preserving dedup of ``names``, keeping the first casing seen."""
    seen: set[str] = set()
    deduped: list[str] = []
    for name in names:
        key = name.casefold()
        if key in seen:
            continue
        seen.add(key)
        deduped.append(name)
    return deduped


def chapter_roster_mentions(key_speakers, speakers) -> list[str]:
    """Return the deduped, non-placeholder mentions from both roster arrays.

    Args:
        key_speakers: List of entries (str, dict with a ``name`` key, or
            arbitrary/malformed values), or falsy.
        speakers: Same shape as ``key_speakers``, or falsy.

    Returns:
        A list of real mention strings (original casing of the first
        occurrence kept), deduped case-insensitively. Never raises.
    """
    try:
        mentions: list[str] = []
        for array in (key_speakers, speakers):
            if not array:
                continue
            for entry in array:
                name = _mention_name_from_entry(entry)
                if name is None:
                    continue
                mentions.append(name)

        return _dedupe_case_insensitive(mentions)
    except Exception:
        return []


def crosscheck_slug(display_name: str, mentions: list[str]) -> str:
    """Classify whether ``display_name`` is supported by ``mentions``.

    Returns:
        ``"accept"`` when a mention shares a significant token with
        ``display_name`` or is a close fuzzy match; ``"no_opinion"`` when
        there is nothing to compare against (fail-open); ``"reject"``
        otherwise. Never raises.
    """
    try:
        if not mentions or not display_name:
            return "no_opinion"

        display_tokens = _significant_tokens(_normalize(display_name))

        for mention in mentions:
            if not isinstance(mention, str) or not mention.strip():
                continue
            mention_tokens = _significant_tokens(_normalize(mention))
            if not mention_tokens:
                continue
            if display_tokens & mention_tokens:
                return "accept"
            ratio = fuzz.token_set_ratio(
                " ".join(sorted(mention_tokens)),
                " ".join(sorted(display_tokens)),
            )
            if ratio >= ROSTER_MATCH_MIN_RATIO:
                return "accept"

        return "reject"
    except Exception:
        return "no_opinion"
