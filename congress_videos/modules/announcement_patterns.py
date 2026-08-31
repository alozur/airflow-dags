"""Shared president-announcement phrase detection (issue #284).

Both the speaker-turn detector and the speaker-resolution module need to
decide, from Spanish congressional transcript text, whether a segment is
introduced by the presiding officer. This module owns the three matching
patterns and the single predicate both consumers call, so the matching
rules live in exactly one place.

Pure: no DB, no ffmpeg, no orchestration imports. Kept free of the two
scheduler-parsing trigger words so it stays a safe module to import from
the parsed folder.
"""

from __future__ import annotations

import re

__all__ = [
    "RE_NAMED",
    "RE_SU_SENORIA",
    "RE_GRACIAS_SENORIA",
    "has_announcement_phrase",
]

# Pattern capturing the name after "el señor / la señora".
RE_NAMED = re.compile(
    r"tiene\s+la\s+palabra\s+(?:el\s+se[nñ]or|la\s+se[nñ]ora)\s+"
    r"(?P<name>[\wÀ-ɏ.\- ]+?)(?:[.,]|\s*$)",
    re.IGNORECASE,
)

# Phrase-only patterns (no name captured).
RE_SU_SENORIA = re.compile(
    r"tiene\s+la\s+palabra\s+su\s+se[nñ]or[íi]a",
    re.IGNORECASE,
)

RE_GRACIAS_SENORIA = re.compile(
    r"gracias,?\s+se[nñ]or[íi]a",
    re.IGNORECASE,
)


def has_announcement_phrase(text: str | None) -> bool:
    """True when text contains any presiding-officer announcement phrase.

    Never raises; None/empty input returns False. Disjunction of the three
    module-level patterns, applied directly to the raw text — each pattern
    is already accent- and case-tolerant via ``[nñ]``/``[íi]``/``IGNORECASE``.
    """
    if not text:
        return False
    return bool(
        RE_NAMED.search(text)
        or RE_SU_SENORIA.search(text)
        or RE_GRACIAS_SENORIA.search(text)
    )
