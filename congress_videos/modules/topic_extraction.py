"""Pure topic-extraction module (issue #432).

Derives normalized subject-matter topics for a chapter from a dedicated LLM
call, independent of mentioned-people resolution and of speaker resolution.
Structurally mirrors ``mentioned_people_resolution``: never raises,
completion_fn defaults to cached_json_completion, normalized/deduplicated/
capped output.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from congress_videos.config.ai_prompts import (
    TOPIC_EXTRACTION_SYSTEM_PROMPT,
    TOPIC_EXTRACTION_USER_TEMPLATE,
)
from utils.llm_config import LLM_CHEAP

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants (tested directly by spec)
# ---------------------------------------------------------------------------

MAX_TOPICS: int = 8
"""Upper bound on distinct topics persisted per chapter."""

MAX_TOPIC_CHARS: int = 60
"""A normalized topic longer than this is rejected as a sentence, not a label."""

TOPICS_MAX_CHARS: int = 20_000
"""Chapter SRT text is truncated to this many characters before the call."""


# ---------------------------------------------------------------------------
# Public data types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TopicsResult:
    """Return value of :func:`extract_topics`."""

    ok: bool = False
    """True only for a parsed, well-formed completion response."""

    topics: tuple[str, ...] = ()
    """Normalized topics, deduplicated, first-seen order, capped at MAX_TOPICS."""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_topics(srt_text: str, completion_fn: Callable | None = None) -> TopicsResult:
    """Extract subject-matter topics discussed in *srt_text*.

    This function NEVER raises. All internal exceptions are caught and
    logged, degrading to an empty :class:`TopicsResult` (ok=False).

    Args:
        srt_text: Persisted chapter SRT transcript text.
        completion_fn: Optional override for the LLM completion call.
            Defaults to ``utils.llm_cache.cached_json_completion``. Must
            accept ``(system_prompt, user_prompt, **kwargs)`` and return a
            dict with ``data`` and ``error`` keys.

    Returns:
        A :class:`TopicsResult`. ``ok`` distinguishes a successful call
        that found no topics from a failed/malformed call.
    """
    try:
        return _extract_inner(srt_text, completion_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "extract_topics: unexpected exception — returning empty result (%s: %s)",
            type(exc).__name__,
            exc,
        )
        return TopicsResult()


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------


def _extract_inner(srt_text: str, completion_fn: Callable | None) -> TopicsResult:
    """Internal extractor (may raise; wrapped by extract_topics)."""
    truncated_text = srt_text[:TOPICS_MAX_CHARS]

    if completion_fn is None:
        from utils.llm_cache import cached_json_completion

        completion_fn = cached_json_completion

    user_prompt = TOPIC_EXTRACTION_USER_TEMPLATE.format(srt_text=truncated_text)
    response = completion_fn(TOPIC_EXTRACTION_SYSTEM_PROMPT, user_prompt, model=LLM_CHEAP)

    if response.get("error") or response.get("data") is None:
        logger.debug("extract_topics: completion error: %s", response.get("error"))
        return TopicsResult()

    raw_topics = response["data"].get("topics") or []
    if not isinstance(raw_topics, list):
        logger.debug("extract_topics: 'topics' field is not a list: %r", raw_topics)
        return TopicsResult()

    topics: list[str] = []
    seen: set[str] = set()
    for raw in raw_topics:
        normalized = " ".join(str(raw).strip().lower().split())
        if len(normalized) > MAX_TOPIC_CHARS:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        topics.append(normalized)
        if len(topics) >= MAX_TOPICS:
            break

    return TopicsResult(ok=True, topics=tuple(topics))
