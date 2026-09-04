"""Two-step monologue speaker resolution (issue #430).

Non-qa turns resolve from the pre-turn announcement window ONLY — never from
the turn's own transcript, so a person merely addressed early in the turn
cannot win the attribution. Step 1 sees the window text alone; Step 2 sees
only Step 1's small JSON plus the participant roster.

Slice A1 added the window-selection primitives. Slice A2a (this file, as
extended) adds the two LLM-step seam functions: `identify_floor_holder`
(Step 1) and `resolve_announced_identity` (Step 2). Each is a pass-through
seam — it validates and shapes whatever `completion_fn` returns but never
catches an exception `completion_fn` raises; the never-raise contract is
the orchestrator's job (`resolve_monologue_speaker`, added in A2b). Nothing
imports this module's public functions yet, so it remains inert until A2b
wires them together and C routes turns to it.

Design constraints (see openspec/changes/monologue-speaker-window/design.md):
- No import-time side effects — this module lives under the DAGs folder and
  is walked by the DagBag.
- Absolute imports only.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from congress_videos.config.ai_prompts import (
    MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT,
    MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE,
    MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT,
    MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE,
)
from congress_videos.modules.speaker_resolution import SPEAKER_RESOLUTION_MIN_CONFIDENCE
from utils.llm_config import LLM_CHEAP

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

MONOLOGUE_WINDOW_SECS: int = 120
"""Seconds before the turn anchor to include as the preceding announcement
window. Fixed per design.md — not a parameter."""

MONOLOGUE_RESOLUTION_METHOD: str = "monologue_window_v1"
"""Value written to the audit JSON's ``method`` key and to
``speaker_resolution_method`` when this resolver produces a result."""


def turn_anchor_seconds(turn: dict) -> float:
    """Return the anchor second used to derive the preceding window.

    ``group_start_seconds`` wins whenever it is present and not ``None``
    (issue #283 grouping semantics) — including ``0.0``, which must NOT be
    treated as falsy. Otherwise falls back to the turn's own
    ``start_seconds``.
    """
    group_start_seconds = turn.get("group_start_seconds")
    if group_start_seconds is not None:
        return float(group_start_seconds)
    return float(turn["start_seconds"])


def select_preceding_window(
    blocks: list[dict],
    anchor_seconds: float,
    window_seconds: int = MONOLOGUE_WINDOW_SECS,
) -> list[dict]:
    """Select the SRT blocks that make up the preceding announcement window.

    A block is selected iff ``window_start <= block["start_secs"] <
    anchor_seconds``, where ``window_start = max(0, anchor_seconds -
    window_seconds)``. Selection is by start time only, so a block that
    overlaps the anchor (starts before it, ends after it) is still included
    — but nothing starting at or after the anchor is ever included.
    """
    window_start = max(0.0, anchor_seconds - window_seconds)
    return [block for block in blocks if window_start <= block["start_secs"] < anchor_seconds]


# ---------------------------------------------------------------------------
# Step results (frozen; the sentinel — all-defaults — means "unresolved")
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FloorHolder:
    """Step-1 result: who the presiding officer just gave the floor to.

    The all-defaults instance (``FloorHolder()``) is the INTERNAL unresolved
    sentinel — the public `resolve_monologue_speaker` wrapper (A2b) maps it
    to ``None``.
    """

    announced_name_or_role: str = ""
    evidence: str = ""
    found: bool = False


@dataclass(frozen=True)
class AnnouncedIdentity:
    """Step-2 result: the roster-backed identity for a Step-1 announcement.

    The all-defaults instance (``AnnouncedIdentity()``) is the INTERNAL
    unresolved sentinel, same convention as `FloorHolder`.
    """

    full_name: str = ""
    participant_slug: str | None = None
    confidence: float = 0.0


# ---------------------------------------------------------------------------
# Step 1 — floor-holder identification (window text only, no roster)
# ---------------------------------------------------------------------------


def identify_floor_holder(
    window_blocks: list[dict],
    completion_fn: Callable | None = None,
) -> FloorHolder:
    """Ask the model who is being given the floor next, from window text only.

    A pass-through seam: shapes whatever `completion_fn` returns into a
    `FloorHolder`. A completion `error`, or `data` missing/not a dict, is an
    ordinary degraded outcome (not an exception) — it logs one WARNING and
    returns the unresolved sentinel. An exception raised by `completion_fn`
    itself is NOT caught here; the never-raise contract belongs to the
    orchestrator (`resolve_monologue_speaker`, A2b).
    """
    window_text = "\n".join(block["text"] for block in window_blocks) if window_blocks else ""
    user_prompt = MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE.format(window_text=window_text)

    if completion_fn is None:
        from utils.llm_cache import cached_json_completion

        completion_fn = cached_json_completion

    response = completion_fn(MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT, user_prompt, model=LLM_CHEAP)

    data = response.get("data")
    if response.get("error") or not isinstance(data, dict):
        logger.warning(
            "identify_floor_holder: step 1 completion error: %s — returning unresolved",
            response.get("error"),
        )
        return FloorHolder()

    return FloorHolder(
        announced_name_or_role=data.get("announced_name_or_role") or "",
        evidence=data.get("evidence") or "",
        found=bool(data.get("found")),
    )


# ---------------------------------------------------------------------------
# Step 2 — roster-backed identity resolution (announcement + evidence only)
# ---------------------------------------------------------------------------


def resolve_announced_identity(
    floor_holder: FloorHolder,
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> AnnouncedIdentity:
    """Ask the model which roster participant Step 1's announcement names.

    A pass-through seam like `identify_floor_holder`, plus the roster and
    confidence gates: the slug MUST exist in `participants` and confidence
    MUST be >= `SPEAKER_RESOLUTION_MIN_CONFIDENCE`, or the result is the
    unresolved sentinel — never a guessed slug. Errors and raises are
    handled exactly as in `identify_floor_holder`.
    """
    roster_lines = [f"{p['slug']} | {p.get('display_name', '')} | {p.get('party', '')}" for p in participants]
    user_prompt = MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE.format(
        announced_name_or_role=floor_holder.announced_name_or_role,
        evidence=floor_holder.evidence,
        participant_roster="\n".join(roster_lines),
    )

    if completion_fn is None:
        from utils.llm_cache import cached_json_completion

        completion_fn = cached_json_completion

    response = completion_fn(MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT, user_prompt, model=LLM_CHEAP)

    data = response.get("data")
    if response.get("error") or not isinstance(data, dict):
        logger.warning(
            "resolve_announced_identity: step 2 completion error: %s — returning unresolved",
            response.get("error"),
        )
        return AnnouncedIdentity()

    return _validate_announced_identity(data, participants)


def _validate_announced_identity(data: dict, participants: list[dict]) -> AnnouncedIdentity:
    """Roster and confidence gates for a well-formed Step-2 response body."""
    valid_slugs = {p["slug"] for p in participants}
    slug = data.get("participant_slug")
    if not slug or slug not in valid_slugs:
        logger.warning(
            "resolve_announced_identity: step 2 returned slug %r not in roster — returning unresolved",
            slug,
        )
        return AnnouncedIdentity()

    try:
        confidence = float(data.get("confidence"))
    except (TypeError, ValueError):
        logger.warning(
            "resolve_announced_identity: invalid confidence %r — returning unresolved",
            data.get("confidence"),
        )
        return AnnouncedIdentity()

    if confidence < SPEAKER_RESOLUTION_MIN_CONFIDENCE:
        logger.info(
            "resolve_announced_identity: confidence %.2f < %.2f — returning unresolved",
            confidence,
            SPEAKER_RESOLUTION_MIN_CONFIDENCE,
        )
        return AnnouncedIdentity()

    return AnnouncedIdentity(full_name=data.get("full_name") or "", participant_slug=slug, confidence=confidence)
