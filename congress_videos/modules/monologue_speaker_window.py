"""Two-step monologue speaker resolution (issue #430).

Non-qa turns resolve from the pre-turn announcement window ONLY — never from
the turn's own transcript, so a person merely addressed early in the turn
cannot win the attribution. Step 1 sees the window text alone; Step 2 sees
only Step 1's small JSON plus the participant roster.

Slice A1 (this file, as first created) carries only the window-selection
primitives: nothing imports this module yet, so it is inert. Later slices
(A2a/A2b) add `identify_floor_holder`, `resolve_announced_identity`,
`build_resolution_audit`, and the never-raise `resolve_monologue_speaker`
orchestrator described in design.md.

Design constraints (see openspec/changes/monologue-speaker-window/design.md):
- No import-time side effects — this module lives under the DAGs folder and
  is walked by the DagBag.
- Absolute imports only.
"""

from __future__ import annotations

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
