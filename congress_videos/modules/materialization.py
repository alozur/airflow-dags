"""Pure planning module for speaker-turn video materialization.

Derives ``MaterializationPlan`` records from turn rows and pre-filtered
approved trim proposals. Contains **no ffmpeg, no database, no Airflow
imports** — all logic is deterministic and unit-testable with plain dicts.

The planner decides *what* to cut; the execution layer (PR2) decides *how*
to cut it using ffmpeg helpers.

Usage::

    plans = plan_turn_materialization(turn_rows, approved_trim_rows)
    for plan in plans:
        ...  # delegate to execution layer

Where ``turn_rows`` are dicts with keys:
    turn_id, chapter_id, start_seconds, end_seconds

And ``approved_trim_rows`` are dicts with keys:
    turn_id, start_seconds, end_seconds, is_approved, is_voice_free
"""

from __future__ import annotations

from dataclasses import dataclass

__all__ = [
    "KeepInterval",
    "MaterializationPlan",
    "MIN_LONG_INTERVENTION_SECS",
    "GROUP_GAP_TOLERANCE_SECS",
    "plan_turn_materialization",
]

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

MIN_LONG_INTERVENTION_SECS: float = 300.0
"""Minimum duration (seconds) for a speaker turn to be treated as a
long intervention requiring its own individual output video."""

GROUP_GAP_TOLERANCE_SECS: float = 0.5
"""Maximum gap (seconds) between two consecutive short turns in the same
chapter that still allows them to be grouped into a single continuous cut."""


# ---------------------------------------------------------------------------
# Frozen dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeepInterval:
    """A single continuous time window to retain in the output video.

    Attributes:
        start: Absolute start of the interval in seconds.
        end:   Absolute end of the interval in seconds.
    """

    start: float
    end: float


@dataclass(frozen=True)
class MaterializationPlan:
    """Complete materialization recipe for one output video.

    Attributes:
        turn_ids:       Tuple of constituent turn_ids (>1 only for grouped
                        short turns).
        chapter_id:     chapter_id shared by all turns in this plan.
        keep_intervals: Ordered, non-overlapping intervals to retain.
                        A single element means stream-copy is safe (no excision).
        output_turn_id: Naming key — always the first (lowest) turn_id in the plan.
        needs_reencode: True when excision forces re-encoding (len(keep_intervals) > 1).
                        AV1-forced re-encoding is determined at execution time.
    """

    turn_ids: tuple[int, ...]
    chapter_id: int
    keep_intervals: tuple[KeepInterval, ...]
    output_turn_id: int
    needs_reencode: bool


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _with_float_seconds(row: dict) -> dict:
    """Return a copy of ``row`` with second fields coerced to float.

    Postgres NUMERIC columns are delivered as ``decimal.Decimal`` by
    psycopg2; mixing them with float tolerances raises TypeError.
    """
    return {
        **row,
        "start_seconds": float(row["start_seconds"]),
        "end_seconds": float(row["end_seconds"]),
    }


def _compute_keep_intervals(
    turn_start: float,
    turn_end: float,
    approved_trims: list[dict],
) -> tuple[KeepInterval, ...]:
    """Invert approved trim intervals within [turn_start, turn_end].

    Produces the ordered list of keeper windows by walking through the
    sorted trim list and emitting the gaps. Zero-duration segments
    (when a trim exactly touches a boundary) are dropped.

    Args:
        turn_start:     Absolute start of the turn in seconds.
        turn_end:       Absolute end of the turn in seconds.
        approved_trims: Already-filtered approved+voice-free trim dicts.

    Returns:
        Tuple of KeepInterval in chronological order.
    """
    if not approved_trims:
        return (KeepInterval(turn_start, turn_end),)

    # Sort trims by start_seconds; clamp to turn bounds
    sorted_trims = sorted(approved_trims, key=lambda t: t["start_seconds"])

    intervals: list[KeepInterval] = []
    cursor = turn_start

    for trim in sorted_trims:
        t_start = max(trim["start_seconds"], turn_start)
        t_end = min(trim["end_seconds"], turn_end)
        if t_start >= t_end:
            continue  # trim is outside turn bounds after clamping

        if cursor < t_start:
            # Emit the gap before this trim
            intervals.append(KeepInterval(cursor, t_start))
        cursor = max(cursor, t_end)

    # Trailing segment after the last trim
    if cursor < turn_end:
        intervals.append(KeepInterval(cursor, turn_end))

    return tuple(intervals)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def plan_turn_materialization(
    turns: list[dict],
    approved_trims: list[dict],
    *,
    min_long_secs: float = MIN_LONG_INTERVENTION_SECS,
) -> list[MaterializationPlan]:
    """Derive a list of materialization plans from turn rows and approved trims.

    Segmentation rules:
    1. Turns are sorted by (chapter_id, start_seconds).
    2. A turn whose duration >= ``min_long_secs`` gets its own plan with
       approved trim intervals excised.
    3. Consecutive short turns (<``min_long_secs``) sharing the same
       ``chapter_id`` are grouped while the gap between adjacent turns
       is <= ``GROUP_GAP_TOLERANCE_SECS``. A long turn or chapter boundary
       flushes the current group.
    4. Only trim proposals with ``is_approved=True AND is_voice_free=True``
       are applied; all others are silently ignored.

    Args:
        turns:          List of turn dicts (turn_id, chapter_id,
                        start_seconds, end_seconds).
        approved_trims: List of trim proposal dicts (turn_id, start_seconds,
                        end_seconds, is_approved, is_voice_free). The function
                        filters this set internally — callers may pass the full
                        proposal list for a chapter.
        min_long_secs:  Override for the long-turn threshold (defaults to
                        ``MIN_LONG_INTERVENTION_SECS``).

    Returns:
        Ordered list of ``MaterializationPlan`` records, one per output video.
    """
    # Postgres NUMERIC columns arrive as decimal.Decimal; coerce every
    # second field to float on copies so interval math never mixes types.
    turns = [_with_float_seconds(t) for t in turns]
    approved_trims = [_with_float_seconds(t) for t in approved_trims]

    # Filter to only approved + voice-free proposals, indexed by turn_id
    effective_trims: dict[int, list[dict]] = {}
    for trim in approved_trims:
        if trim.get("is_approved") and trim.get("is_voice_free"):
            effective_trims.setdefault(trim["turn_id"], []).append(trim)

    # Sort turns by (chapter_id, start_seconds)
    sorted_turns = sorted(
        turns,
        key=lambda t: (t["chapter_id"], t["start_seconds"]),
    )

    plans: list[MaterializationPlan] = []

    # Group accumulator for consecutive short turns
    group: list[dict] = []

    def _flush_group(g: list[dict]) -> None:
        """Emit a single plan for the accumulated short-turn group."""
        if not g:
            return
        group_start = g[0]["start_seconds"]
        group_end = g[-1]["end_seconds"]
        turn_ids = tuple(t["turn_id"] for t in g)
        plans.append(
            MaterializationPlan(
                turn_ids=turn_ids,
                chapter_id=g[0]["chapter_id"],
                keep_intervals=(KeepInterval(group_start, group_end),),
                output_turn_id=turn_ids[0],
                needs_reencode=False,
            )
        )

    for turn in sorted_turns:
        duration = turn["end_seconds"] - turn["start_seconds"]
        is_long = duration >= min_long_secs

        if is_long:
            # Long turn flushes any accumulated short group, then goes solo
            _flush_group(group)
            group = []

            trims_for_turn = effective_trims.get(turn["turn_id"], [])
            keep = _compute_keep_intervals(
                turn["start_seconds"],
                turn["end_seconds"],
                trims_for_turn,
            )
            plans.append(
                MaterializationPlan(
                    turn_ids=(turn["turn_id"],),
                    chapter_id=turn["chapter_id"],
                    keep_intervals=keep,
                    output_turn_id=turn["turn_id"],
                    needs_reencode=len(keep) > 1,
                )
            )
        else:
            # Short turn: check if it belongs to the current group
            if group:
                prev = group[-1]
                same_chapter = prev["chapter_id"] == turn["chapter_id"]
                within_gap = (
                    turn["start_seconds"] <= prev["end_seconds"] + GROUP_GAP_TOLERANCE_SECS
                )
                if same_chapter and within_gap:
                    group.append(turn)
                else:
                    # Gap too large or different chapter — flush and start new group
                    _flush_group(group)
                    group = [turn]
            else:
                group.append(turn)

    # Flush any remaining short-turn group
    _flush_group(group)

    return plans
