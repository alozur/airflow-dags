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

from congress_videos.modules.speaker_placeholders import is_placeholder
from congress_videos.modules.speaker_turns import collapse_foreign_runs, drop_micro_segments

__all__ = [
    "KeepInterval",
    "MaterializationPlan",
    "MIN_LONG_INTERVENTION_SECS",
    "GROUP_GAP_TOLERANCE_SECS",
    "MONOLOGUE",
    "QA",
    "classify_turn_type",
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

MONOLOGUE: str = "monologue"
"""Turn-type value when a group has fewer than 2 distinct real speakers."""

QA: str = "qa"
"""Turn-type value when a group spans 2 or more distinct real (non-placeholder,
non-NULL) resolved_names."""


# ---------------------------------------------------------------------------
# Pure classifier
# ---------------------------------------------------------------------------


def classify_turn_type(
    turn_ids: tuple[int, ...] | list[int],
    resolved_by_id: dict[int, str | None],
    turn_rows_by_id: dict[int, dict],
) -> str:
    """Return 'qa' if the group has >=2 distinct real speakers; 'monologue' otherwise.

    Rule order (issue #282 — label-first, breaking signature change):
    1. A solo turn (len==1) always returns 'monologue'.
    2. Label path: rows for turn_ids with a usable ``speaker_label`` and
       numeric ``start_seconds``/``end_seconds`` are sorted by start time,
       then run through the same noise filters #283 already applies
       (``drop_micro_segments`` -> ``collapse_foreign_runs``, imported as
       public aliases from ``speaker_turns``). >=2 distinct surviving
       labels -> 'qa', regardless of resolved-name state.
    3. Name fallback (pre-#282 behaviour): used only when fewer than 2
       usable rows exist, or fewer than 2 substantial labels survive
       filtering. >=2 distinct non-placeholder, non-NULL resolved_names
       -> 'qa'; else 'monologue'.

    Never performs I/O. Never raises — every input hazard (missing row,
    None/blank label, non-numeric seconds, Decimal seconds) is handled by
    an explicit guard rather than a broad except.

    Args:
        turn_ids:        Ordered sequence of turn_ids in this materialization group.
        resolved_by_id:  Mapping of turn_id → resolved_name (None for unknown).
        turn_rows_by_id: Mapping of turn_id → row dict with at least
                         ``speaker_label``, ``start_seconds``, ``end_seconds``.

    Returns:
        MONOLOGUE or QA.
    """
    if len(turn_ids) == 1:
        return MONOLOGUE

    label_result = _classify_by_label(turn_ids, turn_rows_by_id)
    if label_result is not None:
        return label_result

    return _classify_by_name(turn_ids, resolved_by_id)


def _classify_by_name(
    turn_ids: tuple[int, ...] | list[int],
    resolved_by_id: dict[int, str | None],
) -> str:
    """Legacy fallback: 'qa' when >=2 distinct real resolved_names exist."""
    distinct_real: set[str] = set()
    for tid in turn_ids:
        name = resolved_by_id.get(tid)
        if name is None:
            continue
        stripped = name.strip()
        if not stripped:
            continue
        if is_placeholder(stripped):
            continue
        distinct_real.add(stripped)

    return QA if len(distinct_real) >= 2 else MONOLOGUE


def _classify_by_label(
    turn_ids: tuple[int, ...] | list[int],
    turn_rows_by_id: dict[int, dict],
) -> str | None:
    """Return QA when >=2 substantial distinct speaker_label values survive
    noise filtering; None when the label path is inconclusive (fewer than
    2 usable rows, or fewer than 2 substantial labels after filtering) —
    the caller must fall through to the name-based rule in that case.
    """
    usable_rows: list[dict] = []
    for tid in turn_ids:
        row = turn_rows_by_id.get(tid)
        if row is None:
            continue

        label = row.get("speaker_label")
        if not isinstance(label, str) or not label.strip():
            continue

        start = row.get("start_seconds")
        end = row.get("end_seconds")
        if start is None or end is None:
            continue
        try:
            start_f = float(start)
            end_f = float(end)
        except (TypeError, ValueError):
            continue

        usable_rows.append(
            {
                "start_seconds": start_f,
                "end_seconds": end_f,
                "speaker_label": label.strip(),
            }
        )

    if len(usable_rows) < 2:
        return None

    usable_rows.sort(key=lambda r: r["start_seconds"])
    filtered = drop_micro_segments(usable_rows)
    filtered = collapse_foreign_runs(filtered)

    distinct_labels = {r["speaker_label"] for r in filtered}
    return QA if len(distinct_labels) >= 2 else None


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
    span_start: float,
    span_end: float,
    cuts: list[dict],
) -> tuple[KeepInterval, ...]:
    """Invert cut intervals within [span_start, span_end].

    Generic interval inversion over ``{start_seconds, end_seconds}`` dicts —
    used both for approved trim proposals excised from a single long turn
    and for procedural member turns excised from a grouped short-turn clip
    (issue #143). Produces the ordered list of keeper windows by walking
    through the sorted cut list and emitting the gaps. Zero-duration segments
    (when a cut exactly touches a boundary) are dropped. When the cuts cover
    the whole span, returns an empty tuple (the degenerate all-cut case).

    Args:
        span_start: Absolute start of the span in seconds.
        span_end:   Absolute end of the span in seconds.
        cuts:       Already-filtered dicts to excise (approved+voice-free
                    trims, or procedural member turns).

    Returns:
        Tuple of KeepInterval in chronological order. Empty when fully cut.
    """
    if not cuts:
        return (KeepInterval(span_start, span_end),)

    # Sort cuts by start_seconds; clamp to span bounds
    sorted_cuts = sorted(cuts, key=lambda t: t["start_seconds"])

    intervals: list[KeepInterval] = []
    cursor = span_start

    for trim in sorted_cuts:
        t_start = max(trim["start_seconds"], span_start)
        t_end = min(trim["end_seconds"], span_end)
        if t_start >= t_end:
            continue  # cut is outside span bounds after clamping

        if cursor < t_start:
            # Emit the gap before this trim
            intervals.append(KeepInterval(cursor, t_start))
        cursor = max(cursor, t_end)

    # Trailing segment after the last cut
    if cursor < span_end:
        intervals.append(KeepInterval(cursor, span_end))

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
        """Emit a single plan for the accumulated short-turn group.

        Procedural member turns (issue #143) are excised via the same
        interval-inverter used for approved trims: their spans are fed to
        ``_compute_keep_intervals`` as cuts, wherever they occur (start,
        middle, or end of the group). ``turn_ids`` always lists EVERY group
        member — including excised ones — so downstream idempotency
        (mark_turns_uploaded / select_turns NOT EXISTS) still treats them as
        handled. When every member is procedural, the cuts cover the whole
        span and ``_compute_keep_intervals`` returns an empty tuple: no plan
        is emitted for this group at all (the degenerate case; the DAG layer
        is responsible for still recording those turns — issue #143 D5).
        """
        if not g:
            return
        group_start = g[0]["start_seconds"]
        group_end = g[-1]["end_seconds"]
        turn_ids = tuple(t["turn_id"] for t in g)
        cuts = [t for t in g if t.get("is_procedural")]
        keep = _compute_keep_intervals(group_start, group_end, cuts)
        if not keep:
            return
        plans.append(
            MaterializationPlan(
                turn_ids=turn_ids,
                chapter_id=g[0]["chapter_id"],
                keep_intervals=keep,
                output_turn_id=turn_ids[0],
                needs_reencode=len(keep) > 1,
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
                within_gap = turn["start_seconds"] <= prev["end_seconds"] + GROUP_GAP_TOLERANCE_SECS
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
