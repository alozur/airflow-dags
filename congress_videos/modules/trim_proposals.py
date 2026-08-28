"""Per-turn silence and applause trim proposals.

Pure orchestrator module. Derives non-destructive, auditable trim candidates
from injectable ``VadFn`` and ``YamnetFn`` callables — no real ML model, no
Docker, no subprocess is imported here. All heavy I/O collaborators (VAD
backend, YAMNet Docker wrapper, DB connection) are injected by the caller.

The hard invariant is:
    **Voice always prevails** — a proposal survives only if its interval
    intersects ZERO confirmed voiced segments (``is_voice_free`` gate).

Nothing is ever cut automatically. ``_upsert_proposals`` persists advisory
records only; no media file is touched.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Callable

from congress_videos.modules.sidecar_api_error import SidecarApiError

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Type aliases for injectable callables
# ---------------------------------------------------------------------------

# input:  (wav_path: str, offset: float)
# output: list of (start_seconds, end_seconds) absolute voiced intervals
VadFn = Callable[[str, float], list[tuple[float, float]]]

# input:  (wav_path: str, offset: float)
# output: list of {start, end, max_score} dicts
YamnetFn = Callable[[str, float], list[dict]]

# ---------------------------------------------------------------------------
# Module-level constants (tunable thresholds)
# ---------------------------------------------------------------------------

DEFAULT_MIN_DURATION_SECS: float = 3.0
"""Minimum gap/applause duration (seconds) to emit a proposal."""

DEFAULT_APPLAUSE_SCORE_THRESHOLD: float = 0.5
"""Minimum YAMNet max_score to accept an applause candidate."""

VAD_SOURCE: str = "vad_webrtc"
"""Source identifier written to trim proposals generated from VAD gaps."""

YAMNET_SOURCE: str = "yamnet_tflite"
"""Source identifier written to trim proposals generated from YAMNet detection."""

# ---------------------------------------------------------------------------
# TrimProposal dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TrimProposal:
    """A single non-destructive trim proposal for a speaker turn.

    Attributes:
        turn_id:       FK → speaker_turns.turn_id.
        start_seconds: Absolute start of the proposed trim interval (seconds).
        end_seconds:   Absolute end of the proposed trim interval (seconds).
        kind:          ``"silence"`` or ``"applause"``.
        score:         Detector confidence in [0.0, 1.0], or ``None`` for silence.
        source:        Non-empty backend identifier (e.g. ``"vad_webrtc"``).
        is_voice_free: Result of the voice gate before persistence.
    """

    turn_id: int
    start_seconds: float
    end_seconds: float
    kind: str        # "silence" | "applause"
    score: float | None
    source: str
    is_voice_free: bool


# ---------------------------------------------------------------------------
# Voice gate (THE invariant)
# ---------------------------------------------------------------------------


def is_voice_free(
    interval: tuple[float, float],
    voiced_segments: list[tuple[float, float]],
) -> bool:
    """Return True iff *interval* intersects NO voiced segment.

    Overlap predicate: ``start < voice_end and end > voice_start``.
    Boundary touches (candidate end == voice_start or candidate start == voice_end)
    are NOT considered overlaps.

    Args:
        interval:        (start, end) candidate interval in seconds.
        voiced_segments: List of (start, end) voiced intervals.

    Returns:
        ``True`` if the interval is completely outside all voiced segments.
    """
    start, end = interval
    for voice_start, voice_end in voiced_segments:
        if start < voice_end and end > voice_start:
            return False
    return True


# ---------------------------------------------------------------------------
# Silence gap helper (pure)
# ---------------------------------------------------------------------------


def _silence_gaps(
    voiced_segments: list[tuple[float, float]],
    turn_start: float,
    turn_end: float,
    min_duration: float,
) -> list[tuple[float, float]]:
    """Derive silence gaps within [turn_start, turn_end] from voiced segments.

    Merges the voiced segments, then takes the complement inside the turn window,
    filtering any gap shorter than *min_duration*.

    Args:
        voiced_segments: Voiced (start, end) intervals (may overlap/be unsorted).
        turn_start:      Turn window start (seconds).
        turn_end:        Turn window end (seconds).
        min_duration:    Minimum gap length (seconds) to include.

    Returns:
        List of (gap_start, gap_end) pairs, each within [turn_start, turn_end],
        with duration >= min_duration.
    """
    if turn_end <= turn_start:
        return []

    # Clip segments to the turn window and sort
    clipped: list[tuple[float, float]] = []
    for s, e in voiced_segments:
        cs = max(s, turn_start)
        ce = min(e, turn_end)
        if ce > cs:
            clipped.append((cs, ce))

    if not clipped:
        # No voiced segments — entire turn is one gap
        gap = (turn_start, turn_end)
        if turn_end - turn_start >= min_duration:
            return [gap]
        return []

    # Sort and merge overlapping/adjacent voiced segments
    clipped.sort(key=lambda seg: seg[0])
    merged: list[tuple[float, float]] = [clipped[0]]
    for seg_start, seg_end in clipped[1:]:
        prev_start, prev_end = merged[-1]
        if seg_start <= prev_end:
            merged[-1] = (prev_start, max(prev_end, seg_end))
        else:
            merged.append((seg_start, seg_end))

    # Take complement within [turn_start, turn_end]
    gaps: list[tuple[float, float]] = []
    cursor = turn_start
    for voiced_start, voiced_end in merged:
        if voiced_start > cursor:
            gap_start = cursor
            gap_end = voiced_start
            if gap_end - gap_start >= min_duration:
                gaps.append((gap_start, gap_end))
        cursor = max(cursor, voiced_end)

    # Trailing gap after last voiced segment
    if cursor < turn_end and turn_end - cursor >= min_duration:
        gaps.append((cursor, turn_end))

    return gaps


# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


def generate_trim_proposals(
    turn: dict,
    wav_path: str,
    vad_fn: VadFn,
    yamnet_fn: YamnetFn,
    *,
    min_duration_secs: float = DEFAULT_MIN_DURATION_SECS,
    applause_score_threshold: float = DEFAULT_APPLAUSE_SCORE_THRESHOLD,
) -> list[TrimProposal]:
    """Generate silence and applause trim proposals for a speaker turn.

    Silence path:
        Calls *vad_fn* to get voiced intervals, inverts them within the turn
        window via :func:`_silence_gaps`. All silence gaps are by construction
        voice-free (``is_voice_free=True``).

    Applause path:
        Calls *yamnet_fn* to get scored applause intervals. Filters by
        *applause_score_threshold*, *min_duration_secs*, and the
        ``is_voice_free`` gate against voiced segments.

    Neither path auto-cuts any audio file. Proposals are advisory records only.

    Args:
        turn:                    Speaker-turn dict with ``turn_id``, ``start_seconds``,
                                 ``end_seconds``.
        wav_path:                Path to the turn's extracted WAV (passed to callables).
        vad_fn:                  Injectable VAD callable → voiced (start, end) segments.
        yamnet_fn:                Injectable YAMNet callable → [{start, end, max_score}].
        min_duration_secs:       Minimum proposal duration in seconds.
        applause_score_threshold: Minimum applause max_score to accept.

    Returns:
        List of TrimProposal instances (silence first, then applause).
    """
    turn_id: int = turn["turn_id"]
    turn_start: float = float(turn["start_seconds"])
    turn_end: float = float(turn["end_seconds"])
    offset: float = turn_start

    proposals: list[TrimProposal] = []

    # --- VAD: get voiced segments once; reused for both paths ---
    try:
        voiced_segments: list[tuple[float, float]] = vad_fn(wav_path, offset)
    except Exception as exc:
        log.warning(
            "trim_proposals.vad_fn.failed turn_id=%s wav_path=%s error=%s",
            turn_id, wav_path, exc,
        )
        voiced_segments = []

    # --- Silence path ---
    gaps = _silence_gaps(voiced_segments, turn_start, turn_end, min_duration_secs)
    for gap_start, gap_end in gaps:
        proposals.append(TrimProposal(
            turn_id=turn_id,
            start_seconds=gap_start,
            end_seconds=gap_end,
            kind="silence",
            score=None,
            source=VAD_SOURCE,
            is_voice_free=True,  # gaps are voice-free by construction
        ))

    # --- Applause path ---
    try:
        applause_rounds: list[dict] = yamnet_fn(wav_path, offset)
    except SidecarApiError:
        raise  # infra outage → escalate to DAG loop (task failure)
    except Exception as exc:
        log.warning(
            "trim_proposals.yamnet_fn.failed turn_id=%s wav_path=%s error=%s",
            turn_id, wav_path, exc,
        )
        applause_rounds = []

    for round_dict in applause_rounds:
        start = float(round_dict["start"])
        end = float(round_dict["end"])
        max_score = float(round_dict["max_score"])

        # Filter by score threshold
        if max_score < applause_score_threshold:
            continue

        # Filter by minimum duration
        if end - start < min_duration_secs:
            continue

        # Apply voice-gate
        gate_result = is_voice_free((start, end), voiced_segments)
        if not gate_result:
            continue

        proposals.append(TrimProposal(
            turn_id=turn_id,
            start_seconds=start,
            end_seconds=end,
            kind="applause",
            score=max_score,
            source=YAMNET_SOURCE,
            is_voice_free=True,  # gate passed
        ))

    return proposals


# ---------------------------------------------------------------------------
# Persistence helper (idempotent upsert)
# ---------------------------------------------------------------------------


def _upsert_proposals(
    cursor,
    proposals: list[TrimProposal],
    table: str = "speaker_turn_trim_proposals",
) -> int:
    """Upsert TrimProposal records into the speaker_turn_trim_proposals table.

    Uses ``ON CONFLICT (turn_id, start_seconds, tipo) DO UPDATE`` to update
    ``score``, ``source``, and ``updated_at`` on re-runs. Never commits —
    the DAG controls transactions.

    Follows the same cursor-based contract as ``speaker_turns._upsert_turns``:
    the caller is responsible for opening the cursor and controlling the
    transaction; this function only calls ``cursor.execute()``.

    Args:
        cursor:    An open DB cursor (psycopg2-compatible, ``execute()`` method).
        proposals: Proposals to persist.
        table:     Target table name. Callers with a live connection MUST pass
            the schema-qualified name
            (``pg.get_qualified_table("speaker_turn_trim_proposals")``) — the
            app sets no search_path, so an unqualified name only resolves when
            the role's default schema matches (works in dev, fails in prod).
            Defaults to the bare name so pure unit tests need no connection.

    Returns:
        Number of proposals processed (same as len(proposals)).
    """
    if not proposals:
        return 0

    sql = f"""
        INSERT INTO {table}
            (turn_id, start_seconds, end_seconds, tipo, score, source, is_voice_free, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (turn_id, start_seconds, tipo) DO UPDATE SET
            end_seconds   = EXCLUDED.end_seconds,
            score         = EXCLUDED.score,
            source        = EXCLUDED.source,
            is_voice_free = EXCLUDED.is_voice_free,
            updated_at    = NOW()
    """

    for proposal in proposals:
        cursor.execute(sql, (
            proposal.turn_id,
            proposal.start_seconds,
            proposal.end_seconds,
            proposal.kind,   # SQL col: tipo
            proposal.score,
            proposal.source,
            proposal.is_voice_free,
        ))

    return len(proposals)
