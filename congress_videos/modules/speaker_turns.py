"""Speaker-turn detection for video_chapters.

Pure orchestrator module. Fuses acoustic boundaries from an injectable
``diarize_fn`` with Spanish president-announcement text from SRT blocks into a
list of resolved ``Turn`` records.

All business logic (postprocessing, text gate, extractor) is pure and unit-tested
with stub data. The only side-effecting collaborators (DB cursor, Docker subprocess)
are injected by the caller — this module never opens a connection or spawns a
subprocess directly.

``confirmed_block_duration_seconds`` is carried through the wire format but is
NEVER used as an acceptance threshold.
"""

from __future__ import annotations

import logging
import re
import unicodedata
from dataclasses import dataclass
from typing import Callable

from congress_videos.modules.sidecar_api_error import SidecarApiError

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (tunable thresholds)
# ---------------------------------------------------------------------------

ANNOUNCEMENT_WINDOW_SECONDS: float = 120.0
"""Backward-only announcement search window, in seconds. Mirrors
speaker_resolution.INTRO_WINDOW_SECS: announcements always precede the
speaker they introduce, so the window is [t - window, t], never forward."""

GAP_MERGE_SECONDS: float = 1.0
"""Same-speaker segments closer than this are merged into one."""

PINGPONG_B_MAX_SECONDS: float = 5.0
"""Maximum B-segment duration for A→B→A ping-pong collapse."""

PINGPONG_RETURN_SECONDS: float = 10.0
"""Maximum gap between end-of-B and start-of-return-A for collapse."""

# ---------------------------------------------------------------------------
# Turn dataclass
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Turn:
    """A single resolved speaker turn within a video chapter.

    Attributes:
        start_seconds: Chapter-relative start time of the turn.
        end_seconds: Chapter-relative end time of the turn.
        speaker_label: Acoustic cluster label (e.g. "SPEAKER_01").
        resolved_name: Canonical participant display_name, or None.
        confidence: Attribution confidence in [0.0, 1.0].
        source: One of "text_named", "text_confirmed", or "acoustic".
    """

    start_seconds: float
    end_seconds: float
    speaker_label: str
    resolved_name: str | None
    confidence: float
    source: str


# ---------------------------------------------------------------------------
# Injectable diarisation type alias
# ---------------------------------------------------------------------------

# Wire format == benchmark postprocessed-speaker-changes.json.
# input:  (wav_path, chapter_offset_seconds)
# output: [{start_seconds, from_speaker, to_speaker,
#           confirmed_block_duration_seconds}, ...]
#         start_seconds already rebased to chapter-relative time.
DiarizeFn = Callable[[str, float], list[dict]]

# ---------------------------------------------------------------------------
# Regex patterns (compiled once, accent-tolerant)
# ---------------------------------------------------------------------------

def _normalize_text(text: str) -> str:
    """Strip diacritics for accent-tolerant matching."""
    return unicodedata.normalize("NFD", text).encode("ascii", "ignore").decode("ascii")


# Pattern capturing the name after "el señor / la señora"
_RE_NAMED = re.compile(
    r"tiene\s+la\s+palabra\s+(?:el\s+se[nñ]or|la\s+se[nñ]ora)\s+"
    r"(?P<name>[\wÀ-ɏ.\- ]+?)(?:[.,]|\s*$)",
    re.IGNORECASE,
)

# Phrase-only patterns (no name captured)
_RE_SU_SENORIA = re.compile(
    r"tiene\s+la\s+palabra\s+su\s+se[nñ]or[íi]a",
    re.IGNORECASE,
)

_RE_GRACIAS_SENORIA = re.compile(
    r"gracias,?\s+se[nñ]or[íi]a",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# President-announcement extractor (pure)
# ---------------------------------------------------------------------------


def extract_announcement(
    srt_blocks: list[dict],
    t: float,
    window: float = ANNOUNCEMENT_WINDOW_SECONDS,
) -> tuple[str | None, bool]:
    """Search SRT blocks preceding time *t* for a president-announcement phrase.

    Backward-only: scans blocks fully contained in ``[t − window, t]``
    (mirrors ``speaker_resolution``'s intro-window filter). A block that
    starts before *t* but ends after it — or that starts after *t* entirely —
    is never matched: announcements always precede the speaker they
    introduce, and forward blocks are typically the new speaker's own words,
    a mis-attribution source. Within the window, prefers the block closest
    to and before *t* for name capture.

    Patterns matched (case-insensitive, accent-tolerant):
    - "Tiene la palabra el señor/la señora <name>" → returns (name, True)
    - "Tiene la palabra su señoría" → returns (None, True)
    - "Gracias, señoría" → returns (None, True)

    Returns:
        (raw_name_or_None, phrase_found)
    """
    lo = t - window

    # Collect blocks fully contained in [lo, t] — backward-only.
    window_blocks = [
        b for b in srt_blocks
        if b["start_secs"] >= lo and b["end_secs"] <= t
    ]

    if not window_blocks:
        return (None, False)

    # All matches precede t by construction; prefer the one closest to t.
    sorted_blocks = sorted(window_blocks, key=lambda b: t - b["end_secs"])

    # First pass: look for a named announcement in the best (closest preceding) blocks
    best_named: tuple[str | None, bool] | None = None
    best_phrase: tuple[str | None, bool] | None = None

    for block in sorted_blocks:
        text = block["text"]
        m = _RE_NAMED.search(text)
        if m:
            name = m.group("name").strip()
            # Only accept first (closest) named match
            if best_named is None:
                best_named = (name, True)
                break

    if best_named is not None:
        return best_named

    for block in sorted_blocks:
        text = block["text"]
        if _RE_SU_SENORIA.search(text):
            if best_phrase is None:
                best_phrase = (None, True)
                break
        if _RE_GRACIAS_SENORIA.search(text):
            if best_phrase is None:
                best_phrase = (None, True)
                break

    if best_phrase is not None:
        return best_phrase

    return (None, False)


# ---------------------------------------------------------------------------
# Postprocessing pipeline (pure functions)
# ---------------------------------------------------------------------------


def _merge_gaps(segments: list[dict]) -> list[dict]:
    """Merge adjacent same-label segments separated by less than GAP_MERGE_SECONDS.

    Args:
        segments: Segment dicts with keys ``start_seconds``, ``end_seconds``,
                  ``speaker_label``.

    Returns:
        New list with qualifying gaps closed. Input dicts are never mutated.
    """
    if not segments:
        return []

    result: list[dict] = []
    current = dict(segments[0])

    for seg in segments[1:]:
        gap = seg["start_seconds"] - current["end_seconds"]
        if seg["speaker_label"] == current["speaker_label"] and gap < GAP_MERGE_SECONDS:
            # Extend current segment end
            current = {**current, "end_seconds": seg["end_seconds"]}
        else:
            result.append(current)
            current = dict(seg)

    result.append(current)
    return result


def _collapse_pingpong(segments: list[dict]) -> list[dict]:
    """Collapse A→B→A ping-pong patterns where B is short.

    Collapses the triple when:
    - ``B_duration < PINGPONG_B_MAX_SECONDS``
    - ``gap_between_B_end_and_A_return < PINGPONG_RETURN_SECONDS``

    Args:
        segments: Segment dicts ordered by ``start_seconds``.

    Returns:
        New list with collapsed patterns. Input dicts are never mutated.
    """
    if len(segments) < 3:
        return list(segments)

    result: list[dict] = []
    i = 0

    while i < len(segments):
        if i + 2 < len(segments):
            a1 = segments[i]
            b = segments[i + 1]
            a2 = segments[i + 2]

            b_duration = b["end_seconds"] - b["start_seconds"]
            return_gap = a2["start_seconds"] - b["end_seconds"]

            if (
                a1["speaker_label"] == a2["speaker_label"]
                and a1["speaker_label"] != b["speaker_label"]
                and b_duration < PINGPONG_B_MAX_SECONDS
                and return_gap < PINGPONG_RETURN_SECONDS
            ):
                # Collapse: merge a1 and a2 into one, skip b
                merged = {**a1, "end_seconds": a2["end_seconds"]}
                result.append(merged)
                i += 3
                continue

        result.append(segments[i])
        i += 1

    return result


def _merge_same_name(turns: list[Turn]) -> list[Turn]:
    """Merge adjacent turns with the same non-null resolved_name into one.

    Args:
        turns: Ordered list of Turn instances.

    Returns:
        New list with qualifying turns merged. Input is not mutated.
    """
    if not turns:
        return []

    result: list[Turn] = []
    current = turns[0]

    for turn in turns[1:]:
        if (
            current.resolved_name is not None
            and turn.resolved_name is not None
            and current.resolved_name == turn.resolved_name
        ):
            # Merge: extend end, keep higher confidence
            new_confidence = max(current.confidence, turn.confidence)
            current = Turn(
                start_seconds=current.start_seconds,
                end_seconds=turn.end_seconds,
                speaker_label=current.speaker_label,
                resolved_name=current.resolved_name,
                confidence=new_confidence,
                source=current.source,
            )
        else:
            result.append(current)
            current = turn

    result.append(current)
    return result


# ---------------------------------------------------------------------------
# Text gate (assigns source/confidence, drops noise)
# ---------------------------------------------------------------------------


def _apply_text_gate(
    segments: list[dict],
    srt_blocks: list[dict],
    name_resolver: Callable[[str], dict | None],
) -> list[Turn]:
    """Evaluate each segment against the SRT announcement window.

    For each segment:
    - Calls ``extract_announcement`` on the segment's ``start_seconds``.
    - Routes source/confidence per design table.
    - Drops same-speaker noise (both sides same label, no phrase).
    - Builds ``Turn`` instances. Never reads ``confirmed_block_duration_seconds``
      as a threshold.

    Args:
        segments: Postprocessed segment dicts.
        srt_blocks: SRT blocks for the chapter window.
        name_resolver: Callable mapping a raw name str to a participant dict or None.

    Returns:
        List of Turn instances.
    """
    turns: list[Turn] = []

    for seg in segments:
        t = seg["start_seconds"]
        from_speaker = seg.get("from_speaker", "")
        to_speaker = seg.get("to_speaker", seg.get("speaker_label", ""))
        speaker_label = seg.get("speaker_label", to_speaker)

        raw_name, phrase_found = extract_announcement(srt_blocks, t)

        if phrase_found and raw_name is not None:
            # Try to resolve name
            resolved = name_resolver(raw_name)
            if resolved is not None:
                resolved_name = resolved.get("display_name") or raw_name
                source = "text_named"
                confidence = 0.95
            else:
                resolved_name = None
                source = "text_confirmed"
                confidence = 0.80
        elif phrase_found:
            # Phrase found but no name (su señoría / gracias señoría)
            resolved_name = None
            source = "text_confirmed"
            confidence = 0.80
        else:
            # No phrase: drop if same-speaker noise (both sides same label)
            if from_speaker and to_speaker and from_speaker == to_speaker:
                # Same speaker on both sides, no phrase → noise, drop
                continue
            # Different speakers → acoustic fallback
            resolved_name = None
            source = "acoustic"
            confidence = 0.50

        end_seconds = seg.get("end_seconds", t)
        turns.append(Turn(
            start_seconds=t,
            end_seconds=end_seconds,
            speaker_label=speaker_label,
            resolved_name=resolved_name,
            confidence=confidence,
            source=source,
        ))

    return turns


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def detect_turns(
    chapter: dict,
    srt_blocks: list[dict],
    diarize_fn: DiarizeFn,
    name_resolver: Callable[[str], dict | None] | None = None,
) -> list[Turn]:
    """Detect speaker-turn boundaries within a video chapter.

    Orchestrates: diarize_fn → gap-merge → ping-pong collapse → text gate
    → same-name merge. Returns Turn[].

    Never touches Airflow context, DB, or Docker directly. All
    side-effecting collaborators are injected.

    Args:
        chapter: video_chapters row dict with at least chapter_id, video_id,
                 session_date, start_time, end_time.
        srt_blocks: SRT blocks pre-filtered to the chapter window. Pass []
                    for acoustic-only processing (missing-SRT degradation).
        diarize_fn: Injectable boundary producer. Must return the wire format:
                    [{start_seconds, from_speaker, to_speaker,
                      confirmed_block_duration_seconds}].
        name_resolver: Callable mapping raw name → participant dict or None.
                       Defaults to lookup_participant_fuzzy from participants_db.

    Returns:
        Ordered list of Turn instances.
    """
    if name_resolver is None:
        from congress_videos.modules.participants_db import lookup_participant_fuzzy
        name_resolver = lookup_participant_fuzzy

    chapter_id = chapter.get("chapter_id")

    # Build wav_path from chapter context (for diarize_fn caller)
    # The DAG layer handles _find_source_video + extract_audio_wav;
    # detect_turns receives the wav_path already resolved.
    # For acoustic-only / missing-video, diarize_fn returns [] or raises.
    try:
        acoustic_changes: list[dict] = diarize_fn(
            chapter.get("_wav_path", ""),
            chapter.get("_chapter_offset_seconds", 0.0),
        )
    except SidecarApiError:
        raise  # infra outage → escalate to DAG loop (task failure)

    if not acoustic_changes:
        return []

    # Convert acoustic changes into candidate segments.
    # Each change marks the START of a new speaker block; we need to build
    # (start, end) pairs. End is the start of the next change (or a sentinel).
    segments: list[dict] = []
    for i, change in enumerate(acoustic_changes):
        start = change["start_seconds"]
        if i + 1 < len(acoustic_changes):
            end = acoustic_changes[i + 1]["start_seconds"]
        else:
            end = start + 60.0  # sentinel: last turn extends 60s
        seg = {
            "start_seconds": start,
            "end_seconds": end,
            "speaker_label": change.get("to_speaker", f"SPEAKER_{i:02d}"),
            "from_speaker": change.get("from_speaker", ""),
            "to_speaker": change.get("to_speaker", f"SPEAKER_{i:02d}"),
            "confirmed_block_duration_seconds": change.get("confirmed_block_duration_seconds", 0.0),
        }
        segments.append(seg)

    # Postprocessing pipeline
    segments = _merge_gaps(segments)
    segments = _collapse_pingpong(segments)
    turns = _apply_text_gate(segments, srt_blocks, name_resolver)
    turns = _merge_same_name(turns)

    return turns


# ---------------------------------------------------------------------------
# Persistence helper (idempotent upsert)
# ---------------------------------------------------------------------------


def _upsert_turns(
    cursor, chapter_id: int, turns: list[Turn], table: str = "speaker_turns"
) -> None:
    """Upsert Turn records into the speaker_turns table.

    Each Turn is inserted; conflicts on (chapter_id, start_seconds) update
    all mutable fields. Never calls cursor.commit() — the DAG controls
    transactions.

    Args:
        cursor: DB cursor with an execute() method.
        chapter_id: chapter_id FK value for all turns.
        turns: Turns to persist.
        table: Target table name. Callers with a live connection MUST pass the
            schema-qualified name (``pg.get_qualified_table("speaker_turns")``)
            because the app does not set a search_path — an unqualified name
            only resolves when the role's default schema happens to match
            (works in dev, fails in prod). Defaults to the bare name so pure
            unit tests need no connection.
    """
    if not turns:
        return

    sql = f"""
        INSERT INTO {table}
            (chapter_id, start_seconds, end_seconds, speaker_label, resolved_name,
             confidence, source, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (chapter_id, start_seconds) DO UPDATE SET
            end_seconds   = EXCLUDED.end_seconds,
            speaker_label = EXCLUDED.speaker_label,
            resolved_name = EXCLUDED.resolved_name,
            confidence    = EXCLUDED.confidence,
            source        = EXCLUDED.source,
            updated_at    = NOW()
    """

    for turn in turns:
        cursor.execute(sql, (
            chapter_id,
            turn.start_seconds,
            turn.end_seconds,
            turn.speaker_label,
            turn.resolved_name,
            turn.confidence,
            turn.source,
        ))
