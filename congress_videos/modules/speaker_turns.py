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

import dataclasses
import logging
import os
import re
import unicodedata
from collections.abc import Callable
from dataclasses import dataclass

from congress_videos.config.ai_prompts import (
    TURN_NAME_RESOLUTION_SYSTEM_PROMPT,
    TURN_NAME_RESOLUTION_USER_TEMPLATE,
)
from congress_videos.modules.announcement_patterns import (
    RE_GRACIAS_SENORIA,
    RE_NAMED,
    RE_SU_SENORIA,
)
from congress_videos.modules.sidecar_api_error import SidecarApiError
from utils.llm_config import LLM_CHEAP

log = logging.getLogger(__name__)

__all__ = [
    "Turn",
    "detect_turns",
    "extract_announcement",
    "drop_micro_segments",
    "collapse_foreign_runs",
    "MIN_SEGMENT_DURATION_SECONDS",
    "FOREIGN_INTERRUPTION_MAX_SECONDS",
]

# ---------------------------------------------------------------------------
# Module-level constants (tunable thresholds)
# ---------------------------------------------------------------------------

ANNOUNCEMENT_WINDOW_SECONDS: float = 120.0
"""Backward-only announcement search window, in seconds. Mirrors
speaker_resolution.INTRO_WINDOW_SECS: announcements always precede the
speaker they introduce, so the window is [t - window, t], never forward."""

GAP_MERGE_SECONDS: float = 1.0
"""Same-speaker segments closer than this are merged into one."""

MIN_SEGMENT_DURATION_SECONDS: float = 1.0
"""Segments shorter than this are acoustic blips: dropped, their span absorbed
by the previous kept segment. Measured as end_seconds - start_seconds; never
confirmed_block_duration_seconds."""

FOREIGN_INTERRUPTION_MAX_SECONDS: float = 10.0
"""Maximum aggregate span of a run of consecutive foreign segments bounded by
the same speaker_label on both sides before the run is collapsed away."""

LLM_RESOLVED_CONFIDENCE: float = 0.85
"""Fixed persisted confidence for source='llm_resolved' (not the model's own
number) — keeps the discrete ladder 0.95/0.85/0.80/0.50 comparable across
sources."""

TURN_LLM_MIN_CONFIDENCE: float = 0.80
"""Minimum model self-confidence required to accept an LLM name candidate."""

TURN_LLM_MAX_CALLS_PER_CHAPTER: int = int(os.getenv("TURN_LLM_MAX_CALLS_PER_CHAPTER", "12"))
"""Per-chapter cap on LLM fallback calls. ``0`` disables the fallback
entirely — the sole runtime kill switch (no deploy required, restart the
worker after changing the env var)."""

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
        source: One of "text_named" (0.95), "llm_resolved" (0.85),
            "text_confirmed" (0.80), or "acoustic" (0.50).
        is_procedural: True when this turn is a chair floor-handoff (issue
            #143), set by ``_flag_procedural`` after ``_merge_same_name``.
            Defaulted so every pre-existing call site keeps compiling.
        procedural_reason: Auditable reason string when ``is_procedural`` is
            True (e.g. ``"dur=6.0s coverage=0.91 patterns=gracias_senoria"``);
            always None otherwise.
    """

    start_seconds: float
    end_seconds: float
    speaker_label: str
    resolved_name: str | None
    confidence: float
    source: str
    is_procedural: bool = False
    procedural_reason: str | None = None


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


# Back-compat private aliases (issue #284): the three patterns now live in
# announcement_patterns.py, shared with speaker_resolution.py. These names
# stay importable and identical (same compiled objects) for existing callers.
_RE_NAMED = RE_NAMED
_RE_SU_SENORIA = RE_SU_SENORIA
_RE_GRACIAS_SENORIA = RE_GRACIAS_SENORIA

# ---------------------------------------------------------------------------
# Procedural-turn detection (issue #143) — pure AND-gate
# ---------------------------------------------------------------------------

PROCEDURAL_MAX_DURATION_SECS: float = 15.0
"""A turn is procedural-eligible only when its OWN duration is <= this value.
Longer turns are never flagged regardless of text (precision-first)."""

PROCEDURAL_MIN_COVERAGE: float = 0.6
"""Minimum fraction of the turn's own normalized text that must be covered
by the union of PROCEDURAL_PATTERNS matches for the turn to be flagged."""

PROCEDURAL_MIN_COVERAGE_QA: float = 0.55
"""Relaxed coverage threshold applied only under qa context — when the
chapter's post-filter turns carry the same >=2-distinct-real-labels evidence
that classify_turn_type's label path (issue #282) uses to set turn_type='qa'.
Booster, never a gate: without qa context the strict threshold stands, and
the duration and anti-bleed gates apply unchanged in both modes."""

# Named patterns matched against ACCENT-STRIPPED, LOWERCASED,
# whitespace-collapsed text (see _normalize_text). Order is irrelevant —
# every pattern is evaluated and its match spans unioned for coverage.
# Deliberately excludes "gracias, señor presidente" — the canonical OPENING
# of a real intervention, never a procedural handoff.
PROCEDURAL_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    (
        "tiene_la_palabra_named",
        re.compile(
            r"tiene\s+la\s+palabra\s+(?:el\s+senor|la\s+senora)\s+[a-z.\- ]+",
            re.IGNORECASE,
        ),
    ),
    (
        "tiene_la_palabra_su_senoria",
        re.compile(r"tiene\s+la\s+palabra\s+su\s+senoria", re.IGNORECASE),
    ),
    ("gracias_senoria", re.compile(r"gracias,?\s+senoria", re.IGNORECASE)),
    ("tiene_la_palabra_generic", re.compile(r"tiene\s+la\s+palabra\b", re.IGNORECASE)),
    ("adelante_senoria", re.compile(r"adelante,?\s+senoria", re.IGNORECASE)),
    ("para_contestar_responder", re.compile(r"para\s+(?:contestar|responder)", re.IGNORECASE)),
    ("concluya_senoria", re.compile(r"concluya,?\s+senoria", re.IGNORECASE)),
    ("vaya_terminando", re.compile(r"vaya\s+terminando", re.IGNORECASE)),
    (
        "ha_terminado_su_tiempo",
        re.compile(r"ha\s+(?:terminado|concluido)\s+su\s+tiempo", re.IGNORECASE),
    ),
    ("silencio_por_favor", re.compile(r"silencio,?\s+por\s+favor", re.IGNORECASE)),
    ("ruego_silencio", re.compile(r"ruego\s+silencio", re.IGNORECASE)),
    (
        "suspende_reanuda_sesion",
        re.compile(r"se\s+(?:suspende|reanuda)\s+la\s+sesion", re.IGNORECASE),
    ),
    (
        "siguiente_punto_orden_dia",
        re.compile(
            r"pasamos\s+al\s+(?:siguiente\s+)?punto\s+del\s+orden\s+del\s+dia",
            re.IGNORECASE,
        ),
    ),
    # Corpus-derived additions (validated against production chapters 318/262):
    # the chair thanks the OUTGOING speaker by title+name. The negative
    # lookahead keeps the anti-pattern intact: thanking the president(a) is
    # the canonical OPENING of a real intervention, never a handoff.
    (
        "gracias_titled",
        re.compile(
            r"(?:muchas\s+)?gracias,?\s+(?:el\s+|la\s+)?(?:senor|senora)"
            r"(?!\s+president)\s+[a-z\-]+(?:\s+de\s+[a-z\-]+){0,2}",
            re.IGNORECASE,
        ),
    ),
    ("cuando_quiera", re.compile(r"cuando\s+(?:usted\s+)?quiera", re.IGNORECASE)),
    (
        "preguntas_dirigidas",
        re.compile(
            r"(?:pasamos|vamos)\s+(?:ahora\s+)?a\s+las\s+preguntas\s+dirigidas\s+"
            r"a[a-z\- ]{0,40}",
            re.IGNORECASE,
        ),
    ),
    (
        "pregunta_formula",
        re.compile(
            r"la\s+(?:siguiente|primera|segunda|tercera|ultima)\s+(?:pregunta\s+)?"
            r"(?:se\s+la\s+formula|la\s+formula|va\s+dirigida\s+al?)"
            r"(?:\s+(?:el|la|al))?(?:\s+diputad[oa])?(?:\s+(?:don|dona))?"
            r"(?:\s+[a-z\-]+){0,6}",
            re.IGNORECASE,
        ),
    ),
)

PROCEDURAL_MAX_UNMATCHED_RUN: int = 40
"""Longest contiguous run of normalized characters NOT covered by any
pattern match that a flagged turn may contain. A genuine handoff is formula
end to end (unmatched runs are names and connectives); SRT bleed followed by
real substance leaves one long unmatched tail and must never be flagged."""

# Filler patterns contribute coverage but can NEVER justify a flag on their
# own (a heckle can be pure vocative): at least one PROCEDURAL_PATTERNS
# (core) match is required before fillers are even considered.
PROCEDURAL_FILLER_PATTERNS: tuple[tuple[str, re.Pattern], ...] = (
    (
        "vocative_titled",
        re.compile(
            r"(?:el\s+|la\s+)?(?:senor|senora|senoria)s?\s+[a-z\-]+"
            r"(?:\s+de\s+[a-z\-]+){0,2},?",
            re.IGNORECASE,
        ),
    ),
    ("muchas_gracias_bare", re.compile(r"(?:muchas\s+)?gracias", re.IGNORECASE)),
    ("por_favor", re.compile(r"por\s+favor", re.IGNORECASE)),
    ("silencio_bare", re.compile(r"silencio", re.IGNORECASE)),
    ("un_momentito", re.compile(r"un\s+momentito", re.IGNORECASE)),
    (
        "grupo_parlamentario",
        re.compile(
            r"(?:por\s+el\s+|del\s+|el\s+|la\s+)?grupo\s+parlamentario"
            r"(?:\s+[a-z\-]+)?",
            re.IGNORECASE,
        ),
    ),
    (
        "diputado_titled",
        re.compile(
            r"(?:el\s+|la\s+)?diputad[oa](?:\s+(?:don|dona))?(?:\s+[a-z\-]+){0,4}",
            re.IGNORECASE,
        ),
    ),
)


def _longest_unmatched_run(spans: list[tuple[int, int]], total_length: int) -> int:
    """Length of the longest contiguous stretch NOT covered by any span."""
    if not spans:
        return total_length
    ordered = sorted(spans)
    longest = ordered[0][0]
    cursor = ordered[0][1]
    for start, end in ordered[1:]:
        if start > cursor:
            longest = max(longest, start - cursor)
        cursor = max(cursor, end)
    return max(longest, total_length - cursor)


def _union_length(spans: list[tuple[int, int]]) -> int:
    """Total length covered by a set of (start, end) spans, merging overlaps."""
    if not spans:
        return 0
    ordered = sorted(spans)
    merged: list[list[int]] = [list(ordered[0])]
    for start, end in ordered[1:]:
        last = merged[-1]
        if start <= last[1]:
            last[1] = max(last[1], end)
        else:
            merged.append([start, end])
    return sum(end - start for start, end in merged)


def is_procedural_turn(text: str, duration_seconds: float, *, qa_context: bool = False) -> tuple[bool, str | None]:
    """Pure AND-gate: duration <= 15s AND phrase coverage >= threshold. Never raises.

    Coverage is computed on the turn's OWN accent-stripped, lowercased,
    whitespace-collapsed text — never a caller-supplied window spanning other
    turns. Precision over recall: ambiguous cases (e.g. a courtesy opening
    followed by substance) return ``(False, None)``.

    Args:
        text: The turn's own SRT text (see ``_turn_window_text``).
        duration_seconds: The turn's own duration (``end_seconds - start_seconds``).
        qa_context: True when the chapter carries the issue-#282 label
            evidence for turn_type='qa' (>=2 distinct real speaker labels);
            relaxes the coverage threshold to PROCEDURAL_MIN_COVERAGE_QA.
            All other gates (duration, core-pattern, anti-bleed) unchanged.

    Returns:
        ``(flagged, reason)``. ``reason`` is non-None iff ``flagged`` is True,
        e.g. ``"dur=8.4s coverage=0.91 patterns=gracias_senoria"``.
    """
    if duration_seconds > PROCEDURAL_MAX_DURATION_SECS:
        return (False, None)

    normalized = " ".join(_normalize_text(text or "").lower().split())
    if not normalized:
        return (False, None)

    matched_names: list[str] = []
    spans: list[tuple[int, int]] = []
    for name, pattern in PROCEDURAL_PATTERNS:
        for m in pattern.finditer(normalized):
            spans.append((m.start(), m.end()))
            if name not in matched_names:
                matched_names.append(name)

    # Core gate: fillers can never justify a flag on their own.
    if not spans:
        return (False, None)

    for name, pattern in PROCEDURAL_FILLER_PATTERNS:
        for m in pattern.finditer(normalized):
            spans.append((m.start(), m.end()))
            if name not in matched_names:
                matched_names.append(name)

    min_coverage = PROCEDURAL_MIN_COVERAGE_QA if qa_context else PROCEDURAL_MIN_COVERAGE
    coverage = _union_length(spans) / len(normalized)
    if coverage < min_coverage:
        return (False, None)

    # Anti-bleed gate: a genuine handoff is formula end to end. SRT bleed
    # followed by real substance leaves one long uncovered tail — reject it
    # even when courtesy fillers push raw coverage past the threshold.
    if _longest_unmatched_run(spans, len(normalized)) > PROCEDURAL_MAX_UNMATCHED_RUN:
        return (False, None)

    qa_marker = " qa_context" if qa_context else ""
    reason = f"dur={duration_seconds:.1f}s coverage={coverage:.2f}{qa_marker} patterns={','.join(matched_names)}"
    return (True, reason)


def _turn_window_text(srt_blocks: list[dict], start: float, end: float) -> str:
    """Join the text of SRT blocks overlapping the turn's OWN [start, end).

    Overlap predicate matches ``extract_announcement``/``_window_srt_text``:
    ``block.start_secs < end AND block.end_secs > start``. Pure; no I/O.
    """
    return " ".join(b["text"] for b in srt_blocks if b["start_secs"] < end and b["end_secs"] > start)


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
    window_blocks = [b for b in srt_blocks if b["start_secs"] >= lo and b["end_secs"] <= t]

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


def _drop_micro_segments(segments: list[dict]) -> list[dict]:
    """Drop sub-second segments, absorbing their span into the predecessor.

    Any segment whose duration (``end_seconds - start_seconds``) is strictly
    less than ``MIN_SEGMENT_DURATION_SECONDS`` is treated as an acoustic blip:
    it never survives as its own segment. Its span is absorbed by extending
    the previous KEPT segment's ``end_seconds`` to the blip's ``end_seconds``,
    preserving the contiguous tiling built upstream. A leading blip with no
    predecessor yet established is dropped outright — nothing absorbs it.

    Args:
        segments: Segment dicts ordered by ``start_seconds``, with keys
                  ``start_seconds`` and ``end_seconds`` at minimum.

    Returns:
        New list with sub-second segments removed. Input dicts are never
        mutated.
    """
    result: list[dict] = []
    for seg in segments:
        if seg["end_seconds"] - seg["start_seconds"] >= MIN_SEGMENT_DURATION_SECONDS:
            result.append(dict(seg))
        elif result:
            result[-1] = {**result[-1], "end_seconds": seg["end_seconds"]}
        # else: leading blip with no predecessor -> dropped outright
    return result


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


def _collapse_foreign_runs(segments: list[dict]) -> list[dict]:
    """Collapse runs of short foreign segments bounded by the same label.

    For a run of one or more consecutive segments whose ``speaker_label``
    differs from the preceding kept (anchor) segment, if the run eventually
    returns to the anchor's label, the run's aggregate span
    (``last_foreign.end_seconds - first_foreign.start_seconds``) is compared
    against ``FOREIGN_INTERRUPTION_MAX_SECONDS``. When the span qualifies,
    the run collapses into the anchor (which stays as ``out[-1]``), and the
    sweep continues from just past the returning segment — re-testing the
    now-extended anchor against whatever comes next, so chains of qualifying
    runs cascade into a single merged segment. A run with no return to the
    anchor's label before the list ends is left untouched. Foreign segments
    within a run need not all share one label; only the two bounding
    stretches must carry the identical ``speaker_label``.

    Args:
        segments: Segment dicts ordered by ``start_seconds``.

    Returns:
        New list with collapsed runs. Input dicts are never mutated.
    """
    if len(segments) < 3:
        return [dict(s) for s in segments]

    out: list[dict] = []
    i = 0
    while i < len(segments):
        cur = segments[i]
        if not out or cur["speaker_label"] == out[-1]["speaker_label"]:
            out.append(dict(cur))
            i += 1
            continue

        anchor = out[-1]
        j = i
        while j < len(segments) and segments[j]["speaker_label"] != anchor["speaker_label"]:
            j += 1

        if j < len(segments):
            span = segments[j - 1]["end_seconds"] - segments[i]["start_seconds"]
            if span < FOREIGN_INTERRUPTION_MAX_SECONDS:
                out[-1] = {**anchor, "end_seconds": segments[j]["end_seconds"]}
                i = j + 1
                continue

        out.append(dict(cur))
        i += 1

    return out


# Public aliases (issue #282): materialization.py's classify_turn_type
# reuses these #283 noise filters instead of duplicating them. Same
# functions, same objects — the private names above stay the primary
# call sites within this module and keep their 46 existing test references.
drop_micro_segments = _drop_micro_segments
collapse_foreign_runs = _collapse_foreign_runs


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


def _flag_procedural(turns: list[Turn], srt_blocks: list[dict]) -> list[Turn]:
    """Flag chair floor-handoff turns as procedural (issue #143).

    Must run AFTER ``_merge_same_name`` inside ``detect_turns`` so each
    turn's duration and span are final before the gate runs — a merged
    (now-long) turn must never be evaluated on a pre-merge sub-span.

    Pure; never mutates input turns. Uses ``_turn_window_text`` to build the
    turn's OWN text (never a group/chapter-wide window) and ``is_procedural_turn``
    for the AND-gate decision.

    Args:
        turns: Ordered list of Turn instances (post same-name merge).
        srt_blocks: SRT blocks for the chapter window.

    Returns:
        New list with ``is_procedural``/``procedural_reason`` set on each Turn.
    """
    # qa context (issue #282 label evidence): >=2 distinct real speaker
    # labels among the chapter's post-filter turns — the same signal
    # classify_turn_type's label path uses to set turn_type='qa'. Recomputed
    # fresh from this run's turns; never read back from cached DB state.
    distinct_labels = {t.speaker_label.strip() for t in turns if (t.speaker_label or "").strip()}
    qa_context = len(distinct_labels) >= 2

    result: list[Turn] = []
    for turn in turns:
        duration = turn.end_seconds - turn.start_seconds
        text = _turn_window_text(srt_blocks, turn.start_seconds, turn.end_seconds)
        flagged, reason = is_procedural_turn(text, duration, qa_context=qa_context)
        result.append(dataclasses.replace(turn, is_procedural=flagged, procedural_reason=reason))
    return result


# ---------------------------------------------------------------------------
# LLM fallback (reached only after regex+fuzzy fail — issue #131)
# ---------------------------------------------------------------------------


def _llm_resolve_name(
    srt_blocks: list[dict],
    t: float,
    name_resolver: Callable[[str], dict | None],
    completion_fn: Callable | None,
) -> str | None:
    """Ask the LLM to identify the speaker announced before time *t*.

    Returns the canonical participant ``display_name``, or ``None``. NEVER
    raises — any failure keeps the caller's acoustic outcome. Rejection
    ladder: ``completion_fn is None`` (fallback disabled) | empty intro
    window (D9 — free correctness+cost win) | exception from completion_fn |
    ``response["error"]`` set | missing/blank ``speaker_name`` | non-float or
    ``< TURN_LLM_MIN_CONFIDENCE`` confidence | ``name_resolver(name) is
    None`` (anti-hallucination roster validation, D5) → all return ``None``.

    Args:
        srt_blocks: SRT blocks for the chapter window.
        t: Segment start time to search backward from.
        name_resolver: Callable mapping a raw name str to a participant dict
            or None. Same boundary used by the regex/fuzzy text_named path.
        completion_fn: Injectable LLM call, ``None`` disables the fallback.
    """
    if completion_fn is None:
        return None

    lo = t - ANNOUNCEMENT_WINDOW_SECONDS
    intro_blocks = [b for b in srt_blocks if b["start_secs"] >= lo and b["end_secs"] <= t]
    if not intro_blocks:
        return None

    intro_text = "\n".join(b["text"] for b in intro_blocks)
    user_prompt = TURN_NAME_RESOLUTION_USER_TEMPLATE.format(intro_text=intro_text)

    try:
        response = completion_fn(
            TURN_NAME_RESOLUTION_SYSTEM_PROMPT,
            user_prompt,
            model=LLM_CHEAP,
        )
    except Exception as exc:  # noqa: BLE001 never raise
        log.warning(
            "_llm_resolve_name: completion_fn raised (%s: %s) — falling back to acoustic",
            type(exc).__name__,
            exc,
        )
        return None

    if response.get("error") or not response.get("data"):
        log.debug("_llm_resolve_name: completion error: %s", response.get("error"))
        return None

    data = response["data"]
    raw_name = data.get("speaker_name")
    confidence = data.get("confidence")

    if not raw_name:
        return None

    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        log.debug("_llm_resolve_name: invalid confidence %r", confidence)
        return None

    if confidence < TURN_LLM_MIN_CONFIDENCE:
        return None

    resolved = name_resolver(raw_name)
    if resolved is None:
        log.debug("_llm_resolve_name: hallucinated name %r — no roster match", raw_name)
        return None

    return resolved.get("display_name") or raw_name


# ---------------------------------------------------------------------------
# Text gate (assigns source/confidence, drops noise)
# ---------------------------------------------------------------------------


def _apply_text_gate(
    segments: list[dict],
    srt_blocks: list[dict],
    name_resolver: Callable[[str], dict | None],
    completion_fn: Callable | None = None,
    max_llm_calls: int | None = None,
) -> list[Turn]:
    """Evaluate each segment against the SRT announcement window.

    For each segment:
    - Calls ``extract_announcement`` on the segment's ``start_seconds``.
    - Routes source/confidence per design table.
    - Drops same-speaker noise (both sides same label, no phrase).
    - When regex/fuzzy resolution fails (no phrase, different speakers),
      tries the LLM fallback (issue #131) before giving up to acoustic —
      never overrides an existing text_named/text_confirmed tier.
    - Builds ``Turn`` instances. Never reads ``confirmed_block_duration_seconds``
      as a threshold.

    Args:
        segments: Postprocessed segment dicts.
        srt_blocks: SRT blocks for the chapter window.
        name_resolver: Callable mapping a raw name str to a participant dict or None.
        completion_fn: Injectable LLM call for the fallback. ``None`` (default)
            disables the fallback entirely.
        max_llm_calls: Per-chapter cap on LLM fallback calls. Defaults to
            ``TURN_LLM_MAX_CALLS_PER_CHAPTER`` (read at call time, so tests
            can monkeypatch the module constant).

    Returns:
        List of Turn instances.
    """
    if max_llm_calls is None:
        max_llm_calls = TURN_LLM_MAX_CALLS_PER_CHAPTER

    turns: list[Turn] = []
    llm_calls_made = 0

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
            # Different speakers, regex/fuzzy failed → try the LLM fallback
            # before giving up to acoustic (issue #131).
            resolved_name = None
            source = "acoustic"
            confidence = 0.50
            if completion_fn is not None and llm_calls_made < max_llm_calls:

                def _counting_completion_fn(system, user, **kw):
                    nonlocal llm_calls_made
                    llm_calls_made += 1
                    return completion_fn(system, user, **kw)

                llm_name = _llm_resolve_name(srt_blocks, t, name_resolver, _counting_completion_fn)
                if llm_name is not None:
                    resolved_name = llm_name
                    source = "llm_resolved"
                    confidence = LLM_RESOLVED_CONFIDENCE

        end_seconds = seg.get("end_seconds", t)
        turns.append(
            Turn(
                start_seconds=t,
                end_seconds=end_seconds,
                speaker_label=speaker_label,
                resolved_name=resolved_name,
                confidence=confidence,
                source=source,
            )
        )

    return turns


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def detect_turns(
    chapter: dict,
    srt_blocks: list[dict],
    diarize_fn: DiarizeFn,
    name_resolver: Callable[[str], dict | None] | None = None,
    completion_fn: Callable | None = None,
) -> list[Turn]:
    """Detect speaker-turn boundaries within a video chapter.

    Orchestrates: diarize_fn → drop micro-segments → gap-merge → foreign-run
    collapse → text gate → same-name merge → procedural flag (issue #143).
    Returns Turn[].

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
        completion_fn: Injectable LLM call for the text-gate fallback
                       (issue #131). ``None`` (default) disables the
                       fallback — the module never opens an LLM connection
                       directly; the caller (DAG layer) binds this.

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
    segments = _drop_micro_segments(segments)
    segments = _merge_gaps(segments)
    segments = _collapse_foreign_runs(segments)
    turns = _apply_text_gate(segments, srt_blocks, name_resolver, completion_fn=completion_fn)
    turns = _merge_same_name(turns)
    turns = _flag_procedural(turns, srt_blocks)

    return turns


# ---------------------------------------------------------------------------
# Persistence helper (idempotent upsert)
# ---------------------------------------------------------------------------


def _upsert_turns(cursor, chapter_id: int, turns: list[Turn], table: str = "speaker_turns") -> None:
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
             confidence, source, is_procedural, procedural_reason, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (chapter_id, start_seconds) DO UPDATE SET
            end_seconds       = EXCLUDED.end_seconds,
            speaker_label     = EXCLUDED.speaker_label,
            resolved_name     = EXCLUDED.resolved_name,
            confidence        = EXCLUDED.confidence,
            source            = EXCLUDED.source,
            is_procedural     = EXCLUDED.is_procedural,
            procedural_reason = EXCLUDED.procedural_reason,
            updated_at        = NOW()
    """

    for turn in turns:
        cursor.execute(
            sql,
            (
                chapter_id,
                turn.start_seconds,
                turn.end_seconds,
                turn.speaker_label,
                turn.resolved_name,
                turn.confidence,
                turn.source,
                turn.is_procedural,
                turn.procedural_reason,
            ),
        )
