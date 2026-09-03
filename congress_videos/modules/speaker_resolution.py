"""Pure speaker-resolution module for the turn prepare pipeline (issue #177).

Identifies the speaking participant from the president's intro announcement
(SRT window before the turn starts) plus the first seconds of the turn itself.
Persisted via mark_turn_resolved; consumed in-memory by thumbnail/sidecar steps.

Design constraints:
- resolve_speaker MUST never raise. Any exception returns None.
- All DB I/O and SRT I/O is injected or happens outside this module.
- completion_fn defaults to cached_json_completion (Postgres-backed LLM cache).

NOTE: imports from congress_videos.srt_helpers and utils.llm_cache are done at
module level to enable clean patching in tests. Neither import pulls in task or
scheduler constructs, so this module is safe to parse under the scheduler.
"""
from __future__ import annotations

import logging
import unicodedata
from collections.abc import Callable

from rapidfuzz import fuzz

from congress_videos.config.ai_prompts import (
    SPEAKER_RESOLUTION_SYSTEM_PROMPT,
    SPEAKER_RESOLUTION_USER_TEMPLATE,
    SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE,
)
from congress_videos.config.paths import get_video_chapter_dir
from congress_videos.modules.announcement_patterns import has_announcement_phrase
from congress_videos.srt_helpers import (
    _parse_srt_blocks,
    _srt_timestamp_to_seconds,
    chapter_window_blocks,
    find_srt_for_chapter,
)
from utils.llm_config import LLM_CHEAP

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Public constants (tested directly by spec)
# ---------------------------------------------------------------------------

INTRO_WINDOW_SECS: int = 120
"""Seconds before turn start to include as the president's intro window."""

TURN_CONTEXT_SECS: int = 60
"""Seconds of the turn itself to include as context."""

SPEAKER_RESOLUTION_MIN_CONFIDENCE: float = 0.80
"""Minimum model confidence required to accept a resolution result."""

EVIDENCE_MIN_PARTIAL_RATIO: int = 85
"""Minimum rapidfuzz partial_ratio between normalized evidence and the
normalized model-visible text for a candidate's evidence to be accepted."""

EVIDENCE_MIN_NORMALIZED_CHARS: int = 12
"""Evidence shorter than this many normalized characters is rejected
outright, without computing partial_ratio."""

REQUIRE_ANNOUNCEMENT_PHRASE: bool = True
"""Kill switch (issue #284): when True, resolve_speaker refuses to call
completion_fn unless intro_text or turn_text contains a presiding-officer
announcement phrase. Flip to False to fully revert the pre-gate."""

QA_EVIDENCE_LOOKBACK_SECS: int = 600
"""Backwards-only widening (issue #322) of the anchored evidence-gate
region: how far before ``intro_anchor`` accepted evidence may be located,
clamped to the chapter's own start. The forward edge
(``start_seconds + TURN_CONTEXT_SECS``) is unaffected by this constant."""

ANCHOR_JOIN_BLOCKS: int = 3
"""Maximum number of consecutive SRT blocks joined by
``_evidence_supported_in_blocks`` when locating evidence that straddles a
block boundary (issue #322)."""

QA_WIDE_CONTEXT_ENABLED: bool = True
"""Kill switch (issue #322): qa turns with a parseable chapter span get
wide prompt context + a matching wide pre-gate (D4); False reverts both."""

QA_CONTEXT_MAX_CHARS: int = 40_000
"""Hard cap on rendered qa chapter text; over this, head+tail hybrid (D7)."""

QA_CONTEXT_HEAD_CHARS: int = 4_000
"""Opening slice size for the head+tail hybrid — covers the agenda (D7)."""

QA_TRUNCATION_MARKER: str = "\n[... transcript truncated ...]\n"
"""Marker joining head/tail slices when qa chapter text is truncated (D7)."""


# ---------------------------------------------------------------------------
# Evidence normalization / verification (issue #284)
# ---------------------------------------------------------------------------

def _normalize_for_evidence(text: str) -> str:
    """casefold -> NFD -> drop combining marks -> collapse whitespace."""
    folded = text.casefold()
    decomposed = unicodedata.normalize("NFD", folded)
    stripped = "".join(c for c in decomposed if not unicodedata.combining(c))
    return " ".join(stripped.split())


def _evidence_supported(evidence: str | None, source_text: str) -> bool:
    """False when evidence is None/blank, shorter than
    EVIDENCE_MIN_NORMALIZED_CHARS normalized chars, or its normalized form's
    rapidfuzz partial_ratio against the normalized source_text is below
    EVIDENCE_MIN_PARTIAL_RATIO. Never raises: non-str evidence is rejected
    rather than crashing on ``.casefold()``.
    """
    if not evidence or not isinstance(evidence, str):
        return False

    normalized_evidence = _normalize_for_evidence(evidence)
    if len(normalized_evidence) < EVIDENCE_MIN_NORMALIZED_CHARS:
        return False

    normalized_text = _normalize_for_evidence(source_text)
    ratio = fuzz.partial_ratio(normalized_evidence, normalized_text)
    return ratio >= EVIDENCE_MIN_PARTIAL_RATIO


def _evidence_supported_in_blocks(
    evidence: str | None,
    blocks: list[dict],
    join_size: int = ANCHOR_JOIN_BLOCKS,
) -> bool:
    """True when *evidence* is located within a sliding join of up to
    *join_size* consecutive *blocks* (issue #322).

    Tries every window of 1..join_size consecutive blocks, in order, and
    reuses ``_evidence_supported`` VERBATIM as the per-window primitive —
    this keeps the 12-char floor, NFD normalization, and
    EVIDENCE_MIN_PARTIAL_RATIO threshold identical to the pre-anchor gate.
    ``blocks`` MUST already be pre-filtered to the accepted region by the
    caller: this function only tries joins over what it is given, it does
    not re-check any timestamp. Empty ``blocks`` is always False. Never
    raises.
    """
    if not blocks:
        return False

    n = len(blocks)
    for start_idx in range(n):
        for size in range(1, join_size + 1):
            end_idx = start_idx + size
            if end_idx > n:
                break
            joined_text = "\n".join(b["text"] for b in blocks[start_idx:end_idx])
            if _evidence_supported(evidence, joined_text):
                return True
    return False


def _chapter_span(turn: dict) -> tuple[float, float] | None:
    """Parse the chapter's ``[start, end)`` span from ``turn["start_time"]``/
    ``turn["end_time"]`` (issue #322) — VARCHAR SRT-format strings added to
    ``select_unprepared_turns`` from ``video_chapters``.

    Returns ``(start_seconds, end_seconds)`` or ``None`` on a missing key,
    non-str value, unparseable timestamp, or ``end <= start``. Never raises.
    """
    try:
        start = _srt_timestamp_to_seconds(turn["start_time"])
        end = _srt_timestamp_to_seconds(turn["end_time"])
    except (KeyError, ValueError, TypeError):
        return None

    if end <= start:
        return None

    return (start, end)


def _render_chapter_block(block: dict) -> str:
    """Render one SRT block as ``[HH:MM:SS] text`` (issue #322, D6)."""
    total = max(0, int(block["start_secs"]))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    return f"[{h:02d}:{m:02d}:{s:02d}] {block['text']}"


def _build_qa_chapter_text(blocks: list[dict]) -> str:
    """Render *blocks* as ``[HH:MM:SS] text`` joined by ``\\n`` (D7).

    Passthrough when under ``QA_CONTEXT_MAX_CHARS``. Otherwise a
    block-granular head+tail hybrid: an opening slice capped at
    ``QA_CONTEXT_HEAD_CHARS`` (first block always kept — carries the
    chapter-opening agenda/roll-call) plus a tail nearest the turn (last
    block always kept), joined by ``QA_TRUNCATION_MARKER``. Head/tail never
    overlap. Empty ``blocks`` returns ``""``. Never raises.
    """
    if not blocks:
        return ""

    rendered = [_render_chapter_block(b) for b in blocks]
    full_text = "\n".join(rendered)
    if len(full_text) <= QA_CONTEXT_MAX_CHARS:
        return full_text

    # Head: always keep the first block (block-granular, never mid-block).
    head_lines = [rendered[0]]
    head_len = len(rendered[0])
    head_end_idx = 1
    for i in range(1, len(rendered)):
        line = rendered[i]
        added = len(line) + 1  # +1 for the joining newline
        if head_len + added > QA_CONTEXT_HEAD_CHARS:
            break
        head_lines.append(line)
        head_len += added
        head_end_idx = i + 1
    head_text = "\n".join(head_lines)

    # Tail: always keep the last block (nearest the turn).
    tail_lines = [rendered[-1]]
    tail_len = len(rendered[-1])
    remaining_budget = max(
        0, QA_CONTEXT_MAX_CHARS - len(head_text) - len(QA_TRUNCATION_MARKER) - tail_len
    )
    for i in range(len(rendered) - 2, head_end_idx - 1, -1):
        line = rendered[i]
        added = len(line) + 1
        if added > remaining_budget:
            break
        tail_lines.insert(0, line)
        remaining_budget -= added
    tail_text = "\n".join(tail_lines)

    return f"{head_text}{QA_TRUNCATION_MARKER}{tail_text}"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def resolve_speaker(
    turn: dict,
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> dict | None:
    """Identify the participant speaking in a turn using SRT context windows.

    Returns a dict ``{participant_slug, confidence, evidence}`` when the model
    attributes the turn with confidence >= SPEAKER_RESOLUTION_MIN_CONFIDENCE
    and the returned slug exists in *participants*. Returns None on any failure,
    missing SRT, low confidence, or hallucinated slug.

    This function NEVER raises. All internal exceptions are caught and logged.

    Args:
        turn: Row dict from select_unprepared_turns. Must contain at least
            ``start_seconds``, ``video_id``, ``chapter_id``, ``session_date``.
        participants: List of participant dicts, each with at minimum a ``slug``
            and ``display_name`` key. Fetched once per prepare loop by the caller.
        completion_fn: Optional override for the LLM completion call. Defaults
            to ``utils.llm_cache.cached_json_completion``. Must accept
            (system_prompt, user_prompt, **kwargs) and return a dict with
            ``data`` and ``error`` keys (same contract as cached_json_completion).

    Returns:
        ``{"participant_slug": str, "confidence": float, "evidence": str}``
        or ``None``.
    """
    try:
        return _resolve_speaker_inner(turn, participants, completion_fn)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "resolve_speaker: unexpected exception for turn_id=%s — returning None (%s: %s)",
            turn.get("turn_id"),
            type(exc).__name__,
            exc,
        )
        return None


# ---------------------------------------------------------------------------
# Internal implementation
# ---------------------------------------------------------------------------

def _resolve_speaker_inner(
    turn: dict,
    participants: list[dict],
    completion_fn: Callable | None,
) -> dict | None:
    """Internal resolver (may raise; wrapped by resolve_speaker)."""
    if not participants:
        logger.debug("resolve_speaker: empty participants list — returning None")
        return None

    # Build the valid slug set for validation
    valid_slugs: set[str] = {p["slug"] for p in participants}

    # Locate SRT file for this turn's source video
    video_id = turn.get("video_id")
    chapter_id = turn.get("chapter_id")
    session_date = turn.get("session_date")

    # Issue #340 slice 2: prefer the persisted per-chapter sidecar written
    # by the monitor DAG; find_srt_for_chapter falls back to the legacy
    # downloads/ probes when the canonical file is absent or canonical_dir
    # is None. chapter_id must be truthy (this site already coerces it to
    # 0 below) — a falsy chapter_id has no meaningful chapter directory.
    canonical_dir = (
        str(get_video_chapter_dir(str(video_id), chapter_id))
        if video_id is not None and chapter_id
        else None
    )

    srt_path = find_srt_for_chapter(
        str(video_id) if video_id is not None else "",
        chapter_id or 0,
        str(session_date) if session_date else None,
        canonical_dir,
    )

    if srt_path is None:
        logger.debug(
            "resolve_speaker: no SRT found for turn_id=%s video_id=%s — returning None",
            turn.get("turn_id"),
            video_id,
        )
        return None

    # Parse all SRT blocks
    all_blocks = _parse_srt_blocks(srt_path)

    start_secs = float(turn.get("start_seconds", 0))

    # Intro window anchor (issue #283 rule 3): use the group's real start
    # when available (MIN(start_seconds) OVER the materialized group), not
    # the representative turn's own start_seconds — the representative may
    # be a diarization blip mid-group. Explicit `is not None` so a
    # legitimate group_start_seconds == 0.0 is honoured rather than falling
    # through to start_secs.
    group_start = turn.get("group_start_seconds")
    intro_anchor = float(group_start) if group_start is not None else start_secs

    # Intro window: [intro_anchor - INTRO_WINDOW_SECS, intro_anchor)
    intro_start = max(0.0, intro_anchor - INTRO_WINDOW_SECS)
    intro_blocks = [
        b for b in all_blocks
        if b["start_secs"] >= intro_start and b["end_secs"] <= intro_anchor
    ]

    # Turn context window: [start, start + TURN_CONTEXT_SECS)
    turn_end = start_secs + TURN_CONTEXT_SECS
    turn_blocks = [
        b for b in all_blocks
        if b["start_secs"] >= start_secs and b["start_secs"] < turn_end
    ]

    # Anchored evidence-gate region (issue #322): the ONLY place accepted
    # evidence may be located, uniform for every turn_type. Backward edge
    # widens up to QA_EVIDENCE_LOOKBACK_SECS before intro_anchor, clamped to
    # the chapter's own start when a valid chapter span is available (fails
    # safe to 0.0 otherwise — D5). Forward edge is unchanged: no block at or
    # after start_secs + TURN_CONTEXT_SECS can ever appear in region_blocks,
    # so evidence past that edge is rejected by construction, regardless of
    # confidence.
    chapter_span = _chapter_span(turn)
    chapter_start_seconds = chapter_span[0] if chapter_span is not None else 0.0
    region_start = max(chapter_start_seconds, intro_anchor - QA_EVIDENCE_LOOKBACK_SECS)
    region_end = turn_end
    region_blocks = [
        b for b in all_blocks
        if region_start <= b["start_secs"] < region_end
    ]

    # Both windows may be empty at the start of a session — still attempt resolution
    # if we have at least some turn-window content.
    if not intro_blocks and not turn_blocks:
        logger.debug(
            "resolve_speaker: no SRT blocks in either window for turn_id=%s — returning None",
            turn.get("turn_id"),
        )
        return None

    intro_text = "\n".join(b["text"] for b in intro_blocks) if intro_blocks else "(no intro)"
    turn_text = "\n".join(b["text"] for b in turn_blocks) if turn_blocks else "(no turn context)"
    combined_text = f"{intro_text}\n{turn_text}"

    # qa-gated chapter-wide prompt context (issue #322, D1/D7/D8): only for
    # turn_type == 'qa' with a parseable chapter span and the kill switch
    # on. chapter_text is the single source of truth both the pre-gate (D4)
    # and the prompt builder below read, so they can never drift apart.
    turn_type = turn.get("turn_type")
    chapter_text: str | None = None
    if QA_WIDE_CONTEXT_ENABLED and turn_type == "qa":
        if chapter_span is not None:
            prompt_blocks = chapter_window_blocks(all_blocks, chapter_start_seconds, region_end)
            chapter_text = _build_qa_chapter_text(prompt_blocks)
        else:
            logger.warning(
                "resolve_speaker: turn_id=%s is turn_type='qa' but the chapter "
                "span is unparseable — falling back to narrow intro+turn "
                "prompt context",
                turn.get("turn_id"),
            )

    wide_context_active = chapter_text is not None
    prompt_text_for_gate = chapter_text if wide_context_active else combined_text

    # Announcement pre-gate (issue #284, rebound per #322 D4): reads the
    # SAME text the prompt will show the model — wide for qa, narrow
    # otherwise — so the two can never disagree.
    if REQUIRE_ANNOUNCEMENT_PHRASE and not has_announcement_phrase(prompt_text_for_gate):
        logger.info(
            "resolve_speaker: no announcement phrase in model-visible text for "
            "turn_id=%s — skipping LLM call",
            turn.get("turn_id"),
        )
        return None

    # Serialize participant roster: slug | display_name | party
    roster_lines = [
        f"{p['slug']} | {p.get('display_name', '')} | {p.get('party', '')}"
        for p in participants
    ]
    participant_roster = "\n".join(roster_lines)

    if wide_context_active:
        user_prompt = SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE.format(
            chapter_text=chapter_text,
            intro_text=intro_text,
            turn_text=turn_text,
            participant_roster=participant_roster,
        )
    else:
        user_prompt = SPEAKER_RESOLUTION_USER_TEMPLATE.format(
            intro_text=intro_text,
            turn_text=turn_text,
            participant_roster=participant_roster,
        )

    # Call the LLM (or injected stub)
    if completion_fn is None:
        from utils.llm_cache import cached_json_completion
        completion_fn = cached_json_completion

    response = completion_fn(
        SPEAKER_RESOLUTION_SYSTEM_PROMPT,
        user_prompt,
        model=LLM_CHEAP,
    )

    if response.get("error") or not response.get("data"):
        logger.debug(
            "resolve_speaker: completion error for turn_id=%s: %s",
            turn.get("turn_id"),
            response.get("error"),
        )
        return None

    data = response["data"]

    slug = data.get("participant_slug")
    confidence = data.get("confidence")
    evidence = data.get("evidence", "")

    # Validate slug: must be in participants, confidence must be >= threshold
    if not slug or slug not in valid_slugs:
        logger.debug(
            "resolve_speaker: hallucinated or null slug %r for turn_id=%s — returning None",
            slug,
            turn.get("turn_id"),
        )
        return None

    try:
        confidence = float(confidence)
    except (TypeError, ValueError):
        logger.debug(
            "resolve_speaker: invalid confidence %r for turn_id=%s — returning None",
            confidence,
            turn.get("turn_id"),
        )
        return None

    if confidence < SPEAKER_RESOLUTION_MIN_CONFIDENCE:
        logger.debug(
            "resolve_speaker: confidence %.2f < %.2f for turn_id=%s — returning None",
            confidence,
            SPEAKER_RESOLUTION_MIN_CONFIDENCE,
            turn.get("turn_id"),
        )
        return None

    # Evidence verification (issue #284, anchored per issue #322):
    # independent of self-reported confidence — a candidate whose evidence
    # cannot be located within region_blocks (the anchored gate) is rejected
    # even at confidence 0.99. Runs last so every earlier return path/log
    # line above stays byte-identical.
    if not _evidence_supported_in_blocks(evidence, region_blocks):
        logger.info(
            "resolve_speaker: evidence not locatable in model-visible text "
            "for turn_id=%s — returning None",
            turn.get("turn_id"),
        )
        return None

    logger.info(
        "resolve_speaker: resolved turn_id=%s → slug=%r confidence=%.2f",
        turn.get("turn_id"),
        slug,
        confidence,
    )
    return {"participant_slug": slug, "confidence": confidence, "evidence": evidence}
