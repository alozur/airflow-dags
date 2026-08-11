"""Speaker normalization module for congress_videos.

Resolves free-text dirty speaker strings extracted by the LLM into canonical
``congress_participants`` entries, persists every mapping outcome in
``speaker_normalization_cache`` for idempotency and audit, and rewrites
confirmed chapter speaker fields with canonical display names.

Public API
----------
normalize_chapter_speakers(chapter_id, speakers, key_speakers, timeline,
                           db_conn, config) -> NormalizationResult
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field

from congress_videos.config.ai_prompts import (
    SPEAKER_MATCH_SYSTEM_PROMPT,
    SPEAKER_MATCH_USER_PROMPT_TEMPLATE,
)
from congress_videos.modules.participants_db import lookup_participant_fuzzy
from congress_videos.modules.participants_ingestion import normalize_member_name
from congress_videos.modules.speaker_placeholders import is_placeholder
from utils.llm_cache import cached_json_completion

logger = logging.getLogger(__name__)

# Table name — unqualified; callers supply a connected psycopg2 connection.
_CACHE_TABLE = "speaker_normalization_cache"
_CHAPTERS_TABLE = "video_chapters"


@dataclass
class NormalizationResult:
    """Return value of :func:`normalize_chapter_speakers`."""

    corrections: dict[str, str] = field(default_factory=dict)
    """Mapping of dirty_speaker -> canonical display_name for matched speakers."""

    cache_rows: list[dict] = field(default_factory=list)
    """One entry per unique dirty speaker that was processed (for audit/testing)."""

    updated: bool = False
    """True if video_chapters was written back (at least one match confirmed)."""


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _dedupe_dirty_speakers(
    speakers: list[str],
    key_speakers: list[str],
    timeline: list[dict],
) -> list[str]:
    """Return a de-duplicated, ordered list of unique dirty speaker names.

    Placeholder speaker strings (e.g. "Desconocido", "Unknown",
    "(No especificado)", single-token role abbreviations) are filtered out
    before deduplication so they are never sent to the normalization pipeline.
    """
    seen: list[str] = []
    seen_set: set[str] = set()
    for name in (
        list(speakers)
        + list(key_speakers)
        + [e.get("speaker", "") for e in timeline]
    ):
        if name and not is_placeholder(name) and name not in seen_set:
            seen.append(name)
            seen_set.add(name)
    return seen


def _upsert_cache_row(cursor, chapter_id: int, dirty_speaker: str, status: str,
                      canonical_speaker: str | None = None,
                      participant_normalized_name: str | None = None,
                      confidence_score: float | None = None) -> None:
    """INSERT or UPDATE a speaker_normalization_cache row."""
    cursor.execute(
        f"""
        INSERT INTO {_CACHE_TABLE}
            (chapter_id, dirty_speaker, canonical_speaker,
             participant_normalized_name, status, confidence_score)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (chapter_id, dirty_speaker) DO UPDATE SET
            canonical_speaker           = EXCLUDED.canonical_speaker,
            participant_normalized_name = EXCLUDED.participant_normalized_name,
            status                      = EXCLUDED.status,
            confidence_score            = EXCLUDED.confidence_score,
            updated_at                  = NOW()
        """,
        (chapter_id, dirty_speaker, canonical_speaker,
         participant_normalized_name, status, confidence_score),
    )


def _apply_corrections(
    items: list[str],
    corrections: dict[str, str],
) -> list[str]:
    """Return a new list with dirty names replaced by canonical names."""
    return [corrections.get(item, item) for item in items]


def _apply_corrections_to_timeline(
    timeline: list[dict],
    corrections: dict[str, str],
) -> list[dict]:
    """Return a new timeline with speaker fields replaced by canonical names."""
    result = []
    for entry in timeline:
        new_entry = dict(entry)
        speaker = new_entry.get("speaker", "")
        if speaker in corrections:
            new_entry["speaker"] = corrections[speaker]
        result.append(new_entry)
    return result


def _build_user_prompt(dirty_name: str, candidate: dict, context_enabled: bool) -> str:
    """Build the user prompt for the speaker-match AI call."""
    context_block = ""
    if context_enabled:
        context_block = ""  # no timeline snippet available at this level
    return SPEAKER_MATCH_USER_PROMPT_TEMPLATE.format(
        dirty_name=dirty_name,
        display_name=candidate.get("display_name", ""),
        normalized_name=candidate.get("normalized_name", ""),
        context_block=context_block,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def normalize_chapter_speakers(
    chapter_id: int,
    speakers: list[str],
    key_speakers: list[str],
    timeline: list[dict],
    db_conn,
    config,
) -> NormalizationResult:
    """Normalize dirty speaker strings for a single chapter.

    For each unique dirty speaker name found across ``speakers``,
    ``key_speakers``, and ``timeline[].speaker``:

    1. Call ``lookup_participant_fuzzy``; if no candidate → upsert ``no_match``.
    2. If a candidate exists, call ``cached_json_completion`` with the
       speaker-match prompt; if ``error`` is non-None → skip (no cache write).
    3. Parse ``result['data']``; upsert cache row with mapped status.
    4. On ``matched`` → record correction ``dirty → display_name``.
    5. After the loop: if any corrections, patch all three arrays Python-side and
       issue a single ``UPDATE video_chapters``.

    Args:
        chapter_id: PK of the video_chapters row.
        speakers:     Raw list of speaker names from the chapter.
        key_speakers: Raw list of key speaker names from the chapter.
        timeline:     List of timeline dicts (each with a 'speaker' key).
        db_conn:      Open psycopg2 connection (caller manages lifecycle).
        config:       Config module / namespace with ENABLED, FUZZY_THRESHOLD,
                      AI_MODEL, CONTEXT_ENABLED.

    Returns:
        :class:`NormalizationResult` with corrections, cache_rows, and updated flag.
    """
    result = NormalizationResult()

    if not config.ENABLED:
        logger.debug("normalize_chapter_speakers: ENABLED=False — skipping chapter %d", chapter_id)
        return result

    dirty_names = _dedupe_dirty_speakers(speakers, key_speakers, timeline)
    if not dirty_names:
        return result

    _schema = os.getenv("POSTGRES_SCHEMA", "public")

    with db_conn.cursor() as cursor:
        cursor.execute(f"SET search_path TO {_schema}, public")
        for dirty in dirty_names:
            # Step 1: fuzzy lookup
            candidate = lookup_participant_fuzzy(dirty, threshold=config.FUZZY_THRESHOLD)
            if candidate is None:
                logger.debug(
                    "normalize_chapter_speakers: no fuzzy match for %r (chapter %d)",
                    dirty, chapter_id,
                )
                _upsert_cache_row(cursor, chapter_id, dirty, "no_match")
                result.cache_rows.append({"dirty_speaker": dirty, "status": "no_match"})
                continue

            # Step 2: AI verification
            user_prompt = _build_user_prompt(dirty, candidate, config.CONTEXT_ENABLED)
            ai_response = cached_json_completion(
                SPEAKER_MATCH_SYSTEM_PROMPT,
                user_prompt,
                model=config.AI_MODEL,
            )

            if ai_response.get("error") is not None:
                logger.warning(
                    "normalize_chapter_speakers: AI error for %r (chapter %d): %s",
                    dirty, chapter_id, ai_response["error"],
                )
                # Graceful skip — no cache write per design
                continue

            data = ai_response.get("data") or {}
            decision = data.get("decision", "no_match")
            confidence = data.get("confidence")

            # Map AI decision to cache status
            if decision == "match":
                canonical = candidate["display_name"]
                _upsert_cache_row(
                    cursor, chapter_id, dirty,
                    status="matched",
                    canonical_speaker=canonical,
                    participant_normalized_name=candidate.get("normalized_name"),
                    confidence_score=confidence,
                )
                result.cache_rows.append({
                    "dirty_speaker": dirty,
                    "status": "matched",
                    "canonical_speaker": canonical,
                    "confidence_score": confidence,
                })
                result.corrections[dirty] = canonical
                logger.info(
                    "normalize_chapter_speakers: matched %r -> %r (chapter %d, confidence=%.2f)",
                    dirty, canonical, chapter_id, confidence or 0.0,
                )
            elif decision == "needs_manual":
                _upsert_cache_row(
                    cursor, chapter_id, dirty,
                    status="needs_manual",
                    confidence_score=confidence,
                )
                result.cache_rows.append({"dirty_speaker": dirty, "status": "needs_manual"})
            else:
                # no_match or unknown
                _upsert_cache_row(
                    cursor, chapter_id, dirty,
                    status="no_match",
                    confidence_score=confidence,
                )
                result.cache_rows.append({"dirty_speaker": dirty, "status": "no_match"})

        # Step 3: bulk UPDATE video_chapters if any corrections were recorded
        if result.corrections:
            new_speakers = _apply_corrections(speakers, result.corrections)
            new_key_speakers = _apply_corrections(key_speakers, result.corrections)
            new_timeline = _apply_corrections_to_timeline(timeline, result.corrections)

            cursor.execute(
                f"""
                UPDATE {_CHAPTERS_TABLE}
                SET speakers     = %s,
                    key_speakers = %s,
                    timeline     = %s::jsonb,
                    updated_at   = CURRENT_TIMESTAMP
                WHERE chapter_id = %s
                """,
                (new_speakers, new_key_speakers, json.dumps(new_timeline), chapter_id),
            )
            result.updated = True
            logger.info(
                "normalize_chapter_speakers: updated %d speaker(s) in chapter %d",
                len(result.corrections), chapter_id,
            )

    return result
