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
from datetime import date
from pathlib import Path

from congress_videos.config.ai_prompts import (
    SPEAKER_MATCH_SYSTEM_PROMPT,
    SPEAKER_MATCH_USER_PROMPT_TEMPLATE,
)
from congress_videos.modules.institutional_role_resolver import CatalogLoader
from congress_videos.modules.participants_db import (
    lookup_participant_by_slug,
    lookup_participant_fuzzy,
)
from congress_videos.modules.participants_ingestion import normalize_member_name
from congress_videos.modules.speaker_placeholders import is_placeholder
from utils.llm_cache import cached_json_completion

logger = logging.getLogger(__name__)

# Table name — unqualified; callers supply a connected psycopg2 connection.
_CACHE_TABLE = "speaker_normalization_cache"
_CHAPTERS_TABLE = "video_chapters"

# Bundled institutional-role catalog, loaded lazily and cached per process.
_ROLE_CATALOG_PATH = Path(__file__).resolve().parents[1] / "catalogs" / "institutional_roles.v1.json"
_role_catalog = None


def _get_role_catalog():
    """Load and cache the bundled institutional-role catalog."""
    global _role_catalog
    if _role_catalog is None:
        _role_catalog = CatalogLoader(_ROLE_CATALOG_PATH).load()
    return _role_catalog


def _resolve_role(mention: str, on: date) -> tuple[str, str, bool] | None:
    """Resolve *mention* on *on* to ``(participant_slug, display_name, is_participant)``.

    The role resolves to a participant slug. When that slug matches a stored
    participant, ``is_participant`` is True and the live ``display_name`` wins;
    otherwise the catalog's curated ``display_name`` is used (e.g. ministers who
    are not sitting deputies). Returns ``None`` when *mention* is not a known role
    in force on that date, or no display name is available.
    """
    assignment = _get_role_catalog().resolve_assignment(mention, on)
    if assignment is None:
        return None
    row = lookup_participant_by_slug(assignment.participant_slug)
    if row and row.get("display_name"):
        return assignment.participant_slug, row["display_name"], True
    if assignment.display_name:
        return assignment.participant_slug, assignment.display_name, False
    return None


@dataclass
class NormalizationResult:
    """Return value of :func:`normalize_chapter_speakers`."""

    corrections: dict[str, str] = field(default_factory=dict)
    """Mapping of dirty_speaker -> canonical display_name for matched speakers."""

    cache_rows: list[dict] = field(default_factory=list)
    """One entry per unique dirty speaker that was processed (for audit/testing)."""

    updated: bool = False
    """True if video_chapters was written back (at least one match confirmed)."""

    resolved_participant_slug: str | None = None
    """Slug of the first confirmed matched participant, derived LLM-free from
    the candidate's normalized_name.  None when no match was confirmed."""


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _dedupe_raw_names(
    speakers: list[str],
    key_speakers: list[str],
    timeline: list[dict],
) -> list[str]:
    """Ordered, de-duplicated non-empty raw speaker strings (no placeholder filter).

    Used by institutional-role resolution, which must see role-only mentions
    (e.g. "Ministra de Defensa") *before* :func:`_dedupe_dirty_speakers` drops
    them as placeholders.
    """
    seen: list[str] = []
    seen_set: set[str] = set()
    for name in (
        list(speakers)
        + list(key_speakers)
        + [e.get("speaker", "") for e in timeline]
    ):
        if name and name not in seen_set:
            seen.append(name)
            seen_set.add(name)
    return seen


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
    session_date: date | None = None,
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

    _schema = os.getenv("POSTGRES_SCHEMA", "public")

    with db_conn.cursor() as cursor:
        cursor.execute(f"SET search_path TO {_schema}, public")

        # Step 0: deterministic institutional-role resolution (point-in-time),
        # run over the RAW speaker strings before placeholder filtering removes
        # role-only mentions ("Ministra de Defensa"). Resolved mentions never
        # reach the fuzzy/LLM pipeline because they are filtered as placeholders.
        if session_date is not None:
            for raw in _dedupe_raw_names(speakers, key_speakers, timeline):
                resolved = _resolve_role(raw, session_date)
                if resolved is None:
                    continue
                slug, role_name, is_participant = resolved
                if role_name == raw:
                    continue
                _upsert_cache_row(
                    cursor, chapter_id, raw,
                    status="matched",
                    canonical_speaker=role_name,
                    participant_normalized_name=slug.replace("-", " ") if is_participant else None,
                    confidence_score=1.0,
                )
                result.cache_rows.append({
                    "dirty_speaker": raw,
                    "status": "matched",
                    "canonical_speaker": role_name,
                    "confidence_score": 1.0,
                })
                result.corrections[raw] = role_name
                if result.resolved_participant_slug is None and is_participant:
                    result.resolved_participant_slug = slug
                logger.info(
                    "normalize_chapter_speakers: role-resolved %r -> %r (chapter %d)",
                    raw, role_name, chapter_id,
                )

        # Step 1: fuzzy + LLM pipeline over the placeholder-filtered dirty names.
        dirty_names = _dedupe_dirty_speakers(speakers, key_speakers, timeline)
        for dirty in dirty_names:
            # Step 1a: fuzzy lookup
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
                normalized_name = candidate.get("normalized_name") or ""
                _upsert_cache_row(
                    cursor, chapter_id, dirty,
                    status="matched",
                    canonical_speaker=canonical,
                    participant_normalized_name=normalized_name or None,
                    confidence_score=confidence,
                )
                result.cache_rows.append({
                    "dirty_speaker": dirty,
                    "status": "matched",
                    "canonical_speaker": canonical,
                    "confidence_score": confidence,
                })
                result.corrections[dirty] = canonical
                # Derive slug LLM-free from the authoritative normalized_name.
                # slug = REPLACE(normalized_name, ' ', '-') — same formula as migration 018.
                # Record only the first matched speaker's slug for this chapter.
                if result.resolved_participant_slug is None and normalized_name:
                    result.resolved_participant_slug = normalized_name.replace(" ", "-")
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

            if result.resolved_participant_slug is not None:
                cursor.execute(
                    f"""
                    UPDATE {_CHAPTERS_TABLE}
                    SET speakers     = %s,
                        key_speakers = %s,
                        timeline     = %s::jsonb,
                        resolved_participant_slug = %s,
                        updated_at   = CURRENT_TIMESTAMP
                    WHERE chapter_id = %s
                    """,
                    (new_speakers, new_key_speakers, json.dumps(new_timeline),
                     result.resolved_participant_slug, chapter_id),
                )
            else:
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
                "normalize_chapter_speakers: updated %d speaker(s) in chapter %d%s",
                len(result.corrections), chapter_id,
                f" (slug={result.resolved_participant_slug!r})"
                if result.resolved_participant_slug else "",
            )

    return result
