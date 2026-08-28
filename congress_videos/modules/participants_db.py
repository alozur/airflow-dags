"""
Database operations for the congress_participants table.

Mirrors the CongressionalVideoDB pattern: a class that owns a PostgresConnection,
builds schema-qualified table names in __init__, and issues parameterised SQL
inside `with pg.get_connection() as conn: with conn.cursor() as cur:` blocks.
"""

from __future__ import annotations

import logging

from utils.postgres_helpers import PostgresConnection
from congress_videos.modules.participants_ingestion import (
    ParticipantRecord,
    normalize_member_name,
)
from congress_videos.config.constants import WIKIDATA_FUZZY_THRESHOLD

logger = logging.getLogger(__name__)


class CongressParticipantsDB:
    """Database operations for the congress_participants table."""

    def __init__(self) -> None:
        self.pg_conn = PostgresConnection()
        self.participants_table = self.pg_conn.get_qualified_table("congress_participants")

    def upsert_batch(self, records: list[ParticipantRecord]) -> dict:
        """
        Insert or update a batch of ParticipantRecord rows.

        Uses ON CONFLICT (normalized_name) DO UPDATE so re-running the weekly
        ingestion is idempotent.

        COALESCE ordering rationale:
        - ``photo_url`` uses **table-first** COALESCE
          ``COALESCE(congress_participants.photo_url, EXCLUDED.photo_url)``:
          the stored enriched value wins over the always-None ingest value.
        - ``group_entry_date`` uses **EXCLUDED-first** COALESCE
          ``COALESCE(EXCLUDED.group_entry_date, congress_participants.group_entry_date)``:
          ingestion now always sends ``None`` because the opendataExport portlet
          omits this field.  EXCLUDED-first means a fresh real value from a
          future API run will still win when present; an incoming ``None``
          preserves whatever was previously stored, avoiding silent data loss.
          The asymmetry is deliberate — do not reorder these COALESCEs.

        Args:
            records: List of ParticipantRecord dicts from parse_deputies().

        Returns:
            {"upserted": int, "total": int}
        """
        if not records:
            return {"upserted": 0, "total": 0}

        upserted = 0
        sql = f"""
            INSERT INTO {self.participants_table}
                (normalized_name, slug, display_name, party, parliamentary_group, constituency,
                 biography, full_membership_date, start_date, group_entry_date, photo_url)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (normalized_name) DO UPDATE SET
                slug                 = EXCLUDED.slug,
                display_name         = EXCLUDED.display_name,
                party                = EXCLUDED.party,
                parliamentary_group  = EXCLUDED.parliamentary_group,
                constituency         = EXCLUDED.constituency,
                biography            = EXCLUDED.biography,
                full_membership_date = EXCLUDED.full_membership_date,
                start_date           = EXCLUDED.start_date,
                group_entry_date     = COALESCE(EXCLUDED.group_entry_date, congress_participants.group_entry_date),
                photo_url            = COALESCE(congress_participants.photo_url, EXCLUDED.photo_url),
                updated_at           = NOW()
        """

        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                for record in records:
                    params = (
                        record["normalized_name"],
                        record["slug"],
                        record["display_name"],
                        record.get("party"),
                        record.get("parliamentary_group"),
                        record.get("constituency"),
                        record.get("biography"),
                        record.get("full_membership_date"),
                        record.get("start_date"),
                        record.get("group_entry_date"),
                        record.get("photo_url"),
                    )
                    cur.execute(sql, params)
                    upserted += 1

        logger.info("upsert_batch: upserted %d / %d records", upserted, len(records))
        return {"upserted": upserted, "total": len(records)}

    def get_all_participants(self) -> list[dict]:
        """
        Return all rows from congress_participants ordered by normalized_name.

        Used by lookup functions and the enrichment step.
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT * FROM {self.participants_table} ORDER BY normalized_name"
                )
                return cur.fetchall()

    def update_photo_url(self, normalized_name: str, photo_url: str) -> None:
        """
        Set photo_url for a participant, guarded by WHERE photo_url IS NULL.

        This ensures enrichment never overwrites a manually set or
        previously enriched photo. Called by the Wikidata enrichment step.

        Args:
            normalized_name: Stable upsert key for the participant.
            photo_url: Wikidata image URL to store.
        """
        sql = f"""
            UPDATE {self.participants_table}
            SET photo_url = %s, updated_at = NOW()
            WHERE normalized_name = %s AND photo_url IS NULL
        """
        with self.pg_conn.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (photo_url, normalized_name))
        logger.info("update_photo_url: set photo for %r", normalized_name)


# ---------------------------------------------------------------------------
# Module-level helper used by lookup functions (injectable for testing)
# ---------------------------------------------------------------------------

def _get_participants_for_lookup() -> list[dict]:
    """Instantiate a DB and fetch all participants. Extracted for monkeypatching."""
    return CongressParticipantsDB().get_all_participants()


# ---------------------------------------------------------------------------
# Module-level lookup API
# ---------------------------------------------------------------------------

def get_participants_roster() -> list[dict]:
    """Public roster accessor for chapter_speaker_resolution (issue #263).

    Thin wrapper over :func:`_get_participants_for_lookup`. Named distinctly
    from :meth:`CongressParticipantsDB.get_all_participants` (class method,
    above) to avoid a collision between a module-level function and a class
    method that share a name — see the ratified A1 amendment.
    """
    return _get_participants_for_lookup()

def lookup_participant(name: str) -> dict | None:
    """
    Look up a participant by name using exact normalized_name match.

    Normalizes *name* via normalize_member_name before comparison.
    Does NOT fall back to fuzzy matching — callers decide whether to escalate.

    Args:
        name: Raw or normalized participant name.

    Returns:
        Row dict if an exact match is found, None otherwise.
    """
    key = normalize_member_name(name)
    rows = _get_participants_for_lookup()
    return next((r for r in rows if r["normalized_name"] == key), None)


def lookup_participant_by_slug(slug: str) -> dict | None:
    """Look up a participant by its exact stable slug.

    This lookup deliberately does not normalize or fuzz-match the identifier.

    Args:
        slug: Participant's stable slug.

    Returns:
        Row dict if an exact slug match is found, None otherwise.
    """
    rows = _get_participants_for_lookup()
    return next((row for row in rows if row.get("slug") == slug), None)


def lookup_participant_fuzzy(
    name: str, threshold: float = WIKIDATA_FUZZY_THRESHOLD
) -> dict | None:
    """
    Look up a participant using rapidfuzz token_sort_ratio fuzzy matching.

    Scores each candidate against BOTH ``normalized_name`` AND the normalized
    form of ``display_name`` and returns the candidate with the highest score
    across both fields.  This allows matching dirty speaker strings like
    "Pedro Sanchez" that may not closely match the full official normalized name
    but score well against the shorter display_name form (e.g. "Sánchez, Pedro").

    Returns the single best match whose score is >= threshold, or None.

    Args:
        name: Raw participant name to match.
        threshold: Minimum similarity ratio in [0, 1] (default: WIKIDATA_FUZZY_THRESHOLD = 0.90).

    Returns:
        Best-matching row dict (includes ``display_name``) if score >= threshold,
        None otherwise.
    """
    from rapidfuzz.fuzz import token_sort_ratio, token_set_ratio  # lazy import — Slice 2 dependency

    key = normalize_member_name(name)
    rows = _get_participants_for_lookup()
    best: dict | None = None
    best_score = 0.0
    for row in rows:
        norm = row["normalized_name"]
        disp = normalize_member_name(row["display_name"])
        # token_sort_ratio for full-name matches; token_set_ratio for partial/subset matches
        score_normalized = max(token_sort_ratio(key, norm), token_set_ratio(key, norm)) / 100.0
        score_display = max(token_sort_ratio(key, disp), token_set_ratio(key, disp)) / 100.0
        score = max(score_normalized, score_display)
        if score > best_score:
            best, best_score = row, score
    return best if best_score >= threshold else None
