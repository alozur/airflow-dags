"""
Ingestion of active Spanish Congress of Deputies members.

Downloads the official ``DiputadosActivos__{timestamp}.json`` feed from
congreso.es, normalizes member names into a stable upsert key, and parses
each entry into a JSON-serializable participant record.
"""

from __future__ import annotations

import logging
import os
import re
import unicodedata
from datetime import datetime
from typing import Any, TypedDict
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from congress_videos.config.constants import CONGRESO_DEPUTIES_INDEX

logger = logging.getLogger(__name__)

_DEPUTIES_URL_ENV_VAR = "CONGRESO_DEPUTIES_URL"
_INDEX_REQUEST_TIMEOUT = 30
_DOWNLOAD_LINK_PATTERN = re.compile(r"DiputadosActivos__.*\.json", re.IGNORECASE)
_SOURCE_DATE_FORMAT = "%d/%m/%Y"
_OUTPUT_DATE_FORMAT = "%Y-%m-%d"


class ParticipantRecord(TypedDict):
    """JSON-serializable participant record (XCom-safe)."""

    normalized_name: str
    display_name: str
    party: str | None
    parliamentary_group: str | None
    constituency: str | None
    biography: str | None
    full_membership_date: str | None
    start_date: str | None
    group_entry_date: str | None
    photo_url: str | None


def normalize_member_name(name: str) -> str:
    """
    Build the stable, accent-insensitive upsert key for a deputy name.

    Steps: strip -> reorder "surnames, given" (official NOMBRE order) ->
    strip accents (NFKD, drop combining marks) -> casefold -> non-alphanumeric
    characters (including hyphens in compound surnames) become spaces ->
    collapse whitespace.
    """
    candidate = name.strip()

    if "," in candidate:
        surnames, _, given = candidate.partition(",")
        candidate = f"{given.strip()} {surnames.strip()}"

    decomposed = unicodedata.normalize("NFKD", candidate)
    without_accents = "".join(char for char in decomposed if not unicodedata.combining(char))
    folded = without_accents.casefold()
    spaced = re.sub(r"[^a-z0-9]+", " ", folded)
    return re.sub(r"\s+", " ", spaced).strip()


def resolve_download_url() -> str:
    """
    Resolve the current ``DiputadosActivos__{timestamp}.json`` download URL.

    ``CONGRESO_DEPUTIES_URL`` env var overrides discovery entirely (escape
    hatch for when the index page HTML structure changes). Otherwise scrapes
    ``CONGRESO_DEPUTIES_INDEX`` for the matching link.
    """
    override = os.environ.get(_DEPUTIES_URL_ENV_VAR)
    if override:
        return override

    response = requests.get(CONGRESO_DEPUTIES_INDEX, timeout=_INDEX_REQUEST_TIMEOUT)
    response.raise_for_status()

    soup = BeautifulSoup(response.content, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if _DOWNLOAD_LINK_PATTERN.search(href):
            return urljoin(CONGRESO_DEPUTIES_INDEX, href)

    raise ValueError(
        f"Could not find a DiputadosActivos download link on {CONGRESO_DEPUTIES_INDEX}"
    )


def fetch_active_deputies() -> list[dict[str, Any]]:
    """
    Download the official active-deputies JSON feed.

    HTTP errors propagate via ``raise_for_status`` and malformed JSON raises
    ``ValueError`` — ingestion failures must fail the task visibly, never
    silently proceed with stale data.
    """
    url = resolve_download_url()
    response = requests.get(url, timeout=_INDEX_REQUEST_TIMEOUT)
    response.raise_for_status()

    try:
        return response.json()
    except ValueError as exc:
        raise ValueError(f"Malformed JSON response from {url}") from exc


def _parse_source_date(value: str | None) -> str | None:
    """Parse a ``DD/MM/YYYY`` source date into ISO ``YYYY-MM-DD``, or None."""
    if not value:
        return None
    try:
        return datetime.strptime(value, _SOURCE_DATE_FORMAT).strftime(_OUTPUT_DATE_FORMAT)
    except ValueError:
        logger.warning("Unparseable date value %r — storing as None", value)
        return None


def parse_deputies(raw: list[dict[str, Any]]) -> list[ParticipantRecord]:
    """
    Parse raw ``DiputadosActivos`` entries into ``ParticipantRecord`` dicts.

    An empty payload raises ``ValueError`` — zero active deputies is
    anomalous and warrants alerting rather than silently upserting nothing.
    A record missing ``NOMBRE`` is skipped and logged rather than aborting
    the whole batch — one malformed upstream entry should not block every
    other deputy from syncing. If every entry ends up skipped (e.g. an
    upstream schema change dropping NOMBRE from every record), that is
    treated the same as an empty payload and raises ``ValueError`` — the
    batch must never silently degrade to zero parsed deputies.
    Duplicate normalized names within the batch are logged (first
    occurrence kept) and not deduplicated here (upsert layer also keeps
    first occurrence).
    """
    if not raw:
        raise ValueError("Empty deputies payload — no active deputies is anomalous")

    records: list[ParticipantRecord] = []
    seen_normalized: set[str] = set()
    for entry in raw:
        display_name = entry.get("NOMBRE")
        if not display_name:
            logger.warning("Skipping deputy entry with missing NOMBRE: %r", entry)
            continue

        normalized_name = normalize_member_name(display_name)
        if normalized_name in seen_normalized:
            logger.warning(
                "Duplicate normalized_name %r in batch — keeping first occurrence",
                normalized_name,
            )
            continue
        seen_normalized.add(normalized_name)

        records.append(
            ParticipantRecord(
                normalized_name=normalized_name,
                display_name=display_name,
                party=entry.get("FORMACIONELECTORAL"),
                parliamentary_group=entry.get("GRUPOPARLAMENTARIO"),
                constituency=entry.get("CIRCUNSCRIPCION"),
                biography=entry.get("BIOGRAFIA"),
                full_membership_date=_parse_source_date(entry.get("FECHACONDICIONPLENA")),
                start_date=_parse_source_date(entry.get("FECHAALTA")),
                group_entry_date=_parse_source_date(entry.get("FECHAALTAENGRUPOPARLAMENTARIO")),
                photo_url=None,
            )
        )

    if not records:
        raise ValueError(
            "All deputy entries were skipped (missing NOMBRE) — "
            "possible upstream schema change, treating as anomalous"
        )

    return records
