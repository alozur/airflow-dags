"""
Ingestion of active Spanish Congress of Deputies members.

Downloads the official active-deputies JSON feed from congreso.es via a POST
to the Liferay opendataExport portlet, normalizes member names into a stable
upsert key, and parses each entry into a JSON-serializable participant record.
"""

from __future__ import annotations

import logging
import os
import re
import unicodedata
from datetime import datetime
from typing import Any, TypedDict

import requests

from congress_videos.config.constants import (
    CONGRESO_BROWSER_USER_AGENT,
    CONGRESO_DEPUTIES_PORTLET_URL,
    LEGISLATURE_ID,
)

logger = logging.getLogger(__name__)

_DEPUTIES_URL_ENV_VAR = "CONGRESO_DEPUTIES_URL"
_REQUEST_TIMEOUT = 30
_SOURCE_DATE_FORMAT = "%d/%m/%Y"
_OUTPUT_DATE_FORMAT = "%Y-%m-%d"

# Liferay portlet form-field template.  fileType MUST be lowercase "json";
# "JSON" causes a 400 response from the portlet.
_PORTLET_FORM_FIELDS: dict[str, Any] = {
    "_diputadomodule_idLegislatura": LEGISLATURE_ID,
    "_diputadomodule_filtroProvincias": "[]",
    "_diputadomodule_fileIndex": 0,
    "_diputadomodule_fileType": "json",
    "_diputadomodule_genero": "",
    "_diputadomodule_grupo": "",
    "_diputadomodule_tipo": "",
    "_diputadomodule_nombre": "",
    "_diputadomodule_apellidos": "",
    "_diputadomodule_formacion": "",
    "_diputadomodule_nombreCircunscripcion": "",
}


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

    Steps: strip -> reorder "surnames, given" (official Nombre order) ->
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


def fetch_active_deputies() -> list[dict[str, Any]]:
    """
    Download the official active-deputies JSON feed.

    Default path: POST to the Liferay opendataExport portlet with a
    browser-grade User-Agent and the required form fields.  The response is a
    bare JSON array (~350 records) with TitleCase keys.

    Env-override path: when CONGRESO_DEPUTIES_URL is set, perform a GET to
    that URL instead (escape hatch for static-file local testing).

    Error handling:
    - HTTP 403: raises RuntimeError with a WAF-diagnostic message (URL +
      response body snippet) so an endpoint change or WAF block is
      distinguishable from a transient outage.
    - Other HTTP errors: propagate via raise_for_status() as requests.HTTPError.
    - Malformed JSON: raises ValueError with the request URL.
    - Empty payload: raises ValueError (zero active deputies is anomalous).
    """
    override = os.environ.get(_DEPUTIES_URL_ENV_VAR)

    if override:
        response = requests.get(override, timeout=_REQUEST_TIMEOUT)
        url = override
    else:
        response = requests.post(
            CONGRESO_DEPUTIES_PORTLET_URL,
            data=dict(_PORTLET_FORM_FIELDS),
            headers={"User-Agent": CONGRESO_BROWSER_USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
        )
        url = CONGRESO_DEPUTIES_PORTLET_URL

    if response.status_code == 403:
        snippet = (response.text or "")[:500]
        raise RuntimeError(
            f"congreso.es returned 403 for {url} — likely WAF block or portlet endpoint"
            f" change (not a transient outage). Response body snippet: {snippet!r}"
        )

    response.raise_for_status()

    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError(f"Malformed JSON response from {url}") from exc

    if not payload:
        raise ValueError(
            f"Empty deputies payload from {url} — no active deputies is anomalous"
        )

    return payload


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
    Parse raw opendataExport entries into ``ParticipantRecord`` dicts.

    Expects TitleCase keys from the opendataExport portlet response:
    ``Nombre``, ``Formacion``, ``GrupoParlamentario``, ``Circunscripcion``,
    ``Biografia``, ``FechaAlta``, ``FechaCondicionPlena``.

    ``group_entry_date`` is always ``None`` — the field
    ``FECHAALTAENGRUPOPARLAMENTARIO`` is absent from the opendataExport
    response.

    An empty payload raises ``ValueError`` — zero active deputies is
    anomalous and warrants alerting rather than silently upserting nothing.
    A record missing ``Nombre`` is skipped and logged rather than aborting
    the whole batch — one malformed upstream entry should not block every
    other deputy from syncing. If every entry ends up skipped (e.g. an
    upstream schema change dropping Nombre from every record), that is
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
        display_name = entry.get("Nombre")
        if not display_name:
            logger.warning("Skipping deputy entry with missing Nombre: %r", entry)
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
                party=entry.get("Formacion"),
                parliamentary_group=entry.get("GrupoParlamentario"),
                constituency=entry.get("Circunscripcion"),
                biography=entry.get("Biografia"),
                full_membership_date=_parse_source_date(entry.get("FechaCondicionPlena")),
                start_date=_parse_source_date(entry.get("FechaAlta")),
                group_entry_date=None,
                photo_url=None,
            )
        )

    if not records:
        raise ValueError(
            "All deputy entries were skipped (missing Nombre) — "
            "possible upstream schema change, treating as anomalous"
        )

    return records
