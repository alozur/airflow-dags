"""
Wikidata photo enrichment and congreso.es official-photo fallback for the congress_participants table.

Queries the Wikidata SPARQL endpoint for Spanish Congress of Deputies members,
fuzzy-joins SPARQL labels to normalized_name rows, and back-fills photo_url
for rows where it is currently NULL.

Also provides a deterministic fallback that fetches codParlamentario values via
the searchDiputados Liferay portlet and builds official photo URLs from the
congreso.es deterministic path.
"""

from __future__ import annotations

import logging
import os

import requests
from rapidfuzz.fuzz import token_sort_ratio

from congress_videos.config.constants import (
    CONGRESO_BROWSER_USER_AGENT,
    CONGRESO_PHOTO_URL_TEMPLATE,
    CONGRESO_SEARCH_DIPUTADOS_URL,
    LEGISLATURE_ID,
    WIKIDATA_FUZZY_THRESHOLD,
    WIKIDATA_SPARQL_URL,
    WIKIDATA_TIMEOUT,
    WIKIDATA_USER_AGENT,
)
from congress_videos.modules.participants_db import CongressParticipantsDB
from congress_videos.modules.participants_ingestion import normalize_member_name

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_SEARCH_DIPUTADOS_ENV_VAR = "CONGRESO_SEARCH_DIPUTADOS_URL_OVERRIDE"
_REQUEST_TIMEOUT = 30

# Candidate key lists for defensive extraction from searchDiputados portlet.
# Verified live (2026-07-30, Spain egress): the portlet returns {"data": [...]}
# with keys apellidosNombre ("Apellidos, Nombre" full name) and codParlamentario
# (int). apellidosNombre MUST come before nombre — nombre is the given name only,
# which would break the normalized-name join. Order matters — first non-empty wins.
_COD_CANDIDATE_KEYS = ["codParlamentario", "codigo", "cod", "id"]
_NAME_CANDIDATE_KEYS = ["apellidosNombre", "nombreCompleto", "nombre"]

# Liferay searchDiputados dataRequest body. Verified live: a bare
# {"idLegislatura": 15} body makes the portlet return {"data": []}. The full
# _diputadomodule_* form fields are required (same portlet as opendataExport,
# minus the file-export fileIndex/fileType fields).
_SEARCH_PORTLET_FORM_FIELDS: dict[str, object] = {
    "_diputadomodule_idLegislatura": LEGISLATURE_ID,
    "_diputadomodule_filtroProvincias": "[]",
    "_diputadomodule_genero": "",
    "_diputadomodule_grupo": "",
    "_diputadomodule_tipo": "",
    "_diputadomodule_nombre": "",
    "_diputadomodule_apellidos": "",
    "_diputadomodule_formacion": "",
    "_diputadomodule_nombreCircunscripcion": "",
}


def fetch_congreso_cod_parlamentario() -> dict[str, str]:
    """Bulk searchDiputados POST → {normalized_name: codParlamentario}.

    Issues exactly one POST to CONGRESO_SEARCH_DIPUTADOS_URL per call, using
    a browser User-Agent header as required by the portlet WAF.

    Env-override path: when CONGRESO_SEARCH_DIPUTADOS_URL_OVERRIDE is set,
    performs a GET to that URL instead (escape hatch for static-file local testing).

    Defensive key extraction tries candidate key lists for both codParlamentario
    and the deputy name fields (first non-empty wins). Entries missing either
    key are skipped with a DEBUG log — live key names from the portlet are
    unconfirmed until a Verify step runs against the live endpoint.

    Returns:
        Dict mapping normalized_name (str) → codParlamentario (str) for all
        parseable entries in the response.

    Raises:
        RuntimeError: if the endpoint returns HTTP 403 (WAF block / endpoint change).
        requests.HTTPError: on other non-2xx responses.
    """
    override = os.environ.get(_SEARCH_DIPUTADOS_ENV_VAR)

    if override:
        response = requests.get(override, timeout=_REQUEST_TIMEOUT)
        url = override
    else:
        response = requests.post(
            CONGRESO_SEARCH_DIPUTADOS_URL,
            data=dict(_SEARCH_PORTLET_FORM_FIELDS),
            headers={"User-Agent": CONGRESO_BROWSER_USER_AGENT},
            timeout=_REQUEST_TIMEOUT,
        )
        url = CONGRESO_SEARCH_DIPUTADOS_URL

    if response.status_code == 403:
        snippet = (response.text or "")[:200]
        raise RuntimeError(
            f"searchDiputados 403: {url} — possible WAF block or endpoint change. Response body snippet: {snippet!r}"
        )

    response.raise_for_status()

    payload = response.json()
    # The portlet may return a bare list or a {"data": [...]} envelope.
    if isinstance(payload, dict):
        entries = payload.get("data", [])
    else:
        entries = payload

    result: dict[str, str] = {}
    for entry in entries:
        # Extract codParlamentario using candidate keys; first non-empty wins.
        cod = None
        for key in _COD_CANDIDATE_KEYS:
            value = entry.get(key)
            if value:
                cod = str(value)
                break

        if not cod:
            logger.debug("fetch_congreso_cod_parlamentario: skipping entry with no cod key — %r", entry)
            continue

        # Extract deputy name using candidate keys; first non-empty wins.
        raw_name = None
        for key in _NAME_CANDIDATE_KEYS:
            value = entry.get(key)
            if value:
                raw_name = str(value)
                break

        if not raw_name:
            logger.debug("fetch_congreso_cod_parlamentario: skipping entry with no name key — %r", entry)
            continue

        normalized = normalize_member_name(raw_name)
        result[normalized] = cod

    logger.info("fetch_congreso_cod_parlamentario: resolved %d deputies", len(result))
    return result


def fill_congreso_photo_fallback() -> dict:
    """For each participant with photo_url IS NULL: look up codParlamentario by
    normalized_name, build the deterministic congreso.es photo URL, and write it
    via update_photo_url (which guards with WHERE photo_url IS NULL to avoid
    overwriting existing Wikidata Commons photos).

    Issues exactly one bulk searchDiputados POST per call — never one request per deputy.

    Error handling: any exception from fetch_congreso_cod_parlamentario (including
    RuntimeError on 403, or requests.ConnectionError on network failure) is caught,
    logged at ERROR level, and results in an early return with filled=0 and an error
    key. The DAG task never fails due to a fallback source outage.

    Returns:
        Dict with keys:
          - ``filled`` (int): rows successfully updated
          - ``skipped_no_cod`` (int): rows skipped because no codParlamentario was found
          - ``source_count`` (int): number of deputies in the searchDiputados response
          - ``error`` (str): error message if the fetch failed (only present on error path)
    """
    db = CongressParticipantsDB()

    try:
        cod_map = fetch_congreso_cod_parlamentario()
    except Exception as exc:
        logger.error("fill_congreso_photo_fallback: fetch failed — %s", exc)
        return {"filled": 0, "skipped_no_cod": 0, "source_count": 0, "error": str(exc)}

    source_count = len(cod_map)
    all_rows = db.get_all_participants()
    null_photo_rows = [r for r in all_rows if r.get("photo_url") is None]

    filled = 0
    skipped_no_cod = 0

    for row in null_photo_rows:
        key = row["normalized_name"]
        cod = cod_map.get(key)

        if not cod:
            logger.warning("fill_congreso_photo_fallback: no codParlamentario for %r — skipping", key)
            skipped_no_cod += 1
            continue

        # Optimistic URL write — no HEAD pre-check (deterministic URL; broken-image cost
        # is trivial compared to ~350 extra HEAD requests/week + WAF exposure).
        url = CONGRESO_PHOTO_URL_TEMPLATE.format(cod=cod, leg=LEGISLATURE_ID)
        db.update_photo_url(key, url)
        filled += 1

    logger.info(
        "fill_congreso_photo_fallback: filled=%d skipped_no_cod=%d source_count=%d",
        filled,
        skipped_no_cod,
        source_count,
    )
    return {"filled": filled, "skipped_no_cod": skipped_no_cod, "source_count": source_count}


# ---------------------------------------------------------------------------
# SPARQL query: current members of the Spanish Congress of Deputies (Q18171345)
# P39 = position held; P580 = start time qualifier; P582 = end time qualifier
# P18 = image
# ---------------------------------------------------------------------------
SPARQL_QUERY = """
SELECT ?person ?personLabel ?image WHERE {
  ?person wdt:P39 wd:Q18171345 .
  ?person p:P39 ?stmt .
  ?stmt pq:P580 ?startDate .
  OPTIONAL { ?stmt pq:P582 ?endDate . }
  FILTER(!BOUND(?endDate) || ?endDate >= NOW())
  OPTIONAL { ?person wdt:P18 ?image . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "es,en" . }
}
"""


def fetch_wikidata_photos() -> list[dict]:
    """
    Query Wikidata SPARQL for current Spanish Congress of Deputies members.

    Issues an HTTP GET to WIKIDATA_SPARQL_URL with the position-filter SPARQL
    query.  The ``User-Agent`` header is sourced from ``WIKIDATA_USER_AGENT``
    as required by the Wikidata bot policy.

    Returns:
        List of dicts with keys:
          - ``label`` (str): personLabel from SPARQL (the deputy's name)
          - ``image_url`` (str | None): Wikimedia Commons URL, or None if absent

    Raises:
        requests.HTTPError: on any non-2xx HTTP response.
        ValueError: on malformed JSON response.
    """
    headers = {
        "Accept": "application/sparql-results+json",
        "User-Agent": WIKIDATA_USER_AGENT,
    }
    params = {"query": SPARQL_QUERY, "format": "json"}

    response = requests.get(
        WIKIDATA_SPARQL_URL,
        headers=headers,
        params=params,
        timeout=WIKIDATA_TIMEOUT,
    )
    response.raise_for_status()

    data = response.json()
    bindings = data["results"]["bindings"]

    result = []
    for b in bindings:
        label = b["personLabel"]["value"]
        image_url = b.get("image", {}).get("value")
        result.append({"label": label, "image_url": image_url})

    logger.info("fetch_wikidata_photos: retrieved %d bindings", len(result))
    return result


def enrich_missing_photos() -> dict:
    """
    Back-fill photo_url for congress_participants rows where it is NULL.

    Fetches all participants from the DB, filters to those missing a photo,
    then fuzzy-joins Wikidata SPARQL labels using token_sort_ratio at
    WIKIDATA_FUZZY_THRESHOLD (0.90).  Skips ambiguous matches (2+ candidates
    above threshold) with a WARNING log.  Calls ``update_photo_url`` only when
    there is exactly one match with a non-null image URL.

    Returns:
        Dict with keys:
          - ``enriched`` (int): rows successfully updated
          - ``skipped_ambiguous`` (int): rows skipped due to multiple matches
          - ``skipped_no_match`` (int): rows skipped because no candidate scored >= threshold
          - ``skipped_no_image`` (int): rows skipped because the single match has no image

    Raises:
        requests.HTTPError: if the Wikidata SPARQL request fails (no silent degradation).
    """
    db = CongressParticipantsDB()
    all_rows = db.get_all_participants()
    null_photo_rows = [r for r in all_rows if r.get("photo_url") is None]

    bindings = fetch_wikidata_photos()

    enriched = 0
    skipped_ambiguous = 0
    skipped_no_match = 0
    skipped_no_image = 0

    for row in null_photo_rows:
        key = row["normalized_name"]

        matches = [
            b
            for b in bindings
            if token_sort_ratio(normalize_member_name(b["label"]), key) / 100.0 >= WIKIDATA_FUZZY_THRESHOLD
        ]

        if len(matches) == 0:
            skipped_no_match += 1
            continue

        if len(matches) > 1:
            logger.warning(
                "Ambiguous Wikidata match for %r — %d candidates above threshold, skipping",
                key,
                len(matches),
            )
            skipped_ambiguous += 1
            continue

        match = matches[0]
        image_url = match.get("image_url")
        if not image_url:
            skipped_no_image += 1
            continue

        db.update_photo_url(key, image_url)
        enriched += 1

    logger.info(
        "enrich_missing_photos: enriched=%d skipped_ambiguous=%d skipped_no_match=%d skipped_no_image=%d",
        enriched,
        skipped_ambiguous,
        skipped_no_match,
        skipped_no_image,
    )
    return {
        "enriched": enriched,
        "skipped_ambiguous": skipped_ambiguous,
        "skipped_no_match": skipped_no_match,
        "skipped_no_image": skipped_no_image,
    }
