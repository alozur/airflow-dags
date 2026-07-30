"""
Wikidata photo enrichment for the congress_participants table.

Queries the Wikidata SPARQL endpoint for Spanish Congress of Deputies members,
fuzzy-joins SPARQL labels to normalized_name rows, and back-fills photo_url
for rows where it is currently NULL.
"""

from __future__ import annotations

import logging

import requests
from rapidfuzz.fuzz import token_sort_ratio

from congress_videos.config.constants import (
    WIKIDATA_FUZZY_THRESHOLD,
    WIKIDATA_SPARQL_URL,
    WIKIDATA_TIMEOUT,
    WIKIDATA_USER_AGENT,
)
from congress_videos.modules.participants_db import CongressParticipantsDB
from congress_videos.modules.participants_ingestion import normalize_member_name

logger = logging.getLogger(__name__)

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
            b for b in bindings
            if token_sort_ratio(normalize_member_name(b["label"]), key) / 100.0
            >= WIKIDATA_FUZZY_THRESHOLD
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
        "enrich_missing_photos: enriched=%d skipped_ambiguous=%d "
        "skipped_no_match=%d skipped_no_image=%d",
        enriched, skipped_ambiguous, skipped_no_match, skipped_no_image,
    )
    return {
        "enriched": enriched,
        "skipped_ambiguous": skipped_ambiguous,
        "skipped_no_match": skipped_no_match,
        "skipped_no_image": skipped_no_image,
    }
