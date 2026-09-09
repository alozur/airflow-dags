"""Curated politician display-name catalogue and never-raising resolver.

Maps a resolved ``participant_slug`` to a preferred public display name (for
example, a party leader's bare surname). The catalogue is curated and
versioned, never derived from a live database query, and every structural
defect fails loudly at load time — see :class:`DisplayNameCatalogLoader`.

Public API
----------
canonical_display_name(slug) -> str | None
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from urllib.parse import urlparse

from congress_videos.modules.institutional_role_resolver import CatalogValidationError

logger = logging.getLogger(__name__)

_REQUIRED_PROVENANCE_FIELDS = ("publisher", "reference_url", "evidence_note", "reviewed_on")


@dataclass(frozen=True)
class DisplayName:
    participant_slug: str
    display_name: str
    full_name: str | None
    ambiguous: bool


@dataclass(frozen=True)
class DisplayNameCatalog:
    version: int
    entries: tuple[DisplayName, ...]

    def canonical_name(self, slug: str | None) -> str | None:
        """Return the curated display name for *slug*, or ``None``.

        ``None`` covers a missing/blank slug, an unmapped slug, and a slug
        marked ``ambiguous`` — an ambiguous entry never resolves.
        """
        if not slug:
            return None
        for entry in self.entries:
            if entry.participant_slug == slug and not entry.ambiguous:
                return entry.display_name
        return None


class DisplayNameCatalogLoader:
    """Load and structurally validate one local UTF-8 V1 JSON catalogue."""

    def __init__(self, path: Path | str) -> None:
        self.path = Path(path)

    def load(self) -> DisplayNameCatalog:
        document = self._read_document()
        if not isinstance(document, dict):
            raise CatalogValidationError("top_level")
        if document.get("catalog_version") != 1:
            raise CatalogValidationError("catalog_version")

        raw_entries = document.get("entries")
        if not isinstance(raw_entries, list):
            raise CatalogValidationError("entries")

        entries = tuple(self._parse_entry(item) for item in raw_entries)
        _check_no_duplicate_slugs(entries)
        _check_no_colliding_display_names(entries)
        return DisplayNameCatalog(1, entries)

    def _read_document(self) -> object:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise CatalogValidationError("catalog_load_failed") from error

    def _parse_entry(self, raw: object) -> DisplayName:
        if not isinstance(raw, dict):
            raise CatalogValidationError("invalid_entry")

        slug = raw.get("participant_slug")
        display_name = raw.get("display_name")
        full_name = raw.get("full_name")
        ambiguous = raw.get("ambiguous", False)

        _require_non_blank_str(slug, "missing_participant_slug")
        _require_non_blank_str(display_name, "missing_display_name")
        if full_name is not None:
            _require_non_blank_str(full_name, "invalid_full_name")
        if not isinstance(ambiguous, bool):
            raise CatalogValidationError("invalid_ambiguous")
        _require_non_blank_str(raw.get("selection_note"), "blank_selection_note")

        provenance_error = _provenance_error(raw.get("provenance"))
        if provenance_error:
            raise CatalogValidationError(provenance_error)

        if full_name is not None and not _is_short_form_of(display_name, full_name):
            raise CatalogValidationError("display_name_not_subsequence")

        return DisplayName(slug, display_name, full_name, ambiguous)


def _require_non_blank_str(value: object, error: str) -> None:
    if not isinstance(value, str) or not value:
        raise CatalogValidationError(error)


def _provenance_error(raw: object) -> str | None:
    if not isinstance(raw, dict):
        return "missing_provenance"
    if any(not isinstance(raw.get(field), str) or not raw[field] for field in _REQUIRED_PROVENANCE_FIELDS):
        return "invalid_provenance"
    parsed_url = urlparse(raw["reference_url"])
    if parsed_url.scheme not in {"http", "https"} or not parsed_url.netloc:
        return "invalid_provenance"
    return None if _parse_iso_date(raw["reviewed_on"]) else "invalid_provenance"


def _parse_iso_date(value: object) -> date | None:
    if not isinstance(value, str) or len(value) != 10:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _check_no_duplicate_slugs(entries: tuple[DisplayName, ...]) -> None:
    slugs = [entry.participant_slug for entry in entries]
    if len(set(slugs)) != len(slugs):
        raise CatalogValidationError("duplicate_participant_slug")


def _check_no_colliding_display_names(entries: tuple[DisplayName, ...]) -> None:
    resolvable_names = [_normalize_display_name(entry.display_name) for entry in entries if not entry.ambiguous]
    if len(set(resolvable_names)) != len(resolvable_names):
        raise CatalogValidationError("colliding_display_name")


def _is_short_form_of(display_name: str, full_name: str) -> bool:
    """Return True when every normalized token of *display_name* appears, in
    order, among the normalized tokens of *full_name* (a token subsequence).
    """
    short_tokens = _normalize_display_name(display_name).split()
    long_tokens = iter(_normalize_display_name(full_name).split())
    return all(token in long_tokens for token in short_tokens)


def _normalize_display_name(name: str) -> str:
    """Return an accent- and case-insensitive comparison key for *name*.

    Private and deliberately not shared with the role catalogue's
    ``normalize_role_label`` — person-name normalization (e.g. nobiliary
    particles) must be free to diverge from role-label normalization.
    """
    decomposed = unicodedata.normalize("NFKD", name.strip())
    without_accents = "".join(character for character in decomposed if not unicodedata.combining(character))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", without_accents.casefold())).strip()


# Bundled catalogue, loaded lazily and cached per process. A failure to load
# is cached too, so a broken catalogue logs exactly once per process instead
# of retrying (and re-logging) on every call.
_CATALOG_PATH = Path(__file__).resolve().parents[1] / "catalogs" / "politician_display_names.v1.json"
_catalog: DisplayNameCatalog | None = None
_catalog_load_failed = False


def _get_catalog() -> DisplayNameCatalog | None:
    global _catalog, _catalog_load_failed
    if _catalog is not None:
        return _catalog
    if _catalog_load_failed:
        return None
    try:
        _catalog = DisplayNameCatalogLoader(_CATALOG_PATH).load()
    except CatalogValidationError:
        logger.error("Failed to load politician display-name catalogue at %s", _CATALOG_PATH, exc_info=True)
        _catalog_load_failed = True
        return None
    return _catalog


def canonical_display_name(slug: str | None) -> str | None:
    """Return the curated display name for *slug*, or ``None``.

    Never raises. Returns ``None`` for a missing/blank/unmapped/ambiguous
    slug, and for a catalogue that failed to load — callers must fall back
    to their existing full-name behaviour in every ``None`` case.
    """
    if not slug:
        return None
    catalog = _get_catalog()
    if catalog is None:
        return None
    return catalog.canonical_name(slug)
