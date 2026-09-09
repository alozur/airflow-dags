"""V1 politician display-name catalogue contract tests."""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

import congress_videos.modules.politician_display_names as politician_display_names
from congress_videos.modules.institutional_role_resolver import CatalogValidationError
from congress_videos.modules.politician_display_names import (
    DisplayNameCatalogLoader,
    canonical_display_name,
)

CATALOG_PATH = Path(__file__).parents[2] / "congress_videos" / "catalogs" / "politician_display_names.v1.json"


def valid_catalog() -> dict:
    return {
        "catalog_version": 1,
        "entries": [
            {
                "participant_slug": "pedro-sanchez-perez-castejon",
                "display_name": "Sánchez",
                "full_name": "Pedro Sánchez",
                "ambiguous": False,
                "selection_note": "Party leader; surname nationally unmistakable.",
                "provenance": {
                    "publisher": "Congress of Deputies",
                    "reference_url": "https://www.congreso.es/busqueda-de-diputados",
                    "evidence_note": "Slug cross-checked against the active-deputies register.",
                    "reviewed_on": "2026-09-09",
                },
            }
        ],
    }


def write_catalog(tmp_path: Path, document: dict) -> Path:
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _reset_singleton():
    """Isolate the module-level lazy-singleton cache between tests."""
    politician_display_names._catalog = None
    politician_display_names._catalog_load_failed = False
    yield
    politician_display_names._catalog = None
    politician_display_names._catalog_load_failed = False


# ---------------------------------------------------------------------------
# Loading and structural validation
# ---------------------------------------------------------------------------


def test_bundled_catalog_has_the_v1_contract():
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()

    assert catalog.version == 1
    assert catalog.entries
    assert all(entry.participant_slug for entry in catalog.entries)
    assert all(entry.display_name for entry in catalog.entries)


def test_one_entry_catalog_loads(tmp_path):
    catalog = DisplayNameCatalogLoader(write_catalog(tmp_path, valid_catalog())).load()

    assert catalog.version == 1
    assert len(catalog.entries) == 1
    entry = catalog.entries[0]
    assert entry.participant_slug == "pedro-sanchez-perez-castejon"
    assert entry.display_name == "Sánchez"
    assert entry.full_name == "Pedro Sánchez"
    assert entry.ambiguous is False


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda doc: doc.update(catalog_version=2), "catalog_version"),
        (lambda doc: doc.update(entries={}), "entries"),
        (lambda doc: doc["entries"].__setitem__(0, "not-a-dict"), "invalid_entry"),
    ],
)
def test_loader_rejects_invalid_v1_document_shape(tmp_path, mutate, message):
    document = valid_catalog()
    mutate(document)

    with pytest.raises(CatalogValidationError, match=message):
        DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()


def test_loader_rejects_non_dict_document(tmp_path):
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(["not", "a", "dict"]), encoding="utf-8")

    with pytest.raises(CatalogValidationError, match="top_level"):
        DisplayNameCatalogLoader(path).load()


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda entry: entry.update(participant_slug=""), "missing_participant_slug"),
        (lambda entry: entry.pop("participant_slug"), "missing_participant_slug"),
        (lambda entry: entry.update(display_name=""), "missing_display_name"),
        (lambda entry: entry.pop("display_name"), "missing_display_name"),
        (lambda entry: entry.update(full_name=""), "invalid_full_name"),
        (lambda entry: entry.update(ambiguous="false"), "invalid_ambiguous"),
        (lambda entry: entry.update(selection_note=""), "blank_selection_note"),
        (lambda entry: entry.pop("selection_note"), "blank_selection_note"),
        (lambda entry: entry.pop("provenance"), "missing_provenance"),
        (
            lambda entry: entry["provenance"].update(reference_url="ftp://example.invalid/evidence"),
            "invalid_provenance",
        ),
        (lambda entry: entry["provenance"].pop("publisher"), "invalid_provenance"),
        (lambda entry: entry["provenance"].update(reviewed_on="09/09/2026"), "invalid_provenance"),
        (lambda entry: entry.update(display_name="García"), "display_name_not_subsequence"),
        (lambda entry: entry.update(full_name="Pedro Sanche"), "display_name_not_subsequence"),
    ],
)
def test_loader_rejects_invalid_entry_fields(tmp_path, mutate, message):
    document = valid_catalog()
    mutate(document["entries"][0])

    with pytest.raises(CatalogValidationError, match=message):
        DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()


def test_loader_allows_entry_without_full_name(tmp_path):
    document = valid_catalog()
    document["entries"][0].pop("full_name")

    catalog = DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()

    assert catalog.entries[0].full_name is None


def test_loader_rejects_duplicate_participant_slug_even_when_ambiguous(tmp_path):
    document = valid_catalog()
    duplicate = document["entries"][0].copy()
    duplicate["ambiguous"] = True
    document["entries"].append(duplicate)

    with pytest.raises(CatalogValidationError, match="duplicate_participant_slug"):
        DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()


def test_loader_rejects_colliding_display_name_over_resolvable_set(tmp_path):
    document = valid_catalog()
    colliding = document["entries"][0].copy()
    colliding["participant_slug"] = "some-other-sanchez"
    colliding["full_name"] = "Alberto Sánchez"
    document["entries"].append(colliding)

    with pytest.raises(CatalogValidationError, match="colliding_display_name"):
        DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()


def test_loader_allows_ambiguous_entry_to_skip_collision(tmp_path):
    document = valid_catalog()
    document["entries"][0]["ambiguous"] = True
    colliding = document["entries"][0].copy()
    colliding["participant_slug"] = "some-other-sanchez"
    colliding["full_name"] = "Alberto Sánchez"
    colliding["ambiguous"] = False
    document["entries"].append(colliding)

    catalog = DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()

    assert catalog.canonical_name("pedro-sanchez-perez-castejon") is None
    assert catalog.canonical_name("some-other-sanchez") == "Sánchez"


# ---------------------------------------------------------------------------
# canonical_display_name — never-raising public resolution
# ---------------------------------------------------------------------------


def test_canonical_display_name_resolves_mapped_slug(monkeypatch):
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)

    assert canonical_display_name("pedro-sanchez-perez-castejon") == "Sánchez"


def test_canonical_display_name_returns_none_for_unmapped_slug(monkeypatch):
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)

    assert canonical_display_name("unmapped-person-slug") is None


@pytest.mark.parametrize("slug", [None, "", "   "])
def test_canonical_display_name_returns_none_for_missing_slug(monkeypatch, slug):
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)

    assert canonical_display_name(slug) is None


def test_canonical_display_name_returns_none_for_ambiguous_entry(tmp_path, monkeypatch):
    document = valid_catalog()
    document["entries"][0]["ambiguous"] = True
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", write_catalog(tmp_path, document))

    assert canonical_display_name("pedro-sanchez-perez-castejon") is None


def test_canonical_display_name_resolves_accented_slug(tmp_path, monkeypatch):
    document = valid_catalog()
    document["entries"][0]["participant_slug"] = "gabriel-rufián-romero"
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", write_catalog(tmp_path, document))

    assert canonical_display_name("gabriel-rufián-romero") == "Sánchez"


def test_canonical_display_name_never_raises_on_missing_catalog(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", tmp_path / "does-not-exist.json")

    with caplog.at_level(logging.ERROR):
        assert canonical_display_name("pedro-sanchez-perez-castejon") is None
        assert canonical_display_name("anyone-else") is None

    assert len(caplog.records) == 1


def test_canonical_display_name_never_raises_on_corrupt_catalog(tmp_path, monkeypatch, caplog):
    document = valid_catalog()
    document["catalog_version"] = 2
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", write_catalog(tmp_path, document))

    with caplog.at_level(logging.ERROR):
        assert canonical_display_name("pedro-sanchez-perez-castejon") is None

    assert len(caplog.records) == 1


# ---------------------------------------------------------------------------
# Bundled roster — the initial 11-participant catalogue (>=2 appearances
# across video_chapters.resolved_participant_slug and
# speaker_turn_videos.resolved_participant_slug)
# ---------------------------------------------------------------------------

BUNDLED_ROSTER = {
    "pedro-sanchez-perez-castejon": "Sánchez",
    "miguel-tellado-filgueira": "Tellado",
    "isabel-rodriguez-garcia": "Isabel Rodríguez",
    "concepcion-gamarra-ruiz-clavijo": "Gamarra",
    "maria-dolores-corujo-berriel": "Corujo",
    "pedro-munoz-abrines": "Muñoz",
    "javier-rodriguez-palacios": "Javier Rodríguez",
    "agueda-mico-mico": "Micó",
    "carlos-hernandez-quero": "Hernández",
    "lidia-guinart-moreno": "Guinart",
    "santiago-abascal-conde": "Abascal",
}


def test_bundled_catalog_has_exactly_the_11_entry_roster():
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()

    assert len(catalog.entries) == 11
    assert {entry.participant_slug for entry in catalog.entries} == set(BUNDLED_ROSTER)


def test_bundled_catalog_has_no_ambiguous_entries():
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()

    assert all(entry.ambiguous is False for entry in catalog.entries)


@pytest.mark.parametrize(("slug", "expected_display_name"), sorted(BUNDLED_ROSTER.items()))
def test_bundled_catalog_resolves_each_curated_slug(monkeypatch, slug, expected_display_name):
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)

    assert canonical_display_name(slug) == expected_display_name


@pytest.mark.parametrize(("slug", "expected_display_name"), sorted(BUNDLED_ROSTER.items()))
def test_bundled_catalog_display_name_is_a_subsequence_of_full_name(slug, expected_display_name):
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()
    entry = next(item for item in catalog.entries if item.participant_slug == slug)

    assert entry.display_name == expected_display_name
    assert entry.full_name is not None
    # The loader already enforces this invariant at load time (D1/D4); this
    # assertion pins the specific curated pair so a future edit that keeps
    # the entry loadable but drifts the pairing is still caught here.
    normalized_short = politician_display_names._normalize_display_name(entry.display_name).split()
    normalized_long = politician_display_names._normalize_display_name(entry.full_name).split()
    remaining = iter(normalized_long)
    assert all(token in remaining for token in normalized_short)


def test_bundled_catalog_disambiguates_the_rodriguez_collision():
    """isabel-rodriguez-garcia and javier-rodriguez-palacios would both
    collide on the bare surname "Rodríguez"; the roster must carry distinct,
    first-name-qualified display names for both.
    """
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()

    isabel = catalog.canonical_name("isabel-rodriguez-garcia")
    javier = catalog.canonical_name("javier-rodriguez-palacios")

    assert isabel == "Isabel Rodríguez"
    assert javier == "Javier Rodríguez"
    assert isabel != javier


def test_loader_rejects_the_bare_surname_rodriguez_collision(tmp_path):
    """Proves the loader would reject the two Rodríguez entries if authored
    with the colliding bare surname instead of the disambiguated form
    actually shipped in the bundled catalogue.
    """
    catalog_path = CATALOG_PATH
    document = json.loads(catalog_path.read_text(encoding="utf-8"))
    for entry in document["entries"]:
        if entry["participant_slug"] in ("isabel-rodriguez-garcia", "javier-rodriguez-palacios"):
            entry["display_name"] = "Rodríguez"

    with pytest.raises(CatalogValidationError, match="colliding_display_name"):
        DisplayNameCatalogLoader(write_catalog(tmp_path, document)).load()


def test_canonical_display_name_falls_back_for_out_of_catalog_rodriguez(monkeypatch):
    """A third real Rodríguez, jose-antonio-rodriguez-salas, has only 1
    appearance and is deliberately outside the catalogue: it must fall back
    to existing full-name behaviour, not collide with either mapped entry.
    """
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)

    assert canonical_display_name("jose-antonio-rodriguez-salas") is None


def test_bundled_catalog_accented_entry_round_trips(monkeypatch):
    """agueda-mico-mico carries accented characters in both slug-adjacent
    full_name ("Àgueda") and display_name ("Micó"); confirms JSON I/O does
    not corrupt them.
    """
    monkeypatch.setattr(politician_display_names, "_CATALOG_PATH", CATALOG_PATH)
    catalog = DisplayNameCatalogLoader(CATALOG_PATH).load()
    entry = next(item for item in catalog.entries if item.participant_slug == "agueda-mico-mico")

    assert entry.full_name == "Àgueda Micó Micó"
    assert entry.display_name == "Micó"
    assert canonical_display_name("agueda-mico-mico") == "Micó"
