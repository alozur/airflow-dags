"""V1 institutional-role catalog contract tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from congress_videos.modules.institutional_role_resolver import (
    CatalogLoader,
    CatalogValidationError,
)


CATALOG_PATH = (
    Path(__file__).parents[2]
    / "congress_videos"
    / "catalogs"
    / "institutional_roles.v1.json"
)


def valid_catalog() -> dict:
    return {
        "catalog_version": 1,
        "roles": [
            {
                "key": "presidencia del congreso",
                "scope": "presidency_mesa",
                "aliases": ["presidenta del congreso"],
            }
        ],
        "assignments": [
            {
                "id": "mesa-presidency-2023",
                "role": "presidencia del congreso",
                "participant_slug": "francina-armengol-socias",
                "validity": {"from": "2023-08-17", "to": "open"},
                "provenance": {
                    "publisher": "Congress of Deputies",
                    "reference_url": "https://www.congreso.es/mesa",
                    "evidence_note": "Mesa composition reviewed against the official Congress record.",
                    "reviewed_on": "2023-08-17",
                },
            }
        ],
    }


def write_catalog(tmp_path: Path, document: dict) -> Path:
    path = tmp_path / "catalog.json"
    path.write_text(json.dumps(document), encoding="utf-8")
    return path


def test_bundled_catalog_has_the_v1_contract():
    catalog = CatalogLoader(CATALOG_PATH).load()

    assert catalog.version == 1
    assert catalog.roles
    assert catalog.assignments
    assert all(
        role.scope in {"ministerial", "presidency_mesa", "parliamentary_group"}
        for role in catalog.roles
    )
    assert all(assignment.is_valid for assignment in catalog.assignments)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda doc: doc.update(catalog_version=2), "catalog_version"),
        (lambda doc: doc.update(roles={}), "roles"),
        (lambda doc: doc["roles"][0].update(key="Presidencia del Congreso"), "normalized"),
        (lambda doc: doc["roles"][0].update(aliases=["presidencia del congreso"]), "collides"),
        (lambda doc: doc["roles"][0].update(scope="state_secretary"), "scope"),
    ],
)
def test_loader_rejects_invalid_v1_document_shape(tmp_path, mutate, message):
    document = valid_catalog()
    mutate(document)

    with pytest.raises(CatalogValidationError, match=message):
        CatalogLoader(write_catalog(tmp_path, document)).load()


@pytest.mark.parametrize(
    ("mutate", "reason"),
    [
        (lambda assignment: assignment.pop("provenance"), "missing_provenance"),
        (lambda assignment: assignment["validity"].update(**{"from": "17/08/2023"}), "invalid_from"),
        (lambda assignment: assignment["validity"].update(to=""), "invalid_to"),
        (lambda assignment: assignment["validity"].update(to="2023-08-16"), "invalid_interval"),
        (lambda assignment: assignment.update(role="undeclared role"), "undeclared_role"),
        (lambda assignment: assignment.update(participant_slug=""), "missing_participant_slug"),
    ],
)
def test_loader_keeps_invalid_assignments_non_resolvable(tmp_path, mutate, reason):
    document = valid_catalog()
    mutate(document["assignments"][0])

    catalog = CatalogLoader(write_catalog(tmp_path, document)).load()

    assert catalog.valid_assignments == ()
    assert catalog.assignments[0].is_valid is False
    assert catalog.assignments[0].diagnostic == reason


def test_loader_marks_duplicate_assignment_ids_non_resolvable(tmp_path):
    document = valid_catalog()
    document["assignments"].append(document["assignments"][0].copy())

    catalog = CatalogLoader(write_catalog(tmp_path, document)).load()

    assert catalog.valid_assignments == ()
    assert [assignment.diagnostic for assignment in catalog.assignments] == [
        "duplicate_assignment_id",
        "duplicate_assignment_id",
    ]


def test_loader_accepts_parliamentary_group_scope(tmp_path):
    document = valid_catalog()
    document["roles"].append(
        {
            "key": "portavoz de esquerra republicana",
            "scope": "parliamentary_group",
            "aliases": ["portavoz del grupo parlamentario republicano"],
        }
    )
    document["assignments"].append(
        {
            "id": "erc-spokesperson-2023-gabriel-rufian",
            "role": "portavoz de esquerra republicana",
            "participant_slug": "gabriel-rufian-romero",
            "validity": {"from": "2023-08-17", "to": "open"},
            "provenance": {
                "publisher": "Congress of Deputies",
                "reference_url": "https://www.congreso.es/grupos",
                "evidence_note": "Parliamentary group spokesperson reviewed against the official Congress record.",
                "reviewed_on": "2026-08-12",
            },
        }
    )

    catalog = CatalogLoader(write_catalog(tmp_path, document)).load()

    role = next(item for item in catalog.roles if item.key == "portavoz de esquerra republicana")
    assert role.scope == "parliamentary_group"
    assert all(assignment.is_valid for assignment in catalog.assignments)


def test_bundled_catalog_resolves_expected_current_holders():
    catalog = CatalogLoader(CATALOG_PATH).load()

    open_holders = {
        assignment.role: assignment.participant_slug
        for assignment in catalog.valid_assignments
        if assignment.is_open_ended
    }

    assert open_holders["presidencia del gobierno"] == "pedro-sanchez-perez-castejon"
    assert open_holders["ministerio de transportes y movilidad sostenible"] == "oscar-puente-santiago"
    assert open_holders["ministerio de trabajo y economia social"] == "yolanda-diaz-perez"
    assert open_holders["lider de la oposicion"] == "alberto-nunez-feijoo"
    assert open_holders["presidencia de vox"] == "santiago-abascal-conde"
    assert open_holders["portavoz de esquerra republicana"] == "gabriel-rufian-romero"
    assert open_holders["portavoz de junts per catalunya"] == "miriam-nogueras-i-camero"
    assert open_holders["portavoz de euskal herria bildu"] == "mertxe-aizpurua-arzallus"
    assert open_holders["secretaria general de podemos"] == "ione-belarra-urteaga"


def test_loader_requires_complete_nondereferenced_provenance(tmp_path):
    document = valid_catalog()
    document["assignments"][0]["provenance"]["reference_url"] = "ftp://example.invalid/evidence"

    catalog = CatalogLoader(write_catalog(tmp_path, document)).load()

    assert catalog.assignments[0].is_valid is False
    assert catalog.assignments[0].diagnostic == "invalid_provenance"
