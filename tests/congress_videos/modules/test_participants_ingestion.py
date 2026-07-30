"""Tests for congress_videos.modules.participants_ingestion module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from congress_videos.config.constants import CONGRESO_DEPUTIES_INDEX
from congress_videos.modules.participants_ingestion import (
    fetch_active_deputies,
    normalize_member_name,
    parse_deputies,
    resolve_download_url,
)

FIXTURES_DIR = Path(__file__).parents[2] / "fixtures"


def _load_sample_deputies() -> list[dict]:
    return json.loads((FIXTURES_DIR / "sample_diputados_activos.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# normalize_member_name
# ---------------------------------------------------------------------------


class TestNormalizeMemberName:
    def test_reorders_comma_separated_name(self):
        assert normalize_member_name("Pérez García, María") == "maria perez garcia"

    def test_strips_accents(self):
        assert normalize_member_name("Núñez Fernández, José") == "jose nunez fernandez"

    def test_handles_hyphenated_compound_surname(self):
        assert normalize_member_name("Ruiz-Gallardón Jiménez, Alberto") == "alberto ruiz gallardon jimenez"

    def test_casefolds_result(self):
        assert normalize_member_name("LÓPEZ, ANA") == normalize_member_name("lópez, ana")

    def test_collapses_extra_whitespace(self):
        assert normalize_member_name("  López   Sánchez ,  Ana  ") == "ana lopez sanchez"

    def test_name_without_comma_is_unchanged_order(self):
        assert normalize_member_name("Ana López") == "ana lopez"


# ---------------------------------------------------------------------------
# resolve_download_url
# ---------------------------------------------------------------------------


class TestResolveDownloadUrl:
    def test_discovers_url_from_index_html(self, mock_requests, monkeypatch):
        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        index_html = (
            "<html><body>"
            '<a href="https://www.congreso.es/webpublica/opendata/diputados/'
            'DiputadosActivos__20260101_120000.json">Diputados Activos</a>'
            "</body></html>"
        )
        mock_requests.get.return_value = mock_requests.make_response(status_code=200, text=index_html)

        url = resolve_download_url()

        assert url == (
            "https://www.congreso.es/webpublica/opendata/diputados/"
            "DiputadosActivos__20260101_120000.json"
        )
        mock_requests.get.assert_any_call(CONGRESO_DEPUTIES_INDEX, timeout=30)

    def test_resolves_relative_href_against_index(self, mock_requests, monkeypatch):
        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        index_html = '<html><body><a href="DiputadosActivos__20260101_120000.json">link</a></body></html>'
        mock_requests.get.return_value = mock_requests.make_response(status_code=200, text=index_html)

        url = resolve_download_url()

        assert url == (
            "https://www.congreso.es/webpublica/opendata/diputados/"
            "DiputadosActivos__20260101_120000.json"
        )

    def test_env_override_skips_discovery(self, mock_requests, monkeypatch):
        monkeypatch.setenv("CONGRESO_DEPUTIES_URL", "https://example.test/override.json")

        url = resolve_download_url()

        assert url == "https://example.test/override.json"
        mock_requests.get.assert_not_called()

    def test_raises_when_no_link_found(self, mock_requests, monkeypatch):
        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200, text="<html><body>no links here</body></html>"
        )

        with pytest.raises(ValueError):
            resolve_download_url()


# ---------------------------------------------------------------------------
# fetch_active_deputies
# ---------------------------------------------------------------------------


class TestFetchActiveDeputies:
    def test_successful_download_and_parse(self, mock_requests, monkeypatch):
        monkeypatch.setenv("CONGRESO_DEPUTIES_URL", "https://example.test/deputies.json")
        sample = _load_sample_deputies()
        mock_requests.get.return_value = mock_requests.make_response(status_code=200, json_data=sample)

        raw = fetch_active_deputies()

        assert raw == sample
        assert len(raw) == 4

    def test_http_error_propagates(self, mock_requests, monkeypatch):
        monkeypatch.setenv("CONGRESO_DEPUTIES_URL", "https://example.test/deputies.json")
        error_response = mock_requests.make_response(status_code=503)

        import requests

        error_response.raise_for_status.side_effect = requests.HTTPError("503 Server Error")
        mock_requests.get.return_value = error_response

        with pytest.raises(Exception):
            fetch_active_deputies()

    def test_malformed_json_raises(self, mock_requests, monkeypatch):
        monkeypatch.setenv("CONGRESO_DEPUTIES_URL", "https://example.test/deputies.json")
        bad_response = mock_requests.make_response(status_code=200)
        bad_response.json.side_effect = ValueError("Expecting value")
        mock_requests.get.return_value = bad_response

        with pytest.raises(ValueError):
            fetch_active_deputies()


# ---------------------------------------------------------------------------
# parse_deputies
# ---------------------------------------------------------------------------


class TestParseDeputies:
    def test_parses_all_named_fields(self):
        sample = _load_sample_deputies()

        records = parse_deputies(sample)

        assert len(records) == 4
        first = records[0]
        assert first["normalized_name"] == "maria perez garcia"
        assert first["display_name"] == "Pérez García, María"
        assert first["party"] == "Partido Socialista Obrero Español"
        assert first["parliamentary_group"] == "Grupo Parlamentario Socialista"
        assert first["constituency"] == "Madrid"
        assert first["biography"].startswith("Licenciada en Derecho")
        assert first["full_membership_date"] == "2023-12-17"
        assert first["start_date"] == "2023-12-10"
        assert first["group_entry_date"] == "2023-12-12"
        assert first["photo_url"] is None

    def test_empty_payload_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_deputies([])

    def test_unparseable_date_becomes_none(self):
        entry = {
            "NOMBRE": "Test, Persona",
            "FORMACIONELECTORAL": "Grupo",
            "GRUPOPARLAMENTARIO": "Grupo",
            "CIRCUNSCRIPCION": "Madrid",
            "BIOGRAFIA": "Bio",
            "FECHACONDICIONPLENA": "not-a-date",
            "FECHAALTA": "",
        }
        records = parse_deputies([entry])
        assert records[0]["full_membership_date"] is None
        assert records[0]["start_date"] is None

    def test_skips_entry_missing_nombre(self):
        good = {"NOMBRE": "Test, Persona"}
        bad = {"FORMACIONELECTORAL": "Grupo sin nombre"}

        records = parse_deputies([bad, good])

        assert len(records) == 1
        assert records[0]["display_name"] == "Test, Persona"

    def test_deduplicates_repeated_normalized_name(self):
        entry = {"NOMBRE": "Test, Persona"}

        records = parse_deputies([entry, dict(entry)])

        assert len(records) == 1

    def test_raises_when_all_entries_missing_nombre(self):
        with pytest.raises(ValueError):
            parse_deputies([{"FORMACIONELECTORAL": "Grupo sin nombre"}])
