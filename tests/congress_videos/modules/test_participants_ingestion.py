"""Tests for congress_videos.modules.participants_ingestion module."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from congress_videos.modules.participants_ingestion import (
    fetch_active_deputies,
    normalize_member_name,
    parse_deputies,
)

FIXTURES_DIR = Path(__file__).parents[2] / "fixtures"


def _load_sample_deputies() -> list[dict]:
    return json.loads((FIXTURES_DIR / "sample_diputados_activos.json").read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# TestConstants — A-1 RED → GREEN
# ---------------------------------------------------------------------------


class TestConstants:
    def test_portlet_url_exists_and_has_expected_shape(self):
        from congress_videos.config.constants import CONGRESO_DEPUTIES_PORTLET_URL

        assert CONGRESO_DEPUTIES_PORTLET_URL.startswith("https://www.congreso.es/es/busqueda-de-diputados")
        assert "opendataExport" in CONGRESO_DEPUTIES_PORTLET_URL

    def test_browser_user_agent_is_non_empty_and_not_requests_default(self):
        from congress_videos.config.constants import CONGRESO_BROWSER_USER_AGENT

        assert CONGRESO_BROWSER_USER_AGENT
        assert not CONGRESO_BROWSER_USER_AGENT.startswith("python-requests/")

    def test_congreso_deputies_index_not_imported_in_participants_ingestion(self):
        import congress_videos.modules.participants_ingestion as mod

        assert not hasattr(mod, "CONGRESO_DEPUTIES_INDEX"), (
            "CONGRESO_DEPUTIES_INDEX must not be an attribute of participants_ingestion"
        )


# ---------------------------------------------------------------------------
# TestFixtureSchema — C-1 RED → GREEN
# ---------------------------------------------------------------------------


class TestFixtureSchema:
    def test_fixture_uses_titlecase_nombre_key(self):
        records = _load_sample_deputies()
        assert len(records) >= 4
        for r in records:
            assert "Nombre" in r, f"Expected TitleCase 'Nombre' key, got keys: {list(r.keys())}"

    def test_fixture_uses_titlecase_formacion_key(self):
        records = _load_sample_deputies()
        for r in records:
            assert "Formacion" in r, f"Expected 'Formacion' key, got: {list(r.keys())}"

    def test_fixture_uses_titlecase_grupo_parlamentario_key(self):
        records = _load_sample_deputies()
        for r in records:
            assert "GrupoParlamentario" in r, f"Expected 'GrupoParlamentario' key, got: {list(r.keys())}"

    def test_fixture_has_no_uppercase_fechaalta_grupo(self):
        records = _load_sample_deputies()
        for r in records:
            assert "FECHAALTAENGRUPOPARLAMENTARIO" not in r, (
                "Fixture must not contain FECHAALTAENGRUPOPARLAMENTARIO key"
            )


# ---------------------------------------------------------------------------
# normalize_member_name — unchanged regression guard
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
# TestFetchActiveDeputies — B-1 RED + B-2 RED → GREEN
# ---------------------------------------------------------------------------


class TestFetchActiveDeputies:
    """Tests for fetch_active_deputies() POST-based ingestion."""

    def test_portlet_post_default(self, mock_requests, monkeypatch):
        """Default path: POST to portlet URL with correct form body and browser UA."""
        from congress_videos.config.constants import (
            CONGRESO_DEPUTIES_PORTLET_URL,
            CONGRESO_BROWSER_USER_AGENT,
        )

        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        sample = _load_sample_deputies()
        mock_requests.post.return_value = mock_requests.make_response(status_code=200, json_data=sample)

        result = fetch_active_deputies()

        mock_requests.post.assert_called_once()
        call_args = mock_requests.post.call_args
        call_url = call_args[0][0] if call_args[0] else call_args[1].get("url")
        assert call_url == CONGRESO_DEPUTIES_PORTLET_URL

        data = call_args[1].get("data") or (call_args[0][1] if len(call_args[0]) > 1 else None)
        assert data is not None
        assert data["_diputadomodule_fileType"] == "json"
        assert data["_diputadomodule_fileIndex"] == 0
        assert int(data["_diputadomodule_idLegislatura"]) == 15

        headers = call_args[1].get("headers", {})
        assert headers.get("User-Agent") == CONGRESO_BROWSER_USER_AGENT

        assert result == sample

    def test_env_override_uses_get_not_post(self, mock_requests, monkeypatch):
        """Env-override path: GET to override URL; POST must NOT be called."""
        monkeypatch.setenv("CONGRESO_DEPUTIES_URL", "https://example.test/override.json")
        sample = _load_sample_deputies()
        mock_requests.get.return_value = mock_requests.make_response(status_code=200, json_data=sample)

        result = fetch_active_deputies()

        mock_requests.get.assert_called_once()
        call_url = mock_requests.get.call_args[0][0]
        assert call_url == "https://example.test/override.json"
        mock_requests.post.assert_not_called()
        assert result == sample

    def test_empty_response_raises_value_error(self, mock_requests, monkeypatch):
        """Empty JSON array from portlet raises ValueError."""
        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        empty_response = mock_requests.make_response(status_code=200)
        # Bypass the `json_data or {}` quirk: set return_value directly to empty list
        empty_response.json.return_value = []
        mock_requests.post.return_value = empty_response

        with pytest.raises(ValueError):
            fetch_active_deputies()

    def test_malformed_json_raises_value_error(self, mock_requests, monkeypatch):
        """Malformed JSON raises ValueError with portlet URL in message."""
        from congress_videos.config.constants import CONGRESO_DEPUTIES_PORTLET_URL

        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        bad_response = mock_requests.make_response(status_code=200)
        bad_response.json.side_effect = ValueError("Expecting value")
        mock_requests.post.return_value = bad_response

        with pytest.raises(ValueError, match=CONGRESO_DEPUTIES_PORTLET_URL.split("?")[0]):
            fetch_active_deputies()

    def test_http_error_propagates(self, mock_requests, monkeypatch):
        """503 response raises HTTPError (transient outage, not WAF)."""
        import requests as req_lib

        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        error_response = mock_requests.make_response(status_code=503)
        error_response.raise_for_status.side_effect = req_lib.HTTPError("503 Server Error")
        mock_requests.post.return_value = error_response

        with pytest.raises(Exception):
            fetch_active_deputies()

    def test_403_raises_clear_waf_message(self, mock_requests, monkeypatch):
        """HTTP 403 raises RuntimeError with WAF diagnostic (URL + body snippet)."""
        from congress_videos.config.constants import CONGRESO_DEPUTIES_PORTLET_URL

        monkeypatch.delenv("CONGRESO_DEPUTIES_URL", raising=False)
        waf_response = mock_requests.make_response(
            status_code=403, text="Forbidden WAF body — access denied by security policy"
        )
        mock_requests.post.return_value = waf_response

        with pytest.raises(RuntimeError) as exc_info:
            fetch_active_deputies()

        message = str(exc_info.value)
        assert "403" in message
        assert "congreso.es" in message or CONGRESO_DEPUTIES_PORTLET_URL.split("?")[0] in message
        assert "WAF" in message or "endpoint change" in message
        # S-ERR-01(c): the response body snippet must be surfaced for diagnosis
        assert "access denied by security policy" in message
        # Confirm raise_for_status was never the trigger (RuntimeError not HTTPError)
        assert exc_info.type is RuntimeError


# ---------------------------------------------------------------------------
# TestParseDeputies — D-1 RED → GREEN (TitleCase keys)
# ---------------------------------------------------------------------------


class TestParseDeputies:
    def test_parses_all_named_fields(self):
        """TitleCase fixture drives full field mapping including group_entry_date=None."""
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
        assert first["group_entry_date"] is None  # field absent from opendataExport
        assert first["photo_url"] is None

    def test_group_entry_date_always_none(self):
        """group_entry_date is always None regardless of any key name in input."""
        entry = {
            "Nombre": "Test, Persona",
            "FECHAALTAENGRUPOPARLAMENTARIO": "12/12/2023",  # old key, must be ignored
            "SomeOtherGroupKey": "2024-01-01",
        }

        records = parse_deputies([entry])

        assert records[0]["group_entry_date"] is None

    def test_empty_payload_raises_value_error(self):
        with pytest.raises(ValueError):
            parse_deputies([])

    def test_unparseable_date_becomes_none(self):
        entry = {
            "Nombre": "Test, Persona",
            "FechaCondicionPlena": "not-a-date",
            "FechaAlta": "",
        }
        records = parse_deputies([entry])
        assert records[0]["full_membership_date"] is None
        assert records[0]["start_date"] is None

    def test_skips_entry_missing_nombre(self):
        good = {"Nombre": "Test, Persona"}
        bad = {"Formacion": "Grupo sin nombre"}

        records = parse_deputies([bad, good])

        assert len(records) == 1
        assert records[0]["display_name"] == "Test, Persona"

    def test_deduplicates_repeated_normalized_name(self):
        entry = {"Nombre": "Test, Persona"}

        records = parse_deputies([entry, dict(entry)])

        assert len(records) == 1

    def test_raises_when_all_entries_missing_nombre(self):
        with pytest.raises(ValueError):
            parse_deputies([{"Formacion": "Grupo sin nombre"}])

    def test_slug_is_normalized_name_hyphenated(self):
        """slug == normalized_name.replace(' ', '-') for the first record."""
        records = parse_deputies(_load_sample_deputies())
        first = records[0]
        assert first["slug"] == first["normalized_name"].replace(" ", "-")

    def test_every_record_has_slug(self):
        """Every returned record has a non-empty slug with no spaces."""
        records = parse_deputies(_load_sample_deputies())
        assert all("slug" in r and " " not in r["slug"] for r in records)

    def test_slug_key_present_in_participant_record(self):
        """ParticipantRecord TypedDict has a 'slug' key (non-optional str)."""
        import typing
        from congress_videos.modules.participants_ingestion import ParticipantRecord

        hints = typing.get_type_hints(ParticipantRecord)
        assert "slug" in hints
        assert hints["slug"] is str
