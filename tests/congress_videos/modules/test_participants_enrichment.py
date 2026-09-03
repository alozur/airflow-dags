"""Tests for participants_enrichment.py — Slice 2 (T-11 through T-14) + congreso photo fallback."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
import requests as req_lib

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


def _load_wikidata_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "sample_wikidata_sparql.json").read_text(encoding="utf-8"))


def _load_search_diputados_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "sample_search_diputados.json").read_text(encoding="utf-8"))


@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    """Provide minimal DB env vars so PostgresConnection.__init__ does not raise."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


# ===========================================================================
# T-11 RED: fetch_wikidata_photos
# ===========================================================================


class TestFetchWikidataPhotos:
    """Tests for fetch_wikidata_photos() — RED phase."""

    def test_parses_fixture_returns_list_of_dicts(self, mock_requests):
        """Fixture response → list of dicts with 'label' and 'image_url' keys."""
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        fixture_data = _load_wikidata_fixture()
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200,
            json_data=fixture_data,
        )

        result = fetch_wikidata_photos()

        assert isinstance(result, list)
        assert len(result) == 4  # fixture has 4 bindings
        for item in result:
            assert "label" in item
            assert "image_url" in item

    def test_parses_fixture_extracts_labels_correctly(self, mock_requests):
        """Each returned dict has the correct label from the fixture."""
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        fixture_data = _load_wikidata_fixture()
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200,
            json_data=fixture_data,
        )

        result = fetch_wikidata_photos()

        labels = [r["label"] for r in result]
        assert "Ana López Sánchez" in labels
        assert "José Núñez Fernández" in labels

    def test_parses_fixture_image_url_is_none_when_absent(self, mock_requests):
        """Binding without 'image' key → image_url is None."""
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        fixture_data = _load_wikidata_fixture()
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200,
            json_data=fixture_data,
        )

        result = fetch_wikidata_photos()

        # Third binding in fixture has no image key (Q1000003, "José Núñez Fernández")
        no_image_entries = [r for r in result if r["image_url"] is None]
        assert len(no_image_entries) >= 1

    def test_user_agent_header_set_from_constant(self, mock_requests):
        """requests.get is called with User-Agent header from WIKIDATA_USER_AGENT constant."""
        from congress_videos.config.constants import WIKIDATA_USER_AGENT
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        fixture_data = _load_wikidata_fixture()
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200,
            json_data=fixture_data,
        )

        fetch_wikidata_photos()

        assert mock_requests.get.called
        _, call_kwargs = mock_requests.get.call_args
        headers = call_kwargs.get("headers", {})
        assert headers.get("User-Agent") == WIKIDATA_USER_AGENT

    def test_http_503_raises_exception(self, mock_requests):
        """HTTP 503 → raise_for_status raises, exception propagates to caller."""
        import requests as req_lib

        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        error_response = mock_requests.make_response(status_code=503)
        error_response.raise_for_status.side_effect = req_lib.HTTPError("503 Server Error")
        mock_requests.get.return_value = error_response

        with pytest.raises(req_lib.HTTPError):
            fetch_wikidata_photos()

    def test_empty_sparql_result_returns_empty_list(self, mock_requests):
        """Empty SPARQL bindings → returns empty list, no exception."""
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos

        empty_data = {"head": {"vars": ["person", "personLabel", "image"]}, "results": {"bindings": []}}
        mock_requests.get.return_value = mock_requests.make_response(
            status_code=200,
            json_data=empty_data,
        )

        result = fetch_wikidata_photos()

        assert result == []


# ===========================================================================
# T-13 RED: enrich_missing_photos
# ===========================================================================


def _make_db_row(**overrides) -> dict:
    """Build a minimal DB participant row dict.

    normalized_name uses the actual normalize_member_name format:
    lowercase, accent-stripped, space-separated.
    """
    base = {
        "normalized_name": "garcia ana",
        "display_name": "García, Ana",
        "party": "PartyA",
        "parliamentary_group": "GroupA",
        "constituency": "Madrid",
        "biography": "Bio text.",
        "full_membership_date": "2020-01-15",
        "start_date": "2020-01-15",
        "group_entry_date": "2020-02-01",
        "photo_url": None,
    }
    base.update(overrides)
    return base


class TestEnrichMissingPhotos:
    """Tests for enrich_missing_photos() — RED phase."""

    def test_successful_single_match_calls_update_photo_url(self, mock_psycopg2_connection, monkeypatch):
        """Single match >= 0.90 → update_photo_url called with correct args.

        DB row normalized_name = 'ana lopez sanchez' (real normalize format).
        Wikidata label 'Ana López Sánchez' normalizes to same string -> score 1.0.
        """
        from congress_videos.modules import participants_enrichment as mod

        db_row = _make_db_row(
            normalized_name="ana lopez sanchez",
            photo_url=None,
        )

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [db_row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        bindings = [
            {"label": "Ana López Sánchez", "image_url": "http://example.com/photo.jpg"},
        ]
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: bindings)

        result = mod.enrich_missing_photos()

        mock_db.update_photo_url.assert_called_once_with("ana lopez sanchez", "http://example.com/photo.jpg")
        assert result["enriched"] == 1

    def test_ambiguous_match_warns_and_skips_update(self, mock_psycopg2_connection, monkeypatch, caplog):
        """2+ matches above threshold → WARNING logged, update_photo_url NOT called.

        Two Wikidata labels that both normalize to the same string as the DB key
        → token_sort_ratio=1.0 for both → ambiguous.
        """
        from congress_videos.modules import participants_enrichment as mod

        db_row = _make_db_row(
            normalized_name="jose nunez fernandez",
            photo_url=None,
        )

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [db_row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        # Both normalize to "jose nunez fernandez" -> identical match -> score 1.0 each
        bindings = [
            {"label": "José Núñez Fernández", "image_url": "http://example.com/a.jpg"},
            {"label": "José Núñez Fernandez", "image_url": "http://example.com/b.jpg"},
        ]
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: bindings)

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.participants_enrichment"):
            result = mod.enrich_missing_photos()

        mock_db.update_photo_url.assert_not_called()
        assert result["skipped_ambiguous"] == 1
        assert any("jose nunez fernandez" in r.message for r in caplog.records)

    def test_row_with_existing_photo_skipped(self, mock_psycopg2_connection, monkeypatch):
        """Row with non-null photo_url is excluded from matching; update NOT called."""
        from congress_videos.modules import participants_enrichment as mod

        db_row = _make_db_row(
            normalized_name="ana lopez sanchez",
            photo_url="http://existing.example.com/photo.jpg",
        )

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [db_row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        bindings = [
            {"label": "Ana López Sánchez", "image_url": "http://example.com/new.jpg"},
        ]
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: bindings)

        result = mod.enrich_missing_photos()

        mock_db.update_photo_url.assert_not_called()

    def test_no_candidates_above_threshold_no_updates(self, mock_psycopg2_connection, monkeypatch):
        """No bindings score >= 0.90 → no updates, no warning, no exception."""
        from congress_videos.modules import participants_enrichment as mod

        db_row = _make_db_row(normalized_name="completely different name", photo_url=None)

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [db_row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        bindings = [
            {"label": "Totally Unrelated Person", "image_url": "http://example.com/x.jpg"},
        ]
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: bindings)

        result = mod.enrich_missing_photos()

        mock_db.update_photo_url.assert_not_called()
        assert result["skipped_no_match"] == 1
        assert result["enriched"] == 0

    def test_match_with_no_image_skips_update(self, mock_psycopg2_connection, monkeypatch):
        """Single match above threshold but no image_url → update NOT called, skipped_no_image incremented."""
        from congress_videos.modules import participants_enrichment as mod

        db_row = _make_db_row(
            normalized_name="ana lopez sanchez",
            photo_url=None,
        )

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [db_row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        bindings = [
            {"label": "Ana López Sánchez", "image_url": None},
        ]
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: bindings)

        result = mod.enrich_missing_photos()

        mock_db.update_photo_url.assert_not_called()
        assert result["skipped_no_image"] == 1

    def test_return_value_has_required_keys(self, mock_psycopg2_connection, monkeypatch):
        """Return dict has all four required keys."""
        from congress_videos.modules import participants_enrichment as mod

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = []

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(mod, "fetch_wikidata_photos", lambda: [])

        result = mod.enrich_missing_photos()

        assert "enriched" in result
        assert "skipped_ambiguous" in result
        assert "skipped_no_match" in result
        assert "skipped_no_image" in result


# ===========================================================================
# TASK 4 RED: fetch_congreso_cod_parlamentario
# ===========================================================================


class TestFetchCongresCodParlamentario:
    """Tests for fetch_congreso_cod_parlamentario() — RED phase."""

    def test_returns_dict_keyed_by_normalized_name(self, mock_requests):
        """Fixture response → dict keyed by normalized name with codParlamentario value."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        fixture_data = _load_search_diputados_fixture()
        # Live shape: {"data": [...]} envelope with TitleCase "Apellidos, Nombre" in apellidosNombre.
        mock_requests.post.return_value = mock_requests.make_response(
            status_code=200,
            json_data=fixture_data,
        )

        result = fetch_congreso_cod_parlamentario()

        assert isinstance(result, dict)
        # Entry: codParlamentario=160, apellidosNombre="Abades Martínez, Cristina"
        # normalize_member_name("Abades Martínez, Cristina") → "cristina abades martinez"
        assert result["cristina abades martinez"] == "160"
        assert result["santiago abascal conde"] == "317"
        # Entry missing cod and entry missing name are both skipped.
        assert len(result) == 2

    def test_defensive_alternate_cod_key(self, mock_requests):
        """Entry using 'codigo' instead of 'codParlamentario' → cod still resolved."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        entries = [{"codigo": "DEF456", "nombre": "Pérez Ruiz, Juan"}]
        mock_requests.post.return_value = mock_requests.make_response(
            status_code=200,
            json_data=entries,
        )

        result = fetch_congreso_cod_parlamentario()

        assert "juan perez ruiz" in result
        assert result["juan perez ruiz"] == "DEF456"

    def test_defensive_alternate_name_key(self, mock_requests):
        """Entry using 'apellidosNombre' instead of 'nombre' → name still resolved."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        entries = [{"codParlamentario": "GHI789", "apellidosNombre": "Ruiz Gómez, Laura"}]
        mock_requests.post.return_value = mock_requests.make_response(
            status_code=200,
            json_data=entries,
        )

        result = fetch_congreso_cod_parlamentario()

        assert "laura ruiz gomez" in result
        assert result["laura ruiz gomez"] == "GHI789"

    def test_skips_entry_missing_cod_key(self, mock_requests):
        """Entry with no cod candidate key is absent from result (no KeyError)."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        entries = [{"nombre": "Sin Código, Deputy"}]
        mock_requests.post.return_value = mock_requests.make_response(
            status_code=200,
            json_data=entries,
        )

        result = fetch_congreso_cod_parlamentario()

        assert len(result) == 0

    def test_skips_entry_missing_name_key(self, mock_requests):
        """Entry with no name candidate key is absent from result."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        entries = [{"codParlamentario": "GHI789"}]
        mock_requests.post.return_value = mock_requests.make_response(
            status_code=200,
            json_data=entries,
        )

        result = fetch_congreso_cod_parlamentario()

        assert len(result) == 0

    def test_browser_user_agent_sent(self, mock_requests):
        """POST is called with User-Agent header equal to CONGRESO_BROWSER_USER_AGENT."""
        from congress_videos.config.constants import CONGRESO_BROWSER_USER_AGENT
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        mock_requests.post.return_value = mock_requests.make_response(status_code=200, json_data=[])

        fetch_congreso_cod_parlamentario()

        assert mock_requests.post.called
        _, call_kwargs = mock_requests.post.call_args
        headers = call_kwargs.get("headers", {})
        assert headers.get("User-Agent") == CONGRESO_BROWSER_USER_AGENT

    def test_exactly_one_post_per_call(self, mock_requests):
        """requests.post is called exactly once per invocation."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        mock_requests.post.return_value = mock_requests.make_response(status_code=200, json_data=[])

        fetch_congreso_cod_parlamentario()

        assert mock_requests.post.call_count == 1

    def test_403_raises_runtime_error(self, mock_requests):
        """HTTP 403 → RuntimeError raised with URL and body snippet."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        error_response = mock_requests.make_response(status_code=403, text="WAF blocked")
        mock_requests.post.return_value = error_response

        with pytest.raises(RuntimeError, match="403"):
            fetch_congreso_cod_parlamentario()

    def test_env_override_uses_get_not_post(self, mock_requests, monkeypatch):
        """CONGRESO_SEARCH_DIPUTADOS_URL_OVERRIDE set → GET is used, POST is not called."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        monkeypatch.setenv("CONGRESO_SEARCH_DIPUTADOS_URL_OVERRIDE", "http://static.example.com/search.json")
        mock_requests.get.return_value = mock_requests.make_response(status_code=200, json_data=[])

        fetch_congreso_cod_parlamentario()

        assert mock_requests.get.called
        assert mock_requests.post.call_count == 0

    def test_post_body_includes_portlet_form_fields(self, mock_requests):
        """POST body must carry the Liferay _diputadomodule_* form fields.

        A bare {"idLegislatura": 15} body makes the searchDiputados portlet
        return {"data": []} (verified live), so the full portlet dataRequest
        is required.
        """
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        mock_requests.post.return_value = mock_requests.make_response(status_code=200, json_data={"data": []})

        fetch_congreso_cod_parlamentario()

        _, call_kwargs = mock_requests.post.call_args
        body = call_kwargs.get("data", {})
        assert "_diputadomodule_idLegislatura" in body
        assert "_diputadomodule_filtroProvincias" in body

    def test_prefers_apellidos_nombre_over_given_name(self, mock_requests):
        """When both 'nombre' (given only) and 'apellidosNombre' (full) exist,
        the full name must win — otherwise the normalized-name join fails."""
        from congress_videos.modules.participants_enrichment import fetch_congreso_cod_parlamentario

        entries = {
            "data": [
                {
                    "codParlamentario": 500,
                    "nombre": "Cristina",
                    "apellidosNombre": "Abades Martínez, Cristina",
                }
            ]
        }
        mock_requests.post.return_value = mock_requests.make_response(status_code=200, json_data=entries)

        result = fetch_congreso_cod_parlamentario()

        assert result["cristina abades martinez"] == "500"
        assert "cristina" not in result  # must NOT key by the given name alone


# ===========================================================================
# TASK 6 RED: fill_congreso_photo_fallback
# ===========================================================================


def _make_participant_row(**overrides) -> dict:
    """Build a minimal participant DB row."""
    base = {
        "normalized_name": "garcia lopez maria",
        "display_name": "García López, María",
        "photo_url": None,
    }
    base.update(overrides)
    return base


class TestFillCongresoPhotoFallback:
    """Tests for fill_congreso_photo_fallback() — RED phase."""

    def test_builds_correct_url_and_calls_update_photo_url(self, mock_psycopg2_connection, monkeypatch):
        """Null-photo row + matching cod → update_photo_url called with correct URL."""
        from congress_videos.modules import participants_enrichment as mod

        row = _make_participant_row(normalized_name="garcia lopez maria", photo_url=None)
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(
            mod,
            "fetch_congreso_cod_parlamentario",
            lambda: {"garcia lopez maria": "ABC123"},
        )

        mod.fill_congreso_photo_fallback()

        mock_db.update_photo_url.assert_called_once_with(
            "garcia lopez maria",
            "https://www.congreso.es/docu/imgweb/diputados/ABC123_15.jpg",
        )

    def test_url_uses_legislature_id_15(self, mock_psycopg2_connection, monkeypatch):
        """Constructed URL contains '_15.jpg' (LEGISLATURE_ID=15)."""
        from congress_videos.modules import participants_enrichment as mod

        row = _make_participant_row(normalized_name="garcia lopez maria", photo_url=None)
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(
            mod,
            "fetch_congreso_cod_parlamentario",
            lambda: {"garcia lopez maria": "ABC123"},
        )

        mod.fill_congreso_photo_fallback()

        _, call_args = mock_db.update_photo_url.call_args
        passed_url = mock_db.update_photo_url.call_args[0][1]
        assert "_15.jpg" in passed_url

    def test_existing_photo_rows_not_overwritten(self, mock_psycopg2_connection, monkeypatch):
        """Row with non-null photo_url → update_photo_url NOT called."""
        from congress_videos.modules import participants_enrichment as mod

        row = _make_participant_row(
            normalized_name="garcia lopez maria",
            photo_url="https://commons.wikimedia.org/existing.jpg",
        )
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(
            mod,
            "fetch_congreso_cod_parlamentario",
            lambda: {"garcia lopez maria": "ABC123"},
        )

        mod.fill_congreso_photo_fallback()

        mock_db.update_photo_url.assert_not_called()

    def test_name_join_miss_skips_and_increments_skipped_no_cod(self, mock_psycopg2_connection, monkeypatch):
        """Cod dict has no key for participant → skipped_no_cod == 1, update NOT called."""
        from congress_videos.modules import participants_enrichment as mod

        row = _make_participant_row(normalized_name="unknown deputy", photo_url=None)
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(
            mod,
            "fetch_congreso_cod_parlamentario",
            lambda: {"garcia lopez maria": "ABC123"},
        )

        result = mod.fill_congreso_photo_fallback()

        mock_db.update_photo_url.assert_not_called()
        assert result["skipped_no_cod"] == 1

    def test_name_join_miss_logs_warning(self, mock_psycopg2_connection, monkeypatch, caplog):
        """Name-join miss → WARNING logged containing the normalized_name."""
        from congress_videos.modules import participants_enrichment as mod

        row = _make_participant_row(normalized_name="unknown deputy", photo_url=None)
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = [row]
        mock_db.update_photo_url = MagicMock()

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(
            mod,
            "fetch_congreso_cod_parlamentario",
            lambda: {},
        )

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.participants_enrichment"):
            mod.fill_congreso_photo_fallback()

        assert any("unknown deputy" in r.message for r in caplog.records)

    def test_fetch_error_returns_filled_zero_no_exception(self, mock_psycopg2_connection, monkeypatch):
        """fetch_congreso_cod_parlamentario raises RuntimeError → returns dict with filled==0 and error key."""
        from congress_videos.modules import participants_enrichment as mod

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = []
        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        def _raise():
            raise RuntimeError("searchDiputados 403: ...")

        monkeypatch.setattr(mod, "fetch_congreso_cod_parlamentario", _raise)

        result = mod.fill_congreso_photo_fallback()

        assert result["filled"] == 0
        assert "error" in result

    def test_network_error_returns_filled_zero_no_exception(self, mock_psycopg2_connection, monkeypatch):
        """fetch_congreso_cod_parlamentario raises ConnectionError → returns dict with filled==0."""
        from congress_videos.modules import participants_enrichment as mod

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = []
        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)

        def _raise():
            raise req_lib.ConnectionError("Network unreachable")

        monkeypatch.setattr(mod, "fetch_congreso_cod_parlamentario", _raise)

        result = mod.fill_congreso_photo_fallback()

        assert result["filled"] == 0
        assert "error" in result

    def test_return_dict_has_required_keys(self, mock_psycopg2_connection, monkeypatch):
        """Normal path return value has filled, skipped_no_cod, source_count keys."""
        from congress_videos.modules import participants_enrichment as mod

        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = []
        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(mod, "fetch_congreso_cod_parlamentario", lambda: {})

        result = mod.fill_congreso_photo_fallback()

        assert "filled" in result
        assert "skipped_no_cod" in result
        assert "source_count" in result

    def test_only_one_bulk_fetch_per_call(self, mock_psycopg2_connection, monkeypatch):
        """fetch_congreso_cod_parlamentario called exactly once even with 3 null-photo rows."""
        from congress_videos.modules import participants_enrichment as mod

        rows = [_make_participant_row(normalized_name=f"deputy {i}", photo_url=None) for i in range(3)]
        mock_db = MagicMock()
        mock_db.get_all_participants.return_value = rows

        call_counter = {"count": 0}

        def _fetch():
            call_counter["count"] += 1
            return {}

        monkeypatch.setattr(mod, "CongressParticipantsDB", lambda: mock_db)
        monkeypatch.setattr(mod, "fetch_congreso_cod_parlamentario", _fetch)

        mod.fill_congreso_photo_fallback()

        assert call_counter["count"] == 1
