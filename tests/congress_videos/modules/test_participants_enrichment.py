"""Tests for participants_enrichment.py — Slice 2 (T-11 through T-14)."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

FIXTURES_DIR = Path(__file__).parent.parent.parent / "fixtures"


def _load_wikidata_fixture() -> dict:
    return json.loads((FIXTURES_DIR / "sample_wikidata_sparql.json").read_text(encoding="utf-8"))


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
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos
        from congress_videos.config.constants import WIKIDATA_USER_AGENT

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
        from congress_videos.modules.participants_enrichment import fetch_wikidata_photos
        import requests as req_lib

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

        mock_db.update_photo_url.assert_called_once_with(
            "ana lopez sanchez", "http://example.com/photo.jpg"
        )
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
