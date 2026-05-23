"""Tests for congress_videos.modules.web_scraping module."""

from __future__ import annotations

from datetime import datetime

import pytest
from freezegun import freeze_time

from congress_videos.config.constants import BASE_SESSION_URL, LEGISLATURE_ID, ORGANO_ID
from congress_videos.modules.web_scraping import construct_session_link


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _parse_query_params(url: str) -> dict[str, str]:
    """Extract query params from a URL string into a dict."""
    _, _, query = url.partition("?")
    params: dict[str, str] = {}
    for pair in query.split("&"):
        if "=" in pair:
            k, _, v = pair.partition("=")
            params[k] = v
    return params


# ---------------------------------------------------------------------------
# construct_session_link — basic structure
# ---------------------------------------------------------------------------

class TestConstructSessionLinkStructure:

    def test_returns_string(self):
        result = construct_session_link("123", target_date="2025-10-08")
        assert isinstance(result, str)

    def test_url_starts_with_base_session_url(self):
        result = construct_session_link("123", target_date="2025-10-08")
        assert result.startswith(BASE_SESSION_URL)

    def test_url_contains_cod_organo_param(self):
        result = construct_session_link("123", target_date="2025-10-08")
        params = _parse_query_params(result)
        assert "codOrgano" in params
        assert params["codOrgano"] == str(ORGANO_ID)

    def test_url_contains_cod_sesion_param(self):
        result = construct_session_link("42", target_date="2025-10-08")
        params = _parse_query_params(result)
        assert "codSesion" in params
        assert params["codSesion"] == "42"

    def test_url_contains_legislature_id_param(self):
        result = construct_session_link("123", target_date="2025-10-08")
        params = _parse_query_params(result)
        assert "idLegislaturaElegida" in params
        assert params["idLegislaturaElegida"] == str(LEGISLATURE_ID)

    def test_url_contains_fecha_sesion_param(self):
        result = construct_session_link("123", target_date="2025-10-08")
        params = _parse_query_params(result)
        assert "fechaSesion" in params


# ---------------------------------------------------------------------------
# construct_session_link — with string date
# ---------------------------------------------------------------------------

class TestConstructSessionLinkWithStringDate:

    def test_string_date_formats_as_dd_mm_yyyy(self):
        result = construct_session_link("1", target_date="2025-10-08")
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "08/10/2025"

    def test_string_date_first_of_january(self):
        result = construct_session_link("1", target_date="2025-01-01")
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "01/01/2025"

    def test_string_date_last_of_december(self):
        result = construct_session_link("1", target_date="2024-12-31")
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "31/12/2024"

    @pytest.mark.parametrize("date_str,expected", [
        ("2025-03-15", "15/03/2025"),
        ("2024-07-04", "04/07/2024"),
        ("2026-11-30", "30/11/2026"),
    ])
    def test_various_string_dates(self, date_str: str, expected: str):
        result = construct_session_link("5", target_date=date_str)
        params = _parse_query_params(result)
        assert params["fechaSesion"] == expected


# ---------------------------------------------------------------------------
# construct_session_link — with datetime object
# ---------------------------------------------------------------------------

class TestConstructSessionLinkWithDatetimeObject:

    def test_datetime_object_formats_correctly(self):
        dt = datetime(2025, 10, 8)
        result = construct_session_link("1", target_date=dt)
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "08/10/2025"

    def test_datetime_with_time_component_uses_date_only(self):
        dt = datetime(2025, 6, 15, 14, 30, 0)
        result = construct_session_link("10", target_date=dt)
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "15/06/2025"

    def test_datetime_beginning_of_year(self):
        dt = datetime(2024, 1, 1)
        result = construct_session_link("99", target_date=dt)
        params = _parse_query_params(result)
        assert params["fechaSesion"] == "01/01/2024"


# ---------------------------------------------------------------------------
# construct_session_link — without date (defaults to yesterday)
# ---------------------------------------------------------------------------

class TestConstructSessionLinkWithoutDate:

    @freeze_time("2025-10-09")
    def test_no_date_defaults_to_yesterday(self):
        result = construct_session_link("1")
        params = _parse_query_params(result)
        # Yesterday of 2025-10-09 is 2025-10-08
        assert params["fechaSesion"] == "08/10/2025"

    @freeze_time("2025-01-01")
    def test_no_date_yesterday_crosses_year_boundary(self):
        result = construct_session_link("1")
        params = _parse_query_params(result)
        # Yesterday of 2025-01-01 is 2024-12-31
        assert params["fechaSesion"] == "31/12/2024"

    @freeze_time("2025-03-01")
    def test_no_date_yesterday_crosses_month_boundary(self):
        result = construct_session_link("1")
        params = _parse_query_params(result)
        # Yesterday of 2025-03-01 is 2025-02-28
        assert params["fechaSesion"] == "28/02/2025"

    @freeze_time("2025-10-09")
    def test_no_date_still_includes_cod_sesion(self):
        result = construct_session_link("42")
        params = _parse_query_params(result)
        assert params["codSesion"] == "42"


# ---------------------------------------------------------------------------
# construct_session_link — cod_sesion variations
# ---------------------------------------------------------------------------

class TestConstructSessionLinkCodSesion:

    @pytest.mark.parametrize("cod_sesion", ["1", "42", "100", "PL_115_001"])
    def test_various_session_codes(self, cod_sesion: str):
        result = construct_session_link(cod_sesion, target_date="2025-10-08")
        params = _parse_query_params(result)
        assert params["codSesion"] == cod_sesion
