"""Tests for new constants added in add-congreso-photo-fallback change."""

from __future__ import annotations


class TestCongresoSearchDiputadosUrl:
    """Tests for CONGRESO_SEARCH_DIPUTADOS_URL constant."""

    def test_congreso_search_diputados_url_is_non_empty_string(self):
        """CONGRESO_SEARCH_DIPUTADOS_URL must be a non-empty string starting with https://."""
        from congress_videos.config.constants import CONGRESO_SEARCH_DIPUTADOS_URL

        assert isinstance(CONGRESO_SEARCH_DIPUTADOS_URL, str)
        assert CONGRESO_SEARCH_DIPUTADOS_URL.startswith("https://")

    def test_congreso_search_diputados_url_contains_searchDiputados_resource_id(self):
        """CONGRESO_SEARCH_DIPUTADOS_URL must contain 'searchDiputados' resource ID."""
        from congress_videos.config.constants import CONGRESO_SEARCH_DIPUTADOS_URL

        assert "searchDiputados" in CONGRESO_SEARCH_DIPUTADOS_URL


class TestCongresoPhotoUrlTemplate:
    """Tests for CONGRESO_PHOTO_URL_TEMPLATE constant."""

    def test_congreso_photo_url_template_is_non_empty_string(self):
        """CONGRESO_PHOTO_URL_TEMPLATE must be a non-empty string."""
        from congress_videos.config.constants import CONGRESO_PHOTO_URL_TEMPLATE

        assert isinstance(CONGRESO_PHOTO_URL_TEMPLATE, str)
        assert len(CONGRESO_PHOTO_URL_TEMPLATE) > 0

    def test_congreso_photo_url_template_formats_with_cod_and_leg(self):
        """Template formats correctly with cod and leg parameters."""
        from congress_videos.config.constants import CONGRESO_PHOTO_URL_TEMPLATE

        result = CONGRESO_PHOTO_URL_TEMPLATE.format(cod="ABC123", leg=15)
        assert result == "https://www.congreso.es/docu/imgweb/diputados/ABC123_15.jpg"
