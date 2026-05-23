"""Extended tests for youtube_channel.py — uncovered functions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_video(video_id: str = "vid001", title: str = "Test") -> dict:
    return {
        "video_id": video_id,
        "title": title,
        "description": "",
        "published_at": "2025-05-22T09:00:00Z",
        "thumbnail_url": "https://example.com/t.jpg",
        "channel_title": "Test Channel",
    }


def _make_plenary(videos: list) -> dict:
    return {"total_matches": len(videos), "videos": videos}


# ---------------------------------------------------------------------------
# get_video_descriptions
# ---------------------------------------------------------------------------

class TestGetVideoDescriptions:

    def test_empty_input_returns_zero(self, monkeypatch):
        """Empty or missing videos returns total_videos=0."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        from congress_videos.modules.youtube.youtube_channel import get_video_descriptions

        assert get_video_descriptions({})["total_videos"] == 0
        assert get_video_descriptions(None)["total_videos"] == 0

    def test_missing_api_key_raises_value_error(self, monkeypatch):
        """ValueError raised when YOUTUBE_API_KEY is not set."""
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

        from congress_videos.modules.youtube.youtube_channel import get_video_descriptions

        with pytest.raises(ValueError, match="YOUTUBE_API_KEY"):
            get_video_descriptions({"videos": [_make_video()]})

    def test_fetches_full_description(self, monkeypatch, mocker):
        """Full description is fetched via videos().list() API call."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "snippet": {
                        "description": "Full description text here.\nMultiple lines."
                    }
                }
            ]
        }
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_descriptions

        result = get_video_descriptions(_make_plenary([_make_video("vid-abc")]))

        assert result["total_videos"] == 1
        assert result["videos"][0]["description"] == "Full description text here.\nMultiple lines."
        assert result["videos"][0]["description_length"] > 0

    def test_skips_video_not_found_in_api(self, monkeypatch, mocker):
        """Video not returned by API is skipped."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {"items": []}
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_descriptions

        result = get_video_descriptions(_make_plenary([_make_video("missing")]))

        assert result["total_videos"] == 0

    def test_api_error_raises_runtime_error(self, monkeypatch, mocker):
        """API exception is wrapped in RuntimeError."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.side_effect = Exception(
            "API error"
        )
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_descriptions

        with pytest.raises(RuntimeError, match="Error getting video descriptions"):
            get_video_descriptions(_make_plenary([_make_video()]))


# ---------------------------------------------------------------------------
# parse_description_links
# ---------------------------------------------------------------------------

class TestParseDescriptionLinks:

    def test_empty_input_returns_zero(self):
        """Empty or None input returns total_videos=0."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        assert parse_description_links({})["total_videos"] == 0
        assert parse_description_links(None)["total_videos"] == 0

    def test_extracts_press_release_link(self):
        """Press release URL extracted from description."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        desc_videos = {
            "videos": [
                {
                    "video_id": "v1",
                    "title": "Session",
                    "description": "Nota de prensa: https://congreso.es/nota-123\nOtro texto",
                }
            ]
        }

        result = parse_description_links(desc_videos)

        assert result["total_videos"] == 1
        assert result["videos"][0]["press_release_link"] == "https://congreso.es/nota-123"

    def test_extracts_agenda_link(self):
        """Agenda PDF URL extracted from description."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        desc_videos = {
            "videos": [
                {
                    "video_id": "v2",
                    "title": "Session",
                    "description": "Orden del día: https://congreso.es/agenda.pdf",
                }
            ]
        }

        result = parse_description_links(desc_videos)

        assert result["videos"][0]["agenda_link"] == "https://congreso.es/agenda.pdf"

    def test_missing_links_are_none(self):
        """Video without links returns None for both link fields."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        desc_videos = {
            "videos": [
                {
                    "video_id": "v3",
                    "title": "Session",
                    "description": "No links here at all.",
                }
            ]
        }

        result = parse_description_links(desc_videos)

        assert result["videos"][0]["press_release_link"] is None
        assert result["videos"][0]["agenda_link"] is None

    def test_result_contains_video_id_and_title(self):
        """Result dict preserves video_id and title."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        desc_videos = {
            "videos": [
                {"video_id": "v10", "title": "Debate", "description": ""},
            ]
        }

        result = parse_description_links(desc_videos)

        assert result["videos"][0]["video_id"] == "v10"
        assert result["videos"][0]["title"] == "Debate"

    def test_case_insensitive_match(self):
        """Pattern match is case-insensitive for 'Nota de prensa'."""
        from congress_videos.modules.youtube.youtube_channel import parse_description_links

        desc_videos = {
            "videos": [
                {
                    "video_id": "v4",
                    "title": "Session",
                    "description": "NOTA DE PRENSA: https://example.com/nota",
                }
            ]
        }

        result = parse_description_links(desc_videos)

        assert result["videos"][0]["press_release_link"] == "https://example.com/nota"


# ---------------------------------------------------------------------------
# scrape_press_release
# ---------------------------------------------------------------------------

class TestScrapePressRelease:

    def test_empty_input_returns_zero(self):
        """Empty or None input returns total_scraped=0."""
        from congress_videos.modules.youtube.youtube_channel import scrape_press_release

        assert scrape_press_release({})["total_scraped"] == 0
        assert scrape_press_release(None)["total_scraped"] == 0

    def test_skips_video_without_press_link(self):
        """Video with no press_release_link is skipped."""
        from congress_videos.modules.youtube.youtube_channel import scrape_press_release

        parsed_links = {
            "videos": [
                {"video_id": "v1", "title": "Session", "press_release_link": None}
            ]
        }

        result = scrape_press_release(parsed_links)

        assert result["total_scraped"] == 0

    def test_successful_scrape_returns_content(self, mocker):
        """Successful HTTP response extracts content and title."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.url = "https://congreso.es/real-url"
        mock_response.content = b"<html><article><p>Press content here</p></article></html>"
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.requests.get",
            return_value=mock_response,
        )

        from congress_videos.modules.youtube.youtube_channel import scrape_press_release

        parsed_links = {
            "videos": [
                {
                    "video_id": "v1",
                    "title": "Session",
                    "press_release_link": "https://ow.ly/short123",
                }
            ]
        }

        result = scrape_press_release(parsed_links)

        assert result["total_scraped"] == 1
        assert result["videos"][0]["press_release_url"] == "https://congreso.es/real-url"

    def test_http_error_recorded_as_error_entry(self, mocker):
        """HTTP exception results in error entry, not exception propagation."""
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.requests.get",
            side_effect=Exception("Connection refused"),
        )

        from congress_videos.modules.youtube.youtube_channel import scrape_press_release

        parsed_links = {
            "videos": [
                {
                    "video_id": "v2",
                    "title": "Session",
                    "press_release_link": "https://ow.ly/error",
                }
            ]
        }

        result = scrape_press_release(parsed_links)

        assert result["total_scraped"] == 1
        assert "error" in result["videos"][0]

    def test_paragraph_fallback_when_no_article(self, mocker):
        """When no <article>/<main> found, falls back to paragraph text."""
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.url = "https://example.com/page"
        mock_response.content = b"<html><body><p>First para</p><p>Second para</p></body></html>"
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.requests.get",
            return_value=mock_response,
        )

        from congress_videos.modules.youtube.youtube_channel import scrape_press_release

        parsed_links = {
            "videos": [
                {
                    "video_id": "v3",
                    "title": "Session",
                    "press_release_link": "https://example.com/short",
                }
            ]
        }

        result = scrape_press_release(parsed_links)

        assert result["total_scraped"] == 1
        assert "First para" in result["videos"][0].get("press_release_content", "")


# ---------------------------------------------------------------------------
# extract_session_date
# ---------------------------------------------------------------------------

class TestExtractSessionDate:

    def test_empty_input_returns_zero(self):
        """Empty or None agendas returns total_processed=0."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        assert extract_session_date({}, "2025-05-22")["total_processed"] == 0
        assert extract_session_date(None, "2025-05-22")["total_processed"] == 0

    def test_skips_video_without_agenda_text(self):
        """Video with no agenda_text is skipped."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        agendas = {
            "videos": [
                {"video_id": "v1", "video_title": "Session", "agenda_text": ""}
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 0

    def test_skips_video_with_error(self):
        """Video with error key in dict is skipped."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        agendas = {
            "videos": [
                {
                    "video_id": "v1",
                    "video_title": "Session",
                    "agenda_text": "Sesion nº135",
                    "error": "Download failed",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 0

    def test_session_number_not_found_records_error(self):
        """Agenda without session number pattern records error entry."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        agendas = {
            "videos": [
                {
                    "video_id": "v1",
                    "video_title": "Session",
                    "agenda_text": "Orden del dia sin numero de sesion",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 1
        assert "error" in result["videos"][0]

    def test_extracts_session_number_for_first_date(self):
        """When target date is first date in agenda, offset=0, session=base."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        # Note: regex is 'Sesión\s+nº\s*(\d+)' — must use accented characters
        agenda_text = (
            "Sesión nº135\n"
            "JUEVES, 22 DE MAYO\n"
            "Punto 1: Debate\n"
        )

        agendas = {
            "videos": [
                {
                    "video_id": "v1",
                    "video_title": "Plenaria",
                    "agenda_text": agenda_text,
                    "agenda_file_path": "/data/agenda.pdf",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 1
        assert result["videos"][0]["session_number"] == 135
        assert result["videos"][0]["date_offset"] == 0

    def test_extracts_session_number_for_second_date(self):
        """When target date is second date in agenda, offset=1, session=base+1."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        # Note: regex is 'Sesión\s+nº\s*(\d+)' — must use accented characters
        agenda_text = (
            "Sesión nº135\n"
            "MIÉRCOLES, 21 DE MAYO\n"
            "Punto 1: Debate\n"
            "JUEVES, 22 DE MAYO\n"
            "Punto 2: Votacion\n"
        )

        agendas = {
            "videos": [
                {
                    "video_id": "v1",
                    "video_title": "Plenaria",
                    "agenda_text": agenda_text,
                    "agenda_file_path": "/data/agenda.pdf",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 1
        assert result["videos"][0]["session_number"] == 136
        assert result["videos"][0]["date_offset"] == 1

    def test_target_date_not_found_in_agenda(self):
        """When target date is not in agenda dates, records warning entry."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        agenda_text = (
            "Sesión nº200\n"
            "LUNES, 20 DE ENERO\n"
            "Punto 1\n"
        )

        agendas = {
            "videos": [
                {
                    "video_id": "v2",
                    "video_title": "Plenaria",
                    "agenda_text": agenda_text,
                    "agenda_file_path": "/data/agenda.pdf",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 1
        assert "warning" in result["videos"][0]
        assert result["videos"][0]["session_number"] == 200

    def test_no_date_headers_found_uses_base_session_number(self):
        """Agenda with session number but no date headers uses base session and full agenda."""
        from congress_videos.modules.youtube.youtube_channel import extract_session_date

        agenda_text = "Sesión nº77\nOrden del dia: varios puntos importantes"

        agendas = {
            "videos": [
                {
                    "video_id": "v3",
                    "video_title": "Plenaria",
                    "agenda_text": agenda_text,
                    "agenda_file_path": "/data/agenda.pdf",
                }
            ]
        }

        result = extract_session_date(agendas, "2025-05-22")

        assert result["total_processed"] == 1
        assert result["videos"][0]["session_number"] == 77
        assert "warning" in result["videos"][0]
