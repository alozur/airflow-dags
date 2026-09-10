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
            "items": [{"snippet": {"description": "Full description text here.\nMultiple lines."}}]
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
        fake_service.videos.return_value.list.return_value.execute.side_effect = Exception("API error")
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

        parsed_links = {"videos": [{"video_id": "v1", "title": "Session", "press_release_link": None}]}

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

        agendas = {"videos": [{"video_id": "v1", "video_title": "Session", "agenda_text": ""}]}

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
        agenda_text = "Sesión nº135\nJUEVES, 22 DE MAYO\nPunto 1: Debate\n"

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
        agenda_text = "Sesión nº135\nMIÉRCOLES, 21 DE MAYO\nPunto 1: Debate\nJUEVES, 22 DE MAYO\nPunto 2: Votacion\n"

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

        agenda_text = "Sesión nº200\nLUNES, 20 DE ENERO\nPunto 1\n"

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


# ---------------------------------------------------------------------------
# _parse_agenda_dates / _locate_target_date_offset — RED-first quirk tests
# for the helpers lifted verbatim out of extract_session_date (issue #272)
# ---------------------------------------------------------------------------

_DATE_PATTERN = r"([A-ZÁÉÍÓÚÑ]+),\s*(\d{1,2})\s+[Dd][Ee]\s+([A-ZÁÉÍÓÚÑ]+)(?:\s+[Dd][Ee]\s+(\d{4}))?"
_SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


class TestParseAgendaDates:
    def test_unknown_month_is_skipped_no_entry(self):
        import re
        from datetime import datetime

        from congress_videos.modules.youtube.youtube_channel import _parse_agenda_dates

        matches = list(re.finditer(_DATE_PATTERN, "VIERNES, 8 DE FOOBAR\n"))

        result = _parse_agenda_dates(matches, _SPANISH_MONTHS, datetime(2025, 10, 7))

        assert result == []

    def test_invalid_day_raises_value_error_caught_and_skipped(self):
        import re
        from datetime import datetime

        from congress_videos.modules.youtube.youtube_channel import _parse_agenda_dates

        matches = list(re.finditer(_DATE_PATTERN, "MARTES, 31 DE FEBRERO\n"))

        result = _parse_agenda_dates(matches, _SPANISH_MONTHS, datetime(2025, 10, 7))

        assert result == []

    def test_original_index_counts_accepted_entries_only(self):
        import re
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _parse_agenda_dates

        text = "LUNES, 5 DE ENERO\nMARTES, 31 DE FEBRERO\nJUEVES, 7 DE OCTUBRE DE 2024\nVIERNES, 8 DE FOOBAR\n"
        matches = list(re.finditer(_DATE_PATTERN, text))

        result = _parse_agenda_dates(matches, _SPANISH_MONTHS, datetime(2025, 10, 7))

        assert len(result) == 2
        assert result[0]["date"] == date(2025, 1, 5)
        assert result[0]["original_index"] == 0
        assert result[1]["date"] == date(2024, 10, 7)
        assert result[1]["original_index"] == 1

    def test_explicit_year_beats_target_date_obj_year(self):
        import re
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _parse_agenda_dates

        matches = list(re.finditer(_DATE_PATTERN, "JUEVES, 7 DE OCTUBRE DE 2024\n"))

        result = _parse_agenda_dates(matches, _SPANISH_MONTHS, datetime(2025, 10, 7))

        assert result[0]["date"] == date(2024, 10, 7)

    def test_original_match_object_is_kept(self):
        import re
        from datetime import datetime

        from congress_videos.modules.youtube.youtube_channel import _parse_agenda_dates

        matches = list(re.finditer(_DATE_PATTERN, "LUNES, 5 DE ENERO\n"))

        result = _parse_agenda_dates(matches, _SPANISH_MONTHS, datetime(2025, 10, 7))

        assert result[0]["match"] is matches[0]


class TestLocateTargetDateOffset:
    def test_first_date_is_offset_zero_and_found_true(self):
        """The falsy-valid trap: offset 0 must be paired with found_target
        True, not mistaken for "not found"."""
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _locate_target_date_offset

        sorted_dates = [{"date": date(2025, 5, 21)}, {"date": date(2025, 5, 22)}]

        offset, entry, found_target = _locate_target_date_offset(sorted_dates, datetime(2025, 5, 21), "2025-05-21")

        assert found_target is True
        assert offset == 0
        assert entry is sorted_dates[0]

    def test_not_found_returns_none_none_false(self):
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _locate_target_date_offset

        sorted_dates = [{"date": date(2025, 5, 21)}]

        result = _locate_target_date_offset(sorted_dates, datetime(2025, 6, 1), "2025-06-01")

        assert result == (None, None, False)

    def test_duplicate_dates_first_index_wins(self):
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _locate_target_date_offset

        sorted_dates = [{"date": date(2025, 5, 21)}, {"date": date(2025, 5, 21)}]

        offset, entry, found_target = _locate_target_date_offset(sorted_dates, datetime(2025, 5, 21), "2025-05-21")

        assert offset == 0
        assert entry is sorted_dates[0]

    def test_comparison_uses_date_not_datetime(self):
        """target_date_obj is a datetime; the comparison must call .date()
        so a sorted_dates entry storing a plain date still matches a
        target with a non-midnight time component."""
        from datetime import date, datetime

        from congress_videos.modules.youtube.youtube_channel import _locate_target_date_offset

        sorted_dates = [{"date": date(2025, 5, 21)}]

        offset, _entry, found_target = _locate_target_date_offset(
            sorted_dates, datetime(2025, 5, 21, 13, 45), "2025-05-21"
        )

        assert found_target is True
        assert offset == 0


# ---------------------------------------------------------------------------
# extract_agenda_section — characterization tests (issue #272, slice 5 PR4).
# Zero prior coverage; these pin current behavior BEFORE the lift so the
# refactor commit (PR4 commit 2) can be checked against them unchanged.
# ---------------------------------------------------------------------------

AGENDA_TEXT = (
    "Sesión nº135\nMIÉRCOLES, 21 DE MAYO\nPunto 1: Debate de totalidad\nJUEVES, 22 DE MAYO\nPunto 2: Votación\n"
)
AGENDAS = {
    "total_downloaded": 1,
    "videos": [
        {
            "video_id": "v1",
            "video_title": "Plenaria",
            "agenda_url": "http://x/a.pdf",
            "agenda_file_path": "/data/agenda.pdf",
            "agenda_text": AGENDA_TEXT,
        }
    ],
}
SESSION_INFO = {
    "total_processed": 1,
    "videos": [
        {
            "video_id": "v1",
            "video_title": "Plenaria",
            "target_date": "2025-05-22",
            "session_number": 136,
            "base_session_number": 135,
            "date_offset": 1,
        }
    ],
}


def _session_info(target_date: str, video_id: str = "v1") -> dict:
    return {
        "total_processed": 1,
        "videos": [
            {
                "video_id": video_id,
                "video_title": "Plenaria",
                "target_date": target_date,
                "session_number": 136,
                "base_session_number": 135,
                "date_offset": 1,
            }
        ],
    }


def _agendas(agenda_text: str, video_id: str = "v1", extra: dict | None = None) -> dict:
    video = {
        "video_id": video_id,
        "video_title": "Plenaria",
        "agenda_url": "http://x/a.pdf",
        "agenda_file_path": "/data/agenda.pdf",
        "agenda_text": agenda_text,
    }
    if extra:
        video.update(extra)
    return {"total_downloaded": 1, "videos": [video]}


class TestExtractAgendaSection:
    def test_extracts_section_between_target_header_and_next_header(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        result = extract_agenda_section(AGENDAS, _session_info("2025-05-21"))

        assert result["total_extracted"] == 1
        entry = result["videos"][0]
        expected = "MIÉRCOLES, 21 DE MAYO\nPunto 1: Debate de totalidad"
        assert entry["agenda_section"] == expected
        assert entry["section_length"] == len(expected)
        assert entry["section_length"] == 50
        assert entry["full_agenda_file_path"] == "/data/agenda.pdf"
        assert entry["session_number"] == 136
        assert entry["video_title"] == "Plenaria"

    def test_last_date_section_runs_to_end_of_document(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        result = extract_agenda_section(AGENDAS, _session_info("2025-05-22"))

        entry = result["videos"][0]
        assert entry["agenda_section"] == "JUEVES, 22 DE MAYO\nPunto 2: Votación"

    def test_target_date_absent_returns_full_agenda_with_warning(self):
        """The `if target_section:` else-branch at :1235 — no header matches
        the target date."""
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        result = extract_agenda_section(AGENDAS, _session_info("2025-05-23"))

        entry = result["videos"][0]
        assert entry["agenda_section"] == AGENDA_TEXT
        assert entry["warning"].startswith("Could not find section for 2025-05-23")
        assert "section_length" not in entry
        assert "full_agenda_file_path" not in entry

    def test_no_parseable_date_headers_returns_full_agenda(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        agenda_text = "Sesión nº135\nPunto 1: algo\n"

        result = extract_agenda_section(_agendas(agenda_text), _session_info("2025-05-22"))

        entry = result["videos"][0]
        assert entry["warning"] == "Could not parse date headers, returning full agenda"
        assert entry["agenda_section"] == agenda_text

    def test_invalid_and_unknown_month_headers_do_not_abort_the_scan(self):
        """Pins `if not month_num: continue` (:1206) and
        `except ValueError: continue` (:1231) — the scan keeps going past a
        31-DE-FEBRERO and an unknown month to find the real match."""
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        agenda_text = "LUNES, 31 DE FEBRERO\nx\nMARTES, 5 DE FOOBAR\ny\nJUEVES, 22 DE MAYO\nPunto 2\n"

        result = extract_agenda_section(_agendas(agenda_text), _session_info("2025-05-22"))

        entry = result["videos"][0]
        assert "warning" not in entry
        assert entry["agenda_section"] == "JUEVES, 22 DE MAYO\nPunto 2"

    def test_missing_agenda_for_video_id_yields_error_entry(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        result = extract_agenda_section(AGENDAS, _session_info("2025-05-22", video_id="v2"))

        entry = result["videos"][0]
        assert entry == {
            "video_id": "v2",
            "target_date": "2025-05-22",
            "error": "No agenda found for this video",
        }
        assert "agenda_section" not in entry

    def test_empty_agenda_text_yields_error_entry(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        result = extract_agenda_section(_agendas(""), _session_info("2025-05-22"))

        entry = result["videos"][0]
        assert entry["error"] == "No agenda text available"

    def test_agenda_item_carrying_error_key_yields_error_entry(self):
        """The `or "error" in agenda_item` half of :1162 — a non-empty
        agenda_text is still rejected when the agenda item carries an
        error key from a prior download failure."""
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        agendas = _agendas(AGENDA_TEXT, extra={"error": "boom"})

        result = extract_agenda_section(agendas, _session_info("2025-05-22"))

        entry = result["videos"][0]
        assert entry["error"] == "No agenda text available"

    def test_empty_inputs_return_zero_extracted(self):
        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        assert extract_agenda_section({}, SESSION_INFO) == {"total_extracted": 0, "videos": []}
        assert extract_agenda_section(AGENDAS, None) == {"total_extracted": 0, "videos": []}
        assert extract_agenda_section(AGENDAS, {"videos": []}) == {"total_extracted": 0, "videos": []}

    def test_first_videos_target_date_must_parse(self):
        """Pins :1118 — `target_date_obj` is never read afterwards (masked
        F841) but it is load-bearing: it validates the first video's
        target_date and indexes ["videos"][0]. Deleting it as "dead code"
        would be a behaviour change."""
        import pytest

        from congress_videos.modules.youtube.youtube_channel import extract_agenda_section

        session_date_info = {"videos": [{"video_id": "v1", "target_date": "31/05/2025"}]}

        with pytest.raises(ValueError):
            extract_agenda_section(AGENDAS, session_date_info)
