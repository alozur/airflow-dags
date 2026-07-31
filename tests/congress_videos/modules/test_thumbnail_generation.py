"""Tests for congress_videos.modules.thumbnail_generation (Slice 2).

Strict TDD: all tests in this file are written BEFORE the production module
exists.  First run must produce ImportError / AttributeError failures.

Test groups:
    T-01  resolve_participant_photo
    T-02  generate_and_score_options (happy path + path-creation)
    T-03  choose_best_option
    T-04  generate_title
    T-05  persist_results
    T-06  TRIANGULATE edge cases
"""

from __future__ import annotations

import base64
import logging
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers / shared fixtures
# ---------------------------------------------------------------------------

def _make_cfg(
    *,
    party_logo_map=None,
    lookup_return=None,
    lookup_raises=False,
) -> dict:
    """Build a minimal domain-config dict for testing."""
    if lookup_raises:
        def _lookup(name: str):
            raise RuntimeError("unexpected call to lookup")
    elif lookup_return is not None:
        def _lookup(name: str):
            return lookup_return
    else:
        def _lookup(name: str):
            return None

    styles = [
        {
            "label": "option_a",
            "style": "dramatic style A",
            "persona": "persona A",
        },
        {
            "label": "option_b",
            "style": "editorial style B",
            "persona": "persona B",
        },
    ]
    return {
        "styles": styles,
        "participants_lookup": _lookup,
        "party_logo_map": party_logo_map,
    }


# ---------------------------------------------------------------------------
# T-01: resolve_participant_photo
# ---------------------------------------------------------------------------

class TestResolveParticipantPhoto:
    """resolve_participant_photo(name, cfg) contracts."""

    def test_photo_url_present_returns_base64_dict(self, monkeypatch):
        """When participant has photo_url, HTTP GET is performed and bytes returned."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        participant = {"normalized_name": "garcia_maria", "photo_url": "https://example.com/garcia.jpg"}
        cfg = _make_cfg(lookup_return=participant)

        fake_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 24
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = fake_bytes

        with patch("requests.get", return_value=mock_resp) as mock_get:
            result = resolve_participant_photo("garcia_maria", cfg)

        mock_get.assert_called_once_with("https://example.com/garcia.jpg", timeout=30)
        assert result["source"] == "photo"
        assert result["support_image_b64"] == base64.b64encode(fake_bytes).decode()

    def test_photo_url_none_with_logo_returns_logo_bytes(self, tmp_path):
        """When photo_url is NULL but party logo exists, logo bytes returned, no HTTP."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        logo_file = tmp_path / "logo.png"
        logo_bytes = b"\x89PNG\r\n\x1a\n" + b"\xff" * 20
        logo_file.write_bytes(logo_bytes)

        participant = {"normalized_name": "garcia_maria", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=str(logo_file))

        with patch("requests.get") as mock_get:
            result = resolve_participant_photo("garcia_maria", cfg)

        mock_get.assert_not_called()
        assert result["source"] == "party_logo"
        assert result["support_image_b64"] == base64.b64encode(logo_bytes).decode()

    def test_photo_url_none_no_logo_raises_value_error(self):
        """When photo_url is NULL and no party logo, ValueError raised."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        participant = {"normalized_name": "garcia_maria", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with pytest.raises(ValueError, match="no photo source"):
            resolve_participant_photo("garcia_maria", cfg)

    def test_participant_not_found_raises_lookup_error(self):
        """When lookup returns None, LookupError is raised."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        cfg = _make_cfg(lookup_return=None)

        with pytest.raises(LookupError):
            resolve_participant_photo("unknown_person", cfg)


# ---------------------------------------------------------------------------
# T-02: generate_and_score_options
# ---------------------------------------------------------------------------

class TestGenerateAndScoreOptions:
    """generate_and_score_options(prompt, photo_b64, cfg) contracts."""

    def _make_pikzels_mocks(self, mocker, tmp_path):
        """Patch pikzels_client functions used inside generate_and_score_options."""
        mock_tft = mocker.patch(
            "congress_videos.modules.thumbnail_generation.thumbnail_from_text",
            side_effect=[
                {"output": "https://pikzels.com/a.png", "label": "option_a"},
                {"output": "https://pikzels.com/b.png", "label": "option_b"},
            ],
        )
        mock_dl = mocker.patch(
            "congress_videos.modules.thumbnail_generation.download",
        )
        mock_score = mocker.patch(
            "congress_videos.modules.thumbnail_generation.score_thumbnail",
            side_effect=[72.0, 85.0],
        )
        mock_dir = tmp_path / "vid123"
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.get_thumbnail_dir",
            return_value=mock_dir,
        )
        return mock_tft, mock_dl, mock_score, mock_dir

    def test_exactly_two_thumbnail_calls(self, mocker, tmp_path):
        """Exactly 2 thumbnail_from_text calls are made."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()
        cfg["youtube_video_id"] = "vid123"

        generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        assert mock_tft.call_count == 2

    def test_download_called_immediately_after_each_generation(self, mocker, tmp_path):
        """Each generated thumbnail is downloaded immediately to the local path."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()

        generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        assert mock_dl.call_count == 2
        # Each download uses the generated output URL
        first_call_url = mock_dl.call_args_list[0][0][0]
        second_call_url = mock_dl.call_args_list[1][0][0]
        assert "pikzels.com" in first_call_url
        assert "pikzels.com" in second_call_url

    def test_each_option_is_scored(self, mocker, tmp_path):
        """score_thumbnail is called once per option."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()

        generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        assert mock_score.call_count == 2

    def test_returns_list_with_correct_keys(self, mocker, tmp_path):
        """Return value is a list of 2 dicts containing {label, output_url, local_path, main_score, style}."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()

        result = generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        assert len(result) == 2
        for opt in result:
            assert "label" in opt
            assert "output_url" in opt
            assert "local_path" in opt
            assert "main_score" in opt
            assert "style" in opt

    def test_scores_stored_correctly(self, mocker, tmp_path):
        """main_score values match the side_effect order."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()

        result = generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        assert result[0]["main_score"] == 72.0
        assert result[1]["main_score"] == 85.0

    def test_local_path_uses_thumbnail_dir(self, mocker, tmp_path):
        """local_path for each option is under the thumbnail dir with label.png suffix."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mock_tft, mock_dl, mock_score, mock_dir = self._make_pikzels_mocks(mocker, tmp_path)
        cfg = _make_cfg()

        result = generate_and_score_options("a debate summary", "b64photo==", cfg, "vid123")

        for opt in result:
            lp = Path(opt["local_path"])
            assert lp.suffix == ".png"
            assert lp.parent == mock_dir


class TestGenerateAndScoreOptionsPathCreation:
    """generate_and_score_options does not propagate FileNotFoundError for missing dir."""

    def test_no_file_not_found_error_when_dir_missing(self, mocker, tmp_path):
        """download is called even if local dir does not yet exist (it creates it)."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        # dir does NOT exist; download mock simulates success
        missing_dir = tmp_path / "does_not_exist" / "vid999"

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.thumbnail_from_text",
            side_effect=[
                {"output": "https://pikzels.com/a.png"},
                {"output": "https://pikzels.com/b.png"},
            ],
        )
        mocker.patch("congress_videos.modules.thumbnail_generation.download")
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.score_thumbnail",
            return_value=50.0,
        )
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.get_thumbnail_dir",
            return_value=missing_dir,
        )

        cfg = _make_cfg()
        # Should not raise
        result = generate_and_score_options("summary", "b64==", cfg, "vid999")
        assert len(result) == 2


# ---------------------------------------------------------------------------
# T-03: choose_best_option
# ---------------------------------------------------------------------------

class TestChooseBestOption:
    """choose_best_option(options) selects max score; tie → first option."""

    def test_higher_score_wins(self):
        """Option B (score 85) beats option A (score 72)."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {"label": "option_a", "main_score": 72.0, "output_url": "urlA", "local_path": "/a.png", "style": "s"},
            {"label": "option_b", "main_score": 85.0, "output_url": "urlB", "local_path": "/b.png", "style": "s"},
        ]
        best = choose_best_option(options)
        assert best["label"] == "option_b"

    def test_equal_scores_first_option_wins(self):
        """Tie-break: option A (index 0) wins when scores are equal."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {"label": "option_a", "main_score": 78.0, "output_url": "urlA", "local_path": "/a.png", "style": "s"},
            {"label": "option_b", "main_score": 78.0, "output_url": "urlB", "local_path": "/b.png", "style": "s"},
        ]
        best = choose_best_option(options)
        assert best["label"] == "option_a"

    def test_returns_dict_with_is_chosen_true(self):
        """Return value includes is_chosen=True."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {"label": "option_a", "main_score": 90.0, "output_url": "u", "local_path": "/a.png", "style": "s"},
            {"label": "option_b", "main_score": 50.0, "output_url": "u", "local_path": "/b.png", "style": "s"},
        ]
        best = choose_best_option(options)
        assert best.get("is_chosen") is True


# ---------------------------------------------------------------------------
# T-04: generate_title
# ---------------------------------------------------------------------------

class TestGenerateTitle:
    """generate_title(summary, best, cfg) contracts."""

    def _best_opt(self, label="option_a", style="dramatic style", persona="persona A", prompt="prompt text"):
        return {
            "label": label,
            "style": style,
            "persona": persona,
            "prompt": prompt,
            "output_url": "u",
            "local_path": "/a.png",
            "main_score": 80.0,
        }

    def test_valid_title_accepted_on_first_attempt(self, mocker):
        """Valid title (≤90 chars, no emoji, no forbidden chars) accepted without re-prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        valid_title = "El Congreso vota el futuro de las pensiones"
        mock_completion = mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": {"title": valid_title}, "error": None},
        )
        cfg = _make_cfg()
        result = generate_title("Debate sobre pensiones", self._best_opt(), cfg)

        assert result == valid_title
        assert mock_completion.call_count == 1

    def test_calls_generate_json_completion_not_openai_directly(self, mocker):
        """generate_title calls utils.ai_helpers.generate_json_completion, not openai directly."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mock_fn = mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": {"title": "Valid title aquí"}, "error": None},
        )
        cfg = _make_cfg()
        generate_title("Debate summary", self._best_opt(), cfg)

        mock_fn.assert_called_once()
        # Verify no openai direct call was made via module import
        import congress_videos.modules.thumbnail_generation as m
        assert not hasattr(m, "openai"), "Module must not import openai directly"

    def test_title_exceeding_90_chars_triggers_reprompt(self, mocker):
        """First response >90 chars triggers a second OpenAI call."""
        from congress_videos.modules.thumbnail_generation import generate_title

        long_title = "A" * 110
        short_title = "Título corto válido"
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": long_title}, "error": None}
            return {"data": {"title": short_title}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        result = generate_title("summary", self._best_opt(), cfg)

        assert call_count["n"] == 2
        assert result == short_title

    def test_title_with_emoji_triggers_reprompt(self, mocker):
        """First response containing emoji triggers a second call."""
        from congress_videos.modules.thumbnail_generation import generate_title

        emoji_title = "El Congreso debate \U0001f525 el futuro"
        clean_title = "El Congreso debate el futuro"
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": emoji_title}, "error": None}
            return {"data": {"title": clean_title}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        result = generate_title("summary", self._best_opt(), cfg)

        assert call_count["n"] == 2
        assert "\U0001f525" not in result

    def test_both_attempts_invalid_strip_and_warn_no_raise(self, mocker, caplog):
        """When both attempts are invalid, emojis stripped + truncated to 90 chars + WARNING logged."""
        from congress_videos.modules.thumbnail_generation import generate_title

        bad_title = "El Congreso \U0001f525 vota " + "x" * 100  # emoji + too long
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": {"title": bad_title}, "error": None},
        )
        cfg = _make_cfg()

        with caplog.at_level(logging.WARNING):
            result = generate_title("summary", self._best_opt(), cfg)

        # Must not raise
        assert isinstance(result, str)
        assert len(result) <= 90
        assert "\U0001f525" not in result
        assert any("WARNING" in r.levelname or r.levelno >= logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# T-05: persist_results
# ---------------------------------------------------------------------------

class TestPersistResults:
    """persist_results(chapter_id, youtube_video_id, title, options, best_label) contracts."""

    def _make_options(self):
        return [
            {
                "label": "option_a",
                "output_url": "https://pikzels.com/a.png",
                "local_path": "/opt/airflow/data/thumbnails/vid/option_a.png",
                "main_score": 72.0,
                "style": "style A",
                "prompt": "prompt A",
            },
            {
                "label": "option_b",
                "output_url": "https://pikzels.com/b.png",
                "local_path": "/opt/airflow/data/thumbnails/vid/option_b.png",
                "main_score": 85.0,
                "style": "style B",
                "prompt": "prompt B",
            },
        ]

    def test_both_options_produce_upsert_calls(self, mock_psycopg2_connection):
        """Both options are persisted (2 execute calls total)."""
        from congress_videos.modules.thumbnail_generation import persist_results

        mock_connect, mock_conn, mock_cursor = mock_psycopg2_connection
        options = self._make_options()
        persist_results(7, "vid123", "Un título potente", options, "option_b")

        assert mock_cursor.execute.call_count == 2

    def test_chosen_option_has_title_and_is_chosen_true(self, mock_psycopg2_connection):
        """Chosen row is called with openai_title=title and is_chosen=True."""
        from congress_videos.modules.thumbnail_generation import persist_results

        mock_connect, mock_conn, mock_cursor = mock_psycopg2_connection
        options = self._make_options()
        title = "Un título potente"
        persist_results(7, "vid123", title, options, "option_b")

        # Inspect all execute calls
        all_calls = mock_cursor.execute.call_args_list
        # Find the call for option_b (chosen)
        chosen_params = None
        for c in all_calls:
            params = c[0][1] if len(c[0]) > 1 else c[1].get("params", c[0][0])
            # params is a tuple; check if it contains 'option_b'
            if isinstance(params, (list, tuple)) and "option_b" in params:
                chosen_params = params
                break
        assert chosen_params is not None, "No execute call found for option_b"
        # title and True should be in params
        assert title in chosen_params
        assert True in chosen_params

    def test_non_chosen_option_has_null_title_and_is_chosen_false(self, mock_psycopg2_connection):
        """Non-chosen row has openai_title=None and is_chosen=False."""
        from congress_videos.modules.thumbnail_generation import persist_results

        mock_connect, mock_conn, mock_cursor = mock_psycopg2_connection
        options = self._make_options()
        persist_results(7, "vid123", "Un título", options, "option_b")

        all_calls = mock_cursor.execute.call_args_list
        non_chosen_params = None
        for c in all_calls:
            params = c[0][1] if len(c[0]) > 1 else c[1].get("params", c[0][0])
            if isinstance(params, (list, tuple)) and "option_a" in params:
                non_chosen_params = params
                break
        assert non_chosen_params is not None, "No execute call found for option_a"
        assert None in non_chosen_params
        assert False in non_chosen_params

    def test_local_path_in_params(self, mock_psycopg2_connection):
        """Both rows include their respective local_path values."""
        from congress_videos.modules.thumbnail_generation import persist_results

        mock_connect, mock_conn, mock_cursor = mock_psycopg2_connection
        options = self._make_options()
        persist_results(7, "vid123", "Título", options, "option_a")

        all_calls_flat = [
            c[0][1] if len(c[0]) > 1 else ()
            for c in mock_cursor.execute.call_args_list
        ]
        all_params = [item for row in all_calls_flat for item in row]
        assert "/opt/airflow/data/thumbnails/vid/option_a.png" in all_params
        assert "/opt/airflow/data/thumbnails/vid/option_b.png" in all_params


# ---------------------------------------------------------------------------
# T-06: TRIANGULATE edge cases
# ---------------------------------------------------------------------------

class TestTriangulateEdgeCases:
    """Edge-case triangulation for generate_and_score_options and choose_best_option."""

    def test_pikzels_exception_propagates(self, mocker, tmp_path):
        """When thumbnail_from_text raises, the exception propagates to caller."""
        from congress_videos.modules.thumbnail_generation import generate_and_score_options

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.thumbnail_from_text",
            side_effect=RuntimeError("Pikzels API error"),
        )
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.get_thumbnail_dir",
            return_value=tmp_path / "v",
        )
        cfg = _make_cfg()
        with pytest.raises(RuntimeError, match="Pikzels API error"):
            generate_and_score_options("summary", "b64==", cfg, "vid1")

    def test_resolve_photo_http_non_200_falls_back_to_logo(self, tmp_path):
        """When HTTP GET for photo returns non-200, fallback to party logo."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        logo_file = tmp_path / "logo.png"
        logo_bytes = b"\x89PNG" + b"\xaa" * 16
        logo_file.write_bytes(logo_bytes)

        participant = {"normalized_name": "garcia_maria", "photo_url": "https://example.com/broken.jpg"}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=str(logo_file))

        mock_resp = MagicMock()
        mock_resp.status_code = 404
        mock_resp.content = b""

        with patch("requests.get", return_value=mock_resp):
            result = resolve_participant_photo("garcia_maria", cfg)

        assert result["source"] == "party_logo"
        assert result["support_image_b64"] == base64.b64encode(logo_bytes).decode()

    def test_choose_best_with_more_than_two_options(self):
        """choose_best_option with 3 options still picks the highest score."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {"label": "a", "main_score": 60.0},
            {"label": "b", "main_score": 95.0},
            {"label": "c", "main_score": 70.0},
        ]
        best = choose_best_option(options)
        assert best["label"] == "b"

    def test_resolve_photo_request_exception_falls_back_to_logo(self, tmp_path):
        """When requests.get raises RequestException, fallback to party logo."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo
        import requests as req

        logo_file = tmp_path / "logo.png"
        logo_bytes = b"\x89PNG" + b"\xbb" * 16
        logo_file.write_bytes(logo_bytes)

        participant = {"normalized_name": "garcia_maria", "photo_url": "https://example.com/broken.jpg"}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=str(logo_file))

        with patch("requests.get", side_effect=req.RequestException("connection refused")):
            result = resolve_participant_photo("garcia_maria", cfg)

        assert result["source"] == "party_logo"
        assert result["support_image_b64"] == base64.b64encode(logo_bytes).decode()

    def test_generate_title_both_calls_return_none_sanitises(self, mocker, caplog):
        """When generate_json_completion returns error on both calls, sanitised empty string returned."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": None, "error": "API error"},
        )
        cfg = _make_cfg()
        best = {
            "label": "option_a", "style": "s", "persona": "p", "prompt": "pr",
            "output_url": "u", "local_path": "/a.png", "main_score": 80.0,
        }
        with caplog.at_level(logging.WARNING):
            result = generate_title("summary", best, cfg)

        assert isinstance(result, str)
        assert len(result) <= 90

    def test_choose_best_preserves_all_fields(self):
        """choose_best_option returns the full original dict plus is_chosen=True."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {"label": "a", "main_score": 50.0, "local_path": "/a.png", "style": "s"},
            {"label": "b", "main_score": 99.0, "local_path": "/b.png", "style": "s2"},
        ]
        best = choose_best_option(options)
        assert best["local_path"] == "/b.png"
        assert best["style"] == "s2"
        assert best["is_chosen"] is True
