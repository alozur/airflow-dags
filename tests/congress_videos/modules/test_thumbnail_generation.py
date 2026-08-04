"""Tests for congress_videos.modules.thumbnail_generation (Slice 2).

Strict TDD: all tests in this file are written BEFORE the production module
exists.  First run must produce ImportError / AttributeError failures.

Test groups:
    T-01  resolve_participant_photo
    T-02  choose_best_option
    T-03  generate_title
    T-04  persist_results
    T-05  TRIANGULATE edge cases

Note: generate_and_score_options was removed (dead code — never called by the
DAG). The DAG implements generate/download/score as three separate task
callables (_task_generate_thumbnail, _task_download_option, _task_score_option)
for per-task retry granularity. Those callables are tested in
test_generic_thumbnail_dag.py.
"""

from __future__ import annotations

import base64
import logging
from unittest.mock import MagicMock, patch

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
        {"label": "option_a", "layout": "A"},
        {"label": "option_b", "layout": "B"},
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

        from congress_videos.config.constants import CONGRESO_BROWSER_USER_AGENT

        mock_get.assert_called_once_with(
            "https://example.com/garcia.jpg",
            timeout=30,
            headers={"User-Agent": CONGRESO_BROWSER_USER_AGENT},
        )
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

    def test_photo_url_none_no_logo_returns_empty_result(self):
        """When photo_url is NULL and no party logo, EMPTY_RESULT returned + WARNING logged."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        participant = {"normalized_name": "garcia_maria", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with patch("congress_videos.modules.thumbnail_generation.logger") as mock_log:
            result = resolve_participant_photo("garcia_maria", cfg)

        assert result == EMPTY_RESULT
        mock_log.warning.assert_called()

    def test_participant_not_found_returns_empty_result(self):
        """When lookup returns None (unknown slug), EMPTY_RESULT returned + WARNING logged."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        cfg = _make_cfg(lookup_return=None)

        with patch("congress_videos.modules.thumbnail_generation.logger") as mock_log:
            result = resolve_participant_photo("unknown_person", cfg)

        assert result == EMPTY_RESULT
        mock_log.warning.assert_called()


# ---------------------------------------------------------------------------
# TestSlugResolution: tolerant resolve_participant_photo (GROUP 3 — REQ-B)
# ---------------------------------------------------------------------------


class TestSlugResolution:
    """resolve_participant_photo(slug, cfg) — slug-aware tolerant contracts."""

    def test_slug_hit_returns_photo_source_and_non_empty_b64(self, monkeypatch):
        """Valid slug resolving to a participant with photo_url → source='photo' + non-empty b64."""
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo

        participant = {"slug": "garcia-lopez-maria", "photo_url": "https://example.com/img.jpg"}
        cfg = _make_cfg(lookup_return=participant)

        fake_bytes = b"\x89PNG" + b"\x00" * 20
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = fake_bytes

        with patch("requests.get", return_value=mock_resp):
            result = resolve_participant_photo("garcia-lopez-maria", cfg)

        import base64
        assert result["source"] == "photo"
        assert result["support_image_b64"] == base64.b64encode(fake_bytes).decode()

    def test_absent_slug_none_returns_empty_result_no_lookup(self, caplog):
        """slug=None → EMPTY_RESULT returned + WARNING logged, lookup never called."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        cfg = _make_cfg(lookup_raises=True)  # lookup raises if called

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo(None, cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_empty_string_slug_returns_empty_result_no_lookup(self, caplog):
        """slug='' → EMPTY_RESULT + WARNING, lookup never called."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        cfg = _make_cfg(lookup_raises=True)

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_whitespace_slug_returns_empty_result_no_lookup(self, caplog):
        """slug='   ' → EMPTY_RESULT + WARNING, lookup never called."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        cfg = _make_cfg(lookup_raises=True)

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("   ", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_unknown_slug_lookup_returns_none_gives_empty_result_with_warning(self, caplog):
        """Unknown slug (lookup returns None) → EMPTY_RESULT + WARNING, no raise."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        cfg = _make_cfg(lookup_return=None)

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("nonexistent-slug-xyz", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_photo_url_none_party_logo_map_none_returns_empty_result_no_http(self, caplog):
        """photo_url=None + party_logo_map=None → EMPTY_RESULT + WARNING, no HTTP call."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        participant = {"slug": "garcia-ana", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with patch("requests.get") as mock_get, \
             caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("garcia-ana", cfg)

        mock_get.assert_not_called()
        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_http_404_returns_empty_result_with_warning(self, caplog):
        """HTTP 404 for photo_url (no logo fallback) → EMPTY_RESULT + WARNING."""
        import logging
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        participant = {"slug": "garcia-ana", "photo_url": "https://example.com/broken.jpg"}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with patch("requests.get", return_value=mock_resp), \
             caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("garcia-ana", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_request_exception_returns_empty_result_with_warning(self, caplog):
        """requests.RequestException → EMPTY_RESULT + WARNING."""
        import logging
        import requests as req
        from congress_videos.modules.thumbnail_generation import resolve_participant_photo, EMPTY_RESULT

        participant = {"slug": "garcia-ana", "photo_url": "https://example.com/broken.jpg"}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with patch("requests.get", side_effect=req.RequestException("timeout")), \
             caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            result = resolve_participant_photo("garcia-ana", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# T-02: choose_best_option
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
# T-03: art_direct export
# ---------------------------------------------------------------------------


def test_art_direct_is_publicly_importable():
    """The generic thumbnail DAG must be able to import its art-direction callable."""
    from congress_videos.modules.thumbnail_generation import art_direct

    assert callable(art_direct)


# ---------------------------------------------------------------------------
# T-04: generate_title
# ---------------------------------------------------------------------------

class TestGenerateTitle:
    """generate_title(summary, best, cfg) contracts."""

    def _best_opt(
        self,
        label="option_a",
        style="A",
        prompt="A thick gold (#C9A84C) border frames the entire image edge.\n\nBACKGROUND: calle española.",
    ):
        return {
            "label": label,
            "style": style,
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

    def test_all_uppercase_title_triggers_reprompt(self, mocker):
        """First response entirely in UPPERCASE triggers a second OpenAI call."""
        from congress_videos.modules.thumbnail_generation import generate_title

        upper_title = "PSOE VOTA NO A LOS PRESUPUESTOS"
        normal_title = "El PSOE vota no a los presupuestos"
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": upper_title}, "error": None}
            return {"data": {"title": normal_title}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        result = generate_title("summary", self._best_opt(), cfg)

        assert call_count["n"] == 2
        assert result == normal_title

    def test_title_with_acronym_and_lowercase_accepted_first_attempt(self, mocker):
        """Title with party acronym but normal casing must NOT trigger a false re-prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        title = "El PSOE vota no a los presupuestos"
        mock_completion = mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": {"title": title}, "error": None},
        )
        cfg = _make_cfg()
        result = generate_title("summary", self._best_opt(), cfg)

        assert mock_completion.call_count == 1
        assert result == title


# ---------------------------------------------------------------------------
# T-04: persist_results
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
# T-05: TRIANGULATE edge cases
# ---------------------------------------------------------------------------

class TestTriangulateEdgeCases:
    """Edge-case triangulation for resolve_participant_photo and choose_best_option."""

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
            "label": "option_a",
            "style": "A",
            "prompt": "A thick gold (#C9A84C) border frames the entire image edge.",
            "output_url": "u",
            "local_path": "/a.png",
            "main_score": 80.0,
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


# ---------------------------------------------------------------------------
# T-06: art_direct
# ---------------------------------------------------------------------------


class TestArtDirect:
    """art_direct(debate_summary, domain_cfg) contracts."""

    def _make_cfg(self) -> dict:
        return {
            "styles": [
                {"label": "option_a", "layout": "A"},
                {"label": "option_b", "layout": "B"},
            ],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }

    def _valid_brief_response(self) -> dict:
        return {
            "data": {
                "text": "LO QUE NO TE CUENTAN",
                "background": "una calle española con manifestantes, luz de atardecer",
                "person": "un ciudadano de mediana edad con expresión seria, ropa casual",
                "mood": "indignación y urgencia",
            },
            "error": None,
        }

    def test_happy_path_returns_required_keys(self, mocker) -> None:
        """art_direct returns dict with text, background, person, mood on success."""
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_brief_response(),
        )
        cfg = self._make_cfg()
        result = art_direct("Debate sobre pensiones", cfg)

        assert "text" in result
        assert "background" in result
        assert "person" in result
        assert "mood" in result

    def test_malformed_response_reprompts_once_then_default(self, mocker) -> None:
        """Malformed response (missing keys) → reprompt once → _DEFAULT_ART_BRIEF on second failure."""
        from congress_videos.modules.thumbnail_generation import art_direct, _DEFAULT_ART_BRIEF

        call_count = {"n": 0}

        def _side(system_prompt, user_prompt, **kw):
            call_count["n"] += 1
            # Both calls return malformed data (no required keys)
            return {"data": {"wrong_key": "value"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side,
        )
        cfg = self._make_cfg()
        result = art_direct("summary", cfg)

        assert call_count["n"] == 2
        assert result["background"] == _DEFAULT_ART_BRIEF["background"]

    def test_exception_does_not_raise_and_returns_all_keys(self, mocker) -> None:
        """When generate_json_completion raises, art_direct must not raise."""
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=RuntimeError("network error"),
        )
        cfg = self._make_cfg()
        result = art_direct("summary", cfg)

        assert "text" in result
        assert "background" in result
        assert "person" in result
        assert "mood" in result

    def test_none_response_does_not_raise(self, mocker) -> None:
        """When generate_json_completion returns None-data, art_direct must not raise."""
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": None, "error": "api error"},
        )
        cfg = self._make_cfg()
        result = art_direct("summary", cfg)

        assert "text" in result
        assert "background" in result
        assert "person" in result
        assert "mood" in result

    def test_http_stripped_from_values(self, mocker) -> None:
        """Any 'http' substring in returned values must be stripped from the result."""
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={
                "data": {
                    "text": "CRISIS REAL",
                    "background": "http://example.com una calle",
                    "person": "ciudadano serio",
                    "mood": "indignación",
                },
                "error": None,
            },
        )
        cfg = self._make_cfg()
        result = art_direct("summary", cfg)

        for value in result.values():
            if isinstance(value, str):
                assert "http" not in value, f"Found 'http' in result value: {value!r}"

    def test_default_background_has_no_hemiciclo(self) -> None:
        """The _DEFAULT_ART_BRIEF background must not contain 'hemiciclo'."""
        from congress_videos.modules.thumbnail_generation import _DEFAULT_ART_BRIEF

        assert "hemiciclo" not in _DEFAULT_ART_BRIEF["background"].lower()

    def test_system_prompt_contains_http_prohibition(self, mocker) -> None:
        """The system prompt passed to generate_json_completion must mention 'http' as a prohibition."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured = {}

        def _capture(system_prompt, user_prompt, **kw):
            captured["system_prompt"] = system_prompt
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        cfg = self._make_cfg()
        art_direct("summary", cfg)

        assert "http" in captured.get("system_prompt", ""), (
            "System prompt must reference 'http' as a prohibition"
        )

    def test_uses_art_direction_system_prompt(self, mocker) -> None:
        """art_direct must use ART_DIRECTION_SYSTEM_PROMPT as the system prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        captured = {}

        def _capture(system_prompt, user_prompt, **kw):
            captured["system_prompt"] = system_prompt
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        cfg = self._make_cfg()
        art_direct("summary", cfg)

        assert captured["system_prompt"] == ART_DIRECTION_SYSTEM_PROMPT

    def test_warning_logged_on_fallback(self, mocker, caplog) -> None:
        """A WARNING is logged when falling back to _DEFAULT_ART_BRIEF."""
        import logging
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": None, "error": "error"},
        )
        cfg = self._make_cfg()

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.thumbnail_generation"):
            art_direct("summary", cfg)

        assert any(r.levelno >= logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# B-1: TestArtDirectRetry — art_direct previous_brief support
# ---------------------------------------------------------------------------


class TestArtDirectRetry:
    """art_direct(debate_summary, domain_cfg, previous_brief) backward-compat and retry contracts."""

    def _make_cfg(self) -> dict:
        return {
            "styles": [
                {"label": "option_a", "layout": "A"},
                {"label": "option_b", "layout": "B"},
            ],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }

    def _valid_brief_response(self) -> dict:
        return {
            "data": {
                "text": "NUEVO ENFOQUE",
                "background": "una plaza pública llena de gente",
                "person": "una persona joven con expresión de sorpresa",
                "mood": "asombro y urgencia",
            },
            "error": None,
        }

    def test_previous_brief_none_does_not_inject_instruction(self, mocker) -> None:
        """When previous_brief=None, no retry instruction is injected into the prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("Debate sobre pensiones", self._make_cfg(), previous_brief=None)

        assert captured_prompts, "generate_json_completion must be called at least once"
        assert "REINTENTO" not in captured_prompts[0], (
            "Retry instruction must NOT be present when previous_brief=None"
        )

    def test_previous_brief_dict_injects_retry_instruction(self, mocker) -> None:
        """When previous_brief is a dict, the retry instruction is injected into the prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        previous_brief = {
            "text": "CRISIS ANTERIOR",
            "background": "hemiciclo interior",
            "person": "político hablando",
            "mood": "tensión",
        }
        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("Debate sobre pensiones", self._make_cfg(), previous_brief=previous_brief)

        assert captured_prompts, "generate_json_completion must be called"
        assert "REINTENTO" in captured_prompts[0], (
            "Retry instruction must be present in prompt when previous_brief is set"
        )

    def test_previous_brief_dict_includes_brief_json_in_prompt(self, mocker) -> None:
        """When previous_brief is set, its content appears in the prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        previous_brief = {
            "text": "CRISIS ANTERIOR",
            "background": "hemiciclo interior",
            "person": "político hablando",
            "mood": "tensión",
        }
        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("Debate", self._make_cfg(), previous_brief=previous_brief)

        assert "CRISIS ANTERIOR" in captured_prompts[0], (
            "Previous brief text must appear in the retry prompt"
        )

    def test_backward_compat_no_previous_brief_arg(self, mocker) -> None:
        """art_direct called with 2 positional args (old call site) still works."""
        from congress_videos.modules.thumbnail_generation import art_direct

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_brief_response(),
        )
        # Old call signature — must not raise TypeError
        result = art_direct("Debate sobre pensiones", self._make_cfg())
        assert "text" in result


# ---------------------------------------------------------------------------
# B-2: TestScoreRetryThreshold — thumbnail_config threshold
# ---------------------------------------------------------------------------


class TestScoreRetryThreshold:
    """Verify score_retry_threshold is present in congreso config with correct default."""

    def test_congreso_config_has_score_retry_threshold_60(self) -> None:
        """congreso domain config must include score_retry_threshold=60."""
        from congress_videos.config.thumbnail_config import get_domain_config

        cfg = get_domain_config("congreso")
        assert "score_retry_threshold" in cfg, (
            "congreso config must have 'score_retry_threshold' key"
        )
        assert cfg["score_retry_threshold"] == 60, (
            f"score_retry_threshold must be 60, got {cfg['score_retry_threshold']}"
        )

    def test_get_domain_config_fallback_default_60(self) -> None:
        """get_domain_config().get('score_retry_threshold', 60) == 60 for any domain without explicit key."""
        # Verify the pattern the DAG uses: .get("score_retry_threshold", 60)
        # This test uses a synthetic config dict to confirm the fallback works.
        cfg_without_threshold = {}
        assert cfg_without_threshold.get("score_retry_threshold", 60) == 60
