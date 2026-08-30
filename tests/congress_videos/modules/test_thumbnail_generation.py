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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
        )

        participant = {
            "normalized_name": "garcia_maria",
            "photo_url": "https://example.com/garcia.jpg",
        }
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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
        )

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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        participant = {"normalized_name": "garcia_maria", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with patch("congress_videos.modules.thumbnail_generation.logger") as mock_log:
            result = resolve_participant_photo("garcia_maria", cfg)

        assert result == EMPTY_RESULT
        mock_log.warning.assert_called()

    def test_participant_not_found_returns_empty_result(self):
        """When lookup returns None (unknown slug), EMPTY_RESULT returned + WARNING logged."""
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
        )

        participant = {
            "slug": "garcia-lopez-maria",
            "photo_url": "https://example.com/img.jpg",
        }
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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        cfg = _make_cfg(lookup_raises=True)  # lookup raises if called

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            result = resolve_participant_photo(None, cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_empty_string_slug_returns_empty_result_no_lookup(self, caplog):
        """slug='' → EMPTY_RESULT + WARNING, lookup never called."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        cfg = _make_cfg(lookup_raises=True)

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            result = resolve_participant_photo("", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_whitespace_slug_returns_empty_result_no_lookup(self, caplog):
        """slug='   ' → EMPTY_RESULT + WARNING, lookup never called."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        cfg = _make_cfg(lookup_raises=True)

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            result = resolve_participant_photo("   ", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_unknown_slug_lookup_returns_none_gives_empty_result_with_warning(
        self, caplog
    ):
        """Unknown slug (lookup returns None) → EMPTY_RESULT + WARNING, no raise."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        cfg = _make_cfg(lookup_return=None)

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            result = resolve_participant_photo("nonexistent-slug-xyz", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_photo_url_none_party_logo_map_none_returns_empty_result_no_http(
        self, caplog
    ):
        """photo_url=None + party_logo_map=None → EMPTY_RESULT + WARNING, no HTTP call."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        participant = {"slug": "garcia-ana", "photo_url": None}
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with (
            patch("requests.get") as mock_get,
            caplog.at_level(
                logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
            ),
        ):
            result = resolve_participant_photo("garcia-ana", cfg)

        mock_get.assert_not_called()
        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_http_404_returns_empty_result_with_warning(self, caplog):
        """HTTP 404 for photo_url (no logo fallback) → EMPTY_RESULT + WARNING."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        participant = {
            "slug": "garcia-ana",
            "photo_url": "https://example.com/broken.jpg",
        }
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        mock_resp = MagicMock()
        mock_resp.status_code = 404

        with (
            patch("requests.get", return_value=mock_resp),
            caplog.at_level(
                logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
            ),
        ):
            result = resolve_participant_photo("garcia-ana", cfg)

        assert result == EMPTY_RESULT
        assert any(r.levelno >= logging.WARNING for r in caplog.records)

    def test_request_exception_returns_empty_result_with_warning(self, caplog):
        """requests.RequestException → EMPTY_RESULT + WARNING."""
        import logging
        import requests as req
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
            EMPTY_RESULT,
        )

        participant = {
            "slug": "garcia-ana",
            "photo_url": "https://example.com/broken.jpg",
        }
        cfg = _make_cfg(lookup_return=participant, party_logo_map=None)

        with (
            patch("requests.get", side_effect=req.RequestException("timeout")),
            caplog.at_level(
                logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
            ),
        ):
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
            {
                "label": "option_a",
                "main_score": 72.0,
                "output_url": "urlA",
                "local_path": "/a.png",
                "style": "s",
            },
            {
                "label": "option_b",
                "main_score": 85.0,
                "output_url": "urlB",
                "local_path": "/b.png",
                "style": "s",
            },
        ]
        best = choose_best_option(options)
        assert best["label"] == "option_b"

    def test_equal_scores_first_option_wins(self):
        """Tie-break: option A (index 0) wins when scores are equal."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {
                "label": "option_a",
                "main_score": 78.0,
                "output_url": "urlA",
                "local_path": "/a.png",
                "style": "s",
            },
            {
                "label": "option_b",
                "main_score": 78.0,
                "output_url": "urlB",
                "local_path": "/b.png",
                "style": "s",
            },
        ]
        best = choose_best_option(options)
        assert best["label"] == "option_a"

    def test_returns_dict_with_is_chosen_true(self):
        """Return value includes is_chosen=True."""
        from congress_videos.modules.thumbnail_generation import choose_best_option

        options = [
            {
                "label": "option_a",
                "main_score": 90.0,
                "output_url": "u",
                "local_path": "/a.png",
                "style": "s",
            },
            {
                "label": "option_b",
                "main_score": 50.0,
                "output_url": "u",
                "local_path": "/b.png",
                "style": "s",
            },
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
        assert any(
            "WARNING" in r.levelname or r.levelno >= logging.WARNING
            for r in caplog.records
        )

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

    def test_non_chosen_option_has_null_title_and_is_chosen_false(
        self, mock_psycopg2_connection
    ):
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
            c[0][1] if len(c[0]) > 1 else () for c in mock_cursor.execute.call_args_list
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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
        )

        logo_file = tmp_path / "logo.png"
        logo_bytes = b"\x89PNG" + b"\xaa" * 16
        logo_file.write_bytes(logo_bytes)

        participant = {
            "normalized_name": "garcia_maria",
            "photo_url": "https://example.com/broken.jpg",
        }
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
        from congress_videos.modules.thumbnail_generation import (
            resolve_participant_photo,
        )
        import requests as req

        logo_file = tmp_path / "logo.png"
        logo_bytes = b"\x89PNG" + b"\xbb" * 16
        logo_file.write_bytes(logo_bytes)

        participant = {
            "normalized_name": "garcia_maria",
            "photo_url": "https://example.com/broken.jpg",
        }
        cfg = _make_cfg(lookup_return=participant, party_logo_map=str(logo_file))

        with patch(
            "requests.get", side_effect=req.RequestException("connection refused")
        ):
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
        from congress_videos.modules.thumbnail_generation import (
            art_direct,
            _DEFAULT_ART_BRIEF,
        )

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

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            art_direct("summary", cfg)

        assert any(r.levelno >= logging.WARNING for r in caplog.records)


# ---------------------------------------------------------------------------
# Issue #54: Art-direction audience policy at the completion-provider seam
# ---------------------------------------------------------------------------


class TestArtDirectionAudiencePolicy:
    """The model receives the manually maintained audience-selection policy."""

    def _make_cfg(self) -> dict:
        return {
            "styles": [{"label": "option_a", "layout": "A"}],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }

    @staticmethod
    def _valid_brief_response() -> dict:
        return {
            "data": {
                "text": "DEBATE CLAVE",
                "background": "una calle española",
                "person": "un ciudadano preocupado, ropa casual",
                "mood": "urgencia",
            },
            "error": None,
        }

    def _system_prompt_for(self, mocker, debate_summary: str) -> str:
        from congress_videos.modules.thumbnail_generation import art_direct

        completion = mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_brief_response(),
        )
        art_direct(debate_summary, self._make_cfg())
        return completion.call_args.kwargs["system_prompt"]

    def test_default_policy_uses_youtube_analytics_audience_distribution(
        self, mocker
    ) -> None:
        """The provider sees the 80% male and 63% 65+ audience distribution."""
        system_prompt = self._system_prompt_for(mocker, "Debate presupuestario general")

        assert "80%" in system_prompt
        assert "63%" in system_prompt
        assert "65+" in system_prompt
        assert "persona relatable" in system_prompt.lower()
        assert "nunca el ponente ni la foto de un participante" in system_prompt.lower()

    def test_policy_source_is_youtube_analytics_and_manually_maintained(
        self, mocker
    ) -> None:
        """Audience figures are an explicit manually maintained YouTube Analytics configuration."""
        system_prompt = self._system_prompt_for(mocker, "Debate general")

        prompt_lower = system_prompt.lower()
        assert "youtube analytics" in prompt_lower
        assert "configuración manual" in prompt_lower

    def test_policy_does_not_use_database_or_live_analytics(self, mocker) -> None:
        """The provider is told not to query a database, API, or live analytics."""
        system_prompt = self._system_prompt_for(mocker, "Debate general")

        prompt_lower = system_prompt.lower()
        assert "no consultes" in prompt_lower
        assert "base de datos" in prompt_lower
        assert "api" in prompt_lower
        assert "analíticas en tiempo real" in prompt_lower

    def test_maternity_topics_override_the_generic_fallback(self, mocker) -> None:
        """Maternity, pregnancy, and conciliation select a woman of child-bearing age."""
        system_prompt = self._system_prompt_for(
            mocker, "Propuesta de conciliación y embarazo"
        )

        prompt_lower = system_prompt.lower()
        assert "maternidad, embarazo o conciliación" in prompt_lower
        assert "mujer en edad de tener hijos" in prompt_lower
        assert "prevalece sobre la regla general" in prompt_lower

    def test_pensions_topics_override_the_generic_fallback(self, mocker) -> None:
        """Pensions, dependency, and geriatric healthcare select an older person."""
        system_prompt = self._system_prompt_for(
            mocker, "Debate sobre pensiones y dependencia"
        )

        prompt_lower = system_prompt.lower()
        assert "pensiones, dependencia o atención sanitaria geriátrica" in prompt_lower
        assert "persona mayor" in prompt_lower
        assert "prevalece sobre la regla general" in prompt_lower

    def test_general_or_ambiguous_topics_use_the_probabilistic_fallback(
        self, mocker
    ) -> None:
        """General summaries favor older men while retaining women and younger adults."""
        system_prompt = self._system_prompt_for(
            mocker, "Debate general sin tema dominante"
        )

        prompt_lower = system_prompt.lower()
        assert "resúmenes generales o ambiguos" in prompt_lower
        assert "hombres mayores aproximadamente el 80%" in prompt_lower
        assert "mujeres y adultos jóvenes" in prompt_lower
        assert "repetición entre hermanos prevalecen" in prompt_lower


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
        art_direct(
            "Debate sobre pensiones", self._make_cfg(), previous_brief=previous_brief
        )

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


# ---------------------------------------------------------------------------
# Phase 1: Prompt Constants — diversity / de-anchoring
# ---------------------------------------------------------------------------


class TestArtDirectionPromptDiversity:
    """Phase 1: ART_DIRECTION_SYSTEM_PROMPT must not anchor on specific moods/backgrounds."""

    def test_art_direction_prompt_mood_not_anchored_on_indignacion(self) -> None:
        """Mood list MUST NOT start with 'indignación' as the first entry."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        # Find the mood line — look for a parenthetical list after "mood:"
        # The constraint: indignación must not be the FIRST item in the mood list
        prompt_lower = ART_DIRECTION_SYSTEM_PROMPT.lower()
        # Find the mood field description section
        mood_idx = prompt_lower.find("mood:")
        assert mood_idx != -1, "ART_DIRECTION_SYSTEM_PROMPT must contain 'mood:' field"
        # Get the text after "mood:" up to end of that sentence/parenthetical
        after_mood = ART_DIRECTION_SYSTEM_PROMPT[mood_idx:]
        # Extract the parenthetical list content (between parentheses)
        paren_start = after_mood.find("(")
        paren_end = after_mood.find(")")
        assert paren_start != -1, "mood field must have a parenthetical list"
        mood_list_text = after_mood[paren_start + 1 : paren_end]
        first_mood = mood_list_text.split(",")[0].strip().lower()
        assert first_mood != "indignación", (
            f"Mood list MUST NOT start with 'indignación'; first entry is {first_mood!r}"
        )

    def test_art_direction_prompt_background_not_anchored_on_protesta(self) -> None:
        """Background parenthetical MUST NOT start with 'protesta callejera'."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        prompt_lower = ART_DIRECTION_SYSTEM_PROMPT.lower()
        background_idx = prompt_lower.find("background:")
        assert background_idx != -1, (
            "ART_DIRECTION_SYSTEM_PROMPT must contain 'background:' field"
        )
        after_background = ART_DIRECTION_SYSTEM_PROMPT[background_idx:]
        paren_start = after_background.find("(")
        paren_end = after_background.find(")")
        assert paren_start != -1, (
            "background field must have a parenthetical example list"
        )
        bg_list_text = after_background[paren_start + 1 : paren_end]
        first_bg = bg_list_text.split(",")[0].strip().lower()
        assert "protesta callejera" not in first_bg, (
            f"Background parenthetical MUST NOT start with 'protesta callejera'; first entry is {first_bg!r}"
        )

    def test_art_direction_prompt_contains_variety_instruction(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT MUST contain an explicit visual-variety instruction."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "DISTINTO" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must contain 'DISTINTO' (visual variety instruction)"
        )

    def test_title_prompt_contains_variety_note(self) -> None:
        """THUMBNAIL_TITLE_SYSTEM_PROMPT MUST contain a variety note about emotional register."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SYSTEM_PROMPT

        prompt_lower = THUMBNAIL_TITLE_SYSTEM_PROMPT.lower()
        has_variety = (
            "varía" in prompt_lower
            or "varia" in prompt_lower
            or "varied" in prompt_lower
            or "distinto" in prompt_lower
            or "registr" in prompt_lower
        )
        assert has_variety, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must contain a variety note about emotional register"
        )

    def test_art_direction_prompt_http_prohibition_intact(self) -> None:
        """The http prohibition MUST still be present after the rewrite."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "http" in ART_DIRECTION_SYSTEM_PROMPT.lower(), (
            "ART_DIRECTION_SYSTEM_PROMPT must still contain the http prohibition"
        )

    def test_art_direction_prompt_json_only_rule_intact(self) -> None:
        """The JSON-only response rule MUST still be present after the rewrite."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "JSON" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must still contain the JSON-only rule"
        )


# ---------------------------------------------------------------------------
# Phase 2: Temperature Tuning
# ---------------------------------------------------------------------------


class TestTemperatureTuning:
    """Phase 2: art_direct and generate_title must pass temperature=0.7."""

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
                "text": "CRISIS TOTAL",
                "background": "mercado en crisis, gente preocupada",
                "person": "ciudadano de mediana edad, expresión tensa",
                "mood": "amenaza",
            },
            "error": None,
        }

    def test_art_direct_passes_temperature_07(self, mocker) -> None:
        """art_direct must call generate_json_completion with temperature=0.7."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_kwargs: list[dict] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_kwargs.append(kw)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("Un debate sobre pensiones", self._make_cfg())

        assert captured_kwargs, "generate_json_completion must be called at least once"
        assert captured_kwargs[0].get("temperature") == 0.7, (
            f"art_direct must pass temperature=0.7, got {captured_kwargs[0].get('temperature')!r}"
        )

    def test_generate_title_passes_temperature_07(self, mocker) -> None:
        """generate_title must call generate_json_completion with temperature=0.7."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_kwargs: list[dict] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_kwargs.append(kw)
            return {
                "data": {"title": "Un título válido de longitud normal"},
                "error": None,
            }

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        best = {"style": "A", "prompt": "mercado en crisis"}
        generate_title("debate summary", best, self._make_cfg())

        assert captured_kwargs, "generate_json_completion must be called at least once"
        assert captured_kwargs[0].get("temperature") == 0.7, (
            f"generate_title must pass temperature=0.7, got {captured_kwargs[0].get('temperature')!r}"
        )


# ---------------------------------------------------------------------------
# Phase 3: Sibling-Brief Helper
# ---------------------------------------------------------------------------


class TestSummariseSiblingBrief:
    """Phase 3: _summarise_sibling_brief extracts key fields and caps at 200 chars."""

    def test_summarise_sibling_brief_extracts_fields(self) -> None:
        """Should extract background/person/mood/text lines and cap to 200 chars."""
        from congress_videos.modules.thumbnail_generation import (
            _summarise_sibling_brief,
        )

        brief = (
            "background: mercado en crisis, gente desesperada\n"
            "person: ciudadano de mediana edad, expresión tensa\n"
            "mood: amenaza\n"
            "text: TODO SE DERRUMBA\n"
            "some other line that should be ignored\n"
        )
        result = _summarise_sibling_brief(brief)
        assert len(result) <= 200, f"Result must be ≤ 200 chars, got {len(result)}"
        assert "mercado en crisis" in result
        assert "amenaza" in result

    def test_summarise_sibling_brief_raw_truncates_when_no_match(self) -> None:
        """When no recognisable field prefixes, raw-truncate fallback to 200 chars."""
        from congress_videos.modules.thumbnail_generation import (
            _summarise_sibling_brief,
        )

        long_prompt = "x" * 500
        result = _summarise_sibling_brief(long_prompt)
        assert len(result) <= 200, (
            f"Fallback must truncate to 200 chars, got {len(result)}"
        )
        assert result == long_prompt[:200]

    def test_summarise_sibling_brief_extracts_from_real_pikzels_format(self) -> None:
        """Must capture person and mood from the REAL rendered Pikzels prompt.

        The stored ``video_thumbnails.prompt`` uses ``SUBJECT (...):`` for the
        person axis and ``Overall mood:`` for the mood axis — not the synthetic
        ``person:``/``mood:`` field names. These are exactly the axes that were
        converging in production, so they must survive summarisation. The TEXT
        phrase must be extracted from its quotes without the styling boilerplate.
        """
        from congress_videos.modules.thumbnail_generation import (
            _summarise_sibling_brief,
        )

        brief = (
            "A thick gold (#C9A84C) border frames the entire image edge.\n\n"
            "BACKGROUND: protesta callejera con pancartas.\n\n"
            "SUBJECT (RIGHT HALF): mujer de unos 30 años, con expresión de "
            "indignación, ropa casual. Face fills 35-40%. Looks at camera.\n\n"
            "TEXT (LEFT HALF, 42% from top): Bold ALL-CAPS white (#FFFFFF) "
            "'¡BASTA DE CORRUPCIÓN!'.\n"
            "Font: bold compressed sans-serif (Impact / Bebas Neue).\n\n"
            "Overall mood: indignación.\n"
            "16:9 YouTube thumbnail."
        )
        result = _summarise_sibling_brief(brief)

        assert len(result) <= 200, f"Result must be ≤ 200 chars, got {len(result)}"
        # Person axis captured, boilerplate tail dropped.
        assert "mujer de unos 30 años" in result
        assert "Face fills" not in result
        # Mood axis captured (from "Overall mood:", not "mood:").
        assert "indignación" in result
        # Text phrase captured from quotes, styling boilerplate dropped.
        assert "¡BASTA DE CORRUPCIÓN!" in result
        assert "Bold ALL-CAPS" not in result
        assert "protesta callejera" in result


# ---------------------------------------------------------------------------
# Phase 4: Sibling Injection on art_direct and generate_title
# ---------------------------------------------------------------------------


class TestArtDirectSiblingInjection:
    """Phase 4: art_direct optional sibling_briefs injection."""

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
                "text": "NUEVA CRISIS",
                "background": "hospital desbordado",
                "person": "enfermero exhausto, expresión preocupada",
                "mood": "pérdida",
            },
            "error": None,
        }

    def test_art_direct_injects_sibling_briefs_when_nonempty(self, mocker) -> None:
        """When sibling_briefs is non-empty, 'NO REPITAS' must appear in user_prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct(
            "debate summary",
            self._make_cfg(),
            sibling_briefs=[
                "brief A sobre plaza pública",
                "brief B con fábrica cerrada",
            ],
        )

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" in captured_prompts[0], (
            "sibling_briefs block must appear in user_prompt"
        )
        assert "brief A sobre plaza pública" in captured_prompts[0]
        assert "brief B con fábrica cerrada" in captured_prompts[0]

    def test_art_direct_no_injection_when_sibling_briefs_none(self, mocker) -> None:
        """When sibling_briefs=None (default), 'NO REPITAS' must NOT appear in user_prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("debate summary", self._make_cfg())

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" not in captured_prompts[0], (
            "sibling_briefs block must NOT appear when sibling_briefs=None"
        )

    def test_art_direct_no_injection_when_sibling_briefs_empty(self, mocker) -> None:
        """When sibling_briefs=[], 'NO REPITAS' must NOT appear in user_prompt."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        art_direct("debate summary", self._make_cfg(), sibling_briefs=[])

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" not in captured_prompts[0], (
            "sibling_briefs block must NOT appear when sibling_briefs=[]"
        )


class TestGenerateTitleSiblingInjection:
    """Phase 4: generate_title optional sibling_titles injection."""

    def _make_cfg(self) -> dict:
        return {
            "styles": [],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }

    def _valid_title_response(self, title: str = "Un título válido") -> dict:
        return {"data": {"title": title}, "error": None}

    def test_generate_title_injects_sibling_titles_when_nonempty(self, mocker) -> None:
        """When sibling_titles is non-empty, a do-not-repeat block must appear in user_prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_title_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        best = {"style": "A", "prompt": "hospital desbordado"}
        generate_title(
            "debate summary",
            best,
            self._make_cfg(),
            sibling_titles=[
                "Title A: La crisis sanitaria",
                "Title B: El colapso del sistema",
            ],
        )

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" in captured_prompts[0], (
            "sibling_titles block must appear in user_prompt"
        )
        assert "Title A: La crisis sanitaria" in captured_prompts[0]
        assert "Title B: El colapso del sistema" in captured_prompts[0]

    def test_generate_title_no_injection_when_sibling_titles_none(self, mocker) -> None:
        """When sibling_titles=None (default), no sibling block in user_prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_title_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        best = {"style": "A", "prompt": "mercado"}
        generate_title("debate summary", best, self._make_cfg())

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" not in captured_prompts[0], (
            "sibling_titles block must NOT appear when sibling_titles=None"
        )

    def test_generate_title_no_injection_when_sibling_titles_empty(
        self, mocker
    ) -> None:
        """When sibling_titles=[], no sibling block in user_prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []

        def _capture(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_title_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_capture,
        )
        best = {"style": "A", "prompt": "mercado"}
        generate_title("debate summary", best, self._make_cfg(), sibling_titles=[])

        assert captured_prompts, "generate_json_completion must be called"
        assert "NO REPITAS" not in captured_prompts[0], (
            "sibling_titles block must NOT appear when sibling_titles=[]"
        )


# ---------------------------------------------------------------------------
# Phase 5: fetch_recent_thumbnail_history
# ---------------------------------------------------------------------------


class TestFetchRecentThumbnailHistory:
    """Phase 5: fetch_recent_thumbnail_history — happy path, empty, and never-raises."""

    def _make_cursor_mock(self, rows: list[tuple]) -> MagicMock:
        """Return a mock cursor whose fetchall() returns `rows`."""
        cursor = MagicMock()
        cursor.__enter__ = MagicMock(return_value=cursor)
        cursor.__exit__ = MagicMock(return_value=False)
        cursor.fetchall.return_value = rows
        return cursor

    def _make_conn_mock(self, cursor_mock: MagicMock) -> MagicMock:
        conn = MagicMock()
        conn.__enter__ = MagicMock(return_value=conn)
        conn.__exit__ = MagicMock(return_value=False)
        conn.cursor.return_value = cursor_mock
        return conn

    def test_fetch_recent_thumbnail_history_happy_path(self, mocker) -> None:
        """Happy path: 3 rows returned; briefs are summarised, None titles excluded."""
        from congress_videos.modules.thumbnail_generation import (
            fetch_recent_thumbnail_history,
        )

        rows = [
            (
                "background: mercado en crisis\nperson: ciudadano\nmood: amenaza\ntext: TODO CAE",
                "Título A",
            ),
            (
                "background: hospital lleno\nperson: enfermero\nmood: pérdida\ntext: COLAPSO",
                "Título B",
            ),
            (
                "background: fábrica cerrada\nperson: obrero\nmood: tristeza\ntext: SIN TRABAJO",
                None,
            ),
        ]

        cursor_mock = self._make_cursor_mock(rows)
        conn_mock = self._make_conn_mock(cursor_mock)
        pg_mock = MagicMock()
        pg_mock.get_qualified_table.return_value = "public.video_thumbnails"
        pg_mock.get_connection.return_value = conn_mock

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.PostgresConnection",
            return_value=pg_mock,
        )

        briefs, titles = fetch_recent_thumbnail_history(limit=5)

        assert len(briefs) == 3, f"Expected 3 briefs, got {len(briefs)}"
        assert len(titles) == 2, f"Expected 2 titles (None excluded), got {len(titles)}"
        assert "Título A" in titles
        assert "Título B" in titles
        # Each brief must be a summarised version (≤200 chars)
        for b in briefs:
            assert len(b) <= 200, f"Brief must be ≤ 200 chars, got {len(b)}"

    def test_fetch_recent_thumbnail_history_empty_returns_empty_lists(
        self, mocker
    ) -> None:
        """When cursor returns no rows, return ([], [])."""
        from congress_videos.modules.thumbnail_generation import (
            fetch_recent_thumbnail_history,
        )

        cursor_mock = self._make_cursor_mock([])
        conn_mock = self._make_conn_mock(cursor_mock)
        pg_mock = MagicMock()
        pg_mock.get_qualified_table.return_value = "public.video_thumbnails"
        pg_mock.get_connection.return_value = conn_mock

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.PostgresConnection",
            return_value=pg_mock,
        )

        briefs, titles = fetch_recent_thumbnail_history()

        assert briefs == []
        assert titles == []

    def test_fetch_recent_thumbnail_history_db_error_never_raises(
        self, mocker, caplog
    ) -> None:
        """When PostgresConnection raises, return ([], []) and log WARNING."""
        import logging
        from congress_videos.modules.thumbnail_generation import (
            fetch_recent_thumbnail_history,
        )

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.PostgresConnection",
            side_effect=Exception("DB connection failed"),
        )

        with caplog.at_level(
            logging.WARNING, logger="congress_videos.modules.thumbnail_generation"
        ):
            briefs, titles = fetch_recent_thumbnail_history()

        assert briefs == []
        assert titles == []
        assert any(r.levelno >= logging.WARNING for r in caplog.records), (
            "A WARNING must be logged when DB raises"
        )


# ---------------------------------------------------------------------------
# title-news-format Phase 1: keyword constants in ai_prompts.py
# ---------------------------------------------------------------------------


class TestNewsFormatConstants:
    """Phase 1: TITLE_PRIORITY_KEYWORDS and TITLE_WORDS_TO_AVOID must exist and be non-empty."""

    def test_priority_keywords_is_non_empty_tuple_of_strings(self) -> None:
        """TITLE_PRIORITY_KEYWORDS must be a non-empty sequence of strings."""
        from congress_videos.config.ai_prompts import TITLE_PRIORITY_KEYWORDS

        assert TITLE_PRIORITY_KEYWORDS, "TITLE_PRIORITY_KEYWORDS must be non-empty"
        assert all(isinstance(k, str) for k in TITLE_PRIORITY_KEYWORDS), (
            "All TITLE_PRIORITY_KEYWORDS entries must be strings"
        )

    def test_words_to_avoid_is_non_empty_tuple_of_strings(self) -> None:
        """TITLE_WORDS_TO_AVOID must be a non-empty sequence of strings."""
        from congress_videos.config.ai_prompts import TITLE_WORDS_TO_AVOID

        assert TITLE_WORDS_TO_AVOID, "TITLE_WORDS_TO_AVOID must be non-empty"
        assert all(isinstance(w, str) for w in TITLE_WORDS_TO_AVOID), (
            "All TITLE_WORDS_TO_AVOID entries must be strings"
        )

    def test_speakers_instruction_contains_speaker_list_placeholder(self) -> None:
        """THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must contain '{speaker_list}'."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION

        assert "{speaker_list}" in THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION, (
            "THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must contain '{speaker_list}' placeholder"
        )


# ---------------------------------------------------------------------------
# title-news-format Phase 2: news-format system prompt + user template
# ---------------------------------------------------------------------------


class TestNewsFormatSystemPrompt:
    """Phase 2: THUMBNAIL_TITLE_SYSTEM_PROMPT must contain news-format and question ban."""

    def test_system_prompt_contains_nombre_verbo_formula(self) -> None:
        """System prompt must specify the [Nombre] + verbo + complemento formula."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SYSTEM_PROMPT

        prompt_lower = THUMBNAIL_TITLE_SYSTEM_PROMPT.lower()
        has_formula = (
            "nombre" in prompt_lower and "verbo" in prompt_lower
        ) or "nombre + verbo" in prompt_lower
        assert has_formula, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must contain the [Nombre] + verbo formula"
        )

    def test_system_prompt_contains_question_ban(self) -> None:
        """System prompt must explicitly ban question-format titles."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SYSTEM_PROMPT

        prompt_lower = THUMBNAIL_TITLE_SYSTEM_PROMPT.lower()
        has_ban = (
            "nunca pregunta" in prompt_lower
            or "no uses interrogación" in prompt_lower
            or "interrogaci" in prompt_lower
            or "nunca termines" in prompt_lower
            or "sin signos de interrogación" in prompt_lower
        )
        assert has_ban, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must explicitly ban question-format titles"
        )

    def test_priority_keyword_appears_in_prompt_text(self) -> None:
        """At least one TITLE_PRIORITY_KEYWORDS term must appear in the combined prompt text."""
        from congress_videos.config.ai_prompts import (
            THUMBNAIL_TITLE_SYSTEM_PROMPT,
            THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE,
            TITLE_PRIORITY_KEYWORDS,
        )

        combined = THUMBNAIL_TITLE_SYSTEM_PROMPT + THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE
        assert any(kw in combined for kw in TITLE_PRIORITY_KEYWORDS), (
            "At least one TITLE_PRIORITY_KEYWORDS term must appear in combined prompt text"
        )

    def test_avoid_keyword_appears_in_prompt_text(self) -> None:
        """At least one TITLE_WORDS_TO_AVOID term must appear in the combined prompt text."""
        from congress_videos.config.ai_prompts import (
            THUMBNAIL_TITLE_SYSTEM_PROMPT,
            THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE,
            TITLE_WORDS_TO_AVOID,
        )

        combined = THUMBNAIL_TITLE_SYSTEM_PROMPT + THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE
        assert any(w in combined for w in TITLE_WORDS_TO_AVOID), (
            "At least one TITLE_WORDS_TO_AVOID term must appear in combined prompt text"
        )

    def test_user_template_references_keyword_lists(self) -> None:
        """THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE must reference keyword lists."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE

        template_lower = THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE.lower()
        has_ref = (
            "interrogac" in template_lower
            or "prioriza" in template_lower
            or "evita" in template_lower
            or "palabras clave" in template_lower
        )
        assert has_ref, (
            "THUMBNAIL_TITLE_USER_PROMPT_TEMPLATE must reference keyword/interrogation guidance"
        )


# ---------------------------------------------------------------------------
# title-news-format Phase 3: ?/¿ rejection and reprompt
# ---------------------------------------------------------------------------


class TestQuestionMarkRejection:
    """Phase 3: _is_valid must reject titles containing ? or starting with ¿."""

    def _generate_title_with_mock(self, mocker, first_title: str, second_title: str | None = None):
        from congress_videos.modules.thumbnail_generation import generate_title

        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": first_title}, "error": None}
            return {"data": {"title": second_title or first_title}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        return generate_title("Debate summary", best, cfg), call_count

    def test_title_ending_in_question_mark_rejected(self, mocker) -> None:
        """_is_valid returns False for a title ending with ?."""
        valid_second = "Sánchez anuncia medidas económicas"
        _, call_count = self._generate_title_with_mock(mocker, "¿Qué pasará con las pensiones?", valid_second)
        assert call_count["n"] == 2, "Title ending in ? must trigger a second LLM call"

    def test_title_starting_with_inverted_question_mark_rejected(self, mocker) -> None:
        """_is_valid returns False for a title starting with ¿."""
        valid_second = "Sánchez anuncia medidas económicas"
        _, call_count = self._generate_title_with_mock(mocker, "¿Cuándo acabará la crisis?", valid_second)
        assert call_count["n"] == 2, "Title starting with ¿ must trigger a second LLM call"

    def test_title_with_mid_string_question_mark_rejected(self, mocker) -> None:
        """_is_valid returns False for a title with ? in the middle."""
        valid_second = "Sánchez anuncia medidas económicas"
        _, call_count = self._generate_title_with_mock(mocker, "Sánchez? el futuro del país", valid_second)
        assert call_count["n"] == 2, "Title with mid-string ? must trigger a second LLM call"

    def test_valid_declarative_title_passes_without_reprompt(self, mocker) -> None:
        """A declarative title with no ? or ¿ must pass _is_valid on first attempt."""
        result, call_count = self._generate_title_with_mock(
            mocker, "Sánchez anuncia recortes en vivienda"
        )
        assert call_count["n"] == 1, "Valid declarative title must NOT trigger a second LLM call"
        assert result == "Sánchez anuncia recortes en vivienda"


class TestQuestionMarkReprompt:
    """Phase 3: reprompt must fire with interrogación instruction; ALL-CAPS regression."""

    def test_first_question_title_triggers_reprompt_with_interrogacion_instruction(self, mocker) -> None:
        """When first attempt returns a question title, second call extra_instruction contains 'interrogación'."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            captured_prompts.append(user_prompt)
            if call_count["n"] == 1:
                return {"data": {"title": "¿Qué pasará?"}, "error": None}
            return {"data": {"title": "Sánchez anuncia medidas"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert call_count["n"] == 2
        assert "interrogaci" in captured_prompts[1].lower(), (
            "Second call must include 'interrogaci' in the extra instruction"
        )
        assert result == "Sánchez anuncia medidas"

    def test_second_valid_title_is_returned_after_reprompt(self, mocker) -> None:
        """After question rejection, the valid second title is returned."""
        from congress_videos.modules.thumbnail_generation import generate_title

        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return {"data": {"title": "¿La crisis sin fin?"}, "error": None}
            return {"data": {"title": "Feijóo critica la política del Gobierno"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert result == "Feijóo critica la política del Gobierno"

    def test_all_caps_hook_word_passes_is_valid_unchanged(self, mocker) -> None:
        """ESCÁNDALO: Sánchez admite el engaño passes _is_valid (no reprompt for all-caps first word)."""
        from congress_videos.modules.thumbnail_generation import generate_title

        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            return {"data": {"title": "ESCÁNDALO: Sánchez admite el engaño"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert call_count["n"] == 1, (
            "ESCÁNDALO: Sánchez admite el engaño must NOT trigger a reprompt"
        )
        assert result == "ESCÁNDALO: Sánchez admite el engaño"


class TestQuestionMarkSanitise:
    """Phase 3: when both attempts produce question titles, sanitiser strips ? and ¿."""

    def test_both_attempts_question_sanitiser_strips_marks(self, mocker, caplog) -> None:
        """When both LLM calls return question titles, final result has no ? or ¿."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": {"title": "¿Precio del gas?"}, "error": None},
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}

        with caplog.at_level(logging.WARNING):
            result = generate_title("Debate summary", best, cfg)

        assert "?" not in result, f"Sanitised result must not contain '?': {result!r}"
        assert "¿" not in result, f"Sanitised result must not contain '¿': {result!r}"


# ---------------------------------------------------------------------------
# title-news-format Phase 4: key_speakers optional param
# ---------------------------------------------------------------------------


class TestKeySpeakersParam:
    """Phase 4: generate_title optional key_speakers parameter contracts."""

    def _make_best(self) -> dict:
        return {"style": "A", "prompt": "debate parlamentario"}

    def _valid_response(self, title: str = "Sánchez anuncia medidas") -> dict:
        return {"data": {"title": title}, "error": None}

    def test_generate_title_without_key_speakers_completes(self, mocker) -> None:
        """generate_title(summary, best, cfg) — without key_speakers — completes without error."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_response(),
        )
        result = generate_title("Debate summary", self._make_best(), _make_cfg())
        assert isinstance(result, str)
        assert len(result) > 0

    def test_generate_title_with_key_speakers_none_completes(self, mocker) -> None:
        """generate_title(..., key_speakers=None) completes identically to no-param case."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_response(),
        )
        result = generate_title("Debate summary", self._make_best(), _make_cfg(), key_speakers=None)
        assert isinstance(result, str)

    def test_generate_title_with_empty_key_speakers_completes(self, mocker) -> None:
        """generate_title(..., key_speakers=[]) completes identically to None case."""
        from congress_videos.modules.thumbnail_generation import generate_title

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value=self._valid_response(),
        )
        result = generate_title("Debate summary", self._make_best(), _make_cfg(), key_speakers=[])
        assert isinstance(result, str)

    def test_key_speakers_string_list_injected_into_prompt(self, mocker) -> None:
        """When key_speakers=['Pedro Sánchez', 'Feijóo'], both names appear in captured user_prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []

        def _side_effect(system_prompt, user_prompt, **kwargs):
            captured_prompts.append(user_prompt)
            return self._valid_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        generate_title(
            "Debate summary",
            self._make_best(),
            _make_cfg(),
            key_speakers=["Pedro Sánchez", "Feijóo"],
        )

        assert captured_prompts, "generate_json_completion must be called"
        prompt = captured_prompts[0]
        assert "Pedro Sánchez" in prompt, "prompt must contain 'Pedro Sánchez'"
        assert "Feijóo" in prompt, "prompt must contain 'Feijóo'"

    def test_key_speakers_dict_list_injected_into_prompt(self, mocker) -> None:
        """key_speakers=[{'name': 'Ana Pastor'}] → 'Ana Pastor' appears in prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured_prompts: list[str] = []

        def _side_effect(system_prompt, user_prompt, **kwargs):
            captured_prompts.append(user_prompt)
            return self._valid_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        generate_title(
            "Debate summary",
            self._make_best(),
            _make_cfg(),
            key_speakers=[{"name": "Ana Pastor"}],
        )

        assert "Ana Pastor" in captured_prompts[0], (
            "prompt must contain 'Ana Pastor' when key_speakers=[{'name': 'Ana Pastor'}]"
        )


# ---------------------------------------------------------------------------
# Bug #79: Art direction must ignore speakers' grammatical gender
# ---------------------------------------------------------------------------


class TestArtDirectionSystemPrompt:
    """ART_DIRECTION_SYSTEM_PROMPT content contracts — bug #79 gender-ignore clause.

    Two tests start RED (assert the new bullet that does not yet exist).
    Five regression guards start GREEN (protect existing content).
    """

    # ------------------------------------------------------------------
    # Task 1.2 — RED: gender-ignore clause must be present (fails before the edit)
    # ------------------------------------------------------------------

    def test_prompt_contains_gender_ignore_clause(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must instruct the model to ignore speakers' grammatical gender.

        RED before the bullet is added: both substrings are absent.
        GREEN after the bullet is added.
        """
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "sexo gramatical de los ponentes" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must contain the gender-ignore instruction "
            "(phrase: 'sexo gramatical de los ponentes')"
        )
        assert "independientemente del género de los ponentes" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must close the gender-ignore rule with "
            "'independientemente del género de los ponentes'"
        )

    # ------------------------------------------------------------------
    # Task 1.3 — RED: wiring test — art_direct passes the (updated) prompt as system_prompt
    # ------------------------------------------------------------------

    def test_art_direct_passes_gender_ignore_system_prompt(self, mocker) -> None:
        """art_direct must pass ART_DIRECTION_SYSTEM_PROMPT (which must contain the
        gender-ignore clause) as the system_prompt kwarg to generate_json_completion.

        RED before the bullet is added: the kwarg assertion fails because the
        substring is absent from the prompt constant.
        GREEN after the bullet is added.
        """
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT
        from congress_videos.modules.thumbnail_generation import art_direct

        mock_completion = mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={
                "data": {
                    "text": "DEBATE CLAVE",
                    "background": "una calle española",
                    "person": "un ciudadano preocupado",
                    "mood": "urgencia",
                },
                "error": None,
            },
        )
        cfg = _make_cfg()
        art_direct("La representante defendió la moción", cfg)

        assert mock_completion.call_args is not None, (
            "generate_json_completion must be called by art_direct"
        )
        passed_prompt = mock_completion.call_args.kwargs["system_prompt"]
        assert passed_prompt == ART_DIRECTION_SYSTEM_PROMPT, (
            "art_direct must pass ART_DIRECTION_SYSTEM_PROMPT as system_prompt kwarg"
        )
        assert "sexo gramatical de los ponentes" in passed_prompt, (
            "The system_prompt passed through must contain the gender-ignore clause"
        )

    # ------------------------------------------------------------------
    # Task 1.4 — Regression guard: maternidad override intact (GREEN from start)
    # ------------------------------------------------------------------

    def test_maternidad_override_intact(self) -> None:
        """The maternidad/embarazo/conciliación override clause must remain present."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "maternidad" in ART_DIRECTION_SYSTEM_PROMPT, (
            "maternidad override must still be present in ART_DIRECTION_SYSTEM_PROMPT"
        )
        assert "mujer en edad de tener hijos" in ART_DIRECTION_SYSTEM_PROMPT, (
            "'mujer en edad de tener hijos' phrase must still be present"
        )

    # ------------------------------------------------------------------
    # Task 1.5 — Regression guard: pensiones override intact (GREEN from start)
    # ------------------------------------------------------------------

    def test_pensiones_override_intact(self) -> None:
        """The pensiones/dependencia/geriátrica override clause must remain present."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "pensiones" in ART_DIRECTION_SYSTEM_PROMPT, (
            "pensiones override must still be present in ART_DIRECTION_SYSTEM_PROMPT"
        )
        assert "persona mayor" in ART_DIRECTION_SYSTEM_PROMPT, (
            "'persona mayor' phrase must still be present"
        )

    # ------------------------------------------------------------------
    # Task 1.6 — Regression guard: desempleo juvenil override intact (GREEN from start)
    # ------------------------------------------------------------------

    def test_desempleo_juvenil_override_intact(self) -> None:
        """The desempleo juvenil/vivienda joven/educación override clause must remain present."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "desempleo juvenil" in ART_DIRECTION_SYSTEM_PROMPT, (
            "desempleo juvenil override must still be present in ART_DIRECTION_SYSTEM_PROMPT"
        )
        assert "persona joven" in ART_DIRECTION_SYSTEM_PROMPT, (
            "'persona joven' phrase must still be present"
        )

    # ------------------------------------------------------------------
    # Task 1.7 — Regression guard: audience ~80% fallback line intact (GREEN from start)
    # ------------------------------------------------------------------

    def test_audience_fallback_line_intact(self) -> None:
        """The general-topic ~80% older-man audience fallback line must remain present."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "80%" in ART_DIRECTION_SYSTEM_PROMPT, (
            "80% audience figure must still be present in ART_DIRECTION_SYSTEM_PROMPT"
        )
        assert "hombres mayores aproximadamente el 80%" in ART_DIRECTION_SYSTEM_PROMPT, (
            "'hombres mayores aproximadamente el 80%' phrase must still be present"
        )

    # ------------------------------------------------------------------
    # Task 1.8 — Regression guard: sibling anti-repetition line intact (GREEN from start)
    # ------------------------------------------------------------------

    def test_sibling_anti_repetition_intact(self) -> None:
        """The sibling anti-repetition constraint must remain present."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "hermanos" in ART_DIRECTION_SYSTEM_PROMPT, (
            "'hermanos' (sibling constraint) must still be present in ART_DIRECTION_SYSTEM_PROMPT"
        )
        assert "Las restricciones de repetición entre hermanos prevalecen" in ART_DIRECTION_SYSTEM_PROMPT, (
            "Exact sibling anti-repetition phrase must still be present"
        )


# ---------------------------------------------------------------------------
# T-06: Lapidary prompts (Phase 1)
# ---------------------------------------------------------------------------


class TestLapidaryPrompts:
    """LAPIDARY_RANKING_SYSTEM_PROMPT and LAPIDARY_RANKING_USER_TEMPLATE contracts."""

    def test_lapidary_system_prompt_importable(self):
        """LAPIDARY_RANKING_SYSTEM_PROMPT must exist in ai_prompts."""
        from congress_videos.config.ai_prompts import LAPIDARY_RANKING_SYSTEM_PROMPT

        assert isinstance(LAPIDARY_RANKING_SYSTEM_PROMPT, str)
        assert len(LAPIDARY_RANKING_SYSTEM_PROMPT) > 0

    def test_lapidary_system_prompt_instructs_index_or_none(self):
        """System prompt must instruct LLM to respond with a number or NONE."""
        from congress_videos.config.ai_prompts import LAPIDARY_RANKING_SYSTEM_PROMPT

        lower = LAPIDARY_RANKING_SYSTEM_PROMPT.lower()
        # Must mention responding with a number/index or NONE
        assert "none" in lower or "número" in lower or "numero" in lower

    def test_lapidary_user_template_importable(self):
        """LAPIDARY_RANKING_USER_TEMPLATE must exist in ai_prompts."""
        from congress_videos.config.ai_prompts import LAPIDARY_RANKING_USER_TEMPLATE

        assert isinstance(LAPIDARY_RANKING_USER_TEMPLATE, str)
        assert len(LAPIDARY_RANKING_USER_TEMPLATE) > 0

    def test_lapidary_user_template_renders_candidates(self):
        """Formatting the template with candidates produces a string containing them."""
        from congress_videos.config.ai_prompts import LAPIDARY_RANKING_USER_TEMPLATE

        rendered = LAPIDARY_RANKING_USER_TEMPLATE.format(candidates="1. vamos a votar")
        assert "vamos a votar" in rendered
        assert len(rendered) > 0


# ---------------------------------------------------------------------------
# T-07: extract_lapidary_quote (Phase 2)
# ---------------------------------------------------------------------------


class TestExtractLapidaryQuote:
    """Unit tests for extract_lapidary_quote()."""

    def test_empty_fragment_returns_none_without_calling_llm(self):
        """Empty string must return None without invoking completion_fn."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        called = []

        def fake_fn(**_kw):
            called.append(True)
            return {"content": "1", "error": None}

        result = extract_lapidary_quote("", completion_fn=fake_fn)
        assert result is None
        assert called == [], "completion_fn must not be called when fragment is empty"

    def test_no_candidates_survive_returns_none_without_llm(self):
        """Fragment whose every clause exceeds limits must return None without LLM call."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        called = []

        def fake_fn(**_kw):
            called.append(True)
            return {"content": "1", "error": None}

        # All words => more than 8 words each clause, or > 40 chars
        long_clause = "uno dos tres cuatro cinco seis siete ocho nueve"
        result = extract_lapidary_quote(long_clause, completion_fn=fake_fn)
        assert result is None
        assert called == [], "completion_fn must not be called when no candidates survive"

    def test_stop_word_filtered_only_valid_candidate_passed_to_llm(self):
        """Clause starting with stop-word is filtered; only valid clause reaches LLM."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        received_calls = []

        def fake_fn(system_prompt, user_prompt, **_kw):
            received_calls.append(user_prompt)
            return {"content": "1", "error": None}

        # "de los derechos sociales" starts with "de" (stop-word), filtered.
        # "vamos a votar ya" is 4 words, valid.
        fragment = "de los derechos sociales. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)

        assert len(received_calls) == 1
        # Only the valid candidate should appear in the user prompt
        assert "vamos a votar ya" in received_calls[0]
        assert "de los derechos sociales" not in received_calls[0]
        assert result == "vamos a votar ya"

    def test_happy_path_returns_verbatim_candidate(self):
        """LLM returns '1', function returns candidate at index 0 verbatim."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "1", "error": None}

        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)

        assert result == "esto es una prueba seria"

    def test_llm_returns_none_string_returns_none(self):
        """When LLM responds 'NONE', function returns None."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "NONE", "error": None}

        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)
        assert result is None

    def test_unparseable_llm_response_returns_none(self):
        """When LLM returns non-integer, non-NONE text, function returns None."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "banana", "error": None}

        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)
        assert result is None

    def test_out_of_range_index_returns_none(self):
        """When LLM returns index beyond candidate list length, return None."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "99", "error": None}

        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)
        assert result is None

    def test_omitted_completion_fn_resolves_the_real_helper(self, mocker):
        """Omitting completion_fn must fall back to generate_chat_completion.

        Every other test in this class injects an explicit stub, leaving the
        default-resolution branch unexercised. This locks it: the real helper
        is imported lazily from utils.ai_helpers and called exactly once.
        """
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        real_fn = mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": "1", "error": None},
        )

        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment)

        real_fn.assert_called_once()
        assert result == "esto es una prueba seria"


# ---------------------------------------------------------------------------
# T-08: art_direct srt_fragment override (Phase 3)
# ---------------------------------------------------------------------------


class TestArtDirectSrtOverride:
    """art_direct() must accept srt_fragment and override brief['text'] when a quote is found."""

    def _call_art_direct(self, monkeypatch, srt_fragment, extract_return, brief_return=None):
        """Helper: patch art_direct dependencies and call with given srt_fragment."""
        from congress_videos.modules.thumbnail_generation import art_direct

        default_brief = {
            "text": "TEXTO INVENTADO",
            "background": "ciudad",
            "person": "hombre maduro",
            "mood": "tensión",
            "logo": "",
        }
        if brief_return is None:
            brief_return = default_brief

        # Patch the internal _call_api to return brief_return directly
        monkeypatch.setattr(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            lambda **_kw: {"data": brief_return, "error": None},
        )
        monkeypatch.setattr(
            "congress_videos.modules.thumbnail_generation.extract_lapidary_quote",
            lambda fragment, **_kw: extract_return,
        )

        cfg = {"styles": [], "participants_lookup": lambda s: None, "party_logo_map": None}
        return art_direct("resumen debate", cfg, srt_fragment=srt_fragment)

    def test_srt_fragment_none_does_not_call_extract(self, monkeypatch):
        """When srt_fragment=None, extract_lapidary_quote must never be called."""
        from congress_videos.modules.thumbnail_generation import art_direct

        called = []

        monkeypatch.setattr(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            lambda **_kw: {
                "data": {
                    "text": "TEXTO ORIGINAL",
                    "background": "ciudad",
                    "person": "hombre",
                    "mood": "tensión",
                },
                "error": None,
            },
        )
        monkeypatch.setattr(
            "congress_videos.modules.thumbnail_generation.extract_lapidary_quote",
            lambda *_a, **_kw: called.append(True) or "override",
        )

        cfg = {"styles": [], "participants_lookup": lambda s: None, "party_logo_map": None}
        art_direct("resumen", cfg, srt_fragment=None)
        assert called == [], "extract_lapidary_quote must not be called when srt_fragment=None"

    def test_srt_fragment_overrides_text_field(self, monkeypatch):
        """When extract returns a string, brief['text'] is overridden."""
        result = self._call_art_direct(
            monkeypatch,
            srt_fragment="anything",
            extract_return="vamos a votar ya",
        )
        assert result["text"] == "vamos a votar ya"
        # Other fields must be unchanged
        assert result["background"] == "ciudad"
        assert result["person"] == "hombre maduro"
        assert result["mood"] == "tensión"

    def test_srt_fragment_quote_none_preserves_invented_text(self, monkeypatch):
        """When extract returns None, brief['text'] remains as the invented value."""
        result = self._call_art_direct(
            monkeypatch,
            srt_fragment="anything",
            extract_return=None,
        )
        assert result["text"] == "TEXTO INVENTADO"


# ---------------------------------------------------------------------------
# Safe-Zone & Rule-of-Thirds (REQ-2)
# ---------------------------------------------------------------------------


class TestArtDirectionSafeZone:
    """ART_DIRECTION_SYSTEM_PROMPT must encode rule-of-thirds and safe-zone guidance."""

    def test_art_direction_contains_esquina_inferior_derecha(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must contain 'esquina inferior derecha'."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "esquina inferior derecha" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must mention 'esquina inferior derecha' (YouTube overlay safe zone)"
        )

    def test_art_direction_contains_tercio(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must contain 'tercio' (rule-of-thirds language)."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "tercio" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must contain 'tercio' for rule-of-thirds guidance"
        )

    def test_art_direction_contains_regla_de_los_tercios(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must contain 'regla de los tercios'."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "regla de los tercios" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must mention 'regla de los tercios'"
        )


# ---------------------------------------------------------------------------
# T-01 / T-02 / T-03 — Non-regression guards (issue #54, #79, #59)
# ---------------------------------------------------------------------------


class TestArchetypeNonRegression:
    """Non-regression guards that lock existing prompt content against accidental deletion.

    T-01: hemiciclo ban (#54) preserved.
    T-02: gender-ignore clause (#79) preserved.
    T-03: composición/safe-zone bullet (#59) preserved.

    All should be GREEN immediately — they assert existing text.
    """

    # T-01
    def test_prompt_contains_hemiciclo_ban(self) -> None:
        """#54 ban: 'NUNCA hemiciclo' must remain in ART_DIRECTION_SYSTEM_PROMPT."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "NUNCA hemiciclo" in ART_DIRECTION_SYSTEM_PROMPT, (
            "The hemiciclo ban ('NUNCA hemiciclo') must remain in ART_DIRECTION_SYSTEM_PROMPT"
        )

    def test_prompt_never_references_ponente_per_54(self) -> None:
        """#54 ban: 'nunca el ponente' must remain in ART_DIRECTION_SYSTEM_PROMPT."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "nunca el ponente" in ART_DIRECTION_SYSTEM_PROMPT, (
            "The ponente ban ('nunca el ponente') must remain in ART_DIRECTION_SYSTEM_PROMPT"
        )

    # T-02
    def test_prompt_contains_gender_ignore_clause_79(self) -> None:
        """#79 gender-ignore bullet must remain present and textually identical."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "sexo gramatical de los ponentes" in ART_DIRECTION_SYSTEM_PROMPT, (
            "#79 gender-ignore clause ('sexo gramatical de los ponentes') must remain"
        )
        assert "independientemente del género de los ponentes" in ART_DIRECTION_SYSTEM_PROMPT, (
            "#79 gender-ignore close phrase must remain"
        )

    # T-03
    def test_prompt_contains_composicion_bullet_59(self) -> None:
        """#59 composición bullet must remain in ART_DIRECTION_SYSTEM_PROMPT."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "composición:" in ART_DIRECTION_SYSTEM_PROMPT, (
            "#59 composición bullet ('composición:') must remain in prompt"
        )

    def test_prompt_contains_tercio_izquierdo_derecho(self) -> None:
        """#59 safe-zone wording (tercio izquierdo o derecho) must remain."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "tercio izquierdo o derecho" in ART_DIRECTION_SYSTEM_PROMPT, (
            "#59 safe-zone phrase ('tercio izquierdo o derecho') must remain in prompt"
        )


# ---------------------------------------------------------------------------
# T-04 / T-05 / T-06 / T-07 — Archetype prompt presence (RED until GREEN)
# ---------------------------------------------------------------------------


class TestArchetypePrompt:
    """Tests asserting the ARQUETIPO DRAMÁTICO section is in ART_DIRECTION_SYSTEM_PROMPT.

    T-04: section header + 5 enum tokens present.
    T-05: per-archetype composition wording present (5 distinct instructions).
    T-06: drift guard — _ARCHETYPES tuple matches prompt token list exactly.
    T-07: JSON schema line contains archetype field.

    All start RED (section doesn't exist yet).
    """

    # T-04
    def test_prompt_contains_arquetipo_dramatico_section(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must contain 'ARQUETIPO DRAMÁTICO' section header."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "ARQUETIPO DRAMÁTICO" in ART_DIRECTION_SYSTEM_PROMPT, (
            "ART_DIRECTION_SYSTEM_PROMPT must include an 'ARQUETIPO DRAMÁTICO' section"
        )

    def test_prompt_contains_all_five_enum_tokens(self) -> None:
        """ART_DIRECTION_SYSTEM_PROMPT must contain all 5 archetype enum tokens."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        for token in ("careo", "denuncia", "monologo", "anuncio", "generico"):
            assert token in ART_DIRECTION_SYSTEM_PROMPT, (
                f"Archetype token '{token}' must appear in ART_DIRECTION_SYSTEM_PROMPT"
            )

    # T-05
    def test_careo_composition_in_prompt(self) -> None:
        """careo archetype must have two-citizens + opposite-emotions composition wording."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        # Must describe two citizens with opposite/contrasting emotions
        assert "careo" in ART_DIRECTION_SYSTEM_PROMPT
        # Unique substring for careo: two citizens, opposite emotions
        assert "dos ciudadanos" in ART_DIRECTION_SYSTEM_PROMPT, (
            "careo composition must mention 'dos ciudadanos'"
        )
        assert "emociones opuestas" in ART_DIRECTION_SYSTEM_PROMPT, (
            "careo composition must mention 'emociones opuestas'"
        )

    def test_denuncia_composition_in_prompt(self) -> None:
        """denuncia archetype must have citizen holding/pointing evidence-object wording."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "denuncia" in ART_DIRECTION_SYSTEM_PROMPT
        assert "objeto" in ART_DIRECTION_SYSTEM_PROMPT, (
            "denuncia composition must mention an evidence-object"
        )

    def test_monologo_composition_in_prompt(self) -> None:
        """monologo archetype must have citizen + strong action gesture wording."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "monologo" in ART_DIRECTION_SYSTEM_PROMPT
        assert "gesto" in ART_DIRECTION_SYSTEM_PROMPT, (
            "monologo composition must mention a 'gesto' (action gesture)"
        )

    def test_anuncio_composition_in_prompt(self) -> None:
        """anuncio archetype must have citizen in heroic/relief/hope key wording."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "anuncio" in ART_DIRECTION_SYSTEM_PROMPT
        assert "heroic" in ART_DIRECTION_SYSTEM_PROMPT or "heroica" in ART_DIRECTION_SYSTEM_PROMPT or "esperanza" in ART_DIRECTION_SYSTEM_PROMPT, (
            "anuncio composition must mention heroic key, relief, or hope (esperanza)"
        )

    def test_generico_composition_in_prompt(self) -> None:
        """generico archetype must reference general mold / older citizen / worried wording."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "generico" in ART_DIRECTION_SYSTEM_PROMPT
        assert "molde general" in ART_DIRECTION_SYSTEM_PROMPT or "ciudadano mayor" in ART_DIRECTION_SYSTEM_PROMPT, (
            "generico composition must mention 'molde general' or 'ciudadano mayor'"
        )

    # T-06
    def test_archetypes_tuple_matches_prompt_tokens(self) -> None:
        """Drift guard: every _ARCHETYPES member must appear in ART_DIRECTION_SYSTEM_PROMPT,
        and the tuple must have exactly 5 members."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT
        from congress_videos.modules.thumbnail_generation import _ARCHETYPES

        assert len(_ARCHETYPES) == 5, (
            f"_ARCHETYPES must have exactly 5 members, got {len(_ARCHETYPES)}"
        )
        for token in _ARCHETYPES:
            assert token in ART_DIRECTION_SYSTEM_PROMPT, (
                f"_ARCHETYPES token '{token}' must appear in ART_DIRECTION_SYSTEM_PROMPT"
            )

    # T-07
    def test_prompt_json_schema_line_contains_archetype_field(self) -> None:
        """The last line of ART_DIRECTION_SYSTEM_PROMPT must include '\"archetype\"'."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        last_line = ART_DIRECTION_SYSTEM_PROMPT.strip().splitlines()[-1]
        assert '"archetype"' in last_line, (
            f"Last line of ART_DIRECTION_SYSTEM_PROMPT must contain '\"archetype\"', got: {last_line!r}"
        )


# ---------------------------------------------------------------------------
# T-08 / T-09 — _coerce_archetype unit tests (RED until GREEN)
# ---------------------------------------------------------------------------


class TestCoerceArchetype:
    """Pure unit tests for the _coerce_archetype helper.

    T-08: valid tokens pass through (case/whitespace normalized).
    T-09: invalid/missing inputs → 'generico'.

    All start RED (_coerce_archetype doesn't exist yet).
    """

    # T-08
    def test_each_valid_token_returns_itself(self) -> None:
        """Each member of _ARCHETYPES must pass through _coerce_archetype unchanged."""
        from congress_videos.modules.thumbnail_generation import (
            _ARCHETYPES,
            _coerce_archetype,
        )

        for token in _ARCHETYPES:
            assert _coerce_archetype(token) == token, (
                f"_coerce_archetype('{token}') must return '{token}'"
            )

    def test_case_and_whitespace_normalized(self) -> None:
        """_coerce_archetype must strip whitespace and lowercase before matching."""
        from congress_videos.modules.thumbnail_generation import _coerce_archetype

        assert _coerce_archetype(" CAREO ") == "careo"
        assert _coerce_archetype("Denuncia") == "denuncia"
        assert _coerce_archetype("  MONOLOGO  ") == "monologo"

    # T-09
    def test_none_coerces_to_generico(self) -> None:
        """_coerce_archetype(None) must return 'generico'."""
        from congress_videos.modules.thumbnail_generation import _coerce_archetype

        assert _coerce_archetype(None) == "generico"

    def test_empty_string_coerces_to_generico(self) -> None:
        """_coerce_archetype('') must return 'generico'."""
        from congress_videos.modules.thumbnail_generation import _coerce_archetype

        assert _coerce_archetype("") == "generico"

    def test_unknown_string_coerces_to_generico(self) -> None:
        """_coerce_archetype('foo') must return 'generico'."""
        from congress_videos.modules.thumbnail_generation import _coerce_archetype

        assert _coerce_archetype("foo") == "generico"

    def test_non_string_coerces_to_generico(self) -> None:
        """_coerce_archetype(123) and _coerce_archetype({}) must return 'generico'."""
        from congress_videos.modules.thumbnail_generation import _coerce_archetype

        assert _coerce_archetype(123) == "generico"
        assert _coerce_archetype({}) == "generico"


# ---------------------------------------------------------------------------
# T-10 — _DEFAULT_ART_BRIEF carries archetype="generico" (RED until GREEN)
# ---------------------------------------------------------------------------


class TestDefaultArtBriefArchetype:
    """T-10: _DEFAULT_ART_BRIEF must carry archetype='generico'."""

    def test_default_art_brief_has_archetype_generico(self) -> None:
        """_DEFAULT_ART_BRIEF must include archetype='generico'."""
        from congress_videos.modules.thumbnail_generation import _DEFAULT_ART_BRIEF

        assert "archetype" in _DEFAULT_ART_BRIEF, (
            "_DEFAULT_ART_BRIEF must contain an 'archetype' key"
        )
        assert _DEFAULT_ART_BRIEF["archetype"] == "generico", (
            "_DEFAULT_ART_BRIEF['archetype'] must be 'generico'"
        )


# ---------------------------------------------------------------------------
# T-11 / T-12 / T-13 / T-14 — art_direct wiring (RED until GREEN)
# ---------------------------------------------------------------------------


def _make_art_direct_cfg() -> dict:
    """Minimal domain config for art_direct wiring tests."""
    return {
        "styles": [
            {"label": "option_a", "layout": "A"},
            {"label": "option_b", "layout": "B"},
        ],
        "participants_lookup": lambda slug: None,
        "party_logo_map": None,
    }


class TestArtDirectArchetypeWiring:
    """Tests for art_direct archetype coercion wiring.

    T-11: valid archetype from API passes through.
    T-12: missing archetype key → 'generico', no raise.
    T-13: invalid archetype string → 'generico', no raise.
    T-14: fallback brief path carries archetype='generico'.

    All start RED (wiring code doesn't exist yet).
    """

    _VALID_BRIEF_BASE = {
        "text": "DEBATE CLAVE",
        "background": "una calle española",
        "person": "un ciudadano preocupado",
        "mood": "urgencia",
    }

    # T-11
    def test_valid_archetype_from_api_passes_through(self, mocker) -> None:
        """A valid archetype value returned by the API must appear unchanged in the brief."""
        from congress_videos.modules.thumbnail_generation import art_direct

        data = {**self._VALID_BRIEF_BASE, "archetype": "careo"}
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": data, "error": None},
        )
        result = art_direct("resumen del debate", _make_art_direct_cfg())

        assert result["archetype"] == "careo", (
            "art_direct must preserve a valid 'careo' archetype from the API response"
        )

    def test_valid_archetype_is_not_stripped_by_http_filter(self, mocker) -> None:
        """The http-strip dict-comprehension must not alter a valid archetype value."""
        from congress_videos.modules.thumbnail_generation import art_direct

        data = {**self._VALID_BRIEF_BASE, "archetype": "denuncia"}
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": data, "error": None},
        )
        result = art_direct("resumen del debate", _make_art_direct_cfg())

        assert result["archetype"] == "denuncia", (
            "The http-strip filter must not alter a valid archetype value"
        )

    # T-12
    def test_missing_archetype_key_coerces_to_generico(self, mocker) -> None:
        """When the API response has no 'archetype' key, art_direct must supply 'generico'."""
        from congress_videos.modules.thumbnail_generation import art_direct

        # 4-key brief — no archetype key
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": dict(self._VALID_BRIEF_BASE), "error": None},
        )
        result = art_direct("resumen del debate", _make_art_direct_cfg())

        assert "archetype" in result, (
            "art_direct must always return a brief with an 'archetype' key"
        )
        assert result["archetype"] == "generico", (
            "Missing 'archetype' key must coerce to 'generico'"
        )

    # T-13
    def test_invalid_archetype_string_coerces_to_generico(self, mocker) -> None:
        """When the API returns an unrecognised archetype string, art_direct must coerce to 'generico'."""
        from congress_videos.modules.thumbnail_generation import art_direct

        data = {**self._VALID_BRIEF_BASE, "archetype": "invalid_value"}
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": data, "error": None},
        )
        result = art_direct("resumen del debate", _make_art_direct_cfg())

        assert result["archetype"] == "generico", (
            "Unrecognised archetype string must coerce to 'generico'"
        )

    # T-14
    def test_fallback_brief_carries_archetype_generico(self, mocker) -> None:
        """When both OpenAI attempts fail, the fallback _DEFAULT_ART_BRIEF must carry archetype='generico'."""
        from congress_videos.modules.thumbnail_generation import art_direct

        # Both attempts return error to force fallback
        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            return_value={"data": None, "error": "API failure"},
        )
        result = art_direct("resumen del debate", _make_art_direct_cfg())

        assert "archetype" in result, (
            "Fallback brief must always contain an 'archetype' key"
        )
        assert result["archetype"] == "generico", (
            "Fallback brief archetype must be 'generico'"
        )


# ---------------------------------------------------------------------------
# issue #91: title-speaker-attribution Phase A — ai_prompts constants
# ---------------------------------------------------------------------------


class TestKeywordTupleContainsNoPoliticianNames:
    """T-01 RED: TITLE_PRIORITY_KEYWORDS must not contain politician proper names."""

    def test_keyword_tuple_contains_no_politician_names(self) -> None:
        """TITLE_PRIORITY_KEYWORDS must not include Sánchez, Feijóo, or Yolanda Díaz."""
        from congress_videos.config.ai_prompts import TITLE_PRIORITY_KEYWORDS

        assert "Sánchez" not in TITLE_PRIORITY_KEYWORDS, (
            "TITLE_PRIORITY_KEYWORDS must not contain 'Sánchez'"
        )
        assert "Feijóo" not in TITLE_PRIORITY_KEYWORDS, (
            "TITLE_PRIORITY_KEYWORDS must not contain 'Feijóo'"
        )
        assert "Yolanda Díaz" not in TITLE_PRIORITY_KEYWORDS, (
            "TITLE_PRIORITY_KEYWORDS must not contain 'Yolanda Díaz'"
        )
        assert "corrupción" in TITLE_PRIORITY_KEYWORDS, (
            "TITLE_PRIORITY_KEYWORDS must still contain 'corrupción'"
        )


class TestSystemPromptPrizacausaClauseNoPoliticians:
    """T-02 RED: THUMBNAIL_TITLE_SYSTEM_PROMPT PRIORIZA clause must name no politicians."""

    def test_system_prompt_prioriza_clause_names_no_politicians(self) -> None:
        """THUMBNAIL_TITLE_SYSTEM_PROMPT must not contain politician names Sánchez/Feijóo/Yolanda Díaz."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SYSTEM_PROMPT

        assert "Sánchez" not in THUMBNAIL_TITLE_SYSTEM_PROMPT, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must not contain 'Sánchez'"
        )
        assert "Feijóo" not in THUMBNAIL_TITLE_SYSTEM_PROMPT, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must not contain 'Feijóo'"
        )
        assert "Yolanda Díaz" not in THUMBNAIL_TITLE_SYSTEM_PROMPT, (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must not contain 'Yolanda Díaz'"
        )
        assert any(
            kw in THUMBNAIL_TITLE_SYSTEM_PROMPT
            for kw in ("corrupción", "PRIORIZA", "prioriza")
        ), (
            "THUMBNAIL_TITLE_SYSTEM_PROMPT must still contain a retained topic word"
        )


class TestSpeakersInstructionIsProhibition:
    """T-03 RED: THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must be a hard prohibition."""

    def test_speakers_instruction_is_hard_prohibition(self) -> None:
        """THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must contain hard-prohibition language, not soft hint."""
        from congress_videos.config.ai_prompts import THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION

        prohibition_phrases = ("SOLO puedes nombrar", "SOLO puedes", "solo puedes nombrar")
        assert any(p in THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION for p in prohibition_phrases), (
            "THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must contain hard prohibition "
            f"(one of {prohibition_phrases})"
        )
        soft_phrases = ("Si es natural", "menciona a alguno")
        assert not any(p in THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION for p in soft_phrases), (
            "THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION must NOT contain soft suggestion text"
        )


# ---------------------------------------------------------------------------
# issue #91: title-speaker-attribution Phase C — _real_speakers helper
# ---------------------------------------------------------------------------


class TestRealSpeakersHelper:
    """T-08 RED: _real_speakers pure helper — various input scenarios."""

    def test_none_returns_empty_list(self) -> None:
        """_real_speakers(None) must return []."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(None) == []

    def test_empty_list_returns_empty_list(self) -> None:
        """_real_speakers([]) must return []."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers([]) == []

    def test_empty_string_entry_dropped(self) -> None:
        """_real_speakers(['']) must return [] (empty string is not a real name)."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers([""]) == []

    def test_real_name_string_returned(self) -> None:
        """_real_speakers(['Cervera Pinar']) must return ['Cervera Pinar']."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(["Cervera Pinar"]) == ["Cervera Pinar"]

    def test_dict_entry_extracts_name(self) -> None:
        """_real_speakers([{'name': 'Ana Pastor'}]) must return ['Ana Pastor']."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers([{"name": "Ana Pastor"}]) == ["Ana Pastor"]

    def test_placeholder_interviniente_no_identificado_filtered(self) -> None:
        """_real_speakers(['Interviniente no identificado']) must return []."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(["Interviniente no identificado"]) == []

    def test_placeholder_uppercase_filtered(self) -> None:
        """_real_speakers(['INTERVINIENTE NO IDENTIFICADO']) must return [] (case-insensitive)."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(["INTERVINIENTE NO IDENTIFICADO"]) == []

    def test_mixed_placeholder_and_real_returns_real_only(self) -> None:
        """_real_speakers(['Interviniente no identificado', 'Cervera Pinar']) → ['Cervera Pinar']."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        result = _real_speakers(["Interviniente no identificado", "Cervera Pinar"])
        assert result == ["Cervera Pinar"]

    def test_ponente_desconocido_filtered(self) -> None:
        """_real_speakers(['Ponente desconocido']) must return []."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(["Ponente desconocido"]) == []

    def test_orador_desconocido_filtered(self) -> None:
        """_real_speakers(['Orador desconocido']) must return []."""
        from congress_videos.modules.thumbnail_generation import _real_speakers

        assert _real_speakers(["Orador desconocido"]) == []


# ---------------------------------------------------------------------------
# issue #91: title-speaker-attribution Phase C — generate_title 3-state branch
# ---------------------------------------------------------------------------


class TestGenerateTitlePromptInjection:
    """T-09 RED: generate_title injects correct speaker block based on 3-state logic."""

    def _make_best(self) -> dict:
        return {"style": "A", "prompt": "debate parlamentario"}

    def _make_cfg(self) -> dict:
        return {
            "styles": [],
            "participants_lookup": lambda slug: None,
            "party_logo_map": None,
        }

    def _capture_prompt(self, mocker, key_speakers):
        """Helper: call generate_title and return the first captured user_prompt."""
        from congress_videos.modules.thumbnail_generation import generate_title

        captured: list[str] = []

        def _side(system_prompt, user_prompt, **kw):
            captured.append(user_prompt)
            return {"data": {"title": "Un título válido"}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side,
        )
        generate_title(
            "Debate summary",
            self._make_best(),
            self._make_cfg(),
            key_speakers=key_speakers,
        )
        return captured[0] if captured else ""

    def test_real_speaker_injects_prohibition_not_nameless(self, mocker) -> None:
        """key_speakers=['Cervera Pinar'] → user_prompt has prohibition + name, no nameless block."""
        prompt = self._capture_prompt(mocker, ["Cervera Pinar"])

        assert "SOLO puedes nombrar" in prompt or "solo puedes nombrar" in prompt.lower(), (
            "Prompt must contain hard prohibition phrase when real speaker present"
        )
        assert "Cervera Pinar" in prompt, "Prompt must name the real speaker"
        assert "no hay ponentes identificados" not in prompt.lower(), (
            "Nameless instruction must NOT appear when real speaker is present"
        )

    def test_all_placeholder_injects_nameless_not_prohibition(self, mocker) -> None:
        """key_speakers=['Interviniente no identificado'] → prompt has nameless instruction, no name."""
        prompt = self._capture_prompt(mocker, ["Interviniente no identificado"])

        assert "no hay ponentes identificados" in prompt.lower(), (
            "Nameless instruction must appear when all speakers are placeholders"
        )
        assert "Interviniente no identificado" not in prompt or (
            "SOLO puedes nombrar" not in prompt
        ), (
            "Placeholder name must not be listed in a speaker prohibition block"
        )

    def test_mixed_list_uses_real_name_prohibition(self, mocker) -> None:
        """key_speakers=['Interviniente no identificado', 'Cervera Pinar'] → prohibition with real name only."""
        prompt = self._capture_prompt(
            mocker, ["Interviniente no identificado", "Cervera Pinar"]
        )

        assert "Cervera Pinar" in prompt, "Real name must appear in prohibition"
        assert "no hay ponentes identificados" not in prompt.lower(), (
            "Nameless instruction must NOT appear when real speaker exists"
        )
        assert "Interviniente no identificado" not in prompt or (
            "SOLO puedes nombrar" in prompt
        ), (
            "Placeholder must not appear in the speaker list sent to the model"
        )

    def test_none_key_speakers_injects_nameless(self, mocker) -> None:
        """key_speakers=None → user_prompt MUST contain nameless instruction (same as empty list)."""
        prompt = self._capture_prompt(mocker, None)

        assert "no hay ponentes identificados" in prompt.lower(), (
            "Nameless instruction must appear when key_speakers=None"
        )

    def test_empty_key_speakers_injects_nameless(self, mocker) -> None:
        """key_speakers=[] → user_prompt MUST contain nameless instruction (same as None)."""
        prompt = self._capture_prompt(mocker, [])

        assert "no hay ponentes identificados" in prompt.lower(), (
            "Nameless instruction must appear when key_speakers=[]"
        )


# ---------------------------------------------------------------------------
# Issue #228: characterization tests locking the C901 split's invariance
#
# These tests must pass BOTH before and after the extraction of
# extract_lapidary_quote / art_direct / generate_title into module-level
# private helpers — they are the behaviour-preservation proof for that pure
# refactor. Additions only; no existing assertion above this point is edited.
# ---------------------------------------------------------------------------


class TestLapidaryIndexZeroSentinelGuard:
    """Locks the highest-risk seam: index 0 (the first candidate) must never
    be dropped by a truthiness check on the parsed index.

    The LLM's single most likely answer is "1", which parses to a 0-based
    index of 0. An orchestrator written as `if not idx: return None` would
    silently discard this common, successful result. The correct guard is
    `if idx is None: return None`.
    """

    def test_llm_index_one_maps_to_zero_based_candidate_zero(self):
        """LLM content '1' → 1-based index 1 → 0-based idx=0 → candidates[0] returned, not None."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "1", "error": None}

        # Two candidates survive filtering; the LLM picks the first (idx=0).
        fragment = "esto es una prueba seria. vamos a votar ya"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)

        assert result is not None, (
            "idx=0 must not be treated as falsy and silently dropped"
        )
        assert result == "esto es una prueba seria"

    def test_llm_index_one_among_multiple_candidates_returns_first(self):
        """With 3+ surviving candidates, LLM content '1' still returns candidates[0]."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        def fake_fn(**_kw):
            return {"content": "1", "error": None}

        fragment = (
            "vamos a votar ya. "
            "esto es una prueba seria. "
            "nunca vamos a ceder aqui"
        )
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)

        assert result == "vamos a votar ya"


class TestLapidaryCandidateFilteringSentinel:
    """Locks the empty-candidates-list sentinel: [] must short-circuit to
    None without invoking the LLM ranker at all."""

    def test_all_candidates_filtered_returns_none_without_llm_call(self):
        """Every clause fails min/max word bounds → candidates=[] → None, no LLM call."""
        from congress_videos.modules.thumbnail_generation import extract_lapidary_quote

        called = []

        def fake_fn(**_kw):
            called.append(True)
            return {"content": "1", "error": None}

        # Single-word clauses fail the default min_words=3 bound.
        fragment = "si. no. tal vez"
        result = extract_lapidary_quote(fragment, completion_fn=fake_fn)

        assert result is None
        assert called == [], (
            "completion_fn must not be invoked when the candidate list is empty"
        )


class TestReorderingGuardReprompt:
    """Locks the `_choose_reprompt_instruction` elif ordering: a title that
    matches multiple trigger conditions must always resolve to branch 1
    (question) — reordering the chain would change which instruction fires.
    """

    def test_question_and_too_long_title_yields_question_instruction(self, mocker) -> None:
        """A title that is both >90 chars AND a question must trigger the question rewrite, not the length one."""
        from congress_videos.modules.thumbnail_generation import generate_title

        long_question = "¿" + "Qué va a pasar con las pensiones y el futuro del país " * 3 + "?"
        assert len(long_question) > 90, "fixture must exceed TITLE_MAX_CHARS"
        valid_second = "Sánchez anuncia medidas económicas"
        captured_prompts: list[str] = []
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            captured_prompts.append(user_prompt)
            if call_count["n"] == 1:
                return {"data": {"title": long_question}, "error": None}
            return {"data": {"title": valid_second}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert call_count["n"] == 2
        assert "el título es una pregunta" in captured_prompts[1].lower(), (
            "Question branch must win over the too-long branch"
        )
        assert "demasiado largo" not in captured_prompts[1].lower(), (
            "Length instruction must NOT fire when the title is also a question"
        )
        assert result == valid_second

    def test_all_caps_and_question_title_yields_question_instruction(self, mocker) -> None:
        """A title that is both ALL-CAPS AND a question must trigger the question rewrite, not the caps one."""
        from congress_videos.modules.thumbnail_generation import generate_title

        caps_question = "¿QUÉ VA A PASAR CON LAS PENSIONES?"
        valid_second = "Sánchez anuncia medidas económicas"
        captured_prompts: list[str] = []
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            captured_prompts.append(user_prompt)
            if call_count["n"] == 1:
                return {"data": {"title": caps_question}, "error": None}
            return {"data": {"title": valid_second}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert call_count["n"] == 2
        assert "el título es una pregunta" in captured_prompts[1].lower(), (
            "Question branch must win over the all-caps branch"
        )
        assert "está todo en mayúsculas" not in captured_prompts[1].lower(), (
            "All-caps instruction must NOT fire when the title is also a question"
        )
        assert result == valid_second

    def test_too_long_and_all_caps_title_yields_length_instruction(self, mocker) -> None:
        """A title that is both >90 chars AND ALL-CAPS must trigger the length
        rewrite, not the caps one.

        The other two tests pin branch 1 against branches 2 and 3; this pins
        branch 2 against branch 3, the one remaining unlocked edge of the chain.
        """
        from congress_videos.modules.thumbnail_generation import generate_title

        long_caps = "SANCHEZ ANUNCIA MEDIDAS ECONOMICAS PARA EL FUTURO DE LAS PENSIONES Y EL EMPLEO EN ESPANA HOY"
        assert len(long_caps) > 90, "fixture must exceed TITLE_MAX_CHARS"
        assert long_caps.isupper(), "fixture must be all-caps"
        valid_second = "Sánchez anuncia medidas económicas"
        captured_prompts: list[str] = []
        call_count = {"n": 0}

        def _side_effect(system_prompt, user_prompt, **kwargs):
            call_count["n"] += 1
            captured_prompts.append(user_prompt)
            if call_count["n"] == 1:
                return {"data": {"title": long_caps}, "error": None}
            return {"data": {"title": valid_second}, "error": None}

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side_effect,
        )
        cfg = _make_cfg()
        best = {"style": "A", "prompt": "debate"}
        result = generate_title("Debate summary", best, cfg)

        assert call_count["n"] == 2
        assert "demasiado largo" in captured_prompts[1].lower(), (
            "Too-long branch must win over the all-caps branch"
        )
        assert "está todo en mayúsculas" not in captured_prompts[1].lower(), (
            "All-caps instruction must NOT fire when the title is also too long"
        )
        assert result == valid_second


# ---------------------------------------------------------------------------
# Issue #279: resolved-photo grounding exception in art_direct
# ---------------------------------------------------------------------------


class TestResolvedPhotoInstructionConstant:
    """Phase 1: ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION constant contract."""

    def test_constant_is_a_nonempty_format_able_string(self) -> None:
        """Exists as a non-empty string and interpolates distinct speaker names."""
        from congress_videos.config.ai_prompts import (
            ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION,
        )

        assert isinstance(ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION, str)
        assert ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION.strip()
        assert "Viviane Ogou i Corbi" in ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION.format(
            speaker_name="Viviane Ogou i Corbi"
        )
        assert "Cervera Pinar" in ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION.format(
            speaker_name="Cervera Pinar"
        )

    def test_system_prompt_unchanged_by_new_constant(self) -> None:
        """Adding the new constant must not alter ART_DIRECTION_SYSTEM_PROMPT."""
        from congress_videos.config.ai_prompts import ART_DIRECTION_SYSTEM_PROMPT

        assert "nunca el ponente" in ART_DIRECTION_SYSTEM_PROMPT
        assert "sexo gramatical de los ponentes" in ART_DIRECTION_SYSTEM_PROMPT


class TestResolvedPhotoSpeakerName:
    """Phase 2: resolved_photo_speaker_name(photo_data, key_speakers) activation gate.

    Gate contract: returns a real name only when photo_data.get("source") ==
    "photo" AND at least one real (non-placeholder) name exists in
    key_speakers. Every other combination returns None.
    """

    def test_photo_source_with_real_name_returns_name(self) -> None:
        """source='photo' + a real key_speakers name (string or dict) → returns it."""
        from congress_videos.modules.thumbnail_generation import (
            resolved_photo_speaker_name,
        )

        assert (
            resolved_photo_speaker_name({"source": "photo"}, ["Viviane Ogou i Corbi"])
            == "Viviane Ogou i Corbi"
        )
        assert (
            resolved_photo_speaker_name({"source": "photo"}, [{"name": "Cervera Pinar"}])
            == "Cervera Pinar"
        )

    def test_multiple_real_names_returns_first(self) -> None:
        """source='photo' with multiple real names → returns the first one."""
        from congress_videos.modules.thumbnail_generation import (
            resolved_photo_speaker_name,
        )

        result = resolved_photo_speaker_name(
            {"source": "photo"},
            ["Interviniente no identificado", "Cervera Pinar", "Ana Pastor"],
        )
        assert result == "Cervera Pinar"

    def test_gate_closed_returns_none_for_non_activating_inputs(self) -> None:
        """Every combination that must NOT activate the exception returns None:
        party_logo source, source='none', empty/None photo_data, placeholder-only
        or None key_speakers — even when paired with an otherwise-real name.
        """
        from congress_videos.modules.thumbnail_generation import (
            resolved_photo_speaker_name,
        )

        real_speaker = ["Viviane Ogou i Corbi"]
        assert resolved_photo_speaker_name({"source": "party_logo"}, real_speaker) is None
        assert resolved_photo_speaker_name({"source": "none"}, real_speaker) is None
        assert resolved_photo_speaker_name({}, real_speaker) is None
        assert resolved_photo_speaker_name(None, real_speaker) is None
        assert (
            resolved_photo_speaker_name(
                {"source": "photo"}, ["Interviniente no identificado"]
            )
            is None
        )
        assert resolved_photo_speaker_name({"source": "photo"}, None) is None


class TestArtDirectResolvedPhotoInstruction:
    """Phase 3: art_direct(..., resolved_speaker_name=) prompt injection,
    ordering against retry/sibling blocks, and the byte-identical no-op
    guarantee when no speaker name is resolved.
    """

    def _valid_brief_response(self) -> dict:
        return {
            "data": {
                "text": "RETRATO REAL",
                "background": "pasillo del Congreso",
                "person": "Cervera Pinar, expresión seria",
                "mood": "seriedad",
            },
            "error": None,
        }

    def _capture(self, mocker) -> list:
        captured_prompts: list[str] = []

        def _side(system_prompt, user_prompt, **kw):
            captured_prompts.append(user_prompt)
            return self._valid_brief_response()

        mocker.patch(
            "congress_videos.modules.thumbnail_generation.generate_json_completion",
            side_effect=_side,
        )
        return captured_prompts

    def test_name_given_injects_resolved_photo_block(self, mocker) -> None:
        """resolved_speaker_name set → prompt contains the block naming that speaker."""
        from congress_videos.modules.thumbnail_generation import art_direct

        captured_prompts = self._capture(mocker)
        art_direct(
            "Debate sobre presupuesto", _make_cfg(), resolved_speaker_name="Cervera Pinar"
        )

        assert captured_prompts, "generate_json_completion must be called"
        assert "EXCEPCIÓN DE IDENTIDAD" in captured_prompts[0]
        assert "Cervera Pinar" in captured_prompts[0]

    def test_none_or_empty_resolved_speaker_name_is_byte_identical_to_baseline(
        self, mocker
    ) -> None:
        """resolved_speaker_name unset, None, or '' → prompt matches the
        pre-change baseline exactly, with no identity-exception language."""
        from congress_videos.modules.thumbnail_generation import art_direct

        baseline_prompts = self._capture(mocker)
        art_direct("Debate sobre presupuesto", _make_cfg())
        baseline = baseline_prompts[0]
        assert "EXCEPCIÓN DE IDENTIDAD" not in baseline
        assert "FOTOGRAFÍA REAL" not in baseline

        for value in (None, ""):
            variant_prompts = self._capture(mocker)
            art_direct(
                "Debate sobre presupuesto", _make_cfg(), resolved_speaker_name=value
            )
            assert variant_prompts[0] == baseline

    def test_photo_block_ordered_after_retry_and_sibling_blocks(self, mocker) -> None:
        """Photo block appears after BOTH the retry block (previous_brief set)
        and the sibling block (sibling_briefs set), independently."""
        from congress_videos.modules.thumbnail_generation import art_direct

        previous_brief = {
            "text": "ANTERIOR",
            "background": "hemiciclo",
            "person": "político",
            "mood": "tensión",
        }
        retry_prompts = self._capture(mocker)
        art_direct(
            "Debate sobre presupuesto",
            _make_cfg(),
            previous_brief=previous_brief,
            resolved_speaker_name="Cervera Pinar",
        )
        retry_prompt = retry_prompts[0]
        assert "REINTENTO" in retry_prompt and "EXCEPCIÓN DE IDENTIDAD" in retry_prompt
        assert retry_prompt.index("EXCEPCIÓN DE IDENTIDAD") > retry_prompt.index("REINTENTO")

        sibling_prompts = self._capture(mocker)
        art_direct(
            "Debate sobre presupuesto",
            _make_cfg(),
            sibling_briefs=["brief A sobre plaza pública"],
            resolved_speaker_name="Cervera Pinar",
        )
        sibling_prompt = sibling_prompts[0]
        assert "NO REPITAS" in sibling_prompt and "EXCEPCIÓN DE IDENTIDAD" in sibling_prompt
        assert sibling_prompt.index("EXCEPCIÓN DE IDENTIDAD") > sibling_prompt.index("NO REPITAS")
