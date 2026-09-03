"""Tests for congress_videos.modules.speaker_turns_api — HTTP diarization wrapper.

All HTTP calls are intercepted via the injected ``poster`` (or by patching
``requests.post``).  No real network connection is ever made.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests


def _mock_response(status_code: int = 200, json_body: object = None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = requests.exceptions.JSONDecodeError("no JSON", "", 0)
    resp.text = text
    return resp


SAMPLE_CHANGES = [
    {
        "start_seconds": 12.5,
        "from_speaker": "SPEAKER_00",
        "to_speaker": "SPEAKER_01",
        "confirmed_block_duration_seconds": 30.0,
    }
]


class TestDefaultTimeout:
    def test_default_timeout_is_four_hours(self):
        """Diarize requests must wait up to 4h before giving up.

        Real diarization of the same chapter ranges from ~2 min on a quiet NAS
        to ~4 h under disk I/O contention; the previous 1 h cap made loaded
        runs fail systematically and loop on the same chapters. Single-chapter
        durable runs (#193) bound the cost of waiting to one chapter per run.
        """
        from congress_videos.modules import speaker_turns_api

        assert speaker_turns_api._DEFAULT_TIMEOUT_SECONDS == 4 * 3600


class TestApiDiarizeFnHappyPath:
    def test_returns_speaker_changes_list(self, tmp_path):
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"speaker_changes": SAMPLE_CHANGES})
        poster = MagicMock(return_value=resp)

        result = api_diarize_fn(str(wav), chapter_offset=0.0, poster=poster)

        assert isinstance(result, list)
        assert result[0]["from_speaker"] == "SPEAKER_00"
        assert result[0]["to_speaker"] == "SPEAKER_01"

    def test_sends_audio_file_in_form(self, tmp_path):
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"speaker_changes": []})
        poster = MagicMock(return_value=resp)

        api_diarize_fn(str(wav), chapter_offset=0.0, poster=poster)

        _, kwargs = poster.call_args
        # audio_file must be in the 'files' multipart argument
        assert "files" in kwargs
        assert "audio_file" in kwargs["files"]

    def test_sends_chapter_offset_as_form_field(self, tmp_path):
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"speaker_changes": []})
        poster = MagicMock(return_value=resp)

        api_diarize_fn(str(wav), chapter_offset=12.5, poster=poster)

        _, kwargs = poster.call_args
        assert "data" in kwargs
        assert str(kwargs["data"].get("chapter_offset")) == "12.5"

    def test_triangulate_different_offset_value(self, tmp_path):
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"speaker_changes": []})
        poster = MagicMock(return_value=resp)

        api_diarize_fn(str(wav), chapter_offset=99.9, poster=poster)

        _, kwargs = poster.call_args
        assert "data" in kwargs
        assert str(kwargs["data"].get("chapter_offset")) == "99.9"


class TestApiDiarizeFnErrorHandling:
    def test_connection_error_raises_sidecar_api_error(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import (
            DIARIZE_API_URL,
            api_diarize_fn,
        )

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")

        def failing_poster(*a, **kw):
            raise requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(SidecarApiError) as exc_info:
            api_diarize_fn(str(wav), chapter_offset=0.0, poster=failing_poster)

        assert DIARIZE_API_URL in str(exc_info.value)

    def test_timeout_raises_sidecar_api_error_with_timeout_text(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")

        def timeout_poster(*a, **kw):
            raise requests.exceptions.Timeout("timed out")

        with pytest.raises(SidecarApiError) as exc_info:
            api_diarize_fn(str(wav), chapter_offset=0.0, poster=timeout_poster)

        assert "timed out" in str(exc_info.value).lower() or "timeout" in str(exc_info.value).lower()

    def test_http_500_raises_sidecar_api_error_with_status_code(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(500, None)
        poster = MagicMock(return_value=resp)

        with pytest.raises(SidecarApiError) as exc_info:
            api_diarize_fn(str(wav), chapter_offset=0.0, poster=poster)

        assert "500" in str(exc_info.value)

    def test_http_422_raises_sidecar_api_error(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(422, None)
        poster = MagicMock(return_value=resp)

        with pytest.raises(SidecarApiError) as exc_info:
            api_diarize_fn(str(wav), chapter_offset=0.0, poster=poster)

        assert "422" in str(exc_info.value)

    def test_malformed_json_raises_sidecar_api_error_with_parse_error(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import api_diarize_fn

        wav = tmp_path / "chapter.wav"
        wav.write_bytes(b"RIFF")
        resp = MagicMock()
        resp.status_code = 200
        resp.json.side_effect = requests.exceptions.JSONDecodeError("Expecting value", "", 0)
        poster = MagicMock(return_value=resp)

        with pytest.raises(SidecarApiError) as exc_info:
            api_diarize_fn(str(wav), chapter_offset=0.0, poster=poster)

        assert (
            "json" in str(exc_info.value).lower()
            or "parse" in str(exc_info.value).lower()
            or "malformed" in str(exc_info.value).lower()
        )


class TestUrlFromEnvVars:
    def test_url_uses_diarize_api_host_and_port_env_vars(self, monkeypatch):
        monkeypatch.setenv("DIARIZE_API_HOST", "my-diarize-host")
        monkeypatch.setenv("DIARIZE_API_PORT", "9999")

        import sys

        for key in list(sys.modules.keys()):
            if "speaker_turns_api" in key:
                del sys.modules[key]

        import congress_videos.modules.speaker_turns_api as sta

        assert "my-diarize-host" in sta.DIARIZE_API_URL
        assert "9999" in sta.DIARIZE_API_URL

    def test_url_has_default_host_diarize_api(self, monkeypatch):
        monkeypatch.delenv("DIARIZE_API_HOST", raising=False)
        monkeypatch.delenv("DIARIZE_API_PORT", raising=False)

        import sys

        for key in list(sys.modules.keys()):
            if "speaker_turns_api" in key:
                del sys.modules[key]

        import congress_videos.modules.speaker_turns_api as sta

        assert "diarize-api" in sta.DIARIZE_API_URL
        assert "8080" in sta.DIARIZE_API_URL


class TestNoSubprocessImport:
    def test_no_subprocess_in_speaker_turns_api(self):
        """speaker_turns_api must not import subprocess — it uses HTTP, not Docker."""
        import sys

        for key in list(sys.modules.keys()):
            if "speaker_turns_api" in key:
                del sys.modules[key]

        import inspect

        import congress_videos.modules.speaker_turns_api as sta

        source = inspect.getsource(sta)
        assert "import subprocess" not in source
        assert "subprocess.run" not in source


# ---------------------------------------------------------------------------
# Phase: check_diarize_api_health (issue #156)
# ---------------------------------------------------------------------------


class TestCheckDiarizeApiHealth:
    """Health probe that raises SidecarApiError on any failure condition.

    The getter is injected so no real network connection is ever made.
    """

    def test_healthy_200_returns_without_raising(self):
        """getter returns HTTP 200 → check_diarize_api_health returns None without raising."""
        from congress_videos.modules.speaker_turns_api import check_diarize_api_health

        mock_resp = MagicMock()
        mock_resp.status_code = 200
        getter = MagicMock(return_value=mock_resp)

        # Must not raise
        result = check_diarize_api_health(getter=getter)
        assert result is None

    def test_connection_error_raises_sidecar_api_error_unreachable(self):
        """getter raises requests.ConnectionError → SidecarApiError with 'diarize-api' and 'unreachable'."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import check_diarize_api_health

        def failing_getter(url, timeout):
            raise requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(SidecarApiError) as exc_info:
            check_diarize_api_health(getter=failing_getter)

        message = str(exc_info.value)
        assert "diarize-api" in message
        assert "unreachable" in message

    def test_timeout_raises_sidecar_api_error_with_timeout_substring(self):
        """getter raises requests.Timeout → SidecarApiError with 'diarize-api' AND 'timeout'.

        Dedicated test row — proves the Timeout branch fires BEFORE the generic
        RequestException branch (which would produce 'unreachable', not 'timeout').
        """
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import check_diarize_api_health

        def timeout_getter(url, timeout):
            raise requests.exceptions.Timeout()

        with pytest.raises(SidecarApiError) as exc_info:
            check_diarize_api_health(getter=timeout_getter)

        message = str(exc_info.value)
        assert "diarize-api" in message
        assert "timeout" in message

    def test_non_200_status_raises_sidecar_api_error_with_status_code(self):
        """getter returns HTTP 503 → SidecarApiError with '503' in message."""
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.speaker_turns_api import check_diarize_api_health

        mock_resp = MagicMock()
        mock_resp.status_code = 503
        getter = MagicMock(return_value=mock_resp)

        with pytest.raises(SidecarApiError) as exc_info:
            check_diarize_api_health(getter=getter)

        assert "503" in str(exc_info.value)
