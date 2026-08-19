"""Tests for congress_videos.modules.trim_proposals_api — HTTP YAMNet wrapper.

All HTTP calls are intercepted via the injected ``poster`` (or by patching
``requests.post``).  No real network connection is ever made.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
import requests


def _mock_response(status_code: int = 200, json_body: object = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    if json_body is not None:
        resp.json.return_value = json_body
    else:
        resp.json.side_effect = requests.exceptions.JSONDecodeError("no JSON", "", 0)
    return resp


SAMPLE_INTERVALS = [
    {"start": 10.0, "end": 25.0, "max_score": 0.88},
]


class TestApiYamnetFnHappyPath:
    def test_returns_applause_intervals_list(self, tmp_path):
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"applause_intervals": SAMPLE_INTERVALS})
        poster = MagicMock(return_value=resp)

        result = api_yamnet_fn(str(wav), offset=0.0, poster=poster)

        assert isinstance(result, list)
        assert result[0]["max_score"] == pytest.approx(0.88)

    def test_sends_audio_file_in_multipart(self, tmp_path):
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"applause_intervals": []})
        poster = MagicMock(return_value=resp)

        api_yamnet_fn(str(wav), offset=0.0, poster=poster)

        _, kwargs = poster.call_args
        assert "files" in kwargs
        assert "audio_file" in kwargs["files"]

    def test_sends_offset_as_form_field(self, tmp_path):
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"applause_intervals": []})
        poster = MagicMock(return_value=resp)

        api_yamnet_fn(str(wav), offset=42.5, poster=poster)

        _, kwargs = poster.call_args
        assert "data" in kwargs
        assert str(kwargs["data"].get("offset")) == "42.5"

    def test_triangulate_different_offset_value(self, tmp_path):
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(200, {"applause_intervals": []})
        poster = MagicMock(return_value=resp)

        api_yamnet_fn(str(wav), offset=0.0, poster=poster)

        _, kwargs = poster.call_args
        assert "data" in kwargs
        assert str(kwargs["data"].get("offset")) == "0.0"


class TestApiYamnetFnErrorHandling:
    def test_connection_error_raises_sidecar_api_error(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.trim_proposals_api import (
            YAMNET_API_URL,
            api_yamnet_fn,
        )

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")

        def failing_poster(*a, **kw):
            raise requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(SidecarApiError) as exc_info:
            api_yamnet_fn(str(wav), offset=0.0, poster=failing_poster)

        assert YAMNET_API_URL in str(exc_info.value)

    def test_timeout_raises_sidecar_api_error_with_timeout_message(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")

        def timeout_poster(*a, **kw):
            raise requests.exceptions.Timeout("timed out")

        with pytest.raises(SidecarApiError) as exc_info:
            api_yamnet_fn(str(wav), offset=0.0, poster=timeout_poster)

        assert "timed out" in str(exc_info.value).lower() or "timeout" in str(exc_info.value).lower()

    def test_http_500_raises_sidecar_api_error_with_status_code(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = _mock_response(500, None)
        poster = MagicMock(return_value=resp)

        with pytest.raises(SidecarApiError) as exc_info:
            api_yamnet_fn(str(wav), offset=0.0, poster=poster)

        assert "500" in str(exc_info.value)

    def test_malformed_json_raises_sidecar_api_error(self, tmp_path):
        from congress_videos.modules.sidecar_api_error import SidecarApiError
        from congress_videos.modules.trim_proposals_api import api_yamnet_fn

        wav = tmp_path / "turn.wav"
        wav.write_bytes(b"RIFF")
        resp = MagicMock()
        resp.status_code = 200
        resp.json.side_effect = requests.exceptions.JSONDecodeError("no JSON", "", 0)
        poster = MagicMock(return_value=resp)

        with pytest.raises(SidecarApiError) as exc_info:
            api_yamnet_fn(str(wav), offset=0.0, poster=poster)

        err = str(exc_info.value).lower()
        assert "json" in err or "malformed" in err or "parse" in err


class TestUrlFromEnvVars:
    def test_url_uses_yamnet_api_host_and_port_env_vars(self, monkeypatch):
        monkeypatch.setenv("YAMNET_API_HOST", "my-yamnet-host")
        monkeypatch.setenv("YAMNET_API_PORT", "9999")

        import sys
        for key in list(sys.modules.keys()):
            if "trim_proposals_api" in key:
                del sys.modules[key]

        import congress_videos.modules.trim_proposals_api as tpa

        assert "my-yamnet-host" in tpa.YAMNET_API_URL
        assert "9999" in tpa.YAMNET_API_URL

    def test_url_has_default_host_yamnet_api(self, monkeypatch):
        monkeypatch.delenv("YAMNET_API_HOST", raising=False)
        monkeypatch.delenv("YAMNET_API_PORT", raising=False)

        import sys
        for key in list(sys.modules.keys()):
            if "trim_proposals_api" in key:
                del sys.modules[key]

        import congress_videos.modules.trim_proposals_api as tpa

        assert "yamnet-api" in tpa.YAMNET_API_URL
        assert "8081" in tpa.YAMNET_API_URL


class TestNoSubprocessInTrimProposalsApi:
    def test_no_subprocess_import(self):
        """trim_proposals_api must not import subprocess — it uses HTTP transport."""
        import sys
        for key in list(sys.modules.keys()):
            if "trim_proposals_api" in key:
                del sys.modules[key]

        import congress_videos.modules.trim_proposals_api as tpa
        import inspect

        source = inspect.getsource(tpa)
        assert "import subprocess" not in source
        assert "subprocess.run" not in source
