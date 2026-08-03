"""Tests for congress_videos.modules.pikzels_client (Pikzels v2 trimmed client).

RED phase: all tests are written before the module exists, so they should fail
on the first run with ImportError or AttributeError.
"""

from __future__ import annotations

import base64
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_session_response(status_code: int, json_data: dict | None = None, content: bytes = b"") -> MagicMock:
    """Build a fake requests.Response for Session.request mocking."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.content = content or (b'{}' if json_data is not None else b"")
    resp.json.return_value = json_data or {}
    resp.text = ""
    resp.headers = {}
    return resp


# ---------------------------------------------------------------------------
# T-01: Public surface — required symbols present, trimmed symbols absent
# ---------------------------------------------------------------------------

class TestPublicSurface:
    """Module exposes only the required symbols; trimmed methods are gone."""

    def test_required_imports_succeed(self, monkeypatch):
        """Importing the four required callables must not raise."""
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")
        from congress_videos.modules.pikzels_client import (  # noqa: F401
            download,
            score_thumbnail,
            thumbnail_from_text,
            to_base64_data_url,
        )

    def test_trimmed_thumbnail_from_image_absent(self, monkeypatch):
        """thumbnail_from_image must NOT be present in the module."""
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")
        import congress_videos.modules.pikzels_client as m
        assert not hasattr(m, "thumbnail_from_image"), (
            "thumbnail_from_image must be trimmed from this port"
        )

    def test_trimmed_edit_thumbnail_absent(self, monkeypatch):
        """edit_thumbnail must NOT be present."""
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")
        import congress_videos.modules.pikzels_client as m
        assert not hasattr(m, "edit_thumbnail")

    def test_trimmed_generate_titles_absent(self, monkeypatch):
        """generate_titles must NOT be present."""
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")
        import congress_videos.modules.pikzels_client as m
        assert not hasattr(m, "generate_titles")


# ---------------------------------------------------------------------------
# T-02: Missing PIKZELS_API_KEY raises EnvironmentError
# ---------------------------------------------------------------------------

class TestMissingApiKey:
    """Construction must raise EnvironmentError when PIKZELS_API_KEY is absent or empty."""

    def test_raises_when_env_var_absent(self, monkeypatch):
        """No PIKZELS_API_KEY → EnvironmentError naming the variable."""
        monkeypatch.delenv("PIKZELS_API_KEY", raising=False)
        # Re-import to bypass any module-level caching
        import importlib
        import sys
        sys.modules.pop("congress_videos.modules.pikzels_client", None)
        from congress_videos.modules.pikzels_client import PikzelsClient
        with pytest.raises((EnvironmentError, RuntimeError)) as exc_info:
            PikzelsClient()
        assert "PIKZELS_API_KEY" in str(exc_info.value)

    def test_raises_when_env_var_empty(self, monkeypatch):
        """Empty PIKZELS_API_KEY → EnvironmentError naming the variable."""
        monkeypatch.setenv("PIKZELS_API_KEY", "")
        import sys
        sys.modules.pop("congress_videos.modules.pikzels_client", None)
        from congress_videos.modules.pikzels_client import PikzelsClient
        with pytest.raises((EnvironmentError, RuntimeError)) as exc_info:
            PikzelsClient()
        assert "PIKZELS_API_KEY" in str(exc_info.value)

    def test_valid_key_does_not_raise(self, monkeypatch):
        """Valid pkz_ key → no exception."""
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-valid-key")
        from congress_videos.modules.pikzels_client import PikzelsClient
        client = PikzelsClient()
        assert client is not None


# ---------------------------------------------------------------------------
# T-03: _request retries on HTTP 503 (retryable 5xx)
# ---------------------------------------------------------------------------

class TestRequestRetry:
    """_request retries retryable codes and raises immediately on non-retryable."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_retries_twice_on_503_then_succeeds(self, monkeypatch):
        """Two 503s then one 200 → call count is 3 and result is the 200 payload."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        error_body = {"error": {"code": "INTERNAL_ERROR", "message": "server down"}}
        ok_body = {"output": "https://cdn.example.com/thumb.png", "request_id": "req_001"}

        resp_503a = _make_session_response(503, error_body)
        resp_503b = _make_session_response(503, error_body)
        resp_200 = _make_session_response(200, ok_body)

        client = PikzelsClient(max_retries=5)

        with patch.object(client._session, "request", side_effect=[resp_503a, resp_503b, resp_200]) as mock_req:
            with patch("time.sleep"):  # suppress real sleep
                result = client._request("POST", "/v2/thumbnail/text", {"prompt": "test"})

        assert mock_req.call_count == 3
        assert result == ok_body

    def test_raises_immediately_on_400_no_retry(self, monkeypatch):
        """HTTP 400 (non-retryable) → PikzelsError raised after exactly 1 call."""
        from congress_videos.modules.pikzels_client import PikzelsClient, PikzelsError

        bad_body = {"error": {"code": "INVALID_REQUEST", "message": "bad params"}}
        resp_400 = _make_session_response(400, bad_body)

        client = PikzelsClient(max_retries=5)

        with patch.object(client._session, "request", return_value=resp_400) as mock_req:
            with pytest.raises(PikzelsError) as exc_info:
                client._request("POST", "/v2/thumbnail/text", {"prompt": "bad"})

        assert mock_req.call_count == 1
        assert exc_info.value.status == 400

    def test_network_timeout_is_retryable(self, monkeypatch):
        """requests.Timeout on first call, then 200 on second → succeeds."""
        import requests as req_lib

        from congress_videos.modules.pikzels_client import PikzelsClient

        ok_body = {"output": "https://cdn.example.com/t.png", "request_id": "req_002"}
        resp_200 = _make_session_response(200, ok_body)

        client = PikzelsClient(max_retries=3)

        with patch.object(
            client._session,
            "request",
            side_effect=[req_lib.Timeout("connection timed out"), resp_200],
        ) as mock_req:
            with patch("time.sleep"):
                result = client._request("POST", "/v2/thumbnail/text", {"prompt": "ok"})

        assert mock_req.call_count == 2
        assert result == ok_body

    def test_network_connection_error_is_retryable(self, monkeypatch):
        """requests.ConnectionError on first call, then 200 → succeeds."""
        import requests as req_lib

        from congress_videos.modules.pikzels_client import PikzelsClient

        ok_body = {"output": "https://cdn.example.com/t.png", "request_id": "req_003"}
        resp_200 = _make_session_response(200, ok_body)

        client = PikzelsClient(max_retries=3)

        with patch.object(
            client._session,
            "request",
            side_effect=[req_lib.ConnectionError("no route to host"), resp_200],
        ) as mock_req:
            with patch("time.sleep"):
                result = client._request("POST", "/v2/thumbnail/text", {"prompt": "ok"})

        assert mock_req.call_count == 2
        assert result == ok_body

    def test_exhausted_retries_raises_pikzels_error(self, monkeypatch):
        """After max_retries+1 attempts still retryable → PikzelsError raised."""
        from congress_videos.modules.pikzels_client import PikzelsClient, PikzelsError

        error_body = {"error": {"code": "INTERNAL_ERROR", "message": "still down"}}
        resp_503 = _make_session_response(503, error_body)

        client = PikzelsClient(max_retries=2)

        with patch.object(client._session, "request", return_value=resp_503) as mock_req:
            with patch("time.sleep"):
                with pytest.raises(PikzelsError):
                    client._request("POST", "/v2/thumbnail/text", {"prompt": "x"})

        # max_retries=2 means attempts 0, 1, 2 → 3 total calls
        assert mock_req.call_count == 3


# ---------------------------------------------------------------------------
# T-04: thumbnail_from_text builds correct payload
# ---------------------------------------------------------------------------

class TestThumbnailFromText:
    """thumbnail_from_text sends the correct payload and returns the response dict."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_sends_prompt_in_payload(self, monkeypatch):
        """prompt field appears in the POST body."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        expected = {"output": "https://cdn.example.com/t.png", "request_id": "r1"}
        client = PikzelsClient()

        with patch.object(client, "_request", return_value=expected) as mock_req:
            result = client.thumbnail_from_text("A dramatic Spanish debate")

        mock_req.assert_called_once()
        _method, _path, payload = mock_req.call_args[0]
        assert payload["prompt"] == "A dramatic Spanish debate"
        assert result == expected

    def test_sends_style_and_persona_when_provided(self, monkeypatch):
        """style and persona optional fields are included when given."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()

        with patch.object(client, "_request", return_value={}) as mock_req:
            client.thumbnail_from_text(
                "Congress debate",
                style="dramatic_political",
                persona="orator_v1",
            )

        _m, _p, payload = mock_req.call_args[0]
        assert payload.get("style") == "dramatic_political"
        assert payload.get("persona") == "orator_v1"

    def test_none_fields_dropped_from_payload(self, monkeypatch):
        """None-valued optional fields must not appear in the payload."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()

        with patch.object(client, "_request", return_value={}) as mock_req:
            client.thumbnail_from_text("simple prompt")

        _m, _p, payload = mock_req.call_args[0]
        # None-valued keys should be stripped
        assert "persona" not in payload or payload["persona"] is not None
        assert "style" not in payload or payload["style"] is not None

    def test_support_image_base64_included_when_provided(self, monkeypatch):
        """support_image_base64 passes through to the payload."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        b64 = "data:image/png;base64,iVBORw=="

        with patch.object(client, "_request", return_value={}) as mock_req:
            client.thumbnail_from_text("prompt", support_image_base64=b64)

        _m, _p, payload = mock_req.call_args[0]
        assert payload.get("support_image_base64") == b64


# ---------------------------------------------------------------------------
# T-05: score_thumbnail sends base64-encoded image, not a URL
# ---------------------------------------------------------------------------

class TestScoreThumbnail:
    """score_thumbnail sends the locally-downloaded image as base64, not a URL."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_sends_image_base64_in_payload(self, monkeypatch):
        """image_base64 field is present in the request payload."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        score_response = {"main_score": 84.5, "request_id": "score_01"}
        client = PikzelsClient()

        fake_b64 = "data:image/png;base64,abc123=="
        with patch.object(client, "_request", return_value=score_response) as mock_req:
            result = client.score_thumbnail(image_base64=fake_b64)

        _m, _p, payload = mock_req.call_args[0]
        assert "image_base64" in payload
        assert payload["image_base64"] == fake_b64
        assert result["main_score"] == 84.5

    def test_score_uses_post_to_score_endpoint(self, monkeypatch):
        """score_thumbnail calls /v2/thumbnail/score via POST."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()

        with patch.object(client, "_request", return_value={"main_score": 70}) as mock_req:
            client.score_thumbnail(image_base64="data:image/png;base64,xyz==")

        method, path, _payload = mock_req.call_args[0]
        assert method == "POST"
        assert "/score" in path

    def test_score_returns_numeric_main_score(self, monkeypatch):
        """Return value includes a numeric main_score."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        expected = {"main_score": 72.3, "subscores": {}, "request_id": "r2"}

        with patch.object(client, "_request", return_value=expected):
            result = client.score_thumbnail(image_base64="data:image/png;base64,ok==")

        assert isinstance(result["main_score"], (int, float))
        assert result["main_score"] == 72.3


# ---------------------------------------------------------------------------
# T-06: download(url, dest_path) — GET, mkdir, write bytes
# ---------------------------------------------------------------------------

class TestDownload:
    """download issues a GET request, creates parent dirs, writes bytes."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_performs_get_to_url(self, monkeypatch, tmp_path):
        """GET is called with the supplied URL."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        dest = tmp_path / "sub" / "thumb.png"
        image_bytes = b"\x89PNG\r\n\x1a\n"

        fake_resp = MagicMock()
        fake_resp.content = image_bytes
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp) as mock_get:
            client.download("https://cdn.example.com/img.png", dest)

        mock_get.assert_called_once()
        called_url = mock_get.call_args[0][0]
        assert called_url == "https://cdn.example.com/img.png"

    def test_creates_parent_directory(self, monkeypatch, tmp_path):
        """Parent directory is created even when it does not exist."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        nested = tmp_path / "a" / "b" / "c" / "thumb.png"

        fake_resp = MagicMock()
        fake_resp.content = b"PNG"
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp):
            client.download("https://example.com/img.png", nested)

        assert nested.parent.exists()

    def test_writes_bytes_to_dest_path(self, monkeypatch, tmp_path):
        """Bytes from the response are written to dest_path."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        dest = tmp_path / "out.png"
        image_bytes = b"\x89PNG\r\n\x1a\n" + b"\x00" * 100

        fake_resp = MagicMock()
        fake_resp.content = image_bytes
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp):
            client.download("https://example.com/img.png", dest)

        assert dest.read_bytes() == image_bytes

    def test_existing_parent_directory_does_not_raise(self, monkeypatch, tmp_path):
        """If the parent already exists, download must not raise."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        dest = tmp_path / "thumb.png"  # tmp_path itself exists

        fake_resp = MagicMock()
        fake_resp.content = b"ok"
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp):
            client.download("https://example.com/img.png", dest)  # must not raise

        assert dest.exists()


# ---------------------------------------------------------------------------
# T-07: to_base64_data_url(image_bytes, mime_type) returns correct data URL
# ---------------------------------------------------------------------------

class TestToBase64DataUrl:
    """to_base64_data_url encodes bytes as a data URI."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_returns_data_url_with_correct_mime(self, monkeypatch):
        """Return value starts with 'data:<mime>;base64,'."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        result = to_base64_data_url(b"\x89PNG\r\n", "image/png")
        assert result.startswith("data:image/png;base64,")

    def test_base64_content_matches_input_bytes(self, monkeypatch):
        """The base64 portion decodes back to the original bytes."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        payload = b"\x00\x01\x02\x03\xde\xad\xbe\xef"
        result = to_base64_data_url(payload, "image/jpeg")

        # Strip "data:image/jpeg;base64,"
        prefix = "data:image/jpeg;base64,"
        assert result.startswith(prefix)
        b64_part = result[len(prefix):]
        decoded = base64.b64decode(b64_part)
        assert decoded == payload

    def test_empty_bytes_returns_valid_data_url(self, monkeypatch):
        """Empty bytes → a syntactically valid (empty-content) data URL."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        result = to_base64_data_url(b"", "image/png")
        assert result.startswith("data:image/png;base64,")
        # The base64 part of empty input is the empty string
        prefix = "data:image/png;base64,"
        b64_part = result[len(prefix):]
        assert base64.b64decode(b64_part) == b""

    def test_jpeg_mime_type(self, monkeypatch):
        """JPEG mime type is passed through correctly."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        result = to_base64_data_url(b"\xff\xd8\xff", "image/jpeg")
        assert "image/jpeg" in result


# ---------------------------------------------------------------------------
# TRIANGULATE — edge cases from the tasks spec
# ---------------------------------------------------------------------------


class TestTriangulateRetry:
    """Additional edge cases for retry logic (TRIANGULATE step)."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_network_timeout_exhausted_raises_pikzels_error(self, monkeypatch):
        """Repeated Timeout (exhausted retries) → PikzelsError raised, not Timeout."""
        import requests as req_lib

        from congress_videos.modules.pikzels_client import PikzelsClient, PikzelsError

        client = PikzelsClient(max_retries=1)

        with patch.object(
            client._session,
            "request",
            side_effect=req_lib.Timeout("timed out"),
        ):
            with patch("time.sleep"):
                with pytest.raises(PikzelsError) as exc_info:
                    client._request("POST", "/v2/thumbnail/text", {"prompt": "x"})

        assert exc_info.value.code == "NETWORK_ERROR"

    def test_network_connection_error_exhausted_raises_pikzels_error(self, monkeypatch):
        """Repeated ConnectionError (exhausted) → PikzelsError, not ConnectionError."""
        import requests as req_lib

        from congress_videos.modules.pikzels_client import PikzelsClient, PikzelsError

        client = PikzelsClient(max_retries=0)

        with patch.object(
            client._session,
            "request",
            side_effect=req_lib.ConnectionError("no route"),
        ):
            with pytest.raises(PikzelsError) as exc_info:
                client._request("POST", "/v2/thumbnail/text", {"prompt": "x"})

        assert exc_info.value.code == "NETWORK_ERROR"

    def test_retryable_5xx_exhausted_raises_pikzels_error(self, monkeypatch):
        """5xx exhausting max_retries eventually raises PikzelsError."""
        from congress_videos.modules.pikzels_client import PikzelsClient, PikzelsError

        error_body = {"error": {"code": "SERVICE_UNDER_MAINTENANCE", "message": "down"}}
        resp_503 = _make_session_response(503, error_body)

        client = PikzelsClient(max_retries=1)

        with patch.object(client._session, "request", return_value=resp_503):
            with patch("time.sleep"):
                with pytest.raises(PikzelsError) as exc_info:
                    client._request("POST", "/v2/thumbnail/text", {"prompt": "x"})

        assert exc_info.value.status == 503


class TestTriangulateDownload:
    """Edge cases for download (TRIANGULATE step)."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_already_existing_directory_no_error(self, monkeypatch, tmp_path):
        """Calling download twice to the same directory must not raise."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        dest1 = tmp_path / "thumb_a.png"
        dest2 = tmp_path / "thumb_b.png"

        fake_resp = MagicMock()
        fake_resp.content = b"PNG_DATA"
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp):
            client.download("https://example.com/a.png", dest1)
            client.download("https://example.com/b.png", dest2)  # same parent — no error

        assert dest1.exists()
        assert dest2.exists()

    def test_deeply_nested_path_created(self, monkeypatch, tmp_path):
        """Five-level-deep path is created by download."""
        from congress_videos.modules.pikzels_client import PikzelsClient

        client = PikzelsClient()
        dest = tmp_path / "a" / "b" / "c" / "d" / "e" / "thumb.png"

        fake_resp = MagicMock()
        fake_resp.content = b"ok"
        fake_resp.raise_for_status = MagicMock()

        with patch("requests.get", return_value=fake_resp):
            client.download("https://example.com/img.png", dest)

        assert dest.read_bytes() == b"ok"


class TestTriangulateToBase64DataUrl:
    """Edge cases for to_base64_data_url (TRIANGULATE step)."""

    @pytest.fixture(autouse=True)
    def _set_key(self, monkeypatch):
        monkeypatch.setenv("PIKZELS_API_KEY", "pkz_test-not-real")

    def test_large_payload_round_trips(self, monkeypatch):
        """A 10 KB payload encodes and decodes back to the same bytes."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        payload = bytes(range(256)) * 40  # 10 240 bytes
        result = to_base64_data_url(payload, "image/png")

        prefix = "data:image/png;base64,"
        b64_part = result[len(prefix):]
        assert base64.b64decode(b64_part) == payload

    def test_webp_mime_type(self, monkeypatch):
        """WebP MIME type is correctly embedded in the data URL."""
        from congress_videos.modules.pikzels_client import to_base64_data_url

        result = to_base64_data_url(b"RIFF\x00\x00\x00\x00WEBP", "image/webp")
        assert result.startswith("data:image/webp;base64,")
