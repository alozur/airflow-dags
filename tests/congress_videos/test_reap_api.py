"""Tests for congress_videos.reap_api module."""

from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest
import requests

from congress_videos.reap_api import ReapApiClient, ReapCreditsExhausted


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_response(status_code: int = 200, json_data: dict | None = None, text: str = "") -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 300
    resp.text = text
    resp.content = text.encode("utf-8")
    resp.json.return_value = json_data or {}
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.HTTPError(response=resp)
    else:
        resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# 7.1 — get_upload_url
# ---------------------------------------------------------------------------

class TestGetUploadUrl:

    def test_successful_response_returns_dict_with_id_and_upload_url(self, mocker):
        response_data = {"id": "upload-123", "uploadUrl": "https://s3.example.com/upload/123"}
        mocker.patch("requests.post", return_value=_make_response(200, response_data))
        client = ReapApiClient(api_key="test-key")

        result = client.get_upload_url("video.mp4")

        assert result["id"] == "upload-123"
        assert result["uploadUrl"] == "https://s3.example.com/upload/123"

    def test_successful_response_sends_auth_header(self, mocker):
        mock_post = mocker.patch(
            "requests.post",
            return_value=_make_response(200, {"id": "x", "uploadUrl": "http://s3"}),
        )
        client = ReapApiClient(api_key="my-api-key")

        client.get_upload_url("test.mp4")

        _, kwargs = mock_post.call_args
        headers = kwargs.get("headers", {})
        assert "Authorization" in headers
        assert "Bearer my-api-key" in headers["Authorization"]

    def test_401_response_raises_http_error(self, mocker):
        mocker.patch("requests.post", return_value=_make_response(401, text="Unauthorized"))
        client = ReapApiClient(api_key="bad-key")

        with pytest.raises(requests.HTTPError):
            client.get_upload_url("video.mp4")


# ---------------------------------------------------------------------------
# 7.2 — _check_credits_error
# ---------------------------------------------------------------------------

class TestCheckCreditsError:

    def test_429_with_credit_keyword_raises_reap_credits_exhausted(self):
        resp = _make_response(429, json_data={"message": "credit limit exceeded"}, text="credit limit exceeded")
        client = ReapApiClient(api_key="key")

        with pytest.raises(ReapCreditsExhausted):
            client._check_credits_error(resp)

    def test_429_without_credit_keyword_does_not_raise(self):
        resp = _make_response(429, json_data={"message": "rate limited, try again later"}, text="rate limited")
        client = ReapApiClient(api_key="key")

        # Should NOT raise — no credit/quota keyword present
        client._check_credits_error(resp)

    def test_403_with_billing_keyword_raises_reap_credits_exhausted(self):
        resp = _make_response(403, json_data={"message": "billing plan does not allow"}, text="billing plan")
        client = ReapApiClient(api_key="key")

        with pytest.raises(ReapCreditsExhausted):
            client._check_credits_error(resp)

    def test_402_with_balance_keyword_raises_reap_credits_exhausted(self):
        resp = _make_response(402, json_data={"message": "insufficient balance"}, text="insufficient balance")
        client = ReapApiClient(api_key="key")

        with pytest.raises(ReapCreditsExhausted):
            client._check_credits_error(resp)

    def test_200_response_does_not_raise(self):
        resp = _make_response(200, json_data={}, text="OK")
        client = ReapApiClient(api_key="key")

        # Should not raise
        client._check_credits_error(resp)

    @pytest.mark.parametrize("keyword", ["balance", "quota", "insufficient"])
    def test_429_with_various_credit_keywords_raises(self, keyword: str):
        resp = _make_response(429, json_data={"msg": f"your {keyword} has run out"}, text=f"{keyword} exhausted")
        client = ReapApiClient(api_key="key")

        with pytest.raises(ReapCreditsExhausted):
            client._check_credits_error(resp)


# ---------------------------------------------------------------------------
# 7.3 — get_project_clips pagination
# ---------------------------------------------------------------------------

class TestGetProjectClipsPagination:

    def test_single_page_returns_all_clips(self, mocker):
        clips = [{"clipId": "c1"}, {"clipId": "c2"}]
        page_response = _make_response(200, {"clips": clips})
        empty_response = _make_response(200, {"clips": []})
        mocker.patch("requests.get", side_effect=[page_response, empty_response])
        client = ReapApiClient(api_key="key")

        result = client.get_project_clips("proj-1")

        assert len(result) == 2
        assert result[0]["clipId"] == "c1"
        assert result[1]["clipId"] == "c2"

    def test_two_pages_returns_combined_list_and_makes_two_get_calls(self, mocker):
        page1_clips = [{"clipId": f"c{i}"} for i in range(50)]
        page2_clips = [{"clipId": "c50"}, {"clipId": "c51"}]
        page1_response = _make_response(200, {"clips": page1_clips})
        page2_response = _make_response(200, {"clips": page2_clips})
        empty_response = _make_response(200, {"clips": []})
        mock_get = mocker.patch("requests.get", side_effect=[page1_response, page2_response, empty_response])
        client = ReapApiClient(api_key="key")

        result = client.get_project_clips("proj-1")

        assert len(result) == 52
        assert mock_get.call_count >= 2

    def test_stops_pagination_when_page_is_less_than_page_size(self, mocker):
        """Single page with < 50 clips stops immediately — no second request needed."""
        first = _make_response(200, {"clips": [{"clipId": "c1"}]})
        mock_get = mocker.patch("requests.get", return_value=first)
        client = ReapApiClient(api_key="key")

        result = client.get_project_clips("proj-1")

        assert len(result) == 1
        # One request only — partial page signals end of pagination
        assert mock_get.call_count == 1

    def test_direct_list_response_is_handled(self, mocker):
        # Some endpoints return a raw list instead of {"clips": [...]}
        clips_resp = MagicMock()
        clips_resp.status_code = 200
        clips_resp.raise_for_status.return_value = None
        clips_resp.json.return_value = [{"clipId": "c1"}, {"clipId": "c2"}]

        empty_resp = MagicMock()
        empty_resp.status_code = 200
        empty_resp.raise_for_status.return_value = None
        empty_resp.json.return_value = []

        mocker.patch("requests.get", side_effect=[clips_resp, empty_resp])
        client = ReapApiClient(api_key="key")

        result = client.get_project_clips("proj-1")

        assert len(result) == 2


# ---------------------------------------------------------------------------
# 7.9 — upload_file
# ---------------------------------------------------------------------------

class TestUploadFile:

    def test_upload_file_no_auth_header_sent(self, mocker, tmp_path):
        video = tmp_path / "clip.mp4"
        video.write_bytes(b"\x00" * 16)

        mock_put = mocker.patch("requests.put", return_value=_make_response(200))
        client = ReapApiClient(api_key="my-key")

        client.upload_file("https://s3.example.com/upload/123", str(video))

        _, kwargs = mock_put.call_args
        headers = kwargs.get("headers", {})
        assert "Authorization" not in headers

    def test_upload_file_uses_content_type_video_mp4(self, mocker, tmp_path):
        video = tmp_path / "clip.mp4"
        video.write_bytes(b"\x00" * 8)

        mock_put = mocker.patch("requests.put", return_value=_make_response(200))
        client = ReapApiClient(api_key="key")

        client.upload_file("https://s3.example.com/upload/abc", str(video))

        _, kwargs = mock_put.call_args
        assert kwargs.get("headers", {}).get("Content-Type") == "video/mp4"


# ---------------------------------------------------------------------------
# 7.10 — create_clips_job
# ---------------------------------------------------------------------------

class TestCreateClipsJob:

    def test_returns_dict_with_project_id(self, mocker):
        mocker.patch(
            "requests.post",
            return_value=_make_response(200, {"projectId": "proj-xyz", "status": "processing"}),
        )
        client = ReapApiClient(api_key="key")

        result = client.create_clips_job("upload-001", language="es")

        assert result["project_id"] == "proj-xyz"

    def test_sends_upload_id_in_payload(self, mocker):
        mock_post = mocker.patch(
            "requests.post",
            return_value=_make_response(200, {"projectId": "p1", "id": "p1"}),
        )
        client = ReapApiClient(api_key="key")

        client.create_clips_job("upload-999")

        _, kwargs = mock_post.call_args
        assert kwargs["json"]["uploadId"] == "upload-999"

    def test_credits_exhausted_raises(self, mocker):
        mocker.patch("requests.post", return_value=_make_response(429, text="credit limit exceeded"))
        client = ReapApiClient(api_key="key")

        with pytest.raises(Exception):
            client.create_clips_job("upload-001")


# ---------------------------------------------------------------------------
# 7.11 — get_project_status
# ---------------------------------------------------------------------------

class TestGetProjectStatus:

    def test_returns_status_field_from_response(self, mocker):
        mocker.patch(
            "requests.get",
            return_value=_make_response(200, {"projectId": "p1", "status": "completed"}),
        )
        client = ReapApiClient(api_key="key")

        result = client.get_project_status("p1")

        assert result["status"] == "completed"

    def test_sends_project_id_as_query_param(self, mocker):
        mock_get = mocker.patch(
            "requests.get",
            return_value=_make_response(200, {"status": "processing"}),
        )
        client = ReapApiClient(api_key="key")

        client.get_project_status("proj-abc")

        _, kwargs = mock_get.call_args
        assert kwargs.get("params", {}).get("projectId") == "proj-abc"


# ---------------------------------------------------------------------------
# 7.12 — _normalize_clip
# ---------------------------------------------------------------------------

class TestNormalizeClip:

    def test_aliased_fields_are_mapped_to_canonical_names(self):
        client = ReapApiClient(api_key="key")
        clip = {
            "clipId": "c-001",
            "clipUrl": "https://cdn.reap.video/c-001.mp4",
            "viralityScore": 0.85,
        }

        result = client._normalize_clip(clip)

        assert result["clip_id"] == "c-001"
        assert result["clip_url"] == "https://cdn.reap.video/c-001.mp4"
        assert result["virality_score"] == 0.85

    def test_canonical_fields_not_overwritten_when_already_present(self):
        client = ReapApiClient(api_key="key")
        clip = {
            "clip_id": "existing-id",
            "clipId": "aliased-id",
            "clip_url": "https://existing.url/clip.mp4",
            "clipUrl": "https://aliased.url/clip.mp4",
            "virality_score": 0.9,
        }

        result = client._normalize_clip(clip)

        assert result["clip_id"] == "existing-id"
        assert result["clip_url"] == "https://existing.url/clip.mp4"
        assert result["virality_score"] == 0.9

    def test_missing_virality_defaults_to_zero(self):
        client = ReapApiClient(api_key="key")
        clip = {"clip_id": "c-002", "clip_url": "https://cdn.example.com/c.mp4"}

        result = client._normalize_clip(clip)

        assert result["virality_score"] == 0.0


# ---------------------------------------------------------------------------
# 7.13 — download_clip
# ---------------------------------------------------------------------------

class TestDownloadClip:

    def test_download_clip_creates_dest_dirs_and_writes_file(self, mocker, tmp_path):
        dest = str(tmp_path / "subdir" / "clip.mp4")

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        mocker.patch("requests.get", return_value=mock_response)

        client = ReapApiClient(api_key="key")
        client.download_clip("https://cdn.reap.video/clip.mp4", dest)

        import os
        assert os.path.exists(dest)
        with open(dest, "rb") as f:
            assert f.read() == b"chunk1chunk2"

    def test_download_clip_skips_empty_chunks(self, mocker, tmp_path):
        dest = str(tmp_path / "clip2.mp4")

        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.iter_content.return_value = [b"data", b"", b"more"]
        mocker.patch("requests.get", return_value=mock_response)

        client = ReapApiClient(api_key="key")
        client.download_clip("https://cdn.reap.video/clip2.mp4", dest)

        with open(dest, "rb") as f:
            assert f.read() == b"datamore"
