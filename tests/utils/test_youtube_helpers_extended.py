"""Extended tests for utils/youtube_helpers.py — upload_multiple_videos and upload_from_conf."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# upload_multiple_videos
# ---------------------------------------------------------------------------

class TestUploadMultipleVideos:

    def _make_service(self, mocker):
        """Return a mocked authenticated YouTube service."""
        fake_service = MagicMock()
        mocker.patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=fake_service,
        )
        return fake_service

    def test_empty_videos_returns_zero(self, tmp_path, mocker):
        """Empty videos list returns immediately with 0 uploads."""
        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(str(token_file), [])

        assert result["total_videos"] == 0
        assert result["successful_uploads"] == 0
        assert result["failed_uploads"] == 0

    def test_missing_video_file_field_counted_as_failed(self, tmp_path, mocker):
        """Video dict missing 'video_file' is counted as failed."""
        self._make_service(mocker)

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [{"title": "No File"}],
        )

        assert result["failed_uploads"] == 1
        assert result["successful_uploads"] == 0

    def test_missing_title_field_counted_as_failed(self, tmp_path, mocker):
        """Video dict missing 'title' is counted as failed."""
        self._make_service(mocker)

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [{"video_file": "/data/v.mp4"}],
        )

        assert result["failed_uploads"] == 1

    def test_successful_upload_increments_count(self, tmp_path, mocker):
        """Successful upload increments successful_uploads."""
        self._make_service(mocker)
        mocker.patch(
            "utils.youtube_helpers.upload_video_to_youtube",
            return_value={
                "success": True,
                "video_id": "yt-abc123",
                "video_url": "https://youtu.be/yt-abc123",
                "thumbnail_success": True,
                "error": None,
            },
        )

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [
                {
                    "video_file": "/data/v.mp4",
                    "title": "Test Video",
                    "description": "Desc",
                    "entry_id": "e1",
                }
            ],
        )

        assert result["successful_uploads"] == 1
        assert result["failed_uploads"] == 0
        assert result["upload_details"][0]["success"] is True
        assert result["upload_details"][0]["entry_id"] == "e1"

    def test_failed_upload_increments_failed_count(self, tmp_path, mocker):
        """Failed upload increments failed_uploads."""
        self._make_service(mocker)
        mocker.patch(
            "utils.youtube_helpers.upload_video_to_youtube",
            return_value={
                "success": False,
                "video_id": None,
                "video_url": None,
                "thumbnail_success": False,
                "error": "quota exceeded",
            },
        )

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [{"video_file": "/data/v.mp4", "title": "Test"}],
        )

        assert result["failed_uploads"] == 1
        assert result["upload_details"][0]["success"] is False

    def test_exception_during_auth_sets_all_failed(self, tmp_path, mocker):
        """Exception during authentication sets all videos as failed."""
        mocker.patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            side_effect=FileNotFoundError("token not found"),
        )

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [{"video_file": "/data/v.mp4", "title": "Test"}],
        )

        assert result["failed_uploads"] == 1

    def test_chapter_id_included_in_upload_detail(self, tmp_path, mocker):
        """chapter_id from video config is included in upload_detail."""
        self._make_service(mocker)
        mocker.patch(
            "utils.youtube_helpers.upload_video_to_youtube",
            return_value={
                "success": True,
                "video_id": "yt-xyz",
                "video_url": "https://youtu.be/yt-xyz",
                "thumbnail_success": None,
                "error": None,
            },
        )

        from utils.youtube_helpers import upload_multiple_videos

        token_file = tmp_path / "token.pkl"
        token_file.write_bytes(b"fake")

        result = upload_multiple_videos(
            str(token_file),
            [
                {
                    "video_file": "/data/v.mp4",
                    "title": "Test",
                    "chapter_id": "ch-99",
                }
            ],
        )

        assert result["upload_details"][0]["chapter_id"] == "ch-99"


# ---------------------------------------------------------------------------
# upload_from_conf
# ---------------------------------------------------------------------------

class TestUploadVideosFromConfig:

    def test_successful_upload_returns_results(self, tmp_path, mocker):
        """All uploads successful returns results dict without raising."""
        mocker.patch(
            "utils.youtube_helpers.upload_multiple_videos",
            return_value={
                "total_videos": 1,
                "successful_uploads": 1,
                "failed_uploads": 0,
                "upload_details": [{"success": True}],
            },
        )

        from utils.youtube_helpers import upload_videos_from_config

        conf = {
            "token_file": str(tmp_path / "token.pkl"),
            "videos": [{"video_file": "/v.mp4", "title": "T"}],
        }

        result = upload_videos_from_config(conf)

        assert result["successful_uploads"] == 1

    def test_failed_uploads_raises_exception(self, tmp_path, mocker):
        """Any failed uploads raises Exception with count in message."""
        mocker.patch(
            "utils.youtube_helpers.upload_multiple_videos",
            return_value={
                "total_videos": 2,
                "successful_uploads": 1,
                "failed_uploads": 1,
                "upload_details": [
                    {"success": True},
                    {"success": False, "error": "quota exceeded"},
                ],
            },
        )

        from utils.youtube_helpers import upload_videos_from_config

        conf = {
            "token_file": str(tmp_path / "token.pkl"),
            "videos": [
                {"video_file": "/v1.mp4", "title": "T1"},
                {"video_file": "/v2.mp4", "title": "T2"},
            ],
        }

        with pytest.raises(Exception, match="1 video"):
            upload_videos_from_config(conf)
