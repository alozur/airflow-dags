"""Tests for utils.youtube_helpers."""

from __future__ import annotations

import pickle
from unittest.mock import MagicMock, patch

import pytest

from tests.helpers.assertions import assert_error_result, assert_success_result
from utils.youtube_helpers import (
    get_authenticated_youtube_service,
    set_thumbnail_for_video,
    upload_video_to_youtube,
    validate_upload_config,
)


# ---------------------------------------------------------------------------
# validate_upload_config
# ---------------------------------------------------------------------------

class TestValidateUploadConfig:
    def test_none_raises_value_error(self):
        with pytest.raises(ValueError, match="No configuration provided"):
            validate_upload_config(None)

    def test_empty_dict_raises_value_error(self):
        with pytest.raises(ValueError, match="No configuration provided"):
            validate_upload_config({})

    def test_missing_token_file_raises_value_error(self):
        with pytest.raises(ValueError, match="token_file"):
            validate_upload_config({"videos": []})

    def test_missing_videos_raises_value_error(self):
        with pytest.raises(ValueError, match="videos"):
            validate_upload_config({"token_file": "/path/token.pickle"})

    def test_videos_not_list_raises_value_error(self):
        with pytest.raises(ValueError, match="videos"):
            validate_upload_config({"token_file": "/t.pickle", "videos": "not-a-list"})

    def test_empty_videos_list_raises_value_error(self):
        with pytest.raises(ValueError, match="No videos provided"):
            validate_upload_config({"token_file": "/t.pickle", "videos": []})

    def test_video_missing_video_file_raises_value_error(self):
        with pytest.raises(ValueError, match="video_file"):
            validate_upload_config({
                "token_file": "/t.pickle",
                "videos": [{"title": "My Video"}],
            })

    def test_video_missing_title_raises_value_error(self):
        with pytest.raises(ValueError, match="title"):
            validate_upload_config({
                "token_file": "/t.pickle",
                "videos": [{"video_file": "/v.mp4"}],
            })

    def test_valid_config_returns_same_dict(self):
        conf = {
            "token_file": "/path/to/token.pickle",
            "videos": [{"video_file": "/v.mp4", "title": "My Title"}],
        }
        result = validate_upload_config(conf)
        assert result is conf


# ---------------------------------------------------------------------------
# get_authenticated_youtube_service
# ---------------------------------------------------------------------------

class TestGetAuthenticatedYoutubeService:
    def test_missing_token_file_raises_file_not_found(self, tmp_path):
        missing = str(tmp_path / "no_token.pickle")
        with pytest.raises(FileNotFoundError, match="Token file not found"):
            get_authenticated_youtube_service(missing)

    def test_valid_token_file_returns_service(self, mocker, tmp_path):
        token_file = tmp_path / "token.pickle"
        # Write placeholder bytes so os.path.exists passes
        token_file.write_bytes(b"placeholder")

        fake_creds = MagicMock()
        fake_creds.expired = False
        fake_creds.refresh_token = None

        mocker.patch("builtins.open", mocker.mock_open(read_data=b""))
        mocker.patch("utils.youtube_helpers.pickle.load", return_value=fake_creds)
        fake_service = MagicMock()
        mocker.patch("utils.youtube_helpers.build", return_value=fake_service)

        service = get_authenticated_youtube_service(str(token_file))

        assert service is fake_service

    def test_expired_token_is_refreshed(self, mocker, tmp_path):
        token_file = tmp_path / "token.pickle"
        token_file.write_bytes(b"placeholder")

        fake_creds = MagicMock()
        fake_creds.expired = True
        fake_creds.refresh_token = "some-refresh-token"

        mocker.patch("builtins.open", mocker.mock_open(read_data=b""))
        mocker.patch("utils.youtube_helpers.pickle.load", return_value=fake_creds)
        mocker.patch("utils.youtube_helpers.pickle.dump")
        mocker.patch("utils.youtube_helpers.build")
        mocker.patch("utils.youtube_helpers.Request")

        get_authenticated_youtube_service(str(token_file))

        fake_creds.refresh.assert_called_once()


# ---------------------------------------------------------------------------
# upload_video_to_youtube
# ---------------------------------------------------------------------------

class TestUploadVideoToYoutube:
    def test_missing_video_file_returns_error(self, tmp_path):
        youtube = MagicMock()
        missing = str(tmp_path / "video.mp4")
        result = upload_video_to_youtube(youtube, missing, "Title", "Desc")
        assert_error_result(result, "not found")

    def test_successful_upload_returns_video_id(self, tmp_path):
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"\x00" * 1024)

        youtube = MagicMock()
        fake_response = {"id": "abc123XYZ"}
        insert_request = MagicMock()
        insert_request.next_chunk.return_value = (None, fake_response)
        youtube.videos.return_value.insert.return_value = insert_request

        with patch("utils.youtube_helpers.MediaFileUpload"):
            result = upload_video_to_youtube(youtube, str(video_file), "My Title", "My Desc")

        assert_success_result(result)
        assert result["video_id"] == "abc123XYZ"
        assert result["video_url"] == "https://www.youtube.com/watch?v=abc123XYZ"

    def test_upload_exception_returns_error(self, tmp_path):
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"\x00" * 512)

        youtube = MagicMock()
        youtube.videos.return_value.insert.side_effect = Exception("API quota exceeded")

        with patch("utils.youtube_helpers.MediaFileUpload"):
            result = upload_video_to_youtube(youtube, str(video_file), "Title", "Desc")

        assert_error_result(result, "quota exceeded")

    def test_privacy_status_passed_correctly(self, tmp_path):
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"\x00" * 512)

        youtube = MagicMock()
        fake_response = {"id": "pub999"}
        insert_request = MagicMock()
        insert_request.next_chunk.return_value = (None, fake_response)
        youtube.videos.return_value.insert.return_value = insert_request

        with patch("utils.youtube_helpers.MediaFileUpload"):
            upload_video_to_youtube(
                youtube, str(video_file), "Title", "Desc", privacy_status="public"
            )

        call_kwargs = youtube.videos.return_value.insert.call_args.kwargs
        assert call_kwargs["body"]["status"]["privacyStatus"] == "public"


# ---------------------------------------------------------------------------
# set_thumbnail_for_video
# ---------------------------------------------------------------------------

class TestSetThumbnailForVideo:
    def test_missing_thumbnail_file_returns_error(self, tmp_path):
        youtube = MagicMock()
        missing = str(tmp_path / "thumb.jpg")
        result = set_thumbnail_for_video(youtube, "video-id", missing)
        assert_error_result(result, "not found")

    def test_successful_thumbnail_upload_returns_success(self, tmp_path):
        thumb_file = tmp_path / "thumbnail.jpg"
        thumb_file.write_bytes(b"\xff\xd8\xff" * 100)

        youtube = MagicMock()
        youtube.thumbnails.return_value.set.return_value.execute.return_value = {}

        with patch("utils.youtube_helpers.MediaFileUpload"):
            result = set_thumbnail_for_video(youtube, "video-abc", str(thumb_file))

        assert_success_result(result)

    def test_thumbnail_api_exception_returns_error(self, tmp_path):
        thumb_file = tmp_path / "thumbnail.jpg"
        thumb_file.write_bytes(b"\xff\xd8\xff" * 100)

        youtube = MagicMock()
        youtube.thumbnails.return_value.set.return_value.execute.side_effect = Exception(
            "thumbnail size exceeded"
        )

        with patch("utils.youtube_helpers.MediaFileUpload"):
            result = set_thumbnail_for_video(youtube, "video-abc", str(thumb_file))

        assert_error_result(result, "thumbnail size exceeded")
