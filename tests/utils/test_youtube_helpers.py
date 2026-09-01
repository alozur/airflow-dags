"""Tests for utils.youtube_helpers."""

from __future__ import annotations

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
        missing = str(tmp_path / "no_token.json")
        with pytest.raises(FileNotFoundError, match="Token file not found"):
            get_authenticated_youtube_service(missing)

    def test_pickle_token_raises_value_error(self, tmp_path):
        legacy = tmp_path / "token.pickle"
        legacy.write_bytes(b"placeholder")
        with pytest.raises(ValueError, match="Only .json tokens"):
            get_authenticated_youtube_service(str(legacy))

    def test_valid_token_file_returns_service(self, mocker, tmp_path):
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock()
        fake_creds.expired = False
        fake_creds.refresh_token = None

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )
        fake_service = MagicMock()
        mocker.patch("utils.youtube_helpers.build", return_value=fake_service)

        service = get_authenticated_youtube_service(str(token_file))

        assert service is fake_service

    def test_expired_token_is_refreshed(self, mocker, tmp_path):
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock()
        fake_creds.expired = True
        fake_creds.refresh_token = "some-refresh-token"
        fake_creds.to_json.return_value = "{}"

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )
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

    def test_thumbnail_failure_reported_but_video_upload_still_succeeds(self, tmp_path):
        """Issue #320: a failed thumbnails.set call must not fail the video

        upload — the video is published (success=True) but thumbnail_success
        is explicitly False so the caller (chapter-upload gate) can surface it.
        """
        video_file = tmp_path / "video.mp4"
        video_file.write_bytes(b"\x00" * 512)
        thumbnail_file = tmp_path / "thumbnail.jpg"
        thumbnail_file.write_bytes(b"\xff\xd8\xff" * 100)

        youtube = MagicMock()
        fake_response = {"id": "vid-with-bad-thumb"}
        insert_request = MagicMock()
        insert_request.next_chunk.return_value = (None, fake_response)
        youtube.videos.return_value.insert.return_value = insert_request

        with (
            patch("utils.youtube_helpers.MediaFileUpload"),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": False, "error": "thumbnail rejected"},
            ) as mock_set_thumbnail,
        ):
            result = upload_video_to_youtube(
                youtube,
                str(video_file),
                "Title",
                "Desc",
                thumbnail_file=str(thumbnail_file),
            )

        mock_set_thumbnail.assert_called_once()
        assert_success_result(result)
        assert result["thumbnail_success"] is False


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


# ---------------------------------------------------------------------------
# update_video_title (issue #102) — fetch-then-patch
# ---------------------------------------------------------------------------

class TestUpdateVideoTitle:
    """Spec: update_video_title fetch-then-patch — mutate only title,
    preserve categoryId/tags/description."""

    def _existing_snippet(self) -> dict:
        return {
            "title": "Old Title",
            "description": "A long description that must survive.",
            "categoryId": "25",
            "tags": ["congreso", "politica"],
        }

    def test_title_updated_other_fields_preserved(self):
        """GIVEN a live video with existing categoryId, tags, description
        WHEN update_video_title() is called with a new title
        THEN the title changes and all other snippet fields remain
             unchanged."""
        from utils.youtube_helpers import update_video_title

        youtube = MagicMock()
        youtube.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"id": "video-1", "snippet": self._existing_snippet()}]
        }
        youtube.videos.return_value.update.return_value.execute.return_value = {
            "id": "video-1"
        }

        result = update_video_title(youtube, "video-1", "New Provocative Title")

        assert_success_result(result)
        youtube.videos.return_value.list.assert_called_once()
        list_kwargs = youtube.videos.return_value.list.call_args.kwargs
        assert list_kwargs["part"] == "snippet"

        update_kwargs = youtube.videos.return_value.update.call_args.kwargs
        assert update_kwargs["part"] == "snippet"
        body_snippet = update_kwargs["body"]["snippet"]
        assert body_snippet["title"] == "New Provocative Title"
        assert body_snippet["categoryId"] == "25"
        assert body_snippet["tags"] == ["congreso", "politica"]
        assert body_snippet["description"] == "A long description that must survive."

    def test_video_not_found_returns_error(self):
        """GIVEN videos().list() returns no items
        WHEN update_video_title() is called
        THEN it returns {success: False, error: ...} without raising."""
        from utils.youtube_helpers import update_video_title

        youtube = MagicMock()
        youtube.videos.return_value.list.return_value.execute.return_value = {"items": []}

        result = update_video_title(youtube, "missing-id", "New Title")

        assert_error_result(result, "not found")
        youtube.videos.return_value.update.assert_not_called()

    def test_api_exception_returns_error_without_raising(self):
        """GIVEN videos().update().execute() raises
        WHEN update_video_title() is called
        THEN it returns {success: False, error: ...} without raising."""
        from utils.youtube_helpers import update_video_title

        youtube = MagicMock()
        youtube.videos.return_value.list.return_value.execute.return_value = {
            "items": [{"id": "video-1", "snippet": self._existing_snippet()}]
        }
        youtube.videos.return_value.update.return_value.execute.side_effect = Exception(
            "quotaExceeded"
        )

        result = update_video_title(youtube, "video-1", "New Title")

        assert_error_result(result, "quotaExceeded")
