"""[RED] Tests for utils.youtube_helpers.get_youtube_analytics_service.

Verifies:
- Function calls build('youtubeAnalytics', 'v2', credentials=...) with correct args
- Function reuses the same JSON-load + refresh path as get_authenticated_youtube_service
- Missing token file raises FileNotFoundError
- Expired token is refreshed before building the service
- Returns the service object produced by build()
"""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest

from utils.youtube_helpers import get_youtube_analytics_service


class TestGetYoutubeAnalyticsServiceMissingToken:
    def test_missing_token_file_raises_file_not_found(self, tmp_path):
        """Must raise FileNotFoundError when token file does not exist."""
        missing = str(tmp_path / "no_analytics_token.json")
        with pytest.raises(FileNotFoundError, match="Token file not found"):
            get_youtube_analytics_service(missing)


class TestGetYoutubeAnalyticsServiceBuildsCorrectApi:
    def test_calls_build_with_youtube_analytics_v2(self, mocker, tmp_path):
        """Must call build('youtubeAnalytics', 'v2', ...) — not the Data API."""
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock()
        fake_creds.expired = False
        fake_creds.refresh_token = None

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )
        mock_build = mocker.patch("utils.youtube_helpers.build", return_value=MagicMock())

        get_youtube_analytics_service(str(token_file))

        # Verify the positional args are ('youtubeAnalytics', 'v2')
        assert mock_build.call_count == 1
        pos_args = mock_build.call_args.args
        assert pos_args[0] == "youtubeAnalytics", (
            f"First arg to build() must be 'youtubeAnalytics', got '{pos_args[0]}'"
        )
        assert pos_args[1] == "v2", f"Second arg to build() must be 'v2', got '{pos_args[1]}'"

    def test_returns_service_from_build(self, mocker, tmp_path):
        """Must return the object produced by build()."""
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock()
        fake_creds.expired = False
        fake_creds.refresh_token = None

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )

        fake_service = MagicMock(name="analytics_service")
        mocker.patch("utils.youtube_helpers.build", return_value=fake_service)

        result = get_youtube_analytics_service(str(token_file))

        assert result is fake_service

    def test_credentials_passed_to_build(self, mocker, tmp_path):
        """Loaded credentials must be forwarded to build() as keyword arg."""
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock(name="my_creds")
        fake_creds.expired = False
        fake_creds.refresh_token = None

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )
        mock_build = mocker.patch("utils.youtube_helpers.build", return_value=MagicMock())

        get_youtube_analytics_service(str(token_file))

        kwargs = mock_build.call_args.kwargs
        assert "credentials" in kwargs, "build() must receive credentials= keyword arg"
        assert kwargs["credentials"] is fake_creds


class TestGetYoutubeAnalyticsServiceTokenRefresh:
    def test_expired_token_is_refreshed_before_build(self, mocker, tmp_path):
        """Expired credentials must be refreshed before build() is called."""
        token_file = tmp_path / "token.json"
        token_file.write_text('{"refresh_token": "rt", "scopes": []}')

        fake_creds = MagicMock()
        fake_creds.expired = True
        fake_creds.refresh_token = "refresh-tok"
        fake_creds.to_json.return_value = "{}"

        mocker.patch(
            "utils.youtube_helpers.Credentials.from_authorized_user_info",
            return_value=fake_creds,
        )
        mocker.patch("utils.youtube_helpers.build", return_value=MagicMock())
        mocker.patch("utils.youtube_helpers.Request")

        get_youtube_analytics_service(str(token_file))

        fake_creds.refresh.assert_called_once()
