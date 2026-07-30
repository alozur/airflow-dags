"""D1 [RED] Test: get_video_details propagates the real API snippet.title.

Issue #25: in test mode the mock dict spread (**video) was setting 'title'
from the placeholder in create_test_video_data(), and that placeholder
overwrote the real API title via ON CONFLICT upsert.

The fix is to explicitly set 'title': video_details['snippet']['title'] in
the enriched_video dict so the API value always wins.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "fake-api-key")


def _make_api_response(video_id: str, api_title: str):
    """Build a minimal youtube.videos().list() response."""
    return {
        "items": [
            {
                "snippet": {
                    "title": api_title,
                },
                "contentDetails": {
                    "duration": "PT2H30M15S",
                },
                "liveStreamingDetails": {
                    "actualStartTime": "2025-01-15T08:00:00Z",
                    "actualEndTime": "2025-01-15T20:00:00Z",
                },
            }
        ]
    }


def _plenary_videos(video_id: str, mock_title: str):
    """Input dict as produced by create_test_video_data / filter_plenary_session_videos."""
    return {
        "total_matches": 1,
        "videos": [
            {
                "video_id": video_id,
                "title": mock_title,         # placeholder / mock title
                "url": f"https://www.youtube.com/watch?v={video_id}",
                "published_at": "2025-01-15T08:00:00Z",
                "is_live": False,
                "is_upcoming": False,
            }
        ],
    }


class TestGetVideoDetailsPropagatesRealTitle:

    def test_enriched_video_contains_real_api_title(self):
        """The 'title' key in the enriched video dict must come from snippet.title,
        NOT from the placeholder title in the input mock dict."""
        video_id = "ZBU0bVpYXM4"
        mock_placeholder_title = "Test Video - Sesión Plenaria"
        real_api_title = "Sesión Plenaria 15 enero 2025"

        api_response = _make_api_response(video_id, real_api_title)
        mock_yt_service = MagicMock()
        mock_yt_service.videos.return_value.list.return_value.execute.return_value = api_response

        with patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=mock_yt_service,
        ):
            from congress_videos.modules.youtube.youtube_channel import get_video_details

            result = get_video_details(
                _plenary_videos(video_id, mock_placeholder_title),
                min_hours_since_end=0,
            )

        assert result["total_videos"] == 1
        enriched = result["videos"][0]
        # The real API title must win over the placeholder
        assert enriched["title"] == real_api_title, (
            f"Expected real API title '{real_api_title}', got '{enriched['title']}'"
        )

    def test_placeholder_title_does_not_appear_when_api_returns_different_title(self):
        """Regression: the old spread (**video) allowed placeholder to propagate."""
        video_id = "ABCDEFGHIJK"
        mock_placeholder_title = "Test Video - Sesión Plenaria"
        real_api_title = "Sesión Extraordinaria 20 febrero 2025"

        api_response = _make_api_response(video_id, real_api_title)
        mock_yt_service = MagicMock()
        mock_yt_service.videos.return_value.list.return_value.execute.return_value = api_response

        with patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=mock_yt_service,
        ):
            from congress_videos.modules.youtube.youtube_channel import get_video_details

            result = get_video_details(
                _plenary_videos(video_id, mock_placeholder_title),
                min_hours_since_end=0,
            )

        enriched = result["videos"][0]
        assert enriched["title"] != mock_placeholder_title, (
            "Placeholder title must NOT appear in the enriched video dict"
        )
        assert enriched["title"] == real_api_title
