"""D1 [RED] Test: get_video_details propagates the real API snippet.title.

Issue #25: in test mode the mock dict spread (**video) was setting 'title'
from the placeholder in create_test_video_data(), and that placeholder
overwrote the real API title via ON CONFLICT upsert.

The fix is to explicitly set 'title': video_details['snippet']['title'] in
the enriched_video dict so the API value always wins.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from congress_videos.modules.youtube.download import create_test_video_data


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "fake-api-key")


_UNSET = object()


def _make_api_response(video_id: str, api_title: str, published_at=_UNSET):
    """Build a minimal youtube.videos().list() response."""
    snippet = {"title": api_title}
    if published_at is not _UNSET:
        snippet["publishedAt"] = published_at

    return {
        "items": [
            {
                "snippet": snippet,
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
                "title": mock_title,  # placeholder / mock title
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
        selected_video = create_test_video_data(
            "https://www.youtube.com/watch?v=ZBU0bVpYXM4"
        )
        placeholder = selected_video["videos"][0]
        real_api_title = "Sesión Plenaria 15 enero 2025"
        real_api_published_at = "2025-01-15T08:00:00Z"

        api_response = _make_api_response(
            placeholder["video_id"], real_api_title, real_api_published_at
        )
        mock_yt_service = MagicMock()
        mock_yt_service.videos.return_value.list.return_value.execute.return_value = (
            api_response
        )

        with patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=mock_yt_service,
        ):
            from congress_videos.modules.youtube.youtube_channel import (
                get_video_details,
            )

            result = get_video_details(selected_video, min_hours_since_end=0)

        assert result["total_videos"] == 1
        enriched = result["videos"][0]
        assert enriched["title"] == real_api_title
        assert enriched["title"] != placeholder["title"]
        assert enriched["published_at"] == real_api_published_at
        assert enriched["published_at"] != placeholder["published_at"]

    def test_placeholder_title_does_not_appear_when_api_returns_different_title(self):
        """Regression: the old spread (**video) allowed placeholder to propagate."""
        selected_video = create_test_video_data(
            "https://www.youtube.com/watch?v=ABCDEFGHIJK"
        )
        placeholder = selected_video["videos"][0]
        real_api_title = "Sesión Extraordinaria 20 febrero 2025"

        api_response = _make_api_response(placeholder["video_id"], real_api_title)
        mock_yt_service = MagicMock()
        mock_yt_service.videos.return_value.list.return_value.execute.return_value = (
            api_response
        )

        with patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=mock_yt_service,
        ):
            from congress_videos.modules.youtube.youtube_channel import (
                get_video_details,
            )

            result = get_video_details(selected_video, min_hours_since_end=0)

        enriched = result["videos"][0]
        assert enriched["title"] != placeholder["title"]
        assert enriched["title"] == real_api_title


@pytest.mark.parametrize("published_at", [_UNSET, ""])
def test_enriched_video_keeps_input_publication_time_when_api_omits_it(published_at):
    """Missing and empty API publication time preserve the selected record."""
    selected_video = create_test_video_data(
        "https://www.youtube.com/watch?v=ZBU0bVpYXM4"
    )
    placeholder = selected_video["videos"][0]
    api_response = _make_api_response(
        placeholder["video_id"], "API title", published_at
    )
    mock_yt_service = MagicMock()
    mock_yt_service.videos.return_value.list.return_value.execute.return_value = (
        api_response
    )

    with patch(
        "congress_videos.modules.youtube.youtube_channel.build",
        return_value=mock_yt_service,
    ):
        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details(selected_video, min_hours_since_end=0)

    assert result["videos"][0]["published_at"] == placeholder["published_at"]
