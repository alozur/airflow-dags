"""Tests for youtube_channel module — TASK-023."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


# --------------------------------------------------------------------------- #
# fetch_youtube_channel_videos
# --------------------------------------------------------------------------- #

class TestFetchYoutubeChannelVideos:

    def test_raises_value_error_when_api_key_missing(self, monkeypatch, mocker):
        """ValueError raised when YOUTUBE_API_KEY env var is not set."""
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            side_effect=AssertionError("should not be called"),
        )

        from congress_videos.modules.youtube.youtube_channel import fetch_youtube_channel_videos

        with pytest.raises(ValueError, match="YOUTUBE_API_KEY"):
            fetch_youtube_channel_videos("UC_test")

    def _make_search_response(self, items: list | None = None) -> dict:
        """Build a minimal valid YouTube search.list response."""
        if items is None:
            items = [
                {
                    "kind": "youtube#searchResult",
                    "id": {"kind": "youtube#video", "videoId": "hy1cnx-0Oww"},
                    "snippet": {
                        "publishedAt": "2025-05-22T10:00:00Z",
                        "title": "Sesion Plenaria (original) - 22 mayo",
                        "description": "Sesion plenaria del Congreso",
                        "thumbnails": {
                            "high": {
                                "url": "https://i.ytimg.com/vi/hy1cnx-0Oww/hqdefault.jpg"
                            }
                        },
                        "channelTitle": "Congreso de los Diputados",
                    },
                }
            ]
        return {"items": items}

    def test_returns_video_list_on_success(self, monkeypatch, mocker):
        """Valid API response returns correct total_videos and video dicts."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        api_response = self._make_search_response()
        fake_service = MagicMock()
        fake_service.search.return_value.list.return_value.execute.return_value = api_response
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import fetch_youtube_channel_videos

        result = fetch_youtube_channel_videos("UC_test_channel_id", max_results=5)

        assert result["total_videos"] == 1
        assert "videos" in result
        assert result["videos"][0]["video_id"] == "hy1cnx-0Oww"
        assert "Plenaria" in result["videos"][0]["title"]

    def test_returns_zero_videos_on_empty_response(self, monkeypatch, mocker):
        """Empty items list returns total_videos=0."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.search.return_value.list.return_value.execute.return_value = {"items": []}
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import fetch_youtube_channel_videos

        result = fetch_youtube_channel_videos("UC_test", max_results=10)

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_video_dict_has_required_keys(self, monkeypatch, mocker):
        """Each video in the result contains the expected keys."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        api_response = self._make_search_response()
        fake_service = MagicMock()
        fake_service.search.return_value.list.return_value.execute.return_value = api_response
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import fetch_youtube_channel_videos

        result = fetch_youtube_channel_videos("UC_test")

        for video in result["videos"]:
            assert "video_id" in video
            assert "title" in video
            assert "description" in video
            assert "published_at" in video
            assert "thumbnail_url" in video
            assert "channel_title" in video

    def test_raises_runtime_error_on_api_failure(self, monkeypatch, mocker):
        """API exception is wrapped in RuntimeError."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.search.return_value.list.return_value.execute.side_effect = Exception(
            "network error"
        )
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import fetch_youtube_channel_videos

        with pytest.raises(RuntimeError, match="Error fetching YouTube videos"):
            fetch_youtube_channel_videos("UC_test")


# --------------------------------------------------------------------------- #
# filter_plenary_session_videos
# --------------------------------------------------------------------------- #

class TestFilterPlenarySessionVideos:

    def _make_channel_videos(self, videos: list) -> dict:
        return {"total_videos": len(videos), "videos": videos}

    def _make_video(self, title: str, published_at: str, video_id: str = "vid001") -> dict:
        return {
            "video_id": video_id,
            "title": title,
            "description": "",
            "published_at": published_at,
            "thumbnail_url": "https://example.com/thumb.jpg",
            "channel_title": "Test Channel",
        }

    def test_empty_videos_returns_zero_matches(self):
        """Empty channel_videos input returns total_matches=0."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        result = filter_plenary_session_videos({}, "Sesion Plenaria", "2025-05-22")

        assert result["total_matches"] == 0
        assert result["videos"] == []

    def test_none_input_returns_zero_matches(self):
        """None input returns total_matches=0."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        result = filter_plenary_session_videos(None, "Sesion Plenaria", "2025-05-22")

        assert result["total_matches"] == 0

    def test_title_match_and_date_match_returns_one(self):
        """Video with matching title and date is included."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        video = self._make_video(
            title="Sesion Plenaria (original) - 22 mayo",
            published_at="2025-05-22T10:00:00Z",
        )
        channel_videos = self._make_channel_videos([video])

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="Sesion Plenaria",
            target_date="2025-05-22",
        )

        assert result["total_matches"] == 1
        assert result["videos"][0]["video_id"] == "vid001"

    def test_title_mismatch_returns_zero_matches(self):
        """Video whose title does NOT contain target_title is excluded."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        video = self._make_video(
            title="Comision de Economia",
            published_at="2025-05-22T10:00:00Z",
        )
        channel_videos = self._make_channel_videos([video])

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="Sesion Plenaria",
            target_date="2025-05-22",
        )

        assert result["total_matches"] == 0

    def test_date_outside_lookback_window_returns_zero_matches(self):
        """Video with correct title but a date outside the lookback window is excluded.
        Published 3 days earlier with the default lookback_days=1 -> dropped."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        video = self._make_video(
            title="Sesion Plenaria (original)",
            published_at="2025-05-19T10:00:00Z",  # 3 days earlier, outside window
        )
        channel_videos = self._make_channel_videos([video])

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="Sesion Plenaria",
            target_date="2025-05-22",
        )

        assert result["total_matches"] == 0

    def test_title_match_is_case_insensitive(self):
        """Title matching is case-insensitive."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        video = self._make_video(
            title="SESION PLENARIA (ORIGINAL)",
            published_at="2025-05-22T08:00:00Z",
        )
        channel_videos = self._make_channel_videos([video])

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="sesion plenaria",
            target_date="2025-05-22",
        )

        assert result["total_matches"] == 1

    def test_target_date_preserved_in_result(self):
        """Result dict always contains the target_date used for filtering."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        result = filter_plenary_session_videos(
            {"videos": []},
            target_title="X",
            target_date="2025-01-15",
        )

        assert result["target_date"] == "2025-01-15"

    def test_multiple_matches_all_returned(self):
        """When multiple videos match, all are included."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        videos = [
            self._make_video("Sesion Plenaria A", "2025-05-22T09:00:00Z", "vid-a"),
            self._make_video("Sesion Plenaria B", "2025-05-22T11:00:00Z", "vid-b"),
            self._make_video("Comision", "2025-05-22T10:00:00Z", "vid-c"),
        ]
        channel_videos = self._make_channel_videos(videos)

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="Sesion Plenaria",
            target_date="2025-05-22",
        )

        assert result["total_matches"] == 2

    def test_lookback_range_keeps_today_and_yesterday_drops_older(self):
        """With lookback_days=1 and a fixed reference date, videos published on
        the target date and the day before are KEPT; one 3 days earlier is DROPPED."""
        from congress_videos.modules.youtube.youtube_channel import filter_plenary_session_videos

        target_date = "2025-05-22"
        videos = [
            self._make_video("Sesion Plenaria HOY", "2025-05-22T09:00:00Z", "vid-today"),
            self._make_video("Sesion Plenaria AYER", "2025-05-21T18:00:00Z", "vid-yesterday"),
            self._make_video("Sesion Plenaria VIEJA", "2025-05-19T10:00:00Z", "vid-old"),
        ]
        channel_videos = self._make_channel_videos(videos)

        result = filter_plenary_session_videos(
            channel_videos,
            target_title="Sesion Plenaria",
            target_date=target_date,
            lookback_days=1,
        )

        kept_ids = {v["video_id"] for v in result["videos"]}
        assert kept_ids == {"vid-today", "vid-yesterday"}
        assert "vid-old" not in kept_ids
        assert result["total_matches"] == 2


# --------------------------------------------------------------------------- #
# get_video_details — ISO 8601 duration parsing
# --------------------------------------------------------------------------- #

class TestGetVideoDetails:

    def test_empty_plenary_videos_returns_empty_result(self, monkeypatch):
        """Empty input returns total_videos=0 without touching API."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details({"videos": []})

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_none_input_returns_empty_result(self, monkeypatch):
        """None input returns total_videos=0."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details(None)

        assert result["total_videos"] == 0

    def test_parses_full_iso8601_duration(self, monkeypatch, mocker):
        """PT1H30M15S -> duration_seconds=5415."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "contentDetails": {"duration": "PT1H30M15S"},
                    "liveStreamingDetails": {
                        "actualStartTime": "2025-05-22T09:00:00Z",
                        "actualEndTime": "2025-05-22T11:00:00Z",
                    },
                    "snippet": {"title": "Test Video"},
                }
            ]
        }
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        plenary_videos = {
            "videos": [
                {
                    "video_id": "vid-abc",
                    "title": "Test Video",
                    "description": "",
                    "published_at": "2025-05-22T09:00:00Z",
                    "thumbnail_url": "https://example.com/t.jpg",
                    "channel_title": "Test",
                }
            ]
        }
        result = get_video_details(plenary_videos)

        assert result["total_videos"] == 1
        assert result["videos"][0]["duration_seconds"] == 5415  # 1*3600 + 30*60 + 15
        assert result["videos"][0]["duration_formatted"] == "1:30:15"

    def test_parses_minutes_only_duration(self, monkeypatch, mocker):
        """PT45M -> duration_seconds=2700."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "contentDetails": {"duration": "PT45M"},
                    "liveStreamingDetails": {"actualEndTime": "2025-05-22T11:00:00Z"},
                    "snippet": {"title": "Short"},
                }
            ]
        }
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        plenary_videos = {
            "videos": [
                {
                    "video_id": "vid-xyz",
                    "title": "Short",
                    "description": "",
                    "published_at": "2025-05-22T09:00:00Z",
                    "thumbnail_url": "https://example.com/t.jpg",
                    "channel_title": "Test",
                }
            ]
        }
        result = get_video_details(plenary_videos)

        assert result["videos"][0]["duration_seconds"] == 2700

    def test_raises_value_error_when_api_key_missing(self, monkeypatch):
        """ValueError raised when YOUTUBE_API_KEY is not set."""
        monkeypatch.delenv("YOUTUBE_API_KEY", raising=False)

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        with pytest.raises(ValueError, match="YOUTUBE_API_KEY"):
            get_video_details({"videos": [{"video_id": "vid-x", "title": "T"}]})

    def test_skips_video_not_found_in_api(self, monkeypatch, mocker):
        """Video not returned by API is skipped; total_videos reflects only found."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {"items": []}
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        plenary_videos = {
            "videos": [
                {
                    "video_id": "missing-id",
                    "title": "Gone",
                    "description": "",
                    "published_at": "2025-05-22T09:00:00Z",
                    "thumbnail_url": "https://example.com/t.jpg",
                    "channel_title": "Test",
                }
            ]
        }
        result = get_video_details(plenary_videos)

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_enriched_video_has_youtube_url(self, monkeypatch, mocker):
        """Enriched video includes youtube_url field."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "contentDetails": {"duration": "PT10M"},
                    "liveStreamingDetails": {"actualEndTime": "2025-05-22T11:00:00Z"},
                    "snippet": {"title": "Test"},
                }
            ]
        }
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=fake_service,
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        plenary_videos = {
            "videos": [
                {
                    "video_id": "abc123",
                    "title": "Test",
                    "description": "",
                    "published_at": "2025-05-22T09:00:00Z",
                    "thumbnail_url": "https://example.com/t.jpg",
                    "channel_title": "Test",
                }
            ]
        }
        result = get_video_details(plenary_videos)

        assert "youtube_url" in result["videos"][0]
        assert "abc123" in result["videos"][0]["youtube_url"]


# --------------------------------------------------------------------------- #
# get_video_details — VOD freshness guard
# --------------------------------------------------------------------------- #

class TestGetVideoDetailsFreshnessGuard:

    def _plenary(self, video_id: str = "vid-fresh") -> dict:
        return {
            "videos": [
                {
                    "video_id": video_id,
                    "title": "Sesion Plenaria",
                    "description": "",
                    "published_at": "2025-05-22T09:00:00Z",
                    "thumbnail_url": "https://example.com/t.jpg",
                    "channel_title": "Test",
                }
            ]
        }

    def _fake_service(self, actual_end_time):
        """Build a fake YouTube service returning a single video with the given
        actualEndTime (None means the key is absent)."""
        live_details = {}
        if actual_end_time is not None:
            live_details["actualEndTime"] = actual_end_time

        fake_service = MagicMock()
        fake_service.videos.return_value.list.return_value.execute.return_value = {
            "items": [
                {
                    "contentDetails": {"duration": "PT1H"},
                    "liveStreamingDetails": live_details,
                    "snippet": {"title": "Sesion Plenaria"},
                }
            ]
        }
        return fake_service

    def test_skips_video_without_actual_end_time(self, monkeypatch, mocker):
        """actualEndTime=None (still live / no data) is skipped."""
        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=self._fake_service(None),
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details(self._plenary(), min_hours_since_end=2)

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_skips_video_ended_10_minutes_ago(self, monkeypatch, mocker):
        """A broadcast that ended 10 minutes ago is under the 2h margin -> skipped."""
        from datetime import datetime, timedelta, timezone

        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")
        ended_recently = (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=self._fake_service(ended_recently),
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details(self._plenary(), min_hours_since_end=2)

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_keeps_video_ended_5_hours_ago(self, monkeypatch, mocker):
        """A broadcast that ended 5 hours ago is past the 2h margin -> kept."""
        from datetime import datetime, timedelta, timezone

        monkeypatch.setenv("YOUTUBE_API_KEY", "fake-key")
        ended_long_ago = (
            datetime.now(timezone.utc) - timedelta(hours=5)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        mocker.patch(
            "congress_videos.modules.youtube.youtube_channel.build",
            return_value=self._fake_service(ended_long_ago),
        )

        from congress_videos.modules.youtube.youtube_channel import get_video_details

        result = get_video_details(self._plenary(), min_hours_since_end=2)

        assert result["total_videos"] == 1
        assert result["videos"][0]["video_id"] == "vid-fresh"


# --------------------------------------------------------------------------- #
# filter_unprocessed_videos — idempotency pre-download filter
# --------------------------------------------------------------------------- #

class TestFilterUnprocessedVideos:

    def _plenary(self, video_ids, target_date="2025-10-08"):
        return {
            "total_matches": len(video_ids),
            "videos": [{"video_id": vid, "title": f"T-{vid}"} for vid in video_ids],
            "target_date": target_date,
        }

    def test_drops_processed_keeps_unprocessed_and_preserves_target_date(self, mocker):
        """Processed video_ids are dropped, unprocessed kept, target_date preserved."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )
        mock_db_cls.return_value.get_processed_video_ids.return_value = {"A"}

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        result = filter_unprocessed_videos(self._plenary(["A", "B"]))

        kept_ids = [v["video_id"] for v in result["videos"]]
        assert kept_ids == ["B"]
        assert result["total_matches"] == 1
        assert result["target_date"] == "2025-10-08"

    def test_all_processed_yields_zero_matches(self, mocker):
        """When every candidate is processed, total_matches collapses to 0."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )
        mock_db_cls.return_value.get_processed_video_ids.return_value = {"A", "B"}

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        result = filter_unprocessed_videos(self._plenary(["A", "B"]))

        assert result["videos"] == []
        assert result["total_matches"] == 0
        assert result["target_date"] == "2025-10-08"

    def test_none_processed_keeps_all(self, mocker):
        """No processed rows -> nothing filtered."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )
        mock_db_cls.return_value.get_processed_video_ids.return_value = set()

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        result = filter_unprocessed_videos(self._plenary(["A", "B"]))

        assert [v["video_id"] for v in result["videos"]] == ["A", "B"]
        assert result["total_matches"] == 2

    def test_empty_input_does_not_touch_db(self, mocker):
        """Empty 'videos' returns input unchanged and never instantiates the DB."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        empty = {"total_matches": 0, "videos": [], "target_date": "2025-10-08"}
        result = filter_unprocessed_videos(empty)

        assert result == empty
        mock_db_cls.assert_not_called()

    def test_none_input_does_not_touch_db(self, mocker):
        """None input returns a safe empty dict and never instantiates the DB."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        result = filter_unprocessed_videos(None)

        assert result == {"total_matches": 0, "videos": []}
        mock_db_cls.assert_not_called()

    def test_db_error_propagates_fail_closed(self, mocker):
        """DB error must propagate (fail-closed) — videos are NOT treated as unprocessed."""
        mock_db_cls = mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB"
        )
        mock_db_cls.return_value.get_processed_video_ids.side_effect = RuntimeError("db down")

        from congress_videos.modules.youtube.youtube_channel import filter_unprocessed_videos

        with pytest.raises(RuntimeError, match="db down"):
            filter_unprocessed_videos(self._plenary(["A", "B"]))
