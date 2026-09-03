"""E1 [RED] Test: min_hours_since_end default is 12 everywhere.

Verifies three sites:
1. get_video_details() function signature default = 12
2. DAG param default = 12
3. DAG callsite .get("min_hours_since_end", N) fallback = 12
"""

from __future__ import annotations

import inspect
import os
from datetime import UTC
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def set_api_key(monkeypatch):
    monkeypatch.setenv("YOUTUBE_API_KEY", "fake-api-key")


class TestGetVideoDetailsFunctionSignatureDefault:

    def test_min_hours_since_end_default_is_12(self):
        """get_video_details() must have min_hours_since_end default = 12."""
        from congress_videos.modules.youtube.youtube_channel import get_video_details

        sig = inspect.signature(get_video_details)
        param = sig.parameters.get("min_hours_since_end")
        assert param is not None, "get_video_details must have min_hours_since_end parameter"
        assert param.default == 12, (
            f"min_hours_since_end default must be 12, got {param.default!r}"
        )

    def test_video_ended_3h_ago_is_excluded_with_default(self):
        """With the default 12h margin, a video that ended 3h ago is skipped."""
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock, patch

        video_id = "EXCLUDED_3H"
        end_time = (datetime.now(UTC) - timedelta(hours=3)).isoformat().replace("+00:00", "Z")

        api_response = {
            "items": [
                {
                    "snippet": {"title": "Test"},
                    "contentDetails": {"duration": "PT3H"},
                    "liveStreamingDetails": {
                        "actualStartTime": "2025-01-15T08:00:00Z",
                        "actualEndTime": end_time,
                    },
                }
            ]
        }
        mock_yt = MagicMock()
        mock_yt.videos.return_value.list.return_value.execute.return_value = api_response

        plenary = {
            "total_matches": 1,
            "videos": [{"video_id": video_id, "title": "T", "url": "u",
                        "published_at": "2025-01-15T08:00:00Z",
                        "is_live": False, "is_upcoming": False}],
        }

        with patch("congress_videos.modules.youtube.youtube_channel.build", return_value=mock_yt):
            from congress_videos.modules.youtube.youtube_channel import get_video_details
            result = get_video_details(plenary)  # no explicit min_hours_since_end — use default

        assert result["total_videos"] == 0, (
            "Video ended 3h ago must be excluded with the 12h default"
        )

    def test_video_ended_13h_ago_is_included_with_default(self):
        """With the default 12h margin, a video that ended 13h ago is included."""
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock, patch

        video_id = "INCLUDED_13H"
        end_time = (datetime.now(UTC) - timedelta(hours=13)).isoformat().replace("+00:00", "Z")

        api_response = {
            "items": [
                {
                    "snippet": {"title": "Session 13h ago"},
                    "contentDetails": {"duration": "PT3H"},
                    "liveStreamingDetails": {
                        "actualStartTime": "2025-01-14T06:00:00Z",
                        "actualEndTime": end_time,
                    },
                }
            ]
        }
        mock_yt = MagicMock()
        mock_yt.videos.return_value.list.return_value.execute.return_value = api_response

        plenary = {
            "total_matches": 1,
            "videos": [{"video_id": video_id, "title": "T", "url": "u",
                        "published_at": "2025-01-14T06:00:00Z",
                        "is_live": False, "is_upcoming": False}],
        }

        with patch("congress_videos.modules.youtube.youtube_channel.build", return_value=mock_yt):
            from congress_videos.modules.youtube.youtube_channel import get_video_details
            result = get_video_details(plenary)  # default = 12h

        assert result["total_videos"] == 1, (
            "Video ended 13h ago must be included with the 12h default"
        )

    def test_custom_override_of_6_still_honored(self):
        """When min_hours_since_end=6, a video that ended 7h ago is eligible."""
        from datetime import datetime, timedelta, timezone
        from unittest.mock import MagicMock, patch

        video_id = "CUSTOM_7H"
        end_time = (datetime.now(UTC) - timedelta(hours=7)).isoformat().replace("+00:00", "Z")

        api_response = {
            "items": [
                {
                    "snippet": {"title": "Session 7h ago"},
                    "contentDetails": {"duration": "PT3H"},
                    "liveStreamingDetails": {
                        "actualStartTime": "2025-01-15T00:00:00Z",
                        "actualEndTime": end_time,
                    },
                }
            ]
        }
        mock_yt = MagicMock()
        mock_yt.videos.return_value.list.return_value.execute.return_value = api_response

        plenary = {
            "total_matches": 1,
            "videos": [{"video_id": video_id, "title": "T", "url": "u",
                        "published_at": "2025-01-15T00:00:00Z",
                        "is_live": False, "is_upcoming": False}],
        }

        with patch("congress_videos.modules.youtube.youtube_channel.build", return_value=mock_yt):
            from congress_videos.modules.youtube.youtube_channel import get_video_details
            result = get_video_details(plenary, min_hours_since_end=6)

        assert result["total_videos"] == 1, (
            "Video ended 7h ago must be included when min_hours_since_end=6"
        )


class TestDAGFreshnessDefault:

    def test_dag_param_default_is_12(self):
        """The DAG min_hours_since_end param default must be 12."""
        from congress_videos.youtube_channel_monitor_dag import dag
        assert int(dag.params["min_hours_since_end"]) == 12, (
            f"DAG param min_hours_since_end must default to 12, "
            f"got {dag.params['min_hours_since_end']!r}"
        )

    def test_dag_callsite_fallback_is_12(self):
        """The .get('min_hours_since_end', N) fallback in t3a must be 12.

        We verify this by calling the python_callable with a context where
        min_hours_since_end is absent from params, and confirming get_video_details
        is called with min_hours_since_end=12 (not 2).
        """
        import importlib
        import sys
        # Ensure the DAG module is freshly loaded
        for mod in list(sys.modules.keys()):
            if "youtube_channel_monitor_dag" in mod:
                del sys.modules[mod]

        from congress_videos.youtube_channel_monitor_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t3a = tasks_by_id["get_video_details"]

        captured = {}

        def fake_get_video_details(plenary_videos, min_hours_since_end):
            captured["min_hours_since_end"] = min_hours_since_end
            return {"total_videos": 0, "videos": []}

        ti = MagicMock()
        ti.xcom_pull.return_value = {"total_videos": 0, "videos": []}

        with patch(
            "congress_videos.modules.youtube.youtube_channel.get_video_details",
            side_effect=fake_get_video_details,
        ), patch("utils.airflow_helpers.xcom_task", side_effect=lambda ti, fn, key: fn()):
            # params dict WITHOUT min_hours_since_end → .get() must return 12
            t3a.python_callable(ti, params={})

        assert captured.get("min_hours_since_end") == 12, (
            f"t3a callsite fallback must be 12, got {captured.get('min_hours_since_end')!r}"
        )
