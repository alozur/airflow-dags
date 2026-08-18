"""[RED] Tests for get_turn_video_path helper in congress_videos.config.paths.

Asserts canonical path pattern:
  {DOWNLOADS_DIR}/{date}/{video_id}/turns/{turn_id}/turn_video.mp4
"""

from __future__ import annotations

import pytest

from congress_videos.config.paths import DOWNLOADS_DIR, get_turn_video_path


class TestGetTurnVideoPath:

    def test_returns_string(self):
        """get_turn_video_path must return a str."""
        result = get_turn_video_path("2025-10-08", "abc123", 7)
        assert isinstance(result, str)

    def test_contains_date(self):
        """Returned path must contain the date segment."""
        result = get_turn_video_path("2025-10-08", "abc123", 7)
        assert "2025-10-08" in result

    def test_contains_video_id(self):
        """Returned path must contain the video_id segment."""
        result = get_turn_video_path("2025-10-08", "abc123xyz", 7)
        assert "abc123xyz" in result

    def test_contains_turns_subdir(self):
        """Returned path must contain a 'turns' directory segment."""
        result = get_turn_video_path("2025-10-08", "abc123", 7)
        assert "/turns/" in result

    def test_contains_turn_id(self):
        """Returned path must contain the turn_id as a path segment."""
        result = get_turn_video_path("2025-10-08", "abc123", 42)
        assert "/42/" in result

    def test_ends_with_turn_video_mp4(self):
        """Returned path must end with 'turn_video.mp4'."""
        result = get_turn_video_path("2025-10-08", "abc123", 7)
        assert result.endswith("turn_video.mp4")

    def test_starts_with_downloads_dir(self):
        """Returned path must be rooted under DOWNLOADS_DIR."""
        result = get_turn_video_path("2025-10-08", "abc123", 7)
        assert result.startswith(DOWNLOADS_DIR)

    def test_canonical_pattern(self):
        """Returned path must match the full canonical pattern."""
        date = "2025-10-08"
        video_id = "hy1cnx-0Oww"
        turn_id = 99
        result = get_turn_video_path(date, video_id, turn_id)
        expected = f"{DOWNLOADS_DIR}/{date}/{video_id}/turns/{turn_id}/turn_video.mp4"
        assert result == expected

    @pytest.mark.parametrize("turn_id", [1, 42, 999, 10_000])
    def test_various_turn_ids(self, turn_id: int):
        """Path must correctly embed any integer turn_id."""
        result = get_turn_video_path("2026-01-01", "vid_abc", turn_id)
        assert f"/turns/{turn_id}/turn_video.mp4" in result
