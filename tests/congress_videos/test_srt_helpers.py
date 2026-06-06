"""Tests for congress_videos.srt_helpers module."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from congress_videos.srt_helpers import find_srt_for_chapter, select_pretrim_window


# ---------------------------------------------------------------------------
# 7.4 — find_srt_for_chapter
# ---------------------------------------------------------------------------

class TestFindSrtForChapter:

    def test_file_at_first_candidate_path_returns_that_path(self, mocker):
        """When the first candidate (PROJECT_DATA_DIR path) exists, return it."""
        expected_path = "/data/congress_videos/vid123/srt_files/vid123.srt"

        def _exists(p):
            return p == expected_path

        mocker.patch("os.path.exists", side_effect=_exists)
        mocker.patch("os.path.isdir", return_value=False)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", "/data/congress_videos")

        result = find_srt_for_chapter("vid123", 42, session_date="2025-10-08")

        assert result == expected_path

    def test_no_file_at_any_candidate_returns_none(self, mocker):
        """When no candidate exists, return None."""
        mocker.patch("os.path.exists", return_value=False)
        mocker.patch("os.path.isdir", return_value=False)

        result = find_srt_for_chapter("vid999", 1, session_date="2025-01-01")

        assert result is None

    def test_file_at_second_candidate_returns_that_path(self, mocker):
        """When first candidate is missing but second (downloads/date/...) exists, return second."""
        secondary = "/data/downloads/2025-10-08/vid123/srt_files/vid123.srt"

        def _exists(p):
            return p == secondary

        mocker.patch("os.path.exists", side_effect=_exists)
        mocker.patch("os.path.isdir", return_value=False)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", "/data/project")
        mocker.patch("congress_videos.srt_helpers.DOWNLOADS_DIR", "/data/downloads")

        result = find_srt_for_chapter("vid123", 42, session_date="2025-10-08")

        assert result == secondary

    def test_no_session_date_searches_across_date_folders(self, mocker, tmp_path):
        """When session_date is None, fallback scans all date subfolders in DOWNLOADS_DIR."""
        date_folder = tmp_path / "2025-10-08"
        srt_dir = date_folder / "vid123" / "srt_files"
        srt_dir.mkdir(parents=True)
        srt_file = srt_dir / "vid123.srt"
        srt_file.write_text("1\n00:00:01,000 --> 00:00:02,000\nHello\n", encoding="utf-8")

        mocker.patch("congress_videos.srt_helpers.DOWNLOADS_DIR", str(tmp_path))
        # Primary path won't exist (PROJECT_DATA_DIR is a non-existent path)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", str(tmp_path / "_no_project"))
        # Let real os.path.exists handle the tmp_path contents
        # but mock os.path.isdir to correctly report tmp_path as a dir
        # We don't mock os.path.exists here — let it use the real filesystem

        result = find_srt_for_chapter("vid123", 42, session_date=None)

        assert result == str(srt_file)


# ---------------------------------------------------------------------------
# 7.5 — select_pretrim_window
# ---------------------------------------------------------------------------

class TestSelectPretrimWindow:

    def test_valid_ai_response_returns_start_and_end_seconds(self, mocker):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_seconds": 60.0, "end_seconds": 420.0}},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is not None
        assert result["start_seconds"] == 60.0
        assert result["end_seconds"] == 420.0

    def test_ai_returns_invalid_json_shape_returns_none(self, mocker):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"wrong_key": 123}},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is None

    def test_ai_call_error_returns_none(self, mocker):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": "API timeout", "data": None},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is None

    def test_window_too_short_more_than_30s_deviation_returns_none(self, mocker):
        """Window duration 30s vs target 360s — exceeds 30s deviation threshold → None."""
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_seconds": 0.0, "end_seconds": 30.0}},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is None

    def test_negative_start_returns_none(self, mocker):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_seconds": -10.0, "end_seconds": 350.0}},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is None

    def test_end_before_start_returns_none(self, mocker):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_seconds": 200.0, "end_seconds": 100.0}},
        )
        result = select_pretrim_window("some srt text", target_secs=360)

        assert result is None

    def test_generate_json_completion_called_with_gpt4o_mini_model(self, mocker):
        mock_gen = mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_seconds": 0.0, "end_seconds": 360.0}},
        )
        select_pretrim_window("srt content here", target_secs=360)

        _, kwargs = mock_gen.call_args
        assert kwargs.get("model") == "gpt-4o-mini"


# ---------------------------------------------------------------------------
# 7.6 — parse_srt_to_text
# ---------------------------------------------------------------------------

class TestParseSrtToText:

    def test_basic_three_blocks_returns_text_with_timestamps(self, tmp_path):
        from congress_videos.srt_helpers import parse_srt_to_text

        srt = (
            "1\n00:00:01,000 --> 00:00:05,000\nHello world\n\n"
            "2\n00:00:06,000 --> 00:00:10,000\nSecond line\n\n"
            "3\n00:00:11,000 --> 00:00:15,000\nThird entry\n\n"
        )
        path = tmp_path / "test.srt"
        path.write_text(srt, encoding="utf-8")

        result = parse_srt_to_text(str(path))

        assert "00:00:01 --> 00:00:05" in result
        assert "Hello world" in result
        assert "Second line" in result
        assert "Third entry" in result

    def test_truncation_when_content_exceeds_max_chars(self, tmp_path):
        from congress_videos.srt_helpers import parse_srt_to_text

        block = "1\n00:00:01,000 --> 00:00:05,000\n" + ("A" * 200) + "\n\n"
        repeated = block * 50
        path = tmp_path / "long.srt"
        path.write_text(repeated, encoding="utf-8")

        result = parse_srt_to_text(str(path), max_chars=500)

        assert len(result) <= 500

    def test_file_not_found_returns_empty_string(self):
        from congress_videos.srt_helpers import parse_srt_to_text

        result = parse_srt_to_text("/nonexistent/path/file.srt")

        assert result == ""
