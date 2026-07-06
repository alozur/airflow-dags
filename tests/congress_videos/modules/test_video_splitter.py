"""Tests for congress_videos.modules.video_splitter."""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from congress_videos.modules.video_splitter import (
    build_ffmpeg_cut_cmd,
    compute_ffmpeg_timeout,
    convert_srt_time_to_seconds,
    extract_chapters_from_video,
    split_video_chapter,
)
from tests.helpers.assertions import assert_error_result, assert_success_result


# ---------------------------------------------------------------------------
# convert_srt_time_to_seconds
# ---------------------------------------------------------------------------

class TestConvertSrtTimeToSeconds:
    def test_zero_time(self):
        assert convert_srt_time_to_seconds("00:00:00,000") == 0.0

    def test_only_seconds(self):
        assert convert_srt_time_to_seconds("00:00:30,000") == 30.0

    def test_minutes_and_seconds(self):
        assert convert_srt_time_to_seconds("00:01:30,000") == 90.0

    def test_hours_only(self):
        assert convert_srt_time_to_seconds("01:00:00,000") == 3600.0

    def test_with_milliseconds(self):
        result = convert_srt_time_to_seconds("00:10:15,500")
        assert abs(result - (10 * 60 + 15 + 0.5)) < 0.001

    def test_full_complex_timestamp(self):
        result = convert_srt_time_to_seconds("01:02:03,456")
        expected = 3600 + 2 * 60 + 3 + 0.456
        assert abs(result - expected) < 0.001

    def test_invalid_format_returns_zero(self):
        assert convert_srt_time_to_seconds("not-a-timestamp") == 0.0

    def test_empty_string_returns_zero(self):
        assert convert_srt_time_to_seconds("") == 0.0

    def test_none_returns_zero(self):
        assert convert_srt_time_to_seconds(None) == 0.0


# ---------------------------------------------------------------------------
# split_video_chapter
# ---------------------------------------------------------------------------

class TestSplitVideoChapter:
    def test_source_not_found_returns_error(self, tmp_path):
        missing = str(tmp_path / "missing.mp4")
        output = str(tmp_path / "out.mp4")
        result = split_video_chapter(missing, output, "00:00:00,000", "00:01:00,000")
        assert_error_result(result, "not found")

    def test_ffmpeg_success(self, mocker, tmp_path):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"\x00" * 100)
        output = str(tmp_path / "chapter.mp4")

        mock_run = mocker.patch("congress_videos.modules.video_splitter.subprocess.run")
        mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")
        mocker.patch("congress_videos.modules.video_splitter.os.path.getsize", return_value=1_048_576)

        result = split_video_chapter(str(source), output, "00:00:00,000", "00:01:00,000")

        assert_success_result(result)
        assert result["output_path"] == output
        assert result["duration_seconds"] == pytest.approx(60.0)
        assert result["file_size_mb"] == pytest.approx(1.0)

    def test_ffmpeg_nonzero_returncode_returns_error(self, mocker, tmp_path):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"\x00" * 100)
        output = str(tmp_path / "chapter.mp4")

        mock_run = mocker.patch("congress_videos.modules.video_splitter.subprocess.run")
        mock_run.return_value = MagicMock(returncode=1, stderr="ffmpeg: fatal error occurred", stdout="")

        result = split_video_chapter(str(source), output, "00:00:00,000", "00:01:00,000")

        assert_error_result(result)
        assert "ffmpeg" in result["error"].lower()

    def test_ffmpeg_timeout_returns_specific_error(self, mocker, tmp_path):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"\x00" * 100)
        output = str(tmp_path / "chapter.mp4")

        mock_run = mocker.patch("congress_videos.modules.video_splitter.subprocess.run")
        mock_run.side_effect = subprocess.TimeoutExpired(cmd="ffmpeg", timeout=600)

        result = split_video_chapter(str(source), output, "00:00:00,000", "00:01:00,000")

        assert_error_result(result, "timeout")

    def test_invalid_time_range_end_before_start(self, tmp_path):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"\x00" * 100)
        output = str(tmp_path / "chapter.mp4")

        # end < start -> duration <= 0 -> ValueError path
        result = split_video_chapter(str(source), output, "00:01:00,000", "00:00:00,000")

        assert_error_result(result, "Invalid time range")

    def test_result_success_contains_all_keys(self, mocker, tmp_path):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"\x00" * 100)
        output = str(tmp_path / "chapter.mp4")

        mock_run = mocker.patch("congress_videos.modules.video_splitter.subprocess.run")
        mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")
        mocker.patch("congress_videos.modules.video_splitter.os.path.getsize", return_value=512)

        result = split_video_chapter(str(source), output, "00:00:00,000", "00:00:30,000")

        for key in ["success", "output_path", "file_size_bytes", "file_size_mb",
                    "duration_seconds", "start_time", "end_time", "error"]:
            assert key in result, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# extract_chapters_from_video
# ---------------------------------------------------------------------------

class TestExtractChaptersFromVideo:
    def test_empty_list_returns_zero_totals(self):
        result = extract_chapters_from_video([], "/some/path")
        assert result["total_chapters"] == 0
        assert result["successful_extractions"] == 0
        assert result["failed_extractions"] == 0
        assert result["results"] == []

    def test_none_input_returns_zero_totals(self):
        result = extract_chapters_from_video(None, "/some/path")
        assert result["total_chapters"] == 0
        assert result["successful_extractions"] == 0
        assert result["failed_extractions"] == 0

    def test_missing_downloads_folder_marks_chapter_failed(self, tmp_path):
        chapters = [
            {
                "chapter_id": 1,
                "video_id": "abc123",
                "start_time": "00:00:00,000",
                "end_time": "00:01:00,000",
                "source_video_title": "Test Session",
            }
        ]
        # tmp_path has no 'downloads' subfolder
        result = extract_chapters_from_video(chapters, str(tmp_path))

        assert result["total_chapters"] == 1
        assert result["failed_extractions"] == 1
        assert result["successful_extractions"] == 0
        assert result["results"][0]["chapter_id"] == 1
        assert result["results"][0]["success"] is False

    def test_successful_extraction_increments_counter(self, mocker, tmp_path):
        video_id = "testvid"
        date_folder = "2025-01-01"

        video_folder = tmp_path / "downloads" / date_folder / video_id
        video_folder.mkdir(parents=True)
        fake_video = video_folder / "session.mp4"
        fake_video.write_bytes(b"\x00" * 100)

        chapters = [
            {
                "chapter_id": 42,
                "video_id": video_id,
                "start_time": "00:00:00,000",
                "end_time": "00:05:00,000",
                "source_video_title": "Test Session",
            }
        ]

        mock_run = mocker.patch("congress_videos.modules.video_splitter.subprocess.run")
        mock_run.return_value = MagicMock(returncode=0, stderr="", stdout="")
        mocker.patch("congress_videos.modules.video_splitter.os.path.getsize", return_value=2_097_152)

        result = extract_chapters_from_video(chapters, str(tmp_path))

        assert result["total_chapters"] == 1
        assert result["successful_extractions"] == 1
        assert result["failed_extractions"] == 0
        assert result["results"][0]["chapter_id"] == 42
        assert result["results"][0]["success"] is True


# ---------------------------------------------------------------------------
# build_ffmpeg_cut_cmd (default: frame-accurate re-encode, input-seek)
# ---------------------------------------------------------------------------

class TestBuildFfmpegCutCmd:
    def test_default_input_seek_ss_after_input(self):
        """Frame accuracy requires -ss AFTER -i (input-seek), not before."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=10.0, duration=30.0)
        i_idx = cmd.index("-i")
        ss_idx = cmd.index("-ss")
        assert ss_idx > i_idx, "-ss must come after -i for frame-accurate cuts"

    def test_default_uses_reencode(self):
        """Default mode re-encodes with libx264; does not stream-copy."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=0.0, duration=5.0)
        assert "libx264" in cmd
        assert "copy" not in cmd

    def test_reencode_false_uses_stream_copy_and_output_seek(self):
        """reencode=False stream-copies with -ss BEFORE -i (no decode)."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=10.0, duration=30.0, reencode=False)
        assert "copy" in cmd
        assert "libx264" not in cmd
        assert cmd.index("-ss") < cmd.index("-i"), "-ss before -i so copy fast-seeks without decoding"

    def test_source_and_output_present(self):
        cmd = build_ffmpeg_cut_cmd(src="src.mkv", out="dst.mp4", start=1.0, duration=2.0)
        assert cmd[cmd.index("-i") + 1] == "src.mkv"
        assert cmd[-1] == "dst.mp4"

    def test_start_and_duration_serialised(self):
        cmd = build_ffmpeg_cut_cmd(src="a", out="b", start=12.5, duration=30.0)
        assert cmd[cmd.index("-ss") + 1] == "12.5"
        assert cmd[cmd.index("-t") + 1] == "30.0"

    def test_zero_duration_raises_value_error(self):
        with pytest.raises(ValueError, match="0 seconds"):
            build_ffmpeg_cut_cmd(src="a", out="b", start=0.0, duration=0.0)

    def test_negative_duration_raises_value_error(self):
        with pytest.raises(ValueError):
            build_ffmpeg_cut_cmd(src="a", out="b", start=5.0, duration=-3.0)

    def test_sub_second_duration_is_valid(self):
        cmd = build_ffmpeg_cut_cmd(src="a", out="b", start=0.0, duration=0.5)
        assert cmd[cmd.index("-t") + 1] == "0.5"


# ---------------------------------------------------------------------------
# compute_ffmpeg_timeout (Batch 3, #12: adaptive timeout, base=120 factor=8)
# ---------------------------------------------------------------------------

class TestComputeFfmpegTimeout:
    def test_scales_with_duration(self):
        # 120 + 8 * 300 = 2520
        assert compute_ffmpeg_timeout(300) == 2520

    def test_capped_at_max(self):
        # 120 + 8 * 2000 = 16120 → capped to 3600
        assert compute_ffmpeg_timeout(2000) == 3600

    def test_short_clip(self):
        # 120 + 8 * 1 = 128
        assert compute_ffmpeg_timeout(1) == 128

    def test_zero_duration_returns_base(self):
        assert compute_ffmpeg_timeout(0) == 120

    def test_negative_duration_returns_base(self):
        assert compute_ffmpeg_timeout(-10) == 120

    def test_returns_int(self):
        assert isinstance(compute_ffmpeg_timeout(45.7), int)
