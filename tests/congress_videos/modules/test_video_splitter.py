"""Tests for congress_videos.modules.video_splitter."""

from __future__ import annotations

import os
import shutil
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
    def test_default_input_seek_ss_before_input(self):
        """Input seeking: -ss BEFORE -i so exposure shrinks to the cut window."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=10.0, duration=30.0)
        i_idx = cmd.index("-i")
        ss_idx = cmd.index("-ss")
        assert ss_idx < i_idx, "-ss must come before -i for input-seeking re-encode"

    def test_default_uses_reencode(self):
        """Default mode re-encodes with libx264; does not stream-copy."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=0.0, duration=5.0)
        assert "libx264" in cmd
        assert "copy" not in cmd

    def test_reencode_false_uses_stream_copy_and_input_seek(self):
        """reencode=False stream-copies with -ss BEFORE -i (no decode, keyframe snap)."""
        src = "in.mp4"
        out = "out.mp4"
        start = 10.0
        duration = 30.0
        cmd = build_ffmpeg_cut_cmd(src=src, out=out, start=start, duration=duration, reencode=False)
        assert "copy" in cmd
        assert "libx264" not in cmd
        assert "-err_detect" not in cmd
        assert cmd.index("-ss") < cmd.index("-i"), "-ss before -i so copy fast-seeks without decoding"
        assert cmd == [
            'ffmpeg', '-y',
            '-ss', str(start),
            '-i', src,
            '-t', str(duration),
            '-c', 'copy',
            '-avoid_negative_ts', 'make_zero',
            out,
        ]

    def test_err_detect_ignore_err_is_input_option(self):
        """-err_detect ignore_err appears as an input option, before -i."""
        cmd = build_ffmpeg_cut_cmd(src="in.mp4", out="out.mp4", start=10.0, duration=30.0)
        assert "-err_detect" in cmd
        assert cmd[cmd.index("-err_detect") + 1] == "ignore_err"
        assert cmd.index("-err_detect") < cmd.index("-i")

    def test_default_command_exact_shape(self):
        """Re-encode branch matches the design template exactly."""
        src = "in.mp4"
        out = "out.mp4"
        start = 10.0
        duration = 30.0
        cmd = build_ffmpeg_cut_cmd(src=src, out=out, start=start, duration=duration)
        assert cmd == [
            'ffmpeg', '-y',
            '-err_detect', 'ignore_err',
            '-ss', str(start),
            '-i', src,
            '-t', str(duration),
            '-c:v', 'libx264',
            '-preset', 'veryfast',
            '-crf', '20',
            '-c:a', 'aac',
            '-avoid_negative_ts', 'make_zero',
            out,
        ]

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


# ---------------------------------------------------------------------------
# AV1 integration cut (requires ffmpeg + AV1 encoder)
# ---------------------------------------------------------------------------

@pytest.mark.integration
@pytest.mark.slow
class TestAv1CutIntegration:
    """Exercise a real ffmpeg cut on a synthetic AV1 source.

    Requires ffmpeg with an AV1 encoder installed.  Auto-skips without
    failing the suite when the binary or encoder is missing.
    """

    SOURCE_DURATION = 10.0          # seconds
    SOURCE_SIZE = "128x64"
    SOURCE_RATE = 25                # fps

    def _av1_encoder_available(self) -> str | None:
        """Return the first available AV1 encoder name, or None."""
        if not shutil.which("ffmpeg"):
            return None
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=30,
        )
        for name in ("libaom-av1", "libsvtav1", "librav1e"):
            if name in result.stdout:
                return name
        return None

    def _gen_source(self, tmp_path: str, encoder: str) -> str:
        """Generate a short synthetic AV1 video at *tmp_path*."""
        path = os.path.join(tmp_path, "source_av1.mp4")
        cmd = [
            "ffmpeg", "-y",
            "-f", "lavfi",
            "-i", f"testsrc=duration={self.SOURCE_DURATION}"
                   f":size={self.SOURCE_SIZE}:rate={self.SOURCE_RATE}",
            "-c:v", encoder,
            "-crf", "63",
            "-g", "25",
        ]
        # Speed preset differs per encoder
        if encoder == "libaom-av1":
            cmd.extend(["-cpu-used", "6"])
        else:
            cmd.extend(["-preset", "10"])
        cmd.append(path)

        subprocess.run(cmd, capture_output=True, text=True, timeout=120, check=True)
        return path

    def test_av1_source_cut_produces_h264_aac(self, tmp_path):
        """A real AV1 source cut by split_video_chapter yields h264+aac."""
        encoder = self._av1_encoder_available()
        if encoder is None:
            pytest.skip("ffmpeg with AV1 encoder not available")

        source = self._gen_source(str(tmp_path), encoder)
        output = os.path.join(str(tmp_path), "chapter.mp4")
        start_srt = "00:00:01,000"
        end_srt = "00:00:04,000"

        result = split_video_chapter(source, output, start_srt, end_srt)

        assert result["success"] is True
        assert result["output_path"] == output
        # Output duration should be ~3 s (end_srt - start_srt) within ±1 frame
        assert result["duration_seconds"] == pytest.approx(3.0, abs=0.04)

        # Verify the output codecs via ffprobe
        probe = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name",
                "-of", "csv=p=0",
                output,
            ],
            capture_output=True, text=True, timeout=30,
        )
        assert probe.returncode == 0
        assert "h264" in probe.stdout.strip().lower()

        probe_a = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a:0",
                "-show_entries", "stream=codec_name",
                "-of", "csv=p=0",
                output,
            ],
            capture_output=True, text=True, timeout=30,
        )
        # Audio stream may be absent in the synthetic source
        if probe_a.returncode == 0 and probe_a.stdout.strip():
            assert "aac" in probe_a.stdout.strip().lower()
