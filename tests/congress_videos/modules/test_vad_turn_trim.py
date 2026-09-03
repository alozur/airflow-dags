"""Tests for trim_turn_silence_with_vad (issue #175).

TDD cycle: RED tests written first; GREEN implementation follows.
Covers all 9 spec scenarios for the turn VAD trim helper.
"""

from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_DEFAULT_DUR = 120.0  # seconds — representative turn length


def _make_video_path(tmp_path, name: str = "video.mp4") -> str:
    p = tmp_path / name
    p.write_bytes(b"fake_mp4_data")
    return str(p)


# ---------------------------------------------------------------------------
# Tests: trim_turn_silence_with_vad
# ---------------------------------------------------------------------------


class TestTrimTurnSilenceWithVad:
    """Spec: trim_turn_silence_with_vad — all 9 scenarios."""

    # 2.2 — silence at both ends: trim applied
    def test_both_ends_trimmed(self, tmp_path):
        """detect_speech_bounds returns (2.5, dur-3.0) → returns (2.5, 3.0), file replaced."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = _DEFAULT_DUR
        speech_end_offset = dur - 3.0  # speech ends 3s before file end

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(2.5, speech_end_offset)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_run.return_value = MagicMock(returncode=0)
            mock_extract.return_value = "/tmp/fake.wav"

            result = trim_turn_silence_with_vad(video_path)

        # Returns (trim_start, trim_end): trim_end = dur - speech_end_offset = 3.0
        assert result == pytest.approx((2.5, 3.0)), f"Expected (2.5, 3.0), got {result}"
        mock_replace.assert_called_once()

    # 2.3 — no silence: epsilon guard skips rewrite
    def test_no_silence_epsilon_skip(self, tmp_path):
        """detect_speech_bounds returns bounds near 0 → both trims < epsilon → (0.0, 0.0), no subprocess."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = _DEFAULT_DUR

        # speech starts 0.1s in, ends 0.1s before end → trims are 0.1 < 0.5 epsilon
        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(0.1, dur - 0.1)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            result = trim_turn_silence_with_vad(video_path)

        assert result == (0.0, 0.0), f"Expected (0.0, 0.0), got {result}"
        mock_run.assert_not_called()
        mock_replace.assert_not_called()

    # 2.4 — VAD disabled: no-op
    def test_vad_disabled_noop(self, tmp_path):
        """VAD_ENABLED=False → returns (0.0, 0.0), no subprocess, no file access."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", False),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds") as mock_detect,
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            result = trim_turn_silence_with_vad(video_path)

        assert result == (0.0, 0.0)
        mock_extract.assert_not_called()
        mock_detect.assert_not_called()
        mock_run.assert_not_called()
        mock_replace.assert_not_called()

    # 2.5 — ffmpeg failure: returns zeros, file untouched (threat-matrix rc!=0)
    def test_ffmpeg_failure_returns_zeros(self, tmp_path):
        """ffmpeg rc=1 → returns (0.0, 0.0), os.replace not called, original untouched."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = _DEFAULT_DUR

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(5.0, dur - 5.0)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            mock_run.return_value = MagicMock(returncode=1)

            result = trim_turn_silence_with_vad(video_path)

        assert result == (0.0, 0.0), f"Expected (0.0, 0.0) on ffmpeg failure, got {result}"
        mock_replace.assert_not_called()

    # 2.6 — min-duration guard
    def test_min_duration_guard(self, tmp_path):
        """Post-margin span < VAD_MIN_CHAPTER_SECS → returns (0.0, 0.0), no rewrite."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = 8.0  # very short turn

        # speech_start=3.0 and speech_end=4.0 → span=1s < VAD_MIN_CHAPTER_SECS=5.0
        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(3.0, 4.0)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            result = trim_turn_silence_with_vad(video_path)

        assert result == (0.0, 0.0), f"Expected (0.0, 0.0) due to span guard, got {result}"
        mock_run.assert_not_called()
        mock_replace.assert_not_called()

    # 2.7 — None duration: start-only trim
    def test_none_duration_start_only_trim(self, tmp_path):
        """_get_source_duration returns None → trim_end=0.0, start trim applied if >= eps."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)

        # speech_start=3.0 (>= 0.5 eps), speech_end=99.0 (whatever — duration None)
        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(3.0, 99.0)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=None),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            mock_run.return_value = MagicMock(returncode=0)

            result = trim_turn_silence_with_vad(video_path)

        # trim_end must be 0.0 (no tail trim when duration unknown)
        assert result[1] == pytest.approx(0.0), f"trim_end must be 0.0 when duration is None, got {result}"
        # trim_start must be applied (3.0 >= eps)
        assert result[0] == pytest.approx(3.0), f"trim_start must be 3.0, got {result}"
        mock_replace.assert_called_once()

    # 2.8 — atomicity: temp file in same dir, os.replace called
    def test_atomic_replace_tmp_in_same_dir(self, tmp_path):
        """Successful path: temp file is in same dir as output_path, os.replace called."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = _DEFAULT_DUR

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(3.0, dur - 3.0)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            mock_run.return_value = MagicMock(returncode=0)

            result = trim_turn_silence_with_vad(video_path)

        # os.replace must be called
        mock_replace.assert_called_once()
        # temp file arg to subprocess must be in same dir as video_path
        call_args = mock_run.call_args
        cmd = call_args[0][0]  # positional argv list
        tmp_path_arg = cmd[-1]  # last arg is output temp file
        assert os.path.dirname(tmp_path_arg) == str(tmp_path), (
            f"Temp file {tmp_path_arg!r} must be in same dir as video_path {str(tmp_path)!r}"
        )

    # 2.9 — never raises on exception
    def test_never_raises_on_exception(self, tmp_path):
        """extract_audio_wav raises RuntimeError → function returns (0.0, 0.0) without propagating."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav", side_effect=RuntimeError("ffmpeg failed")),
            patch("congress_videos.modules.vad_helpers.os.replace") as mock_replace,
        ):
            # Must NOT raise
            result = trim_turn_silence_with_vad(video_path)

        assert result == (0.0, 0.0)
        mock_replace.assert_not_called()

    # Additional: -ss/-to before -i in ffmpeg command
    def test_ffmpeg_input_seek_order(self, tmp_path):
        """ffmpeg command must have -ss/-to BEFORE -i (input-side seek for stream copy)."""
        from congress_videos.modules.vad_helpers import trim_turn_silence_with_vad

        video_path = _make_video_path(tmp_path)
        dur = _DEFAULT_DUR

        with (
            patch("congress_videos.modules.vad_helpers.VAD_ENABLED", True),
            patch("congress_videos.modules.vad_helpers.extract_audio_wav") as mock_extract,
            patch("congress_videos.modules.vad_helpers.detect_speech_bounds", return_value=(2.0, dur - 2.0)),
            patch("congress_videos.modules.vad_helpers._get_source_duration", return_value=dur),
            patch("congress_videos.modules.vad_helpers.subprocess.run") as mock_run,
            patch("congress_videos.modules.vad_helpers.os.replace"),
        ):
            mock_extract.return_value = "/tmp/fake.wav"
            mock_run.return_value = MagicMock(returncode=0)
            trim_turn_silence_with_vad(video_path)

        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert "-ss" in cmd, "ffmpeg command must contain -ss"
        assert "-i" in cmd, "ffmpeg command must contain -i"
        ss_idx = cmd.index("-ss")
        i_idx = cmd.index("-i")
        assert ss_idx < i_idx, f"-ss must come before -i in ffmpeg command; cmd={cmd}"
        # -to must also be before -i
        assert "-to" in cmd, "ffmpeg command must contain -to"
        to_idx = cmd.index("-to")
        assert to_idx < i_idx, f"-to must come before -i; cmd={cmd}"
