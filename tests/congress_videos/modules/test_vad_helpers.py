"""Tests for congress_videos.modules.vad_helpers.

Covers the spec scenarios with synthetic ``(start, end)`` segments (no real
audio), and mocks the VAD backends (webrtcvad / silero) and ffmpeg so the suite
never depends on torch or audio files.
"""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

from congress_videos.modules.vad_helpers import (
    detect_speech_bounds,
    extract_audio_wav,
    first_sustained_speech_start,
    last_sustained_speech_end,
)


# ---------------------------------------------------------------------------
# first_sustained_speech_start — pure core (the spec coverage anchor)
# ---------------------------------------------------------------------------

class TestFirstSustainedSpeechStart:

    def test_long_initial_silence_returns_block_start_with_margin(self):
        """Sustained speech from ~320s after silence → block start minus margin."""
        # No voice until 320s, then a sustained 12s block (>= 8s).
        segments = [(320.0, 326.0), (327.0, 333.0)]  # gap 1s < 2s → one block, 12s voiced
        result = first_sustained_speech_start(segments)
        assert result == pytest.approx(318.0)  # 320 - 2s margin

    def test_short_burst_below_min_is_rejected(self):
        """A 1.5s burst (< 8s) is discarded; the later sustained block wins."""
        segments = [
            (40.0, 41.5),          # 1.5s isolated burst
            (320.0, 330.0),        # 10s sustained block
        ]
        result = first_sustained_speech_start(segments)
        assert result == pytest.approx(318.0)  # picks 320 block, not the 40s burst

    def test_voice_from_zero_is_clamped(self):
        """Block starting at 0 → 0 - 2s margin clamps to 0.0."""
        segments = [(0.0, 10.0)]
        result = first_sustained_speech_start(segments)
        assert result == 0.0

    def test_gap_under_threshold_segments_are_joined(self):
        """Gap 1.5s < 2s joins both segments into one ~320s block."""
        segments = [(320.0, 324.0), (325.5, 331.0)]  # gap 1.5s, total 9.5s >= 8s
        result = first_sustained_speech_start(segments)
        assert result == pytest.approx(318.0)

    def test_gap_over_threshold_breaks_the_block(self):
        """A gap > 2s splits blocks; first block < 8s is rejected."""
        segments = [
            (10.0, 13.0),   # 3s
            (20.0, 23.0),   # gap 7s > 2s → new block, only 3s — rejected
        ]
        assert first_sustained_speech_start(segments) is None

    def test_empty_segments_returns_none(self):
        assert first_sustained_speech_start([]) is None

    def test_all_short_isolated_segments_returns_none(self):
        """Multiple short isolated bursts, none accumulating 8s → None."""
        segments = [(10.0, 11.0), (30.0, 31.0), (60.0, 61.5)]
        assert first_sustained_speech_start(segments) is None

    def test_unsorted_input_is_handled(self):
        """Segments out of order are sorted before block detection."""
        segments = [(327.0, 333.0), (320.0, 326.0)]
        result = first_sustained_speech_start(segments)
        assert result == pytest.approx(318.0)

    def test_custom_margin_applied(self):
        segments = [(320.0, 330.0)]
        result = first_sustained_speech_start(segments, safety_margin_secs=5.0)
        assert result == pytest.approx(315.0)

    def test_custom_min_sustained_threshold(self):
        """A 5s block qualifies when min_sustained_secs is lowered to 4."""
        segments = [(100.0, 105.0)]
        assert first_sustained_speech_start(segments, min_sustained_secs=4.0) == pytest.approx(98.0)
        assert first_sustained_speech_start(segments, min_sustained_secs=8.0) is None


# ---------------------------------------------------------------------------
# last_sustained_speech_end — pure mirror core
# ---------------------------------------------------------------------------

class TestLastSustainedSpeechEnd:

    def test_last_block_selected_when_several_qualify(self):
        """Two sustained blocks separated by a >2s gap → end from the LATER one."""
        segments = [
            (100.0, 110.0),   # 10s block (>= 8s) — qualifies but is NOT the last
            (300.0, 312.0),   # gap 190s; 12s block (>= 8s) — the LAST qualifying
        ]
        result = last_sustained_speech_end(segments, end_margin_secs=5.0)
        assert result == pytest.approx(317.0)  # 312 + 5s margin (latest block end)

    def test_trailing_short_clap_after_gap_is_excluded(self):
        """A short clap (<8s) after a >2s gap does NOT extend the end."""
        segments = [
            (100.0, 112.0),   # 12s sustained block (the real end)
            (140.0, 144.0),   # gap 28s > 2s; 4s clap < 8s → ignored
        ]
        result = last_sustained_speech_end(segments, end_margin_secs=5.0)
        assert result == pytest.approx(117.0)  # 112 + 5s, NOT from the clap

    def test_voice_sustained_to_end_returns_block_end_plus_margin(self):
        """One block running to 600s → 600 + 5s margin = 605.0 (numeric)."""
        segments = [(0.0, 600.0)]
        result = last_sustained_speech_end(segments, end_margin_secs=5.0)
        assert result == pytest.approx(605.0)

    def test_empty_segments_returns_none(self):
        assert last_sustained_speech_end([]) is None

    def test_single_block_below_min_returns_none(self):
        """A lone 3s block (< 8s) → None."""
        assert last_sustained_speech_end([(10.0, 13.0)]) is None

    def test_single_block_at_or_above_min_returns_end_plus_margin(self):
        """A lone 10s block → block_end + margin."""
        result = last_sustained_speech_end([(100.0, 110.0)], end_margin_secs=5.0)
        assert result == pytest.approx(115.0)

    def test_gaps_under_threshold_merge_into_one_block(self):
        """Gap 1.5s < 2s joins both segments → end from the merged block."""
        segments = [(100.0, 104.0), (105.5, 111.0)]  # gap 1.5s, total 9.5s >= 8s
        result = last_sustained_speech_end(segments, end_margin_secs=5.0)
        assert result == pytest.approx(116.0)  # 111 + 5s

    def test_margin_applied_numeric(self):
        """Last word ends at 600s with margin 5.0 → 605.0."""
        segments = [(540.0, 600.0)]  # 60s block
        assert last_sustained_speech_end(segments, end_margin_secs=5.0) == pytest.approx(605.0)

    def test_unsorted_input_is_handled(self):
        segments = [(300.0, 312.0), (100.0, 110.0)]
        assert last_sustained_speech_end(segments, end_margin_secs=5.0) == pytest.approx(317.0)


# ---------------------------------------------------------------------------
# detect_speech_bounds — single backend pass yields both edges
# ---------------------------------------------------------------------------

class TestDetectSpeechBounds:

    def test_returns_both_edges_from_same_segments(self, mocker):
        """One block 320..330 → start 318 (−2 margin), end 335 (+5 margin)."""
        mocker.patch(
            "congress_videos.modules.vad_helpers._webrtc_segments",
            return_value=[(320.0, 330.0)],
        )
        start, end = detect_speech_bounds("/tmp/a.wav", backend="webrtc")
        assert start == pytest.approx(318.0)
        assert end == pytest.approx(335.0)

    def test_backend_invoked_exactly_once(self, mocker):
        """The segment-producing backend runs ONCE for both edges (NFR8)."""
        seg = mocker.patch(
            "congress_videos.modules.vad_helpers._webrtc_segments",
            return_value=[(320.0, 330.0)],
        )
        detect_speech_bounds("/tmp/a.wav", backend="webrtc")
        assert seg.call_count == 1

    def test_no_speech_returns_none_none(self, mocker):
        mocker.patch(
            "congress_videos.modules.vad_helpers._webrtc_segments",
            return_value=[],
        )
        assert detect_speech_bounds("/tmp/a.wav", backend="webrtc") == (None, None)

    def test_silero_dep_missing_returns_none_none(self, mocker):
        mocker.patch(
            "congress_videos.modules.vad_helpers._silero_segments",
            return_value=None,
        )
        assert detect_speech_bounds("/tmp/a.wav", backend="silero") == (None, None)

    def test_silero_segments_route_to_both_cores(self, mocker):
        """silero backend with valid segments feeds both edge cores (318/335)."""
        mocker.patch(
            "congress_videos.modules.vad_helpers._silero_segments",
            return_value=[(320.0, 330.0)],
        )
        start, end = detect_speech_bounds("/tmp/a.wav", backend="silero")
        assert start == pytest.approx(318.0)
        assert end == pytest.approx(335.0)

    def test_unknown_backend_returns_none_none(self):
        assert detect_speech_bounds("/tmp/a.wav", backend="bogus") == (None, None)


# ---------------------------------------------------------------------------
# extract_audio_wav — ffmpeg args + failure
# ---------------------------------------------------------------------------

class TestExtractAudioWav:

    def test_builds_mono_16k_wav_command(self, mocker):
        run = mocker.patch(
            "congress_videos.modules.vad_helpers.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )
        out = extract_audio_wav("/data/clip.mp4", "/tmp/out.wav", sample_rate=16000)

        assert out == "/tmp/out.wav"
        run.assert_called_once()
        cmd = run.call_args[0][0]
        # Mono, 16 kHz, WAV format.
        assert cmd[cmd.index("-ac") + 1] == "1"
        assert cmd[cmd.index("-ar") + 1] == "16000"
        assert cmd[cmd.index("-f") + 1] == "wav"
        assert cmd[-1] == "/tmp/out.wav"
        # Adaptive timeout is wired in (base overhead for the cheap audio extract).
        assert run.call_args[1]["timeout"] == 120

    def test_nonzero_exit_raises_runtime_error(self, mocker):
        mocker.patch(
            "congress_videos.modules.vad_helpers.subprocess.run",
            return_value=MagicMock(returncode=1, stderr="boom"),
        )
        with pytest.raises(RuntimeError, match="ffmpeg audio extract failed"):
            extract_audio_wav("/data/clip.mp4", "/tmp/out.wav")

    def test_slice_args_add_ss_and_t_before_input(self, mocker):
        """start_secs/duration_secs build -ss/-t placed BEFORE -i (fast seek)."""
        run = mocker.patch(
            "congress_videos.modules.vad_helpers.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )
        out = extract_audio_wav(
            "/data/clip.mp4", "/tmp/out.wav", start_secs=600.0, duration_secs=120.0
        )

        assert out == "/tmp/out.wav"
        cmd = run.call_args[0][0]
        ss_idx = cmd.index("-ss")
        t_idx = cmd.index("-t")
        i_idx = cmd.index("-i")
        # -ss and -t come before -i (fast input seek).
        assert ss_idx < i_idx
        assert t_idx < i_idx
        assert cmd[ss_idx + 1] == "600.0"
        assert cmd[t_idx + 1] == "120.0"
        # Still mono 16k WAV.
        assert cmd[cmd.index("-ac") + 1] == "1"
        assert cmd[cmd.index("-ar") + 1] == "16000"
        assert cmd[cmd.index("-f") + 1] == "wav"

    def test_whole_file_has_no_slice_args(self, mocker):
        """Both slice args None → whole-file extract, no -ss/-t (back-compatible)."""
        run = mocker.patch(
            "congress_videos.modules.vad_helpers.subprocess.run",
            return_value=MagicMock(returncode=0, stderr=""),
        )
        extract_audio_wav("/data/clip.mp4", "/tmp/out.wav")

        cmd = run.call_args[0][0]
        assert "-ss" not in cmd
        assert "-t" not in cmd
        assert cmd[cmd.index("-ac") + 1] == "1"


# ---------------------------------------------------------------------------
# _webrtc_segments — lazy import + WAV reading (webrtcvad faked, no torch/audio)
# ---------------------------------------------------------------------------

class TestWebrtcSegments:

    def test_webrtc_segments_uses_lazy_import_and_wave(self, mocker):
        """_webrtc_segments lazy-imports webrtcvad and reads the WAV via wave.

        webrtcvad is not installed in CI, so we inject a fake module into
        sys.modules and assert no top-level import is needed.
        """
        from congress_videos.modules import vad_helpers

        # Fake webrtcvad module: Vad(2).is_speech() voiced for the first 10 frames.
        fake_webrtcvad = types.ModuleType("webrtcvad")
        fake_vad = MagicMock()
        # 30ms frames at 16k = 480 samples/frame. Voiced for the first 10 frames.
        call_state = {"i": 0}

        def is_speech(frame_bytes, sr):
            voiced = call_state["i"] < 10
            call_state["i"] += 1
            return voiced

        fake_vad.is_speech.side_effect = is_speech
        fake_webrtcvad.Vad = MagicMock(return_value=fake_vad)
        mocker.patch.dict(sys.modules, {"webrtcvad": fake_webrtcvad})

        # Fake a mono 16k WAV with ~15 frames worth of samples.
        import numpy as np
        n_samples = 480 * 15
        pcm = np.zeros(n_samples, dtype=np.int16).tobytes()

        fake_wav = MagicMock()
        fake_wav.getnchannels.return_value = 1
        fake_wav.getframerate.return_value = 16000
        fake_wav.getnframes.return_value = n_samples
        fake_wav.readframes.return_value = pcm
        wave_ctx = MagicMock()
        wave_ctx.__enter__.return_value = fake_wav
        mocker.patch("congress_videos.modules.vad_helpers.wave.open", return_value=wave_ctx)

        segments = vad_helpers._webrtc_segments("/tmp/a.wav", 16000)
        # The first contiguous voiced run produces one segment starting at 0.0.
        assert segments
        assert segments[0][0] == pytest.approx(0.0)

    def test_webrtc_segments_rejects_non_mono(self, mocker):
        from congress_videos.modules import vad_helpers

        fake_webrtcvad = types.ModuleType("webrtcvad")
        fake_webrtcvad.Vad = MagicMock()
        mocker.patch.dict(sys.modules, {"webrtcvad": fake_webrtcvad})

        fake_wav = MagicMock()
        fake_wav.getnchannels.return_value = 2  # stereo → rejected
        wave_ctx = MagicMock()
        wave_ctx.__enter__.return_value = fake_wav
        mocker.patch("congress_videos.modules.vad_helpers.wave.open", return_value=wave_ctx)

        assert vad_helpers._webrtc_segments("/tmp/a.wav", 16000) == []
