"""Tests for utils.codec_detection — ffmpeg-codec-aware-cuts (Slice 1).

Covers detect_video_codec, get_cached_codec, reencode_for_codec, and
cut_mode_for_reencode per design.md Decisions 2 and 3.
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock

import pytest

from utils.codec_detection import (
    cut_mode_for_reencode,
    detect_video_codec,
    get_cached_codec,
    reencode_for_codec,
)

# ---------------------------------------------------------------------------
# detect_video_codec
# ---------------------------------------------------------------------------


class TestDetectVideoCodec:
    def test_h264_codec_name_returns_h264(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="h264\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "h264"

    def test_avc1_codec_name_returns_h264(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="avc1\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "h264"

    def test_av1_codec_name_returns_av1(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="av1\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "av1"

    def test_av01_codec_name_returns_av1(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="av01\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "av1"

    def test_vp9_codec_name_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="vp9\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_hevc_codec_name_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="hevc\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_empty_stdout_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_ffprobe_binary_missing_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            side_effect=FileNotFoundError("ffprobe not found"),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_ffprobe_timeout_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="ffprobe", timeout=30),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_ffprobe_nonzero_returncode_returns_unknown(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=1, stdout="", stderr="ffprobe: fatal error"),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_never_raises_on_generic_exception(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            side_effect=RuntimeError("boom"),
        )
        assert detect_video_codec("source.mp4") == "unknown"

    def test_distinct_log_message_for_unrecognized_codec(self, mocker, caplog):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="vp9\n", stderr=""),
        )
        with caplog.at_level("WARNING"):
            detect_video_codec("source.mp4")
        assert any("unrecognized codec" in r.message.lower() for r in caplog.records)

    def test_distinct_log_message_for_ffprobe_failure(self, mocker, caplog):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            side_effect=FileNotFoundError("ffprobe not found"),
        )
        with caplog.at_level("WARNING"):
            detect_video_codec("source.mp4")
        assert any("ffprobe" in r.message.lower() and "not found" in r.message.lower() for r in caplog.records)
        # Distinct from the "unrecognized codec" message.
        assert not any("unrecognized codec" in r.message.lower() for r in caplog.records)

    # -- TRIANGULATE: additional edge-case inputs -----------------------------

    def test_mixed_case_h264_name_normalizes(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="H264\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "h264"

    def test_mixed_case_av01_name_normalizes(self, mocker):
        mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="AV01\n", stderr=""),
        )
        assert detect_video_codec("source.mp4") == "av1"

    def test_path_with_spaces_is_passed_through_to_ffprobe(self, mocker):
        mock_run = mocker.patch(
            "utils.codec_detection.subprocess.run",
            return_value=MagicMock(returncode=0, stdout="h264\n", stderr=""),
        )
        path = "/data/some folder/my video.mp4"
        assert detect_video_codec(path) == "h264"
        assert mock_run.call_args[0][0][-1] == path


# ---------------------------------------------------------------------------
# get_cached_codec
# ---------------------------------------------------------------------------


class TestGetCachedCodec:
    def test_cache_none_probes_every_call(self, mocker):
        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        get_cached_codec("source.mp4", None)
        get_cached_codec("source.mp4", None)
        assert mock_detect.call_count == 2

    def test_cache_dict_probes_once_for_same_path(self, mocker):
        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        cache: dict = {}
        get_cached_codec("source.mp4", cache)
        get_cached_codec("source.mp4", cache)
        get_cached_codec("source.mp4", cache)
        assert mock_detect.call_count == 1

    def test_cache_keys_by_absolute_path(self, mocker):
        import os as _os

        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        cache: dict = {}
        get_cached_codec("./source.mp4", cache)
        get_cached_codec(_os.path.abspath("source.mp4"), cache)
        assert mock_detect.call_count == 1

    def test_cache_stores_result_under_abspath(self, mocker):
        import os as _os

        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="av1")
        cache: dict = {}
        get_cached_codec("source.mp4", cache)
        assert cache[_os.path.abspath("source.mp4")] == "av1"

    def test_different_paths_are_separate_entries(self, mocker):
        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        cache: dict = {}
        get_cached_codec("a.mp4", cache)
        get_cached_codec("b.mp4", cache)
        assert mock_detect.call_count == 2
        assert len(cache) == 2

    # -- TRIANGULATE: additional edge-case inputs -----------------------------

    def test_stale_prepopulated_cache_entry_is_trusted_as_is(self, mocker):
        """A pre-populated (stale/different) cache value is trusted, not re-probed."""
        import os as _os

        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec")
        cache = {_os.path.abspath("source.mp4"): "av1"}
        result = get_cached_codec("source.mp4", cache)
        assert result == "av1"
        mock_detect.assert_not_called()

    def test_path_with_spaces_uses_abspath_key(self, mocker):
        import os as _os

        mock_detect = mocker.patch("utils.codec_detection.detect_video_codec", return_value="h264")
        cache: dict = {}
        path = "some folder/my video.mp4"
        get_cached_codec(path, cache)
        assert _os.path.abspath(path) in cache
        assert mock_detect.call_count == 1


# ---------------------------------------------------------------------------
# reencode_for_codec / cut_mode_for_reencode
# ---------------------------------------------------------------------------


class TestReencodeForCodec:
    def test_h264_reencodes(self):
        assert reencode_for_codec("h264") is True

    def test_av1_stream_copies(self):
        assert reencode_for_codec("av1") is False

    def test_unknown_stream_copies(self):
        assert reencode_for_codec("unknown") is False


class TestCutModeForReencode:
    def test_true_maps_to_reencode(self):
        assert cut_mode_for_reencode(True) == "reencode"

    def test_false_maps_to_stream_copy(self):
        assert cut_mode_for_reencode(False) == "stream_copy"

    def test_one_to_one_with_reencode_for_codec(self):
        for codec in ("h264", "av1", "unknown"):
            reencode = reencode_for_codec(codec)
            mode = cut_mode_for_reencode(reencode)
            assert mode == ("reencode" if reencode else "stream_copy")

    # -- TRIANGULATE: additional edge-case inputs -----------------------------

    def test_only_two_possible_outputs(self):
        assert cut_mode_for_reencode(True) != cut_mode_for_reencode(False)

    def test_reencode_for_codec_mixed_case_input_not_specially_handled(self):
        # reencode_for_codec receives the already-normalized output of
        # detect_video_codec (lowercase); an unnormalized raw string like
        # "H264" is not a valid input here and must fall through to False
        # (fail-safe), confirming normalization is detect_video_codec's job.
        assert reencode_for_codec("H264") is False
