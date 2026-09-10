"""Tests for utils/youtube_downloader.py — coverage boost."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_ydl_info(
    video_id: str = "vid123",
    title: str = "Test Video",
    duration: int = 3600,
    ext: str = "webm",
    subtitles: dict | None = None,
    automatic_captions: dict | None = None,
) -> dict:
    return {
        "id": video_id,
        "title": title,
        "duration": duration,
        "duration_string": "01:00:00",
        "ext": ext,
        "subtitles": subtitles or {},
        "automatic_captions": automatic_captions or {},
    }


# ---------------------------------------------------------------------------
# download_with_pytubefix
# ---------------------------------------------------------------------------


class TestDownloadWithPytubefix:
    def test_returns_error_when_pytubefix_not_installed(self, monkeypatch):
        """ImportError on pytubefix import returns error dict without raising."""
        # Remove pytubefix from sys.modules so import fails
        monkeypatch.setitem(sys.modules, "pytubefix", None)
        monkeypatch.setitem(sys.modules, "pytubefix.cli", None)

        from utils.youtube_downloader import download_with_pytubefix

        result = download_with_pytubefix("https://youtube.com/watch?v=x", "/tmp/out")

        assert result["success"] is False
        assert "pytubefix" in result["error"].lower()

    def test_no_stream_found_returns_error(self, tmp_path, mocker):
        """When no adaptive or progressive stream found, returns error."""
        mock_yt_instance = MagicMock()
        mock_yt_instance.video_id = "vid1"
        mock_yt_instance.title = "Test"
        mock_yt_instance.streams.all.return_value = []
        # filter returns something chainable with no results
        mock_filter_chain = MagicMock()
        mock_filter_chain.filter.return_value = mock_filter_chain
        mock_filter_chain.order_by.return_value = mock_filter_chain
        mock_filter_chain.desc.return_value = mock_filter_chain
        mock_filter_chain.first.return_value = None
        mock_yt_instance.streams.filter.return_value = mock_filter_chain

        fake_pytubefix = types.ModuleType("pytubefix")
        fake_pytubefix.YouTube = MagicMock(return_value=mock_yt_instance)
        fake_cli = types.ModuleType("pytubefix.cli")
        fake_cli.on_progress = MagicMock()

        mocker.patch.dict(sys.modules, {"pytubefix": fake_pytubefix, "pytubefix.cli": fake_cli})

        import importlib

        from utils import youtube_downloader

        importlib.reload(youtube_downloader)

        result = youtube_downloader.download_with_pytubefix("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["error"] == "No suitable stream found"

    def test_adaptive_stream_with_ffmpeg_merge_succeeds(self, tmp_path, mocker):
        """Adaptive video+audio streams with successful ffmpeg merge returns success."""
        import importlib

        # Create fake output files so size checks pass
        video_tmp = tmp_path / "vid1_video_temp.webm"
        audio_tmp = tmp_path / "vid1_audio_temp.webm"
        final_file = tmp_path / "vid1_Test.mp4"
        video_tmp.write_bytes(b"\x00" * 16)
        audio_tmp.write_bytes(b"\x00" * 16)
        final_file.write_bytes(b"\x00" * 1024 * 1024)  # 1 MB

        mock_video_stream = MagicMock()
        mock_video_stream.resolution = "720p"
        mock_video_stream.subtype = "webm"
        mock_video_stream.download.return_value = str(video_tmp)

        mock_audio_stream = MagicMock()
        mock_audio_stream.abr = "128kbps"
        mock_audio_stream.subtype = "webm"
        mock_audio_stream.download.return_value = str(audio_tmp)

        mock_yt_instance = MagicMock()
        mock_yt_instance.video_id = "vid1"
        mock_yt_instance.title = "Test"
        mock_yt_instance.length = 3600
        mock_yt_instance.streams.all.return_value = []

        # adaptive video filter chain
        video_chain = MagicMock()
        video_chain.filter.return_value = video_chain
        video_chain.order_by.return_value = video_chain
        video_chain.desc.return_value = video_chain
        video_chain.first.return_value = mock_video_stream

        # audio filter chain
        audio_chain = MagicMock()
        audio_chain.order_by.return_value = audio_chain
        audio_chain.desc.return_value = audio_chain
        audio_chain.first.return_value = mock_audio_stream

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            if kwargs.get("only_audio"):
                return audio_chain
            return video_chain

        mock_yt_instance.streams.filter.side_effect = filter_side_effect

        fake_pytubefix = types.ModuleType("pytubefix")
        fake_pytubefix.YouTube = MagicMock(return_value=mock_yt_instance)
        fake_cli = types.ModuleType("pytubefix.cli")
        fake_cli.on_progress = MagicMock()

        mocker.patch.dict(sys.modules, {"pytubefix": fake_pytubefix, "pytubefix.cli": fake_cli})

        mock_run = mocker.patch("subprocess.run")
        mock_run.return_value = MagicMock(returncode=0, stderr="")

        from utils import youtube_downloader

        importlib.reload(youtube_downloader)

        result = youtube_downloader.download_with_pytubefix("https://youtube.com/watch?v=vid1", str(tmp_path))

        assert result["success"] is True
        assert result["resolution"] == "720p"
        assert result["duration"] == 3600

    def test_progressive_stream_fallback_succeeds(self, tmp_path, mocker):
        """When no adaptive stream, progressive stream fallback succeeds."""
        import importlib

        final_file = tmp_path / "vid2_Test.mp4"
        final_file.write_bytes(b"\x00" * 1024 * 512)  # 0.5 MB

        mock_progressive_stream = MagicMock()
        mock_progressive_stream.resolution = "480p"
        mock_progressive_stream.download.return_value = str(final_file)

        mock_yt_instance = MagicMock()
        mock_yt_instance.video_id = "vid2"
        mock_yt_instance.title = "Test"
        mock_yt_instance.length = 1800
        mock_yt_instance.streams.all.return_value = []

        # All adaptive filters return None; progressive returns stream
        adaptive_chain = MagicMock()
        adaptive_chain.filter.return_value = adaptive_chain
        adaptive_chain.order_by.return_value = adaptive_chain
        adaptive_chain.desc.return_value = adaptive_chain
        adaptive_chain.first.return_value = None

        progressive_chain = MagicMock()
        progressive_chain.order_by.return_value = progressive_chain
        progressive_chain.desc.return_value = progressive_chain
        progressive_chain.first.return_value = mock_progressive_stream

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            if kwargs.get("progressive"):
                return progressive_chain
            return adaptive_chain

        mock_yt_instance.streams.filter.side_effect = filter_side_effect

        fake_pytubefix = types.ModuleType("pytubefix")
        fake_pytubefix.YouTube = MagicMock(return_value=mock_yt_instance)
        fake_cli = types.ModuleType("pytubefix.cli")
        fake_cli.on_progress = MagicMock()

        mocker.patch.dict(sys.modules, {"pytubefix": fake_pytubefix, "pytubefix.cli": fake_cli})

        from utils import youtube_downloader

        importlib.reload(youtube_downloader)

        result = youtube_downloader.download_with_pytubefix("https://youtube.com/watch?v=vid2", str(tmp_path))

        assert result["success"] is True
        assert result["resolution"] == "480p"
        assert result["duration"] == 1800


# ---------------------------------------------------------------------------
# _log_available_streams (lifted out of download_with_pytubefix, #272 slice 5 PR9)
# ---------------------------------------------------------------------------


def _make_pytubefix_stream(resolution="720p", mime_type="video/mp4", is_adaptive=True, is_progressive=False):
    stream = MagicMock()
    stream.resolution = resolution
    stream.mime_type = mime_type
    stream.is_adaptive = is_adaptive
    stream.is_progressive = is_progressive
    return stream


class TestLogAvailableStreams:
    def test_returns_none(self):
        """The helper is a pure logging side effect and returns None."""
        yt = MagicMock()
        yt.streams.all.return_value = []

        from utils.youtube_downloader import _log_available_streams

        assert _log_available_streams(yt) is None

    def test_logs_at_most_first_15_streams_but_header_reports_full_count(self, mocker):
        """20 streams in -> 1 header log line (reporting 20) + 15 detail lines, not 20."""
        streams = [_make_pytubefix_stream() for _ in range(20)]
        yt = MagicMock()
        yt.streams.all.return_value = streams

        mock_logger = mocker.patch("utils.youtube_downloader.logger")

        from utils.youtube_downloader import _log_available_streams

        _log_available_streams(yt)

        assert mock_logger.info.call_count == 16  # 1 header + 15 detail lines
        header_message = mock_logger.info.call_args_list[0].args[0]
        assert "20" in header_message

    def test_stream_with_none_resolution_logs_audio(self, mocker):
        """A stream with resolution=None is logged as `audio`, matching `s.resolution or 'audio'`."""
        yt = MagicMock()
        yt.streams.all.return_value = [_make_pytubefix_stream(resolution=None)]

        mock_logger = mocker.patch("utils.youtube_downloader.logger")

        from utils.youtube_downloader import _log_available_streams

        _log_available_streams(yt)

        detail_message = mock_logger.info.call_args_list[1].args[0]
        assert "audio" in detail_message


# ---------------------------------------------------------------------------
# _select_video_stream (lifted out of download_with_pytubefix, #272 slice 5 PR9)
# ---------------------------------------------------------------------------


def _make_filter_chain(first_return, filter_side_effect=None):
    chain = MagicMock()
    if filter_side_effect is not None:
        chain.filter.side_effect = filter_side_effect
    else:
        chain.filter.return_value = chain
    chain.order_by.return_value = chain
    chain.desc.return_value = chain
    chain.first.return_value = first_return
    return chain


class TestSelectVideoStream:
    def test_prefers_mp4_adaptive_stream_at_or_above_min_resolution(self):
        """The primary mp4-adaptive-at-min-resolution filter wins when it yields a stream."""
        mock_stream = _make_pytubefix_stream(resolution="1080p")
        primary_chain = _make_filter_chain(mock_stream)

        yt = MagicMock()
        yt.streams.filter.return_value = primary_chain

        from utils.youtube_downloader import _select_video_stream

        result = _select_video_stream(yt, 720)

        assert result is mock_stream
        assert yt.streams.filter.call_count == 1  # no fallback attempted

    def test_falls_back_to_any_mp4_adaptive_stream(self):
        """When the primary filter yields nothing, fall back to any mp4 adaptive stream."""
        mock_stream = _make_pytubefix_stream(resolution="480p")
        primary_chain = _make_filter_chain(None)
        fallback1_chain = _make_filter_chain(mock_stream)

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            return primary_chain if call_count[0] == 1 else fallback1_chain

        yt = MagicMock()
        yt.streams.filter.side_effect = filter_side_effect

        from utils.youtube_downloader import _select_video_stream

        result = _select_video_stream(yt, 720)

        assert result is mock_stream
        assert call_count[0] == 2

    def test_falls_back_to_any_adaptive_stream(self):
        """When both mp4 filters yield nothing, fall back to any adaptive stream."""
        mock_stream = _make_pytubefix_stream(resolution="360p")
        primary_chain = _make_filter_chain(None)
        fallback1_chain = _make_filter_chain(None)
        fallback2_chain = _make_filter_chain(mock_stream)

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return primary_chain
            if call_count[0] == 2:
                return fallback1_chain
            return fallback2_chain

        yt = MagicMock()
        yt.streams.filter.side_effect = filter_side_effect

        from utils.youtube_downloader import _select_video_stream

        result = _select_video_stream(yt, 720)

        assert result is mock_stream
        assert call_count[0] == 3

    def test_returns_none_when_all_three_yield_none(self):
        """When none of the three filters yield a stream, the helper returns None."""
        primary_chain = _make_filter_chain(None)
        fallback1_chain = _make_filter_chain(None)
        fallback2_chain = _make_filter_chain(None)

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return primary_chain
            if call_count[0] == 2:
                return fallback1_chain
            return fallback2_chain

        yt = MagicMock()
        yt.streams.filter.side_effect = filter_side_effect

        from utils.youtube_downloader import _select_video_stream

        result = _select_video_stream(yt, 720)

        assert result is None
        assert call_count[0] == 3

    def test_none_resolution_excluded_by_short_circuit_without_raising(self):
        """A stream with resolution=None is excluded by `s.resolution and ...` — no TypeError."""
        captured = {}

        def capture_lambda(fn):
            captured["fn"] = fn
            return primary_chain

        primary_chain = _make_filter_chain(None, filter_side_effect=capture_lambda)
        fallback1_chain = _make_filter_chain(None)
        fallback2_chain = _make_filter_chain(None)

        call_count = [0]

        def filter_side_effect(**kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return primary_chain
            if call_count[0] == 2:
                return fallback1_chain
            return fallback2_chain

        yt = MagicMock()
        yt.streams.filter.side_effect = filter_side_effect

        from utils.youtube_downloader import _select_video_stream

        result = _select_video_stream(yt, 720)

        assert result is None
        no_resolution_stream = _make_pytubefix_stream(resolution=None)
        assert not captured["fn"](no_resolution_stream)  # falsy, not a TypeError


# ---------------------------------------------------------------------------
# download_youtube_video_for_upload
# ---------------------------------------------------------------------------


class TestDownloadYoutubeVideoForUpload:
    def test_pytubefix_success_returns_immediately(self, tmp_path, mocker):
        """If pytubefix succeeds, yt-dlp is never called (guard disabled to isolate
        the download short-circuit from the live-status probe, which also uses
        yt_dlp.YoutubeDL)."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"},
        )
        mock_ydl_class = mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), guard_live_status=False
        )

        assert result["success"] is True
        mock_ydl_class.assert_not_called()

    def test_pytubefix_failure_falls_back_to_ytdlp(self, tmp_path, mocker):
        """When pytubefix fails, yt-dlp is tried as fallback."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "stream error"},
        )

        # Create a fake output file
        fake_file = tmp_path / "vid123_Test Video.mp4"
        fake_file.write_bytes(b"\x00" * 1024)

        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        assert result["file_path"] == str(fake_file)

    def test_ytdlp_download_error_returns_failure(self, tmp_path, mocker):
        """yt_dlp.DownloadError is caught and returned as error dict."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        import yt_dlp

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.side_effect = yt_dlp.utils.DownloadError("rate limited")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert "Download error" in result["error"]

    def test_ytdlp_unexpected_error_returns_failure(self, tmp_path, mocker):
        """Any exception during yt-dlp download returns error dict with success=False."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        # Patch YoutubeDL class itself to raise when instantiated
        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=RuntimeError("unexpected"),
        )

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["error"] is not None
        assert "unexpected" in result["error"]

    def test_skips_pytubefix_when_use_pytubefix_first_false(self, tmp_path, mocker):
        """use_pytubefix_first=False goes straight to yt-dlp."""
        mock_pytubefix = mocker.patch("utils.youtube_downloader.download_with_pytubefix")

        fake_file = tmp_path / "vid123_Test.mp4"
        fake_file.write_bytes(b"\x00" * 512)

        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x",
            str(tmp_path),
            use_pytubefix_first=False,
        )

        mock_pytubefix.assert_not_called()
        assert result["success"] is True

    def test_cookies_file_added_to_opts_when_exists(self, tmp_path, mocker):
        """Cookies file path is included in ydl_opts when file exists."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        cookies_file = tmp_path / "cookies.txt"
        cookies_file.write_text("cookies content")

        fake_file = tmp_path / "vid123_Test.mp4"
        fake_file.write_bytes(b"\x00" * 512)

        fake_info = _make_ydl_info()
        captured_opts: dict = {}

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x",
            str(tmp_path),
            cookies_file=str(cookies_file),
        )

        assert captured_opts.get("cookiefile") == str(cookies_file)

    def test_file_not_found_after_download_returns_error(self, tmp_path, mocker):
        """When yt-dlp downloads but file is missing, returns error."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        missing_path = str(tmp_path / "nonexistent.mp4")
        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = missing_path

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), use_pytubefix_first=False
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_quality_1080p_uses_correct_format(self, tmp_path, mocker):
        """quality='1080p' selects the 1080p format string."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        captured_opts: dict = {}
        fake_file = tmp_path / "vid123_Test.mp4"
        fake_file.write_bytes(b"\x00" * 256)
        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_youtube_video_for_upload

        download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), quality="1080p", use_pytubefix_first=False
        )

        assert "1080" in captured_opts.get("format", "")


# ---------------------------------------------------------------------------
# _check_live_status_guard (lifted out of download_youtube_video_for_upload, #272 slice 5 PR9)
# ---------------------------------------------------------------------------


class TestCheckLiveStatusGuard:
    def test_guard_disabled_returns_none_without_probing(self, mocker):
        """guard_live_status=False returns None without ever probing."""
        mock_probe = mocker.patch("utils.youtube_downloader.probe_live_status")

        from utils.youtube_downloader import _check_live_status_guard

        result = _check_live_status_guard("https://youtube.com/watch?v=x", "cookies.txt", False)

        assert result is None
        mock_probe.assert_not_called()

    def test_probe_error_none_is_non_blocking(self, mocker):
        """A probe error (None) is non-blocking: the guard returns None."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value=None)

        from utils.youtube_downloader import _check_live_status_guard

        result = _check_live_status_guard("https://youtube.com/watch?v=x", "cookies.txt", True)

        assert result is None

    @pytest.mark.parametrize("status", ["was_live", "not_live"])
    def test_ready_status_returns_none(self, mocker, status):
        """A ready live_status (was_live/not_live) returns None — proceed to download."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value=status)

        from utils.youtube_downloader import _check_live_status_guard

        result = _check_live_status_guard("https://youtube.com/watch?v=x", "cookies.txt", True)

        assert result is None

    def test_not_ready_status_returns_skip_dict_with_exact_repr_quoting(self, mocker):
        """A not-ready live_status (post_live) returns the skip dict with `!r`-quoted status."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value="post_live")

        from utils.youtube_downloader import _check_live_status_guard

        result = _check_live_status_guard("https://youtube.com/watch?v=x", "cookies.txt", True)

        assert result == {
            "success": False,
            "skipped": True,
            "file_path": None,
            "file_size_mb": None,
            "duration": None,
            "title": None,
            "error": "live_status 'post_live' not ready — skipped download",
        }


# ---------------------------------------------------------------------------
# _try_pytubefix_download (lifted out of download_youtube_video_for_upload, #272 slice 5 PR9)
# ---------------------------------------------------------------------------


class TestTryPytubefixDownload:
    def test_use_pytubefix_first_false_returns_none_without_calling(self, mocker):
        """use_pytubefix_first=False returns None without calling download_with_pytubefix."""
        mock_pytubefix = mocker.patch("utils.youtube_downloader.download_with_pytubefix")

        from utils.youtube_downloader import _try_pytubefix_download

        result = _try_pytubefix_download("https://youtube.com/watch?v=x", "/tmp/out", 720, False)

        assert result is None
        mock_pytubefix.assert_not_called()

    def test_success_returns_that_exact_dict_object(self, mocker):
        """On pytubefix success, the helper returns the exact same dict object."""
        expected_result = {"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"}
        mocker.patch("utils.youtube_downloader.download_with_pytubefix", return_value=expected_result)
        mocker.patch("utils.youtube_downloader._warn_if_not_h264")

        from utils.youtube_downloader import _try_pytubefix_download

        result = _try_pytubefix_download("https://youtube.com/watch?v=x", "/tmp/out", 720, True)

        assert result is expected_result

    def test_warn_if_not_h264_raising_is_swallowed_and_result_still_returned(self, mocker):
        """A raising _warn_if_not_h264 is swallowed; the pytubefix result is still returned."""
        expected_result = {"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"}
        mocker.patch("utils.youtube_downloader.download_with_pytubefix", return_value=expected_result)
        mocker.patch("utils.youtube_downloader._warn_if_not_h264", side_effect=RuntimeError("codec check boom"))

        from utils.youtube_downloader import _try_pytubefix_download

        result = _try_pytubefix_download("https://youtube.com/watch?v=x", "/tmp/out", 720, True)

        assert result is expected_result

    def test_success_false_returns_none_falls_through_to_ytdlp(self, mocker):
        """A failed pytubefix attempt returns None so the caller falls through to yt-dlp."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "stream error"},
        )

        from utils.youtube_downloader import _try_pytubefix_download

        result = _try_pytubefix_download("https://youtube.com/watch?v=x", "/tmp/out", 720, True)

        assert result is None


# ---------------------------------------------------------------------------
# download_audio_only
# ---------------------------------------------------------------------------


class TestDownloadAudioOnly:
    def test_successful_audio_download(self, tmp_path, mocker):
        """Successful yt-dlp audio download returns success=True with file info."""
        fake_file = tmp_path / "vid123_Test Video_audio.webm"
        fake_file.write_bytes(b"\x00" * 512)

        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_audio_only

        result = download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        assert result["file_path"] == str(fake_file)
        assert result["file_size_mb"] is not None

    def test_convert_to_mp3_sets_postprocessors(self, tmp_path, mocker):
        """convert_to_mp3=True adds FFmpegExtractAudio postprocessor."""
        captured_opts: dict = {}

        fake_info = _make_ydl_info(ext="webm")
        # prepare_filename returns webm, code replaces to .mp3
        mp3_file = tmp_path / "vid123_audio.mp3"
        mp3_file.write_bytes(b"\x00" * 256)

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        # returns the webm path; code .replace(".webm", ".mp3") -> mp3_file
        fake_ydl.prepare_filename.return_value = str(tmp_path / "vid123_audio.webm")

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_audio_only

        result = download_audio_only("https://youtube.com/watch?v=x", str(tmp_path), convert_to_mp3=True)

        assert "postprocessors" in captured_opts
        assert captured_opts["postprocessors"][0]["key"] == "FFmpegExtractAudio"

    def test_file_not_found_searches_for_audio_glob(self, tmp_path, mocker):
        """When expected file missing, searches for audio file with glob."""
        fake_info = _make_ydl_info(video_id="vid999")
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        # Returns path that doesn't exist
        fake_ydl.prepare_filename.return_value = str(tmp_path / "vid999_test_audio.webm")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        # Create actual file matching glob pattern
        actual_file = tmp_path / "vid999_something_audio.webm"
        actual_file.write_bytes(b"\x00" * 256)

        from utils.youtube_downloader import download_audio_only

        result = download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        assert str(actual_file) == result["file_path"]

    def test_file_not_found_and_no_glob_match_returns_error(self, tmp_path, mocker):
        """When neither expected file nor glob match found, returns error."""
        fake_info = _make_ydl_info(video_id="vid404")
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(tmp_path / "vid404_test_audio.webm")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_audio_only

        result = download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_exception_returns_error(self, tmp_path, mocker):
        """Exception during yt-dlp download returns error dict."""
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.side_effect = RuntimeError("network failure")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_audio_only

        result = download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["error"] is not None

    def test_audio_format_mp3_sets_postprocessors(self, tmp_path, mocker):
        """audio_format='mp3' also adds FFmpegExtractAudio postprocessor."""
        captured_opts: dict = {}
        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(tmp_path / "x.webm")

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_audio_only

        download_audio_only("https://youtube.com/watch?v=x", str(tmp_path), audio_format="mp3")

        assert "postprocessors" in captured_opts


# ---------------------------------------------------------------------------
# download_audio_in_chunks
# ---------------------------------------------------------------------------


class TestDownloadAudioInChunks:
    def test_no_duration_returns_error(self, tmp_path, mocker):
        """Video with no duration field returns error without downloading."""
        fake_info = _make_ydl_info(duration=0)
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_audio_in_chunks

        result = download_audio_in_chunks("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert "duration" in result["error"].lower()

    def test_successful_chunks_extracts_total_duration(self, tmp_path, mocker):
        """Short video produces expected total_duration in result."""
        call_count = [0]
        chunk_dir = tmp_path / "audio_chunks"

        def fake_ydl_factory(opts):
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                # First call: info extraction
                m.extract_info.return_value = _make_ydl_info(
                    video_id="vid777",
                    duration=1200,  # 20 minutes
                )
            else:
                # Subsequent calls: chunk downloads, create the chunk files
                chunk_index = call_count[0] - 1
                chunk_dir.mkdir(parents=True, exist_ok=True)
                chunk_path = chunk_dir / f"vid777_chunk_{chunk_index:03d}.webm"
                chunk_path.write_bytes(b"\x00" * 256)

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_audio_in_chunks

        result = download_audio_in_chunks(
            "https://youtube.com/watch?v=x",
            str(tmp_path),
            chunk_duration_minutes=10,
        )

        assert result["total_duration"] == 1200

    def test_exception_returns_error(self, tmp_path, mocker):
        """Exception during info extraction returns error dict."""
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.side_effect = RuntimeError("network error")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_audio_in_chunks

        result = download_audio_in_chunks("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# merge_video_audio_ffmpeg
# ---------------------------------------------------------------------------


class TestMergeVideoAudioFfmpeg:
    def test_successful_merge(self, tmp_path, mock_subprocess_run):
        """Successful ffmpeg merge returns success=True with output path."""
        output_file = tmp_path / "merged.mp4"
        output_file.write_bytes(b"\x00" * 256)

        from utils.youtube_downloader import merge_video_audio_ffmpeg

        result = merge_video_audio_ffmpeg("/tmp/video.mp4", "/tmp/audio.webm", str(output_file))

        assert result["success"] is True
        assert result["output_path"] == str(output_file)

    def test_ffmpeg_not_found_returns_error(self, tmp_path, mocker):
        """FileNotFoundError from ffmpeg returns error with install hint."""
        mocker.patch("subprocess.run", side_effect=FileNotFoundError)

        from utils.youtube_downloader import merge_video_audio_ffmpeg

        result = merge_video_audio_ffmpeg("/v.mp4", "/a.webm", str(tmp_path / "out.mp4"))

        assert result["success"] is False
        assert "ffmpeg" in result["error"].lower()

    def test_ffmpeg_process_error_returns_error(self, tmp_path, mocker):
        """CalledProcessError from ffmpeg returns error with stderr content."""
        import subprocess

        mocker.patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "ffmpeg", stderr=b"error output"),
        )

        from utils.youtube_downloader import merge_video_audio_ffmpeg

        result = merge_video_audio_ffmpeg("/v.mp4", "/a.webm", str(tmp_path / "out.mp4"))

        assert result["success"] is False
        assert "FFmpeg error" in result["error"]

    def test_output_file_not_created_returns_error(self, tmp_path, mock_subprocess_run):
        """Merge returns error if output file does not exist after ffmpeg."""
        from utils.youtube_downloader import merge_video_audio_ffmpeg

        result = merge_video_audio_ffmpeg("/v.mp4", "/a.webm", str(tmp_path / "missing_output.mp4"))

        assert result["success"] is False
        assert result["error"] == "Output file not created"

    def test_generic_exception_returns_error(self, tmp_path, mocker):
        """Generic exception during subprocess.run returns error."""
        mocker.patch("subprocess.run", side_effect=OSError("permission denied"))

        from utils.youtube_downloader import merge_video_audio_ffmpeg

        result = merge_video_audio_ffmpeg("/v.mp4", "/a.webm", str(tmp_path / "out.mp4"))

        assert result["success"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# merge_video_audio_moviepy
# ---------------------------------------------------------------------------


class TestMergeVideoAudioMoviepy:
    def test_returns_error_when_moviepy_not_installed(self, monkeypatch):
        """ImportError on moviepy returns error dict without raising."""
        monkeypatch.setitem(sys.modules, "moviepy", None)
        monkeypatch.setitem(sys.modules, "moviepy.editor", None)

        from utils.youtube_downloader import merge_video_audio_moviepy

        result = merge_video_audio_moviepy("/v.mp4", "/a.webm", "/out.mp4")

        assert result["success"] is False
        assert "MoviePy" in result["error"]


# ---------------------------------------------------------------------------
# download_youtube_subtitles
# ---------------------------------------------------------------------------


class TestDownloadYoutubeSubtitles:
    def test_no_subtitles_available_returns_error(self, tmp_path, mocker):
        """Video with no subtitles returns has_subtitles=False and error."""
        info = _make_ydl_info(subtitles={}, automatic_captions={})
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = info

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_subtitles

        result = download_youtube_subtitles("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["has_subtitles"] is False
        assert result["error"] == "No subtitles available"

    def test_successful_subtitle_download(self, tmp_path, mocker):
        """Subtitle file downloaded successfully returns success=True."""
        info = _make_ydl_info(
            video_id="vid888",
            subtitles={"es": [{"ext": "srt"}]},
        )

        call_count = [0]

        def fake_ydl_factory(opts):
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                m.extract_info.return_value = info
            else:
                # Create an SRT file in srt_files dir
                srt_dir = tmp_path / "srt_files"
                srt_dir.mkdir(parents=True, exist_ok=True)
                srt_file = srt_dir / "vid888_es.srt"
                srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n")

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_youtube_subtitles

        result = download_youtube_subtitles("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["has_subtitles"] is True
        assert result["success"] is True
        assert len(result["subtitle_files"]) > 0

    def test_no_downloaded_files_returns_error(self, tmp_path, mocker):
        """When no subtitle files are actually downloaded, returns error."""
        info = _make_ydl_info(
            video_id="vid999",
            subtitles={"es": [{"ext": "srt"}]},
        )

        def fake_ydl_factory(opts):
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)
            m.extract_info.return_value = info
            # No file is created
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_youtube_subtitles

        result = download_youtube_subtitles(
            "https://youtube.com/watch?v=x",
            str(tmp_path),
            languages=["es"],
        )

        assert result["success"] is False
        assert "Failed to download subtitles" in result["error"]

    def test_exception_returns_error(self, tmp_path, mocker):
        """Exception during info extraction returns error dict."""
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.side_effect = RuntimeError("rate limited")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import download_youtube_subtitles

        result = download_youtube_subtitles("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# _download_subtitle_files (lifted out of download_youtube_subtitles, #272 slice 5 PR8)
# ---------------------------------------------------------------------------


class TestDownloadSubtitleFiles:
    def _fake_ydl_factory(self, on_download):
        """Return a `yt_dlp.YoutubeDL(opts)` factory whose `.download()` calls `on_download()`."""

        def factory(opts):
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)
            m.download.side_effect = on_download
            return m

        return factory

    def test_exception_for_one_language_continues_to_next(self, tmp_path, mocker):
        """A per-language exception is swallowed; the next language is still attempted."""
        calls = []

        def on_download(urls):
            calls.append(urls)
            if len(calls) == 1:
                raise RuntimeError("rate limited for this language")
            # Second (successful) language: create the matching SRT file.
            srt_dir = tmp_path / "srt_files"
            srt_dir.mkdir(parents=True, exist_ok=True)
            (srt_dir / "vid1_en.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nHi\n")

        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=self._fake_ydl_factory(on_download),
        )

        from utils.youtube_downloader import _download_subtitle_files

        result = _download_subtitle_files("https://youtube.com/watch?v=x", "vid1", str(tmp_path), ["es", "en"])

        assert len(calls) == 2  # both languages attempted, no raise propagated
        assert len(result) == 1
        assert result[0]["language"] == "en"

    def test_breaks_after_first_language_with_files(self, tmp_path, mocker):
        """Once a language yields files, later languages are never attempted."""
        calls = []

        def on_download(urls):
            calls.append(urls)
            srt_dir = tmp_path / "srt_files"
            srt_dir.mkdir(parents=True, exist_ok=True)
            (srt_dir / "vid1_es.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nHola\n")

        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=self._fake_ydl_factory(on_download),
        )

        from utils.youtube_downloader import _download_subtitle_files

        result = _download_subtitle_files("https://youtube.com/watch?v=x", "vid1", str(tmp_path), ["es", "en", "auto"])

        assert len(calls) == 1  # "en" and "auto" never attempted
        assert len(result) == 1
        assert result[0]["language"] == "es"

    @pytest.mark.parametrize(
        ("file_name", "lang", "expected_is_auto"),
        [
            ("vid1_es-AUTO.srt", "es", True),
            ("vid1_zz.srt", "auto", True),
            ("vid1_es.srt", "es", False),
        ],
    )
    def test_is_auto_pinned_on_both_halves_of_the_or(self, tmp_path, mocker, file_name, lang, expected_is_auto):
        """`is_auto` fires on either an "auto" filename or lang == "auto" — pinned on both halves."""

        def on_download(urls):
            srt_dir = tmp_path / "srt_files"
            srt_dir.mkdir(parents=True, exist_ok=True)
            (srt_dir / file_name).write_text("1\n00:00:00,000 --> 00:00:01,000\nHi\n")

        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=self._fake_ydl_factory(on_download),
        )

        from utils.youtube_downloader import _download_subtitle_files

        result = _download_subtitle_files("https://youtube.com/watch?v=x", "vid1", str(tmp_path), [lang])

        assert len(result) == 1
        assert result[0]["is_auto_generated"] is expected_is_auto

    def test_all_languages_failing_returns_empty_list(self, tmp_path, mocker):
        """When every language raises, the helper returns `[]` without raising."""

        def on_download(urls):
            raise RuntimeError("always fails")

        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=self._fake_ydl_factory(on_download),
        )

        from utils.youtube_downloader import _download_subtitle_files

        result = _download_subtitle_files("https://youtube.com/watch?v=x", "vid1", str(tmp_path), ["es", "en"])

        assert result == []

    def test_one_entry_per_file_sharing_the_same_language(self, tmp_path, mocker):
        """Multiple matching files for one language each get their own entry, same language."""

        def on_download(urls):
            srt_dir = tmp_path / "srt_files"
            srt_dir.mkdir(parents=True, exist_ok=True)
            (srt_dir / "vid1_es_a.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nA\n")
            (srt_dir / "vid1_es_b.srt").write_text("1\n00:00:00,000 --> 00:00:01,000\nB\n")

        mocker.patch(
            "utils.youtube_downloader.yt_dlp.YoutubeDL",
            side_effect=self._fake_ydl_factory(on_download),
        )

        from utils.youtube_downloader import _download_subtitle_files

        result = _download_subtitle_files("https://youtube.com/watch?v=x", "vid1", str(tmp_path), ["es"])

        assert len(result) == 2
        assert {entry["language"] for entry in result} == {"es"}


# ---------------------------------------------------------------------------
# probe_live_status (finished-stream guard shared helper)
# ---------------------------------------------------------------------------


class TestProbeLiveStatus:
    def _fake_ydl(self, info=None, exc=None):
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        if exc is not None:
            fake_ydl.extract_info.side_effect = exc
        else:
            fake_ydl.extract_info.return_value = info
        return fake_ydl

    @pytest.mark.parametrize(
        "status",
        ["was_live", "not_live", "post_live", "is_live", "is_upcoming"],
    )
    def test_returns_live_status_round_trip(self, mocker, status):
        """Every yt-dlp live_status value round-trips unchanged."""
        fake_ydl = self._fake_ydl(info={"live_status": status})
        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import probe_live_status

        assert probe_live_status("https://youtube.com/watch?v=x") == status
        fake_ydl.extract_info.assert_called_once_with("https://youtube.com/watch?v=x", download=False)

    def test_missing_live_status_key_returns_none(self, mocker):
        """A dict without live_status returns None."""
        fake_ydl = self._fake_ydl(info={"id": "x"})
        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import probe_live_status

        assert probe_live_status("https://youtube.com/watch?v=x") is None

    def test_download_error_returns_none(self, mocker):
        """A yt_dlp DownloadError is swallowed -> None (never raises)."""
        import yt_dlp

        fake_ydl = self._fake_ydl(exc=yt_dlp.utils.DownloadError("rate limited"))
        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import probe_live_status

        assert probe_live_status("https://youtube.com/watch?v=x") is None

    def test_generic_exception_returns_none(self, mocker):
        """Any generic exception is swallowed -> None (never raises)."""
        fake_ydl = self._fake_ydl(exc=RuntimeError("boom"))
        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)

        from utils.youtube_downloader import probe_live_status

        assert probe_live_status("https://youtube.com/watch?v=x") is None

    def test_cookiefile_set_when_file_exists(self, tmp_path, mocker):
        """cookiefile is added to the opts only when the file exists."""
        cookies = tmp_path / "cookies.txt"
        cookies.write_text("cookies")

        captured: dict = {}

        def capture_ydl(opts):
            captured.update(opts)
            fake_ydl = self._fake_ydl(info={"live_status": "was_live"})
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import probe_live_status

        probe_live_status("https://youtube.com/watch?v=x", str(cookies))

        assert captured.get("cookiefile") == str(cookies)

    def test_no_cookiefile_when_absent(self, mocker):
        """No cookiefile key when cookies_file is None."""
        captured: dict = {}

        def capture_ydl(opts):
            captured.update(opts)
            return self._fake_ydl(info={"live_status": "was_live"})

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import probe_live_status

        probe_live_status("https://youtube.com/watch?v=x", None)

        assert "cookiefile" not in captured

    def test_timeout_forwarded_as_socket_timeout(self, mocker):
        """timeout is forwarded to yt-dlp opts as socket_timeout."""
        captured: dict = {}

        def capture_ydl(opts):
            captured.update(opts)
            return self._fake_ydl(info={"live_status": "was_live"})

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import probe_live_status

        probe_live_status("https://youtube.com/watch?v=x", timeout=17)

        assert captured.get("socket_timeout") == 17
        assert captured.get("skip_download") is True


# ---------------------------------------------------------------------------
# download_youtube_video_for_upload — live_status guard (defense-in-depth)
# ---------------------------------------------------------------------------


class TestDownloadGuardLiveStatus:
    @pytest.mark.parametrize("status", ["post_live", "is_live", "is_upcoming"])
    def test_not_ready_status_skips_before_any_download(self, tmp_path, mocker, status):
        """A not-ready live_status returns a skip result with no download attempt."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value=status)
        mock_pytubefix = mocker.patch("utils.youtube_downloader.download_with_pytubefix")
        mock_ydl_class = mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        assert result["skipped"] is True
        assert status in result["error"]
        mock_pytubefix.assert_not_called()
        mock_ydl_class.assert_not_called()

    def test_guard_runs_before_pytubefix_attempt(self, tmp_path, mocker):
        """With use_pytubefix_first=True, the guard short-circuits before pytubefix."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value="is_live")
        mock_pytubefix = mocker.patch("utils.youtube_downloader.download_with_pytubefix")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), use_pytubefix_first=True
        )

        assert result["skipped"] is True
        mock_pytubefix.assert_not_called()

    @pytest.mark.parametrize("status", ["was_live", "not_live"])
    def test_ready_status_proceeds_to_download(self, tmp_path, mocker, status):
        """A ready live_status lets the normal download path proceed."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value=status)
        mock_pytubefix = mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"},
        )

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        mock_pytubefix.assert_called_once()

    def test_probe_error_none_proceeds_to_download(self, tmp_path, mocker):
        """A probe error (None) is non-blocking: the download proceeds."""
        mocker.patch("utils.youtube_downloader.probe_live_status", return_value=None)
        mock_pytubefix = mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"},
        )

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        mock_pytubefix.assert_called_once()

    def test_guard_disabled_issues_no_probe(self, tmp_path, mocker):
        """guard_live_status=False never probes and keeps the legacy flow."""
        mock_probe = mocker.patch("utils.youtube_downloader.probe_live_status")
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": True, "file_path": "/tmp/v.mp4", "resolution": "720p"},
        )

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), guard_live_status=False
        )

        assert result["success"] is True
        mock_probe.assert_not_called()


# ---------------------------------------------------------------------------
# _warn_if_not_h264 (ffmpeg-codec-aware-cuts, Slice 3)
# ---------------------------------------------------------------------------


class TestWarnIfNotH264:
    def test_h264_detected_logs_no_warning(self, mocker, caplog):
        from utils.youtube_downloader import _warn_if_not_h264

        mocker.patch("utils.youtube_downloader.detect_video_codec", return_value="h264")

        with caplog.at_level("WARNING"):
            codec = _warn_if_not_h264("/tmp/video.mp4", context="vid123")

        assert codec == "h264"
        assert not any(r.levelname == "WARNING" for r in caplog.records)

    def test_av1_detected_logs_warning_with_context(self, mocker, caplog):
        from utils.youtube_downloader import _warn_if_not_h264

        mocker.patch("utils.youtube_downloader.detect_video_codec", return_value="av1")

        with caplog.at_level("WARNING"):
            codec = _warn_if_not_h264("/tmp/video.mp4", context="vid123")

        assert codec == "av1"
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert warnings
        message = warnings[0].getMessage()
        assert "av1" in message
        assert "vid123" in message
        assert "avc1" in message.lower() or "h264" in message.lower()

    def test_unknown_detected_logs_warning(self, mocker, caplog):
        from utils.youtube_downloader import _warn_if_not_h264

        mocker.patch("utils.youtube_downloader.detect_video_codec", return_value="unknown")

        with caplog.at_level("WARNING"):
            codec = _warn_if_not_h264("/tmp/video.mp4", context="vid123")

        assert codec == "unknown"
        assert any(r.levelname == "WARNING" for r in caplog.records)

    def test_reuses_shared_detect_video_codec_no_reimplementation(self, mocker):
        from utils.youtube_downloader import _warn_if_not_h264

        mock_detect = mocker.patch("utils.youtube_downloader.detect_video_codec", return_value="h264")
        _warn_if_not_h264("/tmp/video.mp4", context="ctx")

        mock_detect.assert_called_once_with("/tmp/video.mp4")


class TestWarnIfNotH264WiredIntoYtdlpSuccessPath:
    def test_called_post_download_in_ytdlp_success_block(self, tmp_path, mocker):
        """_warn_if_not_h264 must be invoked after the yt-dlp success logs, with
        context=info.get('id') or youtube_url, and must not change any
        format_map/ydl_opts/download-behavior args (same download call as before)."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        fake_file = tmp_path / "vid123_Test Video.mp4"
        fake_file.write_bytes(b"\x00" * 1024)

        fake_info = _make_ydl_info()
        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)
        mock_warn = mocker.patch("utils.youtube_downloader._warn_if_not_h264", return_value="h264")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is True
        mock_warn.assert_called_once_with(str(fake_file), context="vid123")

    def test_not_called_when_yt_dlp_download_fails(self, tmp_path, mocker):
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": False, "error": "fail"},
        )

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = _make_ydl_info()
        fake_ydl.prepare_filename.return_value = str(tmp_path / "does_not_exist.mp4")

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", return_value=fake_ydl)
        mock_warn = mocker.patch("utils.youtube_downloader._warn_if_not_h264")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload("https://youtube.com/watch?v=x", str(tmp_path))

        assert result["success"] is False
        mock_warn.assert_not_called()

    def test_called_before_early_return_on_pytubefix_success(self, tmp_path, mocker):
        """Secondary insertion point (design Decision 4): the pytubefix success
        early-return path also observes AV1 via the same helper."""
        mocker.patch(
            "utils.youtube_downloader.download_with_pytubefix",
            return_value={"success": True, "file_path": "/tmp/pytubefix_video.mp4", "resolution": "720p"},
        )
        mock_ydl_class = mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL")
        mock_warn = mocker.patch("utils.youtube_downloader._warn_if_not_h264", return_value="h264")

        from utils.youtube_downloader import download_youtube_video_for_upload

        result = download_youtube_video_for_upload(
            "https://youtube.com/watch?v=x", str(tmp_path), guard_live_status=False
        )

        assert result["success"] is True
        mock_warn.assert_called_once_with("/tmp/pytubefix_video.mp4", context="https://youtube.com/watch?v=x")
        mock_ydl_class.assert_not_called()


# ---------------------------------------------------------------------------
# YOUTUBE_DOWNLOAD_PROXY (download-only proxy env var, homeserver-vps-dev-foundation)
# ---------------------------------------------------------------------------


class TestDownloadProxyHelpers:
    def test_download_proxy_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)

        from utils.youtube_downloader import _download_proxy

        assert _download_proxy() == ""

    def test_download_proxy_reads_and_strips_env(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "  http://proxy.test:8888  ")

        from utils.youtube_downloader import _download_proxy

        assert _download_proxy() == "http://proxy.test:8888"

    def test_apply_download_proxy_sets_key_when_configured(self, monkeypatch):
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")

        from utils.youtube_downloader import _apply_download_proxy

        opts: dict = {}
        _apply_download_proxy(opts)

        assert opts["proxy"] == "http://proxy.test:8888"

    def test_apply_download_proxy_no_op_when_unset(self, monkeypatch):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)

        from utils.youtube_downloader import _apply_download_proxy

        opts: dict = {}
        _apply_download_proxy(opts)

        assert "proxy" not in opts


class TestDownloadProxyWiredIntoDownloadFunctions:
    def test_probe_live_status_applies_proxy_when_configured(self, monkeypatch, mocker):
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")
        captured_opts: dict = {}

        def capture_ydl(opts):
            captured_opts.update(opts)
            fake_ydl = MagicMock()
            fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
            fake_ydl.__exit__ = MagicMock(return_value=False)
            fake_ydl.extract_info.return_value = {"live_status": "was_live"}
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import probe_live_status

        probe_live_status("https://youtube.com/watch?v=x")

        assert captured_opts["proxy"] == "http://proxy.test:8888"

    def test_probe_live_status_no_proxy_key_when_unset(self, monkeypatch, mocker):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)
        captured_opts: dict = {}

        def capture_ydl(opts):
            captured_opts.update(opts)
            fake_ydl = MagicMock()
            fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
            fake_ydl.__exit__ = MagicMock(return_value=False)
            fake_ydl.extract_info.return_value = {"live_status": "was_live"}
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import probe_live_status

        probe_live_status("https://youtube.com/watch?v=x")

        assert "proxy" not in captured_opts

    def test_download_audio_only_applies_proxy_when_configured(self, tmp_path, monkeypatch, mocker):
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")
        captured_opts: dict = {}

        fake_file = tmp_path / "vid123_Test Video_audio.webm"
        fake_file.write_bytes(b"\x00" * 256)
        fake_info = _make_ydl_info()

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_audio_only

        download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert captured_opts["proxy"] == "http://proxy.test:8888"

    def test_download_audio_only_no_proxy_key_when_unset(self, tmp_path, monkeypatch, mocker):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)
        captured_opts: dict = {}

        fake_file = tmp_path / "vid123_Test Video_audio.webm"
        fake_file.write_bytes(b"\x00" * 256)
        fake_info = _make_ydl_info()

        fake_ydl = MagicMock()
        fake_ydl.__enter__ = MagicMock(return_value=fake_ydl)
        fake_ydl.__exit__ = MagicMock(return_value=False)
        fake_ydl.extract_info.return_value = fake_info
        fake_ydl.prepare_filename.return_value = str(fake_file)

        def capture_ydl(opts):
            captured_opts.update(opts)
            return fake_ydl

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=capture_ydl)

        from utils.youtube_downloader import download_audio_only

        download_audio_only("https://youtube.com/watch?v=x", str(tmp_path))

        assert "proxy" not in captured_opts

    def test_download_audio_in_chunks_applies_proxy_to_every_ydl_opts(self, tmp_path, monkeypatch, mocker):
        """Proxy is applied to both the info-probe opts and each per-chunk opts."""
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")
        # conftest.py's global yt_dlp stub predates download_ranges support: its
        # "utils" SimpleNamespace lacks download_range_func. create=True adds it
        # for this test only, matching a real-ish return value.
        mocker.patch("yt_dlp.utils.download_range_func", create=True, return_value="RANGE")
        captured_opts_list: list[dict] = []
        chunk_dir = tmp_path / "audio_chunks"
        call_count = [0]

        def fake_ydl_factory(opts):
            captured_opts_list.append(dict(opts))
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                m.extract_info.return_value = _make_ydl_info(video_id="vidproxy", duration=600)
            else:
                chunk_index = call_count[0] - 1
                chunk_dir.mkdir(parents=True, exist_ok=True)
                chunk_path = chunk_dir / f"vidproxy_chunk_{chunk_index:03d}.webm"
                chunk_path.write_bytes(b"\x00" * 128)

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_audio_in_chunks

        download_audio_in_chunks("https://youtube.com/watch?v=x", str(tmp_path), chunk_duration_minutes=10)

        assert len(captured_opts_list) >= 2
        assert all(opts.get("proxy") == "http://proxy.test:8888" for opts in captured_opts_list)

    def test_download_audio_in_chunks_no_proxy_key_when_unset(self, tmp_path, monkeypatch, mocker):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)
        mocker.patch("yt_dlp.utils.download_range_func", create=True, return_value="RANGE")
        captured_opts_list: list[dict] = []
        chunk_dir = tmp_path / "audio_chunks"
        call_count = [0]

        def fake_ydl_factory(opts):
            captured_opts_list.append(dict(opts))
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                m.extract_info.return_value = _make_ydl_info(video_id="vidnoproxy", duration=600)
            else:
                chunk_index = call_count[0] - 1
                chunk_dir.mkdir(parents=True, exist_ok=True)
                chunk_path = chunk_dir / f"vidnoproxy_chunk_{chunk_index:03d}.webm"
                chunk_path.write_bytes(b"\x00" * 128)

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_audio_in_chunks

        download_audio_in_chunks("https://youtube.com/watch?v=x", str(tmp_path), chunk_duration_minutes=10)

        assert len(captured_opts_list) >= 2
        assert all("proxy" not in opts for opts in captured_opts_list)

    def test_download_youtube_subtitles_applies_proxy_to_every_ydl_opts(self, tmp_path, monkeypatch, mocker):
        """Proxy is applied to both the info-probe opts and the per-language opts."""
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")
        captured_opts_list: list[dict] = []

        info = _make_ydl_info(video_id="vidsub", subtitles={"es": [{"ext": "srt"}]})
        call_count = [0]

        def fake_ydl_factory(opts):
            captured_opts_list.append(dict(opts))
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                m.extract_info.return_value = info
            else:
                srt_dir = tmp_path / "srt_files"
                srt_dir.mkdir(parents=True, exist_ok=True)
                srt_file = srt_dir / "vidsub_es.srt"
                srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n")

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_youtube_subtitles

        download_youtube_subtitles("https://youtube.com/watch?v=x", str(tmp_path), languages=["es"])

        assert len(captured_opts_list) >= 2
        assert all(opts.get("proxy") == "http://proxy.test:8888" for opts in captured_opts_list)

    def test_download_youtube_subtitles_no_proxy_key_when_unset(self, tmp_path, monkeypatch, mocker):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)
        captured_opts_list: list[dict] = []

        info = _make_ydl_info(video_id="vidsub2", subtitles={"es": [{"ext": "srt"}]})
        call_count = [0]

        def fake_ydl_factory(opts):
            captured_opts_list.append(dict(opts))
            m = MagicMock()
            m.__enter__ = MagicMock(return_value=m)
            m.__exit__ = MagicMock(return_value=False)

            if call_count[0] == 0:
                m.extract_info.return_value = info
            else:
                srt_dir = tmp_path / "srt_files"
                srt_dir.mkdir(parents=True, exist_ok=True)
                srt_file = srt_dir / "vidsub2_es.srt"
                srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nHello\n")

            call_count[0] += 1
            return m

        mocker.patch("utils.youtube_downloader.yt_dlp.YoutubeDL", side_effect=fake_ydl_factory)

        from utils.youtube_downloader import download_youtube_subtitles

        download_youtube_subtitles("https://youtube.com/watch?v=x", str(tmp_path), languages=["es"])

        assert len(captured_opts_list) >= 2
        assert all("proxy" not in opts for opts in captured_opts_list)

    def _fake_pytubefix_youtube(self, mocker, video_id: str):
        """Build a fake pytubefix module whose YouTube() short-circuits to
        'no suitable stream found' (no ffmpeg/subprocess involvement) so the
        test only needs to observe the YouTube(...) constructor call args."""
        mock_yt_instance = MagicMock()
        mock_yt_instance.video_id = video_id
        mock_yt_instance.title = "Test"
        mock_yt_instance.streams.all.return_value = []
        mock_filter_chain = MagicMock()
        mock_filter_chain.filter.return_value = mock_filter_chain
        mock_filter_chain.order_by.return_value = mock_filter_chain
        mock_filter_chain.desc.return_value = mock_filter_chain
        mock_filter_chain.first.return_value = None
        mock_yt_instance.streams.filter.return_value = mock_filter_chain

        fake_pytubefix = types.ModuleType("pytubefix")
        mock_youtube_cls = MagicMock(return_value=mock_yt_instance)
        fake_pytubefix.YouTube = mock_youtube_cls
        fake_cli = types.ModuleType("pytubefix.cli")
        fake_cli.on_progress = MagicMock()

        mocker.patch.dict(sys.modules, {"pytubefix": fake_pytubefix, "pytubefix.cli": fake_cli})
        return mock_youtube_cls

    def test_pytubefix_passes_proxies_kwarg_when_configured(self, tmp_path, monkeypatch, mocker):
        monkeypatch.setenv("YOUTUBE_DOWNLOAD_PROXY", "http://proxy.test:8888")
        import importlib

        mock_youtube_cls = self._fake_pytubefix_youtube(mocker, "vidproxy2")

        from utils import youtube_downloader

        importlib.reload(youtube_downloader)

        youtube_downloader.download_with_pytubefix("https://youtube.com/watch?v=x", str(tmp_path))

        _, kwargs = mock_youtube_cls.call_args
        assert kwargs["proxies"] == {
            "http": "http://proxy.test:8888",
            "https": "http://proxy.test:8888",
        }

    def test_pytubefix_proxies_kwarg_none_when_unset(self, tmp_path, monkeypatch, mocker):
        monkeypatch.delenv("YOUTUBE_DOWNLOAD_PROXY", raising=False)
        import importlib

        mock_youtube_cls = self._fake_pytubefix_youtube(mocker, "vidproxy3")

        from utils import youtube_downloader

        importlib.reload(youtube_downloader)

        youtube_downloader.download_with_pytubefix("https://youtube.com/watch?v=x", str(tmp_path))

        _, kwargs = mock_youtube_cls.call_args
        assert kwargs["proxies"] is None
