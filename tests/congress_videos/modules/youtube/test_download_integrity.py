"""C1 [RED] Test: subprocess threat matrix for check_source_video_integrity.

Covers all 5 rows from the design threat matrix:
  Row 1 — Clean source (rc=0, empty stderr) → integrity_ok=True
  Row 2 — Corrupt source (rc!=0 OR non-empty stderr) → integrity_ok=False
  Row 3 — Binary missing (FileNotFoundError) → integrity_ok=False (fail-safe)
  Row 4 — Timeout / hung probe (TimeoutExpired) → integrity_ok=False (fail-safe)
  Row 5 — Command injection via file_path (no shell=True) → single list element
"""

from __future__ import annotations

import subprocess
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _downloaded(file_path: str = "/tmp/video.mp4", video_id: str = "vid001") -> dict:
    """Build a minimal downloaded_videos dict for testing."""
    return {
        "total_downloaded": 1,
        "videos": [
            {
                "video_id": video_id,
                "file_path": file_path,
                "file_size_mb": 100.0,
                "duration": 3600,
            }
        ],
    }


# ---------------------------------------------------------------------------
# Row 1: Clean source
# ---------------------------------------------------------------------------


class TestRow1CleanSource:
    def test_clean_probe_returns_integrity_ok_true(self):
        """rc=0, empty stderr → integrity_ok=True for the video entry."""
        clean_result = MagicMock()
        clean_result.returncode = 0
        clean_result.stderr = ""

        with patch("subprocess.run", return_value=clean_result) as mock_run:
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded())

        assert result["videos"][0]["integrity_ok"] is True
        assert result["failed_video_ids"] == []

    def test_clean_probe_no_failed_video_ids(self):
        """A clean probe must result in an empty failed_video_ids list."""
        clean_result = MagicMock(returncode=0, stderr="")

        with patch("subprocess.run", return_value=clean_result):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded(video_id="clean_vid"))

        assert "clean_vid" not in result["failed_video_ids"]


# ---------------------------------------------------------------------------
# Row 2: Corrupt source
# ---------------------------------------------------------------------------


class TestRow2CorruptSource:
    def test_nonzero_returncode_returns_integrity_ok_false(self):
        """rc=1, non-empty stderr → integrity_ok=False."""
        corrupt_result = MagicMock(returncode=1, stderr="moov atom not found")

        with patch("subprocess.run", return_value=corrupt_result):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded(video_id="corrupt_rc1"))

        assert result["videos"][0]["integrity_ok"] is False
        assert "corrupt_rc1" in result["failed_video_ids"]

    def test_zero_returncode_but_nonempty_stderr_returns_false(self):
        """rc=0 BUT non-empty stderr ('moov atom not found') → integrity_ok=False."""
        stderr_only_result = MagicMock(returncode=0, stderr="moov atom not found\n")

        with patch("subprocess.run", return_value=stderr_only_result):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded(video_id="corrupt_stderr"))

        assert result["videos"][0]["integrity_ok"] is False
        assert "corrupt_stderr" in result["failed_video_ids"]


# ---------------------------------------------------------------------------
# Row 3: Binary missing
# ---------------------------------------------------------------------------


class TestRow3BinaryMissing:
    def test_file_not_found_returns_integrity_ok_false(self):
        """FileNotFoundError (ffprobe absent) → integrity_ok=False (fail-safe)."""
        with patch("subprocess.run", side_effect=FileNotFoundError("ffprobe not found")):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded(video_id="no_ffprobe"))

        assert result["videos"][0]["integrity_ok"] is False
        assert "no_ffprobe" in result["failed_video_ids"]


# ---------------------------------------------------------------------------
# Row 4: Timeout / hung probe
# ---------------------------------------------------------------------------


class TestRow4Timeout:
    def test_timeout_expired_returns_integrity_ok_false(self):
        """TimeoutExpired → integrity_ok=False (fail-safe, defer 12h)."""
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd="ffprobe", timeout=60),
        ):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(_downloaded(video_id="timeout_vid"))

        assert result["videos"][0]["integrity_ok"] is False
        assert "timeout_vid" in result["failed_video_ids"]


# ---------------------------------------------------------------------------
# Row 5: Command injection via file_path (no shell=True)
# ---------------------------------------------------------------------------


class TestRow5NoShellInjection:
    def test_file_path_with_special_chars_passed_as_single_list_element(self):
        """file_path containing spaces and special chars must reach subprocess.run
        as a single list element (not a shell string). shell kwarg must be absent
        or explicitly False.
        """
        dangerous_path = "/tmp/vid o & rm -rf foo.mp4"

        clean_result = MagicMock(returncode=0, stderr="")

        with patch("subprocess.run", return_value=clean_result) as mock_run:
            from congress_videos.modules.youtube.download import _run_ffprobe

            _run_ffprobe(dangerous_path)

        call_args, call_kwargs = mock_run.call_args
        cmd_list = call_args[0]

        # The file path must appear as a single element in the argv list
        assert dangerous_path in cmd_list, f"dangerous_path must appear as a single argv element, got: {cmd_list}"
        # shell=True must NOT be passed
        assert call_kwargs.get("shell", False) is False, "subprocess.run must NOT use shell=True (injection risk)"

    def test_subprocess_command_structure_is_list_not_string(self):
        """subprocess.run must receive a list, not a string."""
        clean_result = MagicMock(returncode=0, stderr="")

        with patch("subprocess.run", return_value=clean_result) as mock_run:
            from congress_videos.modules.youtube.download import _run_ffprobe

            _run_ffprobe("/tmp/safe_video.mp4")

        call_args, _ = mock_run.call_args
        cmd = call_args[0]
        assert isinstance(cmd, list), f"subprocess.run must receive a list command, got {type(cmd).__name__}"


# ---------------------------------------------------------------------------
# Integration: check_source_video_integrity return shape
# ---------------------------------------------------------------------------


class TestCheckSourceVideoIntegrityShape:
    def test_returns_same_structure_with_integrity_ok_and_failed_video_ids(self):
        """The return dict must pass through all original keys plus add
        integrity_ok per video and a top-level failed_video_ids list."""
        clean_result = MagicMock(returncode=0, stderr="")

        with patch("subprocess.run", return_value=clean_result):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            downloaded = _downloaded(file_path="/tmp/v.mp4", video_id="v1")
            result = check_source_video_integrity(downloaded)

        # Top-level keys preserved
        assert "total_downloaded" in result
        assert "failed_video_ids" in result
        # Per-video integrity_ok key added
        v = result["videos"][0]
        assert "integrity_ok" in v
        assert v["video_id"] == "v1"
        assert v["file_path"] == "/tmp/v.mp4"

    def test_video_without_file_path_is_marked_failed(self):
        """A video entry missing file_path must be treated as integrity_ok=False."""
        downloaded = {
            "total_downloaded": 0,
            "videos": [{"video_id": "no_file", "error": "download failed"}],
        }

        from congress_videos.modules.youtube.download import check_source_video_integrity

        result = check_source_video_integrity(downloaded)

        assert result["videos"][0]["integrity_ok"] is False
        assert "no_file" in result["failed_video_ids"]

    def test_multiple_videos_mixed_results(self):
        """clean video → integrity_ok=True; corrupt video → integrity_ok=False."""

        def mock_run(cmd, **kwargs):
            path = cmd[-1]
            if "good" in path:
                return MagicMock(returncode=0, stderr="")
            return MagicMock(returncode=1, stderr="error")

        downloaded = {
            "total_downloaded": 2,
            "videos": [
                {"video_id": "good_vid", "file_path": "/tmp/good.mp4"},
                {"video_id": "bad_vid", "file_path": "/tmp/bad.mp4"},
            ],
        }

        with patch("subprocess.run", side_effect=mock_run):
            from congress_videos.modules.youtube.download import check_source_video_integrity

            result = check_source_video_integrity(downloaded)

        by_id = {v["video_id"]: v for v in result["videos"]}
        assert by_id["good_vid"]["integrity_ok"] is True
        assert by_id["bad_vid"]["integrity_ok"] is False
        assert "bad_vid" in result["failed_video_ids"]
        assert "good_vid" not in result["failed_video_ids"]
