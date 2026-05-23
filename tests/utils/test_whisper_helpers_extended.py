"""Extended tests for utils/whisper_helpers.py — uncovered functions."""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# save_srt_file — exception handler (fallback path, lines 95-102)
# ---------------------------------------------------------------------------

class TestSaveSrtFileExceptionFallback:

    def test_fallback_when_srt_dir_creation_fails(self, tmp_path, mocker):
        """Exception in mkdir causes fallback to save next to audio file."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 8)

        original_mkdir = Path.mkdir
        call_count = [0]

        def patched_mkdir(self, *args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                raise OSError("Cannot create directory")
            return original_mkdir(self, *args, **kwargs)

        mocker.patch.object(Path, "mkdir", patched_mkdir)

        from utils.whisper_helpers import save_srt_file

        srt_content = "1\n00:00:00,000 --> 00:00:01,000\nHello\n"
        result = save_srt_file(srt_content, str(audio_file))

        assert result is not None
        assert result.endswith(".srt")


# ---------------------------------------------------------------------------
# transcribe_audio_file_with_local_whisper (lines 131-187)
# ---------------------------------------------------------------------------

class TestTranscribeAudioFileWithLocalWhisper:

    def test_missing_audio_file_returns_error(self, tmp_path):
        """Non-existent audio file returns error dict."""
        from utils.whisper_helpers import transcribe_audio_file_with_local_whisper

        result = transcribe_audio_file_with_local_whisper(
            str(tmp_path / "nonexistent.webm")
        )

        assert result["success"] is False
        assert "not found" in result["error"].lower()

    def test_whisper_not_installed_returns_error(self, tmp_path, monkeypatch):
        """ImportError on whisper returns error dict."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        monkeypatch.setitem(sys.modules, "whisper", None)

        from utils.whisper_helpers import transcribe_audio_file_with_local_whisper

        result = transcribe_audio_file_with_local_whisper(str(audio_file))

        assert result["success"] is False
        assert "Whisper" in result["error"] or "whisper" in result["error"].lower()

    def test_successful_transcription_returns_srt_path(self, tmp_path, mocker):
        """Successful whisper transcription returns success=True with srt_path."""
        audio_file = tmp_path / "downloads" / "2025-05-22" / "v1" / "audio_chunks" / "chunk_001.webm"
        audio_file.parent.mkdir(parents=True)
        audio_file.write_bytes(b"\x00" * 16)

        fake_whisper = MagicMock()
        fake_model = MagicMock()
        fake_model.transcribe.return_value = {
            "text": "Hello world",
            "segments": [
                {"start": 0.0, "end": 1.0, "text": "Hello world"}
            ],
        }
        fake_whisper.load_model.return_value = fake_model

        mocker.patch.dict(sys.modules, {"whisper": fake_whisper})

        from utils.whisper_helpers import transcribe_audio_file_with_local_whisper

        result = transcribe_audio_file_with_local_whisper(str(audio_file))

        assert result["success"] is True
        assert result["text"] == "Hello world"
        assert result["srt_path"] is not None

    def test_no_segments_sets_srt_path_none(self, tmp_path, mocker):
        """Transcription with no segments returns srt_path=None."""
        audio_file = tmp_path / "downloads" / "2025-05-22" / "v1" / "audio_chunks" / "chunk_001.webm"
        audio_file.parent.mkdir(parents=True)
        audio_file.write_bytes(b"\x00" * 16)

        fake_whisper = MagicMock()
        fake_model = MagicMock()
        fake_model.transcribe.return_value = {
            "text": "Partial",
            "segments": [],
        }
        fake_whisper.load_model.return_value = fake_model

        mocker.patch.dict(sys.modules, {"whisper": fake_whisper})

        from utils.whisper_helpers import transcribe_audio_file_with_local_whisper

        result = transcribe_audio_file_with_local_whisper(str(audio_file))

        assert result["success"] is True
        assert result["srt_path"] is None

    def test_exception_during_transcription_returns_error(self, tmp_path, mocker):
        """Exception during model.transcribe returns error dict."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        fake_whisper = MagicMock()
        fake_model = MagicMock()
        fake_model.transcribe.side_effect = RuntimeError("CUDA error")
        fake_whisper.load_model.return_value = fake_model

        mocker.patch.dict(sys.modules, {"whisper": fake_whisper})

        from utils.whisper_helpers import transcribe_audio_file_with_local_whisper

        result = transcribe_audio_file_with_local_whisper(str(audio_file))

        assert result["success"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# transcribe_audio_file — fallback paths (lines 225-311)
# ---------------------------------------------------------------------------

class TestTranscribeAudioFileFallback:

    def test_use_local_whisper_false_uses_api_path(self, tmp_path, mocker):
        """use_local_whisper=False goes straight to Docker API path."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "text": "Hello from API",
            "segments": [],
            "duration": 1.0,
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        from utils.whisper_helpers import transcribe_audio_file

        result = transcribe_audio_file(
            str(audio_file), use_local_whisper=False
        )

        assert result["success"] is True
        mock_post.assert_called_once()

    def test_local_whisper_fails_falls_back_to_api(self, tmp_path, mocker):
        """When local whisper fails, falls back to Docker API."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        mocker.patch(
            "utils.whisper_helpers.transcribe_audio_file_with_local_whisper",
            return_value={"success": False, "error": "CUDA error"},
        )

        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "text": "From API fallback",
            "segments": [],
            "duration": 2.0,
        }
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        from utils.whisper_helpers import transcribe_audio_file

        result = transcribe_audio_file(str(audio_file), use_local_whisper=True)

        assert result["success"] is True
        mock_post.assert_called_once()

    def test_local_whisper_success_returns_immediately(self, tmp_path, mocker):
        """use_local_whisper=True with success returns immediately without calling API."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        mocker.patch(
            "utils.whisper_helpers.transcribe_audio_file_with_local_whisper",
            return_value={
                "success": True,
                "text": "Local result",
                "srt_path": "/data/x.srt",
                "error": None,
            },
        )
        mock_post = mocker.patch("utils.whisper_helpers.requests.post")

        from utils.whisper_helpers import transcribe_audio_file

        result = transcribe_audio_file(str(audio_file), use_local_whisper=True)

        assert result["success"] is True
        assert result["text"] == "Local result"
        mock_post.assert_not_called()

    def test_api_unexpected_exception_returns_error(self, tmp_path, mocker):
        """Unexpected exception during API call returns error dict."""
        audio_file = tmp_path / "test.webm"
        audio_file.write_bytes(b"\x00" * 16)

        mock_post = mocker.patch(
            "utils.whisper_helpers.requests.post",
            side_effect=RuntimeError("unexpected crash"),
        )

        from utils.whisper_helpers import transcribe_audio_file

        result = transcribe_audio_file(str(audio_file), use_local_whisper=False)

        assert result["success"] is False
        assert result["error"] is not None


# ---------------------------------------------------------------------------
# transcribe_audio_chunks (lines 333-383)
# ---------------------------------------------------------------------------

class TestTranscribeAudioChunks:

    def test_empty_chunks_returns_zero(self):
        """Empty audio_chunks list returns zero results."""
        from utils.whisper_helpers import transcribe_audio_chunks

        result = transcribe_audio_chunks([])

        assert result["total_chunks"] == 0
        assert result["successful_transcriptions"] == 0
        assert result["chunks"] == []

    def test_chunk_without_file_path_recorded_as_failed(self):
        """Chunk missing 'file_path' is recorded as failed."""
        from utils.whisper_helpers import transcribe_audio_chunks

        result = transcribe_audio_chunks([{"chunk_number": 1}])

        assert result["total_chunks"] == 1
        assert result["successful_transcriptions"] == 0
        assert result["chunks"][0]["success"] is False
        assert "file_path" in result["chunks"][0]["error"].lower()

    def test_successful_transcription_counted(self, tmp_path, mocker):
        """Successful chunk transcription is counted."""
        audio_file = tmp_path / "chunk_001.webm"
        audio_file.write_bytes(b"\x00" * 16)

        mocker.patch(
            "utils.whisper_helpers.transcribe_audio_file",
            return_value={
                "success": True,
                "text": "Hello",
                "srt_path": "/data/chunk_001.srt",
                "error": None,
            },
        )

        from utils.whisper_helpers import transcribe_audio_chunks

        result = transcribe_audio_chunks(
            [
                {
                    "file_path": str(audio_file),
                    "chunk_number": 1,
                    "start_time": 0,
                    "end_time": 60,
                    "duration": 60,
                }
            ]
        )

        assert result["total_chunks"] == 1
        assert result["successful_transcriptions"] == 1
        assert result["chunks"][0]["chunk_number"] == 1
        assert result["chunks"][0]["start_time"] == 0

    def test_multiple_chunks_counted_correctly(self, tmp_path, mocker):
        """Multiple chunks with mixed results counted correctly."""
        mocker.patch(
            "utils.whisper_helpers.transcribe_audio_file",
            side_effect=[
                {"success": True, "text": "A", "srt_path": "/a.srt", "error": None},
                {"success": False, "text": "", "srt_path": None, "error": "fail"},
            ],
        )

        from utils.whisper_helpers import transcribe_audio_chunks

        chunks = [
            {"file_path": "/chunk1.webm", "chunk_number": 1},
            {"file_path": "/chunk2.webm", "chunk_number": 2},
        ]

        result = transcribe_audio_chunks(chunks)

        assert result["total_chunks"] == 2
        assert result["successful_transcriptions"] == 1


# ---------------------------------------------------------------------------
# merge_srt_files — empty/malformed entry paths (lines 433, 437)
# ---------------------------------------------------------------------------

class TestMergeSrtFilesEdgeCases:

    def test_srt_with_empty_entry_skipped(self, tmp_path):
        """Empty entries (blank lines) between valid SRT blocks are skipped."""
        srt1 = tmp_path / "chunk1.srt"
        # Has an empty entry followed by a valid one, and a short malformed one
        srt1.write_text(
            "\n\n"  # empty entry (will hit line 433 continue)
            "1\n00:00:01,000 --> 00:00:02,000\nValid text\n\n"
            "2\n",  # malformed: only 2 lines (will hit line 437 continue)
            encoding="utf-8",
        )

        output_path = str(tmp_path / "merged.srt")

        from utils.whisper_helpers import merge_srt_files

        result = merge_srt_files([str(srt1)], output_path)

        assert result["success"] is True
        assert result["total_entries"] == 1  # only the valid entry


# ---------------------------------------------------------------------------
# check_whisper_api_health (lines 482-505)
# ---------------------------------------------------------------------------

class TestCheckWhisperApiHealth:

    def test_returns_true_when_api_responds_200(self, mocker):
        """HTTP 200 returns True."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mocker.patch("utils.whisper_helpers.requests.get", return_value=mock_response)

        from utils.whisper_helpers import check_whisper_api_health

        assert check_whisper_api_health() is True

    def test_returns_true_when_api_responds_405(self, mocker):
        """HTTP 405 (method not allowed) still returns True — API is running."""
        mock_response = MagicMock()
        mock_response.status_code = 405
        mocker.patch("utils.whisper_helpers.requests.get", return_value=mock_response)

        from utils.whisper_helpers import check_whisper_api_health

        assert check_whisper_api_health() is True

    def test_returns_true_on_connection_error(self, mocker):
        """Connection error returns True (will use local Whisper library)."""
        mocker.patch(
            "utils.whisper_helpers.requests.get",
            side_effect=ConnectionError("refused"),
        )

        from utils.whisper_helpers import check_whisper_api_health

        assert check_whisper_api_health() is True
