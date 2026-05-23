"""Tests for utils.whisper_helpers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
import requests

from tests.helpers.assertions import assert_error_result, assert_success_result
from utils.whisper_helpers import (
    create_srt_from_segments,
    format_timestamp_srt,
    merge_srt_files,
    save_srt_file,
    transcribe_audio_file,
)


# ---------------------------------------------------------------------------
# format_timestamp_srt
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("seconds,expected", [
    (0.0,       "00:00:00,000"),
    (1.0,       "00:00:01,000"),
    (60.0,      "00:01:00,000"),
    (3600.0,    "01:00:00,000"),
    (3723.456,  "01:02:03,456"),
])
def test_format_timestamp_srt_known_values(seconds, expected):
    assert format_timestamp_srt(seconds) == expected


# ---------------------------------------------------------------------------
# create_srt_from_segments
# ---------------------------------------------------------------------------

class TestCreateSrtFromSegments:
    def test_empty_list_returns_empty_string(self):
        result = create_srt_from_segments([])
        assert result == ""

    def test_single_segment_structure(self):
        segments = [{"start": 0.0, "end": 4.5, "text": " Hello world"}]
        result = create_srt_from_segments(segments)
        assert "1\n" in result
        assert "00:00:00,000 --> 00:00:04,500" in result
        assert "Hello world" in result

    def test_multiple_segments_numbered_sequentially(self):
        segments = [
            {"start": 0.0, "end": 2.0, "text": " First"},
            {"start": 2.0, "end": 4.0, "text": " Second"},
        ]
        result = create_srt_from_segments(segments)
        assert "1\n" in result
        assert "2\n" in result

    def test_text_is_stripped(self):
        segments = [{"start": 0.0, "end": 1.0, "text": "  spaces around  "}]
        result = create_srt_from_segments(segments)
        lines = result.split("\n")
        text_line = next(l for l in lines if "spaces around" in l)
        assert text_line == "spaces around"


# ---------------------------------------------------------------------------
# save_srt_file
# ---------------------------------------------------------------------------

class TestSaveSrtFile:
    def test_saves_to_srt_files_folder(self, tmp_path):
        audio_dir = tmp_path / "downloads" / "2025-05-23" / "vid42" / "audio_chunks"
        audio_dir.mkdir(parents=True)
        audio_file = audio_dir / "chunk_001.webm"
        audio_file.write_bytes(b"\x00" * 8)

        srt_content = "1\n00:00:00,000 --> 00:00:04,000\nHello\n"
        returned_path = save_srt_file(srt_content, str(audio_file))

        saved = Path(returned_path)
        assert saved.exists()
        assert saved.suffix == ".srt"
        assert saved.parent.name == "srt_files"
        assert saved.read_text(encoding="utf-8") == srt_content

    def test_filename_matches_audio_stem(self, tmp_path):
        audio_dir = tmp_path / "downloads" / "2025-05-23" / "vid42" / "audio_chunks"
        audio_dir.mkdir(parents=True)
        audio_file = audio_dir / "chunk_007.webm"
        audio_file.write_bytes(b"\x00" * 8)

        returned_path = save_srt_file("content", str(audio_file))
        assert Path(returned_path).stem == "chunk_007"


# ---------------------------------------------------------------------------
# merge_srt_files
# ---------------------------------------------------------------------------

class TestMergeSrtFiles:
    def test_empty_list_returns_error(self, tmp_path):
        output = str(tmp_path / "merged.srt")
        result = merge_srt_files([], output)
        assert result["success"] is False
        assert result["error"] is not None

    def test_two_valid_files_merged(self, tmp_path, sample_srt_content):
        srt1 = tmp_path / "part1.srt"
        srt2 = tmp_path / "part2.srt"
        srt1.write_text(sample_srt_content, encoding="utf-8")
        srt2.write_text(sample_srt_content, encoding="utf-8")

        output = str(tmp_path / "merged.srt")
        result = merge_srt_files([str(srt1), str(srt2)], output)

        assert result["success"] is True
        assert result["output_path"] == output
        assert result["total_entries"] == 6  # 3 entries x 2 files
        assert Path(output).exists()

    def test_missing_srt_files_are_skipped_without_error(self, tmp_path, sample_srt_content):
        existing = tmp_path / "real.srt"
        existing.write_text(sample_srt_content, encoding="utf-8")
        missing = str(tmp_path / "ghost.srt")

        output = str(tmp_path / "merged.srt")
        result = merge_srt_files([str(existing), missing], output)

        assert result["success"] is True
        assert result["total_entries"] == 3

    def test_merged_content_has_simplified_timestamps(self, tmp_path, sample_srt_content):
        srt1 = tmp_path / "part1.srt"
        srt1.write_text(sample_srt_content, encoding="utf-8")

        output = str(tmp_path / "merged.srt")
        merge_srt_files([str(srt1)], output)

        merged_text = Path(output).read_text(encoding="utf-8")
        assert " --> " in merged_text
        # Simplified timestamps should not contain comma-separated milliseconds
        assert "00:00:00 --> 00:00:04" in merged_text


# ---------------------------------------------------------------------------
# transcribe_audio_file  (Docker API path via use_local_whisper=False)
# ---------------------------------------------------------------------------

class TestTranscribeAudioFile:
    def test_missing_audio_file_returns_error(self, tmp_path):
        missing = str(tmp_path / "nonexistent.webm")
        result = transcribe_audio_file(missing, use_local_whisper=False)
        assert_error_result(result, "not found")

    def test_requests_post_success(self, mocker, temp_audio_file):
        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_response = MagicMock()
        mock_response.text = "Transcribed text content"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = transcribe_audio_file(str(temp_audio_file), use_local_whisper=False)

        assert_success_result(result)
        assert result["text"] == "Transcribed text content"
        assert result["file_path"] == str(temp_audio_file)
        mock_post.assert_called_once()

    def test_requests_timeout_returns_error(self, mocker, temp_audio_file):
        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_post.side_effect = requests.exceptions.Timeout()

        result = transcribe_audio_file(
            str(temp_audio_file), timeout=30, use_local_whisper=False
        )

        assert_error_result(result, "timeout")

    def test_requests_connection_error_returns_error(self, mocker, temp_audio_file):
        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_post.side_effect = requests.exceptions.ConnectionError("refused")

        result = transcribe_audio_file(str(temp_audio_file), use_local_whisper=False)

        assert_error_result(result, "API request error")

    def test_result_contains_duration_field(self, mocker, temp_audio_file):
        mock_post = mocker.patch("utils.whisper_helpers.requests.post")
        mock_response = MagicMock()
        mock_response.text = "content"
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = transcribe_audio_file(str(temp_audio_file), use_local_whisper=False)

        assert "duration" in result
        assert isinstance(result["duration"], float)
