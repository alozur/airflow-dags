"""Tests for congress_videos.srt_helpers module."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from congress_videos.srt_helpers import (
    _find_phrase_in_blocks,
    _parse_srt_blocks,
    _srt_timestamp_to_seconds,
    find_srt_for_chapter,
    select_pretrim_window,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_SRT = (
    "1\n00:00:01,000 --> 00:00:05,000\nEl presidente compareció ante el Congreso\n\n"
    "2\n00:00:10,000 --> 00:00:20,000\npara informar sobre el accidente ferroviario\n\n"
    "3\n00:01:00,000 --> 00:01:10,000\nLa oposición criticó la gestión del gobierno\n\n"
    "4\n00:01:15,000 --> 00:01:25,000\nquedan a disposición de sus señorías\n\n"
)


@pytest.fixture
def srt_file(tmp_path) -> str:
    path = tmp_path / "video.srt"
    path.write_text(SAMPLE_SRT, encoding="utf-8")
    return str(path)


# ---------------------------------------------------------------------------
# _srt_timestamp_to_seconds
# ---------------------------------------------------------------------------

class TestSrtTimestampToSeconds:

    def test_zero(self):
        assert _srt_timestamp_to_seconds("00:00:00,000") == 0.0

    def test_one_hour(self):
        assert _srt_timestamp_to_seconds("01:00:00,000") == 3600.0

    def test_mixed(self):
        assert _srt_timestamp_to_seconds("00:01:30,500") == pytest.approx(90.5)

    def test_milliseconds(self):
        assert _srt_timestamp_to_seconds("00:00:01,250") == pytest.approx(1.25)


# ---------------------------------------------------------------------------
# _parse_srt_blocks
# ---------------------------------------------------------------------------

class TestParseSrtBlocks:

    def test_returns_correct_number_of_blocks(self, srt_file):
        blocks = _parse_srt_blocks(srt_file)
        assert len(blocks) == 4

    def test_first_block_timestamps(self, srt_file):
        blocks = _parse_srt_blocks(srt_file)
        assert blocks[0]["start_secs"] == pytest.approx(1.0)
        assert blocks[0]["end_secs"] == pytest.approx(5.0)

    def test_block_text_content(self, srt_file):
        blocks = _parse_srt_blocks(srt_file)
        assert "presidente" in blocks[0]["text"].lower()

    def test_missing_file_returns_empty_list(self):
        blocks = _parse_srt_blocks("/nonexistent/file.srt")
        assert blocks == []


# ---------------------------------------------------------------------------
# _find_phrase_in_blocks
# ---------------------------------------------------------------------------

class TestFindPhraseInBlocks:

    @pytest.fixture
    def blocks(self, srt_file):
        return _parse_srt_blocks(srt_file)

    def test_exact_phrase_found(self, blocks):
        result = _find_phrase_in_blocks(blocks, "El presidente compareció ante el Congreso")
        assert result is not None
        assert result["start_secs"] == pytest.approx(1.0)

    def test_partial_phrase_found_with_4_word_fallback(self, blocks):
        result = _find_phrase_in_blocks(blocks, "La oposición criticó la")
        assert result is not None
        assert result["start_secs"] == pytest.approx(60.0)

    def test_phrase_not_in_any_block_returns_none(self, blocks):
        result = _find_phrase_in_blocks(blocks, "esto no existe en el srt de ninguna manera")
        assert result is None

    def test_case_insensitive_match(self, blocks):
        result = _find_phrase_in_blocks(blocks, "EL PRESIDENTE COMPARECIÓ")
        assert result is not None

    def test_single_word_phrase_returns_none(self, blocks):
        result = _find_phrase_in_blocks(blocks, "presidente")
        assert result is None


# ---------------------------------------------------------------------------
# find_srt_for_chapter
# ---------------------------------------------------------------------------

class TestFindSrtForChapter:

    def test_file_at_first_candidate_path_returns_that_path(self, mocker):
        expected_path = "/data/congress_videos/vid123/srt_files/vid123.srt"

        def _exists(p):
            return p == expected_path

        mocker.patch("os.path.exists", side_effect=_exists)
        mocker.patch("os.path.isdir", return_value=False)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", "/data/congress_videos")

        result = find_srt_for_chapter("vid123", 42, session_date="2025-10-08")

        assert result == expected_path

    def test_no_file_at_any_candidate_returns_none(self, mocker):
        mocker.patch("os.path.exists", return_value=False)
        mocker.patch("os.path.isdir", return_value=False)

        result = find_srt_for_chapter("vid999", 1, session_date="2025-01-01")

        assert result is None

    def test_file_at_second_candidate_returns_that_path(self, mocker):
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
        date_folder = tmp_path / "2025-10-08"
        srt_dir = date_folder / "vid123" / "srt_files"
        srt_dir.mkdir(parents=True)
        srt_file = srt_dir / "vid123.srt"
        srt_file.write_text("1\n00:00:01,000 --> 00:00:02,000\nHello\n", encoding="utf-8")

        mocker.patch("congress_videos.srt_helpers.DOWNLOADS_DIR", str(tmp_path))
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", str(tmp_path / "_no_project"))

        result = find_srt_for_chapter("vid123", 42, session_date=None)

        assert result == str(srt_file)


# ---------------------------------------------------------------------------
# select_pretrim_window
# ---------------------------------------------------------------------------

class TestSelectPretrimWindow:

    def _patch_ai(self, mocker, start_phrase: str, end_phrase: str):
        return mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={
                "error": None,
                "data": {"start_phrase": start_phrase, "end_phrase": end_phrase},
            },
        )

    def test_valid_phrases_resolved_to_seconds_from_srt(self, mocker, srt_file):
        self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is not None
        assert result["start_seconds"] == pytest.approx(1.0)   # start of block 1
        assert result["end_seconds"] == pytest.approx(85.0)    # end of block 4

    def test_seconds_come_from_srt_not_from_ai(self, mocker, srt_file):
        mock_ai = self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert mock_ai.call_count == 1
        ai_response = mock_ai.return_value["data"]
        assert "start_seconds" not in ai_response
        assert "end_seconds" not in ai_response
        assert result is not None

    def test_phrase_not_found_in_srt_returns_none(self, mocker, srt_file):
        self._patch_ai(
            mocker,
            start_phrase="frase inventada que no existe en el srt",
            end_phrase="quedan a disposición de sus señorías",
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_ai_call_error_returns_none(self, mocker, srt_file):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": "API timeout", "data": None},
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_ai_returns_empty_phrases_returns_none(self, mocker, srt_file):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"start_phrase": "", "end_phrase": ""}},
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_ai_returns_wrong_keys_returns_none(self, mocker, srt_file):
        mocker.patch(
            "congress_videos.srt_helpers.generate_json_completion",
            return_value={"error": None, "data": {"wrong_key": "value"}},
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_end_before_start_returns_none(self, mocker, srt_file):
        self._patch_ai(
            mocker,
            start_phrase="quedan a disposición de sus señorías",
            end_phrase="El presidente compareció ante el Congreso",
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_empty_srt_file_returns_none(self, mocker, tmp_path):
        empty_srt = tmp_path / "empty.srt"
        empty_srt.write_text("", encoding="utf-8")

        result = select_pretrim_window(str(empty_srt), target_secs=360)

        assert result is None

    def test_uses_gpt4o_mini_model(self, mocker, srt_file):
        mock_ai = self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )

        select_pretrim_window(srt_file, target_secs=360)

        _, kwargs = mock_ai.call_args
        assert kwargs.get("model") == "gpt-4o-mini"


# ---------------------------------------------------------------------------
# parse_srt_to_text
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
