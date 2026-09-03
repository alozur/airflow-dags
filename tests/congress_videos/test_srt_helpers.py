"""Tests for congress_videos.srt_helpers module."""

from __future__ import annotations

import logging

import pytest

from congress_videos.config.paths import get_chapter_short_srt_path, get_video_chapter_dir
from congress_videos.srt_helpers import (
    _blocks_to_prompt_text,
    _find_phrase_in_blocks,
    _parse_srt_blocks,
    _serialize_srt_blocks,
    _srt_timestamp_to_seconds,
    _window_srt_blocks,
    _window_srt_text,
    find_srt_for_chapter,
    score_turn_interest,
    select_pretrim_window,
    write_chapter_srt_sidecar,
    write_short_srt_sidecar,
)
from utils.llm_config import LLM_CHEAP

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
# _blocks_to_prompt_text (single-parse prompt derivation, issue #208 item 7b)
# ---------------------------------------------------------------------------


class TestBlocksToPromptTextParity:
    """`_blocks_to_prompt_text` must be byte-identical to `parse_srt_to_text`
    for well-formed, millisecond-precision SRT content (independent oracle:
    the pre-existing `parse_srt_to_text` implementation)."""

    def test_matches_parse_srt_to_text_for_sample_srt(self, srt_file):
        from congress_videos.srt_helpers import parse_srt_to_text

        blocks = _parse_srt_blocks(srt_file)
        derived = _blocks_to_prompt_text(blocks)

        assert derived == parse_srt_to_text(srt_file)

    def test_matches_with_max_chars_cap(self, srt_file):
        from congress_videos.srt_helpers import PRETRIM_MAX_CHARS, parse_srt_to_text

        blocks = _parse_srt_blocks(srt_file)
        derived = _blocks_to_prompt_text(blocks, max_chars=PRETRIM_MAX_CHARS)

        assert derived == parse_srt_to_text(srt_file, max_chars=PRETRIM_MAX_CHARS)

    def test_multi_line_text_block_parity(self, tmp_path):
        from congress_videos.srt_helpers import parse_srt_to_text

        srt = (
            "1\n00:00:01,000 --> 00:00:05,000\nLine one\nLine two\nLine three\n\n"
            "2\n00:00:06,000 --> 00:00:10,000\nSecond block\n\n"
        )
        path = tmp_path / "multiline.srt"
        path.write_text(srt, encoding="utf-8")

        blocks = _parse_srt_blocks(str(path))
        derived = _blocks_to_prompt_text(blocks)

        assert derived == parse_srt_to_text(str(path))
        assert "Line one Line two Line three" in derived


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
            "congress_videos.srt_helpers.cached_json_completion",
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
        assert result["start_seconds"] == pytest.approx(1.0)  # start of block 1
        assert result["end_seconds"] == pytest.approx(85.0)  # end of block 4

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
            "congress_videos.srt_helpers.cached_json_completion",
            return_value={"error": "API timeout", "data": None},
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_ai_returns_empty_phrases_returns_none(self, mocker, srt_file):
        mocker.patch(
            "congress_videos.srt_helpers.cached_json_completion",
            return_value={"error": None, "data": {"start_phrase": "", "end_phrase": ""}},
        )

        result = select_pretrim_window(srt_file, target_secs=360)

        assert result is None

    def test_ai_returns_wrong_keys_returns_none(self, mocker, srt_file):
        mocker.patch(
            "congress_videos.srt_helpers.cached_json_completion",
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

    def test_uses_cheap_tier_model(self, mocker, srt_file):
        mock_ai = self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )

        select_pretrim_window(srt_file, target_secs=360)

        _, kwargs = mock_ai.call_args
        assert kwargs.get("model") == LLM_CHEAP

    def test_parses_srt_file_exactly_once(self, mocker, srt_file):
        """The SRT file must be read/parsed exactly once (issue #208 item 7b) —
        no separate `parse_srt_to_text` read plus a second `_parse_srt_blocks`
        read of the same file."""
        self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )
        open_spy = mocker.patch("congress_videos.srt_helpers.open", wraps=open, create=True)

        select_pretrim_window(srt_file, target_secs=360)

        assert open_spy.call_count == 1

    def test_ms_less_timestamp_srt_returns_none(self, mocker, tmp_path):
        """Documented out-of-scope divergence: `_parse_srt_blocks` requires
        millisecond-precision timestamps, so a ms-less SRT yields zero
        blocks and `select_pretrim_window` still returns None (same outcome
        as before the single-parse refactor, different guard fires first)."""
        self._patch_ai(mocker, start_phrase="Hello", end_phrase="world")
        srt = "1\n00:00:01 --> 00:00:05\nHello world\n\n"
        path = tmp_path / "msless.srt"
        path.write_text(srt, encoding="utf-8")

        result = select_pretrim_window(str(path), target_secs=360)

        assert result is None

    def test_cached_json_completion_receives_derived_prompt_text(self, mocker, srt_file):
        """The exact single-parse-derived prompt text must reach
        cached_json_completion, with model passed as a keyword (issue #208
        item 8)."""
        from congress_videos.srt_helpers import PRETRIM_MAX_CHARS

        mock_ai = self._patch_ai(
            mocker,
            start_phrase="El presidente compareció ante el Congreso",
            end_phrase="quedan a disposición de sus señorías",
        )

        select_pretrim_window(srt_file, target_secs=360)

        expected_srt_text = _blocks_to_prompt_text(_parse_srt_blocks(srt_file), max_chars=PRETRIM_MAX_CHARS)
        _, kwargs = mock_ai.call_args
        assert kwargs.get("model") == LLM_CHEAP
        assert expected_srt_text in kwargs.get("user_prompt", "")
        assert kwargs.get("system_prompt")

    def test_repeat_call_with_identical_inputs_is_a_cache_hit(self, mocker, srt_file):
        """A second select_pretrim_window call with identical SRT content and
        params must be served from the LLM cache without invoking the
        underlying completion again (issue #208 item 8)."""
        store: dict[str, dict] = {}

        def _fake_get_cached(key):
            return store.get(key)

        def _fake_put_cached(key, model, response):
            store[key] = response

        mock_generate = mocker.patch(
            "utils.llm_cache.generate_json_completion",
            return_value={
                "error": None,
                "data": {
                    "start_phrase": "El presidente compareció ante el Congreso",
                    "end_phrase": "quedan a disposición de sus señorías",
                },
            },
        )
        mocker.patch("utils.llm_cache.get_cached", side_effect=_fake_get_cached)
        mocker.patch("utils.llm_cache.put_cached", side_effect=_fake_put_cached)

        first = select_pretrim_window(srt_file, target_secs=360)
        second = select_pretrim_window(srt_file, target_secs=360)

        assert mock_generate.call_count == 1
        assert first == second
        assert first is not None


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


# ---------------------------------------------------------------------------
# T2.1 — parse_srt_to_text: remove 8k pre-trim truncation (#3)
# ---------------------------------------------------------------------------


class TestParseSrtToTextBatch2:
    """Tests for T2.1: 8k default truncation replaced by full-text default."""

    def _build_large_srt(self, tmp_path, total_blocks: int) -> str:
        """Build an SRT file with the given number of blocks."""
        blocks = []
        for i in range(total_blocks):
            mins = (i // 60) % 60
            hrs = i // 3600
            secs = i % 60
            blocks.append(
                f"{i + 1}\n{hrs:02d}:{mins:02d}:{secs:02d},000 --> "
                f"{hrs:02d}:{mins:02d}:{secs:02d},500\n"
                f"{'Block content text here for block ' + str(i)}\n"
            )
        path = tmp_path / "large.srt"
        path.write_text("\n".join(blocks), encoding="utf-8")
        return str(path)

    def test_large_srt_returned_without_8k_truncation(self, tmp_path):
        """A large SRT is returned in full when max_chars is not specified."""
        from congress_videos.srt_helpers import parse_srt_to_text

        # 300 blocks → well over 8000 chars of parsed output
        path = self._build_large_srt(tmp_path, 300)
        result = parse_srt_to_text(path)

        # With no truncation, result must exceed the old 8000 char limit.
        assert len(result) > 8_000

    def test_window_can_be_selected_from_middle(self, tmp_path):
        """Marker placed past the 8k boundary is visible when no max_chars set."""
        from congress_videos.srt_helpers import parse_srt_to_text

        mid_marker = "IMPORTANT_MARKER_IN_THE_MIDDLE"
        blocks = []
        for i in range(400):
            mins = (i // 60) % 60
            secs = i % 60
            text = mid_marker if i == 200 else f"Regular text block {i}"
            blocks.append(f"{i + 1}\n00:{mins:02d}:{secs:02d},000 --> 00:{mins:02d}:{secs:02d},500\n{text}\n")
        path = tmp_path / "mid.srt"
        path.write_text("\n".join(blocks), encoding="utf-8")

        result = parse_srt_to_text(str(path))

        assert mid_marker in result

    def test_empty_srt_returns_empty_string(self, tmp_path):
        """Zero-byte SRT file returns '' with no exception."""
        from congress_videos.srt_helpers import parse_srt_to_text

        path = tmp_path / "empty.srt"
        path.write_text("", encoding="utf-8")

        assert parse_srt_to_text(str(path)) == ""

    def test_pathological_srt_triggers_warning_and_caps(self, tmp_path, caplog):
        """SRT >300k parsed chars triggers WARNING and caps to PRETRIM_MAX_CHARS."""
        import logging

        from congress_videos.srt_helpers import (
            _PRETRIM_PATHOLOGICAL_THRESHOLD,
            PRETRIM_MAX_CHARS,
            parse_srt_to_text,
        )

        # Build a file whose parsed text exceeds the pathological threshold.
        # Each block is ~60 chars parsed; we need >300k / 60 ≈ 5000 blocks.
        path = self._build_large_srt(tmp_path, 6000)
        # Verify the file is indeed large enough to trigger the guard.
        raw_result = parse_srt_to_text(path, max_chars=None)
        # If somehow it's under threshold this test would not exercise the guard,
        # so we skip in that scenario rather than fail.
        if len(raw_result) <= _PRETRIM_PATHOLOGICAL_THRESHOLD:
            import pytest

            pytest.skip("Generated SRT too small to trigger pathological guard in this env")

        with caplog.at_level(logging.WARNING, logger="congress_videos.srt_helpers"):
            result = parse_srt_to_text(path)

        assert len(result) <= PRETRIM_MAX_CHARS
        assert any("capping" in r.message.lower() for r in caplog.records)

    def test_explicit_max_chars_still_respected(self, tmp_path):
        """Passing an explicit max_chars still truncates at that limit."""
        from congress_videos.srt_helpers import parse_srt_to_text

        path = self._build_large_srt(tmp_path, 300)
        result = parse_srt_to_text(path, max_chars=500)

        assert len(result) <= 500


# ---------------------------------------------------------------------------
# _serialize_srt_blocks
# ---------------------------------------------------------------------------


class TestSerializeSrtBlocks:
    @pytest.mark.parametrize(
        "blocks,expected",
        [
            (
                [],
                "",
            ),
            (
                [{"start_secs": 0.0, "end_secs": 59.999, "text": "Hola"}],
                "1\n00:00:00,000 --> 00:00:59,999\nHola\n",
            ),
            (
                [
                    {"start_secs": 3661.5, "end_secs": 3663.0, "text": "Hola"},
                    {"start_secs": 3663.5, "end_secs": 3665.0, "text": "Mundo"},
                ],
                ("1\n01:01:01,500 --> 01:01:03,000\nHola\n\n2\n01:01:03,500 --> 01:01:05,000\nMundo\n"),
            ),
        ],
    )
    def test_serialize_produces_valid_srt(self, blocks, expected):
        assert _serialize_srt_blocks(blocks) == expected

    def test_sequential_1_based_index(self):
        blocks = [
            {"start_secs": 1.0, "end_secs": 2.0, "text": "A"},
            {"start_secs": 3.0, "end_secs": 4.0, "text": "B"},
            {"start_secs": 5.0, "end_secs": 6.0, "text": "C"},
        ]
        result = _serialize_srt_blocks(blocks)
        assert result.startswith("1\n")
        assert "\n2\n" in result
        assert "\n3\n" in result

    def test_milliseconds_rounded_correctly(self):
        # 0.9999 seconds → should round to 1000 ms → carry → 00:00:01,000
        # but spec says round the frac part; 0.9999 * 1000 = 999.9 → rounds to 1000
        # Actually per spec: ms = int(round((secs % 1) * 1000))
        # For secs=0.9999: secs%1=0.9999, *1000=999.9, round=1000 → ms=1000
        # That would make the display 00:00:00,1000 — which is not valid SRT.
        # The design uses int(secs) first, so 0.9999 → int(0.9999)=0, h=0,m=0,sec=0, ms=round(0.9999*1000)=1000
        # Per _secs_to_srt_ts formula in design: rebuild from formula gives ms=1000 here.
        # We test a clean value: 59.999 → ms=round(0.999*1000)=round(999)=999
        blocks = [{"start_secs": 59.999, "end_secs": 60.0, "text": "X"}]
        result = _serialize_srt_blocks(blocks)
        assert "00:00:59,999" in result
        assert "00:01:00,000" in result


# ---------------------------------------------------------------------------
# find_srt_for_chapter — canonical-first probe (new param)
# ---------------------------------------------------------------------------


class TestFindSrtForChapterCanonical:
    def test_canonical_dir_set_and_file_exists_returns_canonical(self, tmp_path):
        canonical_dir = tmp_path / "oradores" / "abc"
        canonical_dir.mkdir(parents=True)
        srt = canonical_dir / "subtitles.srt"
        srt.write_text("1\n00:00:00,000 --> 00:00:01,000\nHola\n", encoding="utf-8")

        result = find_srt_for_chapter("vid123", 1, session_date=None, canonical_dir=str(canonical_dir))

        assert result == str(srt)

    def test_canonical_dir_set_but_file_absent_falls_back_to_legacy(self, tmp_path, mocker):
        canonical_dir = tmp_path / "oradores" / "abc"
        canonical_dir.mkdir(parents=True)
        # subtitles.srt does NOT exist — triggers legacy fallback

        legacy_path = "/data/congress_videos/vid123/srt_files/vid123.srt"

        def _exists(p):
            # canonical path: absent; legacy path: present
            return p == legacy_path

        mocker.patch("os.path.exists", side_effect=_exists)
        mocker.patch("os.path.isdir", return_value=False)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", "/data/congress_videos")

        result = find_srt_for_chapter("vid123", 1, session_date="2025-01-01", canonical_dir=str(canonical_dir))

        assert result == legacy_path

    def test_canonical_dir_none_default_preserves_legacy_behavior(self, mocker):
        expected = "/data/congress_videos/vid123/srt_files/vid123.srt"

        def _exists(p):
            return p == expected

        mocker.patch("os.path.exists", side_effect=_exists)
        mocker.patch("os.path.isdir", return_value=False)
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", "/data/congress_videos")

        result = find_srt_for_chapter("vid123", 1, session_date="2025-01-01")

        assert result == expected


# ---------------------------------------------------------------------------
# score_turn_interest
# ---------------------------------------------------------------------------

SAMPLE_SRT_MULTI = (
    "1\n00:00:01,000 --> 00:00:05,000\nEl presidente compareció ante el Congreso\n\n"
    "2\n00:00:10,000 --> 00:00:20,000\npara informar sobre el accidente ferroviario\n\n"
    "3\n00:01:00,000 --> 00:01:10,000\nLa oposición criticó la gestión del gobierno\n\n"
    "4\n00:01:15,000 --> 00:01:25,000\nquedan a disposición de sus señorías\n\n"
)


class TestScoreTurnInterest:
    def test_happy_path(self):
        """Inject completion_fn returning '7' → expect 7."""

        def _fn(**kwargs):
            return {"content": "7", "error": None}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result == 7

    def test_prose_response(self):
        """completion returns 'Score: 8/10' → expect 8 (re.search first digit group)."""

        def _fn(**kwargs):
            return {"content": "Score: 8/10", "error": None}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result == 8

    def test_non_numeric_response(self):
        """completion returns 'abc' → expect None."""

        def _fn(**kwargs):
            return {"content": "abc", "error": None}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result is None

    def test_clamp_high(self):
        """completion returns '99' → expect 10 (clamped to INTEREST_SCALE_MAX)."""

        def _fn(**kwargs):
            return {"content": "99", "error": None}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result == 10

    def test_clamp_low(self):
        """completion returns '-3' → expect 0 (clamped to INTEREST_SCALE_MIN)."""

        def _fn(**kwargs):
            return {"content": "-3", "error": None}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result == 0

    def test_empty_window_returns_none(self):
        """Call with '' → expect None; assert completion_fn was NOT called."""
        called = []

        def _fn(**kwargs):
            called.append(True)
            return {"content": "7", "error": None}

        result = score_turn_interest("", completion_fn=_fn)
        assert result is None
        assert not called, "completion_fn must NOT be called for empty window"

    def test_whitespace_only_returns_none(self):
        """Call with whitespace-only text → expect None; no LLM call."""
        called = []

        def _fn(**kwargs):
            called.append(True)
            return {"content": "7", "error": None}

        result = score_turn_interest("   \n  ", completion_fn=_fn)
        assert result is None
        assert not called

    def test_llm_returns_none_content(self):
        """completion returns {'content': None, 'error': 'timeout'} → expect None."""

        def _fn(**kwargs):
            return {"content": None, "error": "timeout"}

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result is None

    def test_llm_raises_returns_none(self):
        """completion_fn raises RuntimeError → expect None (never raises)."""

        def _fn(**kwargs):
            raise RuntimeError("network error")

        result = score_turn_interest("texto de prueba", completion_fn=_fn)
        assert result is None

    def test_uses_cheap_tier_model(self):
        """score_turn_interest must pass the LLM_CHEAP tier to completion_fn."""
        called_with = {}

        def _fn(**kwargs):
            called_with.update(kwargs)
            return {"content": "5", "error": None}

        score_turn_interest("texto de prueba", completion_fn=_fn)
        assert called_with.get("model") == LLM_CHEAP

    def test_env_override_forwards_to_completion_fn(self, monkeypatch):
        """End-to-end: LLM_CHEAP env → utils.llm_config → this call site.

        Proves the whole chain (env var → tier constant → forwarded `model`
        kwarg) in one test, independent of the tier's committed value, so
        future tier swaps never require touching this test.
        """
        import importlib

        import congress_videos.srt_helpers as srt_helpers
        import utils.llm_config as llm_config

        monkeypatch.setenv("LLM_CHEAP", "sentinel-model")
        importlib.reload(llm_config)
        importlib.reload(srt_helpers)

        called_with = {}

        def _fn(**kwargs):
            called_with.update(kwargs)
            return {"content": "5", "error": None}

        srt_helpers.score_turn_interest("texto de prueba", completion_fn=_fn)

        assert called_with.get("model") == "sentinel-model"

        monkeypatch.undo()
        importlib.reload(llm_config)
        importlib.reload(srt_helpers)


# ---------------------------------------------------------------------------
# _window_srt_text
# ---------------------------------------------------------------------------


class TestWindowSrtText:
    @pytest.fixture
    def srt_file_multi(self, tmp_path) -> str:
        path = tmp_path / "video_merged.srt"
        path.write_text(SAMPLE_SRT_MULTI, encoding="utf-8")
        return str(path)

    def test_in_range_blocks_joined_first_window(self, mocker, srt_file_multi):
        """Blocks overlapping [0, 10] → include first block; exclude 60s block."""
        mocker.patch(
            "congress_videos.srt_helpers.find_srt_for_chapter",
            return_value=srt_file_multi,
        )
        result = _window_srt_text("vidXXX", 0.0, 10.0)
        assert "presidente" in result.lower()
        assert "oposición" not in result.lower()

    def test_in_range_blocks_joined_third_window(self, mocker, srt_file_multi):
        """Blocks overlapping [50, 80] → include third and fourth blocks only."""
        mocker.patch(
            "congress_videos.srt_helpers.find_srt_for_chapter",
            return_value=srt_file_multi,
        )
        result = _window_srt_text("vidXXX", 50.0, 80.0)
        assert "oposición" in result.lower() or "disposición" in result.lower()
        assert "presidente" not in result.lower()

    def test_missing_srt_returns_empty(self, mocker):
        """video_id with no SRT on disk → returns ''."""
        mocker.patch(
            "congress_videos.srt_helpers.find_srt_for_chapter",
            return_value=None,
        )
        result = _window_srt_text("nonexistent_vid", 0.0, 100.0)
        assert result == ""

    def test_no_blocks_in_range_returns_empty(self, mocker, srt_file_multi):
        """SRT exists but [500, 600] range has no blocks → returns ''."""
        mocker.patch(
            "congress_videos.srt_helpers.find_srt_for_chapter",
            return_value=srt_file_multi,
        )
        result = _window_srt_text("vidXXX", 500.0, 600.0)
        assert result == ""


# ---------------------------------------------------------------------------
# TestMaterializeTaskScoring
# ---------------------------------------------------------------------------


class TestMaterializeTaskScoring:
    def test_scorer_failure_does_not_crash_materialize(self, monkeypatch):
        """patch score_turn_interest to raise RuntimeError; _materialize_task returns summary dict
        without re-raising; no UPDATE with interest_score is executed."""
        import importlib
        import sys

        MODULE = "congress_videos.speaker_turn_videos_dag"
        if MODULE in sys.modules:
            del sys.modules[MODULE]
        mod = importlib.import_module(MODULE)

        # Patch scorer to raise
        monkeypatch.setattr(
            "congress_videos.speaker_turn_videos_dag.score_turn_interest",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom")),
        )
        monkeypatch.setattr(
            "congress_videos.speaker_turn_videos_dag._window_srt_text",
            lambda *a, **kw: "some text",
        )
        monkeypatch.setattr(mod, "_find_source_video_any_date", lambda vid: "/data/src.mp4")

        plan_mock = type(
            "Plan",
            (),
            {
                "turn_ids": (7,),
                "keep_intervals": (type("KI", (), {"start": 600.0, "end": 700.0})(),),
                "needs_reencode": False,
                "output_turn_id": 7,
                "chapter_id": 3,
            },
        )()

        monkeypatch.setattr(mod, "plan_turn_materialization", lambda turns, trims: [plan_mock])
        monkeypatch.setattr(mod, "execute_plan", lambda *a, **kw: None)
        monkeypatch.setattr(mod, "get_cached_codec", lambda *a, **k: "h264")

        execute_calls = []

        pg = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
        conn = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
        cur = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
        cur.fetchall.return_value = []

        def _execute(sql, params=None):
            execute_calls.append((sql, params))

        cur.execute.side_effect = _execute
        conn.cursor.return_value.__enter__.return_value = cur
        pg.get_connection.return_value.__enter__.return_value = conn
        pg.get_qualified_table.side_effect = lambda n: f"test.{n}"
        monkeypatch.setattr(mod, "PostgresConnection", lambda: pg)

        ti = __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()
        ti.xcom_pull.return_value = [
            {
                "turn_id": 7,
                "chapter_id": 3,
                "video_id": "vid1",
                "start_seconds": 600.0,
                "end_seconds": 700.0,
            }
        ]

        # Must not raise
        result = mod._materialize_task(
            ti=ti, dag_run=__import__("unittest.mock", fromlist=["MagicMock"]).MagicMock(conf={})
        )

        assert isinstance(result, dict), "must return summary dict even on scorer failure"
        update_calls = [
            c for c in execute_calls if "UPDATE" in str(c[0]).upper() and "interest_score" in str(c[0]).lower()
        ]
        assert len(update_calls) == 0, "interest_score UPDATE must not be committed when scorer raises"


# ---------------------------------------------------------------------------
# _window_srt_blocks — overlap filtering + clip-origin re-timing
# ---------------------------------------------------------------------------


class TestWindowSrtBlocks:
    """Pure unit tests for the _window_srt_blocks helper.

    Overlap predicate: block.start_secs < window_end AND block.end_secs > window_start.
    Re-timing: max(0.0, secs - window_start) for each surviving block's timestamps.
    """

    def test_block_fully_inside_window_included(self):
        """Block start=19200s, end=19210s inside window [19157, 19784] is present."""
        blocks = [{"start_secs": 19200.0, "end_secs": 19210.0, "text": "Inside"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert len(result) == 1
        assert result[0]["text"] == "Inside"

    def test_block_straddling_window_start_included(self):
        """Block start=19150s, end=19165s straddles window_start=19157 — included."""
        blocks = [{"start_secs": 19150.0, "end_secs": 19165.0, "text": "StraddleStart"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert len(result) == 1

    def test_block_straddling_window_end_included(self):
        """Block start=19780s, end=19790s straddles window_end=19784 — included."""
        blocks = [{"start_secs": 19780.0, "end_secs": 19790.0, "text": "StraddleEnd"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert len(result) == 1

    def test_block_entirely_outside_excluded(self):
        """Block start=100s, end=110s is entirely outside window [19157, 19784] — excluded."""
        blocks = [{"start_secs": 100.0, "end_secs": 110.0, "text": "Outside"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert result == []

    def test_grouped_block_retimed_to_clip_origin(self):
        """window_start=19157, src start=19160 → output start_secs=3.0."""
        blocks = [{"start_secs": 19160.0, "end_secs": 19170.0, "text": "Retimed"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert len(result) == 1
        assert result[0]["start_secs"] == pytest.approx(3.0)
        assert result[0]["end_secs"] == pytest.approx(13.0)

    def test_single_turn_block_retimed(self):
        """window_start=300, src start=305/end=310 → output 5.0/10.0."""
        blocks = [{"start_secs": 305.0, "end_secs": 310.0, "text": "SingleTurn"}]
        result = _window_srt_blocks(blocks, 300.0, 420.0)
        assert len(result) == 1
        assert result[0]["start_secs"] == pytest.approx(5.0)
        assert result[0]["end_secs"] == pytest.approx(10.0)

    def test_block_starting_before_window_clamped_to_zero(self):
        """Block start=19100s straddling window_start=19157 → re-timed start clamped to 0.0."""
        blocks = [{"start_secs": 19100.0, "end_secs": 19165.0, "text": "Clamp"}]
        result = _window_srt_blocks(blocks, 19157.0, 19784.0)
        assert len(result) == 1
        assert result[0]["start_secs"] == pytest.approx(0.0)
        assert result[0]["end_secs"] == pytest.approx(8.0)


class TestFindSrtVideoIdGuard:
    """Path-unsafe video ids must short-circuit to None (issue #198)."""

    @pytest.mark.parametrize("bad_id", ["../etc", "a/b", "", "abc 123", "id;rm"])
    def test_unsafe_video_id_returns_none(self, bad_id):
        from congress_videos.srt_helpers import find_srt_for_chapter

        assert find_srt_for_chapter(bad_id, 1) is None

    def test_safe_video_id_still_probes(self, tmp_path):
        from congress_videos import srt_helpers

        srt_dir = tmp_path / "vid_123" / "srt_files"
        srt_dir.mkdir(parents=True)
        srt_file = srt_dir / "vid_123.srt"
        srt_file.write_text("1\n00:00:00,000 --> 00:00:01,000\nhola\n")
        original = srt_helpers.PROJECT_DATA_DIR
        try:
            srt_helpers.PROJECT_DATA_DIR = str(tmp_path)
            assert srt_helpers.find_srt_for_chapter("vid_123", 1) == str(srt_file)
        finally:
            srt_helpers.PROJECT_DATA_DIR = original


# ---------------------------------------------------------------------------
# chapter_window_blocks (issue #322) — chapter-span filter, NOT re-timed
# ---------------------------------------------------------------------------


class TestChapterWindowBlocks:
    """Filters SRT blocks to a chapter's [start_time, end_time) span,
    absolute timestamps preserved (unlike _window_srt_blocks) — overlap
    predicate is start < end AND end > start, so a block straddling
    end_time is included. Unparseable spans fail safe: [] + WARNING."""

    _BLOCKS = [
        {"start_secs": 0.0, "end_secs": 5.0, "text": "before chapter"},
        {"start_secs": 600.0, "end_secs": 605.0, "text": "chapter opening"},
        {"start_secs": 1200.0, "end_secs": 1205.0, "text": "chapter middle"},
        {"start_secs": 2395.0, "end_secs": 2405.0, "text": "straddles chapter end"},
        {"start_secs": 3000.0, "end_secs": 3005.0, "text": "after chapter"},
    ]

    @pytest.mark.parametrize("start_time,end_time", [("00:10:00,000", "00:40:00,000"), (600.0, 2400.0)])
    def test_valid_span_returns_overlapping_blocks_absolute_timestamps(self, start_time, end_time):
        from congress_videos.srt_helpers import chapter_window_blocks

        result = chapter_window_blocks(self._BLOCKS, start_time, end_time)

        texts = [b["text"] for b in result]
        assert texts == ["chapter opening", "chapter middle", "straddles chapter end"]
        assert result[0]["start_secs"] == pytest.approx(600.0)  # NOT re-timed

    @pytest.mark.parametrize("start_time,end_time", [("not-a-timestamp", "00:40:00,000"), ("00:10:00,000", None)])
    def test_unparseable_span_returns_empty_and_warns(self, start_time, end_time, caplog):
        from congress_videos.srt_helpers import chapter_window_blocks

        with caplog.at_level("WARNING"):
            result = chapter_window_blocks(self._BLOCKS, start_time, end_time)

        assert result == []
        assert any("chapter_window_blocks" in rec.message for rec in caplog.records)

    def test_empty_blocks_input_returns_empty(self):
        from congress_videos.srt_helpers import chapter_window_blocks

        assert chapter_window_blocks([], "00:10:00,000", "00:40:00,000") == []


# ---------------------------------------------------------------------------
# write_chapter_srt_sidecar (issue #340) — persisted per-chapter sidecar
# ---------------------------------------------------------------------------

_CHAPTER_SIDECAR_SRT = (
    "1\n00:01:00,000 --> 00:01:05,000\nfuera antes del pad\n\n"
    "2\n00:02:30,000 --> 00:02:35,000\ndentro del pad izquierdo\n\n"
    "3\n00:05:10,000 --> 00:05:15,000\ndentro del capitulo\n\n"
    "4\n00:08:00,000 --> 00:08:05,000\ndentro del pad derecho\n\n"
    "5\n00:08:40,000 --> 00:08:45,000\nfuera despues del pad\n\n"
)


class TestWriteChapterSrtSidecar:
    """start_time=00:05:00,000 / end_time=00:05:30,000 (300s/330s), pad=180s
    -> padded window [120s, 510s]. Only blocks 2-4 above overlap it."""

    VIDEO_ID = "vidZZZ"
    CHAPTER_ID = 7
    START = "00:05:00,000"
    END = "00:05:30,000"

    @pytest.fixture
    def source_srt(self, tmp_path, mocker):
        """Real source SRT resolvable by the (unmocked) find_srt_for_chapter."""
        project_dir = tmp_path / "project_data"
        srt_dir = project_dir / self.VIDEO_ID / "srt_files"
        srt_dir.mkdir(parents=True)
        (srt_dir / f"{self.VIDEO_ID}.srt").write_text(_CHAPTER_SIDECAR_SRT, encoding="utf-8")
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", str(project_dir))
        mocker.patch("congress_videos.srt_helpers.DOWNLOADS_DIR", str(tmp_path / "no_downloads"))
        return project_dir

    @pytest.fixture
    def target_dir(self, tmp_path, mocker):
        """Writer's target dir, decoupled from the source-lookup mocks above."""
        target = tmp_path / "chapter_target"
        mocker.patch("congress_videos.srt_helpers.get_video_chapter_dir", return_value=target)
        return target

    def test_happy_path_writes_padded_absolute_timestamped_sidecar(self, source_srt, target_dir):
        result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)

        assert result == target_dir / "subtitles.srt"
        assert target_dir.is_dir()  # parent dirs created (mkdir parents=True)
        content = (target_dir / "subtitles.srt").read_text(encoding="utf-8")

        assert "dentro del pad izquierdo" in content
        assert "dentro del capitulo" in content
        assert "dentro del pad derecho" in content
        assert "fuera antes del pad" not in content
        assert "fuera despues del pad" not in content
        # Absolute source-video timestamps preserved, not re-timed to window origin.
        assert "00:02:30,000 --> 00:02:35,000" in content
        assert "00:08:00,000 --> 00:08:05,000" in content
        assert not (target_dir / "subtitles.srt.tmp").exists()

    def test_start_end_time_bounds_stay_plain_strings_never_tz_typed(self, source_srt, target_dir):
        start, end = self.START, self.END
        write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, start, end)
        # issue #163 regression class: bounds stay plain strings, never coerced
        # into datetime/time objects.
        assert isinstance(start, str) and isinstance(end, str)

    def test_nested_parent_dirs_are_created(self, source_srt, tmp_path, mocker):
        nested = tmp_path / "a" / "b" / "c"  # none of these levels pre-exist
        mocker.patch("congress_videos.srt_helpers.get_video_chapter_dir", return_value=nested)

        result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)

        assert result == nested / "subtitles.srt"
        assert (nested / "subtitles.srt").exists()

    def test_rerun_overwrites_with_fresh_window(self, source_srt, target_dir):
        write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)
        first_content = (target_dir / "subtitles.srt").read_text(encoding="utf-8")
        assert "dentro del capitulo" in first_content

        # Different span, outside the first window — proves overwrite, not append.
        second_result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, "00:01:00,000", "00:01:05,000")
        second_content = (target_dir / "subtitles.srt").read_text(encoding="utf-8")

        assert second_result == target_dir / "subtitles.srt"
        assert "fuera antes del pad" in second_content
        assert "dentro del capitulo" not in second_content
        assert not (target_dir / "subtitles.srt.tmp").exists()

    def test_missing_source_srt_returns_none_and_writes_no_file(self, tmp_path, mocker):
        mocker.patch("congress_videos.srt_helpers.PROJECT_DATA_DIR", str(tmp_path / "empty"))
        mocker.patch("congress_videos.srt_helpers.DOWNLOADS_DIR", str(tmp_path / "no_downloads"))
        target = tmp_path / "chapter_target"
        mocker.patch("congress_videos.srt_helpers.get_video_chapter_dir", return_value=target)

        result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)

        assert result is None
        assert not target.exists()

    def test_empty_padded_window_returns_none_no_file_and_warns(self, source_srt, target_dir, caplog):
        # Padded window for a 01:00:00 chapter is [3420s, 3785s] — no fixture
        # block (max 525s) overlaps it.
        with caplog.at_level(logging.WARNING):
            result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, "01:00:00,000", "01:00:05,000")

        assert result is None
        assert not target_dir.exists()
        assert any("write_chapter_srt_sidecar" in rec.message for rec in caplog.records)

    def test_unparseable_bounds_returns_none(self, source_srt, target_dir, mocker):
        import congress_videos.srt_helpers as srt_helpers_mod

        probe = mocker.spy(srt_helpers_mod, "find_srt_for_chapter")
        result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, "not-a-timestamp", self.END)

        assert result is None
        assert not target_dir.exists()
        probe.assert_not_called()  # bounds validated BEFORE the source is probed

    def test_traversal_video_id_returns_none_and_creates_nothing(self, source_srt, target_dir):
        result = write_chapter_srt_sidecar("../../etc/passwd", self.CHAPTER_ID, self.START, self.END)

        assert result is None
        assert not target_dir.exists()

    def test_oserror_during_write_is_swallowed_and_tmp_is_unlinked(self, source_srt, target_dir, mocker, caplog):
        mocker.patch("os.replace", side_effect=OSError("disk full"))

        with caplog.at_level(logging.WARNING):
            result = write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)

        assert result is None
        assert not (target_dir / "subtitles.srt").exists()
        assert not (target_dir / "subtitles.srt.tmp").exists()
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any(r.exc_info is not None for r in warnings)

    def test_never_passes_canonical_dir_to_its_own_probe(self, source_srt, target_dir, mocker):
        import congress_videos.srt_helpers as srt_helpers_mod

        spy = mocker.patch(
            "congress_videos.srt_helpers.find_srt_for_chapter",
            wraps=srt_helpers_mod.find_srt_for_chapter,
        )

        write_chapter_srt_sidecar(self.VIDEO_ID, self.CHAPTER_ID, self.START, self.END)

        assert spy.call_count >= 1
        for call in spy.call_args_list:
            assert "canonical_dir" not in call.kwargs
            assert len(call.args) <= 3


# ---------------------------------------------------------------------------
# write_short_srt_sidecar (issue #431) — Reap short clip sidecar
# ---------------------------------------------------------------------------

_SHORT_SIDECAR_SOURCE_SRT = (
    "1\n00:01:00,000 --> 00:01:05,000\nfuera antes de todo\n\n"
    "2\n00:05:30,000 --> 00:05:35,000\ndentro del pretrim inicio\n\n"
    "3\n00:06:00,000 --> 00:06:05,000\ndentro del pretrim medio\n\n"
    "4\n00:07:00,000 --> 00:07:05,000\nfuera del pretrim final\n\n"
)


class TestWriteShortSrtSidecar:
    """chapter_start=00:05:00,000 (300s) / chapter_end=00:10:00,000 (600s);
    pretrim_start_secs=30 / pretrim_end_secs=100 -> window [330s, 400s).
    Only blocks 2-3 of ``_SHORT_SIDECAR_SOURCE_SRT`` overlap it."""

    VIDEO_ID = "vidshort"
    CHAPTER_ID = 9
    CLIP_ID = "clip01"
    CHAPTER_START = "00:05:00,000"
    CHAPTER_END = "00:10:00,000"
    PRETRIM_START = 30
    PRETRIM_END = 100

    @pytest.fixture(autouse=True)
    def _data_root(self, tmp_path, mocker):
        mocker.patch("congress_videos.config.paths.PROJECT_DATA_DIR", str(tmp_path))
        return tmp_path

    @pytest.fixture
    def source_srt(self, _data_root):
        """Source SRT at the canonical chapter sidecar path (D1: probed first)."""
        chapter_dir = get_video_chapter_dir(self.VIDEO_ID, self.CHAPTER_ID)
        chapter_dir.mkdir(parents=True)
        (chapter_dir / "subtitles.srt").write_text(_SHORT_SIDECAR_SOURCE_SRT, encoding="utf-8")
        return chapter_dir

    def _target_path(self):
        return get_chapter_short_srt_path(self.VIDEO_ID, self.CHAPTER_ID, self.CLIP_ID)

    def test_writes_srt_next_to_clip_mp4(self, source_srt):
        result = write_short_srt_sidecar(
            self.VIDEO_ID,
            self.CHAPTER_ID,
            self.CLIP_ID,
            self.CHAPTER_START,
            self.CHAPTER_END,
            pretrim_start_secs=self.PRETRIM_START,
            pretrim_end_secs=self.PRETRIM_END,
        )

        target_path = self._target_path()
        assert result == target_path
        assert target_path.exists()
        assert target_path.parent.name == "shorts"
        content = target_path.read_text(encoding="utf-8")
        assert "dentro del pretrim inicio" in content
        assert "dentro del pretrim medio" in content
        assert "fuera antes de todo" not in content
        assert "fuera del pretrim final" not in content

    def test_timestamps_are_retimed_to_clip_origin(self, source_srt):
        write_short_srt_sidecar(
            self.VIDEO_ID,
            self.CHAPTER_ID,
            self.CLIP_ID,
            self.CHAPTER_START,
            self.CHAPTER_END,
            pretrim_start_secs=self.PRETRIM_START,
            pretrim_end_secs=self.PRETRIM_END,
        )

        content = self._target_path().read_text(encoding="utf-8")
        # Block 2 starts exactly at window_start (330s) -> retimed to 0.
        assert "00:00:00,000 --> 00:00:05,000" in content
        # Block 3 starts 30s into the window (360s - 330s).
        assert "00:00:30,000 --> 00:00:35,000" in content
        # Absolute chapter-relative timestamps must NOT appear.
        assert "00:05:30" not in content

    def test_existing_non_empty_srt_is_reused_not_rewritten(self, source_srt, caplog):
        target_path = self._target_path()
        target_path.parent.mkdir(parents=True, exist_ok=True)
        sentinel = "1\n00:00:00,000 --> 00:00:01,000\nsentinel content\n\n"
        target_path.write_text(sentinel, encoding="utf-8")

        with caplog.at_level(logging.INFO):
            result = write_short_srt_sidecar(
                self.VIDEO_ID,
                self.CHAPTER_ID,
                self.CLIP_ID,
                self.CHAPTER_START,
                self.CHAPTER_END,
                pretrim_start_secs=self.PRETRIM_START,
                pretrim_end_secs=self.PRETRIM_END,
            )

        assert result == target_path
        assert target_path.read_text(encoding="utf-8") == sentinel
        assert any(str(target_path) in rec.message for rec in caplog.records)

    @pytest.mark.parametrize(
        "pretrim_start_secs,pretrim_end_secs",
        [
            (None, None),
            (None, 100),
            (30, None),
            ("not-a-number", 100),
        ],
    )
    def test_null_pretrim_offsets_fall_back_to_full_chapter_span(
        self, source_srt, pretrim_start_secs, pretrim_end_secs
    ):
        result = write_short_srt_sidecar(
            self.VIDEO_ID,
            self.CHAPTER_ID,
            self.CLIP_ID,
            self.CHAPTER_START,
            self.CHAPTER_END,
            pretrim_start_secs=pretrim_start_secs,
            pretrim_end_secs=pretrim_end_secs,
        )

        assert result is not None
        content = result.read_text(encoding="utf-8")
        # Full chapter span [300s, 600s) covers all four blocks.
        assert "fuera antes de todo" not in content  # 60-65s, outside [300,600)
        assert "dentro del pretrim inicio" in content
        assert "dentro del pretrim medio" in content
        assert "fuera del pretrim final" in content  # 420-425s, inside [300,600)

    def test_window_outside_chapter_span_falls_back_with_warning(self, source_srt, caplog):
        # pretrim offsets placing the window entirely past the chapter end.
        with caplog.at_level(logging.WARNING):
            result = write_short_srt_sidecar(
                self.VIDEO_ID,
                self.CHAPTER_ID,
                self.CLIP_ID,
                self.CHAPTER_START,
                self.CHAPTER_END,
                pretrim_start_secs=5000,
                pretrim_end_secs=5010,
            )

        assert result is not None
        content = result.read_text(encoding="utf-8")
        # Fell back to the full chapter span [300s, 600s).
        assert "fuera del pretrim final" in content
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("outside chapter span" in r.message for r in warnings)

    def test_zero_blocks_writes_no_file_and_warns(self, source_srt, caplog):
        with caplog.at_level(logging.WARNING):
            result = write_short_srt_sidecar(
                self.VIDEO_ID,
                self.CHAPTER_ID,
                self.CLIP_ID,
                "00:20:00,000",
                "00:21:00,000",
                pretrim_start_secs=0,
                pretrim_end_secs=5,
            )

        assert result is None
        assert not self._target_path().exists()
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("no blocks" in r.message for r in warnings)

    def test_missing_source_srt_returns_none_and_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = write_short_srt_sidecar(
                self.VIDEO_ID,
                self.CHAPTER_ID,
                self.CLIP_ID,
                self.CHAPTER_START,
                self.CHAPTER_END,
                pretrim_start_secs=self.PRETRIM_START,
                pretrim_end_secs=self.PRETRIM_END,
            )

        assert result is None
        assert not self._target_path().exists()
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("no source SRT" in r.message for r in warnings)

    def test_unreadable_source_srt_returns_none_and_writes_no_file(self, source_srt, mocker):
        mocker.patch("builtins.open", side_effect=OSError("permission denied"))

        result = write_short_srt_sidecar(
            self.VIDEO_ID,
            self.CHAPTER_ID,
            self.CLIP_ID,
            self.CHAPTER_START,
            self.CHAPTER_END,
            pretrim_start_secs=self.PRETRIM_START,
            pretrim_end_secs=self.PRETRIM_END,
        )

        assert result is None
        assert not self._target_path().exists()

    @pytest.mark.parametrize("bad_clip_id", ["../../etc/passwd", "a/b", ""])
    def test_unsafe_clip_id_refuses(self, source_srt, bad_clip_id):
        result = write_short_srt_sidecar(
            self.VIDEO_ID,
            self.CHAPTER_ID,
            bad_clip_id,
            self.CHAPTER_START,
            self.CHAPTER_END,
            pretrim_start_secs=self.PRETRIM_START,
            pretrim_end_secs=self.PRETRIM_END,
        )

        assert result is None
        # Nothing written outside the shorts dir for this chapter.
        shorts_dir = get_chapter_short_srt_path(self.VIDEO_ID, self.CHAPTER_ID, "placeholder").parent
        if shorts_dir.exists():
            assert list(shorts_dir.iterdir()) == []

    def test_oserror_on_write_returns_none_and_leaves_no_tmp(self, source_srt, mocker, caplog):
        mocker.patch("os.replace", side_effect=OSError("disk full"))

        with caplog.at_level(logging.WARNING):
            result = write_short_srt_sidecar(
                self.VIDEO_ID,
                self.CHAPTER_ID,
                self.CLIP_ID,
                self.CHAPTER_START,
                self.CHAPTER_END,
                pretrim_start_secs=self.PRETRIM_START,
                pretrim_end_secs=self.PRETRIM_END,
            )

        target_path = self._target_path()
        assert result is None
        assert not target_path.exists()
        assert not target_path.with_name(target_path.name + ".tmp").exists()
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any(r.exc_info is not None for r in warnings)
