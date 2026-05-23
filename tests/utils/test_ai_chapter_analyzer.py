"""Tests for ai_chapter_analyzer utility functions — TASK-024."""

from __future__ import annotations

import pytest


# --------------------------------------------------------------------------- #
# parse_timestamp_to_seconds
# --------------------------------------------------------------------------- #

class TestParseTimestampToSeconds:

    @pytest.mark.parametrize(
        "timestamp, expected",
        [
            ("00:00:00", 0),
            ("00:01:00", 60),
            ("01:00:00", 3600),
            ("01:30:15", 5415),
            ("00:00:30,500", 30),    # milliseconds stripped
            ("02:00:00,000", 7200),  # milliseconds stripped, full hours
        ],
    )
    def test_parametrize_various_timestamps(self, timestamp: str, expected: int):
        """Correctly converts HH:MM:SS and HH:MM:SS,mmm to total seconds."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        assert parse_timestamp_to_seconds(timestamp) == expected

    def test_returns_zero_for_invalid_format(self):
        """Malformed timestamp returns 0 (fallback behavior)."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        result = parse_timestamp_to_seconds("bad")
        assert result == 0

    def test_handles_leading_whitespace(self):
        """Timestamps with surrounding whitespace are handled."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        assert parse_timestamp_to_seconds("  00:05:30  ") == 330


# --------------------------------------------------------------------------- #
# format_seconds_to_timestamp
# --------------------------------------------------------------------------- #

class TestFormatSecondsToTimestamp:

    @pytest.mark.parametrize(
        "seconds, expected",
        [
            (0, "00:00:00"),
            (60, "00:01:00"),
            (3600, "01:00:00"),
            (5415, "01:30:15"),
            (86399, "23:59:59"),  # just before 24h boundary
        ],
    )
    def test_parametrize_various_seconds(self, seconds: int, expected: str):
        """Correctly formats total seconds to HH:MM:SS."""
        from utils.ai_chapter_analyzer import format_seconds_to_timestamp

        assert format_seconds_to_timestamp(seconds) == expected

    def test_round_trip_parse_format(self):
        """parse_timestamp_to_seconds -> format_seconds_to_timestamp is lossless (no ms)."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds, format_seconds_to_timestamp

        original = "02:45:33"
        seconds = parse_timestamp_to_seconds(original)
        result = format_seconds_to_timestamp(seconds)

        assert result == original

    def test_zero_seconds_is_zero_string(self):
        """Zero seconds returns '00:00:00'."""
        from utils.ai_chapter_analyzer import format_seconds_to_timestamp

        assert format_seconds_to_timestamp(0) == "00:00:00"


# --------------------------------------------------------------------------- #
# SRT helper
# --------------------------------------------------------------------------- #

def _make_srt(entries: list[tuple[str, str, str]]) -> str:
    """
    Build SRT content from (start, end, text) tuples.
    Uses HH:MM:SS format (no milliseconds) for simplicity.
    """
    lines = []
    for i, (start, end, text) in enumerate(entries, start=1):
        lines.append(str(i))
        lines.append(f"{start} --> {end}")
        lines.append(text)
        lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# detect_silence_gaps
# --------------------------------------------------------------------------- #

class TestDetectSilenceGaps:

    def test_empty_srt_returns_empty_list(self):
        """Empty SRT string produces no gaps."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        result = detect_silence_gaps("")

        assert result == []

    def test_no_gaps_when_silence_below_threshold(self):
        """Gap of 5s with threshold=15s is not reported."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:10", "First subtitle"),
            ("00:00:15", "00:00:25", "Second subtitle"),  # 5s gap
        ])
        result = detect_silence_gaps(srt, min_silence_seconds=15)

        assert result == []

    def test_detects_one_large_gap(self):
        """Gap of 30s with threshold=15s is detected and reported."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:10", "Before gap"),
            ("00:00:40", "00:00:50", "After gap"),  # 30s gap
        ])
        result = detect_silence_gaps(srt, min_silence_seconds=15)

        assert len(result) == 1
        assert result[0]["gap_duration_seconds"] == 30

    def test_gap_contains_required_fields(self):
        """Each gap dict has gap_start, gap_end, gap_duration_seconds fields."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:01:00", "00:01:10", "A"),
            ("00:02:00", "00:02:10", "B"),  # 50s gap
        ])
        result = detect_silence_gaps(srt, min_silence_seconds=10)

        assert len(result) == 1
        gap = result[0]
        assert "gap_start" in gap
        assert "gap_end" in gap
        assert "gap_duration_seconds" in gap
        assert "previous_text" in gap
        assert "next_text" in gap

    def test_custom_threshold_filters_smaller_gaps(self):
        """Custom threshold=60s filters a 30s gap but not a 130s one."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:00:35", "00:00:40", "B"),   # 30s gap — below 60s threshold
            ("00:02:50", "00:02:55", "C"),   # 130s gap — above 60s threshold
        ])
        result = detect_silence_gaps(srt, min_silence_seconds=60)

        assert len(result) == 1
        assert result[0]["gap_duration_seconds"] == 130

    def test_multiple_gaps_detected(self):
        """Two gaps above threshold are both reported."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:00:25", "00:00:30", "B"),   # 20s gap
            ("00:01:00", "00:01:05", "C"),   # 30s gap
        ])
        result = detect_silence_gaps(srt, min_silence_seconds=15)

        assert len(result) == 2


# --------------------------------------------------------------------------- #
# chunk_by_silence
# --------------------------------------------------------------------------- #

class TestChunkBySilence:

    def test_empty_srt_returns_single_default_chunk(self):
        """Empty SRT returns exactly 1 chunk with default timestamps."""
        from utils.ai_chapter_analyzer import chunk_by_silence

        result = chunk_by_silence("")

        assert len(result) == 1
        assert result[0]["chunk_number"] == 1

    def test_no_gaps_returns_single_chunk(self):
        """SRT with no silence gaps returns 1 chunk covering all content."""
        from utils.ai_chapter_analyzer import chunk_by_silence

        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:00:06", "00:00:11", "B"),
            ("00:00:12", "00:00:17", "C"),
        ])
        result = chunk_by_silence(srt, min_silence_seconds=30)

        assert len(result) == 1
        assert result[0]["content"]

    def test_chunk_has_required_fields(self):
        """Each chunk contains chunk_number, start_time, end_time, duration_seconds."""
        from utils.ai_chapter_analyzer import chunk_by_silence

        srt = _make_srt([
            ("00:00:00", "00:05:00", "Beginning"),
            ("00:35:00", "00:40:00", "After big gap"),
        ])
        result = chunk_by_silence(srt, min_silence_seconds=15, min_chunk_duration_minutes=0)

        for chunk in result:
            assert "chunk_number" in chunk
            assert "start_time" in chunk
            assert "end_time" in chunk
            assert "duration_seconds" in chunk
            assert "duration_minutes" in chunk

    def test_chunks_are_numbered_sequentially(self):
        """Chunk numbers start at 1 and increase sequentially."""
        from utils.ai_chapter_analyzer import chunk_by_silence

        # 3 blocks of ~35 minutes separated by 2-minute gaps — exceeds max_chunk_duration_minutes=30
        entries = []
        for block in range(3):
            start_min = block * 37
            end_min = start_min + 35
            entries.append((
                f"00:{start_min:02d}:00",
                f"00:{end_min:02d}:00",
                f"Block {block} content",
            ))

        srt = _make_srt(entries)
        result = chunk_by_silence(
            srt,
            min_silence_seconds=15,
            min_chunk_duration_minutes=0,
            max_chunk_duration_minutes=30,
        )

        for i, chunk in enumerate(result, start=1):
            assert chunk["chunk_number"] == i

    def test_single_chunk_when_no_qualifying_gaps(self):
        """Content without qualifying silence produces a single chunk."""
        from utils.ai_chapter_analyzer import chunk_by_silence

        srt = _make_srt([
            ("00:00:00", "00:10:00", "Part 1"),
            ("00:10:01", "00:20:00", "Part 2"),  # 1s gap — well below 15s
        ])
        result = chunk_by_silence(srt, min_silence_seconds=15)

        assert len(result) == 1


# --------------------------------------------------------------------------- #
# analyze_chapters_with_ai
# --------------------------------------------------------------------------- #

class TestAnalyzeChaptersWithAi:

    def test_analyze_chapters_success(self, mocker):
        """Mock generate_json_completion returning valid chapters → success result."""
        from utils.ai_chapter_analyzer import analyze_chapters_with_ai

        mocker.patch(
            "utils.ai_chapter_analyzer.generate_json_completion",
            return_value={
                "data": {
                    "interesting_chapters": [
                        {
                            "title": "Debate sobre Vivienda",
                            "description": "Discusión sobre la crisis de vivienda.",
                            "start_time": "00:00:00",
                            "end_time": "00:30:00",
                            "duration_minutes": 30.0,
                            "speakers": ["Portavoz PP"],
                            "topics": ["Vivienda", "Alquiler"],
                        },
                        {
                            "title": "Debate sobre Sanidad",
                            "description": "Debate sobre el sistema sanitario.",
                            "start_time": "00:30:00",
                            "end_time": "01:00:00",
                            "duration_minutes": 30.0,
                            "speakers": ["Ministra"],
                            "topics": ["Sanidad"],
                        },
                    ]
                },
                "raw_content": "...",
                "error": None,
            },
        )

        srt = _make_srt([
            ("00:00:00", "00:29:59", "Texto vivienda"),
            ("00:30:00", "00:59:59", "Texto sanidad"),
        ])

        result = analyze_chapters_with_ai(srt_content=srt, agenda_content="Agenda de la sesión")

        assert result["success"] is True
        assert result["total_chapters"] == 2
        assert result["total_duration_seconds"] == 3600
        assert len(result["chapters"]) == 2
        assert result["error"] is None

        first = result["chapters"][0]
        assert first["chapter_number"] == 1
        assert first["title"] == "Debate sobre Vivienda"
        assert first["start_time"] == "00:00:00"
        assert first["end_time"] == "00:30:00"
        assert first["duration_seconds"] == 1800
        assert "Vivienda" in first["topics"]

    def test_analyze_chapters_empty_srt(self, mocker):
        """Empty SRT content returns failure with descriptive error and no AI call."""
        from utils.ai_chapter_analyzer import analyze_chapters_with_ai

        mock_ai = mocker.patch("utils.ai_chapter_analyzer.generate_json_completion")

        result = analyze_chapters_with_ai(srt_content="", agenda_content="Agenda")

        assert result["success"] is False
        assert result["total_chapters"] == 0
        assert result["chapters"] == []
        assert result["error"] is not None
        mock_ai.assert_not_called()

    def test_analyze_chapters_api_error(self, mocker):
        """When generate_json_completion returns an error, result reflects failure."""
        from utils.ai_chapter_analyzer import analyze_chapters_with_ai

        mocker.patch(
            "utils.ai_chapter_analyzer.generate_json_completion",
            return_value={
                "data": None,
                "raw_content": None,
                "error": "OpenAI API error: rate limit exceeded",
            },
        )

        srt = _make_srt([("00:00:00", "00:30:00", "Contenido de prueba")])

        result = analyze_chapters_with_ai(srt_content=srt, agenda_content="Agenda")

        assert result["success"] is False
        assert result["chapters"] == []
        assert "rate limit" in result["error"]
