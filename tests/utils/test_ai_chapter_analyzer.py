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
            ("00:00:00", 0.0),
            ("00:01:00", 60.0),
            ("01:00:00", 3600.0),
            ("01:30:15", 5415.0),
            # Now millisecond-precise (float return, not int-truncated)
            ("00:00:30,500", 30.5),
            ("02:00:00,000", 7200.0),
        ],
    )
    def test_parametrize_various_timestamps(self, timestamp: str, expected: float):
        """Correctly converts HH:MM:SS and HH:MM:SS,mmm to total float seconds."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        assert parse_timestamp_to_seconds(timestamp) == expected

    def test_returns_zero_for_invalid_format(self):
        """Malformed timestamp returns 0.0 (graceful fallback behavior)."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        result = parse_timestamp_to_seconds("bad")
        assert result == 0.0

    def test_handles_leading_whitespace(self):
        """Timestamps with surrounding whitespace are handled."""
        from utils.ai_chapter_analyzer import parse_timestamp_to_seconds

        assert parse_timestamp_to_seconds("  00:05:30  ") == 330.0


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

    def test_use_adaptive_is_forwarded_to_detect_silence_gaps(self, mocker):
        """#13 wiring: chunk_by_silence(use_adaptive=True) must call
        detect_silence_gaps with use_adaptive=True so the adaptive path is
        reachable in production."""
        from utils import ai_chapter_analyzer

        spy = mocker.patch.object(
            ai_chapter_analyzer,
            "detect_silence_gaps",
            wraps=ai_chapter_analyzer.detect_silence_gaps,
        )

        srt = _make_srt([
            ("00:00:00", "00:05:00", "A"),
            ("00:35:00", "00:40:00", "B"),  # 30-min gap → always detected
        ])
        ai_chapter_analyzer.chunk_by_silence(
            srt,
            min_silence_seconds=15,
            min_chunk_duration_minutes=0,
            use_adaptive=True,
            adaptive_percentile=80.0,
        )

        spy.assert_called_once()
        _, kwargs = spy.call_args
        assert kwargs.get("use_adaptive") is True, (
            "chunk_by_silence must forward use_adaptive=True to detect_silence_gaps"
        )
        assert kwargs.get("adaptive_percentile") == 80.0


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


# --------------------------------------------------------------------------- #
# T2.3 — _adaptive_silence_threshold  (#13)
# --------------------------------------------------------------------------- #

class TestAdaptiveSilenceThreshold:
    """Tests for spec #13: percentile-based adaptive silence threshold."""

    def test_75th_percentile_of_known_distribution(self):
        """Spec #13: [2,3,5,10,12,15,20,30] at p75 with linear interpolation.

        Using the standard linear-interpolation formula (same as numpy default):
          idx = 0.75 * (8-1) = 5.25  →  lower=5 (15.0), upper=6 (20.0), frac=0.25
          value = 15 + 0.25 * (20 - 15) = 16.25

        The spec's '≈17.5' referred to the midpoint method; our implementation uses
        linear interpolation (matches numpy default). Expected value is 16.25.
        """
        from utils.ai_chapter_analyzer import _adaptive_silence_threshold

        gaps = [2.0, 3.0, 5.0, 10.0, 12.0, 15.0, 20.0, 30.0]
        result = _adaptive_silence_threshold(gaps, percentile=75.0, floor=8)

        # Linear interpolation: 15 + 0.25*(20-15) = 16.25
        assert abs(result - 16.25) < 0.01, f"Expected ~16.25, got {result}"
        # And it must be above the floor
        assert result >= 8.0

    def test_floor_applied_when_percentile_below_floor(self):
        """Floor ensures result never drops below the minimum."""
        from utils.ai_chapter_analyzer import _adaptive_silence_threshold

        # All tiny gaps → percentile would be <8 without floor
        gaps = [0.1, 0.2, 0.3, 0.4, 0.5]
        result = _adaptive_silence_threshold(gaps, percentile=50.0, floor=8)

        assert result >= 8.0

    def test_empty_gaps_returns_floor(self):
        """Edge case: empty list returns floor value."""
        from utils.ai_chapter_analyzer import _adaptive_silence_threshold

        result = _adaptive_silence_threshold([], percentile=75.0, floor=8)

        assert result == 8.0

    def test_single_gap_returns_that_gap_or_floor(self):
        """Edge case: single gap → any percentile equals that gap (if >= floor)."""
        from utils.ai_chapter_analyzer import _adaptive_silence_threshold

        result = _adaptive_silence_threshold([25.0], percentile=75.0, floor=8)

        assert result == pytest.approx(25.0)


# --------------------------------------------------------------------------- #
# T2.3 — detect_silence_gaps with use_adaptive=True  (#13)
# --------------------------------------------------------------------------- #

class TestDetectSilenceGapsAdaptive:
    """Integration tests for adaptive mode in detect_silence_gaps."""

    def test_fixed_mode_unchanged_by_default(self):
        """Spec #13: default (use_adaptive=False) behaves identically to pre-Batch2."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:10", "A"),
            ("00:00:40", "00:00:50", "B"),  # 30s gap
            ("00:01:30", "00:01:40", "C"),  # 40s gap
        ])

        result_old = detect_silence_gaps(srt, min_silence_seconds=15)
        result_fixed = detect_silence_gaps(srt, min_silence_seconds=15, use_adaptive=False)

        assert result_old == result_fixed
        assert len(result_fixed) == 2

    def test_adaptive_uses_percentile_threshold(self):
        """Spec #13: adaptive mode computes threshold from gap distribution."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        # Gaps: 2s (below), 5s, 10s, 12s, 15s, 20s, 30s (above p75≈17.5)
        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:00:07", "00:00:08", "B"),  # gap ~2s
            ("00:00:13", "00:00:15", "C"),  # gap ~5s
            ("00:00:25", "00:00:27", "D"),  # gap ~10s
            ("00:00:39", "00:00:41", "E"),  # gap ~12s
            ("00:00:56", "00:00:58", "F"),  # gap ~15s
            ("00:01:18", "00:01:20", "G"),  # gap ~20s
            ("00:01:50", "00:01:52", "H"),  # gap ~30s
        ])

        result_adaptive = detect_silence_gaps(srt, min_silence_seconds=15, use_adaptive=True, adaptive_percentile=75)
        result_fixed = detect_silence_gaps(srt, min_silence_seconds=15, use_adaptive=False)

        # Adaptive should return fewer gaps than fixed (higher threshold)
        assert len(result_adaptive) <= len(result_fixed)

    def test_adaptive_fallback_when_no_gaps_pass(self, caplog):
        """Spec #13: when adaptive produces 0 gaps, falls back to fixed with WARNING."""
        import logging
        from utils.ai_chapter_analyzer import detect_silence_gaps

        # All gaps are tiny — p75 would be huge, but so are all gaps tiny.
        # Actually we need a case where the adaptive threshold exceeds all gaps.
        # Use p99 on a distribution where all gaps are 1-3s, but min_silence=0.5s
        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:00:06", "00:00:11", "B"),   # 1s gap
            ("00:00:12", "00:00:17", "C"),   # 1s gap
            ("00:00:19", "00:00:24", "D"),   # 2s gap
        ])

        with caplog.at_level(logging.WARNING, logger="utils.ai_chapter_analyzer"):
            # p99 of [1,1,2] = ~2s; min_silence=0.5s → all 3 gaps are above fixed threshold
            # but with floor=8, adaptive threshold is 8s, which all 1-2s gaps fail.
            result = detect_silence_gaps(
                srt,
                min_silence_seconds=1,
                use_adaptive=True,
                adaptive_percentile=75,
            )

        # Should have fallen back to fixed threshold (min_silence=1s)
        # All 1-2s gaps pass the fixed threshold of 1s
        assert len(result) > 0, "Expected fallback to fixed threshold producing gaps"
        assert any("falling back" in r.message.lower() for r in caplog.records)

    def test_single_gap_adaptive_returns_it(self):
        """Spec #13: single gap in audio → adaptive returns that gap."""
        from utils.ai_chapter_analyzer import detect_silence_gaps

        srt = _make_srt([
            ("00:00:00", "00:00:05", "A"),
            ("00:05:00", "00:05:05", "B"),  # 295s gap — huge, definitely above p75
        ])

        result = detect_silence_gaps(srt, min_silence_seconds=15, use_adaptive=True)

        assert len(result) == 1
        assert result[0]["gap_duration_seconds"] == pytest.approx(295, rel=1e-2)
