"""Tests for utils.time_utils — Batch 0 (T0.1).

Covers all five spec scenarios for parse_timestamp plus ValueError edge case,
and basic coverage for format_timestamp.
"""

from __future__ import annotations

import pytest

from utils.time_utils import (
    format_timestamp,
    format_youtube_timestamp,
    parse_timestamp,
)


# ---------------------------------------------------------------------------
# parse_timestamp
# ---------------------------------------------------------------------------


class TestParseTimestamp:
    """Table-driven coverage of all spec #11 scenarios."""

    @pytest.mark.parametrize(
        "ts, expected",
        [
            # Scenario: HH:MM:SS format parsed correctly
            ("01:23:45", 5025.0),
            # Scenario: HH:MM:SS,mmm format preserves milliseconds
            ("00:01:30,500", 90.5),
            # Scenario: HH:MM:SS.mmm format (dot separator)
            ("00:00:05.250", 5.25),
            # Edge case: zero timestamp
            ("00:00:00,000", 0.0),
            ("00:00:00", 0.0),
            # Other valid inputs
            ("01:00:00", 3600.0),
            ("00:01:00", 60.0),
            ("01:30:15", 5415.0),
            # Milliseconds rounded correctly for 3-digit ms
            ("00:00:01,250", 1.25),
            ("00:10:15,500", 615.5),
            ("01:02:03,456", pytest.approx(3723.456, abs=1e-3)),
        ],
    )
    def test_parse_valid_timestamps(self, ts: str, expected: float) -> None:
        """Valid timestamp strings return the correct float seconds."""
        result = parse_timestamp(ts)
        assert result == expected

    def test_result_type_is_float(self) -> None:
        """parse_timestamp always returns a float, never int."""
        result = parse_timestamp("00:01:30,250")
        assert isinstance(result, float)

    def test_ms_precision_preserved(self) -> None:
        """Millisecond part contributes to the returned float."""
        with_ms = parse_timestamp("00:00:00,500")
        without_ms = parse_timestamp("00:00:00")
        assert with_ms == pytest.approx(0.5)
        assert without_ms == 0.0
        assert with_ms != without_ms

    def test_leading_trailing_whitespace_stripped(self) -> None:
        """Surrounding whitespace is normalised before parsing."""
        assert parse_timestamp("  01:23:45  ") == 5025.0

    # Edge case: invalid format raises ValueError
    def test_invalid_format_raises_value_error(self) -> None:
        """Non-timestamp string raises ValueError with a descriptive message."""
        with pytest.raises(ValueError, match="not-a-timestamp"):
            parse_timestamp("not-a-timestamp")

    def test_non_string_input_raises_value_error(self) -> None:
        """Non-string input raises ValueError (replaces old silent-0 fallback)."""
        with pytest.raises(ValueError):
            parse_timestamp(None)  # type: ignore[arg-type]

    def test_empty_string_raises_value_error(self) -> None:
        """Empty string raises ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp("")

    def test_missing_colon_separator_raises_value_error(self) -> None:
        """Timestamps without three colon-separated parts raise ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp("1:30")

    def test_minutes_out_of_range_raises_value_error(self) -> None:
        """Minutes >= 60 raises ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp("00:60:00")

    def test_seconds_out_of_range_raises_value_error(self) -> None:
        """Seconds >= 60 raises ValueError."""
        with pytest.raises(ValueError):
            parse_timestamp("00:00:60")


# ---------------------------------------------------------------------------
# format_timestamp
# ---------------------------------------------------------------------------


class TestFormatTimestamp:
    """Basic coverage for format_timestamp."""

    @pytest.mark.parametrize(
        "seconds, with_ms, expected",
        [
            (5025.0, True, "01:23:45,000"),
            (90.5, True, "00:01:30,500"),
            (5.25, True, "00:00:05,250"),
            (0.0, True, "00:00:00,000"),
            (5025.0, False, "01:23:45"),
            (90.5, False, "00:01:30"),
            (3600.0, False, "01:00:00"),
        ],
    )
    def test_format_various_values(
        self, seconds: float, with_ms: bool, expected: str
    ) -> None:
        """format_timestamp produces correct strings for various inputs."""
        assert format_timestamp(seconds, with_ms=with_ms) == expected

    def test_round_trip_no_ms(self) -> None:
        """parse_timestamp -> format_timestamp round-trip is lossless for whole seconds."""
        original = "02:45:33"
        result = format_timestamp(parse_timestamp(original), with_ms=False)
        assert result == original

    def test_round_trip_with_ms(self) -> None:
        """parse_timestamp -> format_timestamp round-trip preserves milliseconds."""
        original = "00:01:30,500"
        result = format_timestamp(parse_timestamp(original), with_ms=True)
        assert result == original


class TestFormatYoutubeTimestamp:

    @pytest.mark.parametrize(
        "seconds, expected",
        [
            (0.0, "00:00"),
            (5.0, "00:05"),
            (90.0, "01:30"),
            (90.9, "01:30"),  # milliseconds dropped (truncated)
            (3600.0, "1:00:00"),  # hour present -> no leading-zero hour
            (3930.0, "1:05:30"),
        ],
    )
    def test_format_youtube_various_values(self, seconds: float, expected: str) -> None:
        """format_youtube_timestamp uses M:SS below an hour and H:MM:SS above."""
        assert format_youtube_timestamp(seconds) == expected
