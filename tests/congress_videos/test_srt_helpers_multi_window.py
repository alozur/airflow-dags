"""[RED] Tests for _window_srt_blocks_multi — multi-interval SRT retiming (issue #143).

Retimes SRT blocks across N kept windows with cumulative offsets equal to the
sum of the PREVIOUSLY KEPT window durations (so the output SRT lines up with
the concatenated, gap-free cut video). Pure; no I/O.
"""
from __future__ import annotations

from congress_videos.srt_helpers import _window_srt_blocks, _window_srt_blocks_multi


def _block(start: float, end: float, text: str) -> dict:
    return {"start_secs": start, "end_secs": end, "text": text}


class TestCumulativeOffsetsTwoWindows:
    """Spec example: keep_intervals [(0,30),(36,80)] (6s dropped between them);
    a block originally at 50s must map to 44s in the output."""

    def test_block_in_second_window_offset_by_first_windows_kept_duration(self):
        blocks = [_block(50.0, 51.0, "hola")]
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (36.0, 80.0)])
        assert len(result) == 1
        assert result[0]["start_secs"] == 44.0
        assert result[0]["end_secs"] == 45.0
        assert result[0]["text"] == "hola"

    def test_block_in_first_window_is_unshifted(self):
        blocks = [_block(10.0, 12.0, "primero")]
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (36.0, 80.0)])
        assert len(result) == 1
        assert result[0]["start_secs"] == 10.0
        assert result[0]["end_secs"] == 12.0

    def test_block_dropped_in_the_gap_is_excluded(self):
        """A block fully inside the excised [30,36) gap must not appear at all."""
        blocks = [_block(31.0, 33.0, "excisado")]
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (36.0, 80.0)])
        assert result == []

    def test_both_blocks_present_and_correctly_ordered(self):
        blocks = [
            _block(10.0, 12.0, "primero"),
            _block(50.0, 51.0, "segundo"),
        ]
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (36.0, 80.0)])
        assert [b["text"] for b in result] == ["primero", "segundo"]
        assert result[0]["start_secs"] < result[1]["start_secs"]


class TestCumulativeOffsetsThreeWindows:
    """keep_intervals [(0,10),(20,30),(50,60)] — each window kept 10s, so the
    third window's blocks are offset by the sum of the first two (20s)."""

    INTERVALS = [(0.0, 10.0), (20.0, 30.0), (50.0, 60.0)]

    def test_first_window_unshifted(self):
        result = _window_srt_blocks_multi([_block(2.0, 4.0, "a")], self.INTERVALS)
        assert result[0]["start_secs"] == 2.0
        assert result[0]["end_secs"] == 4.0

    def test_second_window_offset_by_first_kept_duration(self):
        # source 22s is 2s into the second window [20,30); output = 10 (first kept) + 2
        result = _window_srt_blocks_multi([_block(22.0, 24.0, "b")], self.INTERVALS)
        assert result[0]["start_secs"] == 12.0
        assert result[0]["end_secs"] == 14.0

    def test_third_window_offset_by_first_two_kept_durations(self):
        # source 55s is 5s into the third window [50,60); output = 10+10 (first two kept) + 5
        result = _window_srt_blocks_multi([_block(55.0, 57.0, "c")], self.INTERVALS)
        assert result[0]["start_secs"] == 25.0
        assert result[0]["end_secs"] == 27.0


class TestStraddlingBlockClampedInBothWindows:
    """A block whose original span crosses a cut boundary must be emitted in
    BOTH surviving windows, each clamped to that window's edge."""

    def test_block_straddling_the_cut_appears_twice_clamped(self):
        # Block spans [28, 38) — straddles the gap [30, 36) between the two
        # kept windows [0,30) and [36,80).
        blocks = [_block(28.0, 38.0, "straddle")]
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (36.0, 80.0)])

        assert len(result) == 2
        first, second = result
        # Clamped to the end of the first window: [28, 30) -> unshifted
        assert first["start_secs"] == 28.0
        assert first["end_secs"] == 30.0
        assert first["text"] == "straddle"
        # Clamped to the start of the second window: [36, 38) -> offset by 30
        assert second["start_secs"] == 30.0
        assert second["end_secs"] == 32.0
        assert second["text"] == "straddle"


class TestSingleIntervalEqualsWindowSrtBlocks:
    """With exactly one interval, output must equal _window_srt_blocks."""

    def test_matches_window_srt_blocks_exactly(self):
        blocks = [
            _block(5.0, 8.0, "one"),
            _block(20.0, 22.0, "two"),
            _block(100.0, 105.0, "outside"),
        ]
        multi_result = _window_srt_blocks_multi(blocks, [(0.0, 40.0)])
        single_result = _window_srt_blocks(blocks, 0.0, 40.0)
        assert multi_result == single_result

    def test_matches_window_srt_blocks_with_nonzero_start(self):
        blocks = [
            _block(15.0, 18.0, "kept"),
            _block(2.0, 4.0, "dropped-before"),
        ]
        multi_result = _window_srt_blocks_multi(blocks, [(10.0, 40.0)])
        single_result = _window_srt_blocks(blocks, 10.0, 40.0)
        assert multi_result == single_result


class TestMalformedOrUnsortedIntervalsFallback:
    """Defensive handling: unsorted intervals are sorted; degenerate
    (non-positive-duration) intervals are dropped rather than raising."""

    def test_unsorted_intervals_are_sorted_before_offsetting(self):
        # Intervals passed out of order — result must still match the
        # chronological (sorted) application.
        blocks = [_block(50.0, 51.0, "hola"), _block(10.0, 12.0, "primero")]
        unsorted_intervals = [(36.0, 80.0), (0.0, 30.0)]
        sorted_intervals = [(0.0, 30.0), (36.0, 80.0)]

        result_unsorted = _window_srt_blocks_multi(blocks, unsorted_intervals)
        result_sorted = _window_srt_blocks_multi(blocks, sorted_intervals)

        assert result_unsorted == result_sorted

    def test_degenerate_zero_length_interval_is_dropped_not_raised(self):
        blocks = [_block(10.0, 12.0, "hola")]
        # (30.0, 30.0) is degenerate (start == end) and must be silently skipped.
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (30.0, 30.0)])
        assert result == [{"start_secs": 10.0, "end_secs": 12.0, "text": "hola"}]

    def test_inverted_interval_is_dropped_not_raised(self):
        blocks = [_block(10.0, 12.0, "hola")]
        # (40.0, 20.0) has start > end — must be dropped, never raise.
        result = _window_srt_blocks_multi(blocks, [(0.0, 30.0), (40.0, 20.0)])
        assert result == [{"start_secs": 10.0, "end_secs": 12.0, "text": "hola"}]

    def test_empty_intervals_returns_empty_list(self):
        assert _window_srt_blocks_multi([_block(1.0, 2.0, "x")], []) == []

    def test_empty_blocks_returns_empty_list(self):
        assert _window_srt_blocks_multi([], [(0.0, 30.0)]) == []
