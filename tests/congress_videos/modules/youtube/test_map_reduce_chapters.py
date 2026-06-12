"""Tests for congress_videos.modules.youtube.map_reduce_chapters (#8, #15)."""

from __future__ import annotations

from congress_videos.modules.youtube.map_reduce_chapters import (
    Window,
    map_chapters,
    map_reduce_identify_chapters,
    reduce_chapters,
    window_srt,
    _resolve_seams,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_srt(num_blocks: int, block_chars: int = 100, start_idx: int = 1) -> str:
    """Build a synthetic SRT of ``num_blocks`` blocks, each ~``block_chars`` long.

    Each block is a valid SRT block: index, ``HH:MM:SS,mmm --> HH:MM:SS,mmm``,
    and a text line padded to the requested size.
    """
    blocks = []
    for i in range(num_blocks):
        n = start_idx + i
        secs = n * 10
        start = f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d},000"
        end_secs = secs + 5
        end = f"{end_secs // 3600:02d}:{(end_secs % 3600) // 60:02d}:{end_secs % 60:02d},000"
        text = ("palabra " * (block_chars // 8)).strip()
        blocks.append(f"{n}\n{start} --> {end}\n{text}")
    return "\n\n".join(blocks)


def _ch(start: str, end: str, title: str = "x") -> dict:
    return {"title": title, "start_time": start, "end_time": end}


# ---------------------------------------------------------------------------
# #8 — windowing
# ---------------------------------------------------------------------------


class TestWindowSrt:

    def test_short_srt_single_window(self):
        """SRT below the window threshold → exactly one window (single pass)."""
        srt = _make_srt(5, block_chars=200)  # ~1k chars
        windows = window_srt(srt, window_chars=80_000)
        assert len(windows) == 1
        assert windows[0].index == 0
        assert windows[0].text == srt.strip()

    def test_srt_exactly_equals_window_uses_single_pass(self):
        """Boundary `==` uses single pass (`<=`)."""
        srt = _make_srt(10, block_chars=100)
        windows = window_srt(srt, window_chars=len(srt.strip()))
        assert len(windows) == 1

    def test_window_larger_than_srt_single_pass(self):
        srt = _make_srt(10, block_chars=100)
        windows = window_srt(srt, window_chars=500_000)
        assert len(windows) == 1

    def test_long_srt_multiple_windows_with_overlap(self):
        """A long SRT is split into several windows (240k/80k ≈ 3+)."""
        srt = _make_srt(2400, block_chars=100)  # ~240k chars
        windows = window_srt(srt, window_chars=80_000, overlap_pct=0.12)
        assert len(windows) >= 3
        # Overlap: each window (after the first) must start before the previous ended.
        for prev, cur in zip(windows, windows[1:]):
            assert cur.char_start < prev.char_end

    def test_windows_never_split_mid_block(self):
        """No window text may start in the middle of an SRT block."""
        srt = _make_srt(800, block_chars=120)
        windows = window_srt(srt, window_chars=40_000, overlap_pct=0.1)
        assert len(windows) >= 2
        for w in windows:
            # A whole-block window starts with a digit index line.
            first_line = w.text.splitlines()[0]
            assert first_line.strip().isdigit()


class TestMapChapters:

    def test_single_window_one_call(self):
        srt = _make_srt(5, block_chars=200)
        windows = window_srt(srt, window_chars=80_000)
        calls = []

        def identify(text):
            calls.append(text)
            return [_ch("00:00:10", "00:01:00")]

        results = map_chapters(windows, identify)
        assert len(calls) == 1
        assert results == [[_ch("00:00:10", "00:01:00")]]

    def test_one_call_per_window(self):
        srt = _make_srt(2400, block_chars=100)
        windows = window_srt(srt, window_chars=80_000)
        calls = []

        def identify(text):
            calls.append(text)
            return []

        map_chapters(windows, identify)
        assert len(calls) == len(windows)

    def test_failing_window_isolated_as_empty(self):
        """A window whose identify_fn raises contributes [] (others survive)."""
        windows = [Window(0, "a", 0, 1), Window(1, "b", 1, 2)]

        def identify(text):
            if text == "a":
                raise RuntimeError("boom")
            return [_ch("00:00:10", "00:01:00")]

        results = map_chapters(windows, identify)
        assert results[0] == []
        assert results[1] == [_ch("00:00:10", "00:01:00")]


# ---------------------------------------------------------------------------
# #15 — seam resolution + reduce
# ---------------------------------------------------------------------------


class TestResolveSeams:

    def test_wider_boundary_wins(self):
        """Same chapter from adjacent windows → union of bounds (wider wins)."""
        w1 = [_ch("00:10:00", "00:15:00", "narrow")]
        w2 = [_ch("00:09:30", "00:15:30", "wider title here")]
        merged = _resolve_seams([w1, w2])
        assert len(merged) == 1
        assert merged[0]["start_time"] == "00:09:30"
        assert merged[0]["end_time"] == "00:15:30"
        # Longer title kept.
        assert merged[0]["title"] == "wider title here"

    def test_non_overlapping_both_kept(self):
        w1 = [_ch("00:01:00", "00:08:00", "a")]
        w2 = [_ch("00:20:00", "00:27:00", "b")]
        merged = _resolve_seams([w1, w2])
        assert len(merged) == 2

    def test_empty_window_no_error(self):
        w1 = [_ch("00:01:00", "00:08:00", "a")]
        w2: list[dict] = []
        w3 = [_ch("00:20:00", "00:27:00", "c")]
        merged = _resolve_seams([w1, w2, w3])
        assert len(merged) == 2


class TestReduceChapters:

    def test_seams_resolved_before_dedup(self):
        """Seam union runs first, then #6 dedup drops the narrower overlapper."""
        # W1/W2 = same chapter across seam → merged to 00:09:30–00:15:30.
        # W3 has a narrower chapter overlapping that merge >50% → dedup drops it.
        w1 = [_ch("00:10:00", "00:15:00", "seam-a")]
        w2 = [_ch("00:09:30", "00:15:30", "seam-a-wider")]
        w3 = [_ch("00:09:45", "00:14:00", "narrow-overlapper")]
        result = reduce_chapters([w1, w2, w3])
        assert len(result) == 1
        assert result[0]["start_time"] == "00:09:30"
        assert result[0]["end_time"] == "00:15:30"

    def test_distinct_chapters_sorted_by_start(self):
        w1 = [_ch("00:20:00", "00:27:00", "late")]
        w2 = [_ch("00:01:00", "00:08:00", "early")]
        result = reduce_chapters([w1, w2])
        assert [c["title"] for c in result] == ["early", "late"]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


class TestMapReduceIdentifyChapters:

    def test_short_srt_single_pass_passthrough(self):
        srt = _make_srt(5, block_chars=200)
        calls = []

        def identify(text):
            calls.append(text)
            return [_ch("00:00:10", "00:01:00", "only")]

        result = map_reduce_identify_chapters(srt, identify_fn=identify)
        assert len(calls) == 1
        assert result == [_ch("00:00:10", "00:01:00", "only")]

    def test_long_srt_maps_and_reduces(self):
        srt = _make_srt(2400, block_chars=100)

        def identify(text):
            # Each window returns one distinct chapter far apart so no merging.
            idx = text.splitlines()[0].strip()
            n = int(idx) * 10
            start = f"{n // 3600:02d}:{(n % 3600) // 60:02d}:{n % 60:02d}"
            end_n = n + 60
            end = f"{end_n // 3600:02d}:{(end_n % 3600) // 60:02d}:{end_n % 60:02d}"
            return [_ch(start, end, f"w{idx}")]

        result = map_reduce_identify_chapters(srt, identify_fn=identify)
        assert len(result) >= 1
        # Result is sorted by start time.
        from congress_videos.modules.youtube.map_reduce_chapters import _secs
        starts = [_secs(c["start_time"]) for c in result]
        assert starts == sorted(starts)
