"""Tests for the chapter-duration split (issue #466) in vad_helpers.

Covers ``_timeline_within`` (D5 helper) and ``_split_one_chapter`` (per-chapter
audio-cut composition) with SYNTHETIC chapters. ``_split_one_chapter`` is
exercised with ``source_video=None``, which forces every cut through the D4
arithmetic fallback — this isolates the composition logic (bound computation,
deepcopy, metadata inheritance, title suffixing, timeline filtering) from VAD/
audio mocking, which is covered end-to-end once ``split_long_chapters_with_vad``
lands. Mirrors ``test_vad_chapter_adjust.py``'s synthetic-chapter style.
"""

from __future__ import annotations

from congress_videos.modules.vad_helpers import _split_one_chapter, _timeline_within
from utils.time_utils import parse_timestamp

# ---------------------------------------------------------------------------
# _timeline_within — D5 pure helper (no import of download.py's private symbol)
# ---------------------------------------------------------------------------


class TestTimelineWithin:
    def test_keeps_moments_within_range(self):
        timeline = [
            {"time": "00:00:05,000"},
            {"time": "00:10:00,000"},
            {"time": "00:20:00,000"},
        ]
        result = _timeline_within(timeline, 0.0, 600.0)  # 0..10min
        assert [m["time"] for m in result] == ["00:00:05,000", "00:10:00,000"]

    def test_empty_or_missing_timeline_returns_empty(self):
        assert _timeline_within([], 0.0, 100.0) == []
        assert _timeline_within(None, 0.0, 100.0) == []

    def test_unparseable_or_missing_time_entries_dropped(self):
        timeline = [{"time": None}, {"speaker": "x"}, {"time": "bogus"}, {"time": "00:00:05,000"}]
        result = _timeline_within(timeline, 0.0, 100.0)
        assert [m.get("time") for m in result] == ["00:00:05,000"]


# ---------------------------------------------------------------------------
# _split_one_chapter — audio-cut composition (source_video=None forces D4
# arithmetic fallback for every cut, isolating composition from VAD mocking)
# ---------------------------------------------------------------------------


class TestSplitOneChapter:
    def _kwargs(self):
        return {
            "max_chapter_secs": 2400.0,
            "min_child_secs": 300.0,
            "window_secs": 120.0,
            "widen_factor": 2.0,
            "gap_merge_secs": 2.0,
            "min_gap_secs": 3.0,
        }

    def test_90_minute_chapter_yields_3_contiguous_children_covering_parent_span(self):
        chapter = {"title": "Pleno", "start_time": "00:00:00,000", "end_time": "01:30:00,000"}

        children = _split_one_chapter(chapter, None, **self._kwargs())

        assert len(children) == 3
        assert children[0]["start_time"] == "00:00:00,000"
        assert children[0]["end_time"] == children[1]["start_time"]
        assert children[1]["end_time"] == children[2]["start_time"]
        assert children[2]["end_time"] == "01:30:00,000"
        for child in children:
            span = parse_timestamp(child["end_time"]) - parse_timestamp(child["start_time"])
            assert span <= 2400.0

    def test_90_minute_chapter_children_have_numbered_titles(self):
        chapter = {"title": "Pleno", "start_time": "00:00:00,000", "end_time": "01:30:00,000"}

        children = _split_one_chapter(chapter, None, **self._kwargs())

        assert [child["title"] for child in children] == [
            "Pleno (Parte 1/3)",
            "Pleno (Parte 2/3)",
            "Pleno (Parte 3/3)",
        ]

    def test_90_minute_chapter_children_inherit_parent_metadata(self):
        chapter = {
            "title": "Pleno",
            "start_time": "00:00:00,000",
            "end_time": "01:30:00,000",
            "description": "La descripción",
            "speakers": ["Ana"],
            "topics": ["Economía"],
        }

        children = _split_one_chapter(chapter, None, **self._kwargs())

        for child in children:
            assert child["description"] == "La descripción"
            assert child["speakers"] == ["Ana"]
            assert child["topics"] == ["Economía"]

    def test_90_minute_chapter_timeline_filtered_per_child(self):
        timeline = [
            {"time": "00:10:00,000"},  # child 1 (0-30min)
            {"time": "00:45:00,000"},  # child 2 (30-60min)
            {"time": "01:20:00,000"},  # child 3 (60-90min)
        ]
        chapter = {
            "title": "Pleno",
            "start_time": "00:00:00,000",
            "end_time": "01:30:00,000",
            "timeline": timeline,
        }

        children = _split_one_chapter(chapter, None, **self._kwargs())

        assert [m["time"] for m in children[0]["timeline"]] == ["00:10:00,000"]
        assert [m["time"] for m in children[1]["timeline"]] == ["00:45:00,000"]
        assert [m["time"] for m in children[2]["timeline"]] == ["01:20:00,000"]

    def test_under_threshold_chapter_returns_same_object_unsplit(self):
        chapter = {"title": "Pleno", "start_time": "00:00:00,000", "end_time": "00:30:00,000"}

        children = _split_one_chapter(chapter, None, **self._kwargs())

        assert children == [chapter]
        assert children[0] is chapter  # SAME object — no copy, no re-suffix (idempotence)
