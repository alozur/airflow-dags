"""Tests for the chapter-duration split (issue #466) in vad_helpers.

Covers ``_timeline_within`` (D5 helper), ``_split_one_chapter`` (per-chapter
audio-cut composition, exercised with ``source_video=None`` to isolate the
composition logic from VAD/audio mocking), and ``split_long_chapters_with_vad``
(entry point, with ``_find_source_video``, ``extract_audio_wav``, and
``detect_speech_segments`` mocked) with SYNTHETIC chapters. The suite never
touches real audio, ffmpeg, torch, or webrtcvad. Mirrors
``test_vad_chapter_adjust.py``'s ``_patch_pipeline`` pattern.
"""

from __future__ import annotations

import copy

from congress_videos.modules.vad_helpers import _split_one_chapter, _timeline_within, split_long_chapters_with_vad
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


# ---------------------------------------------------------------------------
# split_long_chapters_with_vad — synthetic chapters, mocked VAD/ffmpeg/locator
# ---------------------------------------------------------------------------


def _scored(chapters: list[dict], video_id: str = "vid-1") -> dict:
    return {"videos": [{"video_id": video_id, "scored_chapters": chapters}]}


class TestSplitLongChaptersWithVad:
    def _patch_pipeline(self, mocker, *, segments, video="/data/vid-1/source.mp4"):
        """Mock the locator, ffmpeg slice extract, and segment detection.

        ``segments`` is what the mocked single-pass ``detect_speech_segments``
        returns for EVERY candidate-cut slice (kept simple: an empty list means
        "ran, no gaps found anywhere" → every cut falls back to arithmetic).
        """
        mocker.patch("congress_videos.modules.vad_helpers._find_source_video", return_value=video)
        mocker.patch("congress_videos.modules.vad_helpers.extract_audio_wav", return_value="/tmp/slice.wav")
        detect = mocker.patch(
            "congress_videos.modules.vad_helpers.detect_speech_segments",
            return_value=segments,
        )
        mocker.patch("congress_videos.modules.vad_helpers.tempfile.mkstemp", return_value=(99, "/tmp/slice.wav"))
        mocker.patch("congress_videos.modules.vad_helpers.os.close")
        mocker.patch("congress_videos.modules.vad_helpers.os.path.exists", return_value=False)
        return detect

    def test_64_minute_chapter_splits_into_two_contiguous_children(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        chapter = {"title": "Debate general", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")

        children = result["videos"][0]["scored_chapters"]
        assert len(children) == 2
        assert children[0]["start_time"] == "00:00:00,000"
        assert children[0]["end_time"] == children[1]["start_time"]  # contiguous
        assert children[1]["end_time"] == "01:04:00,000"  # exactly covers the parent

    def test_children_inherit_metadata_and_get_numbered_titles(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        chapter = {
            "title": "Debate general",
            "start_time": "00:00:00,000",
            "end_time": "01:04:00,000",
            "description": "La descripción",
            "speakers": ["Ana"],
            "topics": ["Economía"],
            "key_speakers": ["Ana"],
            "relevance_score": 4,
            "speaker_relevance_points": 2,
            "topic_relevance_points": 1,
            "public_interest_points": 1,
            "scoring_reasoning": "motivo",
            "is_current_topic": True,
            "scoring_error": None,
        }
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert children[0]["title"] == "Debate general (Parte 1/2)"
        assert children[1]["title"] == "Debate general (Parte 2/2)"
        for child in children:
            assert child["description"] == "La descripción"
            assert child["speakers"] == ["Ana"]
            assert child["topics"] == ["Economía"]
            assert child["key_speakers"] == ["Ana"]
            assert child["relevance_score"] == 4
            assert child["speaker_relevance_points"] == 2
            assert child["topic_relevance_points"] == 1
            assert child["public_interest_points"] == 1
            assert child["scoring_reasoning"] == "motivo"
            assert child["is_current_topic"] is True
            assert child["scoring_error"] is None

    def test_duration_minutes_recomputed_per_child(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert children[0]["duration_minutes"] == 32.0
        assert children[1]["duration_minutes"] == 32.0

    def test_timeline_filtered_per_child(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        timeline = [
            {"time": "00:05:00,000", "speaker": "A", "content": "x"},  # in child 1 (0-32min)
            {"time": "00:50:00,000", "speaker": "B", "content": "y"},  # in child 2 (32-64min)
        ]
        chapter = {
            "title": "T",
            "start_time": "00:00:00,000",
            "end_time": "01:04:00,000",
            "timeline": timeline,
        }
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert [m["time"] for m in children[0]["timeline"]] == ["00:05:00,000"]
        assert [m["time"] for m in children[1]["timeline"]] == ["00:50:00,000"]

    def test_empty_timeline_child_stays_empty(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        timeline = [{"time": "00:05:00,000", "speaker": "A", "content": "x"}]  # only inside child 1
        chapter = {
            "title": "T",
            "start_time": "00:00:00,000",
            "end_time": "01:04:00,000",
            "timeline": timeline,
        }
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert children[1]["timeline"] == []

    def test_video_not_found_still_splits_arithmetically(self, mocker):
        """Diverges from trim_chapter_silence_with_vad (D4): fails FORWARD, not passthrough."""
        mocker.patch("congress_videos.modules.vad_helpers._find_source_video", return_value=None)
        detect = mocker.patch("congress_videos.modules.vad_helpers.detect_speech_segments")
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert len(children) == 2
        assert children[0]["end_time"] == children[1]["start_time"]
        detect.assert_not_called()  # no audio attempted without a source video

    def test_extract_audio_failure_falls_back_to_arithmetic(self, mocker):
        mocker.patch("congress_videos.modules.vad_helpers._find_source_video", return_value="/data/vid-1/s.mp4")
        mocker.patch(
            "congress_videos.modules.vad_helpers.extract_audio_wav",
            side_effect=RuntimeError("ffmpeg audio extract failed"),
        )
        mocker.patch("congress_videos.modules.vad_helpers.tempfile.mkstemp", return_value=(99, "/tmp/slice.wav"))
        mocker.patch("congress_videos.modules.vad_helpers.os.close")
        mocker.patch("congress_videos.modules.vad_helpers.os.path.exists", return_value=False)
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert len(children) == 2
        assert children[0]["end_time"] == children[1]["start_time"]

    def test_vad_detection_raises_falls_back_to_arithmetic(self, mocker):
        mocker.patch("congress_videos.modules.vad_helpers._find_source_video", return_value="/data/vid-1/s.mp4")
        mocker.patch("congress_videos.modules.vad_helpers.extract_audio_wav", return_value="/tmp/slice.wav")
        mocker.patch(
            "congress_videos.modules.vad_helpers.detect_speech_segments",
            side_effect=RuntimeError("vad backend crashed"),
        )
        mocker.patch("congress_videos.modules.vad_helpers.tempfile.mkstemp", return_value=(99, "/tmp/slice.wav"))
        mocker.patch("congress_videos.modules.vad_helpers.os.close")
        mocker.patch("congress_videos.modules.vad_helpers.os.path.exists", return_value=False)
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert len(children) == 2

    def test_under_threshold_chapter_is_untouched(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "00:30:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        children = result["videos"][0]["scored_chapters"]

        assert children == [chapter]
        assert children[0] is chapter  # SAME object — no copy, no re-suffix (idempotence)

    def test_idempotent_rerun_over_already_split_output(self, mocker):
        self._patch_pipeline(mocker, segments=[])
        chapter = {"title": "Debate general", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        first = split_long_chapters_with_vad(scored, target_date="2025-10-08")
        first_children = copy.deepcopy(first["videos"][0]["scored_chapters"])

        second = split_long_chapters_with_vad(first, target_date="2025-10-08")
        second_children = second["videos"][0]["scored_chapters"]

        assert second_children == first_children

    def test_disabled_via_constant_is_passthrough(self, mocker):
        mocker.patch("congress_videos.modules.vad_helpers.CHAPTER_SPLIT_ENABLED", False)
        detect = mocker.patch("congress_videos.modules.vad_helpers.detect_speech_segments")
        chapter = {"title": "T", "start_time": "00:00:00,000", "end_time": "01:04:00,000"}
        scored = _scored([chapter])

        result = split_long_chapters_with_vad(scored, target_date="2025-10-08")

        assert result["videos"][0]["scored_chapters"] == [chapter]
        detect.assert_not_called()

    def test_empty_input_returns_unchanged(self):
        assert split_long_chapters_with_vad({}, target_date="2025-10-08") == {}
        assert split_long_chapters_with_vad({"videos": []}, target_date="2025-10-08") == {"videos": []}
