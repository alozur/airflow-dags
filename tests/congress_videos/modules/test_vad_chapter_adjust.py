"""Tests for the VAD chapter silence-trim in vad_helpers.

Covers ``trim_chapter_silence_with_vad`` (both edges) with SYNTHETIC chapters.
``extract_audio_wav``, ``detect_speech_bounds`` and the
on-disk video locator are mocked — the suite never touches real audio, ffmpeg,
torch, or webrtcvad.
"""

from __future__ import annotations

import copy

import pytest

from congress_videos.modules.vad_helpers import trim_chapter_silence_with_vad
from utils.time_utils import parse_timestamp


# ---------------------------------------------------------------------------
# trim_chapter_silence_with_vad — synthetic chapters, mocked VAD/ffmpeg/locator
# ---------------------------------------------------------------------------

def _scored(chapters: list[dict], video_id: str = "vid-1") -> dict:
    return {"videos": [{"video_id": video_id, "scored_chapters": chapters}]}


class TestTrimChapterSilenceWithVad:

    def _patch_pipeline(self, mocker, *, bounds, video="/data/vid-1/source.mp4"):
        """Mock the locator, ffmpeg slice extract, and bounds detection.

        ``bounds`` is the ``(start_offset, end_offset)`` tuple returned by the
        mocked single-pass ``detect_speech_bounds`` (relative to the slice).
        """
        mocker.patch(
            "congress_videos.modules.vad_helpers._find_source_video",
            return_value=video,
        )
        mocker.patch(
            "congress_videos.modules.vad_helpers.extract_audio_wav",
            return_value="/tmp/slice.wav",
        )
        detect = mocker.patch(
            "congress_videos.modules.vad_helpers.detect_speech_bounds",
            return_value=bounds,
        )
        # tempfile + cleanup are stubbed to avoid disk I/O.
        mocker.patch(
            "congress_videos.modules.vad_helpers.tempfile.mkstemp",
            return_value=(99, "/tmp/slice.wav"),
        )
        mocker.patch("congress_videos.modules.vad_helpers.os.close")
        mocker.patch("congress_videos.modules.vad_helpers.os.path.exists", return_value=False)
        return detect

    # --- start edge (iter-2 parity) ---

    def test_silent_start_chapter_start_rises_by_sum(self, mocker):
        """start_offset 48s on a chapter starting at 600s → new start 648s (SUM)."""
        self._patch_pipeline(mocker, bounds=(48.0, None))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        assert result["videos"][0]["scored_chapters"][0]["start_time"] == "00:10:48,000"

    def test_chapter_opening_on_voice_unchanged(self, mocker):
        """start_offset 0 → start effectively unchanged."""
        self._patch_pipeline(mocker, bounds=(0.0, None))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        assert result["videos"][0]["scored_chapters"][0]["start_time"] == "00:10:00,000"

    def test_short_chapter_under_300s_is_trimmed_both_edges(self, mocker):
        """120s chapter with 15s initial + trailing silence trimmed on BOTH edges."""
        # slice 0..120; start_offset 15s → 00:00:15; end_offset 105s → 00:01:45 (< 120s).
        self._patch_pipeline(mocker, bounds=(15.0, 105.0))
        scored = _scored([{"start_time": "00:00:00,000", "end_time": "00:02:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:00:15,000"
        assert chapter["end_time"] == "00:01:45,000"

    def test_new_start_breaking_min_duration_rejected(self, mocker):
        """new_start within min_chapter_secs of the end → keep original start."""
        # chapter 100s long; start_offset 98s → gap to end 2s < 5s min.
        self._patch_pipeline(mocker, bounds=(98.0, None))
        scored = _scored([{"start_time": "00:00:00,000", "end_time": "00:01:40,000"}])

        result = trim_chapter_silence_with_vad(
            scored, target_date="2025-10-08", min_chapter_secs=5.0
        )

        assert result["videos"][0]["scored_chapters"][0]["start_time"] == "00:00:00,000"

    # --- end edge (NEW) ---

    def test_trailing_silence_end_drops_to_numeric(self, mocker):
        """end_offset 840s on a chapter at 600s → new end 1440s = 00:24:00 (< 1500s)."""
        # start 600s (00:10:00), end 1500s (00:25:00); slice 0..900s.
        self._patch_pipeline(mocker, bounds=(None, 840.0))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:00,000"  # start unchanged (None)
        assert chapter["end_time"] == "00:24:00,000"     # 600 + 840 = 1440s

    def test_voice_to_end_clamp_keeps_end_unchanged(self, mocker):
        """end_offset beyond the slice → min() clamps to the original end → no change."""
        # slice is 900s; end_offset 905s (voice-to-end + 5s margin) → 600+905=1505 > 1500.
        self._patch_pipeline(mocker, bounds=(None, 905.0))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        assert result["videos"][0]["scored_chapters"][0]["end_time"] == "00:25:00,000"

    def test_clamp_never_exceeds_original_end(self, mocker):
        """Even a huge end_offset NEVER produces new_end > original end."""
        self._patch_pipeline(mocker, bounds=(None, 99999.0))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        new_end = result["videos"][0]["scored_chapters"][0]["end_time"]
        assert parse_timestamp(new_end) <= parse_timestamp("00:25:00,000")
        assert new_end == "00:25:00,000"

    def test_end_offset_none_keeps_end(self, mocker):
        """end_offset None → end unchanged (start may still move)."""
        self._patch_pipeline(mocker, bounds=(48.0, None))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:48,000"
        assert chapter["end_time"] == "00:25:00,000"

    def test_both_edges_adjusted_together(self, mocker):
        """start rises AND end drops in the same single-pass call."""
        # start 600s, end 1500s, slice 0..900; start_offset 48s, end_offset 840s.
        self._patch_pipeline(mocker, bounds=(48.0, 840.0))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:48,000"  # 648s
        assert chapter["end_time"] == "00:24:00,000"     # 1440s

    def test_end_trim_breaking_min_duration_kept(self, mocker):
        """end trim that would leave < min_chapter_secs → end kept original."""
        # chapter 0..100s; start_offset None; end_offset 3s → new_end 3s, dur 3s < 5s.
        self._patch_pipeline(mocker, bounds=(None, 3.0))
        scored = _scored([{"start_time": "00:00:00,000", "end_time": "00:01:40,000"}])

        result = trim_chapter_silence_with_vad(
            scored, target_date="2025-10-08", min_chapter_secs=5.0
        )

        assert result["videos"][0]["scored_chapters"][0]["end_time"] == "00:01:40,000"

    # --- best-effort / both-edge None ---

    def test_bounds_none_none_keeps_both(self, mocker):
        """detect_speech_bounds → (None, None) → both timestamps unchanged."""
        self._patch_pipeline(mocker, bounds=(None, None))
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:00,000"
        assert chapter["end_time"] == "00:25:00,000"

    def test_one_vad_pass_per_chapter(self, mocker):
        """detect_speech_bounds is called exactly ONCE per chapter (NFR8)."""
        detect = self._patch_pipeline(mocker, bounds=(48.0, 840.0))
        scored = _scored([
            {"start_time": "00:10:00,000", "end_time": "00:25:00,000"},
            {"start_time": "00:30:00,000", "end_time": "00:45:00,000"},
        ])

        trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        assert detect.call_count == 2  # one pass per chapter, no doubling

    def test_video_not_found_leaves_all_chapters_unchanged(self, mocker):
        """No media on disk → every chapter of that video unchanged, no raise."""
        mocker.patch(
            "congress_videos.modules.vad_helpers._find_source_video",
            return_value=None,
        )
        detect = mocker.patch("congress_videos.modules.vad_helpers.detect_speech_bounds")
        scored = _scored([
            {"start_time": "00:10:00,000", "end_time": "00:25:00,000"},
            {"start_time": "00:30:00,000", "end_time": "00:40:00,000"},
        ])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        detect.assert_not_called()
        chapters = result["videos"][0]["scored_chapters"]
        assert chapters[0]["start_time"] == "00:10:00,000"
        assert chapters[0]["end_time"] == "00:25:00,000"
        assert chapters[1]["start_time"] == "00:30:00,000"

    def test_extract_audio_failure_best_effort_no_raise(self, mocker):
        """extract_audio_wav raising → chapter unchanged, task does not fail."""
        mocker.patch(
            "congress_videos.modules.vad_helpers._find_source_video",
            return_value="/data/vid-1/source.mp4",
        )
        mocker.patch(
            "congress_videos.modules.vad_helpers.extract_audio_wav",
            side_effect=RuntimeError("ffmpeg boom"),
        )
        mocker.patch(
            "congress_videos.modules.vad_helpers.tempfile.mkstemp",
            return_value=(99, "/tmp/slice.wav"),
        )
        mocker.patch("congress_videos.modules.vad_helpers.os.close")
        mocker.patch("congress_videos.modules.vad_helpers.os.path.exists", return_value=False)
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        result = trim_chapter_silence_with_vad(scored, target_date="2025-10-08")

        chapter = result["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:00,000"
        assert chapter["end_time"] == "00:25:00,000"

    def test_idempotent_rerun_no_drift(self, mocker):
        """Re-run over an already-trimmed chapter yields no drift on either edge."""
        scored = _scored([{"start_time": "00:10:00,000", "end_time": "00:25:00,000"}])

        # First run: 48s initial silence, end at 1440s.
        self._patch_pipeline(mocker, bounds=(48.0, 840.0))
        first = trim_chapter_silence_with_vad(
            copy.deepcopy(scored), target_date="2025-10-08"
        )
        chapter = first["videos"][0]["scored_chapters"][0]
        assert chapter["start_time"] == "00:10:48,000"
        assert chapter["end_time"] == "00:24:00,000"

        # Second run over the corrected chapter: opens on voice (start_offset 0) and
        # the end offset lands beyond the now-shorter slice → clamp pins it → no drift.
        # slice now 648..1440 (792s); end_offset 999s clamps to 1440s.
        self._patch_pipeline(mocker, bounds=(0.0, 999.0))
        second = trim_chapter_silence_with_vad(first, target_date="2025-10-08")
        chapter2 = second["videos"][0]["scored_chapters"][0]
        assert chapter2["start_time"] == "00:10:48,000"
        assert chapter2["end_time"] == "00:24:00,000"

    def test_empty_input_returns_unchanged(self, mocker):
        assert trim_chapter_silence_with_vad({}, target_date="2025-10-08") == {}
        assert trim_chapter_silence_with_vad(
            {"videos": []}, target_date="2025-10-08"
        ) == {"videos": []}
