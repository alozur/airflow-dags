"""Unit tests for congress_videos.modules.speaker_resolution (issue #177).

TDD RED cycle: all 14 scenarios written before implementation.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_turn(
    turn_id: int = 1,
    start_seconds: float = 500.0,
    end_seconds: float = 700.0,
    video_id: str = "vidABC",
    chapter_id: int = 10,
    session_date: str = "2026-01-01",
) -> dict:
    return {
        "turn_id": turn_id,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "video_id": video_id,
        "chapter_id": chapter_id,
        "session_date": session_date,
        "output_path": "/data/turns/1/video.mp4",
    }


def _make_participants(slugs=("pedro-sanchez", "alberto-feijoo", "yolanda-diaz")):
    return [
        {"slug": s, "display_name": s.replace("-", " ").title(), "party": "TEST"}
        for s in slugs
    ]


def _make_blocks(blocks_data):
    """Build SRT block dicts from list of (start_secs, end_secs, text)."""
    return [
        {"start_secs": s, "end_secs": e, "text": t}
        for s, e, t in blocks_data
    ]


def _ok_completion(slug: str, confidence: float = 0.92):
    """Return a cached_json_completion-style result for a happy-path resolution."""
    return {
        "data": {
            "participant_slug": slug,
            "confidence": confidence,
            "evidence": "El presidente dice: tiene la palabra...",
        },
        "error": None,
    }


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_intro_window_secs_constant_value(self):
        """INTRO_WINDOW_SECS must equal 120."""
        from congress_videos.modules.speaker_resolution import INTRO_WINDOW_SECS

        assert INTRO_WINDOW_SECS == 120

    def test_turn_context_secs_constant_value(self):
        """TURN_CONTEXT_SECS must equal 60."""
        from congress_videos.modules.speaker_resolution import TURN_CONTEXT_SECS

        assert TURN_CONTEXT_SECS == 60

    def test_speaker_resolution_min_confidence_constant_value(self):
        """SPEAKER_RESOLUTION_MIN_CONFIDENCE must equal 0.80."""
        from congress_videos.modules.speaker_resolution import SPEAKER_RESOLUTION_MIN_CONFIDENCE

        assert SPEAKER_RESOLUTION_MIN_CONFIDENCE == 0.80


# ---------------------------------------------------------------------------
# resolve_speaker — core scenarios
# ---------------------------------------------------------------------------

class TestResolveSpeakerHappyPath:

    def test_resolve_speaker_happy_path(self):
        """Happy path: model returns valid slug >= 0.80 → returns dict."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0, end_seconds=500.0)
        participants = _make_participants(["pedro-sanchez"])
        # Intro window: [180, 300); turn window: [300, 360)
        blocks = _make_blocks([
            (200.0, 210.0, "Tiene la palabra el diputado"),
            (310.0, 320.0, "Señor presidente, el Gobierno..."),
        ])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"
        assert result["confidence"] >= 0.80
        assert "evidence" in result


class TestResolveSpeakerBelowConfidence:

    def test_resolve_speaker_below_confidence(self):
        """Model returns confidence 0.65 (< 0.80) → returns None."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.65)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerHallucinatedSlug:

    def test_resolve_speaker_hallucinated_slug(self):
        """Model returns slug not in participants list → returns None."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return _ok_completion("invented-politician-xyz", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerEmptyIntroWindowFallback:

    def test_resolve_speaker_empty_intro_window_fallback(self):
        """No SRT blocks before start_seconds → falls back to turn-only context; resolution attempted."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        # start_seconds = 10.0 so intro window [0-120, 10) is empty of real blocks
        turn = _make_turn(start_seconds=10.0, end_seconds=200.0)
        participants = _make_participants(["pedro-sanchez"])
        # Only blocks inside the turn window [10, 70)
        blocks = _make_blocks([
            (15.0, 25.0, "Señor presidente, comienzo mi intervención"),
        ])
        called = []

        def fake_completion(system, user, **kw):
            called.append(True)
            return _ok_completion("pedro-sanchez", 0.88)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        # Resolution must have been attempted (completion_fn called)
        assert len(called) == 1
        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"


class TestResolveSpeakerNoSrtFile:

    def test_resolve_speaker_no_srt_file(self):
        """SRT file absent → returns None without raising."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn()
        participants = _make_participants()

        with patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value=None):
            result = resolve_speaker(turn, participants)

        assert result is None


class TestResolveSpeakerCompletionRaises:

    def test_resolve_speaker_completion_raises(self):
        """completion_fn raises exception → returns None (never-raise)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            raise RuntimeError("OpenAI API error")

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerParseError:

    def test_resolve_speaker_parse_error(self):
        """completion_fn returns error field set → returns None."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return {"data": None, "error": "invalid json from model"}

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerConfidenceBoundary:

    def test_resolve_speaker_confidence_exactly_at_threshold(self):
        """confidence == 0.80 → returns dict (boundary inclusive)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.80)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert result["confidence"] == 0.80

    def test_resolve_speaker_confidence_just_below_threshold(self):
        """confidence == 0.79 → returns None."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.79)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerEmptyParticipants:

    def test_resolve_speaker_empty_participants_list(self):
        """participants=[] → returns None (no valid slug to validate against)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants: list = []
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


class TestResolveSpeakerSlugCaseSensitivity:

    def test_resolve_speaker_slug_case_sensitivity(self):
        """slug returned with wrong case vs participants → returns None."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, "Tiene la palabra")])

        def fake_completion(system, user, **kw):
            # Return uppercase — not in participants list
            return _ok_completion("PEDRO-SANCHEZ", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None
