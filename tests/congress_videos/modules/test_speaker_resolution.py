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


def _ok_completion(
    slug: str,
    confidence: float = 0.92,
    evidence: str = "Tiene la palabra el señor Sánchez",
):
    """Return a cached_json_completion-style result for a happy-path resolution.

    The default evidence is a verbatim substring of ANNOUNCEMENT_TEXT (see
    below) — issue #284's evidence-verification gate rejects any candidate
    whose evidence cannot be located in the model-visible text, so fixtures
    that use the default block text must keep this string locatable.
    """
    return {
        "data": {
            "participant_slug": slug,
            "confidence": confidence,
            "evidence": evidence,
        },
        "error": None,
    }


# Canonical announcement text (issue #284): matches RE_NAMED (the pre-gate)
# AND is the source _ok_completion's default evidence is a verbatim
# substring of, so re-fixtured blocks below satisfy both the pre-gate and
# the evidence-verification check without per-test overrides.
ANNOUNCEMENT_TEXT = "Tiene la palabra el señor Sánchez."


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
            (200.0, 210.0, ANNOUNCEMENT_TEXT),
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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        # Only blocks inside the turn window [10, 70) — must still carry an
        # announcement phrase somewhere in intro+turn text for the pre-gate.
        blocks = _make_blocks([
            (15.0, 25.0, "Tiene la palabra el señor Sánchez, comienzo mi intervención"),
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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

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
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            # Return uppercase — not in participants list
            return _ok_completion("PEDRO-SANCHEZ", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


# ---------------------------------------------------------------------------
# Rule 3 (issue #283) — intro-window anchor on group_start_seconds
# ---------------------------------------------------------------------------

class TestIntroWindowAnchor:

    def test_group_start_seconds_anchors_intro_window(self):
        """group_start_seconds anchors the 120s intro look-back window, not
        the representative turn's own start_seconds (issue #283 rule 3)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=400.0)
        turn["group_start_seconds"] = 100.0
        participants = _make_participants(["pedro-sanchez"])
        # Block near t=95 is inside the NEW window [max(0,100-120),100)=[0,100)
        # but far outside the OLD window [400-120,400)=[280,400). "señor" is
        # inserted so the block also satisfies the announcement pre-gate.
        blocks = _make_blocks([
            (90.0, 95.0, "Tiene la palabra el señor diputado Sanchez."),
        ])
        captured = {}

        def fake_completion(system, user, **kw):
            captured["user"] = user
            return _ok_completion("pedro-sanchez", 0.95, evidence="diputado Sanchez")

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert "diputado Sanchez" in captured["user"]

    def test_missing_group_start_seconds_key_behaves_as_today(self):
        """No group_start_seconds key -> identical to pre-fix behaviour
        (anchors on start_seconds), matching every existing call site."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        assert "group_start_seconds" not in turn
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"

    def test_group_start_seconds_none_falls_back_to_start_seconds(self):
        """group_start_seconds present but None -> falls back to
        start_seconds (explicit `is not None` check, not a truthy `or`, so a
        legitimate 0.0 would be honoured too)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        turn["group_start_seconds"] = None
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"

    def test_forward_turn_context_window_stays_pinned_to_start_seconds(self):
        """The forward turn-context window remains anchored on the
        representative turn's own start_seconds/end_seconds, unaffected by
        the group_start_seconds intro-window change."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=400.0, end_seconds=600.0)
        turn["group_start_seconds"] = 100.0
        participants = _make_participants(["pedro-sanchez"])
        # Block at start_seconds + 10 must remain in the turn-context window.
        blocks = _make_blocks([
            (410.0, 420.0, "Tiene la palabra el señor Sánchez, comienzo mi intervención"),
        ])
        captured = {}

        def fake_completion(system, user, **kw):
            captured["user"] = user
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert "comienzo mi intervención" in captured["user"]


# ---------------------------------------------------------------------------
# Announcement pre-gate (issue #284)
# ---------------------------------------------------------------------------

class TestAnnouncementPreGate:
    """resolve_speaker MUST call has_announcement_phrase against the
    combined intro_text + turn_text BEFORE invoking completion_fn."""

    def test_no_phrase_anywhere_skips_the_llm(self):
        """intro_text and turn_text both lack any announcement pattern ->
        resolve_speaker returns None and completion_fn is never invoked."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([
            (200.0, 210.0, "El Gobierno presentará el proyecto de ley mañana."),
            (310.0, 320.0, "Continuamos con el siguiente punto del orden del día."),
        ])
        call_count = []

        def fake_completion(system, user, **kw):
            call_count.append(1)
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None
        assert len(call_count) == 0

    def test_announcement_present_still_invokes_the_llm(self):
        """A matching phrase anywhere in intro/turn text -> completion_fn is
        still called exactly as before (the pre-gate only blocks absence)."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])
        call_count = []

        def fake_completion(system, user, **kw):
            call_count.append(1)
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert len(call_count) == 1

    def test_turn_205_style_window_with_no_announcement_resolves_nothing(self):
        """Regression: a turn-205-shaped window (sub-second diarization-blip
        text, no presidential announcement anywhere in the visible text)
        must resolve to None without a completion_fn call."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=8934.913, end_seconds=8934.930)
        participants = _make_participants(["pedro-sanchez"])
        # Diarization-blip text fragments — ordinary speech, never an
        # announcement phrase (same shape as the #283 chapter-263 blips).
        blocks = _make_blocks([
            (8870.00, 8934.683, "y por eso el grupo mantiene su posición"),
            (8934.683, 8934.763, "eh"),
            (8934.763, 8934.913, "no, no es así"),
        ])
        call_count = []

        def fake_completion(system, user, **kw):
            call_count.append(1)
            return _ok_completion("pedro-sanchez", 0.95)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None
        assert len(call_count) == 0


# ---------------------------------------------------------------------------
# Evidence verification (issue #284)
# ---------------------------------------------------------------------------

class TestEvidenceVerification:
    """resolve_speaker independently verifies completion_fn's evidence
    against the model-visible text, regardless of self-reported confidence."""

    def test_absent_evidence_rejected_even_at_high_confidence(self):
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return {
                "data": {"participant_slug": "pedro-sanchez", "confidence": 0.99, "evidence": None},
                "error": None,
            }

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None

    def test_whitespace_only_evidence_rejected(self):
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.99, evidence="   ")

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None

    def test_evidence_under_12_chars_rejected(self):
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            # "Tiene la p" is 10 normalized chars — under the 12-char floor
            # even though it IS a verbatim substring of the block text.
            return _ok_completion("pedro-sanchez", 0.99, evidence="Tiene la p")

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None

    def test_fabricated_evidence_rejected_even_at_high_confidence(self):
        """Evidence describing something not present in the visible text ->
        None, regardless of the model's self-reported confidence."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return _ok_completion(
                "pedro-sanchez", 0.99,
                evidence="El orador afirmó categóricamente que la economía mejorará",
            )

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None

    def test_accent_and_case_only_drift_accepted(self):
        """Evidence differing only by accents/casing from the source text
        must still be accepted — normalization tolerates this drift."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=300.0)
        participants = _make_participants(["pedro-sanchez"])
        blocks = _make_blocks([(200.0, 210.0, ANNOUNCEMENT_TEXT)])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95, evidence="SEÑOR SÁNCHEZ")

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"


class TestEvidenceSupportedBoundary:
    """Direct unit tests of _evidence_supported (issue #284) — pins the
    exact partial_ratio=85 boundary computed against a fixed source text."""

    SOURCE_TEXT = "Tiene la palabra el señor Sánchez, muchas gracias señoría"

    def test_ratio_just_above_threshold_is_accepted(self):
        """partial_ratio(norm(evidence), norm(SOURCE_TEXT)) == 86.2 (>= 85)."""
        from congress_videos.modules.speaker_resolution import _evidence_supported

        evidence = "Tiene la palabra el señorXXXXXXXX"
        assert _evidence_supported(evidence, self.SOURCE_TEXT) is True

    def test_ratio_just_below_threshold_is_rejected(self):
        """partial_ratio(norm(evidence), norm(SOURCE_TEXT)) == 84.2 (< 85)."""
        from congress_videos.modules.speaker_resolution import _evidence_supported

        evidence = "Tiene la palabra el señoXXXXXXXXX"
        assert _evidence_supported(evidence, self.SOURCE_TEXT) is False

    def test_none_evidence_rejected(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported(None, self.SOURCE_TEXT) is False

    def test_empty_string_evidence_rejected(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported("", self.SOURCE_TEXT) is False

    def test_whitespace_only_evidence_rejected(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported("   \n\t  ", self.SOURCE_TEXT) is False

    def test_evidence_under_12_normalized_chars_rejected(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported("short", self.SOURCE_TEXT) is False

    def test_verbatim_substring_accepted(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported("señor Sánchez, muchas gracias", self.SOURCE_TEXT) is True

    def test_non_str_evidence_rejected(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported

        assert _evidence_supported(["not", "a", "string"], self.SOURCE_TEXT) is False
