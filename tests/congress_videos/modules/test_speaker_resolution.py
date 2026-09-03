"""Unit tests for congress_videos.modules.speaker_resolution (issue #177).

TDD RED cycle: all 14 scenarios written before implementation.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

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
    return [{"slug": s, "display_name": s.replace("-", " ").title(), "party": "TEST"} for s in slugs]


def _make_blocks(blocks_data):
    """Build SRT block dicts from list of (start_secs, end_secs, text)."""
    return [{"start_secs": s, "end_secs": e, "text": t} for s, e, t in blocks_data]


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
        blocks = _make_blocks(
            [
                (200.0, 210.0, ANNOUNCEMENT_TEXT),
                (310.0, 320.0, "Señor presidente, el Gobierno..."),
            ]
        )

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
        blocks = _make_blocks(
            [
                (15.0, 25.0, "Tiene la palabra el señor Sánchez, comienzo mi intervención"),
            ]
        )
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


class TestResolveSpeakerCanonicalDir:
    """canonical_dir wiring (issue #340 slice 2): prefer the persisted
    per-chapter sidecar over the legacy downloads/ probes.

    ``find_srt_for_chapter`` itself already resolves preference-order and
    fallback behavior for a given ``canonical_dir`` (see
    TestFindSrtForChapterCanonical in test_srt_helpers.py); these tests
    verify the caller in speaker_resolution.py computes and passes the
    right value (or None) at the call site.
    """

    def test_canonical_dir_passed_when_chapter_id_truthy(self):
        """canonical wins: caller passes get_video_chapter_dir(video_id, chapter_id)."""
        from congress_videos.config.paths import get_video_chapter_dir
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(video_id="vidABC", chapter_id=10, session_date="2026-01-01")
        participants = _make_participants(["pedro-sanchez"])

        with patch(
            "congress_videos.modules.speaker_resolution.find_srt_for_chapter",
            return_value=None,
        ) as mock_find:
            result = resolve_speaker(turn, participants)

        expected_dir = str(get_video_chapter_dir("vidABC", 10))
        mock_find.assert_called_once_with("vidABC", 10, "2026-01-01", expected_dir)
        assert result is None

    def test_no_canonical_dir_when_chapter_id_falsy(self):
        """Legacy fallback unchanged: falsy chapter_id (site coerces to 0)
        → canonical_dir stays None, matching pre-change behavior."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(video_id="vidABC", chapter_id=0, session_date="2026-01-01")
        participants = _make_participants(["pedro-sanchez"])

        with patch(
            "congress_videos.modules.speaker_resolution.find_srt_for_chapter",
            return_value=None,
        ) as mock_find:
            result = resolve_speaker(turn, participants)

        mock_find.assert_called_once_with("vidABC", 0, "2026-01-01", None)
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
        blocks = _make_blocks(
            [
                (90.0, 95.0, "Tiene la palabra el señor diputado Sanchez."),
            ]
        )
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
        blocks = _make_blocks(
            [
                (410.0, 420.0, "Tiene la palabra el señor Sánchez, comienzo mi intervención"),
            ]
        )
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
        blocks = _make_blocks(
            [
                (200.0, 210.0, "El Gobierno presentará el proyecto de ley mañana."),
                (310.0, 320.0, "Continuamos con el siguiente punto del orden del día."),
            ]
        )
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
        blocks = _make_blocks(
            [
                (8870.00, 8934.683, "y por eso el grupo mantiene su posición"),
                (8934.683, 8934.763, "eh"),
                (8934.763, 8934.913, "no, no es así"),
            ]
        )
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
                "pedro-sanchez",
                0.99,
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


# ---------------------------------------------------------------------------
# _chapter_span (issue #322)
# ---------------------------------------------------------------------------


class TestChapterSpan:
    """_chapter_span parses turn['start_time']/['end_time'] (video_chapters
    VARCHAR SRT timestamps) into (start_seconds, end_seconds), or None."""

    @pytest.mark.parametrize(
        "start_time,end_time,expected",
        [
            ("00:10:00,000", "00:40:00,500", (600.0, 2400.5)),  # SRT comma format
            ("00:10:00", "00:40:00", (600.0, 2400.0)),  # no-millis format
            ("not-a-timestamp", "00:40:00", None),  # malformed
            ("00:40:00", "00:40:00", None),  # end == start
            ("00:40:01", "00:40:00", None),  # end < start
        ],
    )
    def test_parses_or_rejects(self, start_time, end_time, expected):
        from congress_videos.modules.speaker_resolution import _chapter_span

        turn = _make_turn()
        turn["start_time"] = start_time
        turn["end_time"] = end_time

        assert _chapter_span(turn) == expected

    def test_missing_keys_returns_none(self):
        from congress_videos.modules.speaker_resolution import _chapter_span

        assert _chapter_span(_make_turn()) is None

    def test_none_values_returns_none(self):
        from congress_videos.modules.speaker_resolution import _chapter_span

        turn = _make_turn()
        turn["start_time"] = None
        turn["end_time"] = None

        assert _chapter_span(turn) is None


# ---------------------------------------------------------------------------
# _evidence_supported_in_blocks (issue #322) — sliding-join anchored gate
# ---------------------------------------------------------------------------


class TestEvidenceSupportedInBlocks:
    """Locates evidence across a sliding join of up to ANCHOR_JOIN_BLOCKS=3
    consecutive blocks, reusing _evidence_supported verbatim per join.
    Ratios pinned against real rapidfuzz output with >5pt margin either
    side of EVIDENCE_MIN_PARTIAL_RATIO=85."""

    def test_empty_blocks_returns_false(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported_in_blocks

        assert _evidence_supported_in_blocks("Some verbatim evidence quote", []) is False

    def test_twelve_char_floor_still_enforced(self):
        from congress_videos.modules.speaker_resolution import _evidence_supported_in_blocks

        blocks = _make_blocks([(0.0, 5.0, "Tiene la palabra el señor Sánchez, comienza")])

        assert _evidence_supported_in_blocks("short", blocks) is False

    def test_straddle_two_blocks_accepted(self):
        """Evidence spans the tail of block 0 + head of block 1; neither
        block alone reaches 85, the 2-block join does."""
        from congress_videos.modules.speaker_resolution import _evidence_supported_in_blocks

        b0 = "Comparece hoy ante esta Camara el senor Ministro de Hacienda para explicar"
        b1 = "las nuevas medidas fiscales aprobadas por el Consejo de Ministros la semana pasada"
        evidence = "Ministro de Hacienda para explicar las nuevas medidas fiscales aprobadas"
        blocks = _make_blocks([(0.0, 5.0, b0), (5.0, 10.0, b1)])

        assert _evidence_supported_in_blocks(evidence, blocks[:1]) is False
        assert _evidence_supported_in_blocks(evidence, blocks[1:]) is False
        assert _evidence_supported_in_blocks(evidence, blocks) is True

    def test_straddle_three_blocks_accepted(self):
        """Evidence spans three consecutive blocks; none alone reaches 85,
        the 3-block join (default join_size=ANCHOR_JOIN_BLOCKS) does."""
        from congress_videos.modules.speaker_resolution import _evidence_supported_in_blocks

        r0 = "valora con gran detalle"
        r1 = "el nuevo decreto aprobado recientemente por unanimidad y consenso"
        r2 = "por el consejo de ministros"
        evidence = f"{r0} {r1} {r2}"
        blocks = _make_blocks(
            [
                (0.0, 5.0, f"Interviene la portavoz socialista para {r0}"),
                (5.0, 10.0, f"eh pues bueno la verdad es que {r1}"),
                (10.0, 15.0, f"{r2} este mismo lunes por la tarde noche"),
            ]
        )

        for window in (blocks[:1], blocks[1:2], blocks[2:3]):
            assert _evidence_supported_in_blocks(evidence, window) is False
        assert _evidence_supported_in_blocks(evidence, blocks) is True

    def test_span_four_blocks_rejected(self):
        """Evidence needs a 4th block; with default join_size=3 no 1..3
        window reconstructs enough of it to pass."""
        from congress_videos.modules.speaker_resolution import _evidence_supported_in_blocks

        r0, r1, r2, r3 = (
            "valora con gran detalle",
            "el nuevo decreto aprobado",
            "por el consejo de ministros",
            "sobre vivienda social urbana",
        )
        evidence = f"{r0} {r1} {r2} {r3}"
        blocks = _make_blocks(
            [
                (0.0, 5.0, f"Interviene la portavoz socialista para {r0}"),
                (5.0, 10.0, f"eh pues bueno {r1}"),
                (10.0, 15.0, f"la verdad es que {r2}"),
                (15.0, 20.0, f"{r3} segun confirmaron fuentes del propio ministerio"),
            ]
        )

        assert _evidence_supported_in_blocks(evidence, blocks, join_size=3) is False


# ---------------------------------------------------------------------------
# Anchored evidence gate — resolve_speaker integration (issue #322)
# ---------------------------------------------------------------------------

_PREGATE_BLOCK = (900.0, 905.0, ANNOUNCEMENT_TEXT)  # inside intro window [880,1000)


def _run_anchored_gate_case(evidence_offset, confidence=0.95, turn_type=None):
    """Shared harness: turn anchored at start_seconds=1000, pre-gate block
    inside the narrow intro window, evidence block at anchor+evidence_offset."""
    from congress_videos.modules.speaker_resolution import resolve_speaker

    turn = _make_turn(start_seconds=1000.0)
    if turn_type is not None:
        turn["turn_type"] = turn_type
    participants = _make_participants(["pedro-sanchez"])
    unique_evidence = "Comparece hoy el ministro de Hacienda ante la prensa"
    blocks = _make_blocks(
        [
            _PREGATE_BLOCK,
            (1000.0 + evidence_offset, 1005.0 + evidence_offset, unique_evidence),
        ]
    )

    def fake_completion(system, user, **kw):
        return _ok_completion("pedro-sanchez", confidence, evidence=unique_evidence)

    with (
        patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
        patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
    ):
        return resolve_speaker(turn, participants, completion_fn=fake_completion)


class TestAnchoredEvidenceGateIntegration:
    """resolve_speaker-level proof the region_blocks filter wires
    _evidence_supported_in_blocks: default 600s lookback, unchanged forward
    edge, and chapter-start clamp."""

    def test_evidence_at_anchor_minus_700_rejected(self):
        """700s back is outside the default 600s lookback."""
        assert _run_anchored_gate_case(evidence_offset=-700) is None

    def test_evidence_at_anchor_minus_500_accepted(self):
        """500s back is within the default 600s lookback."""
        result = _run_anchored_gate_case(evidence_offset=-500)
        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"

    def test_evidence_at_start_plus_90_rejected_even_at_high_confidence(self):
        """Past the forward edge (start + TURN_CONTEXT_SECS=60) — rejected
        even at confidence 0.99."""
        assert _run_anchored_gate_case(evidence_offset=90, confidence=0.99) is None

    def test_lookback_constant_is_live_tunable(self, monkeypatch):
        """QA_EVIDENCE_LOOKBACK_SECS is read at call time, not baked in:
        narrowing it to 100s rejects the same -500s evidence the 600s
        default accepts (see test_evidence_at_anchor_minus_500_accepted)."""
        import congress_videos.modules.speaker_resolution as sr

        monkeypatch.setattr(sr, "QA_EVIDENCE_LOOKBACK_SECS", 100)
        assert _run_anchored_gate_case(evidence_offset=-500) is None

    @pytest.mark.parametrize("turn_type", ["monologue", "qa"])
    def test_gate_uniform_across_turn_types(self, turn_type):
        """The anchored evidence gate has no turn_type branch: in-region
        evidence resolves for monologue and qa alike."""
        result = _run_anchored_gate_case(evidence_offset=-500, turn_type=turn_type)
        assert result is not None
        assert result["participant_slug"] == "pedro-sanchez"

    def test_chapter_start_clamp_blocks_pre_chapter_pickup(self):
        """A chapter starting at 900s clamps the lookback's backward edge
        past what 500s-back would otherwise allow."""
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=1000.0)
        turn["start_time"] = "00:15:00"  # chapter starts at 900s
        turn["end_time"] = "00:35:00"
        participants = _make_participants(["pedro-sanchez"])
        unique_evidence = "Comparece hoy el ministro de Hacienda ante la prensa"
        blocks = _make_blocks([_PREGATE_BLOCK, (500.0, 505.0, unique_evidence)])

        def fake_completion(system, user, **kw):
            return _ok_completion("pedro-sanchez", 0.95, evidence=unique_evidence)

        with (
            patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
            patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
        ):
            result = resolve_speaker(turn, participants, completion_fn=fake_completion)

        assert result is None


# ---------------------------------------------------------------------------
# Announcement pre-gate stays unchanged in slice 1 (issue #322)
# ---------------------------------------------------------------------------


class TestPreGateUnchangedSlice1:
    """Slice 1 only anchors the EVIDENCE gate; the pre-gate keeps reading
    the narrow intro+turn text for every turn_type (D4's rebind is
    slice-2/qa-only) — proves slice 1 does not widen which turns reach the
    LLM, for monologue AND qa turn types alike."""

    @pytest.mark.parametrize("turn_type", ["monologue", "qa", None])
    def test_still_vetoed_announcement_300s_back(self, turn_type):
        from congress_videos.modules.speaker_resolution import resolve_speaker

        turn = _make_turn(start_seconds=1000.0)
        if turn_type is not None:
            turn["turn_type"] = turn_type
        participants = _make_participants(["pedro-sanchez"])
        # anchor=1000; announcement at anchor-300=700 sits inside the
        # widened evidence region ([400,1060)) but outside the narrow
        # intro [880,1000) and turn [1000,1060) windows the pre-gate reads.
        blocks = _make_blocks(
            [
                (700.0, 705.0, ANNOUNCEMENT_TEXT),
                (900.0, 905.0, "El Gobierno remite el informe correspondiente."),
                (1010.0, 1015.0, "Continuamos con el siguiente punto del orden del dia."),
            ]
        )
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
# Evidence-aware resolution prompt (issue #284)
# ---------------------------------------------------------------------------


class TestSystemPromptEvidenceRule:
    """SPEAKER_RESOLUTION_SYSTEM_PROMPT must instruct the model that
    evidence must be a verbatim quote, and that unverifiable answers must
    return a null slug rather than fabricate a name."""

    def test_prompt_requires_verbatim_evidence(self):
        from congress_videos.config.ai_prompts import SPEAKER_RESOLUTION_SYSTEM_PROMPT

        assert "verbatim" in SPEAKER_RESOLUTION_SYSTEM_PROMPT.lower()

    def test_prompt_instructs_null_over_fabrication(self):
        from congress_videos.config.ai_prompts import SPEAKER_RESOLUTION_SYSTEM_PROMPT

        prompt_lower = SPEAKER_RESOLUTION_SYSTEM_PROMPT.lower()
        assert "null" in prompt_lower
        assert "fabricat" in prompt_lower


# ---------------------------------------------------------------------------
# qa-gated wide prompt context (issue #322, slice 2)
# ---------------------------------------------------------------------------


class TestWideUserTemplate:
    """SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE (D6): tail from INTRO WINDOW
    onward is byte-identical to the narrow template; a new {chapter_text}
    section renders before it."""

    def test_chapter_text_prepended_tail_identical_to_narrow_template(self):
        from congress_videos.config.ai_prompts import (
            SPEAKER_RESOLUTION_USER_TEMPLATE,
            SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE,
        )

        marker = "INTRO WINDOW"
        wide_tail = SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE[SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE.index(marker) :]
        narrow_tail = SPEAKER_RESOLUTION_USER_TEMPLATE[SPEAKER_RESOLUTION_USER_TEMPLATE.index(marker) :]
        assert wide_tail == narrow_tail

        rendered = SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE.format(
            chapter_text="[00:10:00] Sample chapter text",
            intro_text="intro sample",
            turn_text="turn sample",
            participant_roster="slug | Name | Party",
        )
        assert rendered.index("Sample chapter text") < rendered.index("intro sample")


class TestNonQaPromptUnchanged:
    """Approval test (issue #322 D4): non-qa turn_type keeps today's narrow
    SPEAKER_RESOLUTION_USER_TEMPLATE prompt, byte-identical, both BEFORE and
    AFTER the qa-gated wide-context branch is wired into the resolver.
    Uses the _run_qa_case harness defined below (resolved at call time)."""

    @pytest.mark.parametrize("turn_type", ["monologue", None])
    def test_narrow_prompt_byte_identical_for_non_qa(self, turn_type):
        from congress_videos.config.ai_prompts import SPEAKER_RESOLUTION_USER_TEMPLATE

        _, user, _ = _run_qa_case(
            [(900.0, 905.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
            turn_type=turn_type,
        )

        expected_prompt = SPEAKER_RESOLUTION_USER_TEMPLATE.format(
            intro_text=ANNOUNCEMENT_TEXT,
            turn_text=_TURN_EVIDENCE_TEXT,
            participant_roster="pedro-sanchez | Pedro Sanchez | TEST",
        )
        assert user == expected_prompt


# ---------------------------------------------------------------------------
# _build_qa_chapter_text (issue #322, D7) — head+tail hybrid truncation
# ---------------------------------------------------------------------------


class TestBuildQaChapterText:
    """Pure function: renders '[HH:MM:SS] text' per block, joined by '\\n'.
    Passthrough under QA_CONTEXT_MAX_CHARS; block-granular head+tail hybrid
    (always keeping the first and last block) joined by QA_TRUNCATION_MARKER
    when over budget. Never raises."""

    def test_empty_and_short_text_pass_through_unchanged(self):
        from congress_videos.modules import speaker_resolution as sr

        assert sr._build_qa_chapter_text([]) == ""

        blocks = _make_blocks([(0.0, 5.0, "hola"), (5.0, 10.0, "mundo")])
        result = sr._build_qa_chapter_text(blocks)
        assert result == "[00:00:00] hola\n[00:00:05] mundo"
        assert sr.QA_TRUNCATION_MARKER not in result

    def test_truncation_keeps_head_and_tail_dropping_the_middle(self, monkeypatch):
        """MAX/HEAD small enough that a single block already exceeds
        HEAD_CHARS — the first block is still always kept (block-granular,
        never mid-block), same for the last block in the tail."""
        from congress_videos.modules import speaker_resolution as sr

        monkeypatch.setattr(sr, "QA_CONTEXT_MAX_CHARS", 200)
        monkeypatch.setattr(sr, "QA_CONTEXT_HEAD_CHARS", 40)
        blocks = _make_blocks(
            [(i * 10.0, i * 10.0 + 5.0, f"bloque numero {i} con texto de relleno adicional") for i in range(10)]
        )

        result = sr._build_qa_chapter_text(blocks)

        assert result.startswith("[00:00:00] bloque numero 0")
        assert result.rstrip().endswith("bloque numero 9 con texto de relleno adicional")
        assert sr.QA_TRUNCATION_MARKER in result
        assert "bloque numero 5 con texto de relleno adicional" not in result


# ---------------------------------------------------------------------------
# qa-gated wide prompt context wiring (issue #322, D1/D7/D8) —
# resolve_speaker integration
# ---------------------------------------------------------------------------

_FAR_BACK_ANNOUNCEMENT = "Tiene la palabra el señor Sánchez, portavoz del grupo."
_TURN_EVIDENCE_TEXT = "Comienzo mi intervencion en el turno de preguntas."


def _run_qa_case(
    blocks_data,
    turn_type="qa",
    start_time="00:10:00,000",  # chapter starts at 600s
    end_time="00:40:00,000",  # chapter ends at 2400s
    wide_enabled=True,
):
    """Shared harness for qa-gated wide-context cases (issue #322, D1/D4/D7):
    turn anchored at start_seconds=1000 (region [600,1060) by default).
    Returns (result, captured_user_prompt_or_None, llm_call_count)."""
    from congress_videos.modules import speaker_resolution as sr

    turn = _make_turn(start_seconds=1000.0)
    if turn_type is not None:
        turn["turn_type"] = turn_type
    if start_time is not None:
        turn["start_time"] = start_time
    if end_time is not None:
        turn["end_time"] = end_time
    participants = _make_participants(["pedro-sanchez"])
    blocks = _make_blocks(blocks_data)
    captured = {}
    call_count = []

    def fake_completion(system, user, **kw):
        call_count.append(1)
        captured["user"] = user
        return _ok_completion("pedro-sanchez", 0.95, evidence=_TURN_EVIDENCE_TEXT)

    with (
        patch.object(sr, "QA_WIDE_CONTEXT_ENABLED", wide_enabled),
        patch("congress_videos.modules.speaker_resolution.find_srt_for_chapter", return_value="/fake/src.srt"),
        patch("congress_videos.modules.speaker_resolution._parse_srt_blocks", return_value=blocks),
    ):
        result = sr.resolve_speaker(turn, participants, completion_fn=fake_completion)

    return result, captured.get("user"), len(call_count)


class TestQaGatedWideContext:
    """turn_type == 'qa' + a parseable chapter span widens BOTH the prompt
    and the announcement pre-gate in lockstep (D1/D4/D7), dropping text
    at/after the forward edge; unparseable spans fail back to narrow,
    logging loudly (D7)."""

    def test_qa_turn_widens_prompt_and_pre_gate_dropping_forward_edge(self):
        future_text = "Este texto pertenece a un turno futuro fuera de la region."
        # Announcement only at anchor-300=700s: inside chapter/region,
        # OUTSIDE the narrow intro window [880,1000) the pre-gate used to read.
        result, user, call_count = _run_qa_case(
            [
                (700.0, 705.0, _FAR_BACK_ANNOUNCEMENT),
                (1000.0, 1005.0, _TURN_EVIDENCE_TEXT),
                (1200.0, 1205.0, future_text),  # >= region_end (1060), still inside chapter span
            ]
        )

        assert call_count == 1  # pre-gate widened too (D4) — LLM reached
        assert result is not None
        assert "CHAPTER TRANSCRIPT" in user  # proves the wide branch ran
        assert _FAR_BACK_ANNOUNCEMENT in user  # widening reached the announcement
        assert future_text not in user  # forward-edge drop

    def test_qa_turn_unparseable_chapter_span_falls_back_to_narrow(self, caplog):
        """Unparseable start_time collapses prompt AND pre-gate to narrow
        together (D4/D7): an announcement outside the narrow window is
        vetoed (LLM never reached); one inside it still resolves, but with
        the narrow (non-CHAPTER-TRANSCRIPT) prompt."""
        with caplog.at_level("WARNING"):
            vetoed, _, vetoed_calls = _run_qa_case(
                [(700.0, 705.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
                start_time="not-a-timestamp",
            )
        assert vetoed is None
        assert vetoed_calls == 0
        assert any("chapter span" in rec.message.lower() for rec in caplog.records)

        result, user, _ = _run_qa_case(
            [(900.0, 905.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
            start_time="not-a-timestamp",
        )
        assert result is not None
        assert "CHAPTER TRANSCRIPT" not in user


class TestD4PreGateRebind:
    """has_announcement_phrase reads the SAME text as the prompt; every
    non-qa turn_type stays vetoed on the narrow window it always used."""

    @pytest.mark.parametrize("turn_type", ["monologue", None])
    def test_non_qa_pre_gate_still_vetoed(self, turn_type):
        result, _, call_count = _run_qa_case(
            [(700.0, 705.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
            turn_type=turn_type,
        )

        assert result is None
        assert call_count == 0


class TestD4FailSafeCollapse:
    """QA_WIDE_CONTEXT_ENABLED=False collapses the qa pre-gate back to
    narrow combined_text, in lockstep with the prompt — never
    independently (the unparseable-chapter-span fail-safe is covered by
    TestQaGatedWideContext's collapse test above)."""

    def test_kill_switch_disabled_qa_pre_gate_reverts_to_narrow(self):
        # Would pass the WIDE pre-gate (anchor-300s) but must be vetoed once
        # the kill switch collapses the prompt back to narrow.
        result, _, call_count = _run_qa_case(
            [(700.0, 705.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
            wide_enabled=False,
        )

        assert result is None
        assert call_count == 0

    def test_kill_switch_disabled_uses_narrow_prompt_when_announcement_in_narrow_window(self):
        """Kill-switch off + qa turn_type + a valid chapter span: the LLM is
        still reached (announcement is in the narrow window too), but the
        prompt sent must be the narrow template, not the wide one."""
        result, user, _ = _run_qa_case(
            [(900.0, 905.0, ANNOUNCEMENT_TEXT), (1000.0, 1005.0, _TURN_EVIDENCE_TEXT)],
            wide_enabled=False,
        )

        assert result is not None
        assert "CHAPTER TRANSCRIPT" not in user
