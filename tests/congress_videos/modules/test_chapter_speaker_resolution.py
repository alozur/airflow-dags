"""Unit tests for congress_videos.modules.chapter_speaker_resolution (issue #263).

TDD RED cycle: all scenarios written before implementation. Covers the
resolver contract from the spec: roster validation, confidence gate,
never-raises behavior, and input-order preservation.
"""
from __future__ import annotations

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _make_participants(entries=None):
    """Return a roster list. Defaults to a small fixed set."""
    if entries is not None:
        return entries
    return [
        {"slug": "edurne-uriarte-bengoechea", "display_name": "Edurne Uriarte Bengoechea", "party": "PP"},
        {"slug": "pedro-sanchez", "display_name": "Pedro Sánchez", "party": "PSOE"},
        {"slug": "yolanda-diaz", "display_name": "Yolanda Díaz", "party": "SUMAR"},
    ]


def _stub_completion(matches, error=None, calls=None):
    """Build a completion_fn stub that records calls and returns fixed matches."""
    call_log = calls if calls is not None else []

    def _fn(system_prompt, user_prompt, **kwargs):
        call_log.append({"system_prompt": system_prompt, "user_prompt": user_prompt, "kwargs": kwargs})
        if error is not None:
            return {"data": None, "error": error}
        return {"data": {"matches": matches}, "error": None}

    return _fn


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

class TestConstants:
    def test_min_confidence_constant_value(self):
        from congress_videos.modules.chapter_speaker_resolution import CHAPTER_SPEAKER_MIN_CONFIDENCE

        assert CHAPTER_SPEAKER_MIN_CONFIDENCE == 0.80

    def test_max_mentions_per_call_constant_value(self):
        from congress_videos.modules.chapter_speaker_resolution import MAX_MENTIONS_PER_CALL

        assert MAX_MENTIONS_PER_CALL == 8


# ---------------------------------------------------------------------------
# Honorific / dirty name resolution (spec: Dirty honorific name scenario)
# ---------------------------------------------------------------------------

class TestDirtyHonorificResolution:
    def test_dirty_honorific_name_resolves_via_roster_search(self):
        """'Señora Uriarte Bengo Echea' resolves to the roster's validated slug."""
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        calls = []
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Señora Uriarte Bengo Echea",
                    "participant_slug": "edurne-uriarte-bengoechea",
                    "confidence": 0.91,
                    "evidence": "Matches Edurne Uriarte Bengoechea by surname.",
                }
            ],
            calls=calls,
        )

        result = resolve_chapter_speakers(
            ["Señora Uriarte Bengo Echea"], participants, completion_fn=completion_fn
        )

        assert result.primary is not None
        assert result.primary.participant_slug == "edurne-uriarte-bengoechea"
        assert result.primary.confidence >= 0.80
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# Hallucinated slug rejection
# ---------------------------------------------------------------------------

class TestHallucinatedSlugRejected:
    def test_slug_absent_from_roster_is_rejected(self):
        """A model-returned slug not present in the roster must not be accepted."""
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Alguien Inventado",
                    "participant_slug": "alguien-inventado-que-no-existe",
                    "confidence": 0.95,
                    "evidence": "made up",
                }
            ],
        )

        result = resolve_chapter_speakers(["Alguien Inventado"], participants, completion_fn=completion_fn)

        assert result.primary is None
        assert result.by_mention == {}


# ---------------------------------------------------------------------------
# Confidence gate — 0.79 rejected, 0.80 accepted
# ---------------------------------------------------------------------------

class TestConfidenceGate:
    def test_confidence_0_79_is_rejected(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Pedro Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.79,
                    "evidence": "close but not enough",
                }
            ],
        )

        result = resolve_chapter_speakers(["Pedro Sanchez"], participants, completion_fn=completion_fn)

        assert result.primary is None

    def test_confidence_0_80_is_accepted(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Pedro Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.80,
                    "evidence": "exact threshold",
                }
            ],
        )

        result = resolve_chapter_speakers(["Pedro Sanchez"], participants, completion_fn=completion_fn)

        assert result.primary is not None
        assert result.primary.participant_slug == "pedro-sanchez"


# ---------------------------------------------------------------------------
# Empty mentions / roster — no LLM call
# ---------------------------------------------------------------------------

class TestEmptyInputsSkipLLMCall:
    def test_empty_mentions_skips_llm_call(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        calls = []
        completion_fn = _stub_completion(matches=[], calls=calls)

        result = resolve_chapter_speakers([], _make_participants(), completion_fn=completion_fn)

        assert result.matches == ()
        assert calls == []

    def test_empty_roster_skips_llm_call(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        calls = []
        completion_fn = _stub_completion(matches=[], calls=calls)

        result = resolve_chapter_speakers(["Pedro Sanchez"], [], completion_fn=completion_fn)

        assert result.matches == ()
        assert calls == []


# ---------------------------------------------------------------------------
# Order = input order; display_name from roster not model
# ---------------------------------------------------------------------------

class TestOrderAndDisplayNameSource:
    def test_result_order_matches_input_order_not_model_order(self):
        """Model returns matches out of order; resolver output follows input order."""
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        # Model responds with Yolanda Díaz FIRST even though she was the
        # second mention in the input list.
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Yolanda Diaz",
                    "participant_slug": "yolanda-diaz",
                    "confidence": 0.90,
                    "evidence": "second speaker",
                },
                {
                    "mention": "Pedro Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.90,
                    "evidence": "first speaker",
                },
            ],
        )

        result = resolve_chapter_speakers(
            ["Pedro Sanchez", "Yolanda Diaz"], participants, completion_fn=completion_fn
        )

        assert [m.mention for m in result.matches] == ["Pedro Sanchez", "Yolanda Diaz"]
        assert result.primary.participant_slug == "pedro-sanchez"

    def test_display_name_comes_from_roster_not_model(self):
        """Even if the model echoes a different-looking name, display_name is the roster's."""
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Sr. Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.85,
                    # evidence field could carry a model-invented name; display_name must
                    # never be sourced from anything but the roster row.
                    "evidence": "Pedro Sanchez Perez Gonzalez",
                }
            ],
        )

        result = resolve_chapter_speakers(["Sr. Sanchez"], participants, completion_fn=completion_fn)

        assert result.primary.display_name == "Pedro Sánchez"


# ---------------------------------------------------------------------------
# Cap at MAX_MENTIONS_PER_CALL = 8
# ---------------------------------------------------------------------------

class TestMentionCap:
    def test_mentions_beyond_cap_are_not_sent_to_the_model(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = [
            {"slug": f"person-{i}", "display_name": f"Person {i}", "party": "X"}
            for i in range(10)
        ]
        mentions = [f"Person {i}" for i in range(10)]
        calls = []
        completion_fn = _stub_completion(matches=[], calls=calls)

        resolve_chapter_speakers(mentions, participants, completion_fn=completion_fn)

        assert len(calls) == 1
        user_prompt = calls[0]["user_prompt"]
        mention_section = user_prompt.split("KNOWN PARTICIPANTS")[0]
        for i in range(8):
            assert f"Person {i}" in mention_section
        assert "Person 8" not in mention_section
        assert "Person 9" not in mention_section


# ---------------------------------------------------------------------------
# completion_fn failure — never raises
# ---------------------------------------------------------------------------

class TestCompletionFailureNeverRaises:
    def test_completion_fn_raising_returns_empty_result(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        def _raising_completion(system_prompt, user_prompt, **kwargs):
            raise TimeoutError("upstream timed out")

        result = resolve_chapter_speakers(
            ["Pedro Sanchez"], _make_participants(), completion_fn=_raising_completion
        )

        assert result.matches == ()
        assert result.primary is None

    def test_completion_fn_error_field_returns_empty_result(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        completion_fn = _stub_completion(matches=[], error="rate_limited")

        result = resolve_chapter_speakers(
            ["Pedro Sanchez"], _make_participants(), completion_fn=completion_fn
        )

        assert result.matches == ()
        assert result.primary is None
