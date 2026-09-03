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

        result = resolve_chapter_speakers(["Señora Uriarte Bengo Echea"], participants, completion_fn=completion_fn)

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

    def test_confidence_0_0_parses_as_a_real_value_then_is_rejected(self):
        """A structurally valid 0.0 confidence must parse (not fail parsing)
        and then be rejected by the 0.80 gate — never collapsed with a
        missing/non-numeric confidence (issue #272 slice 2, D1)."""
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        participants = _make_participants()
        completion_fn = _stub_completion(
            matches=[
                {
                    "mention": "Pedro Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.0,
                    "evidence": "structurally valid zero, not a parse failure",
                }
            ],
        )

        result = resolve_chapter_speakers(["Pedro Sanchez"], participants, completion_fn=completion_fn)

        assert result.primary is None


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

        result = resolve_chapter_speakers(["Pedro Sanchez", "Yolanda Diaz"], participants, completion_fn=completion_fn)

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

        participants = [{"slug": f"person-{i}", "display_name": f"Person {i}", "party": "X"} for i in range(10)]
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

        result = resolve_chapter_speakers(["Pedro Sanchez"], _make_participants(), completion_fn=_raising_completion)

        assert result.matches == ()
        assert result.primary is None

    def test_completion_fn_error_field_returns_empty_result(self):
        from congress_videos.modules.chapter_speaker_resolution import resolve_chapter_speakers

        completion_fn = _stub_completion(matches=[], error="rate_limited")

        result = resolve_chapter_speakers(["Pedro Sanchez"], _make_participants(), completion_fn=completion_fn)

        assert result.matches == ()
        assert result.primary is None


# ---------------------------------------------------------------------------
# _parse_confidence (issue #272 slice 2: extracted from _resolve_inner to
# reduce cyclomatic complexity)
# ---------------------------------------------------------------------------


class TestParseConfidence:
    """_parse_confidence(value) -> float | None; None on parse failure only."""

    def test_float_zero_parses_to_float_zero(self):
        """A structurally valid 0.0 must parse, never collapse with failure."""
        from congress_videos.modules.chapter_speaker_resolution import _parse_confidence

        result = _parse_confidence(0.0)

        assert result == 0.0
        assert result is not None

    def test_string_zero_parses_to_float_zero(self):
        """A numeric string '0.0' parses to the float 0.0."""
        from congress_videos.modules.chapter_speaker_resolution import _parse_confidence

        assert _parse_confidence("0.0") == 0.0

    def test_none_value_returns_none(self):
        """A missing confidence (None) returns the None sentinel."""
        from congress_videos.modules.chapter_speaker_resolution import _parse_confidence

        assert _parse_confidence(None) is None

    def test_non_numeric_string_returns_none(self):
        """A non-numeric string ('abc') returns the None sentinel."""
        from congress_videos.modules.chapter_speaker_resolution import _parse_confidence

        assert _parse_confidence("abc") is None


# ---------------------------------------------------------------------------
# _first_raw_match_by_mention (issue #272 slice 2)
# ---------------------------------------------------------------------------


class TestFirstRawMatchByMention:
    """_first_raw_match_by_mention(raw_matches, capped_mentions) -> dict[str, dict]."""

    def test_keeps_first_entry_per_mention(self):
        """When the model echoes the same mention twice, the first entry wins."""
        from congress_videos.modules.chapter_speaker_resolution import (
            _first_raw_match_by_mention,
        )

        raw_matches = [
            {"mention": "Pedro Sanchez", "participant_slug": "pedro-sanchez", "confidence": 0.9},
            {"mention": "Pedro Sanchez", "participant_slug": "someone-else", "confidence": 0.5},
        ]

        result = _first_raw_match_by_mention(raw_matches, ["Pedro Sanchez"])

        assert result["Pedro Sanchez"]["participant_slug"] == "pedro-sanchez"

    def test_ignores_entries_for_mentions_not_asked_about(self):
        """A hallucinated mention absent from capped_mentions is dropped."""
        from congress_videos.modules.chapter_speaker_resolution import (
            _first_raw_match_by_mention,
        )

        raw_matches = [
            {"mention": "Alguien Inventado", "participant_slug": "x", "confidence": 0.9},
        ]

        result = _first_raw_match_by_mention(raw_matches, ["Pedro Sanchez"])

        assert result == {}

    def test_empty_raw_matches_returns_empty_dict(self):
        """No raw matches at all -> empty dict."""
        from congress_videos.modules.chapter_speaker_resolution import (
            _first_raw_match_by_mention,
        )

        assert _first_raw_match_by_mention([], ["Pedro Sanchez"]) == {}


# ---------------------------------------------------------------------------
# _accept_matches (issue #272 slice 2) — mutation-kill on the confidence
# sentinel guard (spec: "Confidence parsing preserves the 0.0-vs-parse-
# failure distinction")
# ---------------------------------------------------------------------------


class TestAcceptMatchesConfidenceSentinel:
    def test_zero_confidence_logs_below_threshold_never_invalid(self, caplog):
        """A 0.0 confidence must be logged as 'below threshold', never as
        'invalid confidence' — this kills a mutant that replaces the `is
        None` sentinel guard with a truthiness check (`if not confidence`),
        which would misclassify 0.0 as a parse failure."""
        import logging

        from congress_videos.modules.chapter_speaker_resolution import _accept_matches

        roster_by_slug = {"pedro-sanchez": {"slug": "pedro-sanchez", "display_name": "Pedro Sánchez"}}
        raw_by_mention = {
            "Pedro Sanchez": {
                "mention": "Pedro Sanchez",
                "participant_slug": "pedro-sanchez",
                "confidence": 0.0,
                "evidence": "structurally valid zero",
            }
        }

        with caplog.at_level(logging.DEBUG):
            matches, by_mention = _accept_matches(["Pedro Sanchez"], raw_by_mention, roster_by_slug)

        assert matches == []
        assert by_mention == {}
        assert "confidence 0.00 < 0.80" in caplog.text
        assert "invalid confidence" not in caplog.text
