"""Unit tests for congress_videos.modules.mentioned_people_resolution (issue #432).

TDD RED cycle: all scenarios written before implementation. Covers the
resolver contract from the design/spec: roster validation, confidence gate,
dedup + cap, malformed-output handling, never-raises behavior, and the
speaker/mentioned prompt distinction.
"""

from __future__ import annotations

import pytest

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


def _stub_completion(mentions, error=None, calls=None):
    """Build a completion_fn stub that records calls and returns fixed mentions."""
    call_log = calls if calls is not None else []

    def _fn(system_prompt, user_prompt, **kwargs):
        call_log.append({"system_prompt": system_prompt, "user_prompt": user_prompt, "kwargs": kwargs})
        if error is not None:
            return {"data": None, "error": error}
        return {"data": {"mentions": mentions}, "error": None}

    return _fn


# ---------------------------------------------------------------------------
# Empty inputs — no LLM call
# ---------------------------------------------------------------------------


class TestEmptyInputsSkipLLMCall:
    def test_returns_empty_and_ok_false_on_empty_text_or_empty_roster(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        calls = []
        completion_fn = _stub_completion(mentions=[], calls=calls)

        empty_text_result = resolve_mentioned_people("", _make_participants(), completion_fn=completion_fn)
        empty_roster_result = resolve_mentioned_people("some text", [], completion_fn=completion_fn)

        assert empty_text_result.ok is False
        assert empty_text_result.people == ()
        assert empty_roster_result.ok is False
        assert empty_roster_result.people == ()
        assert calls == []


# ---------------------------------------------------------------------------
# Happy path — zero, one, and multiple resolved mentions
# ---------------------------------------------------------------------------


class TestHappyPathResolution:
    def test_zero_people_resolved_is_ok_true_empty(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        completion_fn = _stub_completion(mentions=[])

        result = resolve_mentioned_people("some chapter text", _make_participants(), completion_fn=completion_fn)

        assert result.ok is True
        assert result.people == ()
        assert result.slugs == ()

    def test_one_person_resolved(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        participants = _make_participants()
        completion_fn = _stub_completion(
            mentions=[
                {
                    "name": "el señor Sánchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.9,
                    "evidence": "Referred to by the speaker as the head of government.",
                }
            ],
        )

        result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert result.slugs == ("pedro-sanchez",)
        assert result.people[0].display_name == "Pedro Sánchez"

    def test_multiple_people_resolved(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        participants = _make_participants()
        completion_fn = _stub_completion(
            mentions=[
                {
                    "name": "el señor Sánchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.9,
                    "evidence": "Head of government.",
                },
                {
                    "name": "la señora Díaz",
                    "participant_slug": "yolanda-diaz",
                    "confidence": 0.85,
                    "evidence": "Vice president referenced by the speaker.",
                },
            ],
        )

        result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert result.slugs == ("pedro-sanchez", "yolanda-diaz")


# ---------------------------------------------------------------------------
# Drop-reason gates — unknown slug, low confidence, non-numeric confidence
# ---------------------------------------------------------------------------


class TestDropReasons:
    @pytest.mark.parametrize(
        "mention_name,slug,confidence",
        [
            ("Alguien Inventado", "alguien-inventado-que-no-existe", 0.95),  # unknown slug
            ("Pedro Sanchez", "pedro-sanchez", 0.79),  # below MENTIONED_PEOPLE_MIN_CONFIDENCE
            ("Pedro Sanchez", "pedro-sanchez", "high"),  # non-numeric confidence
        ],
    )
    def test_dropped_and_logged(self, caplog, mention_name, slug, confidence):
        import logging

        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        participants = _make_participants()
        completion_fn = _stub_completion(
            mentions=[
                {
                    "name": mention_name,
                    "participant_slug": slug,
                    "confidence": confidence,
                    "evidence": "under test",
                }
            ],
        )

        with caplog.at_level(logging.INFO):
            result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert result.slugs == ()
        assert result.dropped_mentions == (mention_name,)
        assert mention_name in caplog.text

    def test_confidence_0_80_is_accepted(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        participants = _make_participants()
        completion_fn = _stub_completion(
            mentions=[
                {
                    "name": "Pedro Sanchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.80,
                    "evidence": "exact threshold",
                }
            ],
        )

        result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert result.slugs == ("pedro-sanchez",)


# ---------------------------------------------------------------------------
# Dedup and cap
# ---------------------------------------------------------------------------


class TestDedupAndCap:
    def test_duplicate_slugs_deduplicated_first_seen_order(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        participants = _make_participants()
        completion_fn = _stub_completion(
            mentions=[
                {
                    "name": "el señor Sánchez",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.9,
                    "evidence": "first mention",
                },
                {
                    "name": "la señora Díaz",
                    "participant_slug": "yolanda-diaz",
                    "confidence": 0.85,
                    "evidence": "second speaker mentioned",
                },
                {
                    "name": "el presidente del Gobierno",
                    "participant_slug": "pedro-sanchez",
                    "confidence": 0.88,
                    "evidence": "same person referred to again",
                },
            ],
        )

        result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert result.slugs == ("pedro-sanchez", "yolanda-diaz")

    def test_capped_at_max_mentioned_people(self):
        from congress_videos.modules.mentioned_people_resolution import (
            MAX_MENTIONED_PEOPLE,
            resolve_mentioned_people,
        )

        participants = [{"slug": f"person-{i}", "display_name": f"Person {i}", "party": "X"} for i in range(20)]
        mentions = [
            {
                "name": f"Person {i}",
                "participant_slug": f"person-{i}",
                "confidence": 0.9,
                "evidence": "mentioned",
            }
            for i in range(20)
        ]
        completion_fn = _stub_completion(mentions=mentions)

        result = resolve_mentioned_people("some chapter text", participants, completion_fn=completion_fn)

        assert result.ok is True
        assert len(result.slugs) == MAX_MENTIONED_PEOPLE
        assert result.slugs == tuple(f"person-{i}" for i in range(MAX_MENTIONED_PEOPLE))


# ---------------------------------------------------------------------------
# Malformed completion responses — never a clobbering write
# ---------------------------------------------------------------------------


class TestMalformedResponse:
    @pytest.mark.parametrize(
        "response",
        [
            {"data": None, "error": "rate_limited"},
            {"data": None, "error": None},
            {"error": "missing data key entirely"},
            {"data": {"mentions": "not-a-list"}, "error": None},
        ],
    )
    def test_malformed_response_returns_ok_false(self, response):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        def completion_fn(system_prompt, user_prompt, **kwargs):
            return response

        result = resolve_mentioned_people("some chapter text", _make_participants(), completion_fn=completion_fn)

        assert result.ok is False
        assert result.people == ()


# ---------------------------------------------------------------------------
# completion_fn failure — never raises
# ---------------------------------------------------------------------------


class TestCompletionFailureNeverRaises:
    def test_never_raises_on_completion_fn_exception(self):
        from congress_videos.modules.mentioned_people_resolution import resolve_mentioned_people

        def _raising_completion(system_prompt, user_prompt, **kwargs):
            raise TimeoutError("upstream timed out")

        result = resolve_mentioned_people("some chapter text", _make_participants(), completion_fn=_raising_completion)

        assert result.ok is False
        assert result.people == ()


# ---------------------------------------------------------------------------
# Prompt content — speaker/mentioned distinction (issue #432 success criterion)
# ---------------------------------------------------------------------------


class TestPromptDistinguishesSpeakerFromMentioned:
    def test_prompt_states_speaker_is_not_a_mention(self):
        from congress_videos.config.ai_prompts import MENTIONED_PEOPLE_SYSTEM_PROMPT

        assert (
            "The person who is SPEAKING is not automatically a mentioned person. "
            "Include only people REFERRED TO in the transcript content."
        ) in MENTIONED_PEOPLE_SYSTEM_PROMPT
