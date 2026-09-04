"""Unit tests for congress_videos.modules.topic_extraction (issue #432).

TDD RED cycle: all scenarios written before implementation. Covers the
extractor contract from the design/spec: normalization, dedup, overlong
rejection, cap, empty-but-ok output, and malformed-output handling.
"""

from __future__ import annotations

import pytest


def _stub_completion(topics, error=None, calls=None):
    """Build a completion_fn stub that records calls and returns fixed topics."""
    call_log = calls if calls is not None else []

    def _fn(system_prompt, user_prompt, **kwargs):
        call_log.append({"system_prompt": system_prompt, "user_prompt": user_prompt, "kwargs": kwargs})
        if error is not None:
            return {"data": None, "error": error}
        return {"data": {"topics": topics}, "error": None}

    return _fn


class TestNormalization:
    def test_topics_normalized_lowercase_trimmed_whitespace_collapsed(self):
        """Design scenario: mixed-case/duplicate topics normalize and dedup
        (whitespace collapse verified via the ``sanidad`` / ``sanidad `` pair)."""
        from congress_videos.modules.topic_extraction import extract_topics

        completion_fn = _stub_completion(topics=["Sanidad", "sanidad ", "Educación"])

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is True
        assert result.topics == ("sanidad", "educación")


class TestDedupAndCap:
    def test_topics_deduplicated_preserving_first_seen_order(self):
        from congress_videos.modules.topic_extraction import extract_topics

        completion_fn = _stub_completion(topics=["vivienda", "sanidad", "vivienda", "educación", "sanidad"])

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is True
        assert result.topics == ("vivienda", "sanidad", "educación")

    def test_overlong_topic_dropped(self):
        from congress_videos.modules.topic_extraction import extract_topics

        overlong = "x" * 61
        completion_fn = _stub_completion(topics=["sanidad", overlong])

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is True
        assert result.topics == ("sanidad",)

    def test_capped_at_max_topics(self):
        from congress_videos.modules.topic_extraction import MAX_TOPICS, extract_topics

        topics_in = [f"tema {i}" for i in range(20)]
        completion_fn = _stub_completion(topics=topics_in)

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is True
        assert len(result.topics) == MAX_TOPICS
        assert result.topics == tuple(f"tema {i}" for i in range(MAX_TOPICS))


class TestEmptyOutput:
    def test_no_topics_returns_ok_true_empty(self):
        """A successful call that found no topics is distinct from a failed
        extraction: ok=True with an empty tuple (D9's persist gate depends
        on this distinction)."""
        from congress_videos.modules.topic_extraction import extract_topics

        completion_fn = _stub_completion(topics=[])

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is True
        assert result.topics == ()


class TestMalformedResponse:
    @pytest.mark.parametrize(
        "response",
        [
            {"data": None, "error": "rate_limited"},
            {"data": None, "error": None},
            {"error": "missing data key entirely"},
            {"data": {"topics": "not-a-list"}, "error": None},
        ],
    )
    def test_malformed_output_returns_ok_false(self, response):
        from congress_videos.modules.topic_extraction import extract_topics

        def completion_fn(system_prompt, user_prompt, **kwargs):
            return response

        result = extract_topics("some chapter text", completion_fn=completion_fn)

        assert result.ok is False
        assert result.topics == ()


class TestCompletionFailureNeverRaises:
    def test_never_raises_on_completion_fn_exception(self):
        from congress_videos.modules.topic_extraction import extract_topics

        def _raising_completion(system_prompt, user_prompt, **kwargs):
            raise TimeoutError("upstream timed out")

        result = extract_topics("some chapter text", completion_fn=_raising_completion)

        assert result.ok is False
        assert result.topics == ()
