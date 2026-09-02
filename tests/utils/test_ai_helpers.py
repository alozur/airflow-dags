"""Tests for utils.ai_helpers module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import openai
import pytest

from utils.ai_helpers import (
    classify_openai_error,
    clamp_value,
    generate_chat_completion,
    generate_json_completion,
    parse_json_response,
    reset_openai_quota_latch,
    truncate_text,
)


@pytest.fixture(autouse=True)
def _reset_openai_quota_latch_between_tests():
    """Test-isolation seam (mandatory, not hygiene).

    pytest runs the whole suite in one process, so the module-global quota
    latch would otherwise silently poison every later test and make the suite
    order-dependent. Reset before AND after each test.
    """
    reset_openai_quota_latch()
    yield
    reset_openai_quota_latch()


def _fake_request() -> httpx.Request:
    return httpx.Request("POST", "https://api.openai.com/v1/chat/completions")


def _fake_response(status_code: int) -> httpx.Response:
    return httpx.Response(status_code, request=_fake_request())


# ---------------------------------------------------------------------------
# clamp_value
# ---------------------------------------------------------------------------

class TestClampValue:

    @pytest.mark.parametrize("value,min_v,max_v,expected", [
        (5, 1, 10, 5),       # within range — unchanged
        (0, 1, 10, 1),       # below min → clamped to min
        (15, 1, 10, 10),     # above max → clamped to max
        (1, 1, 10, 1),       # exactly at min boundary
        (10, 1, 10, 10),     # exactly at max boundary
    ])
    def test_clamp_parametrized(self, value: int, min_v: int, max_v: int, expected: int):
        assert clamp_value(value, min_v, max_v) == expected

    def test_clamp_with_equal_min_and_max(self):
        assert clamp_value(7, 5, 5) == 5

    def test_clamp_with_negative_values(self):
        assert clamp_value(-10, -5, 5) == -5

    def test_clamp_with_zero_bounds(self):
        assert clamp_value(3, 0, 0) == 0


# ---------------------------------------------------------------------------
# truncate_text
# ---------------------------------------------------------------------------

class TestTruncateText:

    @pytest.mark.parametrize("text,max_length,suffix,expected", [
        ("hello", 10, "...", "hello"),               # shorter than max — unchanged
        ("hello world", 8, "...", "hello..."),        # truncated with default suffix
        ("abcde", 5, "...", "abcde"),                 # exactly at max — unchanged
        ("abcdef", 5, "...", "ab..."),                # one over max
        ("hello", 5, "!", "hello"),                   # exactly at max with custom suffix
    ])
    def test_truncate_parametrized(self, text: str, max_length: int, suffix: str, expected: str):
        assert truncate_text(text, max_length, suffix) == expected

    def test_truncated_result_length_equals_max_length(self):
        text = "a" * 20
        result = truncate_text(text, 10, "...")
        assert len(result) == 10

    def test_empty_string_unchanged(self):
        assert truncate_text("", 5, "...") == ""

    def test_custom_suffix(self):
        # "…" is 1 character, so truncate_text("hello world", 8, "…") → text[:7] + "…" = "hello w…"
        result = truncate_text("hello world", 8, "…")
        assert result == "hello w…"
        assert len(result) == 8


# ---------------------------------------------------------------------------
# parse_json_response
# ---------------------------------------------------------------------------

class TestParseJsonResponse:

    def test_valid_json_object(self):
        result = parse_json_response('{"key": "value", "num": 42}')
        assert result["error"] is None
        assert result["data"] == {"key": "value", "num": 42}

    def test_valid_json_array(self):
        result = parse_json_response('[1, 2, 3]')
        assert result["error"] is None
        assert result["data"] == [1, 2, 3]

    def test_json_in_markdown_fence(self):
        text = '```json\n{"title": "test"}\n```'
        result = parse_json_response(text)
        assert result["error"] is None
        assert result["data"] == {"title": "test"}

    def test_json_in_plain_code_fence(self):
        text = '```\n{"title": "test"}\n```'
        result = parse_json_response(text)
        assert result["error"] is None
        assert result["data"] == {"title": "test"}

    def test_invalid_json_returns_error(self):
        result = parse_json_response("not valid json {{{")
        assert result["data"] is None
        assert result["error"] is not None
        assert "Failed to parse JSON" in result["error"]

    def test_empty_string_returns_error(self):
        result = parse_json_response("")
        assert result["data"] is None
        assert result["error"] is not None

    def test_nested_json(self):
        text = '{"chapters": [{"title": "Intro", "start": 0}]}'
        result = parse_json_response(text)
        assert result["error"] is None
        assert result["data"]["chapters"][0]["title"] == "Intro"

    def test_result_has_required_keys(self):
        result = parse_json_response('{"x": 1}')
        assert "data" in result
        assert "error" in result


# ---------------------------------------------------------------------------
# generate_chat_completion
# ---------------------------------------------------------------------------

class TestGenerateChatCompletion:

    def _make_fake_response(self, content: str) -> MagicMock:
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = content
        return fake_response

    def test_returns_content_on_success(self, mocker):
        fake_response = self._make_fake_response("Generated text")
        mocker.patch("utils.ai_helpers.openai.chat.completions.create", return_value=fake_response)

        result = generate_chat_completion("system", "user")

        assert result["error"] is None
        assert result["content"] == "Generated text"

    def test_strips_whitespace_from_content(self, mocker):
        fake_response = self._make_fake_response("  trimmed  ")
        mocker.patch("utils.ai_helpers.openai.chat.completions.create", return_value=fake_response)

        result = generate_chat_completion("sys", "usr")
        assert result["content"] == "trimmed"

    def test_passes_model_to_api(self, mocker):
        fake_response = self._make_fake_response("ok")
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        generate_chat_completion("sys", "usr", model="gpt-4o")
        call_kwargs = mock_create.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o"

    def test_passes_temperature_and_max_tokens_to_api(self, mocker):
        fake_response = self._make_fake_response("ok")
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        generate_chat_completion("sys", "usr", temperature=0.1, max_tokens=200)
        call_kwargs = mock_create.call_args.kwargs
        assert call_kwargs["temperature"] == 0.1
        # The `max_tokens` Python parameter is forwarded on the wire as
        # `max_completion_tokens` (issue #365) — GPT-5-family models reject
        # `max_tokens` with a 400 "Unsupported parameter" error.
        assert call_kwargs["max_completion_tokens"] == 200
        assert "max_tokens" not in call_kwargs

    def test_never_sends_max_tokens_kwarg(self, mocker):
        """Regression guard for issue #365: `max_tokens` must never reach the wire.

        Uses a negative assertion (key absence), not just an equality check on
        `max_completion_tokens` — an equality-only assertion would not catch a
        `max_tokens=` kwarg re-added alongside it.
        """
        fake_response = self._make_fake_response("ok")
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        generate_chat_completion("sys", "usr")
        assert "max_tokens" not in mock_create.call_args.kwargs

    def test_max_completion_tokens_used_for_non_gpt5_model(self, mocker):
        """No model-name branching: the wire kwarg is the same regardless of model."""
        fake_response = self._make_fake_response("ok")
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        generate_chat_completion("sys", "usr", model="gpt-4o-mini", max_tokens=200)
        call_kwargs = mock_create.call_args.kwargs
        assert call_kwargs["max_completion_tokens"] == 200
        assert "max_tokens" not in call_kwargs

    def test_sends_system_and_user_messages(self, mocker):
        fake_response = self._make_fake_response("ok")
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        generate_chat_completion("my system", "my user")
        messages = mock_create.call_args.kwargs["messages"]
        assert messages[0] == {"role": "system", "content": "my system"}
        assert messages[1] == {"role": "user", "content": "my user"}

    def test_returns_error_on_api_exception(self, mocker):
        mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create",
            side_effect=Exception("API timeout"),
        )
        result = generate_chat_completion("sys", "usr")
        assert result["content"] is None
        assert result["error"] is not None
        assert "API timeout" in result["error"]

    def test_returns_error_when_openai_not_available(self, mocker):
        mocker.patch("utils.ai_helpers.openai", None)
        result = generate_chat_completion("sys", "usr")
        assert result["content"] is None
        assert result["error"] == "OpenAI module not installed"

    def test_result_has_required_keys(self, mocker):
        fake_response = self._make_fake_response("text")
        mocker.patch("utils.ai_helpers.openai.chat.completions.create", return_value=fake_response)
        result = generate_chat_completion("sys", "usr")
        assert "content" in result
        assert "error" in result


# ---------------------------------------------------------------------------
# generate_json_completion
# ---------------------------------------------------------------------------

class TestGenerateJsonCompletion:

    def test_successful_json_completion(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": '{"chapters": []}', "error": None},
        )
        result = generate_json_completion("sys", "usr")
        assert result["error"] is None
        assert result["data"] == {"chapters": []}
        assert result["raw_content"] == '{"chapters": []}'

    def test_propagates_chat_completion_error(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": None, "error": "Network error"},
        )
        result = generate_json_completion("sys", "usr")
        assert result["data"] is None
        assert result["raw_content"] is None
        assert result["error"] == "Network error"

    def test_returns_parse_error_for_invalid_json(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": "not json at all", "error": None},
        )
        result = generate_json_completion("sys", "usr")
        assert result["data"] is None
        assert result["error"] is not None

    def test_raw_content_always_set_when_completion_succeeds(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": '{"x": 1}', "error": None},
        )
        result = generate_json_completion("sys", "usr")
        assert result["raw_content"] == '{"x": 1}'

    def test_passes_model_and_temperature_to_chat_completion(self, mocker):
        mock_chat = mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": '{"ok": true}', "error": None},
        )
        generate_json_completion("sys", "usr", model="gpt-4o-mini", temperature=0.0)
        call_kwargs = mock_chat.call_args.kwargs
        assert call_kwargs["model"] == "gpt-4o-mini"
        assert call_kwargs["temperature"] == 0.0

    def test_result_has_required_keys(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": '{"k": "v"}', "error": None},
        )
        result = generate_json_completion("sys", "usr")
        assert "data" in result
        assert "raw_content" in result
        assert "error" in result

    def test_json_in_markdown_fence_is_parsed(self, mocker):
        mocker.patch(
            "utils.ai_helpers.generate_chat_completion",
            return_value={"content": '```json\n{"title": "Test"}\n```', "error": None},
        )
        result = generate_json_completion("sys", "usr")
        assert result["error"] is None
        assert result["data"] == {"title": "Test"}

    def test_json_completion_forwards_max_tokens_to_wire_param(self, mocker):
        """Pins the public-signature-unchanged requirement (issue #365):

        `generate_json_completion(max_tokens=...)` still ends up as
        `max_completion_tokens=...` on the actual OpenAI SDK call, exercising the
        real (unmocked) `generate_chat_completion` in between.
        """
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = '{"ok": true}'
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create", return_value=fake_response
        )
        result = generate_json_completion("sys", "usr", max_tokens=120)
        call_kwargs = mock_create.call_args.kwargs
        assert call_kwargs["max_completion_tokens"] == 120
        assert "max_tokens" not in call_kwargs
        assert result["data"] == {"ok": True}


# ---------------------------------------------------------------------------
# classify_openai_error
# ---------------------------------------------------------------------------

class TestClassifyOpenAIError:

    def test_quota_via_type_is_permanent(self):
        exc = openai.RateLimitError(
            "You exceeded your current quota",
            response=_fake_response(429),
            body={"type": "insufficient_quota", "code": "credit_balance_exhausted"},
        )
        result = classify_openai_error(exc)
        assert result["permanent"] is True
        assert result["type"] == "insufficient_quota"
        assert result["code"] == "credit_balance_exhausted"
        assert result["status"] == 429

    def test_quota_via_code_alone_is_permanent(self):
        exc = openai.RateLimitError(
            "Billing hard limit reached",
            response=_fake_response(429),
            body={"code": "billing_hard_limit_reached"},
        )
        result = classify_openai_error(exc)
        assert result["permanent"] is True
        assert result["type"] is None
        assert result["code"] == "billing_hard_limit_reached"

    def test_plain_rate_limit_is_transient(self):
        exc = openai.RateLimitError(
            "Rate limit exceeded",
            response=_fake_response(429),
            body={"type": "rate_limit_exceeded"},
        )
        result = classify_openai_error(exc)
        assert result["permanent"] is False
        assert result["type"] == "rate_limit_exceeded"
        assert result["status"] == 429

    def test_authentication_error_is_permanent(self):
        exc = openai.AuthenticationError(
            "Incorrect API key provided",
            response=_fake_response(401),
            body={"type": "invalid_request_error"},
        )
        result = classify_openai_error(exc)
        assert result["permanent"] is True
        assert result["status"] == 401

    def test_timeout_error_is_transient(self):
        exc = openai.APITimeoutError(request=_fake_request())
        result = classify_openai_error(exc)
        assert result["permanent"] is False
        assert result["status"] is None

    def test_unrelated_exception_is_unclassified(self):
        result = classify_openai_error(ValueError("not an openai error"))
        assert result["permanent"] is None
        assert result["type"] is None
        assert result["code"] is None
        assert result["status"] is None

    def test_non_mapping_body_is_unclassified_without_crash(self):
        # Pins the E2 KeyError gotcha: exc.body is not a dict here (proxy/HTML
        # error page shape). classify_openai_error must use getattr, never
        # exc.body["error"]["type"], or this raises instead of returning.
        exc = openai.APIError(
            "proxy error", _fake_request(), body="<html>502 Bad Gateway</html>"
        )
        result = classify_openai_error(exc)
        assert result["permanent"] is None
        assert result["type"] is None
        assert result["code"] is None


# ---------------------------------------------------------------------------
# OpenAI quota latch
# ---------------------------------------------------------------------------

class TestOpenAIQuotaLatch:

    def _make_fake_response(self, content: str) -> MagicMock:
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = content
        return fake_response

    def _quota_error(self) -> openai.RateLimitError:
        return openai.RateLimitError(
            "You exceeded your current quota",
            response=_fake_response(429),
            body={"type": "insufficient_quota", "code": "credit_balance_exhausted"},
        )

    def _plain_rate_limit_error(self) -> openai.RateLimitError:
        return openai.RateLimitError(
            "Rate limit exceeded",
            response=_fake_response(429),
            body={"type": "rate_limit_exceeded"},
        )

    def test_quota_error_arms_latch_and_skips_subsequent_calls(self, mocker):
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create",
            side_effect=self._quota_error(),
        )

        first = generate_chat_completion("sys", "usr")
        assert first["content"] is None
        assert mock_create.call_count == 1

        # Even if the underlying call would now succeed, the latch must still
        # short-circuit — that is the whole point of the fast-fail.
        mock_create.side_effect = None
        mock_create.return_value = self._make_fake_response("should not be reached")

        second = generate_chat_completion("sys", "usr")
        third = generate_chat_completion("sys", "usr")

        assert second["content"] is None
        assert third["content"] is None
        assert mock_create.call_count == 1  # calls 2 and 3 never touched the network

    def test_plain_rate_limit_does_not_arm_latch(self, mocker):
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create",
            side_effect=self._plain_rate_limit_error(),
        )

        first = generate_chat_completion("sys", "usr")
        assert first["content"] is None
        assert mock_create.call_count == 1

        mock_create.side_effect = None
        mock_create.return_value = self._make_fake_response("ok")
        second = generate_chat_completion("sys", "usr")

        assert second["content"] == "ok"
        assert mock_create.call_count == 2  # normal behaviour: call 2 hit the network

    def test_reset_restores_normal_behaviour(self, mocker):
        mock_create = mocker.patch(
            "utils.ai_helpers.openai.chat.completions.create",
            side_effect=self._quota_error(),
        )

        generate_chat_completion("sys", "usr")
        assert mock_create.call_count == 1

        reset_openai_quota_latch()

        mock_create.side_effect = None
        mock_create.return_value = self._make_fake_response("back to normal")
        result = generate_chat_completion("sys", "usr")

        assert result["content"] == "back to normal"
        assert mock_create.call_count == 2
