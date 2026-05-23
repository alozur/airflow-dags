"""Tests for utils.ai_helpers module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from utils.ai_helpers import (
    clamp_value,
    generate_chat_completion,
    generate_json_completion,
    parse_json_response,
    truncate_text,
)


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
        assert call_kwargs["max_tokens"] == 200

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
