"""Tests for utils.llm_config — LLM_DEFAULT / LLM_CHEAP tier constants."""

from __future__ import annotations

import importlib

import pytest

import utils.llm_config as llm_config


@pytest.fixture(autouse=True)
def _reload_llm_config_after_test():
    """Reload the module after each test so later tests see fresh env resolution.

    The module resolves LLM_DEFAULT/LLM_CHEAP at import time; monkeypatch
    reverts the environment automatically, but the already-imported module
    object keeps whatever value was captured during the test. Reloading here
    restores it to the real-environment state for any test that runs after.
    """
    yield
    importlib.reload(llm_config)


class TestLlmDefault:

    def test_fallback_when_env_unset(self, monkeypatch):
        monkeypatch.delenv("LLM_DEFAULT", raising=False)
        monkeypatch.delenv("LLM_CHEAP", raising=False)

        importlib.reload(llm_config)

        assert llm_config.LLM_DEFAULT == "gpt-5.6-luna"
        assert llm_config.LLM_CHEAP == "gpt-5-nano"

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("LLM_CHEAP", "gpt-4o-mini")

        importlib.reload(llm_config)

        assert llm_config.LLM_CHEAP == "gpt-4o-mini"

    def test_empty_string_env_falls_back_to_default(self, monkeypatch):
        """Compose passes "${LLM_DEFAULT:-}" = empty string, not unset.

        `os.getenv(...) or "<fallback>"` must treat the empty string as falsy
        and fall back to the committed default — `os.getenv(key, default)`
        would NOT do this (it only substitutes when the key is entirely
        absent), which is exactly the production-only bug this guards.
        """
        monkeypatch.setenv("LLM_DEFAULT", "")
        monkeypatch.setenv("LLM_CHEAP", "")

        importlib.reload(llm_config)

        assert llm_config.LLM_DEFAULT == "gpt-5.6-luna"
        assert llm_config.LLM_CHEAP == "gpt-5-nano"


class TestLlmTimeouts:

    def test_fallbacks_when_env_unset(self, monkeypatch):
        monkeypatch.delenv("LLM_TIMEOUT_SECONDS", raising=False)
        monkeypatch.delenv("LLM_CONNECT_TIMEOUT_SECONDS", raising=False)

        importlib.reload(llm_config)

        assert llm_config.LLM_TIMEOUT_SECONDS == 120.0
        assert llm_config.LLM_CONNECT_TIMEOUT_SECONDS == 10.0

    def test_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "45.5")

        importlib.reload(llm_config)

        assert llm_config.LLM_TIMEOUT_SECONDS == 45.5

    def test_connect_env_override_wins(self, monkeypatch):
        monkeypatch.setenv("LLM_CONNECT_TIMEOUT_SECONDS", "3")

        importlib.reload(llm_config)

        assert llm_config.LLM_CONNECT_TIMEOUT_SECONDS == 3.0

    def test_empty_string_falls_back(self, monkeypatch):
        """Mirrors the LLM_DEFAULT/LLM_CHEAP empty-string rule above: compose
        passes an unset override through as "", which must fall back, not raise.
        """
        monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "")
        monkeypatch.setenv("LLM_CONNECT_TIMEOUT_SECONDS", "")

        importlib.reload(llm_config)

        assert llm_config.LLM_TIMEOUT_SECONDS == 120.0
        assert llm_config.LLM_CONNECT_TIMEOUT_SECONDS == 10.0

    def test_malformed_value_raises_at_import(self, monkeypatch):
        monkeypatch.setenv("LLM_TIMEOUT_SECONDS", "abc")

        with pytest.raises(ValueError, match=r"LLM_TIMEOUT_SECONDS.*'abc'"):
            importlib.reload(llm_config)

    @pytest.mark.parametrize("bad_value", ["0", "-1", "-0.5"])
    def test_non_positive_raises(self, monkeypatch, bad_value: str):
        monkeypatch.setenv("LLM_CONNECT_TIMEOUT_SECONDS", bad_value)

        with pytest.raises(ValueError, match=r"LLM_CONNECT_TIMEOUT_SECONDS"):
            importlib.reload(llm_config)

    @pytest.mark.parametrize("bad_value", ["inf", "-inf", "nan"])
    def test_non_finite_raises(self, monkeypatch, bad_value: str):
        monkeypatch.setenv("LLM_TIMEOUT_SECONDS", bad_value)

        with pytest.raises(ValueError, match=r"LLM_TIMEOUT_SECONDS"):
            importlib.reload(llm_config)
