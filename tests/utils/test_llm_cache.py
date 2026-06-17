"""Tests for utils.llm_cache (Batch 3, improvement #2: LLM idempotency cache).

Covers the two contracts that matter:
  1. Cache hit/miss behaviour and deterministic keying.
  2. Graceful degradation: ANY database error must fall through to a live
     ``generate_json_completion`` call and NEVER raise.
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import utils.llm_cache as llm_cache
from utils.llm_cache import (
    cached_json_completion,
    get_cached,
    make_cache_key,
    put_cached,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_pg(fetch_row=None, capture=None):
    """Build a fake PostgresConnection whose cursor returns ``fetch_row``.

    ``capture`` (a list) collects ``(sql, params)`` tuples passed to execute.
    """
    cursor = MagicMock()
    cursor.fetchone.return_value = fetch_row

    def _execute(sql, params=None):
        if capture is not None:
            capture.append((sql, params))

    cursor.execute.side_effect = _execute

    conn = MagicMock()

    @contextmanager
    def _cursor_cm():
        yield cursor

    conn.cursor.side_effect = _cursor_cm

    pg = MagicMock()

    @contextmanager
    def _conn_cm():
        yield conn

    pg.get_connection.side_effect = _conn_cm
    return pg


# ---------------------------------------------------------------------------
# make_cache_key
# ---------------------------------------------------------------------------

class TestMakeCacheKey:
    def test_key_is_deterministic_for_same_inputs(self):
        k1 = make_cache_key("gpt-4o-mini", "sys", "user", temperature=0.0)
        k2 = make_cache_key("gpt-4o-mini", "sys", "user", temperature=0.0)
        assert k1 == k2

    def test_key_is_64_char_hex(self):
        key = make_cache_key("gpt-4o-mini", "sys", "user")
        assert len(key) == 64
        assert all(c in "0123456789abcdef" for c in key)

    def test_different_prompts_produce_different_keys(self):
        k1 = make_cache_key("gpt-4o-mini", "sys", "user A")
        k2 = make_cache_key("gpt-4o-mini", "sys", "user B")
        assert k1 != k2

    def test_different_model_produces_different_key(self):
        k1 = make_cache_key("gpt-4o-mini", "sys", "user")
        k2 = make_cache_key("gpt-4o", "sys", "user")
        assert k1 != k2

    def test_different_params_produce_different_keys(self):
        k1 = make_cache_key("gpt-4o-mini", "sys", "user", temperature=0.0)
        k2 = make_cache_key("gpt-4o-mini", "sys", "user", temperature=0.9)
        assert k1 != k2


# ---------------------------------------------------------------------------
# get_cached / put_cached graceful degradation
# ---------------------------------------------------------------------------

class TestGetCached:
    def test_hit_returns_stored_response(self, mocker):
        stored = {"data": {"ok": True}, "raw_content": "{}", "error": None}
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            return_value=_fake_pg(fetch_row={"response": stored}),
        )
        assert get_cached("abc") == stored

    def test_miss_returns_none(self, mocker):
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            return_value=_fake_pg(fetch_row=None),
        )
        assert get_cached("abc") is None

    def test_db_error_returns_none_and_does_not_raise(self, mocker):
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            side_effect=RuntimeError("connection refused"),
        )
        # Must degrade gracefully — treated as a miss, never raises.
        assert get_cached("abc") is None

    def test_select_sql_uses_literal_table_name_not_fstring(self, mocker):
        """DEF-1 regression guard: the SELECT query must use the literal table
        name 'llm_cache' and must not contain any f-string brace remnants."""
        capture: list = []
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            return_value=_fake_pg(fetch_row=None, capture=capture),
        )
        get_cached("anykey")

        assert len(capture) == 1
        sql, _ = capture[0]
        assert "llm_cache" in sql
        assert "{" not in sql and "}" not in sql, "SQL must not contain f-string braces"


class TestPutCached:
    def test_uses_insert_on_conflict_do_nothing(self, mocker):
        capture: list = []
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            return_value=_fake_pg(capture=capture),
        )
        put_cached("key123", "gpt-4o-mini", {"data": 1})

        assert len(capture) == 1
        sql, params = capture[0]
        assert "INSERT INTO" in sql
        assert "ON CONFLICT (cache_key) DO NOTHING" in sql
        # Parameterised — never f-string interpolated values.
        assert "%s" in sql
        assert params[0] == "key123"
        assert params[1] == "gpt-4o-mini"

    def test_db_error_does_not_raise(self, mocker):
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            side_effect=RuntimeError("disk full"),
        )
        # Should swallow the error and return None without raising.
        assert put_cached("key", "model", {"data": 1}) is None

    def test_insert_sql_uses_literal_table_name_not_fstring(self, mocker):
        """DEF-1 regression guard: the INSERT query must use the literal table
        name 'llm_cache' and must not contain any f-string brace remnants."""
        capture: list = []
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            return_value=_fake_pg(capture=capture),
        )
        put_cached("key123", "gpt-4o-mini", {"data": 1})

        assert len(capture) == 1
        sql, _ = capture[0]
        assert "llm_cache" in sql
        assert "{" not in sql and "}" not in sql, "SQL must not contain f-string braces"


# ---------------------------------------------------------------------------
# cached_json_completion
# ---------------------------------------------------------------------------

class TestCachedJsonCompletion:
    def test_miss_calls_model_and_stores_result(self, mocker):
        live = {"data": {"foo": "bar"}, "raw_content": "{}", "error": None}
        mocker.patch.object(llm_cache, "get_cached", return_value=None)
        gen = mocker.patch.object(
            llm_cache, "generate_json_completion", return_value=live
        )
        put = mocker.patch.object(llm_cache, "put_cached")

        result = cached_json_completion("sys", "user", model="gpt-4o-mini")

        assert result == live
        gen.assert_called_once()
        put.assert_called_once()

    def test_hit_returns_cached_without_calling_model(self, mocker):
        cached = {"data": {"hit": True}, "raw_content": "{}", "error": None}
        mocker.patch.object(llm_cache, "get_cached", return_value=cached)
        gen = mocker.patch.object(llm_cache, "generate_json_completion")
        put = mocker.patch.object(llm_cache, "put_cached")

        result = cached_json_completion("sys", "user")

        assert result == cached
        gen.assert_not_called()
        put.assert_not_called()

    def test_error_result_is_not_cached(self, mocker):
        err = {"data": None, "raw_content": None, "error": "model down"}
        mocker.patch.object(llm_cache, "get_cached", return_value=None)
        mocker.patch.object(
            llm_cache, "generate_json_completion", return_value=err
        )
        put = mocker.patch.object(llm_cache, "put_cached")

        result = cached_json_completion("sys", "user")

        assert result == err
        put.assert_not_called()

    def test_db_unavailable_falls_through_to_live_call(self, mocker):
        """Full graceful-degradation path: DB raising on read+write still
        yields the live result and never raises."""
        live = {"data": {"ok": 1}, "raw_content": "{}", "error": None}
        mocker.patch.object(
            llm_cache, "PostgresConnection",
            side_effect=RuntimeError("db unreachable"),
        )
        gen = mocker.patch.object(
            llm_cache, "generate_json_completion", return_value=live
        )

        result = cached_json_completion("sys", "user")

        assert result == live
        gen.assert_called_once()

    def test_empty_string_response_cached_as_is(self, mocker):
        """A successful response whose data is falsy (empty) is still stored."""
        live = {"data": {}, "raw_content": "", "error": None}
        mocker.patch.object(llm_cache, "get_cached", return_value=None)
        mocker.patch.object(
            llm_cache, "generate_json_completion", return_value=live
        )
        put = mocker.patch.object(llm_cache, "put_cached")

        result = cached_json_completion("sys", "user")

        assert result == live
        put.assert_called_once()
