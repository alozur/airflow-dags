"""Idempotent Postgres-backed cache for LLM JSON completions.

Caches the result of :func:`utils.ai_helpers.generate_json_completion` keyed by a
sha256 hash of the model, prompts and any extra parameters. Repeated task runs or
Airflow retries with identical inputs reuse the stored response instead of
re-calling the model. Because the prompt content is part of the key, any prompt
change automatically invalidates the cached entry.

Graceful degradation: the cache is an optimisation, never a hard dependency. ANY
database error (table missing, connection failure, etc.) is logged at WARNING level
and the call falls through to a live LLM call. The cache layer NEVER raises.

The backing table is created by
``congress_videos/sql/migrations/009_create_llm_cache.sql``.
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

from psycopg2.extras import Json

from utils.ai_helpers import generate_json_completion
from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)


def make_cache_key(
    model: str,
    system_prompt: str,
    user_prompt: str,
    **params: Any,
) -> str:
    """Build a deterministic sha256 cache key for an LLM call.

    The key is the hex digest of a JSON-serialised payload with sorted keys, so
    the same inputs always produce the same key and different inputs (different
    prompts, model or params) produce different keys.

    Args:
        model: Model identifier (e.g. ``"gpt-4o-mini"``).
        system_prompt: System message.
        user_prompt: User message.
        **params: Any extra parameters that affect the response (temperature, etc).

    Returns:
        64-character lowercase sha256 hex digest.
    """
    payload = {
        "model": model,
        "system_prompt": system_prompt,
        "user_prompt": user_prompt,
        "params": params,
    }
    serialised = json.dumps(payload, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(serialised.encode("utf-8")).hexdigest()


def get_cached(cache_key: str) -> dict | None:
    """Return the cached response dict for ``cache_key`` or ``None`` on a miss.

    On ANY database error this logs a WARNING and returns ``None`` (treated as a
    cache miss) — it never raises.

    Args:
        cache_key: sha256 hex digest from :func:`make_cache_key`.

    Returns:
        The stored response dict, or ``None`` if not cached or the cache is
        unavailable.
    """
    try:
        pg = PostgresConnection()
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT response FROM llm_cache WHERE cache_key = %s",
                    (cache_key,),
                )
                row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - cache must never break the pipeline
        logger.warning("LLM cache unavailable on read (%s); treating as miss", exc)
        return None

    if row is None:
        return None
    # RealDictCursor → row is a mapping with the column name as key.
    return row["response"]


def put_cached(cache_key: str, model: str, response: dict) -> None:
    """Store ``response`` under ``cache_key``, ignoring duplicates.

    Uses ``ON CONFLICT (cache_key) DO NOTHING`` so concurrent writers are safe.
    On ANY database error this logs a WARNING and returns without raising.

    Args:
        cache_key: sha256 hex digest from :func:`make_cache_key`.
        model: Model identifier stored alongside the response.
        response: The JSON-serialisable response dict to cache.
    """
    try:
        pg = PostgresConnection()
        with pg.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO llm_cache (cache_key, model, response) "
                    "VALUES (%s, %s, %s) ON CONFLICT (cache_key) DO NOTHING",
                    (cache_key, model, Json(response)),
                )
    except Exception as exc:  # noqa: BLE001 - cache must never break the pipeline
        logger.warning("LLM cache unavailable on write (%s); skipping store", exc)


def cached_json_completion(
    system_prompt: str,
    user_prompt: str,
    model: str = "gpt-4o-mini",
    **kw: Any,
) -> dict:
    """Return a JSON completion, served from the Postgres cache when available.

    Wraps :func:`utils.ai_helpers.generate_json_completion`. On a cache hit the
    stored result dict is returned without calling the model. On a miss the model
    is called and, if it returned no error, the result is stored before returning.

    The cache is purely an optimisation: any database error degrades gracefully to
    a live call (see :func:`get_cached` / :func:`put_cached`).

    Args:
        system_prompt: System message defining model behaviour.
        user_prompt: User message with the request.
        model: Model identifier (default ``"gpt-4o-mini"``).
        **kw: Extra keyword args forwarded to ``generate_json_completion`` (e.g.
            ``temperature``, ``max_tokens``). They also participate in the cache key.

    Returns:
        The same dict shape as ``generate_json_completion``:
        ``{"data": ..., "raw_content": ..., "error": ...}``.
    """
    cache_key = make_cache_key(model, system_prompt, user_prompt, **kw)

    cached = get_cached(cache_key)
    if cached is not None:
        logger.debug("LLM cache hit for key %s", cache_key)
        return cached

    result = generate_json_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        **kw,
    )

    # Only cache successful responses — errors should be retried, not memoised.
    if result.get("error") is None:
        put_cached(cache_key, model, result)

    return result
