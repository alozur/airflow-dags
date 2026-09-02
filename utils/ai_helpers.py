"""
AI Helper utilities for OpenAI integration.

This module provides reusable functions for working with OpenAI API across all projects.
Includes text generation, JSON response parsing, and error handling utilities.
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional

from utils.llm_config import (
    LLM_CHEAP,
    LLM_CONNECT_TIMEOUT_SECONDS,
    LLM_DEFAULT,
    LLM_TIMEOUT_SECONDS,
)

try:
    import openai

    # OpenAI Configuration - gets API key from environment
    openai.api_key = os.getenv("OPENAI_API_KEY")
except ImportError:
    openai = None
    logging.warning(
        "OpenAI module not installed. AI functions will not work. Install with: pip install openai"
    )

# Codes observed on the `insufficient_quota` / billing-exhaustion family (issue #311
# comment 4). Checked against BOTH exc.type and exc.code because the observed
# production payload splits the discriminator across the two attributes:
# {'type': 'insufficient_quota', 'code': 'credit_balance_exhausted'}.
_QUOTA_CODES = frozenset(
    {"insufficient_quota", "credit_balance_exhausted", "billing_hard_limit_reached"}
)

# Process-local latch: once a permanent OpenAI failure (quota/billing exhaustion,
# 401/403 auth failure) is observed, every subsequent generate_chat_completion call
# short-circuits before touching the network, so calls 2..N cost zero instead of
# each paying the SDK's own bounded retry/backoff budget again.
#
# Scope note: AIRFLOW__CORE__EXECUTOR is LocalExecutor (docker-compose.yml:30),
# which forks a fresh process per task instance, so this latch dies with the task —
# a credit top-up or key rotation is picked up automatically on the very next run,
# with no TTL and no stale-block risk. A future move to CeleryExecutor (long-lived
# workers shared across runs) would need to add a TTL so a stale latch cannot
# outlive the condition that armed it.
_openai_quota_latched = False


def reset_openai_quota_latch() -> None:
    """Clear the process-local OpenAI permanent-failure latch.

    Test-isolation seam: pytest runs the whole suite in one process, so a latched
    module global would silently poison every later test unless it is reset
    between tests. Call this from an autouse fixture in test suites that exercise
    `generate_chat_completion`.
    """
    global _openai_quota_latched
    _openai_quota_latched = False


def classify_openai_error(exc: BaseException) -> Dict[str, Any]:
    """Classify an openai>=2 exception as permanent, transient, or unknown.

    Returns {"permanent": True|False|None, "type": str|None, "code": str|None,
    "status": int|None}.

    `permanent` is TRI-VALUED — compare with `is True`/`is False`/`is None`, never
    truthiness (per #320): `False` means "retry may work", `None` means "we could
    not classify this", and those demand different responses.

    `permanent is True` for:
    - quota/billing exhaustion, detected on BOTH `exc.type` and `exc.code`
      (the observed payload splits the discriminator across the two attributes)
    - a 401/403 authentication/authorization failure — will not clear without a
      human fixing the key or permissions

    `permanent is False` for 429 (non-quota), 5xx, and connection/timeout errors —
    these are exactly the shapes the SDK's own bounded retry budget already
    handles. Anything else (including non-OpenAI exceptions) is `permanent is
    None`.

    Gotcha (load-bearing): the SDK unwraps the `error` envelope before
    constructing the exception (`openai/_client.py:679`) and assigns
    `.type`/`.code` from the INNER dict (`openai/_exceptions.py:73-80`), so
    `exc.body["error"]["type"]` would raise `KeyError`. Always read via
    `getattr(exc, "type", None)` / `getattr(exc, "code", None)` — both are `None`
    when the body is not a mapping (HTML error page, proxy error), which must not
    crash this function.
    """
    exc_type = getattr(exc, "type", None)
    exc_code = getattr(exc, "code", None)
    status = getattr(exc, "status_code", None)

    if exc_type == "insufficient_quota" or exc_code in _QUOTA_CODES:
        permanent: Optional[bool] = True
    elif status in (401, 403):
        permanent = True
    elif status == 429 or (isinstance(status, int) and 500 <= status < 600):
        permanent = False
    elif openai is not None and isinstance(exc, openai.APIConnectionError):
        # Covers APITimeoutError too (APITimeoutError subclasses APIConnectionError
        # and carries no status_code of its own).
        permanent = False
    else:
        permanent = None

    return {
        "permanent": permanent,
        "type": exc_type,
        "code": exc_code,
        "status": status,
    }


def generate_chat_completion(
    system_prompt: str,
    user_prompt: str,
    model: str = LLM_DEFAULT,
    temperature: float = 0.7,
    max_tokens: int = 500,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Generate a chat completion using OpenAI API.

    Args:
        system_prompt: System message defining AI behavior
        user_prompt: User message with the actual request
        model: OpenAI model to use (default: LLM_DEFAULT tier)
        temperature: Sampling temperature 0-1 (default: 0.7)
        max_tokens: Maximum tokens in the response (default: 500). Sent on the
            wire as OpenAI's `max_completion_tokens` (issue #365) — the Python
            parameter name is kept deliberately so the 13 existing call sites
            need no change.
        timeout: Optional per-call read/write budget in seconds. None
            (default) uses LLM_TIMEOUT_SECONDS; the LLM_CONNECT_TIMEOUT_SECONDS
            connect budget always applies. Deliberately `Optional[float] = None`
            rather than `= LLM_TIMEOUT_SECONDS` (unlike `model` above): the
            value must COMPOSE with the operator connect budget into a single
            `Timeout` object, and a scalar signature default would both
            discard the connect budget and freeze the env read at import time.

    Returns:
        Dict with:
        - content: Generated text content
        - error: Error message if generation failed, None otherwise
    """
    global _openai_quota_latched

    if not openai:
        return {
            "content": None,
            "error": "OpenAI module not installed",
        }

    if _openai_quota_latched:
        # Fast path: a prior call in this process already hit a permanent
        # failure (quota/billing exhaustion or auth). Skip the network entirely
        # instead of paying the SDK's retry/backoff budget again for a call that
        # is guaranteed to fail the same way.
        return {
            "content": None,
            "error": "OpenAI permanently unavailable this process (quota/auth latch armed); skipping call",
        }

    # Resolved per call so the module-level floats stay monkeypatchable and
    # `openai.Timeout` is only dereferenced past the `if not openai` guard
    # above (survives `mocker.patch("utils.ai_helpers.openai", None)`). A
    # caller-supplied float overrides ONLY the read/write/pool budget; the
    # operator-level connect budget always applies (issue #355). `is None` is
    # the "not specified" sentinel — never truthiness, and never "unbounded".
    request_timeout = openai.Timeout(
        timeout=LLM_TIMEOUT_SECONDS if timeout is None else timeout,
        connect=LLM_CONNECT_TIMEOUT_SECONDS,
    )

    try:
        response = openai.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            # The wire kwarg name DELIBERATELY differs from the Python parameter
            # name (`max_tokens` above). Both configured production tiers
            # (`gpt-5.6-luna`, `gpt-5-nano`) are GPT-5-family and reject
            # `max_tokens` with a 400 "Unsupported parameter" error (issue #365),
            # while `max_completion_tokens` is accepted by every current Chat
            # Completions model — a strict superset. Sent unconditionally, with
            # no model-name branching, because the real prod model id
            # `gpt-5.6-luna` does not even match a clean `gpt-5-` prefix.
            # Do NOT "restore" `max_tokens` here — a regression test
            # (test_never_sends_max_tokens_kwarg) asserts its absence.
            max_completion_tokens=max_tokens,
            temperature=temperature,
            # Without this the SDK uses its own DEFAULT_TIMEOUT (read 600s,
            # connect 5s) with DEFAULT_MAX_RETRIES=2, so one stalled call can
            # hold a task ~30 min — and only 2 of ~5 DAGs set an
            # execution_timeout, so most have no Airflow ceiling at all
            # (issue #355). Split, not scalar: a bare float would overwrite the
            # connect budget too. Never pass `timeout=None` here — the SDK reads
            # None as "unbounded", which is the exact bug being fixed.
            timeout=request_timeout,
        )

        content = response.choices[0].message.content.strip()
        logging.debug(f"OpenAI response generated successfully (model: {model})")

        return {
            "content": content,
            "error": None,
        }

    except Exception as e:
        classification = classify_openai_error(e)
        if classification["permanent"] is True:
            _openai_quota_latched = True

        error_msg = f"Error generating chat completion: {str(e)}"
        logging.error(error_msg)
        return {
            "content": None,
            "error": error_msg,
        }


def parse_json_response(response_text: str) -> Dict[str, Any]:
    """
    Parse JSON from OpenAI response, handling markdown code blocks.

    Args:
        response_text: Raw response text from OpenAI that may contain JSON

    Returns:
        Dict with:
        - data: Parsed JSON data (dict) if successful, None otherwise
        - error: Error message if parsing failed, None otherwise
    """
    try:
        # Remove markdown code blocks if present
        cleaned_text = response_text.strip()
        if cleaned_text.startswith("```"):
            # Extract content between code fences
            parts = cleaned_text.split("```")
            if len(parts) >= 2:
                cleaned_text = parts[1]
                # Remove language identifier if present
                if cleaned_text.startswith("json"):
                    cleaned_text = cleaned_text[4:]
                cleaned_text = cleaned_text.strip()

        # Parse JSON
        data = json.loads(cleaned_text)
        return {
            "data": data,
            "error": None,
        }

    except json.JSONDecodeError as e:
        error_msg = f"Failed to parse JSON: {str(e)}"
        logging.warning(f"{error_msg}. Response text: {response_text}")
        return {
            "data": None,
            "error": error_msg,
        }


def generate_json_completion(
    system_prompt: str,
    user_prompt: str,
    model: str = LLM_CHEAP,
    temperature: float = 0.3,
    max_tokens: int = 500,
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Generate a JSON completion using OpenAI API with automatic parsing.

    This is a convenience function that combines generate_chat_completion
    and parse_json_response for JSON-based responses.

    Args:
        system_prompt: System message defining AI behavior
        user_prompt: User message with the actual request
        model: OpenAI model to use (default: LLM_CHEAP tier)
        temperature: Sampling temperature 0-1 (default: 0.3 for more deterministic JSON)
        max_tokens: Maximum tokens in the response (default: 500). Sent on the
            wire as OpenAI's `max_completion_tokens` (issue #365) — the Python
            parameter name is kept deliberately so the 13 existing call sites
            need no change.
        timeout: Optional per-call read/write budget in seconds. None
            (default) uses LLM_TIMEOUT_SECONDS; the LLM_CONNECT_TIMEOUT_SECONDS
            connect budget always applies. Forwarded unchanged to
            generate_chat_completion.

    Returns:
        Dict with:
        - data: Parsed JSON data (dict) if successful, None otherwise
        - raw_content: Raw text content from OpenAI
        - error: Error message if generation or parsing failed, None otherwise
    """
    # Generate completion
    completion_result = generate_chat_completion(
        system_prompt=system_prompt,
        user_prompt=user_prompt,
        model=model,
        temperature=temperature,
        max_tokens=max_tokens,
        timeout=timeout,
    )

    if completion_result["error"]:
        return {
            "data": None,
            "raw_content": None,
            "error": completion_result["error"],
        }

    # Parse JSON from response
    raw_content = completion_result["content"]
    parse_result = parse_json_response(raw_content)

    return {
        "data": parse_result["data"],
        "raw_content": raw_content,
        "error": parse_result["error"],
    }


def clamp_value(value: int, min_value: int, max_value: int) -> int:
    """
    Clamp a value between min and max bounds.

    Args:
        value: Value to clamp
        min_value: Minimum allowed value
        max_value: Maximum allowed value

    Returns:
        Clamped value
    """
    return max(min_value, min(max_value, value))


def truncate_text(text: str, max_length: int, suffix: str = "...") -> str:
    """
    Truncate text to maximum length, adding suffix if truncated.

    Args:
        text: Text to truncate
        max_length: Maximum length (including suffix)
        suffix: Suffix to add if truncated (default: "...")

    Returns:
        Truncated text with suffix if necessary
    """
    if len(text) <= max_length:
        return text

    return text[: max_length - len(suffix)] + suffix
