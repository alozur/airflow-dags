"""Centralized model-tier constants for OpenAI-compatible completions.

Two named tiers resolve from the environment at import time, each with a
committed fallback so the pipeline keeps working with zero configuration:

- ``LLM_DEFAULT``: general-purpose completions (chat, summaries, titles).
- ``LLM_CHEAP``: high-volume, low-complexity completions (JSON extraction,
  scoring, classification) where a cheaper model is acceptable.

Any call site that hardcodes a model literal should import one of these two
constants instead, so operators can retier the whole pipeline by setting one
environment variable per tier rather than editing source.

``os.getenv(...) or "<fallback>"`` (not the ``os.getenv(key, default)`` form)
is deliberate: container orchestration commonly passes through an unset
override as an empty string (for example ``"${LLM_DEFAULT:-}"`` in compose),
which is not ``None`` and would otherwise silently send an empty model value
to the completion API.

Two additional constants, ``LLM_TIMEOUT_SECONDS`` and
``LLM_CONNECT_TIMEOUT_SECONDS``, bound every OpenAI request with an
operator-tunable timeout budget (issue #355), replacing the SDK's own
600s-read/5s-connect default that otherwise lets a single stalled request
block a task indefinitely.
"""

import math
import os


def _positive_float_env(name: str, fallback: float) -> float:
    """Resolve a positive, finite float from the environment, or fail loud.

    ``if not raw`` is the float-typed analog of this module's ``os.getenv(...)
    or "<fallback>"`` rule: compose passes an unset override through as the
    EMPTY STRING (for example ``"${LLM_TIMEOUT_SECONDS:-}"``), which means
    "unset", not "malformed". Anything else that is not a positive finite
    number is an operator typo and is raised at import so it surfaces in
    ``airflow dags list-import-errors`` (the release gate after every
    git_sync_dag) instead of in a stalled task. ``inf`` is rejected on
    purpose: the SDK reads an unbounded timeout as the exact bug issue #355
    fixes. ``nan`` is rejected because ``nan <= 0`` is False and it would
    otherwise slip through into undefined httpx behaviour.
    """
    raw = os.getenv(name)
    if not raw:
        return fallback
    problem = (
        f"{name}={raw!r} is not a valid timeout: expected a positive, finite "
        f'number of seconds (for example "{fallback}"). An empty value means '
        f'"unset" and falls back to {fallback}.'
    )
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(problem) from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(problem)
    return value


LLM_DEFAULT: str = os.getenv("LLM_DEFAULT") or "gpt-5.6-luna"
LLM_CHEAP: str = os.getenv("LLM_CHEAP") or "gpt-5-nano"

LLM_TIMEOUT_SECONDS: float = _positive_float_env("LLM_TIMEOUT_SECONDS", 120.0)
LLM_CONNECT_TIMEOUT_SECONDS: float = _positive_float_env("LLM_CONNECT_TIMEOUT_SECONDS", 10.0)
