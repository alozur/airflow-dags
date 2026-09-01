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
"""

import os

LLM_DEFAULT: str = os.getenv("LLM_DEFAULT") or "gpt-5.6-luna"
LLM_CHEAP: str = os.getenv("LLM_CHEAP") or "gpt-5-nano"
