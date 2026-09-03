"""Shared fixtures for tests/utils.

Autouse cache reset for utils.whisper_helpers' process-lifetime model cache
(issue #202) — without this, a model mocked/loaded by one test would leak
into the next test that requests the same model_size.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _clear_whisper_model_cache():
    from utils.whisper_helpers import clear_model_cache

    clear_model_cache()
    yield
    clear_model_cache()
