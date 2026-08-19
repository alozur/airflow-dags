"""HTTP YAMNet backend for applause detection in speaker turns.

Provides the default production ``api_yamnet_fn`` used by
:func:`congress_videos.modules.trim_proposals.generate_trim_proposals`. It
calls the persistent ``yamnet-api`` FastAPI sidecar over HTTP and parses its
applause-interval JSON.

Kept separate so ``trim_proposals.py`` stays pure — it imports no HTTP
machinery and is fully unit-testable with a stubbed ``yamnet_fn``. Here the
``poster`` (``requests.post`` by default) is injected so tests mock at the
HTTP boundary and never make a real network connection.

On any transport or parse failure the function raises :class:`SidecarApiError`
— it never returns an empty list silently or hangs indefinitely.  This
preserves the ``YamnetFn -> list[dict]`` contract.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

import requests

from congress_videos.modules.sidecar_api_error import SidecarApiError

logger = logging.getLogger(__name__)

YAMNET_API_HOST = os.environ.get("YAMNET_API_HOST", "yamnet-api")
YAMNET_API_PORT = os.environ.get("YAMNET_API_PORT", "8081")
YAMNET_API_URL = f"http://{YAMNET_API_HOST}:{YAMNET_API_PORT}"

# YAMNet runs faster than pyannote; 30 min timeout is generous.
_DEFAULT_TIMEOUT_SECONDS = 30 * 60


def api_yamnet_fn(
    wav_path: str,
    offset: float,
    *,
    poster: Callable[..., object] = requests.post,
    timeout: int | None = _DEFAULT_TIMEOUT_SECONDS,
) -> list[dict]:
    """Run YAMNet applause detection via the yamnet-api sidecar; return intervals.

    POSTs the WAV as a multipart file upload alongside ``offset`` (sent as a
    form field so the server applies offset adjustment server-side before
    returning). Returns the ``applause_intervals`` list from the API response.

    Matches the :data:`congress_videos.modules.trim_proposals.YamnetFn`
    contract.

    Args:
        wav_path: Absolute path to the turn's WAV file.
        offset:   Turn start in absolute session seconds.  The server adds
                  this to every ``start`` and ``end`` before returning.
        poster:   Injectable HTTP POST callable (default ``requests.post``).
        timeout:  Request timeout in seconds.

    Returns:
        List of ``{start, end, max_score}`` dicts with absolute timestamps.

    Raises:
        SidecarApiError: On connection refused, timeout, non-200 HTTP status,
            or malformed JSON response.
    """
    url = f"{YAMNET_API_URL}/detect"
    wav = Path(wav_path)

    logger.info("yamnet-api: POST %s (offset=%.3f)", url, float(offset))

    try:
        with wav.open("rb") as audio_fh:
            resp = poster(
                url,
                files={"audio_file": (wav.name, audio_fh, "audio/wav")},
                data={"offset": float(offset)},
                timeout=timeout,
            )
    except requests.exceptions.Timeout as exc:
        raise SidecarApiError(
            f"yamnet-api timed out after {timeout}s at {YAMNET_API_URL}"
        ) from exc
    except requests.exceptions.ConnectionError as exc:
        raise SidecarApiError(
            f"yamnet-api unreachable at {YAMNET_API_URL}: {exc}"
        ) from exc

    if resp.status_code != 200:
        raise SidecarApiError(
            f"yamnet-api returned HTTP {resp.status_code} for {url}"
        )

    try:
        payload = resp.json()
    except Exception as exc:
        raise SidecarApiError(
            f"yamnet-api malformed JSON response from {url}: {exc}"
        ) from exc

    return payload["applause_intervals"]
