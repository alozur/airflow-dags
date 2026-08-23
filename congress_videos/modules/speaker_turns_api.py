"""HTTP diarization backend for speaker-turn detection.

Provides the default production ``api_diarize_fn`` used by
:func:`congress_videos.modules.speaker_turns.detect_turns`. It calls the
persistent ``diarize-api`` FastAPI sidecar over HTTP and parses its
speaker-change JSON.

Kept in a separate module so ``speaker_turns.py`` stays pure — it imports no
HTTP machinery and is fully unit-testable with a stubbed ``diarize_fn``. Here
the ``poster`` (``requests.post`` by default) is injected so tests mock at the
HTTP boundary and never make a real network connection.

On any transport or parse failure the function raises :class:`SidecarApiError`
— it never returns an empty list silently.  This preserves the
``DiarizeFn -> list[dict]`` contract: callers that need to handle failures must
catch the exception.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable

import requests

from congress_videos.modules.sidecar_api_error import SidecarApiError

logger = logging.getLogger(__name__)

DIARIZE_API_HOST = os.environ.get("DIARIZE_API_HOST", "diarize-api")
DIARIZE_API_PORT = os.environ.get("DIARIZE_API_PORT", "8080")
DIARIZE_API_URL = f"http://{DIARIZE_API_HOST}:{DIARIZE_API_PORT}"

_DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60  # 6 h — diarization is slow


def api_diarize_fn(
    wav_path: str,
    chapter_offset: float = 0.0,
    *,
    poster: Callable[..., object] = requests.post,
    timeout: int | None = _DEFAULT_TIMEOUT_SECONDS,
) -> list[dict]:
    """Diarize ``wav_path`` via the diarize-api sidecar; return speaker changes.

    POSTs the WAV as a multipart file upload alongside ``chapter_offset`` (sent
    as a form field so the server applies offset adjustment server-side).
    Returns the ``speaker_changes`` list from the API response.

    Matches the :data:`congress_videos.modules.speaker_turns.DiarizeFn`
    contract.

    Args:
        wav_path:       Absolute path to the chapter WAV file.
        chapter_offset: Chapter start in absolute session seconds.  The server
                        adds this to every ``start_seconds`` before returning.
        poster:         Injectable HTTP POST callable (default ``requests.post``).
        timeout:        Request timeout in seconds.

    Returns:
        List of ``{start_seconds, from_speaker, to_speaker,
        confirmed_block_duration_seconds}`` dicts.

    Raises:
        SidecarApiError: On connection refused, timeout, non-200 HTTP status,
            or malformed JSON response.
    """
    url = f"{DIARIZE_API_URL}/diarize"
    wav = Path(wav_path)

    logger.info("diarize-api: POST %s (offset=%.3f)", url, chapter_offset)

    try:
        with wav.open("rb") as audio_fh:
            resp = poster(
                url,
                files={"audio_file": (wav.name, audio_fh, "audio/wav")},
                data={"chapter_offset": chapter_offset},
                timeout=timeout,
            )
    except requests.exceptions.Timeout as exc:
        raise SidecarApiError(
            f"diarize-api timed out after {timeout}s at {DIARIZE_API_URL}"
        ) from exc
    except requests.exceptions.ConnectionError as exc:
        raise SidecarApiError(
            f"diarize-api unreachable at {DIARIZE_API_URL}: {exc}"
        ) from exc

    if resp.status_code != 200:
        raise SidecarApiError(
            f"diarize-api returned HTTP {resp.status_code} for {url}"
        )

    try:
        payload = resp.json()
    except Exception as exc:
        raise SidecarApiError(
            f"diarize-api malformed JSON response from {url}: {exc}"
        ) from exc

    return payload["speaker_changes"]


def check_diarize_api_health(
    *,
    getter: Callable[..., object] = requests.get,
    timeout: int = 5,
) -> None:
    """Probe diarize-api /health; raise SidecarApiError if unreachable.

    Reachability only — does not verify model readiness (accepted).

    Args:
        getter:  Injectable HTTP GET callable (default ``requests.get``).
        timeout: Request timeout in seconds.

    Raises:
        SidecarApiError: On timeout, connection error, or non-200 HTTP status.
    """
    url = f"{DIARIZE_API_URL}/health"
    try:
        resp = getter(url, timeout=timeout)
    except requests.exceptions.Timeout as exc:
        raise SidecarApiError(
            f"diarize-api health probe timeout after {timeout}s at {url}. "
            "Check the diarize-api sidecar container is running and that "
            "the whisper_network docker bridge is up (Synology/DSM firewall "
            "can drop inter-container TCP on new bridges)."
        ) from exc
    except requests.exceptions.RequestException as exc:
        raise SidecarApiError(
            f"diarize-api unreachable at {url}: {exc}. "
            "Check the diarize-api sidecar container is running and that "
            "the whisper_network docker bridge is up (Synology/DSM firewall "
            "can drop inter-container TCP on new bridges)."
        ) from exc
    if resp.status_code != 200:
        raise SidecarApiError(
            f"diarize-api unhealthy: HTTP {resp.status_code} from {url}. "
            "Verify the diarize-api sidecar container finished startup."
        )
