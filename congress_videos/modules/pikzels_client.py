"""Pikzels v2 API client — trimmed port for congress_videos.

Exposes only the subset of the Pikzels v2 API required by the
generic-thumbnail-generator DAG:

    - thumbnail_from_text
    - score_thumbnail
    - download
    - to_base64_data_url (module-level helper)

Excluded from this port (trimmed):
    thumbnail_from_image, edit_thumbnail, generate_titles,
    and all pikzonality methods.

Setup:
    export PIKZELS_API_KEY=pkz_...

Usage:
    from congress_videos.modules.pikzels_client import PikzelsClient

    client = PikzelsClient()
    result = client.thumbnail_from_text("El Congreso debate los presupuestos")
    dest = client.download(result["output"], "/tmp/thumb.png")
"""

from __future__ import annotations

import base64
import os
import random
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://api.pikzels.com"

MODELS = ("pkz_2", "pkz_3", "pkz_4", "pkz_4_5")
FORMATS = ("16:9", "9:16", "1:1")
SUPPORT_IMAGE_MODELS = ("pkz_4", "pkz_4_5")

# Error codes that should be retried with exponential backoff.
RETRYABLE_CODES = {
    "TOO_MANY_REQUESTS",
    "GENERATION_TIMEOUT",
    "SCORING_TIMEOUT",
    "TITLE_TIMEOUT",
    "GENERATION_FAILED",
    "INTERNAL_ERROR",
    "SERVICE_UNDER_MAINTENANCE",
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class PikzelsError(Exception):
    """Raised for Pikzels API errors, keyed on the public error code."""

    def __init__(self, code: str, message: str, request_id: str | None, status: int) -> None:
        super().__init__(f"[{status}] {code}: {message} (request_id={request_id})")
        self.code = code
        self.message = message
        self.request_id = request_id
        self.status = status


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class PikzelsClient:
    """Trimmed Pikzels v2 HTTP client with retry/exponential-backoff logic."""

    def __init__(
        self,
        api_key: str | None = None,
        timeout: int = 300,
        max_retries: int = 5,
    ) -> None:
        """Initialise the client.

        Args:
            api_key: Pikzels API key.  When omitted, read from the
                ``PIKZELS_API_KEY`` environment variable.
            timeout: Per-request timeout in seconds.
            max_retries: Maximum number of retries for retryable failures.
                The total attempt count is ``max_retries + 1``.

        Raises:
            EnvironmentError: When ``PIKZELS_API_KEY`` is absent or empty.
            ValueError: When the supplied key does not start with ``pkz_``.
        """
        self.api_key = api_key or os.getenv("PIKZELS_API_KEY")
        if not self.api_key:
            raise OSError("PIKZELS_API_KEY env var is not set and no api_key was passed")
        if not self.api_key.startswith("pkz_"):
            raise ValueError("Pikzels API keys must start with 'pkz_'")
        self.timeout = timeout
        self.max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update(
            {
                "X-Api-Key": self.api_key,
                "Content-Type": "application/json",
            }
        )

    # ------------------------------------------------------------------
    # Internal: request with retry / exponential backoff
    # ------------------------------------------------------------------

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        """Execute an authenticated HTTP request, retrying retryable errors.

        Args:
            method: HTTP verb (e.g. ``"POST"``, ``"GET"``).
            path: API path relative to ``BASE_URL``.
            payload: JSON body to send.

        Returns:
            Parsed JSON response body as a dict.

        Raises:
            PikzelsError: On non-retryable HTTP errors or exhausted retries.
        """
        url = f"{BASE_URL}{path}"
        last_error: PikzelsError | None = None

        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.request(method, url, json=payload, timeout=self.timeout)
            except (requests.Timeout, requests.ConnectionError) as exc:
                # Network-level errors are retryable.
                if attempt == self.max_retries:
                    raise PikzelsError(
                        code="NETWORK_ERROR",
                        message=str(exc),
                        request_id=None,
                        status=0,
                    ) from exc
                wait = min(60, 2**attempt) + random.random()
                time.sleep(wait)
                continue

            if response.status_code == 200:
                return response.json()

            body = response.json() if response.content else {}
            error = body.get("error", {})
            err = PikzelsError(
                code=error.get("code", "UNKNOWN"),
                message=error.get("message", response.text[:200]),
                request_id=body.get("request_id") or response.headers.get("X-Request-Id"),
                status=response.status_code,
            )

            if err.code not in RETRYABLE_CODES or attempt == self.max_retries:
                raise err

            last_error = err
            wait = min(60, 2**attempt) + random.random()
            time.sleep(wait)

        raise last_error  # unreachable; satisfies type checkers

    # ------------------------------------------------------------------
    # Thumbnails
    # ------------------------------------------------------------------

    def thumbnail_from_text(
        self,
        prompt: str,
        model: str = "pkz_4_5",
        format: str = "16:9",
        persona: str | None = None,
        style: str | None = None,
        support_image_url: str | None = None,
        support_image_base64: str | None = None,
    ) -> dict:
        """Generate a thumbnail from a text prompt.

        Args:
            prompt: Descriptive text for the thumbnail.
            model: Pikzels model identifier.
            format: Aspect ratio (``"16:9"``, ``"9:16"``, or ``"1:1"``).
            persona: Optional pikzonality persona ID.
            style: Optional pikzonality style ID.
            support_image_url: URL of a support/reference image.
            support_image_base64: Base64 data-URI of a support/reference image.

        Returns:
            Dict containing ``output`` (image URL) and ``request_id``.
        """
        _check_model_support(model, support_image_url, support_image_base64)
        payload = _drop_none(
            {
                "prompt": prompt,
                "model": model,
                "format": format,
                "persona": persona,
                "style": style,
                "support_image_url": support_image_url,
                "support_image_base64": support_image_base64,
            }
        )
        return self._request("POST", "/v2/thumbnail/text", payload)

    def score_thumbnail(
        self,
        image_url: str | None = None,
        image_base64: str | None = None,
        title: str | None = None,
    ) -> dict:
        """Score a thumbnail using the locally-downloaded image.

        Exactly one of ``image_url`` or ``image_base64`` must be supplied.

        Args:
            image_url: Public URL of the image.
            image_base64: Base64 data-URI of the image (preferred — avoids
                relying on expiring Pikzels CDN URLs).
            title: Optional title to include in the scoring context.

        Returns:
            Dict containing ``main_score``, ``subscores``, ``suggestion``,
            and ``request_id``.
        """
        _check_xor(image_url=image_url, image_base64=image_base64)
        payload = _drop_none(
            {
                "image_url": image_url,
                "image_base64": image_base64,
                "title": title,
            }
        )
        return self._request("POST", "/v2/thumbnail/score", payload)

    def download(self, url: str, dest: str | Path) -> Path:
        """Download a generated output immediately.

        Pikzels-issued image URLs expire after 24 hours, so this method
        must be called before the expiry window.

        Args:
            url: Remote URL of the generated thumbnail.
            dest: Local filesystem destination path.

        Returns:
            The resolved :class:`pathlib.Path` of the saved file.
        """
        dest = Path(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        response = requests.get(url, timeout=self.timeout)
        response.raise_for_status()
        dest.write_bytes(response.content)
        return dest


# ---------------------------------------------------------------------------
# Module-level convenience functions
# ---------------------------------------------------------------------------


def thumbnail_from_text(
    prompt: str,
    model: str = "pkz_4_5",
    format: str = "16:9",
    persona: str | None = None,
    style: str | None = None,
    support_image_url: str | None = None,
    support_image_base64: str | None = None,
) -> dict:
    """Module-level shorthand: creates a ``PikzelsClient`` and calls
    :meth:`PikzelsClient.thumbnail_from_text`.

    See :meth:`PikzelsClient.thumbnail_from_text` for full documentation.
    """
    return PikzelsClient().thumbnail_from_text(
        prompt=prompt,
        model=model,
        format=format,
        persona=persona,
        style=style,
        support_image_url=support_image_url,
        support_image_base64=support_image_base64,
    )


def score_thumbnail(
    image_url: str | None = None,
    image_base64: str | None = None,
    title: str | None = None,
) -> dict:
    """Module-level shorthand: creates a ``PikzelsClient`` and calls
    :meth:`PikzelsClient.score_thumbnail`.

    See :meth:`PikzelsClient.score_thumbnail` for full documentation.
    """
    return PikzelsClient().score_thumbnail(
        image_url=image_url,
        image_base64=image_base64,
        title=title,
    )


def download(url: str, dest: str | Path) -> Path:
    """Module-level shorthand: creates a ``PikzelsClient`` and calls
    :meth:`PikzelsClient.download`.

    See :meth:`PikzelsClient.download` for full documentation.
    """
    return PikzelsClient().download(url=url, dest=dest)


def to_base64_data_url(image_bytes: bytes, mime_type: str) -> str:
    """Encode raw image bytes as a base64 data URI.

    Args:
        image_bytes: Raw image bytes (e.g. contents of a PNG file).
        mime_type: MIME type string (e.g. ``"image/png"``).

    Returns:
        A ``data:<mime_type>;base64,<encoded>`` string suitable for the
        Pikzels ``support_image_base64`` and ``image_base64`` fields.
    """
    encoded = base64.b64encode(image_bytes).decode()
    return f"data:{mime_type};base64,{encoded}"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _drop_none(payload: dict) -> dict:
    """Return a copy of ``payload`` with all ``None``-valued keys removed."""
    return {key: value for key, value in payload.items() if value is not None}


def _check_xor(**fields: object) -> None:
    """Assert exactly one of the keyword arguments is non-``None``.

    Args:
        **fields: Keyword arguments to check.

    Raises:
        ValueError: When zero or more than one field is provided.
    """
    provided = [name for name, value in fields.items() if value is not None]
    if len(provided) != 1:
        names = " / ".join(fields)
        raise ValueError(f"Provide exactly one of: {names}")


def _check_model_support(
    model: str,
    support_image_url: str | None,
    support_image_base64: str | None,
) -> None:
    """Validate that support-image arguments are only used with compatible models.

    Args:
        model: Pikzels model identifier.
        support_image_url: Support image URL (or ``None``).
        support_image_base64: Support image base64 data-URI (or ``None``).

    Raises:
        ValueError: When a support image is given with an incompatible model.
    """
    if (support_image_url or support_image_base64) and model not in SUPPORT_IMAGE_MODELS:
        raise ValueError(f"Support images require {' or '.join(SUPPORT_IMAGE_MODELS)}; got {model}")
