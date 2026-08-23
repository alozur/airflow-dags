"""Post-upload verification logic for YouTube uploads (issue #141).

This module implements the two-step state machine for verifying that uploaded
videos are actually live on YouTube. It is intentionally pure and dependency-
injected (http_get, youtube_service) so that it is trivially unit-testable.

State machine:
    Step 1 — oembed gate:
        GET https://www.youtube.com/oembed?url=...&format=json
        HTTP 200 → ok (no quota consumed)

    Step 2 — Data API fallback (only when oembed is non-200):
        videos.list(part="status,processingDetails")
        - processingStatus=succeeded OR privacyStatus in LIVE_PRIVACY_STATUSES → ok
        - processingStatus=processing → processing
        - empty items OR processingStatus=failed → abandoned
        - any exception (quota, network, etc.) → unknown (never abandoned)
"""
from __future__ import annotations

import logging
import os
from typing import Any, Callable

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (no magic numbers in callers)
# ---------------------------------------------------------------------------

VERIFY_WINDOW_MIN_HOURS: int = 1
VERIFY_WINDOW_MAX_HOURS: int = 48

# Per-run cap on Data API (videos.list) calls.
# oembed hits do NOT count toward this cap.
MAX_API_CALLS_PER_RUN: int = 50

# Template for the oembed check (Step 1).
OEMBED_URL: str = (
    "https://www.youtube.com/oembed"
    "?url=https://www.youtube.com/watch?v={video_id}&format=json"
)

HTTP_TIMEOUT_SECONDS: int = 10

# Staleness tolerance: skip replays older than this many minutes.
STALE_RUN_TOLERANCE_MINUTES: int = int(
    os.getenv("VERIFICATION_STALE_RUN_TOLERANCE_MINUTES", "30")
)

# Privacy statuses that indicate the video is live (not abandoned).
# "public" is handled by the oembed 200 path, but included here for
# completeness in case a public video returns non-200 from oembed.
LIVE_PRIVACY_STATUSES: frozenset[str] = frozenset({"private", "unlisted", "public"})


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

def check_video_status(
    video_id: str,
    *,
    http_get: Callable[..., Any],
    youtube_service: Any | None = None,
) -> tuple[str, str]:
    """Check whether a YouTube video is live, still processing, abandoned, or unknown.

    Args:
        video_id: YouTube video ID to verify (e.g. "dQw4w9WgXcQ").
        http_get: Callable with the same signature as ``requests.get``.
            Injected so callers can mock it in tests.
        youtube_service: A built googleapiclient YouTube resource, or None.
            When None, Step 2 is skipped and the result is "unknown"
            (the video will be retried on the next run).

    Returns:
        A ``(status, detail)`` tuple where ``status`` is one of:
        - ``"ok"``          — video is live; set upload_verified_at.
        - ``"processing"``  — video is still being processed; do nothing.
        - ``"abandoned"``   — video is gone or failed; call record_failure.
        - ``"unknown"``     — API/quota error; do nothing (not abandoned).
    """
    # ------------------------------------------------------------------
    # Step 1: oembed gate — quota-free, covers the happy-path majority
    # ------------------------------------------------------------------
    oembed_url = OEMBED_URL.format(video_id=video_id)
    try:
        resp = http_get(oembed_url, timeout=HTTP_TIMEOUT_SECONDS)
        if resp.status_code == 200:
            logger.debug("post_upload_verification: oembed 200 for %s → ok", video_id)
            return "ok", "oembed_200"
    except Exception as exc:
        logger.warning(
            "post_upload_verification: oembed request failed for %s: %s", video_id, exc
        )
        # Fall through to Step 2

    # ------------------------------------------------------------------
    # Step 2: Data API fallback — only reached on non-200 oembed
    # ------------------------------------------------------------------
    if youtube_service is None:
        logger.warning(
            "post_upload_verification: no youtube_service for %s, status unknown", video_id
        )
        return "unknown", "no_youtube_service"

    try:
        api_result = (
            youtube_service.videos()
            .list(id=video_id, part="status,processingDetails")
            .execute()
        )
    except Exception as exc:
        logger.warning(
            "post_upload_verification: Data API error for %s: %s — treating as unknown",
            video_id,
            exc,
        )
        return "unknown", f"api_error: {exc}"

    items = api_result.get("items", [])

    if not items:
        logger.info(
            "post_upload_verification: video %s absent from Data API → abandoned", video_id
        )
        return "abandoned", "empty_items"

    item = items[0]
    processing_status = (item.get("processingDetails") or {}).get("processingStatus")
    privacy_status = (item.get("status") or {}).get("privacyStatus")

    logger.debug(
        "post_upload_verification: video=%s processingStatus=%s privacyStatus=%s",
        video_id,
        processing_status,
        privacy_status,
    )

    if processing_status == "failed":
        return "abandoned", f"processingStatus={processing_status}"

    if processing_status == "processing":
        return "processing", "processingStatus=processing"

    # processingStatus=succeeded or privacyStatus in LIVE_PRIVACY_STATUSES → ok
    if processing_status == "succeeded" or privacy_status in LIVE_PRIVACY_STATUSES:
        return "ok", f"processingStatus={processing_status} privacyStatus={privacy_status}"

    # Any other combination is treated as still processing to avoid false abandonment
    return "processing", f"processingStatus={processing_status} privacyStatus={privacy_status}"
