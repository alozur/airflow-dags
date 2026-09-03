"""Thumbnail republish healer's pure state machine (issue #331).

This module implements the classification logic for retrying a failed
thumbnail publish on YouTube. It is intentionally pure and dependency-
injected (set_thumbnail_fn), mirroring congress_videos/modules/
post_upload_verification.py's check_video_status(http_get=...) pattern, so
it is trivially unit-testable and carries no googleapiclient import.

State machine (classify_republish_result):
    {"success": True}                              -> healed
    {"success": False, "error": "<missing-file>"}   -> abandon (no retry,
        no regeneration -- the missing-file check already runs inside
        set_thumbnail_for_video before any YouTube API quota is spent)
    {"success": False, "error": "<anything else>"}  -> retry
    None / {} / no usable error text                -> retry

Hard import constraint (design DD5): this module lives under a tree the
scheduler walks looking for workflow definitions, and a helper module whose
source text names both the orchestration framework and its scheduling unit
crashes that walk. This file therefore names neither. It has zero
import-time side effects (no DB connection, no token resolution, no
filesystem probe) beyond the module-level constants and logger below.
"""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

from congress_videos.modules.database import THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants (no magic numbers in callers)
# ---------------------------------------------------------------------------

# Per-run cap on set_thumbnail_for_video calls (each one spends YouTube API
# quota), mirroring post_upload_verification.MAX_API_CALLS_PER_RUN.
MAX_THUMBNAIL_CALLS_PER_RUN: int = 20

# Reuses database.py's THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT rather than
# defining a second, independently-drifting literal for the same number
# (resolves WU2a's deviation #2 -- see apply-progress).
CANDIDATE_LIMIT: int = THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT

# Staleness tolerance: skip replays older than this many minutes. 180, not
# the 30 used for the hourly post_upload_verification workflow, because this
# healer runs once a day (DD6) -- 30 minutes would skip legitimate runs
# merely delayed by NAS I/O contention.
STALE_RUN_TOLERANCE_MINUTES: int = int(os.getenv("THUMBNAIL_REPUBLISH_STALE_TOLERANCE_MINUTES", "180"))

THUMBNAIL_FILENAME: str = "thumbnail.png"

# Verbatim prefix set_thumbnail_for_video returns before it spends any
# YouTube API quota (utils/youtube_helpers.py:272) -- the sole signal this
# module relies on to distinguish "file missing, abandon" from any other,
# potentially transient, failure.
MISSING_FILE_ERROR_PREFIX: str = "Thumbnail file not found:"


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------


def thumbnail_path_for(output_path: str) -> str:
    """Return the sidecar thumbnail path for a turn's output video.

    Replays the exact rule already executed at upload time
    (congress_videos/modules/youtube/youtube_upload.py:57,72) -- an
    existing contract, not a new one.

    Args:
        output_path: Absolute path to the turn's materialized video.mp4.

    Returns:
        Absolute path to the sibling thumbnail.png.
    """
    return os.path.join(os.path.dirname(output_path), THUMBNAIL_FILENAME)


def classify_republish_result(result: dict | None) -> tuple[str, str]:
    """Classify a set_thumbnail_for_video-shaped result dict.

    Args:
        result: Dict with a "success" key and, on failure, an "error" key
            (utils/youtube_helpers.py:set_thumbnail_for_video), or None.

    Returns:
        A ``(status, detail)`` tuple where ``status`` is one of:
        - ``"healed"``  — thumbnail published; call mark_turn_thumbnail_republished.
        - ``"retry"``   — transient failure; call
          record_turn_thumbnail_republish_failure(abandon=False).
        - ``"abandon"`` — thumbnail.png missing on disk; call
          record_turn_thumbnail_republish_failure(abandon=True), never
          regenerate.
    """
    if result and result.get("success"):
        return "healed", "success"

    error = (result or {}).get("error") if result else None
    if not error:
        return "retry", "no_result"

    if error.startswith(MISSING_FILE_ERROR_PREFIX):
        return "abandon", error

    return "retry", error


def attempt_thumbnail_republish(
    output_path: str,
    *,
    set_thumbnail_fn: Callable[[str], dict],
) -> tuple[str, str]:
    """Invoke the injected republish call and classify its outcome.

    Dependency-injected so this module never imports googleapiclient — the
    calling workflow binds ``youtube`` and ``video_id`` into
    ``set_thumbnail_fn`` before passing it in, the same split
    post_upload_verification's caller uses to keep heavy imports inside
    task callables (DD5).

    Any exception from ``set_thumbnail_fn`` is caught here and classified
    as "retry" rather than propagated, so one bad candidate never aborts a
    single run processing several candidates (spec: per-item try/except
    isolation).

    Args:
        output_path: Absolute path shared by the candidate turn's siblings.
        set_thumbnail_fn: Callable taking the derived thumbnail path and
            returning a set_thumbnail_for_video-shaped result dict.

    Returns:
        Same ``(status, detail)`` shape as classify_republish_result.
    """
    thumbnail_path = thumbnail_path_for(output_path)
    try:
        result = set_thumbnail_fn(thumbnail_path)
    except Exception as exc:
        logger.warning(
            "thumbnail_republish: set_thumbnail_fn raised for %s: %s",
            output_path,
            exc,
        )
        return "retry", str(exc)

    return classify_republish_result(result)
