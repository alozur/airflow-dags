"""
Pure analytics helpers for video_analytics_checkpoints (issue #53).

No Airflow imports. No DB or API dependencies.
All functions are pure: same input -> same output, zero side effects.

Functions:
- pending_checkpoints(now, videos, collected) -> list[dict]
- parse_analytics_response(resp)             -> dict
- should_persist(metrics)                    -> bool
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from congress_videos.config.analytics_config import CHECKPOINTS, MAX_WINDOW_HOURS, METRIC_FIELDS


def pending_checkpoints(
    now: datetime,
    videos: list[dict[str, Any]],
    collected: set[tuple[str, str]],
) -> list[dict[str, Any]]:
    """Compute the set of (youtube_video_id, checkpoint) pairs that are due.

    Args:
        now: Current UTC datetime (caller-supplied for testability).
        videos: Rows from video_chapters. Each row must have:
            - chapter_id (int)
            - youtube_video_id (str | None)
            - youtube_upload_date (datetime, UTC-aware)
        collected: Set of (youtube_video_id, checkpoint) tuples already persisted.

    Returns:
        List of dicts {chapter_id, youtube_video_id, checkpoint} for each
        pending pair where:
        - youtube_video_id is not None
        - elapsed hours >= CHECKPOINTS[checkpoint]
        - elapsed hours <= MAX_WINDOW_HOURS (90d)
        - (youtube_video_id, checkpoint) not in collected
    """
    result: list[dict[str, Any]] = []

    for row in videos:
        yt_id = row.get("youtube_video_id")
        if not yt_id:
            continue

        upload_date: datetime = row["youtube_upload_date"]
        # Ensure both are timezone-aware for safe arithmetic.
        if upload_date.tzinfo is None:
            upload_date = upload_date.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)

        elapsed_hours = (now - upload_date).total_seconds() / 3600.0

        # Exclude videos outside the 90-day monitoring window.
        if elapsed_hours > MAX_WINDOW_HOURS:
            continue

        for label, threshold_hours in CHECKPOINTS.items():
            if elapsed_hours < threshold_hours:
                continue
            if (yt_id, label) in collected:
                continue
            result.append({
                "chapter_id": row["chapter_id"],
                "youtube_video_id": yt_id,
                "checkpoint": label,
            })

    return result


def parse_analytics_response(resp: dict[str, Any]) -> dict[str, Any | None]:
    """Map a YouTube Analytics API response to a flat metrics dict.

    Reads 'columnHeaders' and 'rows' from the API response. Returns a dict
    with exactly the five METRIC_FIELDS keys. Missing columns or an empty
    rows list yield None for the missing field.

    Args:
        resp: Raw dict from ``reports.query().execute()``.

    Returns:
        Dict with keys: views, impressions, impressionClickThroughRate,
        averageViewDuration, watchTimeMinutes. Value is None when the API
        did not return that column or returned no rows.
    """
    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", [])

    if not rows:
        return {field: None for field in METRIC_FIELDS}

    row = rows[0]
    col_index: dict[str, int] = {name: i for i, name in enumerate(headers)}

    metrics: dict[str, Any | None] = {}
    for field in METRIC_FIELDS:
        idx = col_index.get(field)
        metrics[field] = row[idx] if idx is not None else None

    return metrics


def should_persist(metrics: dict[str, Any | None]) -> bool:
    """Decide whether a snapshot row should be written.

    Returns False (skip-and-retry) when ALL metric values are None or zero.
    Returns True when at least one metric is non-None and non-zero.

    The Analytics API can return all-None or all-zero during the first few
    hours of a video's life (processing lag). Skipping keeps the
    (youtube_video_id, checkpoint) pair pending so the next DAG run retries.

    Args:
        metrics: Dict produced by parse_analytics_response.

    Returns:
        True if the snapshot is worth persisting, False otherwise.
    """
    for value in metrics.values():
        if value is not None and value != 0:
            return True
    return False
