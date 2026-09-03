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

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

from congress_videos.config.analytics_config import (
    CHECKPOINTS,
    MAX_THUMBNAIL_ACTIONS_PER_VIDEO,
    MAX_TITLE_ACTIONS_PER_VIDEO,
    MAX_WINDOW_HOURS,
    METRIC_FIELDS,
    MIN_PRIOR_SNAPSHOTS,
    TITLE_UPDATE_CHECKPOINTS,
    UNDERPERFORM_RATIO,
)


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
            upload_date = upload_date.replace(tzinfo=UTC)
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)

        elapsed_hours = (now - upload_date).total_seconds() / 3600.0

        # Exclude videos outside the 90-day monitoring window.
        if elapsed_hours > MAX_WINDOW_HOURS:
            continue

        for label, threshold_hours in CHECKPOINTS.items():
            if elapsed_hours < threshold_hours:
                continue
            if (yt_id, label) in collected:
                continue
            result.append(
                {
                    "chapter_id": row["chapter_id"],
                    "youtube_video_id": yt_id,
                    "checkpoint": label,
                }
            )

    return result


def parse_analytics_response(resp: dict[str, Any]) -> dict[str, Any | None]:
    """Map a YouTube Analytics API response to a flat metrics dict.

    Reads 'columnHeaders' and 'rows' from the API response. Returns a dict
    with exactly the METRIC_FIELDS keys. Missing columns or an empty
    rows list yield None for the missing field.

    Args:
        resp: Raw dict from ``reports.query().execute()``.

    Returns:
        Dict keyed by every name in ``METRIC_FIELDS`` (views,
        estimatedMinutesWatched, averageViewDuration, averageViewPercentage,
        likes, dislikes, comments, shares, subscribersGained,
        subscribersLost). Value is None when the API did not return that
        column or returned no rows.
    """
    headers = [h["name"] for h in resp.get("columnHeaders", [])]
    rows = resp.get("rows", [])

    if not rows:
        return dict.fromkeys(METRIC_FIELDS)

    row = rows[0]
    col_index: dict[str, int] = {name: i for i, name in enumerate(headers)}

    metrics: dict[str, Any | None] = {}
    for field in METRIC_FIELDS:
        idx = col_index.get(field)
        metrics[field] = row[idx] if idx is not None else None

    return metrics


def _cap_reached(prior_actions: dict[str, int], checkpoint: str) -> bool:
    """Return True when a relevant lifetime action cap is already consumed.

    Thumbnail actions are relevant at every checkpoint. Title actions are
    only relevant at TITLE_UPDATE_CHECKPOINTS (24h). Either cap being
    reached is sufficient to mark the checkpoint 'capped' — there is no
    partial action_taken value for "thumbnail only, title already capped".
    """
    thumbnail_count = prior_actions.get("thumbnail", 0)
    if thumbnail_count >= MAX_THUMBNAIL_ACTIONS_PER_VIDEO:
        return True

    if checkpoint in TITLE_UPDATE_CHECKPOINTS:
        title_count = prior_actions.get("title", 0)
        if title_count >= MAX_TITLE_ACTIONS_PER_VIDEO:
            return True

    return False


def evaluate_action(
    views: float | int,
    median_views: float | int,
    sample_size: int,
    checkpoint: str,
    prior_actions: dict[str, int],
) -> str:
    """Decide the action_taken literal for one (video, checkpoint) pair.

    Pure function — no I/O. Gate order (load-bearing, precedence top to
    bottom): capped -> cold_start -> ok -> act.

    Args:
        views: The evaluated video's views at this checkpoint.
        median_views: Channel-wide median views at this checkpoint (computed
            via a single grouped query across all checkpoints; MAY include
            the evaluated video's own snapshot — self-inclusion is strictly
            conservative, see spec).
        sample_size: Total snapshot count feeding the median, INCLUDING the
            evaluated video's own row. The gate excludes self arithmetically:
            (sample_size - 1) >= MIN_PRIOR_SNAPSHOTS.
        checkpoint: One of '24h','48h','7d','30d','90d'.
        prior_actions: {'thumbnail': int, 'title': int} counts of consumed
            lifetime cap slots for this video, where 'in_progress' rows
            count as consumed slots alongside completed records.

    Returns:
        One of: 'capped', 'cold_start', 'ok',
        'thumbnail_regenerated', 'thumbnail_and_title_regenerated'.
    """
    if _cap_reached(prior_actions, checkpoint):
        return "capped"

    if sample_size - 1 < MIN_PRIOR_SNAPSHOTS:
        return "cold_start"

    if median_views <= 0 or views >= UNDERPERFORM_RATIO * median_views:
        return "ok"

    if checkpoint in TITLE_UPDATE_CHECKPOINTS:
        return "thumbnail_and_title_regenerated"
    return "thumbnail_regenerated"


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
