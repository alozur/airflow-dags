"""
Analytics configuration for video_analytics_checkpoints.

Plain Python dict — no Airflow Variables, no runtime dependencies.
Import-safe in any context (pure Python module).
"""

# Checkpoint labels mapped to their elapsed-time thresholds in hours.
# A snapshot is collected once per (youtube_video_id, checkpoint) pair
# when the video age >= CHECKPOINTS[label] hours and no snapshot exists yet.
CHECKPOINTS: dict[str, int] = {
    "24h": 24,
    "48h": 48,
    "7d": 168,
    "30d": 720,
    "90d": 2160,
}

# Maximum monitoring window in hours (90 days).
# Videos uploaded more than this many hours ago are excluded from collection.
MAX_WINDOW_HOURS: int = 2160

# Ordered list of metric field names expected in every snapshot JSONB payload.
# Matches the columnHeaders returned by the YouTube Analytics API reports.query.
#
# Every name below is a metric the YouTube Analytics API actually supports for a
# per-video channel report (ids=channel==MINE, filters=video==ID, no dimensions,
# read-only scope). They can all be requested in a single query.
#
# Deliberately NOT included, because the YouTube Analytics API does not expose
# them for channel reports and adding them makes the whole query fail with
# "Unknown identifier ... given in field parameters.metrics":
#   - impressions / impressionClickThroughRate: thumbnail impressions and CTR
#     live only in the YouTube Studio UI; there is no public API for them.
#     (adImpressions is AD impressions — a different metric, monetary-scope only.)
#   - watchTimeMinutes: not a real metric name; watch time is estimatedMinutesWatched.
#   - revenue/ad metrics (estimatedRevenue, cpm, ...): unsupported in channel reports.
METRIC_FIELDS: list[str] = [
    "views",
    "estimatedMinutesWatched",
    "averageViewDuration",
    "averageViewPercentage",
    "likes",
    "dislikes",
    "comments",
    "shares",
    "subscribersGained",
    "subscribersLost",
]

# ---------------------------------------------------------------------------
# Checkpoint action evaluation (issue #102)
# ---------------------------------------------------------------------------

# A video is "underperforming" at a checkpoint when its views fall below this
# ratio of the channel's own historical median views for that checkpoint.
UNDERPERFORM_RATIO: float = 0.5

# Minimum number of OTHER videos' snapshots required at a checkpoint before
# evaluate_action() will judge underperformance there. Enforced arithmetically
# as (sample_size - 1) >= MIN_PRIOR_SNAPSHOTS, i.e. the evaluated video's own
# snapshot is excluded from the count (but not from the median computation).
MIN_PRIOR_SNAPSHOTS: int = 10

# Title regeneration is only evaluated at these checkpoints. Thumbnail
# regeneration MAY occur at any checkpoint in CHECKPOINTS.
TITLE_UPDATE_CHECKPOINTS: tuple[str, ...] = ("24h",)

# Lifetime cap per video, across all checkpoints, on each action type.
MAX_THUMBNAIL_ACTIONS_PER_VIDEO: int = 1
MAX_TITLE_ACTIONS_PER_VIDEO: int = 1

# Mirrors the migration 041 CHECK constraint on
# video_analytics_snapshots.action_taken. Kept in sync manually — if this
# set changes, migration 041 (or a follow-up migration) must change too.
ACTION_VALUES: set[str] = {
    "cold_start",
    "ok",
    "capped",
    "in_progress",
    "thumbnail_regenerated",
    "thumbnail_and_title_regenerated",
    "failed",
}
