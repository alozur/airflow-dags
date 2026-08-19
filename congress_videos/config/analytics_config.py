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
