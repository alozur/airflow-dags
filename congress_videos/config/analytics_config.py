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
METRIC_FIELDS: list[str] = [
    "views",
    "impressions",
    "impressionClickThroughRate",
    "averageViewDuration",
    "watchTimeMinutes",
]
