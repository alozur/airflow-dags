"""Fetch published YouTube titles for a list of video ids.

Reads video ids from stdin (one per line), calls the YouTube Data API v3
videos.list endpoint in batches of 50 using YOUTUBE_API_KEY from the
environment, and writes one JSON object per line to stdout.

Runs INSIDE the production container so the API key never leaves the NAS.
Only public video metadata for our own channel is requested; no private data
is sent outward beyond the video ids, which are already public.
"""

import json
import os
import sys
import urllib.parse
import urllib.request

API_URL = "https://www.googleapis.com/youtube/v3/videos"
BATCH_SIZE = 50


def fetch_batch(video_ids: list[str], api_key: str) -> list[dict]:
    params = urllib.parse.urlencode(
        {
            "part": "snippet,contentDetails,statistics",
            "id": ",".join(video_ids),
            "key": api_key,
            "maxResults": BATCH_SIZE,
        }
    )
    with urllib.request.urlopen(f"{API_URL}?{params}", timeout=30) as response:
        payload = json.load(response)
    return payload.get("items", [])


def main() -> int:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        print("YOUTUBE_API_KEY not set", file=sys.stderr)
        return 1

    video_ids = [line.strip() for line in sys.stdin if line.strip()]
    for start in range(0, len(video_ids), BATCH_SIZE):
        batch = video_ids[start : start + BATCH_SIZE]
        try:
            items = fetch_batch(batch, api_key)
        except Exception as exc:  # noqa: BLE001 - one-off extraction script
            print(f"batch {start} failed: {exc}", file=sys.stderr)
            continue
        for item in items:
            snippet = item.get("snippet", {})
            statistics = item.get("statistics", {})
            print(
                json.dumps(
                    {
                        "youtube_video_id": item.get("id"),
                        "published_title": snippet.get("title"),
                        "published_description": snippet.get("description"),
                        "published_at": snippet.get("publishedAt"),
                        "duration": item.get("contentDetails", {}).get("duration"),
                        "view_count": statistics.get("viewCount"),
                        "like_count": statistics.get("likeCount"),
                        "comment_count": statistics.get("commentCount"),
                    },
                    ensure_ascii=False,
                )
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
