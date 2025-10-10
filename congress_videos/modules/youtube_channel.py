"""
YouTube channel monitoring operations.

This module handles fetching and filtering videos from YouTube channels,
specifically for monitoring the Congress YouTube channel for plenary sessions.
"""

import logging
import os
import re
from datetime import datetime

from googleapiclient.discovery import build


def fetch_youtube_channel_videos(channel_id: str, max_results: int = 10):
    """
    Fetch recent videos from YouTube channel using YouTube Data API.

    Uses API key authentication (no OAuth needed for public data).

    Args:
        channel_id: YouTube channel ID
        max_results: Maximum number of videos to fetch

    Returns:
        Dict with video information:
        - total_videos: Number of videos found
        - videos: List of video details

    Raises:
        ValueError: If API key is missing
        RuntimeError: If channel not found or API request fails
    """
    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    logging.info(f"Fetching STREAM videos from YouTube channel: {channel_id}")

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        # Search for completed LIVE STREAMS only (not regular uploads)
        # This specifically queries the channel's "Streams" tab
        # eventType='completed' ensures we only get finished broadcasts
        search_response = youtube.search().list(
            part='snippet',
            channelId=channel_id,
            maxResults=max_results,
            order='date',
            type='video',
            eventType='completed'  # Only completed broadcasts (finished streams)
        ).execute()

        videos = []
        for item in search_response.get('items', []):
            video_data = {
                'video_id': item['id']['videoId'],
                'title': item['snippet']['title'],
                'description': item['snippet']['description'],
                'published_at': item['snippet']['publishedAt'],
                'thumbnail_url': item['snippet']['thumbnails']['high']['url'],
                'channel_title': item['snippet']['channelTitle'],
            }
            videos.append(video_data)

        logging.info(f"Found {len(videos)} videos from channel")
        return {
            'total_videos': len(videos),
            'videos': videos
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error fetching YouTube videos: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e


def filter_plenary_session_videos(channel_videos, target_title: str, target_date: str):
    """
    Filter videos for "Sesión Plenaria (original)" based on title and date.

    Args:
        channel_videos: Results from fetch_youtube_channel_videos
        target_title: Title to filter for (e.g., "Sesión Plenaria (original)")
        target_date: Target date in YYYY-MM-DD format

    Returns:
        Dict with filtered videos:
        - total_matches: Number of matching videos
        - videos: List of matching video details
        - target_date: Target date used for filtering
    """
    if not channel_videos or not channel_videos.get('videos'):
        logging.warning("No channel videos to filter")
        return {
            'total_matches': 0,
            'videos': [],
            'target_date': target_date
        }

    target_date_obj = datetime.strptime(target_date, "%Y-%m-%d").date()
    logging.info(f"Filtering for videos with title containing '{target_title}' on {target_date}")

    matching_videos = []
    for video in channel_videos['videos']:
        # Check if title contains target string (case-insensitive)
        if target_title.lower() in video['title'].lower():
            # Parse published date
            published_at = datetime.fromisoformat(video['published_at'].replace('Z', '+00:00'))
            published_date = published_at.date()

            # Check if date matches
            if published_date == target_date_obj:
                logging.info(f"Match found: {video['title']} - {video['video_id']}")
                matching_videos.append(video)

    logging.info(f"Found {len(matching_videos)} matching videos for {target_date}")
    return {
        'total_matches': len(matching_videos),
        'videos': matching_videos,
        'target_date': target_date
    }


def get_video_details(plenary_videos):
    """
    Get detailed information for videos (duration, timing, etc.).

    Note: We already filtered for completed streams, so no need to check status again.

    Args:
        plenary_videos: Results from filter_plenary_session_videos

    Returns:
        Dict with enriched video information:
        - total_videos: Number of videos
        - videos: List of video details with duration, timing, etc.
    """
    if not plenary_videos or not plenary_videos.get('videos'):
        logging.warning("No plenary videos to process")
        return {'total_videos': 0, 'videos': []}

    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        enriched_videos = []
        for video in plenary_videos['videos']:
            video_id = video['video_id']

            # Get detailed video information
            video_response = youtube.videos().list(
                part='snippet,contentDetails,liveStreamingDetails',
                id=video_id
            ).execute()

            if not video_response.get('items'):
                logging.warning(f"Video not found: {video_id}")
                continue

            video_details = video_response['items'][0]
            live_details = video_details.get('liveStreamingDetails', {})

            # Extract duration
            duration_iso = video_details['contentDetails']['duration']

            # Parse ISO 8601 duration (PT2H30M15S -> 2:30:15)
            duration_match = re.match(r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?', duration_iso)
            if duration_match:
                hours = int(duration_match.group(1) or 0)
                minutes = int(duration_match.group(2) or 0)
                seconds = int(duration_match.group(3) or 0)
                duration_seconds = hours * 3600 + minutes * 60 + seconds
            else:
                duration_seconds = 0

            enriched_video = {
                **video,
                'duration_seconds': duration_seconds,
                'duration_formatted': f"{hours}:{minutes:02d}:{seconds:02d}",
                'actual_start_time': live_details.get('actualStartTime'),
                'actual_end_time': live_details.get('actualEndTime'),
                'youtube_url': f"https://www.youtube.com/watch?v={video_id}",
            }

            logging.info(f"Video details: {video['title']} - Duration: {enriched_video['duration_formatted']}")
            enriched_videos.append(enriched_video)

        logging.info(f"Total videos enriched: {len(enriched_videos)}")
        return {
            'total_videos': len(enriched_videos),
            'videos': enriched_videos
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error getting video details: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e


def get_video_descriptions(plenary_videos):
    """
    Get full descriptions for videos.

    The search API only returns truncated descriptions, so we need to fetch
    full descriptions separately.

    Args:
        plenary_videos: Results from filter_plenary_session_videos

    Returns:
        Dict with video descriptions:
        - total_videos: Number of videos
        - videos: List with video_id and full description
    """
    if not plenary_videos or not plenary_videos.get('videos'):
        logging.warning("No plenary videos to process")
        return {'total_videos': 0, 'videos': []}

    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        error_msg = "YOUTUBE_API_KEY environment variable not set"
        logging.error(error_msg)
        raise ValueError(error_msg)

    try:
        # Build YouTube service with API key
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        video_descriptions = []
        for video in plenary_videos['videos']:
            video_id = video['video_id']

            # Get full video description
            video_response = youtube.videos().list(
                part='snippet',
                id=video_id
            ).execute()

            if not video_response.get('items'):
                logging.warning(f"Video not found: {video_id}")
                continue

            video_details = video_response['items'][0]
            full_description = video_details['snippet'].get('description', '')

            video_desc_data = {
                'video_id': video_id,
                'title': video['title'],
                'description': full_description,
                'description_length': len(full_description)
            }

            logging.info(f"Description fetched: {video['title']} ({len(full_description)} chars)")
            video_descriptions.append(video_desc_data)

        logging.info(f"Total descriptions fetched: {len(video_descriptions)}")
        return {
            'total_videos': len(video_descriptions),
            'videos': video_descriptions
        }

    except (ValueError, RuntimeError):
        # Re-raise known errors
        raise
    except Exception as e:
        error_msg = f"Error getting video descriptions: {e}"
        logging.error(error_msg)
        raise RuntimeError(error_msg) from e
