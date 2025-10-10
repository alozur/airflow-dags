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
    """
    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        logging.error("YOUTUBE_API_KEY environment variable not set")
        return {'total_videos': 0, 'videos': [], 'error': 'Missing API key'}

    logging.info(f"Fetching videos from YouTube channel: {channel_id}")

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        # Get channel's uploads playlist ID
        channel_response = youtube.channels().list(
            part='contentDetails',
            id=channel_id
        ).execute()

        if not channel_response.get('items'):
            logging.error(f"Channel not found: {channel_id}")
            return {'total_videos': 0, 'videos': []}

        # Get recent videos from the channel
        # Note: This gets all uploads, not just live streams
        # We'll filter for live streams in the next step
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

    except Exception as e:
        logging.error(f"Error fetching YouTube videos: {e}")
        return {'total_videos': 0, 'videos': [], 'error': str(e)}


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
    """
    if not channel_videos or not channel_videos.get('videos'):
        logging.warning("No channel videos to filter")
        return {'total_matches': 0, 'videos': []}

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


def check_stream_status(plenary_videos):
    """
    Check if videos are finished streams (not live) and get additional details.

    Args:
        plenary_videos: Results from filter_plenary_session_videos

    Returns:
        Dict with finished stream information:
        - total_finished: Number of finished streams
        - videos: List of finished stream details with duration, etc.
    """
    if not plenary_videos or not plenary_videos.get('videos'):
        logging.warning("No plenary videos to check")
        return {'total_finished': 0, 'videos': []}

    # Get YouTube API key from environment
    youtube_api_key = os.getenv('YOUTUBE_API_KEY')

    if not youtube_api_key:
        logging.error("YOUTUBE_API_KEY environment variable not set")
        return {'total_finished': 0, 'videos': [], 'error': 'Missing API key'}

    try:
        # Build YouTube service with API key (no OAuth needed for public data)
        youtube = build('youtube', 'v3', developerKey=youtube_api_key)

        finished_streams = []
        for video in plenary_videos['videos']:
            video_id = video['video_id']

            # Get detailed video information
            video_response = youtube.videos().list(
                part='snippet,contentDetails,liveStreamingDetails,status',
                id=video_id
            ).execute()

            if not video_response.get('items'):
                logging.warning(f"Video not found: {video_id}")
                continue

            video_details = video_response['items'][0]

            # Check if it's a finished live stream
            live_details = video_details.get('liveStreamingDetails', {})
            is_live = video_details['snippet'].get('liveBroadcastContent') == 'live'
            is_upcoming = video_details['snippet'].get('liveBroadcastContent') == 'upcoming'

            if is_live or is_upcoming:
                logging.info(f"Skipping live/upcoming stream: {video['title']}")
                continue

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

            finished_stream = {
                **video,
                'duration_seconds': duration_seconds,
                'duration_formatted': f"{hours}:{minutes:02d}:{seconds:02d}",
                'actual_start_time': live_details.get('actualStartTime'),
                'actual_end_time': live_details.get('actualEndTime'),
                'is_finished': True,
                'youtube_url': f"https://www.youtube.com/watch?v={video_id}",
            }

            logging.info(f"Finished stream found: {video['title']} - Duration: {finished_stream['duration_formatted']}")
            finished_streams.append(finished_stream)

        logging.info(f"Total finished streams: {len(finished_streams)}")
        return {
            'total_finished': len(finished_streams),
            'videos': finished_streams
        }

    except Exception as e:
        logging.error(f"Error checking stream status: {e}")
        return {'total_finished': 0, 'videos': [], 'error': str(e)}
