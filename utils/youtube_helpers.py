"""
Generic YouTube upload helper functions.

This module provides reusable functions for uploading videos to YouTube
using the YouTube Data API v3 with OAuth 2.0 authentication.

Contains both low-level YouTube API functions and DAG helper functions.
Can be used across multiple projects and YouTube channels by providing
different token files.
"""

import logging
import os
import pickle
from typing import Dict, List, Optional

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload


# =============================================================================
# LOW-LEVEL YOUTUBE API FUNCTIONS
# =============================================================================


def get_authenticated_youtube_service(token_file: str):
    """
    Get authenticated YouTube API service using saved token.

    Args:
        token_file: Path to the token pickle file

    Returns:
        Resource: Authenticated YouTube API service object

    Raises:
        FileNotFoundError: If token file doesn't exist
        Exception: If authentication fails
    """
    if not os.path.exists(token_file):
        raise FileNotFoundError(
            f"Token file not found: {token_file}. "
            "Please authenticate first using the test script."
        )

    logging.info(f"Loading YouTube credentials from {token_file}")

    # Load credentials from token file
    with open(token_file, 'rb') as token:
        credentials = pickle.load(token)

    # Refresh token if expired
    if credentials.expired and credentials.refresh_token:
        logging.info("Token expired. Refreshing...")
        credentials.refresh(Request())

        # Save refreshed token
        with open(token_file, 'wb') as token:
            pickle.dump(credentials, token)
        logging.info("Token refreshed and saved")

    # Build and return YouTube service
    youtube = build('youtube', 'v3', credentials=credentials)
    logging.info("YouTube service authenticated successfully")

    return youtube


def upload_video_to_youtube(
    youtube,
    video_file: str,
    title: str,
    description: str,
    category_id: str = '22',
    privacy_status: str = 'private',
    tags: Optional[List[str]] = None,
    made_for_kids: bool = False,
) -> Dict:
    """
    Upload a single video to YouTube.

    Args:
        youtube: Authenticated YouTube API service
        video_file: Path to the video file to upload
        title: Video title (max 100 characters)
        description: Video description (max 5000 characters)
        category_id: YouTube category ID (default: 22 = People & Blogs)
        privacy_status: Privacy status ('private', 'unlisted', 'public')
        tags: List of tags for the video
        made_for_kids: Whether video is made for kids (COPPA requirement)

    Returns:
        Dict with upload result:
        - success: Boolean indicating if upload succeeded
        - video_id: YouTube video ID (if successful)
        - video_url: YouTube video URL (if successful)
        - error: Error message (if failed)

    YouTube Category IDs:
        1 - Film & Animation
        2 - Autos & Vehicles
        10 - Music
        15 - Pets & Animals
        17 - Sports
        19 - Travel & Events
        20 - Gaming
        22 - People & Blogs
        23 - Comedy
        24 - Entertainment
        25 - News & Politics
        26 - Howto & Style
        27 - Education
        28 - Science & Technology
        29 - Nonprofits & Activism
    """
    if tags is None:
        tags = []

    # Validate video file exists
    if not os.path.exists(video_file):
        error_msg = f"Video file not found: {video_file}"
        logging.error(error_msg)
        return {
            "success": False,
            "video_id": None,
            "video_url": None,
            "error": error_msg,
        }

    # Get file size for logging
    file_size_mb = os.path.getsize(video_file) / (1024 * 1024)
    logging.info(f"Uploading video: {video_file} ({file_size_mb:.2f} MB)")
    logging.info(f"Title: {title}")
    logging.info(f"Privacy: {privacy_status}")

    try:
        # Prepare video metadata
        body = {
            'snippet': {
                'title': title[:100],  # Max 100 characters
                'description': description[:5000],  # Max 5000 characters
                'tags': tags,
                'categoryId': category_id,
            },
            'status': {
                'privacyStatus': privacy_status,
                'selfDeclaredMadeForKids': made_for_kids,
            },
        }

        # Create MediaFileUpload object
        media = MediaFileUpload(
            video_file,
            chunksize=-1,  # Upload in a single request
            resumable=True,
        )

        # Execute upload request
        request = youtube.videos().insert(
            part=','.join(body.keys()),
            body=body,
            media_body=media,
        )

        response = None
        while response is None:
            status, response = request.next_chunk()
            if status:
                progress = int(status.progress() * 100)
                logging.info(f"Upload progress: {progress}%")

        video_id = response['id']
        video_url = f"https://www.youtube.com/watch?v={video_id}"

        logging.info(f"Upload successful! Video ID: {video_id}")
        logging.info(f"Video URL: {video_url}")

        return {
            "success": True,
            "video_id": video_id,
            "video_url": video_url,
            "error": None,
        }

    except Exception as e:
        error_msg = f"Upload failed: {str(e)}"
        logging.error(error_msg)
        return {
            "success": False,
            "video_id": None,
            "video_url": None,
            "error": error_msg,
        }


def upload_multiple_videos(
    token_file: str,
    videos: List[Dict],
) -> Dict:
    """
    Upload multiple videos to YouTube.

    Args:
        token_file: Path to the YouTube token pickle file
        videos: List of video dicts, each containing:
            - video_file: Path to video file
            - title: Video title
            - description: Video description
            - category_id: (optional) Category ID (default: 22)
            - privacy_status: (optional) Privacy (default: 'private')
            - tags: (optional) List of tags
            - made_for_kids: (optional) COPPA setting (default: False)

    Returns:
        Dict with upload results:
        - total_videos: Total number of videos to upload
        - successful_uploads: Number of successful uploads
        - failed_uploads: Number of failed uploads
        - upload_details: List of detailed results per video
    """
    results = {
        "total_videos": len(videos),
        "successful_uploads": 0,
        "failed_uploads": 0,
        "upload_details": [],
    }

    if not videos:
        logging.warning("No videos provided for upload")
        return results

    try:
        # Authenticate once for all uploads
        youtube = get_authenticated_youtube_service(token_file)

        # Upload each video
        for video_info in videos:
            video_file = video_info.get('video_file')
            title = video_info.get('title')
            description = video_info.get('description', '')

            if not video_file or not title:
                logging.error("Missing required fields: video_file or title")
                results['failed_uploads'] += 1
                results['upload_details'].append({
                    "video_file": video_file,
                    "success": False,
                    "error": "Missing required fields",
                })
                continue

            # Upload video
            upload_result = upload_video_to_youtube(
                youtube=youtube,
                video_file=video_file,
                title=title,
                description=description,
                category_id=video_info.get('category_id', '22'),
                privacy_status=video_info.get('privacy_status', 'private'),
                tags=video_info.get('tags', []),
                made_for_kids=video_info.get('made_for_kids', False),
            )

            # Track results
            if upload_result['success']:
                results['successful_uploads'] += 1
            else:
                results['failed_uploads'] += 1

            results['upload_details'].append({
                "video_file": video_file,
                **upload_result,
            })

        logging.info(
            f"Batch upload complete: {results['successful_uploads']}/{results['total_videos']} successful"
        )

    except Exception as e:
        logging.error(f"Batch upload failed: {e}")
        results['failed_uploads'] = results['total_videos'] - results['successful_uploads']

    return results


# =============================================================================
# DAG HELPER FUNCTIONS
# =============================================================================


def validate_upload_config(conf):
    """
    Validate the configuration passed from a triggering DAG.

    Expected conf structure:
    {
        'token_file': str - Path to YouTube token pickle file,
        'videos': list - List of video dicts with upload parameters
    }

    Args:
        conf: Configuration dict from dag_run.conf

    Returns:
        Validated configuration dict

    Raises:
        ValueError: If configuration is invalid
    """
    # Validate required fields
    if not conf:
        raise ValueError("No configuration provided. dag_run.conf is empty.")

    if 'token_file' not in conf:
        raise ValueError("Missing required field: 'token_file' in dag_run.conf")

    if 'videos' not in conf or not isinstance(conf['videos'], list):
        raise ValueError("Missing or invalid field: 'videos' must be a list in dag_run.conf")

    if len(conf['videos']) == 0:
        raise ValueError("No videos provided for upload")

    # Validate each video configuration
    for idx, video in enumerate(conf['videos']):
        if 'video_file' not in video:
            raise ValueError(f"Video {idx}: missing required field 'video_file'")
        if 'title' not in video:
            raise ValueError(f"Video {idx}: missing required field 'title'")

    logging.info(f"Configuration validated successfully")
    logging.info(f"Token file: {conf['token_file']}")
    logging.info(f"Number of videos to upload: {len(conf['videos'])}")

    return conf


def upload_videos_from_config(conf):
    """
    Upload videos to YouTube using the provided configuration.

    This is a convenience wrapper around upload_multiple_videos() for use in DAGs.

    Args:
        conf: Validated configuration dict containing:
            - token_file: Path to YouTube token pickle file
            - videos: List of video dicts with upload parameters

    Returns:
        Dict with upload results:
        - total_videos: Total number of videos
        - successful_uploads: Number of successful uploads
        - failed_uploads: Number of failed uploads
        - upload_details: List of detailed results per video

    Raises:
        Exception: If any uploads fail
    """
    token_file = conf['token_file']
    videos = conf['videos']

    logging.info("=" * 70)
    logging.info("Starting YouTube Upload")
    logging.info("=" * 70)
    logging.info(f"Token: {token_file}")
    logging.info(f"Videos: {len(videos)}")

    # Upload videos using the batch upload function
    results = upload_multiple_videos(
        token_file=token_file,
        videos=videos,
    )

    logging.info("=" * 70)
    logging.info("Upload Complete")
    logging.info("=" * 70)
    logging.info(f"Total: {results['total_videos']}")
    logging.info(f"Successful: {results['successful_uploads']}")
    logging.info(f"Failed: {results['failed_uploads']}")

    # Raise error if any uploads failed
    if results['failed_uploads'] > 0:
        error_details = [
            detail for detail in results['upload_details']
            if not detail.get('success', False)
        ]
        logging.error(f"Failed uploads: {error_details}")
        raise Exception(f"{results['failed_uploads']} video(s) failed to upload")

    return results
