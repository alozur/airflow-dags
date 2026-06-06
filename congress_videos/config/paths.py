"""
Path configuration for Congress videos project.

This module centralizes all filesystem paths used in the project.
Modify paths here to update them across the entire codebase.

NOTE: All paths use forward slashes (/) for Docker/Linux compatibility.
Even when developing on Windows, these paths target the Docker container environment.
"""

import os
from pathlib import Path

# -------------------------
# Base Directories
# -------------------------
# Base Airflow data directory (mounted from NAS in Docker environment)
BASE_DATA_DIR = "/opt/airflow/data"

# Project-specific data directory
# Using forward slashes for Docker/Linux compatibility
PROJECT_DATA_DIR = f"{BASE_DATA_DIR}/congress_videos"

# -------------------------
# Video Storage
# -------------------------
# Videos are stored in a dedicated 'videos' subdirectory
# Structure: /opt/airflow/data/congress_videos/videos/{session_number}/{entry_id}/video.mp4
VIDEOS_DIR = f"{PROJECT_DATA_DIR}/videos"

# -------------------------
# Downloads Directory (YouTube Monitor DAG)
# -------------------------
# All downloads from YouTube channel monitoring are stored here
# Structure: /opt/airflow/data/congress_videos/downloads/{date}/{video_id}/
#   - audio_chunks/  (audio files)
#   - srt_files/     (subtitle files)
DOWNLOADS_DIR = f"{PROJECT_DATA_DIR}/downloads"

# -------------------------
# Assets Directories
# -------------------------
# Assets directory for thumbnail generation (images, logos, etc.)
ASSETS_DIR = f"{PROJECT_DATA_DIR}/assets"

# Fonts directory for thumbnail generation
FONTS_DIR = f"{ASSETS_DIR}/fonts"

# -------------------------
# Authentication & Tokens
# -------------------------
# YouTube API authentication token
YOUTUBE_TOKEN_FILE = f"{PROJECT_DATA_DIR}/congress_youtube_token.pickle"

# -------------------------
# Asset Files
# -------------------------
# Font files for thumbnail generation
FONT_BOLD = f"{FONTS_DIR}/LiberationSans-Bold.ttf"
FONT_REGULAR = f"{FONTS_DIR}/LiberationSans-Regular.ttf"

# Image assets for thumbnail generation
BACKGROUND_IMAGE = f"{ASSETS_DIR}/congress_chamber_background.png"
CHANNEL_LOGO = f"{ASSETS_DIR}/congress_channel_logo.png"


def get_session_path(session_number: str) -> str:
    """
    Get the full path for a specific session's videos directory.

    Args:
        session_number: Session number (e.g., "PL_115_001")

    Returns:
        Full path to session directory (e.g., /opt/airflow/data/congress_videos/videos/PL_115_001)
    """
    return f"{VIDEOS_DIR}/{session_number}"


def get_topic_path(session_number: str, topic_entry_id: str) -> str:
    """
    Get the full path for a specific topic within a session.

    Args:
        session_number: Session number (e.g., "PL_115_001")
        topic_entry_id: Topic entry ID (e.g., "20250101_01")

    Returns:
        Full path to topic directory
    """
    return f"{get_session_path(session_number)}/{topic_entry_id}"


def get_video_path(session_number: str, topic_entry_id: str, filename: str) -> str:
    """
    Get the full path for a specific video file.

    Args:
        session_number: Session number
        topic_entry_id: Topic entry ID
        filename: Video filename (e.g., "video.mp4")

    Returns:
        Full path to video file
    """
    return f"{get_topic_path(session_number, topic_entry_id)}/{filename}"


def get_download_date_path(date: str) -> str:
    """
    Get the full path for a specific date's downloads directory.

    Args:
        date: Date string in YYYY-MM-DD format (e.g., "2025-10-08")

    Returns:
        Full path to date directory (e.g., /opt/airflow/data/congress_videos/downloads/2025-10-08)
    """
    return f"{DOWNLOADS_DIR}/{date}"


def get_download_video_path(date: str, video_id: str) -> str:
    """
    Get the full path for a specific video's downloads directory.

    Args:
        date: Date string in YYYY-MM-DD format (e.g., "2025-10-08")
        video_id: YouTube video ID (e.g., "hy1cnx-0Oww")

    Returns:
        Full path to video download directory
    """
    return f"{get_download_date_path(date)}/{video_id}"


def get_download_file_path(date: str, video_id: str, filename: str) -> str:
    """
    Get the full path for a specific downloaded file.

    Args:
        date: Date string in YYYY-MM-DD format
        video_id: YouTube video ID
        filename: File name (e.g., "agenda.pdf", "video.mp4")

    Returns:
        Full path to the file
    """
    return f"{get_download_video_path(date, video_id)}/{filename}"


# -------------------------
# Shorts Storage (Reap Video Shorts pipeline)
# -------------------------
# Short clips downloaded from Reap are stored per chapter
# Structure: /opt/airflow/data/congress_videos/{chapter_id}/shorts/{clip_id}.mp4


def get_shorts_dir(chapter_id: int) -> str:
    """
    Get the directory where Reap-generated short clips are stored for a chapter.

    Args:
        chapter_id: DB chapter_id (e.g., 42)

    Returns:
        Full path to shorts directory (e.g., /opt/airflow/data/congress_videos/42/shorts)
    """
    return f"{PROJECT_DATA_DIR}/{chapter_id}/shorts"


def get_short_file_path(chapter_id: int, clip_id: str) -> str:
    """
    Get the full path for a downloaded Reap short clip file.

    Args:
        chapter_id: DB chapter_id (e.g., 42)
        clip_id: Reap clip ID (e.g., "abc123")

    Returns:
        Full path to clip file (e.g., /opt/airflow/data/congress_videos/42/shorts/abc123.mp4)
    """
    return f"{get_shorts_dir(chapter_id)}/{clip_id}.mp4"


# -------------------------
# Path Validation
# -------------------------
def ensure_directory_exists(path: str) -> str:
    """
    Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path to create

    Returns:
        The path that was created/validated
    """
    os.makedirs(path, exist_ok=True)
    return path
