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
# Legacy single YouTube token (pre per-channel/per-purpose split).
# Kept for backward compatibility; see congress_videos/config/youtube_channels.py
# and resolve_token_path() for the current per-channel layout.
YOUTUBE_TOKEN_FILE = f"{PROJECT_DATA_DIR}/congress_youtube_token.pickle"

# Root directory for per-channel, per-purpose OAuth token pickles.
# Layout: {YOUTUBE_TOKENS_DIR}/{channel}/{purpose}.pickle
YOUTUBE_TOKENS_DIR = f"{PROJECT_DATA_DIR}/youtube_tokens"

# -------------------------
# Asset Files
# -------------------------
# Font files for thumbnail generation
FONT_BOLD = f"{FONTS_DIR}/LiberationSans-Bold.ttf"
FONT_REGULAR = f"{FONTS_DIR}/LiberationSans-Regular.ttf"

# Image assets for thumbnail generation
BACKGROUND_IMAGE = f"{ASSETS_DIR}/congress_chamber_background.png"
CHANNEL_LOGO = f"{ASSETS_DIR}/congress_channel_logo.png"


def get_thumbnail_dir(youtube_video_id: str) -> Path:
    """Return the local directory where thumbnail PNGs for a video are stored.

    The directory is not created by this function; callers are responsible
    for creating it (e.g. via ``Path.mkdir(parents=True, exist_ok=True)``).

    Args:
        youtube_video_id: YouTube video identifier (e.g. ``"dQw4w9WgXcQ"``).

    Returns:
        ``Path`` to ``/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/``.
    """
    return Path(f"{PROJECT_DATA_DIR}/thumbnails/{youtube_video_id}")


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


def get_turn_video_path(date: str, video_id: str, turn_id: int) -> str:
    """Return the canonical output path for a materialized speaker-turn video.

    The path follows the convention::

        {DOWNLOADS_DIR}/{date}/{video_id}/turns/{turn_id}/turn_video.mp4

    The directory is not created by this function; callers are responsible
    for creating it (e.g. via ``Path.mkdir(parents=True, exist_ok=True)``).

    Args:
        date:     Recording date in YYYY-MM-DD format (e.g. ``"2025-10-08"``).
        video_id: YouTube video identifier (e.g. ``"hy1cnx-0Oww"``).
        turn_id:  Database ``turn_id`` from ``speaker_turns``.

    Returns:
        Absolute path string to the turn video MP4 file.
    """
    return f"{get_download_video_path(date, video_id)}/turns/{turn_id}/turn_video.mp4"


# ---------------------------------------------------------------------------
# Canonical per-channel speaker-turn artifact layout
# {PROJECT_DATA_DIR}/{channel_slug}/{source_video_id}/video_chapters/{chapter_id}/oradores/{output_turn_id}/{artifact}
# See epic #133 for storage layout specification.
#
# NOTE: DEFAULT_CHANNEL is resolved via a function-local import of
# congress_videos.config.youtube_channels.DEFAULT_CHANNEL at call time.
# A top-level import would create a circular dependency because
# youtube_channels.py imports YOUTUBE_TOKEN_FILE and YOUTUBE_TOKENS_DIR
# from this module (paths.py).
# ---------------------------------------------------------------------------


def get_orador_video_dir(
    source_video_id: str,
    chapter_id: int,
    output_turn_id: int,
    channel_slug: str | None = None,
) -> Path:
    """Return the oradores turn directory path (no side effects, no mkdir).

    The directory is not created by this function; callers are responsible
    for creating it (e.g. via ``Path.mkdir(parents=True, exist_ok=True)``).

    Args:
        source_video_id: YouTube video identifier (e.g. ``"abc123"``).
        chapter_id:      Database chapter ID (e.g. ``7``).
        output_turn_id:  Database turn ID for the output video (e.g. ``42``).
        channel_slug:    Channel slug (defaults to ``DEFAULT_CHANNEL``).

    Returns:
        ``Path`` to ``{PROJECT_DATA_DIR}/{channel_slug}/{source_video_id}/
        video_chapters/{chapter_id}/oradores/{output_turn_id}/``.
    """
    if channel_slug is None:
        # Function-local import avoids paths <-> youtube_channels circular dependency.
        from congress_videos.config.youtube_channels import DEFAULT_CHANNEL  # noqa: PLC0415
        channel_slug = DEFAULT_CHANNEL
    return Path(
        f"{PROJECT_DATA_DIR}/{channel_slug}/{source_video_id}"
        f"/video_chapters/{chapter_id}/oradores/{output_turn_id}"
    )


def get_orador_artifact_path(
    source_video_id: str,
    chapter_id: int,
    output_turn_id: int,
    filename: str,
    channel_slug: str | None = None,
) -> Path:
    """Return the path to a named artifact inside the orador turn directory.

    The directory is not created by this function; callers are responsible
    for creating it (e.g. via ``Path.mkdir(parents=True, exist_ok=True)``).

    Args:
        source_video_id: YouTube video identifier (e.g. ``"abc123"``).
        chapter_id:      Database chapter ID (e.g. ``7``).
        output_turn_id:  Database turn ID for the output video (e.g. ``42``).
        filename:        Artifact filename (e.g. ``"video.mp4"``).
        channel_slug:    Channel slug (defaults to ``DEFAULT_CHANNEL``).

    Returns:
        ``Path`` to the artifact file inside the orador turn directory.
    """
    return get_orador_video_dir(
        source_video_id, chapter_id, output_turn_id, channel_slug
    ) / filename


# --- Chapter-level short clip paths ---

def get_video_chapter_dir(
    source_video_id: str,
    chapter_id: int,
    channel_slug: str | None = None,
) -> Path:
    """Return the chapter directory path (no side effects, no mkdir).

    The directory is not created by this function; callers are responsible
    for creating it (e.g. via ``Path.mkdir(parents=True, exist_ok=True)``).

    Args:
        source_video_id: YouTube video identifier (e.g. ``"abc123"``).
        chapter_id:      Database chapter ID (e.g. ``7``).
        channel_slug:    Channel slug (defaults to ``DEFAULT_CHANNEL``).

    Returns:
        ``Path`` to ``{PROJECT_DATA_DIR}/{channel_slug}/{source_video_id}/
        video_chapters/{chapter_id}/``.
    """
    if channel_slug is None:
        # Function-local import avoids paths <-> youtube_channels circular dependency.
        from congress_videos.config.youtube_channels import DEFAULT_CHANNEL  # noqa: PLC0415
        channel_slug = DEFAULT_CHANNEL
    return Path(
        f"{PROJECT_DATA_DIR}/{channel_slug}/{source_video_id}"
        f"/video_chapters/{chapter_id}"
    )


def get_chapter_shorts_dir(
    source_video_id: str,
    chapter_id: int,
    channel_slug: str | None = None,
) -> Path:
    """Return the shorts sub-directory for a chapter (no side effects, no mkdir).

    Args:
        source_video_id: YouTube video identifier.
        chapter_id:      Database chapter ID.
        channel_slug:    Channel slug (defaults to ``DEFAULT_CHANNEL``).

    Returns:
        ``Path`` to the ``shorts/`` directory inside the chapter directory.
    """
    return get_video_chapter_dir(source_video_id, chapter_id, channel_slug) / "shorts"


def get_chapter_short_file_path(
    source_video_id: str,
    chapter_id: int,
    clip_id: str,
    channel_slug: str | None = None,
) -> Path:
    """Return the canonical path for a Reap short clip (no side effects, no mkdir).

    Args:
        source_video_id: YouTube source video identifier.
        chapter_id:      Database chapter ID.
        clip_id:         Reap clip identifier (used as filename stem).
        channel_slug:    Channel slug (defaults to ``DEFAULT_CHANNEL``).

    Returns:
        ``Path`` to ``{shorts_dir}/{clip_id}.mp4``.
    """
    return get_chapter_shorts_dir(source_video_id, chapter_id, channel_slug) / f"{clip_id}.mp4"


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
