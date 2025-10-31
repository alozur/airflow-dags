"""
File and video download operations.

This module handles file system operations including folder creation,
video downloading, and info file generation.
"""

import logging
import os
from urllib.parse import urlparse

import requests

from congress_videos.config.paths import get_topic_path, get_video_path, ensure_directory_exists


def download_video_file(video_url, output_path):
    """
    Downloads a video file from URL to the specified path.

    Args:
        video_url: URL of the video to download
        output_path: Full file path where to save the video

    Returns:
        Dict with success status and details containing:
        - success: Boolean indicating if download succeeded
        - file_path: Path where video was saved
        - file_size_bytes: Size of downloaded file
        - error: Error message if download failed
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "video/mp4,video/*;q=0.9,*/*;q=0.8",
            "Accept-Language": "es,en;q=0.9",
        }

        logging.info(f"Starting download: {video_url}")
        response = requests.get(
            video_url, headers=headers, stream=True, verify=False, timeout=300
        )

        if response.status_code == 200:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

            with open(output_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = os.path.getsize(output_path)
            logging.info(f"Successfully downloaded {file_size} bytes to: {output_path}")

            return {
                "success": True,
                "file_path": output_path,
                "file_size_bytes": file_size,
                "error": None,
            }
        else:
            error_msg = f"HTTP {response.status_code}: {response.reason}"
            logging.error(f"Failed to download video: {error_msg}")
            return {
                "success": False,
                "file_path": None,
                "file_size_bytes": 0,
                "error": error_msg,
            }

    except Exception as e:
        error_msg = f"Download error: {str(e)}"
        logging.error(f"Exception during video download: {error_msg}")
        return {
            "success": False,
            "file_path": None,
            "file_size_bytes": 0,
            "error": error_msg,
        }


def download_videos_for_upload(top_videos, data_directory_path):
    """
    Downloads videos selected for YouTube upload.

    Args:
        top_videos: List of video records from database
        data_directory_path: Base directory for downloads

    Returns:
        Dict with download results containing:
        - total_videos: Total number of videos to download
        - successful_downloads: Number of successful downloads
        - failed_downloads: Number of failed downloads
        - download_details: List of detailed results per video
    """
    download_results = {
        "total_videos": len(top_videos) if top_videos else 0,
        "successful_downloads": 0,
        "failed_downloads": 0,
        "download_details": [],
    }

    if not top_videos:
        logging.warning("No videos provided for download")
        return download_results

    for video in top_videos:
        entry_id = video.get("entry_id")
        video_url = video.get("video_url")
        session_number = video.get("session_number")

        # Create topic folder using centralized path configuration
        # Structure: {VIDEOS_DIR}/{session_number}/{entry_id}/video.mp4
        topic_folder = get_topic_path(str(session_number), entry_id)
        ensure_directory_exists(topic_folder)

        # Download video to correct location
        logging.info(f"Downloading video {entry_id} from {video_url}")
        output_path = get_video_path(str(session_number), entry_id, "video.mp4")
        download_result = download_video_file(video_url, output_path)

        download_detail = {
            "entry_id": entry_id,
            "success": download_result.get("success", False),
            "file_path": download_result.get("file_path"),
            "file_size": download_result.get("file_size_bytes"),  # Match key from download_video_file
            "duration": download_result.get("duration"),
            "error": download_result.get("error"),
        }

        download_results["download_details"].append(download_detail)

        if download_detail["success"]:
            download_results["successful_downloads"] += 1
        else:
            download_results["failed_downloads"] += 1

    logging.info(
        f"Download complete: {download_results['successful_downloads']}/{download_results['total_videos']} videos downloaded successfully"
    )
    return download_results
