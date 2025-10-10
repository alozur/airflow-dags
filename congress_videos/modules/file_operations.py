"""
File and video download operations.

This module handles file system operations including folder creation,
video downloading, and info file generation.
"""

import logging
import os
from urllib.parse import urlparse

import requests

from congress_videos.config.paths import get_session_path, get_topic_path, get_video_path, ensure_directory_exists


def create_session_folder(session_number, base_data_path=None):
    """
    Creates a session folder inside the congreso_youtube/videos data directory.

    Args:
        session_number: The session number from get_session_number task
        base_data_path: Deprecated - kept for compatibility, but not used

    Returns:
        Full path to the created session directory
    """
    # Use centralized path configuration
    session_folder_path = get_session_path(session_number)

    if not os.path.exists(session_folder_path):
        os.makedirs(session_folder_path, exist_ok=True)
        logging.info(f"Created session folder: {session_folder_path}")
    else:
        logging.info(f"Session folder already exists: {session_folder_path}")

    return session_folder_path


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


def create_topic_info_file(topic_entry_id, main_topic, interventions, topic_folder_path):
    """
    Creates an informational file for a topic with entry details and speaker information.

    Args:
        topic_entry_id: Entry ID for the main topic
        main_topic: Main topic data from enriched video groups
        interventions: List of interventions for this topic
        topic_folder_path: Path to the topic folder

    Returns:
        Path to the created info file, or None if creation failed
    """
    info_file_path = os.path.join(topic_folder_path, f"{topic_entry_id}_info.txt")

    try:
        with open(info_file_path, "w", encoding="utf-8") as f:
            f.write("TOPIC INFORMATION\n")
            f.write("=================\n\n")
            f.write(f"Entry ID: {topic_entry_id}\n")
            f.write(f"Profile Link: {main_topic.get('profile_link', 'N/A')}\n\n")

            f.write("MAIN TOPIC:\n")
            f.write("-----------\n")
            f.write(f"{main_topic.get('content', 'N/A')}\n\n")

            f.write("SPEAKERS AND INTERVENTIONS:\n")
            f.write("---------------------------\n")

            for i, intervention in enumerate(interventions, 1):
                speaker_name = intervention.get("speaker_name", "Unknown")
                role = intervention.get("role")

                f.write(f"{i}. {speaker_name}")
                if role:
                    f.write(f" - {role}")
                f.write("\n")

            if not interventions:
                f.write("No interventions recorded for this topic.\n")

        logging.info(f"Created topic info file: {info_file_path}")
        return info_file_path

    except Exception as e:
        logging.error(f"Failed to create info file {info_file_path}: {str(e)}")
        return None


def download_main_topic_videos(enriched_video_groups, session_folder_path):
    """
    Downloads ONLY main topic videos and creates folder structure for each topic.

    IMPORTANT: This function downloads only the main topic videos (tema principal)
    from each topic group. Individual intervention videos (intervenciones de diputados)
    are NOT downloaded - they are processed for metadata only.

    For each topic group:
    - Downloads: 1 main topic video per group
    - Skips: All intervention videos within that group
    - Creates: Topic folder structure and info files

    Args:
        enriched_video_groups: Enriched video groups from previous task
        session_folder_path: Path to the session folder

    Returns:
        Dict with download results and summary containing:
        - successful_downloads: Number of main topic videos downloaded successfully
        - failed_downloads: Number of main topic videos that failed to download
        - total_topics: Total number of topic groups processed
        - download_details: List of detailed results per topic
    """
    download_results = {
        "successful_downloads": 0,
        "failed_downloads": 0,
        "total_topics": 0,
        "download_details": [],
    }

    for group in enriched_video_groups:
        if group.get("type") == "topic_group":
            download_results["total_topics"] += 1

            main_topic = group.get("main_topic", {})
            topic_entry_id = main_topic.get("entry_id")

            if not topic_entry_id:
                logging.warning("Skipping topic group without entry_id")
                continue

            # Create folder for this topic
            topic_folder_path = os.path.join(session_folder_path, topic_entry_id)
            os.makedirs(topic_folder_path, exist_ok=True)

            # Create info file
            interventions = group.get("interventions", [])
            info_file_path = create_topic_info_file(
                topic_entry_id, main_topic, interventions, topic_folder_path
            )

            # Download main topic video
            video_url = main_topic.get("video_url")
            if video_url:
                # Extract filename from metadata or URL
                metadata = main_topic.get("metadata_url", {})
                filename = metadata.get("filename")
                if not filename:
                    # Fallback to extracting from URL
                    parsed_url = urlparse(video_url)
                    filename = (
                        parsed_url.path.split("/")[-1]
                        if parsed_url.path
                        else f"{topic_entry_id}.mp4"
                    )

                video_output_path = os.path.join(topic_folder_path, filename)

                download_result = download_video_file(video_url, video_output_path)

                result_detail = {
                    "topic_entry_id": topic_entry_id,
                    "video_url": video_url,
                    "output_path": video_output_path
                    if download_result["success"]
                    else None,
                    "info_file_path": info_file_path,
                    "success": download_result["success"],
                    "error": download_result["error"],
                    "file_size_bytes": download_result["file_size_bytes"],
                }

                download_results["download_details"].append(result_detail)

                if download_result["success"]:
                    download_results["successful_downloads"] += 1
                    logging.info(f"Successfully processed topic {topic_entry_id}")
                else:
                    download_results["failed_downloads"] += 1
                    logging.error(
                        f"Failed to download video for topic {topic_entry_id}: {download_result['error']}"
                    )
            else:
                logging.warning(f"No video URL found for topic {topic_entry_id}")
                download_results["failed_downloads"] += 1

    logging.info(
        f"Download summary: {download_results['successful_downloads']}/{download_results['total_topics']} topics processed successfully"
    )
    return download_results


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
