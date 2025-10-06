"""
YouTube video upload operations for Congressional videos.

This module handles preparing upload configuration for the generic
YouTube uploader DAG.
"""

import logging
import os


def prepare_youtube_upload_config(download_results, youtube_metadata_results, is_testing=False):
    """
    Prepare configuration for the generic YouTube uploader DAG.

    Converts download results and metadata into the format expected by
    the generic_youtube_uploader DAG.

    Args:
        download_results: Results from download_videos_for_upload
        youtube_metadata_results: Results from generate_youtube_metadata_for_selected_videos
        is_testing: If True, uploads as private; if False, uploads as public

    Returns:
        Dict configuration for generic_youtube_uploader DAG:
        - token_file: Path to YouTube token pickle file
        - videos: List of video objects with upload parameters
        Returns None if no videos to upload
    """
    if not download_results or not download_results.get('download_details'):
        logging.warning("No download results to upload")
        return None

    # Create metadata lookup by entry_id
    metadata_lookup = {}
    if youtube_metadata_results and youtube_metadata_results.get('topic_metadata'):
        for metadata in youtube_metadata_results['topic_metadata']:
            entry_id = metadata.get('topic_entry_id')
            if entry_id:
                metadata_lookup[entry_id] = metadata

    # Build videos list for generic uploader
    videos = []
    for download_detail in download_results['download_details']:
        if not download_detail.get('success') or not download_detail.get('file_path'):
            continue

        entry_id = download_detail.get('entry_id')
        metadata = metadata_lookup.get(entry_id, {})

        # Extract title and description from nested dicts
        title_data = metadata.get('title', {})
        description_data = metadata.get('description', {})

        title = title_data.get('title', f'Congressional Video {entry_id}') if isinstance(title_data, dict) else str(title_data)
        description = description_data.get('description', '') if isinstance(description_data, dict) else str(description_data)

        logging.info(f"Video {entry_id}: Using title='{title[:50]}...' (metadata found: {bool(metadata)})")

        videos.append({
            'video_file': download_detail['file_path'],
            'title': title,
            'description': description,
            'category_id': '25',  # News & Politics
            'privacy_status': 'private' if is_testing else 'public',
            'tags': ['congress', 'politics', 'españa', 'congreso'],
            'made_for_kids': False,
        })

    if not videos:
        logging.warning("No valid videos to upload")
        return None

    # Configuration for generic uploader
    # Token is mounted from NAS to /opt/airflow/data (non-symlinked path)
    token_file = '/opt/airflow/data/congress_youtube_token.pickle'

    config = {
        'token_file': token_file,
        'videos': videos
    }

    logging.info(f"Prepared upload config for {len(videos)} videos")
    logging.info(f"Privacy status: {'private' if is_testing else 'public'} (is_testing={is_testing})")

    return config
