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
    if youtube_metadata_results and youtube_metadata_results.get('metadata_results'):
        for metadata in youtube_metadata_results['metadata_results']:
            entry_id = metadata.get('entry_id')
            if entry_id:
                metadata_lookup[entry_id] = metadata

    # Build videos list for generic uploader
    videos = []
    for download_detail in download_results['download_details']:
        if not download_detail.get('success') or not download_detail.get('file_path'):
            continue

        entry_id = download_detail.get('entry_id')
        metadata = metadata_lookup.get(entry_id, {})

        videos.append({
            'video_file': download_detail['file_path'],
            'title': metadata.get('youtube_title', f'Congressional Video {entry_id}'),
            'description': metadata.get('youtube_description', ''),
            'category_id': '25',  # News & Politics
            'privacy_status': 'private' if is_testing else 'public',
            'tags': metadata.get('youtube_tags', ['congress', 'politics']),
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
