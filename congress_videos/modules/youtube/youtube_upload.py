"""
YouTube video upload operations for Congressional videos.

This module handles preparing upload configuration for the generic
YouTube uploader DAG.
"""

import logging

from congress_videos.config.paths import YOUTUBE_TOKEN_FILE


def prepare_chapter_upload_config(
    chapter_extraction_results,
    youtube_metadata_results,
    thumbnail_results=None,
    is_testing=False
):
    """
    Prepare configuration for the generic YouTube uploader DAG (chapter videos).

    Converts chapter extraction results, metadata, and thumbnails into the format
    expected by the generic_youtube_uploader DAG.

    Args:
        chapter_extraction_results: Results from video_splitter.extract_chapters_from_video
            Expected structure:
            {
                'total_chapters': int,
                'successful_extractions': int,
                'results': [
                    {
                        'chapter_id': int,
                        'video_id': str,
                        'success': bool,
                        'output_path': str,
                        'file_size_mb': float,
                        'duration_seconds': float,
                        'error': str or None
                    }
                ]
            }
        youtube_metadata_results: Results from youtube_ai.generate_youtube_metadata_for_selected_videos
            Expected structure:
            {
                'topic_metadata': [
                    {
                        'chapter_id': int,
                        'video_id': str,
                        'title': {'title': str, ...},
                        'description': {'description': str, ...},
                        ...
                    }
                ]
            }
        thumbnail_results: (optional) Results from thumbnail_generator.generate_video_thumbnails
            Expected structure:
            {
                'results': [
                    {
                        'chapter_id': int,
                        'success': bool,
                        'output_path': str
                    }
                ]
            }
        is_testing: If True, uploads as private; if False, uploads as public

    Returns:
        Dict configuration for generic_youtube_uploader DAG:
        - token_file: Path to YouTube token pickle file
        - videos: List of video objects with upload parameters
        Returns None if no videos to upload
    """
    if not chapter_extraction_results or not chapter_extraction_results.get('results'):
        logging.warning("No chapter extraction results to upload")
        return None

    # Create metadata lookup by chapter_id
    metadata_lookup = {}
    if youtube_metadata_results and youtube_metadata_results.get('topic_metadata'):
        for metadata in youtube_metadata_results['topic_metadata']:
            chapter_id = metadata.get('chapter_id')
            if chapter_id:
                metadata_lookup[chapter_id] = metadata

    # Create thumbnail lookup by chapter_id
    thumbnail_lookup = {}
    if thumbnail_results and thumbnail_results.get('results'):
        for thumbnail in thumbnail_results['results']:
            chapter_id = thumbnail.get('chapter_id')
            if chapter_id and thumbnail.get('success'):
                thumbnail_lookup[chapter_id] = thumbnail.get('output_path')

    # Build videos list for generic uploader
    videos = []
    for extraction_result in chapter_extraction_results['results']:
        if not extraction_result.get('success') or not extraction_result.get('output_path'):
            logging.warning(
                f"Skipping chapter {extraction_result.get('chapter_id')}: "
                f"extraction failed or no output path"
            )
            continue

        chapter_id = extraction_result.get('chapter_id')
        video_id = extraction_result.get('video_id')
        metadata = metadata_lookup.get(chapter_id, {})
        thumbnail_file = thumbnail_lookup.get(chapter_id)

        # Extract title and description from nested dicts
        title_data = metadata.get('title', {})
        description_data = metadata.get('description', {})

        title = (
            title_data.get('title', f'Congreso - Capítulo {chapter_id}')
            if isinstance(title_data, dict)
            else str(title_data)
        )
        description = (
            description_data.get('description', '')
            if isinstance(description_data, dict)
            else str(description_data)
        )

        logging.info(
            f"Chapter {chapter_id} (video {video_id}): "
            f"title='{title[:50]}...' (metadata: {bool(metadata)}, thumbnail: {bool(thumbnail_file)})"
        )

        video_config = {
            'chapter_id': chapter_id,  # Include chapter_id for tracking in upload results
            'video_id': video_id,  # Include source video_id for reference
            'video_file': extraction_result['output_path'],
            'title': title,
            'description': description,
            'category_id': '25',  # News & Politics
            'privacy_status': 'private' if is_testing else 'public',
            'tags': ['congress', 'politics', 'españa', 'congreso', 'debate', 'parlamento'],
            'made_for_kids': False,
        }

        # Add thumbnail if available
        if thumbnail_file:
            video_config['thumbnail_file'] = thumbnail_file

        videos.append(video_config)

    if not videos:
        logging.warning("No valid chapter videos to upload")
        return None

    # Configuration for generic uploader
    config = {
        'token_file': YOUTUBE_TOKEN_FILE,
        'videos': videos
    }

    logging.info(f"Prepared chapter upload config for {len(videos)} videos")
    logging.info(f"Privacy status: {'private' if is_testing else 'public'} (is_testing={is_testing})")

    return config
