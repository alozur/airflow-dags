"""
YouTube video upload operations for Congressional videos.

This module handles preparing upload configuration for the generic
YouTube uploader DAG.
"""

import logging
import os

from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path


_REQUIRED_SIDECARS = ("title.txt", "description.txt", "thumbnail.png", "subtitles.srt")


def _write_orador_sidecars(video_file: str, title: str, description: str) -> None:
    """Best-effort: co-locate title.txt + description.txt with the turn video.
    Errors are logged and swallowed; never raises.
    """
    try:
        d = os.path.dirname(video_file)
        with open(os.path.join(d, "title.txt"), "w", encoding="utf-8") as f:
            f.write(title or "")
        with open(os.path.join(d, "description.txt"), "w", encoding="utf-8") as f:
            f.write(description or "")
    except Exception as exc:
        logging.warning("Failed to write orador sidecars for %s: %s", video_file, exc)


def prepare_orador_upload_config(
    output_path: str,
    is_testing: bool = False,
) -> dict:
    """Build upload config for a TURN item by reading pre-prepared sidecars.

    The PREPARE DAG (speaker_turn_prepare) must have already written all four
    sidecars to the directory containing ``output_path``:
    - ``title.txt``
    - ``description.txt``
    - ``thumbnail.png``
    - ``subtitles.srt``

    This function performs zero AI calls, zero ffmpeg calls, and zero
    generic_thumbnail_generator triggers. It only reads files from disk.

    Args:
        output_path: Absolute path to the pre-materialized turn ``video.mp4``.
        is_testing: When True, sets privacy_status='private'.

    Returns:
        Dict with all fields required by the generic_youtube_uploader DAG.

    Raises:
        FileNotFoundError: When any required sidecar is missing from disk.
    """
    video_dir = os.path.dirname(output_path)

    # Presence-verify all required sidecars before reading any of them.
    for sidecar in _REQUIRED_SIDECARS:
        sidecar_path = os.path.join(video_dir, sidecar)
        if not os.path.isfile(sidecar_path):
            raise FileNotFoundError(
                f"prepare_orador_upload_config: required sidecar missing: "
                f"{sidecar} (expected at {sidecar_path})"
            )

    title = (
        open(os.path.join(video_dir, "title.txt"), encoding="utf-8").read().strip()
    )
    description = (
        open(os.path.join(video_dir, "description.txt"), encoding="utf-8").read().strip()
    )
    thumbnail_file = os.path.join(video_dir, "thumbnail.png")

    logging.info(
        "prepare_orador_upload_config: sidecars ready for %s — title=%r",
        output_path,
        title[:60],
    )

    return {
        "video_file": output_path,
        "title": title,
        "description": description,
        "thumbnail_file": thumbnail_file,
        "category_id": "25",  # News & Politics
        "privacy_status": "private" if is_testing else "public",
        "tags": ["congress", "politics", "españa", "congreso", "debate", "parlamento"],
        "made_for_kids": False,
    }


def prepare_chapter_upload_config(
    chapter_extraction_results,
    youtube_metadata_results,
    thumbnail_results=None,
    thumbnail_result=None,
    is_testing=False,
    dry_run=False,
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
        thumbnail_results: (optional) Legacy batch results from
            thumbnail_generator.generate_video_thumbnails.
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
        thumbnail_result: (optional) Single-chapter result from the generic
            Pikzels pipeline (_generate_thumbnail). When provided and
            ``success`` is True, ``output_path`` is used as the thumbnail
            file and ``title`` overrides the upload metadata title.
            Expected structure:
            {
                'chapter_id': int,
                'success': bool,
                'output_path': str | None,
                'title': str | None,
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

    # Create thumbnail lookup by chapter_id (legacy batch path)
    thumbnail_lookup = {}
    if thumbnail_results and thumbnail_results.get('results'):
        for thumbnail in thumbnail_results['results']:
            chapter_id = thumbnail.get('chapter_id')
            if chapter_id and thumbnail.get('success'):
                thumbnail_lookup[chapter_id] = thumbnail.get('output_path')

    # Parse single-chapter Pikzels thumbnail result
    pikzels_chapter_id = None
    pikzels_thumbnail_path = None
    pikzels_title = None
    if thumbnail_result and thumbnail_result.get('success'):
        pikzels_chapter_id = thumbnail_result.get('chapter_id')
        pikzels_thumbnail_path = thumbnail_result.get('output_path')
        pikzels_title = thumbnail_result.get('title')

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

        # Apply Pikzels thumbnail result override when it matches this chapter
        if chapter_id == pikzels_chapter_id and pikzels_thumbnail_path:
            thumbnail_file = pikzels_thumbnail_path
        if chapter_id == pikzels_chapter_id and pikzels_title:
            title = pikzels_title

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

        if not dry_run and extraction_result.get('turn_id'):
            _write_orador_sidecars(video_config['video_file'], title, description)

        videos.append(video_config)

    if not videos:
        logging.warning("No valid chapter videos to upload")
        return None

    # Configuration for generic uploader
    config = {
        'token_file': resolve_token_path(DEFAULT_CHANNEL, 'upload'),
        'videos': videos
    }

    logging.info(f"Prepared chapter upload config for {len(videos)} videos")
    logging.info(f"Privacy status: {'private' if is_testing else 'public'} (is_testing={is_testing})")

    return config
