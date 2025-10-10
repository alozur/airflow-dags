"""
YouTube-related modules for Congress videos project.

This package contains modules for YouTube channel monitoring, video/audio downloads,
AI-powered metadata generation, and video upload preparation.
"""

# Import all functions from youtube_channel module
from .youtube_channel import (
    fetch_youtube_channel_videos,
    filter_plenary_session_videos,
    get_video_details,
    get_video_descriptions,
    parse_description_links,
    scrape_press_release,
    download_and_read_agenda,
    extract_session_date,
    extract_agenda_section,
)

# Import all functions from download module
from .download import (
    download_video_from_youtube,
    extract_audio_from_youtube,
)

# Import all functions from youtube_ai module
from .youtube_ai import (
    generate_youtube_title,
    generate_youtube_description,
    generate_youtube_metadata_from_enriched_groups,
    generate_youtube_metadata_for_topics,
    evaluate_video_interest_with_ai,
    generate_youtube_metadata_for_selected_videos,
)

# Import all functions from youtube_upload module
from .youtube_upload import (
    prepare_youtube_upload_config,
)

__all__ = [
    # youtube_channel functions
    'fetch_youtube_channel_videos',
    'filter_plenary_session_videos',
    'get_video_details',
    'get_video_descriptions',
    'parse_description_links',
    'scrape_press_release',
    'download_and_read_agenda',
    'extract_session_date',
    'extract_agenda_section',
    # download functions
    'download_video_from_youtube',
    'extract_audio_from_youtube',
    # youtube_ai functions
    'generate_youtube_title',
    'generate_youtube_description',
    'generate_youtube_metadata_from_enriched_groups',
    'generate_youtube_metadata_for_topics',
    'evaluate_video_interest_with_ai',
    'generate_youtube_metadata_for_selected_videos',
    # youtube_upload functions
    'prepare_youtube_upload_config',
]
