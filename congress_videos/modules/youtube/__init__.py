"""
YouTube-related modules for Congress videos project.

This package contains modules for YouTube channel monitoring, video/audio downloads,
AI-powered metadata generation, and video upload preparation.

IMPORTANT: Uses lazy imports to avoid slowing down Airflow DAG parsing.
Functions are imported only when called, not at module load time.
"""

def __getattr__(name):
    """
    Lazy import handler for youtube package.

    This allows the package to be imported quickly without loading all dependencies,
    following Airflow best practices for DAG import optimization.
    """
    # youtube_channel functions
    if name in ['fetch_youtube_channel_videos', 'filter_plenary_session_videos',
                'get_video_details', 'get_video_descriptions', 'parse_description_links',
                'scrape_press_release', 'download_and_read_agenda', 'extract_session_date',
                'extract_agenda_section']:
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
        return locals()[name]

    # download functions
    elif name in ['download_video_from_youtube', 'extract_audio_from_youtube', 'create_test_video_data', 'transcribe_audio_with_whisper', 'merge_transcription_srt_files', 'identify_interesting_chapters', 'try_download_subtitles_from_youtube']:
        from .download import (
            download_video_from_youtube,
            extract_audio_from_youtube,
            create_test_video_data,
            transcribe_audio_with_whisper,
            merge_transcription_srt_files,
            identify_interesting_chapters,
            try_download_subtitles_from_youtube,
        )
        return locals()[name]

    # youtube_ai functions
    elif name in ['generate_youtube_title', 'generate_youtube_description',
                  'generate_youtube_metadata_from_enriched_groups', 'generate_youtube_metadata_for_topics',
                  'evaluate_video_interest_with_ai', 'generate_youtube_metadata_for_selected_videos']:
        from .youtube_ai import (
            generate_youtube_title,
            generate_youtube_description,
            generate_youtube_metadata_from_enriched_groups,
            generate_youtube_metadata_for_topics,
            evaluate_video_interest_with_ai,
            generate_youtube_metadata_for_selected_videos,
        )
        return locals()[name]

    # youtube_upload functions
    elif name == 'prepare_youtube_upload_config':
        from .youtube_upload import prepare_youtube_upload_config
        return prepare_youtube_upload_config

    raise AttributeError(f"module '{__name__}' has no attribute '{name}'")

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
    'create_test_video_data',
    'transcribe_audio_with_whisper',
    'merge_transcription_srt_files',
    'identify_interesting_chapters',
    'try_download_subtitles_from_youtube',
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
