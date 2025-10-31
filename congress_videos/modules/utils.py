"""
Congressional video processing utilities.

This module provides backward compatibility by re-exporting functions
from the new modular structure. All new code should import directly
from the specific modules instead.

DEPRECATED: This file will be maintained for backward compatibility only.
For new code, import from:
- congress_videos.config.constants
- congress_videos.modules.web_scraping
- congress_videos.modules.file_operations
- congress_videos.modules.youtube (youtube_ai, youtube_upload, youtube_channel, download)
"""

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Re-export constants for backward compatibility
from congress_videos.config.constants import (
    BASE_ARCHIVE_URL,
    BASE_SESSION_URL,
    LEGISLATURE_ID,
    ORGANO_ID,
)

# Re-export web scraping functions
from congress_videos.modules.web_scraping import (
    construct_session_link,
    construct_url,
    get_session_number,
    get_soup,
    has_plenary_session,
)

# Re-export file operations functions
from congress_videos.modules.file_operations import (
    download_video_file,
    download_videos_for_upload,
)

# Re-export YouTube AI functions (lazy imports to avoid DAG import timeout)
def evaluate_video_interest_with_ai(*args, **kwargs):
    from congress_videos.modules.youtube import evaluate_video_interest_with_ai as _func
    return _func(*args, **kwargs)

def generate_youtube_description(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_description as _func
    return _func(*args, **kwargs)

def generate_youtube_metadata_for_selected_videos(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_metadata_for_selected_videos as _func
    return _func(*args, **kwargs)

def generate_youtube_metadata_for_topics(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_metadata_for_topics as _func
    return _func(*args, **kwargs)

def generate_youtube_metadata_from_enriched_groups(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_metadata_from_enriched_groups as _func
    return _func(*args, **kwargs)

def generate_youtube_title(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_title as _func
    return _func(*args, **kwargs)

# Re-export YouTube upload functions (lazy import)
def prepare_youtube_upload_config(*args, **kwargs):
    from congress_videos.modules.youtube import prepare_youtube_upload_config as _func
    return _func(*args, **kwargs)

# Private functions (internal use only - not re-exported)
def _evaluate_intervention_interest(*args, **kwargs):
    from congress_videos.modules.youtube.youtube_ai import _evaluate_intervention_interest as _func
    return _func(*args, **kwargs)

def _evaluate_main_topic_interest(*args, **kwargs):
    from congress_videos.modules.youtube.youtube_ai import _evaluate_main_topic_interest as _func
    return _func(*args, **kwargs)


# All function implementations have been moved to specialized modules.
# This file now serves as a backward compatibility layer.
# For new code, please import directly from the specific modules listed above.
