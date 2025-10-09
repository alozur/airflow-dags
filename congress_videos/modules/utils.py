"""
Congressional video processing utilities.

This module provides backward compatibility by re-exporting functions
from the new modular structure. All new code should import directly
from the specific modules instead.

DEPRECATED: This file will be maintained for backward compatibility only.
For new code, import from:
- congress_videos.config.constants
- congress_videos.modules.web_scraping
- congress_videos.modules.video_extraction
- congress_videos.modules.file_operations
- congress_videos.modules.youtube_ai
- congress_videos.modules.youtube_upload
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

# Re-export video extraction functions
from congress_videos.modules.video_extraction import (
    enrich_with_metadata,
    extract_video_data,
    extract_video_metadata,
    limit_enriched_groups_for_testing,
    organize_video_groups,
)

# Re-export file operations functions
from congress_videos.modules.file_operations import (
    create_session_folder,
    create_topic_info_file,
    download_main_topic_videos,
    download_video_file,
    download_videos_for_upload,
)

# Re-export YouTube AI functions
from congress_videos.modules.youtube_ai import (
    evaluate_video_interest_with_ai,
    generate_youtube_description,
    generate_youtube_metadata_for_selected_videos,
    generate_youtube_metadata_for_topics,
    generate_youtube_metadata_from_enriched_groups,
    generate_youtube_title,
)

# Re-export YouTube upload functions
from congress_videos.modules.youtube_upload import prepare_youtube_upload_config

# Private functions (internal use only - not re-exported)
from congress_videos.modules.youtube_ai import (
    _evaluate_intervention_interest,
    _evaluate_main_topic_interest,
)


# All function implementations have been moved to specialized modules.
# This file now serves as a backward compatibility layer.
# For new code, please import directly from the specific modules listed above.
