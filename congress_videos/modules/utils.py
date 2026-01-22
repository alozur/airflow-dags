"""
Congressional video processing utilities.

This module provides backward compatibility by re-exporting functions
from the new modular structure. All new code should import directly
from the specific modules instead.

DEPRECATED: This file will be maintained for backward compatibility only.
For new code, import from:
- congress_videos.modules.file_operations
- congress_videos.modules.youtube (youtube_ai, youtube_upload, youtube_channel, download)
"""

import logging

# Configure logging
logging.basicConfig(level=logging.INFO)

# Re-export file operations functions
from congress_videos.modules.file_operations import (
    download_videos_for_upload,
)

# Re-export YouTube AI functions (lazy imports to avoid DAG import timeout)
def generate_youtube_metadata_for_selected_videos(*args, **kwargs):
    from congress_videos.modules.youtube import generate_youtube_metadata_for_selected_videos as _func
    return _func(*args, **kwargs)


# All function implementations have been moved to specialized modules.
# This file now serves as a backward compatibility layer.
# For new code, please import directly from the specific modules listed above.
