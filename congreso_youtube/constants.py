"""
Congressional video processing constants.

This module contains shared constants used across the congreso_youtube project.
"""

import urllib3

# -------------------------
# URL Configuration
# -------------------------
BASE_ARCHIVE_URL = "https://www.congreso.es/es/archivo-audiovisual"
BASE_SESSION_URL = "https://app.congreso.es/AudiovisualCongreso/audiovisualdetalledisponible"

# -------------------------
# Congressional Parameters
# -------------------------
LEGISLATURE_ID = 15
ORGANO_ID = 400

# -------------------------
# Global Settings
# -------------------------
# Disable SSL warnings for congressional website
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
