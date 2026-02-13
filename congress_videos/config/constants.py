"""
Congressional video processing constants.

This module contains shared constants used across the congreso_youtube project.
"""

import urllib3

# -------------------------
# URL Configuration
# -------------------------
BASE_SESSION_URL = "https://app.congreso.es/AudiovisualCongreso/audiovisualdetalledisponible"

# -------------------------
# Congressional Parameters
# -------------------------
LEGISLATURE_ID = 15
ORGANO_ID = 400

# -------------------------
# YouTube Channel Configuration
# -------------------------
# Official Congress YouTube channel
YOUTUBE_CHANNEL_ID = "UCT3tvU3bVxOa3ZiVD-B7h9g"  # @CanalParlamento-Congreso_Es
YOUTUBE_CHANNEL_HANDLE = "@CanalParlamento-Congreso_Es"
TARGET_VIDEO_TITLE = "Sesión Plenaria (original)"  # Title to filter for monitoring

# -------------------------
# Global Settings
# -------------------------
# Disable SSL warnings for congressional website
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
