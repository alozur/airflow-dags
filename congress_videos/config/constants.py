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
# Voice Activity Detection (VAD) — chapter-start adjustment (monitor)
# -------------------------
# VAD detects the real start AND end of sustained speech in each chapter's audio
# span and trims the silence on BOTH edges BEFORE the chapter is persisted to
# `video_chapters`, so chapters no longer open in the silent "dead start" nor end
# in trailing applause/silence. Runs in the monitor DAG at chapter level from a
# SINGLE VAD pass. The start only moves FORWARD; the end only moves BACKWARD
# (strict clamp to the original LLM end — VAD only ever shrinks the chapter).
VAD_ENABLED = True
VAD_BACKEND = "webrtc"  # "webrtc" (default, no torch) | "silero" (opt-in, lazy torch)
VAD_GAP_MERGE_SECS = 2.0  # join adjacent voice segments separated by gaps < this
VAD_MIN_SUSTAINED_SECS = 8.0  # a block must accumulate >= this voiced time to qualify
VAD_SAFETY_MARGIN_SECS = 2.0  # cut this many seconds before the first sustained voice (start)
VAD_END_MARGIN_SECS = 5.0  # keep this many seconds after the last sustained voice (end)
VAD_SAMPLE_RATE = 16000  # mono WAV sample rate fed to the VAD backend
VAD_MIN_CHAPTER_SECS = 5.0  # never trim an edge so far the chapter is shorter than this

# -------------------------
# Global Settings
# -------------------------
# Disable SSL warnings for congressional website
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
