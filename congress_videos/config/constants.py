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
# Turn-train DAG ids (issue #159)
# -------------------------
# Canonical dag_ids for the chained turn pipeline. Chain trigger callables MUST
# import these instead of the sibling DAG module: importing a DAG module
# executes it, and Airflow >=2.4 auto-registers every DAG constructed during
# file parse — including transitively imported ones — raising
# AirflowDagDuplicatedIdException. This module defines no DAGs, so it is safe
# to import from any DAG file.
SPEAKER_TURNS_DAG_ID = "speaker_turns"
SPEAKER_TURN_VIDEOS_DAG_ID = "speaker_turn_videos"
SPEAKER_TURN_PREPARE_DAG_ID = "speaker_turn_prepare"

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
VAD_TURN_TRIM_EPSILON_SECS = 0.5  # skip rewrite when both start AND end trims are below this

# -------------------------
# Congress Participants Sync (opendataExport portlet + Wikidata enrichment)
# -------------------------
# Liferay opendataExport portlet — single POST replaces the old directory-scrape.
# Use CONGRESO_DEPUTIES_URL env var to override with a static-file URL (escape hatch).
CONGRESO_DEPUTIES_PORTLET_URL = (
    "https://www.congreso.es/es/busqueda-de-diputados"
    "?p_p_id=diputadomodule"
    "&p_p_lifecycle=2"
    "&p_p_state=normal"
    "&p_p_mode=view"
    "&p_p_resource_id=opendataExport"
    "&p_p_cacheability=cacheLevelPage"
)
# Browser-grade User-Agent required by the portlet WAF; do not use the requests default.
CONGRESO_BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0"
)
WIKIDATA_SPARQL_URL = "https://query.wikidata.org/sparql"
WIKIDATA_POSITION_QID = "Q18171345"  # member of the Congress of Deputies
WIKIDATA_FUZZY_THRESHOLD = 0.90
WIKIDATA_TIMEOUT = 30

WIKIDATA_USER_AGENT = "airflow-dags/1.0 (https://github.com/alozur/airflow-dags; alonsozurera@gmail.com)"

# -------------------------
# Congreso.es photo fallback (searchDiputados portlet + deterministic photo URL)
# -------------------------
# Env override: set CONGRESO_SEARCH_DIPUTADOS_URL_OVERRIDE to a static-file URL for local testing.
CONGRESO_SEARCH_DIPUTADOS_URL = (
    "https://www.congreso.es/es/busqueda-de-diputados"
    "?p_p_id=diputadomodule"
    "&p_p_lifecycle=2"
    "&p_p_state=normal"
    "&p_p_mode=view"
    "&p_p_resource_id=searchDiputados"
    "&p_p_cacheability=cacheLevelPage"
)
CONGRESO_PHOTO_URL_TEMPLATE = (
    "https://www.congreso.es/docu/imgweb/diputados/{cod}_{leg}.jpg"
)

# -------------------------
# Global Settings
# -------------------------
# Disable SSL warnings for congressional website
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
