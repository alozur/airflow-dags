"""Surface guard for `CongressionalVideoDB` — issue #226.

Asserts the 21 audited dead methods (12 fully orphaned + 9 kept alive only
by their own tests in `test_database.py`) are gone, and the 6 confirmed-live
methods are untouched.
"""

from __future__ import annotations

import pytest

from congress_videos.modules.database import CongressionalVideoDB

DEAD_METHOD_NAMES = [
    # Orphaned — zero references anywhere in congress_videos/**, tests/**,
    # scripts/**, utils/**, docs/** before this deletion.
    "update_main_topic_status",
    "get_main_topics_by_session",
    "update_youtube_metadata",
    "update_thumbnail_info",
    "get_interventions_by_main_topic",
    "update_ai_interest_evaluation",
    "update_youtube_upload_status",
    "get_videos_from_upload_queue",
    "add_videos_to_upload_queue",
    "update_upload_queue_status",
    "remove_from_upload_queue",
    "get_chapter_statistics",
    # Test-only — kept alive solely by their own test class in
    # tests/congress_videos/modules/test_database.py (deleted alongside).
    "create_or_update_session",
    "upsert_video_topic",
    "mark_video_uploaded",
    "get_uploadable_videos",
    "add_to_upload_queue",
    "get_session_by_number_and_date",
    "get_video_topics_by_session",
    "update_session_total_topics",
    "get_top_videos_for_upload",
]

LIVE_METHOD_NAMES = [
    "get_uploadable_chapters",
    "get_uploadable_turns",
    "mark_short_uploaded",
    "get_chapters_for_shorts",
    "mark_upload_verified",
    "record_upload_verification_failure",
]


@pytest.mark.parametrize("name", DEAD_METHOD_NAMES)
def test_dead_method_absent(name):
    """None of the 21 audited dead method names remain on the class."""
    assert not hasattr(CongressionalVideoDB, name)


@pytest.mark.parametrize("name", LIVE_METHOD_NAMES)
def test_live_method_present(name):
    """The 6 confirmed-live methods are untouched by the deletion."""
    assert hasattr(CongressionalVideoDB, name)
