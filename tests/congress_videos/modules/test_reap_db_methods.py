"""Tests for CongressionalVideoDB — Reap pipeline methods (video_shorts)."""

from __future__ import annotations

import logging

import pytest

from congress_videos.modules.database import (
    SHORTS_PENDING_CANDIDATE_LIMIT,
    SHORTS_UPLOAD_HISTORY_LIMIT,
    filter_shorts_by_source_cooldown,
)


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def db(mocker):
    from unittest.mock import MagicMock

    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = None
    mock_cursor.fetchall.return_value = []
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__ = MagicMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)

    mocker.patch("psycopg2.connect", return_value=mock_conn)

    from congress_videos.modules.database import CongressionalVideoDB
    instance = CongressionalVideoDB()
    return instance, mock_cursor


# --------------------------------------------------------------------------- #
# get_chapters_for_shorts
# --------------------------------------------------------------------------- #

class TestGetChaptersForShorts:

    def test_returns_list_of_chapters(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"chapter_id": 1, "relevance_score": 4},
            {"chapter_id": 2, "relevance_score": 5},
        ]

        result = instance.get_chapters_for_shorts()

        assert len(result) == 2

    def test_empty_result_returns_empty_list(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_chapters_for_shorts()

        assert result == []

    def test_passes_limit_and_min_score_params(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_chapters_for_shorts(limit=5, min_relevance_score=4)

        _, params = mock_cursor.execute.call_args[0]
        assert params == [4, 5]

    def test_query_contains_not_exists_subquery(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_chapters_for_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "video_shorts" in sql


# --------------------------------------------------------------------------- #
# insert_video_short
# --------------------------------------------------------------------------- #

class TestInsertVideoShort:

    def test_returns_inserted_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 42}

        result = instance.insert_video_short(
            chapter_id=10,
            reap_project_id="proj-001",
            reap_status="processing",
        )

        assert result == 42

    def test_passes_all_params_to_query(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 7}

        instance.insert_video_short(
            chapter_id=3,
            reap_project_id="proj-xyz",
            reap_status="processing",
            pretrim_start_secs=60.0,
            pretrim_end_secs=420.0,
            pretrim_used_srt=True,
        )

        _, params = mock_cursor.execute.call_args[0]
        assert 3 in params
        assert "proj-xyz" in params
        assert 60.0 in params
        assert 420.0 in params
        assert True in params

    def test_query_contains_insert_returning(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short(chapter_id=1, reap_project_id="p-1")

        sql = mock_cursor.execute.call_args[0][0]
        assert "INSERT" in sql
        assert "RETURNING" in sql


# --------------------------------------------------------------------------- #
# insert_video_short_clip
# --------------------------------------------------------------------------- #

class TestInsertVideoShortClip:

    def test_returns_inserted_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 99}

        result = instance.insert_video_short_clip(
            chapter_id=5,
            reap_project_id="proj-abc",
            reap_clip_id="clip-001",
            reap_virality_score=0.85,
            reap_clip_url="https://cdn.reap.video/c.mp4",
            local_file_path="/data/clip.mp4",
        )

        assert result == 99

    def test_passes_clip_id_and_virality_to_query(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"id": 1}

        instance.insert_video_short_clip(
            chapter_id=1,
            reap_project_id="p",
            reap_clip_id="clip-xyz",
            reap_virality_score=0.75,
            reap_clip_url="https://cdn.example.com/clip.mp4",
            local_file_path="/data/clip.mp4",
        )

        _, params = mock_cursor.execute.call_args[0]
        assert "clip-xyz" in params
        assert 0.75 in params


# --------------------------------------------------------------------------- #
# update_video_short_status
# --------------------------------------------------------------------------- #

class TestUpdateVideoShortStatus:

    def test_executes_update_with_correct_params(self, db):
        instance, mock_cursor = db

        instance.update_video_short_status("proj-001", "failed")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "reap_status" in sql
        assert params == ("failed", "proj-001")

    def test_parameterized_no_injection(self, db):
        instance, mock_cursor = db

        instance.update_video_short_status("proj-safe", "expired")

        sql, params = mock_cursor.execute.call_args[0]
        assert "proj-safe" not in sql
        assert "proj-safe" in params


# --------------------------------------------------------------------------- #
# filter_shorts_by_source_cooldown (pure helper)
# --------------------------------------------------------------------------- #

class TestFilterShortsBySourceCooldown:

    def test_blocked_before_cooldown_elapses(self):
        """Only 4 other-video uploads since V's last upload — still blocked."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "V"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == []

    def test_released_after_exactly_five_other_uploads(self):
        """Exactly 5 other-video uploads since V's last upload — eligible."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "other5"},
            {"video_id": "V"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_boundary_four_vs_five(self):
        """Index 4 (blocked) vs index 5 (eligible) for two different source videos."""
        candidates = [
            {"id": 1, "video_id": "A"},  # index 4 in history
            {"id": 2, "video_id": "B"},  # index 5 in history
        ]
        upload_history = [
            {"video_id": "x1"},
            {"video_id": "x2"},
            {"video_id": "x3"},
            {"video_id": "x4"},
            {"video_id": "A"},
            {"video_id": "B"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == [{"id": 2, "video_id": "B"}]

    def test_video_with_no_upload_history_is_eligible(self):
        """V never appears in upload_history — eligible regardless of other videos' history."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "other2"},
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_history_window_outside_bounded_history_is_eligible(self):
        """V absent from the (bounded) history list passed in — never a stale lockout."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [{"video_id": f"other{i}"} for i in range(SHORTS_UPLOAD_HISTORY_LIMIT)]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_multi_occurrence_uses_first_match_index(self):
        """Repeated V entries in history: only the MOST RECENT (first) occurrence counts."""
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [
            {"video_id": "other1"},
            {"video_id": "V"},          # most recent V occurrence — index 1
            {"video_id": "other2"},
            {"video_id": "other3"},
            {"video_id": "other4"},
            {"video_id": "other5"},
            {"video_id": "V"},          # older occurrence — must be ignored
        ]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == []

    def test_missing_video_id_fails_open(self):
        """Candidate row without a video_id key is eligible regardless of history."""
        candidates = [{"id": 1}]
        upload_history = [{"video_id": "irrelevant"}]

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_order_preserved_among_eligible(self):
        """Filtering never reorders — only removes cooling-down rows."""
        candidates = [
            {"id": 1, "video_id": "A"},
            {"id": 2, "video_id": "B"},
            {"id": 3, "video_id": "C"},
        ]
        upload_history: list[dict] = []

        result = filter_shorts_by_source_cooldown(candidates, upload_history)

        assert result == candidates

    def test_empty_upload_history_all_eligible(self):
        candidates = [{"id": 1, "video_id": "V"}, {"id": 2, "video_id": "W"}]

        result = filter_shorts_by_source_cooldown(candidates, [])

        assert result == candidates

    def test_cooldown_zero_all_eligible(self):
        candidates = [{"id": 1, "video_id": "V"}]
        upload_history = [{"video_id": "V"}]

        result = filter_shorts_by_source_cooldown(candidates, upload_history, cooldown=0)

        assert result == candidates


# --------------------------------------------------------------------------- #
# get_pending_shorts
# --------------------------------------------------------------------------- #

class TestGetPendingShorts:

    def test_returns_list_of_shorts(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"id": 1, "reap_clip_id": "c-001", "reap_virality_score": 0.8},
        ]

        result = instance.get_pending_shorts()

        assert len(result) == 1

    def test_empty_result_returns_empty_list(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        result = instance.get_pending_shorts()

        assert result == []

    def test_limit_is_applied(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_pending_shorts(limit=3)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LIMIT" in sql

    def test_virality_filter_passed_as_param(self, db):
        """min_virality_score and SHORTS_PENDING_CANDIDATE_LIMIT are bound to the candidate
        query — the candidate LIMIT is decoupled from `limit`, which is applied afterward in
        Python and asserted here via the truncated return value."""
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [
            [],
            [{"id": 1, "video_id": "A"}, {"id": 2, "video_id": "B"}],
        ]

        result = instance.get_pending_shorts(limit=1, min_virality_score=0.6)

        _, candidate_params = mock_cursor.execute.call_args_list[1][0]
        assert 0.6 in candidate_params
        assert SHORTS_PENDING_CANDIDATE_LIMIT in candidate_params
        assert result == [{"id": 1, "video_id": "A"}]

    def test_query_excludes_abandoned_shorts(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_pending_shorts()

        sql = mock_cursor.execute.call_args[0][0]
        assert "is_upload_abandoned = FALSE" in sql

    def test_history_query_runs_first_with_expected_shape(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts()

        history_sql, history_params = mock_cursor.execute.call_args_list[0][0]
        assert "is_uploaded = TRUE" in history_sql
        assert "ORDER BY vs.updated_at DESC" in history_sql
        assert SHORTS_UPLOAD_HISTORY_LIMIT in history_params

    def test_candidate_query_joins_video_chapters_and_selects_video_id(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.side_effect = [[], []]

        instance.get_pending_shorts(min_virality_score=0.6)

        candidate_sql, candidate_params = mock_cursor.execute.call_args_list[1][0]
        assert "JOIN" in candidate_sql
        assert "video_chapters" in candidate_sql
        assert "vc.video_id" in candidate_sql
        assert 0.6 in candidate_params
        assert SHORTS_PENDING_CANDIDATE_LIMIT in candidate_params

    def test_result_truncated_to_limit(self, db):
        instance, mock_cursor = db
        candidates = [
            {"id": 1, "video_id": "A"},
            {"id": 2, "video_id": "B"},
            {"id": 3, "video_id": "C"},
        ]
        mock_cursor.fetchall.side_effect = [[], candidates]

        result = instance.get_pending_shorts(limit=2)

        assert result == candidates[:2]

    def test_eligible_row_deeper_than_limit_is_returned_past_cooling_down_head(self, db):
        """Cooling-down head row must not zero out the run — over-fetch + Python filter
        surfaces the eligible row that sits deeper in the candidate list."""
        instance, mock_cursor = db
        history = [
            {"video_id": "hot"},
            {"video_id": "other"},
            {"video_id": "hot"},
        ]
        candidates = [
            {"id": 1, "video_id": "hot"},   # cooling down (index 0 < cooldown 5)
            {"id": 2, "video_id": "cold"},  # never uploaded -> eligible
        ]
        mock_cursor.fetchall.side_effect = [history, candidates]

        result = instance.get_pending_shorts(limit=1)

        assert result == [{"id": 2, "video_id": "cold"}]

    def test_all_cooling_down_returns_empty_and_logs_info(self, db, caplog):
        instance, mock_cursor = db
        history = [{"video_id": "V"}]
        candidates = [{"id": 1, "video_id": "V"}]
        mock_cursor.fetchall.side_effect = [history, candidates]

        with caplog.at_level(logging.INFO, logger="congress_videos.modules.database"):
            result = instance.get_pending_shorts()

        assert result == []
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("cooling down" in m.lower() for m in info_messages)

    def test_partial_block_logs_blocked_count(self, db, caplog):
        instance, mock_cursor = db
        history = [{"video_id": "hot"}]
        candidates = [
            {"id": 1, "video_id": "hot"},   # blocked
            {"id": 2, "video_id": "cold"},  # eligible
        ]
        mock_cursor.fetchall.side_effect = [history, candidates]

        with caplog.at_level(logging.INFO, logger="congress_videos.modules.database"):
            result = instance.get_pending_shorts()

        assert result == [{"id": 2, "video_id": "cold"}]
        info_messages = [r.message for r in caplog.records if r.levelno == logging.INFO]
        assert any("blocked 1" in m.lower() for m in info_messages)


# --------------------------------------------------------------------------- #
# mark_short_uploaded
# --------------------------------------------------------------------------- #

class TestMarkShortUploaded:

    def test_executes_update_with_correct_params(self, db):
        instance, mock_cursor = db

        instance.mark_short_uploaded("clip-001", "yt-video-abc")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "is_uploaded" in sql
        assert params == ("yt-video-abc", "clip-001")

    def test_parameterized_query(self, db):
        instance, mock_cursor = db

        instance.mark_short_uploaded("clip-safe", "yt-safe")

        sql, params = mock_cursor.execute.call_args[0]
        assert "clip-safe" not in sql
        assert "clip-safe" in params


# --------------------------------------------------------------------------- #
# record_short_upload_failure
# --------------------------------------------------------------------------- #

class TestRecordShortUploadFailure:

    def test_normal_increment_updates_attempts_and_error(self, db):
        """Non-threshold-crossing failure increments upload_attempts, stores error."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-001", error_message="quota exceeded")

        sql, params = mock_cursor.execute.call_args[0]
        assert "UPDATE" in sql
        assert "upload_attempts = upload_attempts + 1" in sql
        assert "is_upload_abandoned" in sql
        assert "last_upload_error" in sql
        assert params == ("quota exceeded", "clip-001")

    def test_threshold_crossing_sets_abandoned_condition(self, db):
        """SQL encodes the >= 3 (SHORTS_UPLOAD_ABANDON_THRESHOLD) abandon condition."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-002", error_message="timeout")

        sql, _ = mock_cursor.execute.call_args[0]
        assert ">= 3" in sql

    def test_error_message_none_path(self, db):
        """error_message=None is passed through as a None param, not a crash."""
        instance, mock_cursor = db

        instance.record_short_upload_failure(reap_clip_id="clip-003", error_message=None)

        sql, params = mock_cursor.execute.call_args[0]
        assert params == (None, "clip-003")

    def test_warning_logged_when_threshold_crossed_on_this_call(self, db, caplog):
        """A distinct WARNING fires when this call is the one crossing the abandon threshold."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 3,
            "is_upload_abandoned": True,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_short_upload_failure(reap_clip_id="clip-004", error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "clip-004" in warnings[0].message
        assert "abandoned" in warnings[0].message.lower()

    def test_no_warning_logged_for_ordinary_retry_increment(self, db, caplog):
        """An ordinary (non-crossing) failure increment does not emit the abandonment WARNING."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "upload_attempts": 1,
            "is_upload_abandoned": False,
        }

        with caplog.at_level(logging.WARNING, logger="congress_videos.modules.database"):
            instance.record_short_upload_failure(reap_clip_id="clip-005", error_message="boom")

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 0


# --------------------------------------------------------------------------- #
# get_chapter_titles
# --------------------------------------------------------------------------- #

class TestGetChapterTitles:

    def test_returns_dict_of_id_to_title(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"chapter_id": 1, "title": "Title A"},
            {"chapter_id": 2, "title": "Title B"},
        ]

        result = instance.get_chapter_titles([1, 2])

        assert result == {1: "Title A", 2: "Title B"}

    def test_empty_input_returns_empty_dict_without_db_call(self, db):
        instance, mock_cursor = db

        result = instance.get_chapter_titles([])

        assert result == {}
        mock_cursor.execute.assert_not_called()

    def test_passes_chapter_ids_as_array_param(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_chapter_titles([10, 20, 30])

        _, params = mock_cursor.execute.call_args[0]
        assert [10, 20, 30] in params


# --------------------------------------------------------------------------- #
# get_chapter_metadata
# --------------------------------------------------------------------------- #

class TestGetChapterMetadata:

    def test_get_chapter_metadata_returns_source_fields(self, db):
        """AC#1 — chapter with linked source video returns both source fields and youtube_video_id."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 1,
            "title": "Debate vivienda",
            "description": "Descripción del capítulo",
            "speakers": ["Pedro Sánchez"],
            "key_speakers": ["Pedro Sánchez"],
            "topics": ["vivienda"],
            "scoring_reasoning": "Relevant",
            "relevance_score": 4,
            "youtube_video_id": "yt-own-abc",
            "source_video_title": "Sesión plenaria 2024-01-15",
            "source_video_url": "https://youtube.com/watch?v=abc123",
        }

        result = instance.get_chapter_metadata(1)

        assert result is not None
        assert result["youtube_video_id"] == "yt-own-abc"
        assert result["source_video_title"] == "Sesión plenaria 2024-01-15"
        assert result["source_video_url"] == "https://youtube.com/watch?v=abc123"

    def test_get_chapter_metadata_null_source(self, db):
        """AC#2, AC#7 — chapter with no source video row returns None for source fields and youtube_video_id."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {
            "chapter_id": 2,
            "title": "Sin fuente",
            "description": "Sin descripción",
            "speakers": [],
            "key_speakers": [],
            "topics": [],
            "scoring_reasoning": "",
            "relevance_score": 3,
            "youtube_video_id": None,
            "source_video_title": None,
            "source_video_url": None,
        }

        result = instance.get_chapter_metadata(2)

        assert result is not None
        assert result["youtube_video_id"] is None
        assert result["source_video_title"] is None
        assert result["source_video_url"] is None

    def test_get_chapter_metadata_missing_chapter(self, db):
        """AC#3 — chapter_id not found returns None (unchanged behaviour)."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        result = instance.get_chapter_metadata(999)

        assert result is None

    def test_get_chapter_metadata_query_uses_left_join(self, db):
        """Query must include LEFT JOIN to youtube_source_videos."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "LEFT JOIN" in sql
        assert "youtube_source_videos" in sql

    def test_get_chapter_metadata_selects_source_columns(self, db):
        """Query must select youtube_video_id, source_video_title and source_video_url aliases."""
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_chapter_metadata(1)

        sql = mock_cursor.execute.call_args[0][0]
        assert "youtube_video_id" in sql
        assert "source_video_title" in sql
        assert "source_video_url" in sql


# --------------------------------------------------------------------------- #
# get_source_video_id_for_chapter
# --------------------------------------------------------------------------- #

class TestGetSourceVideoIdForChapter:

    @pytest.mark.parametrize("row,expected", [
        ({"video_id": "src_vid_001"}, "src_vid_001"),
        (None, None),
    ])
    def test_returns_video_id_or_none(self, db, row, expected):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = row

        result = instance.get_source_video_id_for_chapter(7)

        assert result == expected

    def test_null_video_id_in_row_returns_none(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = {"video_id": None}

        result = instance.get_source_video_id_for_chapter(99)

        assert result is None

    def test_query_selects_from_video_chapters(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_source_video_id_for_chapter(5)

        sql = mock_cursor.execute.call_args[0][0]
        assert "video_chapters" in sql
        assert "video_id" in sql

    def test_passes_chapter_id_as_param(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchone.return_value = None

        instance.get_source_video_id_for_chapter(42)

        _, params = mock_cursor.execute.call_args[0]
        assert 42 in params
