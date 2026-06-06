"""Tests for CongressionalVideoDB — Reap pipeline methods (video_shorts)."""

from __future__ import annotations

import pytest


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
        assert params == (4, 5)

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
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = []

        instance.get_pending_shorts(limit=2, min_virality_score=0.6)

        _, params = mock_cursor.execute.call_args[0]
        assert 0.6 in params
        assert 2 in params


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
# get_chapter_titles
# --------------------------------------------------------------------------- #

class TestGetChapterTitles:

    def test_returns_dict_of_id_to_title(self, db):
        instance, mock_cursor = db
        mock_cursor.fetchall.return_value = [
            {"id": 1, "title": "Title A"},
            {"id": 2, "title": "Title B"},
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
