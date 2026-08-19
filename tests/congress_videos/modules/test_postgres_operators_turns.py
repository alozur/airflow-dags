"""Tests for turn-related operations in PostgreSQLOperator.

Tests:
- operation='get_uploadable_turns': returns turn rows via output_xcom_key
- operation='mark_turns_uploaded': reads upload_results xcom and calls db.mark_turns_uploaded
- operation='check_upload_quota': extended — now includes turns_pending count
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def mock_db(mocker):
    mock_instance = MagicMock()
    # Default turn-related counts to 0 so combined arithmetic stays valid
    mock_instance.count_turns_uploaded_today.return_value = 0
    mock_instance.count_pending_uploadable_turns.return_value = 0
    mocker.patch(
        "congress_videos.modules.postgres_operators.CongressionalVideoDB",
        return_value=mock_instance,
    )
    return mock_instance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_context(ti, params=None):
    return {"ti": ti, "params": params or {}}


# ---------------------------------------------------------------------------
# get_uploadable_turns
# ---------------------------------------------------------------------------


class TestGetUploadableTurns:
    """operation='get_uploadable_turns' fetches turns and pushes them via output_xcom_key."""

    def test_calls_get_uploadable_turns_with_limit_1(
        self, mock_db, mock_task_instance
    ):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_turns.return_value = []

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_uploadable_turns",
            output_xcom_key="uploadable_turns",
        )
        op.execute(_make_context(mock_task_instance))

        mock_db.get_uploadable_turns.assert_called_once_with(limit=1)

    def test_returns_turn_rows(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        fake_turns = [
            {"turn_id": 1, "output_path": "/path/t1.mp4", "resolved_name": "Ana G."}
        ]
        mock_db.get_uploadable_turns.return_value = fake_turns

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_uploadable_turns",
            output_xcom_key="uploadable_turns",
        )
        result = op.execute(_make_context(mock_task_instance))

        assert result == fake_turns

    def test_xcom_push_with_output_key(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        fake_turns = [{"turn_id": 5}]
        mock_db.get_uploadable_turns.return_value = fake_turns

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_uploadable_turns",
            output_xcom_key="uploadable_turns",
        )
        op.execute(_make_context(mock_task_instance))

        assert mock_task_instance.xcom_store.get("uploadable_turns") == fake_turns


# ---------------------------------------------------------------------------
# mark_turns_uploaded
# ---------------------------------------------------------------------------


class TestMarkTurnsUploaded:
    """operation='mark_turns_uploaded' reads upload_results xcom and marks each turn."""

    def test_calls_mark_turns_uploaded_per_entry(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"turn_id": 1, "youtube_video_id": "abc", "success": True},
                {"turn_id": 2, "youtube_video_id": "xyz", "success": True},
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        op.execute(_make_context(mock_task_instance))

        assert mock_db.mark_turns_uploaded.call_count == 2
        mock_db.mark_turns_uploaded.assert_any_call(turn_id=1, youtube_video_id="abc")
        mock_db.mark_turns_uploaded.assert_any_call(turn_id=2, youtube_video_id="xyz")

    def test_skips_failed_uploads(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"turn_id": 1, "youtube_video_id": None, "success": False},
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        op.execute(_make_context(mock_task_instance))

        mock_db.mark_turns_uploaded.assert_not_called()

    def test_handles_empty_upload_results(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = None

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        mock_db.mark_turns_uploaded.assert_not_called()
        assert result is not None  # returns a result dict


# ---------------------------------------------------------------------------
# check_upload_quota — extended with turns_pending
# ---------------------------------------------------------------------------


class TestCheckUploadQuotaExtended:
    """check_upload_quota now includes turns_pending in the result dict."""

    def test_includes_turns_pending_in_result(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 3
        mock_db.count_pending_uploadable_turns.return_value = 2

        op = PostgreSQLOperator(
            task_id="t",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        result = op.execute(_make_context(mock_task_instance, params={"min_relevance_score": 2}))

        assert "turns_pending" in result
        assert result["turns_pending"] == 2

    def test_calls_count_pending_uploadable_turns(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 0
        mock_db.count_pending_uploadable_turns.return_value = 0

        op = PostgreSQLOperator(
            task_id="t",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        op.execute(_make_context(mock_task_instance, params={"min_relevance_score": 2}))

        mock_db.count_pending_uploadable_turns.assert_called_once()

    def test_uploads_today_combines_chapters_and_turns(
        self, mock_db, mock_task_instance
    ):
        """uploads_today must equal chapters_uploaded_today + turns_uploaded_today (CRITICAL-2).

        When only chapters were uploaded (turns=0), uploads_today must equal the chapter count.
        Both count methods must be called.
        """
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 5
        mock_db.count_pending_uploadable_turns.return_value = 0

        op = PostgreSQLOperator(
            task_id="t",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        result = op.execute(_make_context(mock_task_instance, params={"min_relevance_score": 2}))

        assert result["uploads_today"] == 1
        mock_db.count_chapters_uploaded_today.assert_called_once()
        mock_db.count_turns_uploaded_today.assert_called_once()
