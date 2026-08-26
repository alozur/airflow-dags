"""Tests for turn-related operations in PostgreSQLOperator.

Tests:
- operation='get_uploadable_turns': returns turn rows via output_xcom_key
- operation='mark_turns_uploaded': reads upload_results xcom and calls db.mark_turns_uploaded
- operation='check_upload_quota': extended — now includes turns_pending count
"""

from __future__ import annotations

import logging
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

    def test_marked_turn_logs_info(self, mock_db, mock_task_instance, caplog):
        """A successfully marked turn emits a logger.info, not a bare print() (#205 C3)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"turn_id": 7, "youtube_video_id": "yt-info", "success": True},
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
        )

        with caplog.at_level(logging.INFO):
            op.execute(_make_context(mock_task_instance))

        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("turn 7" in r.message and "yt-info" in r.message for r in infos)


class TestMarkTurnsUploadedOutputPathFallback:
    """operation='mark_turns_uploaded' falls back to output_path when turn_id is missing (#230)."""

    def test_fallback_fires_when_turn_id_missing_and_video_file_present(
        self, mock_db, mock_task_instance
    ):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.mark_turns_uploaded_by_output_path.return_value = 2
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                },
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
        mock_db.mark_turns_uploaded_by_output_path.assert_called_once_with(
            "/path/turn1.mp4", "abc"
        )

    def test_turn_id_wins_when_both_present(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": 5,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        op.execute(_make_context(mock_task_instance))

        mock_db.mark_turns_uploaded.assert_called_once_with(
            turn_id=5, youtube_video_id="abc"
        )
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()

    def test_neither_key_is_skipped_without_db_call(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": None,
                    "success": True,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        mock_db.mark_turns_uploaded.assert_not_called()
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()
        assert result["details"][0]["status"] == "skipped"

    def test_failed_upload_does_not_trigger_fallback(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": False,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        op.execute(_make_context(mock_task_instance))

        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()

    def test_details_carry_matched_by_and_output_path_on_fallback(
        self, mock_db, mock_task_instance
    ):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.mark_turns_uploaded_by_output_path.return_value = 2
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        detail = result["details"][0]
        assert detail["matched_by"] == "output_path"
        assert detail["output_path"] == "/path/turn1.mp4"
        assert detail["status"] == "updated"

    def test_details_carry_matched_by_turn_id_on_primary_path(
        self, mock_db, mock_task_instance
    ):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"turn_id": 1, "youtube_video_id": "abc", "success": True},
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        detail = result["details"][0]
        assert detail["matched_by"] == "turn_id"

    def test_fallback_zero_rows_is_skipped_not_updated(self, mock_db, mock_task_instance):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.mark_turns_uploaded_by_output_path.return_value = 0
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/gone.mp4",
                    "success": True,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        assert result["updated_turns"] == 0
        detail = result["details"][0]
        assert detail["status"] == "skipped"
        assert detail["reason"] == "output_path_not_found"

    def test_fallback_db_exception_increments_failed_updates(
        self, mock_db, mock_task_instance
    ):
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.mark_turns_uploaded_by_output_path.side_effect = Exception("db down")
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                },
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        assert result["failed_updates"] == 1
        assert result["updated_turns"] == 0

    def test_existing_skip_reason_string_unchanged_for_no_usable_key(
        self, mock_db, mock_task_instance
    ):
        """The pre-existing skip reason string must stay 'upload_failed_or_missing_fields'."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"turn_id": None, "youtube_video_id": None, "success": False},
            ]
        }

        op = PostgreSQLOperator(
            task_id="t",
            operation="mark_turns_uploaded",
            xcom_keys={"upload_results": "upload_results"},
            output_xcom_key="turn_upload_updates",
        )
        result = op.execute(_make_context(mock_task_instance))

        assert result["details"][0]["reason"] == "upload_failed_or_missing_fields"


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
