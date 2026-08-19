"""Tests for PostgreSQLOperator custom Airflow operator — TASK-022."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

# NOTE: no `airflow.utils.decorators` stub here. The real `apache-airflow==2.10.2`
# package is now a declared runtime dependency (see pyproject.toml) and already
# provides `apply_defaults`. A previous shim unconditionally replaced
# `sys.modules["airflow.utils.decorators"]` with a fake module missing symbols
# like `fixup_decorator_warning_stack`, which poisoned every later import of
# `airflow.models.baseoperator` for the rest of the pytest process. Removed.


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture(autouse=True)
def set_pg_env(monkeypatch):
    """Provide minimal env vars so PostgresConnection.__init__ does not raise."""
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def mock_db(mocker):
    """Patch CongressionalVideoDB and return the mock instance.

    Default return values for turn-related methods are set to 0 so existing
    tests that only configure chapter methods still produce valid integer sums
    in check_upload_quota (combined uploads_today = chapters + turns).
    """
    mock_instance = MagicMock()
    mock_instance.count_turns_uploaded_today.return_value = 0
    mock_instance.count_pending_uploadable_turns.return_value = 0
    mocker.patch(
        "congress_videos.modules.postgres_operators.CongressionalVideoDB",
        return_value=mock_instance,
    )
    return mock_instance


@pytest.fixture
def make_context(mock_task_instance):
    """Build a minimal Airflow context dict."""

    def _make(params: dict | None = None, ti=None):
        return {
            "ti": ti or mock_task_instance,
            "params": params or {},
        }

    return _make


# --------------------------------------------------------------------------- #
# __init__ attribute tests
# --------------------------------------------------------------------------- #

class TestPostgreSQLOperatorInit:

    def test_operation_stored(self, mock_db):
        """operation attribute is set correctly on __init__."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_metadata")
        assert op.operation == "save_youtube_metadata"

    def test_xcom_keys_default_to_empty_dict(self, mock_db):
        """xcom_keys defaults to {} when not provided."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="get_top_videos_for_upload")
        assert op.xcom_keys == {}

    def test_output_xcom_key_default_none(self, mock_db):
        """output_xcom_key defaults to None when not provided."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="get_top_videos_for_upload")
        assert op.output_xcom_key is None

    def test_custom_xcom_keys(self, mock_db):
        """Custom xcom_keys are preserved."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        keys = {"download_results": "my_key"}
        op = PostgreSQLOperator(task_id="t", operation="update_downloads", xcom_keys=keys)
        assert op.xcom_keys == keys

    def test_output_xcom_key_set(self, mock_db):
        """Custom output_xcom_key is stored."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_top_videos_for_upload",
            output_xcom_key="result_key",
        )
        assert op.output_xcom_key == "result_key"


# --------------------------------------------------------------------------- #
# execute — update_downloads operation
# --------------------------------------------------------------------------- #

class TestExecuteUpdateDownloads:

    def test_happy_path_with_download_details_dict(
        self, mock_db, mock_task_instance, make_context
    ):
        """download_results with 'download_details' list updates each successful item."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="update_downloads")

        mock_task_instance.xcom_store["download_results"] = {
            "download_details": [
                {"success": True, "file_path": "/data/v1.mp4", "file_size": 1024, "duration": 300},
                {"success": False, "file_path": None},
            ]
        }
        mock_task_instance.xcom_store["db_topic_ids"] = ["entry-001", "entry-002"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 1
        mock_db.update_download_info.assert_called_once_with(
            "entry-001", "/data/v1.mp4", 1024, 300
        )

    def test_download_results_as_list(
        self, mock_db, mock_task_instance, make_context
    ):
        """download_results directly as list is handled."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="update_downloads")

        mock_task_instance.xcom_store["download_results"] = [
            {"success": True, "file_path": "/data/v.mp4", "file_size": 512, "duration": 60},
        ]
        mock_task_instance.xcom_store["db_topic_ids"] = ["entry-x"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 1

    def test_invalid_download_results_format_returns_error(
        self, mock_db, mock_task_instance, make_context
    ):
        """Unexpected download_results type returns error dict without exception."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="update_downloads")

        mock_task_instance.xcom_store["download_results"] = "not-a-dict-or-list"
        mock_task_instance.xcom_store["db_topic_ids"] = ["entry-x"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 0
        assert "error" in result

    def test_topic_ids_not_list_returns_error(
        self, mock_db, mock_task_instance, make_context
    ):
        """topic_ids not a list returns error without exception."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="update_downloads")

        mock_task_instance.xcom_store["download_results"] = {
            "download_details": [{"success": True, "file_path": "/p.mp4"}]
        }
        mock_task_instance.xcom_store["db_topic_ids"] = "not-a-list"

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 0
        assert "error" in result


# --------------------------------------------------------------------------- #
# execute — save_youtube_metadata operation
# --------------------------------------------------------------------------- #

class TestExecuteSaveYoutubeMetadata:

    def test_saves_valid_metadata(
        self, mock_db, mock_task_instance, make_context
    ):
        """update_youtube_metadata called for each item with title+description."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_metadata")

        mock_task_instance.xcom_store["youtube_metadata_results"] = {
            "topic_metadata": [
                {
                    "topic_entry_id": "e1",
                    "title": {"title": "Debate sobre Presupuestos"},
                    "description": {"description": "Descripcion larga aqui."},
                }
            ]
        }
        mock_task_instance.xcom_store["db_topic_ids"] = ["e1"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 1
        mock_db.update_youtube_metadata.assert_called_once_with(
            "e1", "Debate sobre Presupuestos", "Descripcion larga aqui."
        )

    def test_skips_item_with_missing_title(
        self, mock_db, mock_task_instance, make_context
    ):
        """Item with empty title is skipped silently."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_metadata")

        mock_task_instance.xcom_store["youtube_metadata_results"] = {
            "topic_metadata": [
                {
                    "topic_entry_id": "e2",
                    "title": {"title": ""},
                    "description": {"description": "Some desc"},
                }
            ]
        }
        mock_task_instance.xcom_store["db_topic_ids"] = ["e2"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 0
        mock_db.update_youtube_metadata.assert_not_called()

    def test_invalid_metadata_format_returns_error(
        self, mock_db, mock_task_instance, make_context
    ):
        """Unexpected metadata format returns error dict."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_metadata")

        mock_task_instance.xcom_store["youtube_metadata_results"] = 12345
        mock_task_instance.xcom_store["db_topic_ids"] = ["e1"]

        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["updated_topics"] == 0
        assert "error" in result


# --------------------------------------------------------------------------- #
# execute — unknown operation raises ValueError
# --------------------------------------------------------------------------- #

class TestExecuteUnknownOperation:

    def test_unknown_operation_raises_value_error(
        self, mock_db, mock_task_instance, make_context
    ):
        """Unsupported operation string raises ValueError."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="does_not_exist")
        ctx = make_context(ti=mock_task_instance)

        with pytest.raises(ValueError, match="Unknown operation"):
            op.execute(ctx)


# --------------------------------------------------------------------------- #
# execute — output_xcom_key pushes result
# --------------------------------------------------------------------------- #

class TestExecuteXcomPush:

    def test_result_pushed_to_xcom_when_key_set(
        self, mock_db, mock_task_instance, make_context
    ):
        """Result is pushed to xcom when output_xcom_key is configured."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_top_videos_for_upload.return_value = [{"entry_id": "v1"}]

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_top_videos_for_upload",
            output_xcom_key="top_vids",
        )
        ctx = make_context(
            params={"max_videos": 5, "min_interest_score": 6},
            ti=mock_task_instance,
        )
        op.execute(ctx)

        assert mock_task_instance.xcom_store.get("top_vids") is not None

    def test_result_not_pushed_when_key_is_none(
        self, mock_db, mock_task_instance, make_context
    ):
        """No xcom_push when output_xcom_key is None."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_top_videos_for_upload.return_value = []

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_top_videos_for_upload",
        )
        ctx = make_context(
            params={"max_videos": 5, "min_interest_score": 6},
            ti=mock_task_instance,
        )
        op.execute(ctx)

        mock_task_instance.xcom_push.assert_not_called()


# --------------------------------------------------------------------------- #
# execute — check_upload_quota operation
# --------------------------------------------------------------------------- #

class TestExecuteCheckUploadQuota:

    def test_returns_uploads_today_and_queue_size(
        self, mock_db, mock_task_instance, make_context
    ):
        """check_upload_quota returns uploads_today and queue_size."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 10

        op = PostgreSQLOperator(
            task_id="t",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["uploads_today"] == 0
        assert result["queue_size"] == 10

    def test_returns_correct_queue_size_when_large_backlog(
        self, mock_db, mock_task_instance, make_context
    ):
        """queue_size reflects actual DB count when backlog is large."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 16

        op = PostgreSQLOperator(task_id="t", operation="check_upload_quota")
        ctx = make_context(params={}, ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["queue_size"] == 16
        assert result["uploads_today"] == 0

    def test_result_pushed_to_xcom(
        self, mock_db, mock_task_instance, make_context
    ):
        """Result dict is pushed to XCom under upload_quota key."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 3

        op = PostgreSQLOperator(
            task_id="t",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ctx = make_context(params={}, ti=mock_task_instance)
        op.execute(ctx)

        stored = mock_task_instance.xcom_store.get("upload_quota")
        assert stored is not None
        assert "queue_size" in stored

    # REQ-QUOTA-01: max_uploads/remaining_quota removed from check_upload_quota
    def test_does_not_contain_max_uploads(
        self, mock_db, mock_task_instance, make_context
    ):
        """check_upload_quota result MUST NOT contain max_uploads key (REQ-QUOTA-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 5

        op = PostgreSQLOperator(task_id="t", operation="check_upload_quota")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        result = op.execute(ctx)

        assert "max_uploads" not in result

    def test_does_not_contain_remaining_quota(
        self, mock_db, mock_task_instance, make_context
    ):
        """check_upload_quota result MUST NOT contain remaining_quota key (REQ-QUOTA-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_pending_uploadable_chapters.return_value = 20

        op = PostgreSQLOperator(task_id="t", operation="check_upload_quota")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        result = op.execute(ctx)

        assert "remaining_quota" not in result

    def test_result_contains_queue_size_and_uploads_today(
        self, mock_db, mock_task_instance, make_context
    ):
        """check_upload_quota result must contain at least queue_size and uploads_today (REQ-QUOTA-01).

        turns_pending is also included in the result dict as an informational field.
        uploads_today = chapters_uploaded_today + turns_uploaded_today (combined cap).
        queue_size = chapters_pending + turns_pending (combined queue).
        """
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.count_chapters_uploaded_today.return_value = 2
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 30
        mock_db.count_pending_uploadable_turns.return_value = 0

        op = PostgreSQLOperator(task_id="t", operation="check_upload_quota")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        result = op.execute(ctx)

        assert result["uploads_today"] == 2
        assert result["queue_size"] == 30
        # turns_pending is now included as an informational key alongside queue_size/uploads_today
        assert "queue_size" in result
        assert "uploads_today" in result


# --------------------------------------------------------------------------- #
# execute — get_uploadable_chapters uses hardcoded limit=1
# --------------------------------------------------------------------------- #

class TestGetUploadableChaptersHardcodedLimit:

    def test_always_uses_limit_1_regardless_of_xcom(
        self, mock_db, mock_task_instance, make_context
    ):
        """get_uploadable_chapters uses hardcoded limit=1, ignoring any XCom remaining_quota (REQ-LIMIT-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_chapters.return_value = []
        # Even when XCom carries remaining_quota=5, limit must be 1
        mock_task_instance.xcom_store["upload_quota"] = {
            "uploads_today": 0,
            "queue_size": 50,
        }

        op = PostgreSQLOperator(task_id="t", operation="get_uploadable_chapters")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        op.execute(ctx)

        mock_db.get_uploadable_chapters.assert_called_once_with(limit=1, min_relevance_score=2)

    def test_uses_limit_1_when_no_xcom(
        self, mock_db, mock_task_instance, make_context
    ):
        """get_uploadable_chapters uses limit=1 even when upload_quota XCom is absent (REQ-LIMIT-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_chapters.return_value = []

        op = PostgreSQLOperator(task_id="t", operation="get_uploadable_chapters")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        op.execute(ctx)

        mock_db.get_uploadable_chapters.assert_called_once_with(limit=1, min_relevance_score=2)


# --------------------------------------------------------------------------- #
# execute — get_uploadable_chapters reads limit from XCom
# --------------------------------------------------------------------------- #

class TestGetUploadableChaptersQuotaLimit:

    def test_uses_hardcoded_limit_1(
        self, mock_db, mock_task_instance, make_context
    ):
        """get_uploadable_chapters always uses hardcoded limit=1 (REQ-LIMIT-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_chapters.return_value = []
        mock_task_instance.xcom_store["upload_quota"] = {
            "uploads_today": 0,
            "queue_size": 5,
        }

        op = PostgreSQLOperator(task_id="t", operation="get_uploadable_chapters")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        op.execute(ctx)

        mock_db.get_uploadable_chapters.assert_called_once_with(limit=1, min_relevance_score=2)

    def test_uses_hardcoded_limit_1_when_no_xcom(
        self, mock_db, mock_task_instance, make_context
    ):
        """Uses hardcoded limit=1 even when upload_quota XCom key is absent (REQ-LIMIT-01)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_chapters.return_value = []

        op = PostgreSQLOperator(task_id="t", operation="get_uploadable_chapters")
        ctx = make_context(params={"min_relevance_score": 2}, ti=mock_task_instance)
        op.execute(ctx)

        mock_db.get_uploadable_chapters.assert_called_once_with(limit=1, min_relevance_score=2)


# --------------------------------------------------------------------------- #
# execute — analytics dispatch operations
# --------------------------------------------------------------------------- #

class TestExecuteGetPendingAnalyticsCheckpoints:
    """op='get_pending_analytics_checkpoints' routes to db.get_pending_analytics_checkpoints()."""

    def test_routes_to_db_method(self, mock_db, mock_task_instance, make_context):
        """Executing 'get_pending_analytics_checkpoints' calls the DB method."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_pending_analytics_checkpoints.return_value = [
            {"chapter_id": 1, "youtube_video_id": "abc", "youtube_upload_date": None}
        ]

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_pending_analytics_checkpoints",
            output_xcom_key="pending_checkpoints",
        )
        ctx = make_context(ti=mock_task_instance)
        result = op.execute(ctx)

        mock_db.get_pending_analytics_checkpoints.assert_called_once()
        assert isinstance(result, list)

    def test_pushes_result_to_xcom(self, mock_db, mock_task_instance, make_context):
        """Result is pushed to XCom under the configured output_xcom_key."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        rows = [{"chapter_id": 2, "youtube_video_id": "xyz", "youtube_upload_date": None}]
        mock_db.get_pending_analytics_checkpoints.return_value = rows

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_pending_analytics_checkpoints",
            output_xcom_key="pending_checkpoints",
        )
        ctx = make_context(ti=mock_task_instance)
        op.execute(ctx)

        assert mock_task_instance.xcom_store.get("pending_checkpoints") == rows


class TestExecuteRecordAnalyticsSnapshots:
    """op='record_analytics_snapshots' routes to db.record_analytics_snapshot() per item."""

    def test_routes_to_db_method_for_each_collected_item(
        self, mock_db, mock_task_instance, make_context
    ):
        """Each item in 'collected' XCom triggers one record_analytics_snapshot call."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        collected = [
            {
                "chapter_id": 1,
                "youtube_video_id": "abc",
                "checkpoint": "24h",
                "metrics": {"views": 100, "estimatedMinutesWatched": 50.0,
                            "averageViewDuration": 30.0, "likes": 12},
            }
        ]
        mock_task_instance.xcom_store["collected"] = collected

        op = PostgreSQLOperator(
            task_id="t",
            operation="record_analytics_snapshots",
            xcom_keys={"collected": "collected"},
        )
        ctx = make_context(ti=mock_task_instance)
        op.execute(ctx)

        mock_db.record_analytics_snapshot.assert_called_once_with(
            chapter_id=1,
            youtube_video_id="abc",
            checkpoint="24h",
            metrics=collected[0]["metrics"],
        )

    def test_action_taken_never_written(
        self, mock_db, mock_task_instance, make_context
    ):
        """record_analytics_snapshot must NOT receive an action_taken argument."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["collected"] = [
            {
                "chapter_id": 5,
                "youtube_video_id": "zzz",
                "checkpoint": "7d",
                "metrics": {"views": 1, "estimatedMinutesWatched": 0.1,
                            "averageViewDuration": 0.0, "likes": 0},
            }
        ]

        op = PostgreSQLOperator(
            task_id="t",
            operation="record_analytics_snapshots",
            xcom_keys={"collected": "collected"},
        )
        ctx = make_context(ti=mock_task_instance)
        op.execute(ctx)

        call_kwargs = mock_db.record_analytics_snapshot.call_args.kwargs
        assert "action_taken" not in call_kwargs
