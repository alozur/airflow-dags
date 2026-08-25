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

        op = PostgreSQLOperator(task_id="t", operation="check_upload_quota")
        assert op.operation == "check_upload_quota"

    def test_xcom_keys_default_to_empty_dict(self, mock_db):
        """xcom_keys defaults to {} when not provided."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="get_pending_analytics_checkpoints")
        assert op.xcom_keys == {}

    def test_output_xcom_key_default_none(self, mock_db):
        """output_xcom_key defaults to None when not provided."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation="get_pending_analytics_checkpoints")
        assert op.output_xcom_key is None

    def test_custom_xcom_keys(self, mock_db):
        """Custom xcom_keys are preserved."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        keys = {"collected": "my_key"}
        op = PostgreSQLOperator(task_id="t", operation="record_analytics_snapshots", xcom_keys=keys)
        assert op.xcom_keys == keys

    def test_output_xcom_key_set(self, mock_db):
        """Custom output_xcom_key is stored."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_pending_analytics_checkpoints",
            output_xcom_key="result_key",
        )
        assert op.output_xcom_key == "result_key"


# --------------------------------------------------------------------------- #
# execute — removed dead-branch operations raise ValueError (post #205 cleanup)
# --------------------------------------------------------------------------- #

class TestExecuteRemovedOperations:

    @pytest.mark.parametrize(
        "operation",
        [
            "update_downloads",
            "save_youtube_metadata",
            "save_thumbnail_info",
            "get_top_videos_for_upload",
            "add_to_upload_queue",
            "get_from_upload_queue",
            "update_queue_status",
            "update_youtube_status",
            "get_uploadable_chapters",
            "get_uploadable_turns",
            "get_chapters_for_shorts",
            "mark_shorts_uploaded",
            "record_verification_results",
        ],
    )
    def test_removed_operation_raises_value_error(
        self, mock_db, mock_task_instance, make_context, operation
    ):
        """Each of the 13 dead-branch operation names now hits the unknown-operation
        else-arm and raises ValueError (branches removed in the #205 cleanup)."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        op = PostgreSQLOperator(task_id="t", operation=operation)
        ctx = make_context(ti=mock_task_instance)

        with pytest.raises(ValueError, match="Unknown operation"):
            op.execute(ctx)


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

        mock_db.get_pending_analytics_checkpoints.return_value = [{"chapter_id": 1}]

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_pending_analytics_checkpoints",
            output_xcom_key="pending_checkpoints",
        )
        ctx = make_context(ti=mock_task_instance)
        op.execute(ctx)

        assert mock_task_instance.xcom_store.get("pending_checkpoints") is not None

    def test_result_not_pushed_when_key_is_none(
        self, mock_db, mock_task_instance, make_context
    ):
        """No xcom_push when output_xcom_key is None."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_pending_analytics_checkpoints.return_value = []

        op = PostgreSQLOperator(
            task_id="t",
            operation="get_pending_analytics_checkpoints",
        )
        ctx = make_context(ti=mock_task_instance)
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
        """check_upload_quota result contains queue_size, uploads_today, and turns_pending
        (REQ-QUOTA-01; combined cap: uploads_today = chapters + turns; queue_size = chapters + turns).
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
        assert "turns_pending" in result
        assert {"uploads_today", "queue_size", "turns_pending"}.issubset(result.keys())


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
