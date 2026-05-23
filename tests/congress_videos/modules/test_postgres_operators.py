"""Tests for PostgreSQLOperator custom Airflow operator — TASK-022."""

from __future__ import annotations

import sys
import types
from unittest.mock import MagicMock, patch

import pytest


# --------------------------------------------------------------------------- #
# Shim: apply_defaults was removed in Airflow 3 — provide a no-op before import
# --------------------------------------------------------------------------- #

def _noop_apply_defaults(func):
    return func


if "airflow.utils.decorators" not in sys.modules:
    _mod = types.ModuleType("airflow.utils.decorators")
    _mod.apply_defaults = _noop_apply_defaults
    sys.modules["airflow.utils.decorators"] = _mod
else:
    sys.modules["airflow.utils.decorators"].apply_defaults = _noop_apply_defaults


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
    """Patch CongressionalVideoDB and return the mock instance."""
    mock_instance = MagicMock()
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
