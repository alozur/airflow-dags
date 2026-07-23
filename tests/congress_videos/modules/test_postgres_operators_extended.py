"""Extended tests for PostgreSQLOperator — remaining operations coverage."""

from __future__ import annotations

import logging
import sys
import types
from unittest.mock import MagicMock

import pytest


# --------------------------------------------------------------------------- #
# Shim: apply_defaults was removed in Airflow 3
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
    monkeypatch.setenv("POSTGRES_HOST", "localhost")
    monkeypatch.setenv("POSTGRES_PORT", "5432")
    monkeypatch.setenv("POSTGRES_DB", "testdb")
    monkeypatch.setenv("POSTGRES_USER", "testuser")
    monkeypatch.setenv("POSTGRES_PASSWORD", "testpass")
    monkeypatch.setenv("POSTGRES_SCHEMA", "public")


@pytest.fixture
def mock_db(mocker):
    mock_instance = MagicMock()
    mocker.patch(
        "congress_videos.modules.postgres_operators.CongressionalVideoDB",
        return_value=mock_instance,
    )
    return mock_instance


@pytest.fixture
def make_context(mock_task_instance):
    def _make(params: dict | None = None, ti=None):
        return {
            "ti": ti or mock_task_instance,
            "params": params or {},
        }
    return _make


# --------------------------------------------------------------------------- #
# save_thumbnail_info
# --------------------------------------------------------------------------- #

class TestExecuteSaveThumbnailInfo:

    def test_updates_valid_thumbnail_info(self, mock_db, mock_task_instance, make_context):
        """update_thumbnail_info is called when text, path, and success are all present."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["thumbnail_text_results"] = {
            "results": [
                {"entry_id": "e1", "thumbnail_text": "Sesion Plenaria 135"}
            ]
        }
        mock_task_instance.xcom_store["thumbnail_results"] = {
            "results": [
                {"entry_id": "e1", "output_path": "/data/thumb.png", "success": True}
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="save_thumbnail_info")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_topics"] == 1
        mock_db.update_thumbnail_info.assert_called_once_with(
            "e1", "Sesion Plenaria 135", "/data/thumb.png"
        )

    def test_skips_when_thumbnail_failed(self, mock_db, mock_task_instance, make_context):
        """Entry with success=False is skipped."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["thumbnail_text_results"] = {
            "results": [{"entry_id": "e2", "thumbnail_text": "Some text"}]
        }
        mock_task_instance.xcom_store["thumbnail_results"] = {
            "results": [{"entry_id": "e2", "output_path": "/x.png", "success": False}]
        }

        op = PostgreSQLOperator(task_id="t", operation="save_thumbnail_info")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_topics"] == 0
        mock_db.update_thumbnail_info.assert_not_called()

    def test_handles_non_dict_thumbnail_results(self, mock_db, mock_task_instance, make_context):
        """Non-dict thumbnail results produce empty processing."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["thumbnail_text_results"] = None
        mock_task_instance.xcom_store["thumbnail_results"] = None

        op = PostgreSQLOperator(task_id="t", operation="save_thumbnail_info")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_topics"] == 0
        assert result["total_processed"] == 0


# --------------------------------------------------------------------------- #
# add_to_upload_queue
# --------------------------------------------------------------------------- #

class TestExecuteAddToUploadQueue:

    def test_adds_videos_to_queue(self, mock_db, mock_task_instance, make_context):
        """add_videos_to_upload_queue called with correct entry ids."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["top_videos"] = [
            {"entry_id": "v1"},
            {"entry_id": "v2"},
        ]

        op = PostgreSQLOperator(task_id="t", operation="add_to_upload_queue")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["queued_videos"] == 2
        assert result["entry_ids"] == ["v1", "v2"]
        mock_db.add_videos_to_upload_queue.assert_called_once_with(["v1", "v2"])

    def test_no_videos_to_queue(self, mock_db, mock_task_instance, make_context):
        """Empty or None top_videos returns 0 queued videos."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["top_videos"] = None

        op = PostgreSQLOperator(task_id="t", operation="add_to_upload_queue")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["queued_videos"] == 0
        mock_db.add_videos_to_upload_queue.assert_not_called()


# --------------------------------------------------------------------------- #
# get_from_upload_queue
# --------------------------------------------------------------------------- #

class TestExecuteGetFromUploadQueue:

    def test_returns_videos_from_queue(self, mock_db, mock_task_instance, make_context):
        """get_videos_from_upload_queue called with max_videos from params."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_videos_from_upload_queue.return_value = [
            {"entry_id": "v1", "title": "Test"}
        ]

        op = PostgreSQLOperator(task_id="t", operation="get_from_upload_queue")
        result = op.execute(make_context(params={"max_videos": 3}, ti=mock_task_instance))

        mock_db.get_videos_from_upload_queue.assert_called_once_with(3)
        assert len(result) == 1


# --------------------------------------------------------------------------- #
# update_queue_status
# --------------------------------------------------------------------------- #

class TestExecuteUpdateQueueStatus:

    def test_successful_download_updates_to_processing(
        self, mock_db, mock_task_instance, make_context
    ):
        """Successful download detail updates queue status to 'processing'."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["download_results"] = {
            "download_details": [
                {
                    "entry_id": "e1",
                    "success": True,
                    "file_path": "/data/video.mp4",
                    "file_size": 1024,
                    "duration": 3600,
                }
            ]
        }
        mock_task_instance.xcom_store["upload_results"] = None

        op = PostgreSQLOperator(task_id="t", operation="update_queue_status")
        result = op.execute(make_context(ti=mock_task_instance))

        mock_db.update_upload_queue_status.assert_any_call("e1", "processing")
        assert result["updated_items"] >= 1

    def test_failed_download_updates_to_failed(
        self, mock_db, mock_task_instance, make_context
    ):
        """Failed download detail updates queue status to 'failed'."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["download_results"] = {
            "download_details": [
                {
                    "entry_id": "e2",
                    "success": False,
                    "error": "Network timeout",
                }
            ]
        }
        mock_task_instance.xcom_store["upload_results"] = None

        op = PostgreSQLOperator(task_id="t", operation="update_queue_status")
        result = op.execute(make_context(ti=mock_task_instance))

        mock_db.update_upload_queue_status.assert_any_call("e2", "failed", "Network timeout")
        assert result["updated_items"] >= 1

    def test_successful_upload_marks_completed(
        self, mock_db, mock_task_instance, make_context
    ):
        """Successful upload result marks entry as completed."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["download_results"] = None
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "entry_id": "e3",
                    "video_id": "yt-abc123",
                    "success": True,
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="update_queue_status")
        result = op.execute(make_context(ti=mock_task_instance))

        mock_db.update_upload_queue_status.assert_any_call("e3", "completed")
        assert result["updated_items"] >= 1

    def test_failed_upload_marks_as_failed(
        self, mock_db, mock_task_instance, make_context
    ):
        """Failed upload result marks entry as failed."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["download_results"] = None
        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "entry_id": "e4",
                    "video_id": None,
                    "success": False,
                    "error": "quota exceeded",
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="update_queue_status")
        result = op.execute(make_context(ti=mock_task_instance))

        mock_db.update_upload_queue_status.assert_any_call("e4", "failed", "quota exceeded")


# --------------------------------------------------------------------------- #
# update_youtube_status
# --------------------------------------------------------------------------- #

class TestExecuteUpdateYoutubeStatus:

    def test_updates_youtube_status_for_successful_uploads(
        self, mock_db, mock_task_instance, make_context
    ):
        """update_youtube_upload_status called for each successful upload."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"entry_id": "e1", "video_id": "yt-xxx", "success": True},
                {"entry_id": "e2", "video_id": "yt-yyy", "success": True},
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="update_youtube_status")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_videos"] == 2
        assert result["total_uploads"] == 2

    def test_skips_failed_uploads(
        self, mock_db, mock_task_instance, make_context
    ):
        """Failed uploads are skipped; only successes are updated."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"entry_id": "e1", "video_id": "yt-ok", "success": True},
                {"entry_id": "e2", "video_id": None, "success": False},
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="update_youtube_status")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_videos"] == 1

    def test_invalid_upload_results_format_returns_error(
        self, mock_db, mock_task_instance, make_context
    ):
        """Non-dict/list upload results returns error."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = "invalid"

        op = PostgreSQLOperator(task_id="t", operation="update_youtube_status")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_videos"] == 0
        assert "error" in result

    def test_upload_results_as_list(
        self, mock_db, mock_task_instance, make_context
    ):
        """upload_results as direct list is handled correctly."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = [
            {"entry_id": "e1", "video_id": "yt-z", "success": True},
        ]

        op = PostgreSQLOperator(task_id="t", operation="update_youtube_status")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_videos"] == 1

    def test_skips_upload_missing_entry_or_video_id(
        self, mock_db, mock_task_instance, make_context
    ):
        """Success=True but missing entry_id or video_id is skipped."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {"entry_id": None, "video_id": "yt-ok", "success": True},
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="update_youtube_status")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_videos"] == 0
        mock_db.update_youtube_upload_status.assert_not_called()


# --------------------------------------------------------------------------- #
# save_youtube_chapters
# --------------------------------------------------------------------------- #

class TestExecuteSaveYoutubeChapters:

    def test_no_scored_chapters_returns_zero(
        self, mock_db, mock_task_instance, make_context
    ):
        """No scored_chapters returns early with zero saved."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["scored_chapters"] = None
        mock_task_instance.xcom_store["session_date"] = None

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_chapters")
        result = op.execute(
            make_context(params={"target_date": "2025-05-22"}, ti=mock_task_instance)
        )

        assert result["total_videos_saved"] == 0
        assert result["total_chapters_saved"] == 0

    def test_saves_chapters_with_session_data(
        self, mock_db, mock_task_instance, make_context
    ):
        """save_youtube_chapters_to_db called with correct args when session_date_data present."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.save_youtube_chapters_to_db.return_value = {
            "total_videos_saved": 1,
            "total_chapters_saved": 5,
            "videos": [],
        }

        mock_task_instance.xcom_store["scored_chapters"] = {"videos": [{"video_id": "v1"}]}
        mock_task_instance.xcom_store["session_date"] = {
            "total_processed": 1,
            "videos": [
                {
                    "video_id": "v1",
                    "session_number": 135,
                    "target_date": "2025-05-22",
                }
            ],
        }

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_chapters")
        result = op.execute(
            make_context(params={"target_date": "2025-05-22"}, ti=mock_task_instance)
        )

        assert result["total_chapters_saved"] == 5
        mock_db.save_youtube_chapters_to_db.assert_called_once()

    def test_uses_target_date_fallback_when_no_session_data(
        self, mock_db, mock_task_instance, make_context
    ):
        """target_date param used as fallback when session_date_data is missing."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.save_youtube_chapters_to_db.return_value = {
            "total_videos_saved": 1,
            "total_chapters_saved": 2,
            "videos": [],
        }

        mock_task_instance.xcom_store["scored_chapters"] = {"videos": []}
        mock_task_instance.xcom_store["session_date"] = None

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_chapters")
        result = op.execute(
            make_context(params={"target_date": "2025-05-22"}, ti=mock_task_instance)
        )

        call_kwargs = mock_db.save_youtube_chapters_to_db.call_args[1]
        from datetime import date
        assert call_kwargs["session_date"] == date(2025, 5, 22)

    def test_empty_session_videos_list(
        self, mock_db, mock_task_instance, make_context
    ):
        """session_date_data with empty videos list falls back to target_date."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.save_youtube_chapters_to_db.return_value = {
            "total_videos_saved": 0,
            "total_chapters_saved": 0,
            "videos": [],
        }

        mock_task_instance.xcom_store["scored_chapters"] = {"videos": []}
        mock_task_instance.xcom_store["session_date"] = {"total_processed": 0, "videos": []}

        op = PostgreSQLOperator(task_id="t", operation="save_youtube_chapters")
        result = op.execute(
            make_context(params={"target_date": "2025-05-22"}, ti=mock_task_instance)
        )

        mock_db.save_youtube_chapters_to_db.assert_called_once()


# --------------------------------------------------------------------------- #
# get_uploadable_chapters
# --------------------------------------------------------------------------- #

class TestExecuteGetUploadableChapters:

    def test_returns_chapters_from_db(self, mock_db, mock_task_instance, make_context):
        """get_uploadable_chapters called with limit and min_relevance_score from params."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.get_uploadable_chapters.return_value = [
            {"chapter_id": "c1", "title": "Debate"},
        ]

        op = PostgreSQLOperator(task_id="t", operation="get_uploadable_chapters")
        result = op.execute(
            make_context(
                params={"max_chapters": 3, "min_relevance_score": 2},
                ti=mock_task_instance,
            )
        )

        mock_db.get_uploadable_chapters.assert_called_once_with(limit=3, min_relevance_score=2)
        assert len(result) == 1


# --------------------------------------------------------------------------- #
# mark_chapters_uploaded
# --------------------------------------------------------------------------- #

class TestExecuteMarkChaptersUploaded:

    def test_no_upload_results_returns_zero(
        self, mock_db, mock_task_instance, make_context
    ):
        """Missing upload_results returns zero updated chapters."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = None

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_chapters"] == 0

    def test_marks_successful_chapter_uploaded(
        self, mock_db, mock_task_instance, make_context
    ):
        """Successful upload with chapter_id and youtube_video_id marks chapter uploaded."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": "ch-01",
                    "youtube_video_id": "yt-xyz",
                    "success": True,
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_chapters"] == 1
        mock_db.mark_chapter_uploaded.assert_called_once_with("ch-01", "yt-xyz")

    def test_failed_upload_records_failure(self, mock_db, mock_task_instance, make_context):
        """Failed upload with a resolvable chapter_id records the failure via the DB."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": "ch-02",
                    "youtube_video_id": None,
                    "success": False,
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_chapters"] == 0
        mock_db.record_chapter_upload_failure.assert_called_once_with("ch-02", None)
        mock_db.mark_chapter_uploaded.assert_not_called()

    def test_failed_upload_with_error_records_error_text(
        self, mock_db, mock_task_instance, make_context
    ):
        """Failed upload carrying an 'error' string forwards it to record_chapter_upload_failure."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": "ch-03",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "quota exceeded",
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        op.execute(make_context(ti=mock_task_instance))

        mock_db.record_chapter_upload_failure.assert_called_once_with("ch-03", "quota exceeded")

    def test_failed_upload_no_chapter_id_is_defensively_skipped(
        self, mock_db, mock_task_instance, make_context
    ):
        """Failed upload with no resolvable chapter_id never calls the DB and never crashes."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": None,
                    "youtube_video_id": None,
                    "success": False,
                    "error": "unknown",
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        result = op.execute(make_context(ti=mock_task_instance))

        mock_db.record_chapter_upload_failure.assert_not_called()
        assert any(
            d["status"] == "skipped" and d.get("reason") == "upload_failed_no_chapter_id"
            for d in result["details"]
        )

    def test_two_failed_uploads_recorded_independently(
        self, mock_db, mock_task_instance, make_context
    ):
        """Two distinct failed chapters in one batch each record their own failure."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": "ch-10",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "err-a",
                },
                {
                    "chapter_id": "ch-11",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "err-b",
                },
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        op.execute(make_context(ti=mock_task_instance))

        mock_db.record_chapter_upload_failure.assert_any_call("ch-10", "err-a")
        mock_db.record_chapter_upload_failure.assert_any_call("ch-11", "err-b")
        assert mock_db.record_chapter_upload_failure.call_count == 2

    def test_db_write_failure_while_recording_is_distinguishably_logged(
        self, mock_db, mock_task_instance, make_context, caplog
    ):
        """If record_chapter_upload_failure itself raises, log a distinguishable ERROR
        (recording the failure was lost) and keep the loop going without raising."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db.record_chapter_upload_failure.side_effect = Exception("transient DB error")

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": "ch-20",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "upload failed",
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")

        with caplog.at_level(logging.ERROR):
            result = op.execute(make_context(ti=mock_task_instance))

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "ch-20" in errors[0].message
        assert "record" in errors[0].message.lower()
        assert any(d["status"] == "failed" for d in result["details"])

    def test_successful_upload_missing_fields_recorded_as_skipped(
        self, mock_db, mock_task_instance, make_context
    ):
        """Success=True but missing chapter_id or youtube_video_id is recorded as skipped."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_task_instance.xcom_store["upload_results"] = {
            "upload_details": [
                {
                    "chapter_id": None,
                    "youtube_video_id": "yt-zzz",
                    "success": True,
                }
            ]
        }

        op = PostgreSQLOperator(task_id="t", operation="mark_chapters_uploaded")
        result = op.execute(make_context(ti=mock_task_instance))

        assert result["updated_chapters"] == 0
        assert any(d["status"] == "skipped" for d in result["details"])
