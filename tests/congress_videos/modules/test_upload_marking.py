"""Tests for the pure `upload_marking` branch logic — issue #227.

Ports the `mark_chapters_uploaded` / `mark_turns_uploaded` scenarios from the
deleted `test_postgres_operators_extended.py` / `test_postgres_operators_turns.py`
onto the new pure functions `mark_chapter_uploads(db, upload_results)` and
`mark_turn_uploads(db, upload_results)`.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from congress_videos.modules.upload_marking import (
    mark_chapter_uploads,
    mark_turn_uploads,
)


@pytest.fixture
def mock_db():
    return MagicMock()


# --------------------------------------------------------------------------- #
# mark_chapter_uploads
# --------------------------------------------------------------------------- #


class TestMarkChapterUploads:
    def test_no_upload_results_returns_zero(self, mock_db):
        """Missing upload_results returns zero updated chapters."""
        result = mark_chapter_uploads(mock_db, None)

        assert result["updated_chapters"] == 0
        mock_db.mark_chapter_uploaded.assert_not_called()

    def test_empty_upload_details_returns_zero(self, mock_db):
        """upload_results with no upload_details returns zero updated chapters."""
        result = mark_chapter_uploads(mock_db, {"upload_details": []})

        assert result["updated_chapters"] == 0

    def test_marks_successful_chapter_uploaded(self, mock_db):
        """Successful upload with chapter_id and youtube_video_id marks chapter uploaded."""
        upload_results = {"upload_details": [{"chapter_id": "ch-01", "youtube_video_id": "yt-xyz", "success": True}]}

        result = mark_chapter_uploads(mock_db, upload_results)

        assert result["updated_chapters"] == 1
        mock_db.mark_chapter_uploaded.assert_called_once_with("ch-01", "yt-xyz")

    def test_failed_upload_records_failure(self, mock_db):
        """Failed upload with a resolvable chapter_id records the failure via the DB."""
        upload_results = {"upload_details": [{"chapter_id": "ch-02", "youtube_video_id": None, "success": False}]}

        result = mark_chapter_uploads(mock_db, upload_results)

        assert result["updated_chapters"] == 0
        mock_db.record_chapter_upload_failure.assert_called_once_with("ch-02", None)
        mock_db.mark_chapter_uploaded.assert_not_called()

    def test_failed_upload_with_error_records_error_text(self, mock_db):
        """Failed upload carrying an 'error' string forwards it to record_chapter_upload_failure."""
        upload_results = {
            "upload_details": [
                {
                    "chapter_id": "ch-03",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "quota exceeded",
                }
            ]
        }

        mark_chapter_uploads(mock_db, upload_results)

        mock_db.record_chapter_upload_failure.assert_called_once_with("ch-03", "quota exceeded")

    def test_failed_upload_no_chapter_id_is_defensively_skipped(self, mock_db):
        """Failed upload with no resolvable chapter_id never calls the DB and never crashes."""
        upload_results = {
            "upload_details": [{"chapter_id": None, "youtube_video_id": None, "success": False, "error": "unknown"}]
        }

        result = mark_chapter_uploads(mock_db, upload_results)

        mock_db.record_chapter_upload_failure.assert_not_called()
        assert any(
            d["status"] == "skipped" and d.get("reason") == "upload_failed_no_chapter_id" for d in result["details"]
        )

    def test_two_failed_uploads_recorded_independently(self, mock_db):
        """Two distinct failed chapters in one batch each record their own failure."""
        upload_results = {
            "upload_details": [
                {"chapter_id": "ch-10", "youtube_video_id": None, "success": False, "error": "err-a"},
                {"chapter_id": "ch-11", "youtube_video_id": None, "success": False, "error": "err-b"},
            ]
        }

        mark_chapter_uploads(mock_db, upload_results)

        mock_db.record_chapter_upload_failure.assert_any_call("ch-10", "err-a")
        mock_db.record_chapter_upload_failure.assert_any_call("ch-11", "err-b")
        assert mock_db.record_chapter_upload_failure.call_count == 2

    def test_db_write_failure_while_recording_is_distinguishably_logged(self, mock_db, caplog):
        """If record_chapter_upload_failure itself raises, log a distinguishable ERROR
        (recording the failure was lost) and keep the loop going without raising."""
        mock_db.record_chapter_upload_failure.side_effect = Exception("transient DB error")
        upload_results = {
            "upload_details": [
                {"chapter_id": "ch-20", "youtube_video_id": None, "success": False, "error": "upload failed"}
            ]
        }

        with caplog.at_level(logging.ERROR):
            result = mark_chapter_uploads(mock_db, upload_results)

        errors = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(errors) == 1
        assert "ch-20" in errors[0].message
        assert "record" in errors[0].message.lower()
        assert any(d["status"] == "failed" for d in result["details"])

    def test_successful_upload_missing_fields_recorded_as_skipped(self, mock_db):
        """Success=True but missing chapter_id or youtube_video_id is recorded as skipped."""
        upload_results = {"upload_details": [{"chapter_id": None, "youtube_video_id": "yt-zzz", "success": True}]}

        result = mark_chapter_uploads(mock_db, upload_results)

        assert result["updated_chapters"] == 0
        assert any(d["status"] == "skipped" for d in result["details"])


# --------------------------------------------------------------------------- #
# mark_turn_uploads
# --------------------------------------------------------------------------- #


class TestMarkTurnUploads:
    def test_calls_mark_turns_uploaded_per_entry(self, mock_db):
        upload_results = {
            "upload_details": [
                {"turn_id": 1, "youtube_video_id": "abc", "success": True},
                {"turn_id": 2, "youtube_video_id": "xyz", "success": True},
            ]
        }

        mark_turn_uploads(mock_db, upload_results)

        assert mock_db.mark_turns_uploaded.call_count == 2
        mock_db.mark_turns_uploaded.assert_any_call(turn_id=1, youtube_video_id="abc")
        mock_db.mark_turns_uploaded.assert_any_call(turn_id=2, youtube_video_id="xyz")

    def test_skips_failed_uploads(self, mock_db):
        upload_results = {"upload_details": [{"turn_id": 1, "youtube_video_id": None, "success": False}]}

        mark_turn_uploads(mock_db, upload_results)

        mock_db.mark_turns_uploaded.assert_not_called()

    def test_handles_empty_upload_results(self, mock_db):
        result = mark_turn_uploads(mock_db, None)

        mock_db.mark_turns_uploaded.assert_not_called()
        assert result is not None
        assert result["updated_turns"] == 0

    def test_marked_turn_logs_info(self, mock_db, caplog):
        """A successfully marked turn emits a logger.info, not a bare print()."""
        upload_results = {"upload_details": [{"turn_id": 7, "youtube_video_id": "yt-info", "success": True}]}

        with caplog.at_level(logging.INFO):
            mark_turn_uploads(mock_db, upload_results)

        infos = [r for r in caplog.records if r.levelno == logging.INFO]
        assert any("7" in r.message and "yt-info" in r.message for r in infos)

    def test_fallback_fires_when_turn_id_missing_and_video_file_present(self, mock_db):
        mock_db.mark_turns_uploaded_by_output_path.return_value = 2
        upload_results = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                }
            ]
        }

        mark_turn_uploads(mock_db, upload_results)

        mock_db.mark_turns_uploaded.assert_not_called()
        mock_db.mark_turns_uploaded_by_output_path.assert_called_once_with("/path/turn1.mp4", "abc")

    def test_turn_id_wins_when_both_present(self, mock_db):
        upload_results = {
            "upload_details": [
                {
                    "turn_id": 5,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                }
            ]
        }

        mark_turn_uploads(mock_db, upload_results)

        mock_db.mark_turns_uploaded.assert_called_once_with(turn_id=5, youtube_video_id="abc")
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()

    def test_neither_key_is_skipped_without_db_call(self, mock_db):
        upload_results = {
            "upload_details": [{"turn_id": None, "youtube_video_id": "abc", "video_file": None, "success": True}]
        }

        result = mark_turn_uploads(mock_db, upload_results)

        mock_db.mark_turns_uploaded.assert_not_called()
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()
        assert result["details"][0]["status"] == "skipped"

    def test_failed_upload_does_not_trigger_fallback(self, mock_db):
        upload_results = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": False,
                }
            ]
        }

        mark_turn_uploads(mock_db, upload_results)

        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()

    def test_details_carry_matched_by_and_output_path_on_fallback(self, mock_db):
        mock_db.mark_turns_uploaded_by_output_path.return_value = 2
        upload_results = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                }
            ]
        }

        result = mark_turn_uploads(mock_db, upload_results)

        detail = result["details"][0]
        assert detail["matched_by"] == "output_path"
        assert detail["output_path"] == "/path/turn1.mp4"
        assert detail["status"] == "updated"

    def test_details_carry_matched_by_turn_id_on_primary_path(self, mock_db):
        upload_results = {"upload_details": [{"turn_id": 1, "youtube_video_id": "abc", "success": True}]}

        result = mark_turn_uploads(mock_db, upload_results)

        assert result["details"][0]["matched_by"] == "turn_id"

    def test_fallback_zero_rows_is_skipped_not_updated(self, mock_db):
        mock_db.mark_turns_uploaded_by_output_path.return_value = 0
        upload_results = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/gone.mp4",
                    "success": True,
                }
            ]
        }

        result = mark_turn_uploads(mock_db, upload_results)

        assert result["updated_turns"] == 0
        detail = result["details"][0]
        assert detail["status"] == "skipped"
        assert detail["reason"] == "output_path_not_found"

    def test_fallback_db_exception_increments_failed_updates(self, mock_db):
        mock_db.mark_turns_uploaded_by_output_path.side_effect = Exception("db down")
        upload_results = {
            "upload_details": [
                {
                    "turn_id": None,
                    "youtube_video_id": "abc",
                    "video_file": "/path/turn1.mp4",
                    "success": True,
                }
            ]
        }

        result = mark_turn_uploads(mock_db, upload_results)

        assert result["failed_updates"] == 1
        assert result["updated_turns"] == 0

    def test_existing_skip_reason_string_unchanged_for_no_usable_key(self, mock_db):
        """The pre-existing skip reason string must stay 'upload_failed_or_missing_fields'."""
        upload_results = {"upload_details": [{"turn_id": None, "youtube_video_id": None, "success": False}]}

        result = mark_turn_uploads(mock_db, upload_results)

        assert result["details"][0]["reason"] == "upload_failed_or_missing_fields"


# --------------------------------------------------------------------------- #
# mark_turn_uploads — thumbnail republish marker branch (issue #331)
# --------------------------------------------------------------------------- #


class TestMarkTurnUploadsThumbnailRepublishMarker:
    """Independent `if`, not `elif` — fires on both dispatch paths. Identity
    check: only literal False (of the FOUR-valued thumbnail_success) marks."""

    @pytest.mark.parametrize(
        "extra",
        [{"thumbnail_success": True}, {"thumbnail_success": None}, {}],
        ids=["true", "none", "absent"],
    )
    def test_non_false_thumbnail_success_never_marks(self, mock_db, extra):
        detail = {"turn_id": 1, "youtube_video_id": "abc", "success": True, **extra}

        mark_turn_uploads(mock_db, {"upload_details": [detail]})

        mock_db.mark_turn_thumbnail_republish_needed.assert_not_called()

    def test_marks_on_turn_id_branch_and_forwards_error(self, mock_db):
        detail = {
            "turn_id": 1,
            "youtube_video_id": "abc",
            "success": True,
            "thumbnail_success": False,
            "thumbnail_error": "quota exceeded",
        }

        result = mark_turn_uploads(mock_db, {"upload_details": [detail]})

        mock_db.mark_turn_thumbnail_republish_needed.assert_called_once_with(
            output_path=None, turn_id=1, error_message="quota exceeded"
        )
        assert result["thumbnail_markers"] == 1
        assert result["failed_updates"] == 0
        assert result["updated_turns"] == 1

    def test_marks_on_output_path_fallback_branch(self, mock_db):
        mock_db.mark_turns_uploaded_by_output_path.return_value = 2
        detail = {
            "turn_id": None,
            "youtube_video_id": "abc",
            "video_file": "/path/turn1.mp4",
            "success": True,
            "thumbnail_success": False,
        }

        result = mark_turn_uploads(mock_db, {"upload_details": [detail]})

        mock_db.mark_turn_thumbnail_republish_needed.assert_called_once_with(
            output_path="/path/turn1.mp4", turn_id=None, error_message=None
        )
        assert result["thumbnail_markers"] == 1

    def test_upload_itself_failed_never_marks_even_if_thumbnail_false(self, mock_db):
        detail = {
            "turn_id": 1,
            "youtube_video_id": None,
            "success": False,
            "thumbnail_success": False,
        }

        mark_turn_uploads(mock_db, {"upload_details": [detail]})

        mock_db.mark_turn_thumbnail_republish_needed.assert_not_called()

    def test_marker_write_failure_never_escapes_and_leaves_failed_updates_untouched(self, mock_db):
        mock_db.mark_turn_thumbnail_republish_needed.side_effect = Exception("db down")
        detail = {
            "turn_id": 1,
            "youtube_video_id": "abc",
            "success": True,
            "thumbnail_success": False,
        }

        result = mark_turn_uploads(mock_db, {"upload_details": [detail]})  # must not raise

        assert result["thumbnail_marker_failures"] == 1
        assert result["failed_updates"] == 0
        assert any(d.get("status") == "thumbnail_marker_failed" for d in result["details"])
