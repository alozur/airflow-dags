"""Tests for post_upload_verification module and CongressionalVideoDB methods.

Covers:
- Phase 2: check_video_status() state machine (pure module)
- Phase 3: select_unverified_uploads, mark_upload_verified,
           record_upload_verification_failure DB methods
"""

from __future__ import annotations

from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_youtube_service_stub(
    *,
    processing_status: str | None = None,
    privacy_status: str | None = None,
    items: list | None = None,
):
    """Build a minimal youtube_service stub for videos().list().execute()."""
    if items is None:
        item: dict = {}
        if processing_status is not None:
            item.setdefault("processingDetails", {})["processingStatus"] = processing_status
        if privacy_status is not None:
            item.setdefault("status", {})["privacyStatus"] = privacy_status
        items = [item] if (processing_status or privacy_status) else []

    service = MagicMock()
    service.videos.return_value.list.return_value.execute.return_value = {"items": items}
    return service


# ---------------------------------------------------------------------------
# Phase 2: check_video_status — state machine
# ---------------------------------------------------------------------------


class TestCheckVideoStatusOembedOk:
    """oembed 200 → ok without calling the Data API."""

    def test_oembed_200_returns_ok(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(200)
        from congress_videos.modules.post_upload_verification import check_video_status

        status, detail = check_video_status("abc123", http_get=mock_requests.get, youtube_service=None)
        assert status == "ok"

    def test_oembed_200_does_not_call_api(self, mock_requests):
        """When oembed returns 200, the Data API must NOT be called."""
        mock_requests.get.return_value = mock_requests.make_response(200)
        service = _make_youtube_service_stub(processing_status="succeeded")

        from congress_videos.modules.post_upload_verification import check_video_status

        check_video_status("vid1", http_get=mock_requests.get, youtube_service=service)
        service.videos.assert_not_called()


class TestCheckVideoStatusApiOk:
    """oembed non-200 + Data API says ok (succeeded, private, unlisted)."""

    def test_processing_succeeded_returns_ok(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(404)
        service = _make_youtube_service_stub(processing_status="succeeded")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v1", http_get=mock_requests.get, youtube_service=service)
        assert status == "ok"

    def test_privacy_private_returns_ok(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(401)
        service = _make_youtube_service_stub(privacy_status="private")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v2", http_get=mock_requests.get, youtube_service=service)
        assert status == "ok"

    def test_privacy_unlisted_returns_ok(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(401)
        service = _make_youtube_service_stub(privacy_status="unlisted")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v3", http_get=mock_requests.get, youtube_service=service)
        assert status == "ok"


class TestCheckVideoStatusProcessing:
    """oembed non-200 + API says processing → processing."""

    def test_processing_status_returns_processing(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(403)
        service = _make_youtube_service_stub(processing_status="processing")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v4", http_get=mock_requests.get, youtube_service=service)
        assert status == "processing"


class TestCheckVideoStatusAbandoned:
    """oembed non-200 + API says missing/failed → abandoned."""

    def test_empty_items_returns_abandoned(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(404)
        service = _make_youtube_service_stub(items=[])

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v5", http_get=mock_requests.get, youtube_service=service)
        assert status == "abandoned"

    def test_processing_failed_returns_abandoned(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(403)
        service = _make_youtube_service_stub(processing_status="failed")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v6", http_get=mock_requests.get, youtube_service=service)
        assert status == "abandoned"


class TestCheckVideoStatusQuotaError:
    """API quota or HTTP error → unknown (never abandoned)."""

    def test_api_exception_returns_unknown(self, mock_requests):
        mock_requests.get.return_value = mock_requests.make_response(403)
        service = MagicMock()
        service.videos.return_value.list.return_value.execute.side_effect = Exception("quota exceeded")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v7", http_get=mock_requests.get, youtube_service=service)
        assert status == "unknown"

    def test_api_exception_is_not_abandoned(self, mock_requests):
        """Quota error must never produce 'abandoned' status."""
        mock_requests.get.return_value = mock_requests.make_response(403)
        service = MagicMock()
        service.videos.return_value.list.return_value.execute.side_effect = Exception("quota")

        from congress_videos.modules.post_upload_verification import check_video_status

        status, _ = check_video_status("v8", http_get=mock_requests.get, youtube_service=service)
        assert status != "abandoned"


class TestCheckVideoStatusModuleConstants:
    """Module-level constants must be defined with correct types/values."""

    def test_module_constants_exist(self):
        from congress_videos.modules import post_upload_verification as m

        assert hasattr(m, "VERIFY_WINDOW_MIN_HOURS")
        assert hasattr(m, "VERIFY_WINDOW_MAX_HOURS")
        assert hasattr(m, "MAX_API_CALLS_PER_RUN")
        assert hasattr(m, "OEMBED_URL")
        assert hasattr(m, "HTTP_TIMEOUT_SECONDS")

    def test_window_min_is_1_hour(self):
        from congress_videos.modules.post_upload_verification import VERIFY_WINDOW_MIN_HOURS

        assert VERIFY_WINDOW_MIN_HOURS == 1

    def test_window_max_is_48_hours(self):
        from congress_videos.modules.post_upload_verification import VERIFY_WINDOW_MAX_HOURS

        assert VERIFY_WINDOW_MAX_HOURS == 48

    def test_max_api_calls_is_positive_int(self):
        from congress_videos.modules.post_upload_verification import MAX_API_CALLS_PER_RUN

        assert isinstance(MAX_API_CALLS_PER_RUN, int)
        assert MAX_API_CALLS_PER_RUN > 0


# ---------------------------------------------------------------------------
# Phase 3: DB methods — select_unverified_uploads
# ---------------------------------------------------------------------------


class TestSelectUnverifiedUploads:
    """select_unverified_uploads returns rows in the 1h–48h window."""

    def test_chapter_in_window_returned(self, mock_psycopg2_connection):
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchall.return_value = [{"chapter_id": 1, "youtube_video_id": "abc", "item_type": "chapter"}]

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        result = db.select_unverified_uploads()

        assert len(result) == 1
        assert mock_cursor.execute.called

    def test_sql_uses_upload_verified_at_is_null(self, mock_psycopg2_connection):
        """Query must filter on upload_verified_at IS NULL."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchall.return_value = []

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.select_unverified_uploads()

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper() if call_args else ""
        assert "UPLOAD_VERIFIED_AT IS NULL" in sql

    def test_sql_uses_is_uploaded_true_gate(self, mock_psycopg2_connection):
        """Query must filter on is_uploaded_to_youtube = TRUE."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchall.return_value = []

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.select_unverified_uploads()

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper() if call_args else ""
        assert "IS_UPLOADED_TO_YOUTUBE = TRUE" in sql

    def test_sql_uses_distinct_on_output_path_for_turns(self, mock_psycopg2_connection):
        """Turns sub-query must use DISTINCT ON (output_path)."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchall.return_value = []

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.select_unverified_uploads()

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper() if call_args else ""
        assert "DISTINCT ON" in sql and "OUTPUT_PATH" in sql

    def test_returns_empty_when_no_rows(self, mock_psycopg2_connection):
        """Returns empty list when no unverified uploads exist."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchall.return_value = []

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        result = db.select_unverified_uploads()

        assert result == []


# ---------------------------------------------------------------------------
# Phase 3: DB methods — mark_upload_verified
# ---------------------------------------------------------------------------


class TestMarkUploadVerified:
    """mark_upload_verified sets upload_verified_at=NOW()."""

    def test_chapter_sets_upload_verified_at(self, mock_psycopg2_connection):
        _, _, mock_cursor = mock_psycopg2_connection

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.mark_upload_verified("chapter", 42)

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper()
        assert "UPLOAD_VERIFIED_AT" in sql
        assert "NOW()" in sql or "CURRENT_TIMESTAMP" in sql

    def test_chapter_update_targets_chapter_id(self, mock_psycopg2_connection):
        _, _, mock_cursor = mock_psycopg2_connection

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.mark_upload_verified("chapter", 42)

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert 42 in params

    def test_turn_update_uses_output_path(self, mock_psycopg2_connection):
        """Turns branch must update all rows sharing the same output_path."""
        _, _, mock_cursor = mock_psycopg2_connection

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.mark_upload_verified("turn", "/path/to/turn/video.mp4")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper()
        # Must update by output_path, not turn_id
        assert "OUTPUT_PATH" in sql
        params = call_args[0][1]
        assert "/path/to/turn/video.mp4" in params


# ---------------------------------------------------------------------------
# Phase 3: DB methods — record_upload_verification_failure
# ---------------------------------------------------------------------------


class TestRecordUploadVerificationFailure:
    """record_upload_verification_failure tracks abandonment state."""

    def test_chapter_below_cap_resets_uploaded_flag(self, mock_psycopg2_connection):
        """First failure: sets is_uploaded_to_youtube=FALSE and increments upload_attempts."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchone.return_value = {"upload_attempts": 1, "is_upload_abandoned": False}

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.record_upload_verification_failure("chapter", 10, "video gone 404")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper()
        assert "IS_UPLOADED_TO_YOUTUBE" in sql
        assert "UPLOAD_ATTEMPTS" in sql

    def test_chapter_at_cap_sets_abandoned(self, mock_psycopg2_connection):
        """When upload_attempts reaches threshold → is_upload_abandoned=TRUE."""
        _, _, mock_cursor = mock_psycopg2_connection
        from congress_videos.modules.database import CHAPTER_UPLOAD_ABANDON_THRESHOLD

        mock_cursor.fetchone.return_value = {
            "upload_attempts": CHAPTER_UPLOAD_ABANDON_THRESHOLD,
            "is_upload_abandoned": True,
        }

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.record_upload_verification_failure("chapter", 10, "still gone")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper()
        assert "IS_UPLOAD_ABANDONED" in sql

    def test_turn_failure_uses_output_path(self, mock_psycopg2_connection):
        """Turns branch updates all rows sharing the same output_path."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchone.return_value = {"upload_attempts": 1, "is_upload_abandoned": False}

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.record_upload_verification_failure("turn", "/group/path.mp4", "404 error")

        call_args = mock_cursor.execute.call_args
        sql = call_args[0][0].upper()
        assert "OUTPUT_PATH" in sql
        params = call_args[0][1]
        assert "/group/path.mp4" in params

    def test_error_message_is_stored(self, mock_psycopg2_connection):
        """last_upload_error must be passed as a parameter."""
        _, _, mock_cursor = mock_psycopg2_connection
        mock_cursor.fetchone.return_value = {"upload_attempts": 1, "is_upload_abandoned": False}

        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        db.record_upload_verification_failure("chapter", 5, "my-error-message")

        call_args = mock_cursor.execute.call_args
        params = call_args[0][1]
        assert "my-error-message" in params
