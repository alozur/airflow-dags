"""Tests for the thumbnail republish healer's pure state machine (issue #331, WU2b).

Covers:
- thumbnail_path_for: replays the existing publish-time path derivation
  (congress_videos/modules/youtube/youtube_upload.py:57,72) so the healer
  looks for the sidecar in exactly the same place the uploader wrote it.
- classify_republish_result: maps a set_thumbnail_for_video-shaped result
  dict to one of "healed" | "retry" | "abandon".
- attempt_thumbnail_republish: dependency-injected wrapper (same pattern as
  post_upload_verification.check_video_status's injected http_get) that
  invokes the injected callable, catches its exceptions, and delegates the
  rest to classify_republish_result -- so this module never imports
  googleapiclient and stays trivially unit-testable.

This module must be Airflow-free (DD5): no `airflow` import, no import-time
DB connection, token resolution, or filesystem probe.
"""

from __future__ import annotations


class TestThumbnailPathFor:
    """thumbnail_path_for must replay the exact rule already executed at
    upload time, not invent a new one."""

    def test_joins_dirname_with_thumbnail_filename(self):
        from congress_videos.modules.thumbnail_republish import thumbnail_path_for

        result = thumbnail_path_for("/data/turns/2026/09/01/turn_42/video.mp4")

        assert result == "/data/turns/2026/09/01/turn_42/thumbnail.png"

    def test_matches_youtube_upload_derivation(self):
        """Must equal os.path.join(os.path.dirname(output_path), "thumbnail.png"),
        the exact rule at youtube_upload.py:57,72."""
        import os

        from congress_videos.modules.thumbnail_republish import thumbnail_path_for

        output_path = "/videos/output.mp4"
        expected = os.path.join(os.path.dirname(output_path), "thumbnail.png")

        assert thumbnail_path_for(output_path) == expected


class TestClassifyRepublishResult:
    """Pure classification of an already-obtained set_thumbnail_for_video result."""

    def test_success_true_is_healed(self):
        from congress_videos.modules.thumbnail_republish import (
            classify_republish_result,
        )

        status, _detail = classify_republish_result({"success": True})

        assert status == "healed"

    def test_missing_file_error_is_abandon(self):
        """Spec: missing thumbnail.png is abandon-and-record, never regenerated."""
        from congress_videos.modules.thumbnail_republish import (
            MISSING_FILE_ERROR_PREFIX,
            classify_republish_result,
        )

        error = f"{MISSING_FILE_ERROR_PREFIX} /data/turn/thumbnail.png"
        status, detail = classify_republish_result({"success": False, "error": error})

        assert status == "abandon"
        assert detail == error

    def test_other_error_is_retry(self):
        from congress_videos.modules.thumbnail_republish import (
            classify_republish_result,
        )

        status, detail = classify_republish_result({"success": False, "error": "quota exceeded"})

        assert status == "retry"
        assert detail == "quota exceeded"

    def test_none_result_is_retry(self):
        from congress_videos.modules.thumbnail_republish import (
            classify_republish_result,
        )

        status, _detail = classify_republish_result(None)

        assert status == "retry"

    def test_empty_dict_result_is_retry(self):
        from congress_videos.modules.thumbnail_republish import (
            classify_republish_result,
        )

        status, _detail = classify_republish_result({})

        assert status == "retry"

    def test_success_false_without_error_text_is_retry(self):
        from congress_videos.modules.thumbnail_republish import (
            classify_republish_result,
        )

        status, _detail = classify_republish_result({"success": False})

        assert status == "retry"


class TestAttemptThumbnailRepublish:
    """Dependency-injected invocation wrapper around classify_republish_result."""

    def test_delegates_success_to_classify(self):
        from congress_videos.modules.thumbnail_republish import (
            attempt_thumbnail_republish,
        )

        def fake_set_thumbnail(thumbnail_path: str) -> dict:
            assert thumbnail_path.endswith("thumbnail.png")
            return {"success": True}

        status, _detail = attempt_thumbnail_republish("/data/turn/video.mp4", set_thumbnail_fn=fake_set_thumbnail)

        assert status == "healed"

    def test_delegates_missing_file_to_abandon(self):
        from congress_videos.modules.thumbnail_republish import (
            MISSING_FILE_ERROR_PREFIX,
            attempt_thumbnail_republish,
        )

        def fake_set_thumbnail(thumbnail_path: str) -> dict:
            return {
                "success": False,
                "error": f"{MISSING_FILE_ERROR_PREFIX} {thumbnail_path}",
            }

        status, _detail = attempt_thumbnail_republish("/data/turn/video.mp4", set_thumbnail_fn=fake_set_thumbnail)

        assert status == "abandon"

    def test_raising_callable_is_retry_with_exception_text(self):
        """An injected callable that raises must never propagate -- the DAG
        loops over multiple candidates and one bad item must not abort the
        rest (spec: per-item try/except isolation)."""
        from congress_videos.modules.thumbnail_republish import (
            attempt_thumbnail_republish,
        )

        def raising_set_thumbnail(thumbnail_path: str) -> dict:
            raise RuntimeError("connection reset")

        status, detail = attempt_thumbnail_republish("/data/turn/video.mp4", set_thumbnail_fn=raising_set_thumbnail)

        assert status == "retry"
        assert "connection reset" in detail


class TestModuleConstants:
    def test_constants_exported(self):
        from congress_videos.modules import thumbnail_republish as module

        assert isinstance(module.MAX_THUMBNAIL_CALLS_PER_RUN, int)
        assert isinstance(module.CANDIDATE_LIMIT, int)
        assert isinstance(module.STALE_RUN_TOLERANCE_MINUTES, int)
        assert module.THUMBNAIL_FILENAME == "thumbnail.png"
        assert module.MISSING_FILE_ERROR_PREFIX == "Thumbnail file not found:"

    def test_candidate_limit_reuses_database_constant(self):
        """CANDIDATE_LIMIT must be the SAME number as database.py's
        THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT, not a second, independently
        defined literal (WU2a deviation #2, resolved in WU2b)."""
        from congress_videos.modules import thumbnail_republish as module
        from congress_videos.modules.database import (
            THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT,
        )

        assert module.CANDIDATE_LIMIT == THUMBNAIL_REPUBLISH_CANDIDATE_LIMIT
