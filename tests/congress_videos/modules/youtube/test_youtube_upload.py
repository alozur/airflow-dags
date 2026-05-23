"""Tests for congress_videos.modules.youtube.youtube_upload."""

from __future__ import annotations

import pytest

from congress_videos.modules.youtube.youtube_upload import prepare_chapter_upload_config


# ---------------------------------------------------------------------------
# Helpers / builder functions
# ---------------------------------------------------------------------------

def _make_extraction_results(chapters: list[dict]) -> dict:
    return {
        "total_chapters": len(chapters),
        "successful_extractions": sum(1 for c in chapters if c.get("success")),
        "failed_extractions": sum(1 for c in chapters if not c.get("success")),
        "results": chapters,
    }


def _make_chapter(chapter_id: int, video_id: str = "vid1", success: bool = True,
                  output_path: str | None = None) -> dict:
    return {
        "chapter_id": chapter_id,
        "video_id": video_id,
        "success": success,
        "output_path": output_path or f"/data/{video_id}/{chapter_id}/chapter_video.mp4",
        "file_size_mb": 50.0,
        "duration_seconds": 300.0,
        "error": None if success else "Extraction error",
    }


def _make_metadata_results(topic_metadata: list[dict]) -> dict:
    return {"topic_metadata": topic_metadata}


def _make_topic_metadata(chapter_id: int, title: str, description: str,
                          video_id: str = "vid1") -> dict:
    return {
        "chapter_id": chapter_id,
        "video_id": video_id,
        "title": {"title": title, "character_count": len(title), "within_limit": True, "error": None},
        "description": {"description": description, "character_count": len(description), "error": None},
        "main_topic_content": title,
        "session_number": 42,
        "generation_success": True,
    }


def _make_thumbnail_results(thumbnails: list[dict]) -> dict:
    return {"results": thumbnails}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestPrepareChapterUploadConfig:

    # Guard conditions

    def test_none_extraction_results_returns_none(self):
        result = prepare_chapter_upload_config(None, None)
        assert result is None

    def test_empty_extraction_results_returns_none(self):
        empty = _make_extraction_results([])
        result = prepare_chapter_upload_config(empty, None)
        assert result is None

    def test_extraction_results_without_results_key_returns_none(self):
        result = prepare_chapter_upload_config({}, None)
        assert result is None

    # Failed extractions excluded

    def test_failed_extraction_only_returns_none(self):
        extraction = _make_extraction_results([_make_chapter(1, success=False)])
        result = prepare_chapter_upload_config(extraction, None)
        assert result is None

    def test_mixed_results_only_successful_included(self):
        extraction = _make_extraction_results([
            _make_chapter(1, success=True),
            _make_chapter(2, success=False),
        ])
        result = prepare_chapter_upload_config(extraction, None)
        assert result is not None
        assert len(result["videos"]) == 1
        assert result["videos"][0]["chapter_id"] == 1

    # Default title fallback

    def test_no_metadata_uses_default_title_with_chapter_id(self):
        extraction = _make_extraction_results([_make_chapter(5)])
        result = prepare_chapter_upload_config(extraction, None)
        assert result is not None
        assert "5" in result["videos"][0]["title"]

    # Metadata applied correctly

    def test_with_metadata_uses_provided_title(self):
        chapter_id = 7
        extraction = _make_extraction_results([_make_chapter(chapter_id)])
        metadata = _make_metadata_results([
            _make_topic_metadata(chapter_id, "El gran debate", "Una descripción detallada")
        ])
        result = prepare_chapter_upload_config(extraction, metadata)
        assert result["videos"][0]["title"] == "El gran debate"

    def test_with_metadata_uses_provided_description(self):
        chapter_id = 7
        extraction = _make_extraction_results([_make_chapter(chapter_id)])
        metadata = _make_metadata_results([
            _make_topic_metadata(chapter_id, "El gran debate", "Una descripción detallada")
        ])
        result = prepare_chapter_upload_config(extraction, metadata)
        assert result["videos"][0]["description"] == "Una descripción detallada"

    # Thumbnail integration

    def test_successful_thumbnail_included_in_video_config(self):
        chapter_id = 3
        extraction = _make_extraction_results([_make_chapter(chapter_id)])
        metadata = _make_metadata_results([_make_topic_metadata(chapter_id, "T", "D")])
        thumbnails = _make_thumbnail_results([
            {"chapter_id": chapter_id, "success": True, "output_path": "/thumbs/3.jpg"}
        ])
        result = prepare_chapter_upload_config(extraction, metadata, thumbnail_results=thumbnails)
        assert result["videos"][0]["thumbnail_file"] == "/thumbs/3.jpg"

    def test_failed_thumbnail_not_included_in_video_config(self):
        chapter_id = 4
        extraction = _make_extraction_results([_make_chapter(chapter_id)])
        metadata = _make_metadata_results([_make_topic_metadata(chapter_id, "T", "D")])
        thumbnails = _make_thumbnail_results([
            {"chapter_id": chapter_id, "success": False, "output_path": None}
        ])
        result = prepare_chapter_upload_config(extraction, metadata, thumbnail_results=thumbnails)
        assert "thumbnail_file" not in result["videos"][0]

    def test_none_thumbnail_results_no_thumbnail_in_video_config(self):
        chapter_id = 6
        extraction = _make_extraction_results([_make_chapter(chapter_id)])
        result = prepare_chapter_upload_config(extraction, None, thumbnail_results=None)
        assert "thumbnail_file" not in result["videos"][0]

    # Privacy status via is_testing flag

    def test_is_testing_true_sets_private_privacy_status(self):
        extraction = _make_extraction_results([_make_chapter(1)])
        result = prepare_chapter_upload_config(extraction, None, is_testing=True)
        assert result["videos"][0]["privacy_status"] == "private"

    def test_is_testing_false_sets_public_privacy_status(self):
        extraction = _make_extraction_results([_make_chapter(1)])
        result = prepare_chapter_upload_config(extraction, None, is_testing=False)
        assert result["videos"][0]["privacy_status"] == "public"

    # Config shape

    def test_returned_config_has_token_file_and_videos(self):
        extraction = _make_extraction_results([_make_chapter(1)])
        result = prepare_chapter_upload_config(extraction, None)
        assert "token_file" in result
        assert "videos" in result
        assert isinstance(result["videos"], list)

    def test_video_config_has_all_required_fields(self):
        extraction = _make_extraction_results([_make_chapter(1)])
        result = prepare_chapter_upload_config(extraction, None)
        video = result["videos"][0]
        for field in ["chapter_id", "video_id", "video_file", "title", "description",
                      "category_id", "privacy_status", "tags", "made_for_kids"]:
            assert field in video, f"Missing field: {field}"
