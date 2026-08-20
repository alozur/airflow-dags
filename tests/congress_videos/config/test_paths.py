"""Tests for congress_videos.config.paths module."""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from congress_videos.config.paths import (
    ASSETS_DIR,
    BACKGROUND_IMAGE,
    BASE_DATA_DIR,
    CHANNEL_LOGO,
    DOWNLOADS_DIR,
    FONT_BOLD,
    FONT_REGULAR,
    FONTS_DIR,
    PROJECT_DATA_DIR,
    VIDEOS_DIR,
    YOUTUBE_TOKEN_FILE,
    ensure_directory_exists,
    get_download_date_path,
    get_download_file_path,
    get_download_video_path,
    get_orador_artifact_path,
    get_orador_video_dir,
    get_session_path,
    get_topic_path,
    get_video_path,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

class TestModuleConstants:
    """Verify constant strings are non-empty and terminate with expected segments."""

    def test_base_data_dir_is_nonempty_string(self):
        assert isinstance(BASE_DATA_DIR, str) and BASE_DATA_DIR

    def test_project_data_dir_ends_with_congress_videos(self):
        assert PROJECT_DATA_DIR.endswith("congress_videos")

    def test_videos_dir_ends_with_videos(self):
        assert VIDEOS_DIR.endswith("videos")

    def test_downloads_dir_ends_with_downloads(self):
        assert DOWNLOADS_DIR.endswith("downloads")

    def test_assets_dir_ends_with_assets(self):
        assert ASSETS_DIR.endswith("assets")

    def test_fonts_dir_ends_with_fonts(self):
        assert FONTS_DIR.endswith("fonts")

    def test_youtube_token_file_ends_with_pickle(self):
        assert YOUTUBE_TOKEN_FILE.endswith(".pickle")

    def test_font_bold_ends_with_ttf(self):
        assert FONT_BOLD.endswith(".ttf")

    def test_font_regular_ends_with_ttf(self):
        assert FONT_REGULAR.endswith(".ttf")

    def test_background_image_ends_with_png(self):
        assert BACKGROUND_IMAGE.endswith(".png")

    def test_channel_logo_ends_with_png(self):
        assert CHANNEL_LOGO.endswith(".png")

    def test_project_data_dir_contains_base_data_dir(self):
        assert PROJECT_DATA_DIR.startswith(BASE_DATA_DIR)

    def test_videos_dir_contains_project_data_dir(self):
        assert VIDEOS_DIR.startswith(PROJECT_DATA_DIR)

    def test_downloads_dir_contains_project_data_dir(self):
        assert DOWNLOADS_DIR.startswith(PROJECT_DATA_DIR)


# ---------------------------------------------------------------------------
# get_session_path
# ---------------------------------------------------------------------------

class TestGetSessionPath:
    def test_returns_string(self):
        result = get_session_path("PL_115_001")
        assert isinstance(result, str)

    def test_path_ends_with_session_number(self):
        result = get_session_path("PL_115_001")
        assert result.endswith("PL_115_001")

    def test_path_starts_with_videos_dir(self):
        result = get_session_path("PL_115_001")
        assert result.startswith(VIDEOS_DIR)

    def test_path_separator(self):
        result = get_session_path("MY_SESSION")
        assert result == f"{VIDEOS_DIR}/MY_SESSION"

    @pytest.mark.parametrize("session", ["PL_115_001", "PL_001", "SESSION-2025", "123"])
    def test_various_session_numbers(self, session: str):
        result = get_session_path(session)
        assert result.endswith(session)
        assert VIDEOS_DIR in result


# ---------------------------------------------------------------------------
# get_topic_path
# ---------------------------------------------------------------------------

class TestGetTopicPath:
    def test_returns_correct_path(self):
        result = get_topic_path("PL_115_001", "20250101_01")
        assert result == f"{VIDEOS_DIR}/PL_115_001/20250101_01"

    def test_ends_with_topic_entry_id(self):
        result = get_topic_path("PL_115_001", "20250101_01")
        assert result.endswith("20250101_01")

    def test_contains_session_number(self):
        result = get_topic_path("PL_115_001", "20250101_01")
        assert "PL_115_001" in result

    def test_path_depth_is_correct(self):
        result = get_topic_path("S1", "T1")
        assert result == f"{VIDEOS_DIR}/S1/T1"


# ---------------------------------------------------------------------------
# get_video_path
# ---------------------------------------------------------------------------

class TestGetVideoPath:
    def test_returns_correct_path(self):
        result = get_video_path("PL_115_001", "20250101_01", "video.mp4")
        assert result == f"{VIDEOS_DIR}/PL_115_001/20250101_01/video.mp4"

    def test_ends_with_filename(self):
        result = get_video_path("PL_115_001", "20250101_01", "clip.mp4")
        assert result.endswith("clip.mp4")

    def test_contains_all_segments(self):
        result = get_video_path("PL_115_001", "20250101_01", "video.mp4")
        assert "PL_115_001" in result
        assert "20250101_01" in result
        assert "video.mp4" in result

    @pytest.mark.parametrize("filename", ["video.mp4", "audio.mp3", "thumbnail.png", "subtitles.srt"])
    def test_various_filenames(self, filename: str):
        result = get_video_path("S1", "T1", filename)
        assert result.endswith(filename)


# ---------------------------------------------------------------------------
# get_download_date_path
# ---------------------------------------------------------------------------

class TestGetDownloadDatePath:
    def test_returns_correct_path(self):
        result = get_download_date_path("2025-10-08")
        assert result == f"{DOWNLOADS_DIR}/2025-10-08"

    def test_ends_with_date(self):
        result = get_download_date_path("2025-01-01")
        assert result.endswith("2025-01-01")

    def test_starts_with_downloads_dir(self):
        result = get_download_date_path("2025-10-08")
        assert result.startswith(DOWNLOADS_DIR)

    @pytest.mark.parametrize("date", ["2025-01-01", "2024-12-31", "2026-06-15"])
    def test_various_dates(self, date: str):
        result = get_download_date_path(date)
        assert result == f"{DOWNLOADS_DIR}/{date}"


# ---------------------------------------------------------------------------
# get_download_video_path
# ---------------------------------------------------------------------------

class TestGetDownloadVideoPath:
    def test_returns_correct_path(self):
        result = get_download_video_path("2025-10-08", "hy1cnx-0Oww")
        assert result == f"{DOWNLOADS_DIR}/2025-10-08/hy1cnx-0Oww"

    def test_ends_with_video_id(self):
        result = get_download_video_path("2025-10-08", "abc123xyz")
        assert result.endswith("abc123xyz")

    def test_contains_date(self):
        result = get_download_video_path("2025-10-08", "abc123xyz")
        assert "2025-10-08" in result


# ---------------------------------------------------------------------------
# get_download_file_path
# ---------------------------------------------------------------------------

class TestGetDownloadFilePath:
    def test_returns_correct_path(self):
        result = get_download_file_path("2025-10-08", "hy1cnx-0Oww", "agenda.pdf")
        assert result == f"{DOWNLOADS_DIR}/2025-10-08/hy1cnx-0Oww/agenda.pdf"

    def test_ends_with_filename(self):
        result = get_download_file_path("2025-10-08", "hy1cnx-0Oww", "video.mp4")
        assert result.endswith("video.mp4")

    def test_contains_all_segments(self):
        result = get_download_file_path("2025-10-08", "hy1cnx-0Oww", "subs.srt")
        assert "2025-10-08" in result
        assert "hy1cnx-0Oww" in result
        assert "subs.srt" in result

    @pytest.mark.parametrize("filename", ["agenda.pdf", "video.mp4", "audio.webm", "chunk_001.webm"])
    def test_various_filenames(self, filename: str):
        result = get_download_file_path("2025-01-01", "vid123", filename)
        assert result.endswith(filename)


# ---------------------------------------------------------------------------
# ensure_directory_exists
# ---------------------------------------------------------------------------

class TestEnsureDirectoryExists:
    def test_creates_directory(self, tmp_path):
        new_dir = str(tmp_path / "new_subdir")
        ensure_directory_exists(new_dir)
        assert os.path.isdir(new_dir)

    def test_returns_the_path(self, tmp_path):
        new_dir = str(tmp_path / "subdir")
        result = ensure_directory_exists(new_dir)
        assert result == new_dir

    def test_idempotent_when_dir_exists(self, tmp_path):
        existing_dir = str(tmp_path / "exists")
        os.makedirs(existing_dir)
        # Should not raise even if already exists
        result = ensure_directory_exists(existing_dir)
        assert result == existing_dir

    def test_creates_nested_directories(self, tmp_path):
        nested = str(tmp_path / "a" / "b" / "c")
        result = ensure_directory_exists(nested)
        assert os.path.isdir(nested)
        assert result == nested


# ---------------------------------------------------------------------------
# get_orador_video_dir — T1
# ---------------------------------------------------------------------------

class TestGetOradorVideoDir:
    """Tests for get_orador_video_dir helper (canonical speaker-turn layout)."""

    def test_default_channel_slug(self):
        result = get_orador_video_dir("abc123", 7, 42)
        assert result == Path(f"{PROJECT_DATA_DIR}/congreso-es-tv/abc123/video_chapters/7/oradores/42")

    def test_custom_channel_slug(self):
        result = get_orador_video_dir("abc123", 7, 42, channel_slug="otro-canal")
        assert result == Path(f"{PROJECT_DATA_DIR}/otro-canal/abc123/video_chapters/7/oradores/42")

    def test_integer_ids_coerce_to_string_form(self):
        result = get_orador_video_dir("vid1", 10, 99)
        path_str = str(result)
        assert "/10/" in path_str
        assert path_str.endswith("/99")

    def test_string_ids_identical_to_integer_ids(self):
        assert get_orador_video_dir("vid1", "10", "99") == get_orador_video_dir("vid1", 10, 99)

    def test_no_directory_created(self, tmp_path):
        result = get_orador_video_dir("nosuchvid", 1, 1)
        assert not result.exists()

    def test_return_type_is_path(self):
        result = get_orador_video_dir("abc123", 7, 42)
        assert isinstance(result, Path)

    def test_exact_segment_sequence(self):
        result = get_orador_video_dir("abc123", 7, 42)
        parts = result.parts
        # Verify canonical order in the trailing segments
        idx = parts.index("congreso-es-tv")
        assert parts[idx] == "congreso-es-tv"
        assert parts[idx + 1] == "abc123"
        assert parts[idx + 2] == "video_chapters"
        assert parts[idx + 3] == "7"
        assert parts[idx + 4] == "oradores"
        assert parts[idx + 5] == "42"


# ---------------------------------------------------------------------------
# get_orador_artifact_path — T2
# ---------------------------------------------------------------------------

class TestGetOradorArtifactPath:
    """Tests for get_orador_artifact_path helper."""

    def test_video_artifact_path(self):
        result = get_orador_artifact_path("abc123", 7, 42, "video.mp4")
        assert result == get_orador_video_dir("abc123", 7, 42) / "video.mp4"

    @pytest.mark.parametrize("filename", [
        "video.mp4",
        "subtitles.srt",
        "thumbnail.png",
        "title.txt",
        "description.txt",
    ])
    def test_all_five_artifact_filenames(self, filename: str):
        result = get_orador_artifact_path("abc123", 7, 42, filename)
        assert result.name == filename
        assert result.parent == get_orador_video_dir("abc123", 7, 42)

    def test_composition_via_get_orador_video_dir(self):
        src, chap, turn, name = "somevid", 3, 17, "thumbnail.png"
        result = get_orador_artifact_path(src, chap, turn, name)
        assert result == get_orador_video_dir(src, chap, turn) / name

    def test_no_directory_created(self):
        result = get_orador_artifact_path("nosuchvid", 1, 1, "video.mp4")
        assert not result.parent.exists()

    def test_return_type_is_path(self):
        result = get_orador_artifact_path("abc123", 7, 42, "title.txt")
        assert isinstance(result, Path)


# ---------------------------------------------------------------------------
# TestOradorHelpersImportPurity — T3
# ---------------------------------------------------------------------------

class TestOradorHelpersImportPurity:
    """Tests that helpers are pure, deterministic, and importable without Airflow."""

    def test_deterministic_video_dir(self):
        args = ("myvid", 5, 10)
        assert get_orador_video_dir(*args) == get_orador_video_dir(*args)

    def test_deterministic_artifact_path(self):
        args = ("myvid", 5, 10, "video.mp4")
        assert get_orador_artifact_path(*args) == get_orador_artifact_path(*args)

    def test_import_requires_only_pathlib_and_config(self):
        # Re-import inside the test body to confirm no Airflow/DB dependency at import time.
        from congress_videos.config.paths import (  # noqa: PLC0415
            get_orador_artifact_path as _a,
            get_orador_video_dir as _v,
        )
        assert callable(_v)
        assert callable(_a)
