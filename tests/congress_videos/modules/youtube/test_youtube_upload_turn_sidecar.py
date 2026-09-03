"""Tests for turn upload sidecar read path (issue #146).

RED tests (4.1–4.2):
- prepare_orador_upload_config reads pre-written sidecars (no AI/ffmpeg calls)
- Missing sidecar raises explicit error naming the missing file
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# 4.1 Turn upload reads pre-prepared sidecars; no thumbnail trigger
# ---------------------------------------------------------------------------


class TestPrepareOradorUploadConfig:
    """prepare_orador_upload_config reads sidecars + presence-verifies; zero AI calls."""

    def test_reads_title_from_sidecar_file(self, tmp_path):
        """title must come from title.txt, not from AI metadata generation."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("TÍTULO PREPARADO", encoding="utf-8")
        (turn_dir / "description.txt").write_text("Descripción.", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("1\n00:00:01,000 --> 00:00:02,000\nHola\n", encoding="utf-8")

        video_path = str(turn_dir / "video.mp4")
        config = prepare_orador_upload_config(output_path=video_path, is_testing=False)

        assert config is not None
        assert config["title"] == "TÍTULO PREPARADO"

    def test_reads_description_from_sidecar_file(self, tmp_path):
        """description must come from description.txt sidecar."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("Desc preparada.", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        video_path = str(turn_dir / "video.mp4")
        config = prepare_orador_upload_config(output_path=video_path, is_testing=False)

        assert config["description"] == "Desc preparada."

    def test_reads_thumbnail_from_sidecar_file(self, tmp_path):
        """thumbnail_file must point to the pre-written thumbnail.png sidecar."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        thumb = turn_dir / "thumbnail.png"
        thumb.write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        video_path = str(turn_dir / "video.mp4")
        config = prepare_orador_upload_config(output_path=video_path, is_testing=False)

        assert config["thumbnail_file"] == str(thumb)

    def test_no_generic_thumbnail_generator_call(self, tmp_path):
        """prepare_orador_upload_config must NOT call generic_thumbnail_generator."""
        from unittest.mock import patch

        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        with patch("airflow.api.common.trigger_dag.trigger_dag") as mock_trigger:
            video_path = str(turn_dir / "video.mp4")
            prepare_orador_upload_config(output_path=video_path, is_testing=False)

        mock_trigger.assert_not_called()

    def test_no_ai_or_ffmpeg_service_calls(self, tmp_path):
        """prepare_orador_upload_config must not call any AI service (no subprocess for ffmpeg)."""
        import subprocess
        from unittest.mock import patch

        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        with patch("subprocess.run") as mock_sub:
            video_path = str(turn_dir / "video.mp4")
            prepare_orador_upload_config(output_path=video_path, is_testing=False)

        mock_sub.assert_not_called()

    def test_returned_config_has_required_fields(self, tmp_path):
        """Config must include video_file, title, description, thumbnail_file."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        video_path = str(turn_dir / "video.mp4")
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        config = prepare_orador_upload_config(output_path=video_path, is_testing=False)

        assert "video_file" in config
        assert "title" in config
        assert "description" in config
        assert "thumbnail_file" in config

    def test_is_testing_sets_private_status(self, tmp_path):
        """is_testing=True must set privacy_status to private."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        config = prepare_orador_upload_config(output_path=str(turn_dir / "video.mp4"), is_testing=True)
        assert config["privacy_status"] == "private"


# ---------------------------------------------------------------------------
# 4.2 Missing sidecar presence-verify raises with explicit error
# ---------------------------------------------------------------------------


class TestPrepareOradorUploadConfigMissingSidecar:
    @pytest.mark.parametrize(
        "missing_file",
        [
            "title.txt",
            "description.txt",
            "thumbnail.png",
            "subtitles.srt",
        ],
    )
    def test_missing_sidecar_raises_file_not_found(self, tmp_path, missing_file):
        """When a required sidecar is absent, must raise naming the missing file."""
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        # Remove the target sidecar to trigger the guard
        (turn_dir / missing_file).unlink()

        with pytest.raises((FileNotFoundError, ValueError, RuntimeError)) as exc_info:
            prepare_orador_upload_config(output_path=str(turn_dir / "video.mp4"), is_testing=False)

        # Error message must name the missing file (or at least indicate what is missing)
        assert missing_file in str(exc_info.value) or "sidecar" in str(exc_info.value).lower()
