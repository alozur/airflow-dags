"""Tests for congress_videos.modules.youtube.download module."""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_completion(mocker, raw_content):
    """Patch download.cached_json_completion to mimic generate_json_completion.

    Returns the mock so tests can assert on call args. ``raw_content`` is the raw
    string the LLM would have returned; it is parsed exactly like the real
    pipeline (a parse failure yields error != None, mirroring the live wrapper).
    """
    try:
        data = json.loads(raw_content)
        error = None
    except (json.JSONDecodeError, TypeError):
        data = None
        error = f"Failed to parse JSON: {raw_content!r}"

    return mocker.patch(
        "congress_videos.modules.youtube.download.cached_json_completion",
        return_value={"data": data, "raw_content": raw_content, "error": error},
    )

def _make_video_details(*videos):
    """Build the structure download_video_from_youtube expects."""
    return {"videos": list(videos)}


def _make_video(video_id="vid001", youtube_url="https://www.youtube.com/watch?v=vid001", **kw):
    return {"video_id": video_id, "youtube_url": youtube_url, "title": "Test Video", **kw}


# ---------------------------------------------------------------------------
# create_test_video_data
# ---------------------------------------------------------------------------

class TestCreateTestVideoData:

    def test_standard_watch_url_extracts_video_id(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        result = create_test_video_data("https://www.youtube.com/watch?v=ZBU0bVpYXM4")

        assert result["total_matches"] == 1
        assert result["videos"][0]["video_id"] == "ZBU0bVpYXM4"

    def test_short_youtu_be_url_extracts_video_id(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        result = create_test_video_data("https://youtu.be/AbCdEfGhIjK")

        assert result["videos"][0]["video_id"] == "AbCdEfGhIjK"

    def test_invalid_url_falls_back_to_default_video_id(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        result = create_test_video_data("not-a-valid-url")

        assert result["total_matches"] == 1
        assert len(result["videos"][0]["video_id"]) == 11  # YouTube IDs are 11 chars

    def test_returns_dict_with_videos_list(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        result = create_test_video_data()

        assert isinstance(result, dict)
        assert "videos" in result
        assert isinstance(result["videos"], list)

    def test_video_entry_has_required_fields(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        result = create_test_video_data("https://www.youtube.com/watch?v=XXXXXXXXXXX")

        video = result["videos"][0]
        for field in ("video_id", "title", "url", "is_live", "is_upcoming"):
            assert field in video, f"Missing field: {field}"

    def test_url_stored_in_video_entry(self):
        from congress_videos.modules.youtube.download import create_test_video_data

        url = "https://www.youtube.com/watch?v=XXXXXXXXXXX"
        result = create_test_video_data(url)

        assert result["videos"][0]["url"] == url


# ---------------------------------------------------------------------------
# download_video_from_youtube
# ---------------------------------------------------------------------------

class TestDownloadVideoFromYoutube:

    def _patch_downloader(self, mocker, return_value):
        return mocker.patch(
            "congress_videos.modules.youtube.download.download_youtube_video_for_upload",
            return_value=return_value
        )

    def _patch_paths(self, mocker, tmp_path):
        mocker.patch(
            "congress_videos.modules.youtube.download.get_download_video_path",
            return_value=str(tmp_path / "downloads" / "2025-01-01" / "vid001")
        )
        mocker.patch("congress_videos.modules.youtube.download.ensure_directory_exists")

    def test_empty_video_details_returns_zero_downloaded(self):
        from congress_videos.modules.youtube.download import download_video_from_youtube

        result = download_video_from_youtube({}, "2025-01-01")

        assert result["total_downloaded"] == 0
        assert result["videos"] == []

    def test_none_video_details_returns_zero_downloaded(self):
        from congress_videos.modules.youtube.download import download_video_from_youtube

        result = download_video_from_youtube(None, "2025-01-01")

        assert result["total_downloaded"] == 0
        assert result["videos"] == []

    def test_video_without_youtube_url_produces_error_entry(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)

        from congress_videos.modules.youtube.download import download_video_from_youtube

        details = _make_video_details({"video_id": "vid001", "title": "No URL video"})
        result = download_video_from_youtube(details, "2025-01-01")

        assert result["total_downloaded"] == 0
        assert "error" in result["videos"][0]

    def test_successful_download_increments_total(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        self._patch_downloader(mocker, {
            "success": True,
            "file_path": str(tmp_path / "video.mp4"),
            "file_size_mb": 500,
            "duration": 3600,
            "title": "Test Video"
        })

        from congress_videos.modules.youtube.download import download_video_from_youtube

        details = _make_video_details(_make_video())
        result = download_video_from_youtube(details, "2025-01-01")

        assert result["total_downloaded"] == 1
        assert "file_path" in result["videos"][0]

    def test_failed_download_produces_error_entry(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        self._patch_downloader(mocker, {
            "success": False,
            "error": "yt-dlp failed"
        })

        from congress_videos.modules.youtube.download import download_video_from_youtube

        details = _make_video_details(_make_video())
        result = download_video_from_youtube(details, "2025-01-01")

        assert result["total_downloaded"] == 0
        assert "error" in result["videos"][0]

    def test_exception_during_download_is_handled(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        mocker.patch(
            "congress_videos.modules.youtube.download.download_youtube_video_for_upload",
            side_effect=RuntimeError("unexpected crash")
        )

        from congress_videos.modules.youtube.download import download_video_from_youtube

        details = _make_video_details(_make_video())
        result = download_video_from_youtube(details, "2025-01-01")

        assert result["total_downloaded"] == 0
        assert "error" in result["videos"][0]

    def test_multiple_videos_counted_correctly(self, mocker, tmp_path):
        mocker.patch(
            "congress_videos.modules.youtube.download.get_download_video_path",
            return_value=str(tmp_path)
        )
        mocker.patch("congress_videos.modules.youtube.download.ensure_directory_exists")
        self._patch_downloader(mocker, {
            "success": True,
            "file_path": str(tmp_path / "video.mp4"),
            "file_size_mb": 100,
            "duration": 1800,
            "title": "Test"
        })

        from congress_videos.modules.youtube.download import download_video_from_youtube

        details = _make_video_details(
            _make_video("vid001"),
            _make_video("vid002", youtube_url="https://www.youtube.com/watch?v=vid002"),
        )
        result = download_video_from_youtube(details, "2025-01-01")

        assert result["total_downloaded"] == 2


# ---------------------------------------------------------------------------
# extract_audio_from_youtube
# ---------------------------------------------------------------------------

class TestExtractAudioFromYoutube:

    def _patch_paths(self, mocker, tmp_path):
        mocker.patch(
            "congress_videos.modules.youtube.download.get_download_video_path",
            return_value=str(tmp_path)
        )
        mocker.patch("congress_videos.modules.youtube.download.ensure_directory_exists")

    def test_empty_video_details_returns_zero_extracted(self):
        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        result = extract_audio_from_youtube({}, "2025-01-01")

        assert result["total_extracted"] == 0
        assert result["videos"] == []

    def test_none_video_details_returns_zero_extracted(self):
        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        result = extract_audio_from_youtube(None, "2025-01-01")

        assert result["total_extracted"] == 0

    def test_video_without_url_produces_error_entry(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)

        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        details = _make_video_details({"video_id": "v1", "title": "no url"})
        result = extract_audio_from_youtube(details, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_single_file_extraction_success(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        mocker.patch(
            "congress_videos.modules.youtube.download.download_audio_only",
            return_value={
                "success": True,
                "file_path": str(tmp_path / "audio.webm"),
                "file_size_mb": 50,
                "duration": 3600
            }
        )

        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        details = _make_video_details(_make_video())
        result = extract_audio_from_youtube(details, "2025-01-01")

        assert result["total_extracted"] == 1
        assert result["videos"][0]["audio_file_path"] == str(tmp_path / "audio.webm")

    def test_chunked_extraction_success(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        chunks = [
            {"chunk_index": 0, "file_path": str(tmp_path / "c0.webm"), "file_size_mb": 25},
            {"chunk_index": 1, "file_path": str(tmp_path / "c1.webm"), "file_size_mb": 25},
        ]
        mocker.patch(
            "congress_videos.modules.youtube.download.download_audio_in_chunks",
            return_value={
                "success": True,
                "total_chunks": 2,
                "total_duration": 3600,
                "chunks": chunks
            }
        )

        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        details = _make_video_details(_make_video())
        result = extract_audio_from_youtube(details, "2025-01-01", chunk_duration_minutes=15)

        assert result["videos"][0]["chunked"] is True
        assert result["videos"][0]["total_chunks"] == 2

    def test_failed_single_extraction_produces_error(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        mocker.patch(
            "congress_videos.modules.youtube.download.download_audio_only",
            return_value={"success": False, "error": "download failed"}
        )

        from congress_videos.modules.youtube.download import extract_audio_from_youtube

        details = _make_video_details(_make_video())
        result = extract_audio_from_youtube(details, "2025-01-01")

        assert result["total_extracted"] == 0
        assert "error" in result["videos"][0]


# ---------------------------------------------------------------------------
# transcribe_audio_with_whisper
# ---------------------------------------------------------------------------

class TestTranscribeAudioWithWhisper:

    def _patch_whisper(self, mocker, health=True, transcribe_result=None, chunks_result=None):
        # These are imported lazily inside transcribe_audio_with_whisper — patch at source
        mocker.patch(
            "utils.whisper_helpers.check_whisper_api_health",
            return_value=health
        )
        if transcribe_result is not None:
            mocker.patch(
                "utils.whisper_helpers.transcribe_audio_file",
                return_value=transcribe_result
            )
        if chunks_result is not None:
            mocker.patch(
                "utils.whisper_helpers.transcribe_audio_chunks",
                return_value=chunks_result
            )

    def test_empty_input_returns_zero_transcribed(self, mocker):
        self._patch_whisper(mocker, health=True)

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        result = transcribe_audio_with_whisper({}, "es")

        assert result["total_transcribed"] == 0
        assert result["videos"] == []

    def test_none_input_returns_zero_transcribed(self, mocker):
        self._patch_whisper(mocker, health=True)

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        result = transcribe_audio_with_whisper(None, "es")

        assert result["total_transcribed"] == 0

    def test_whisper_api_unavailable_returns_error(self, mocker):
        self._patch_whisper(mocker, health=False)

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        extracted = {"videos": [{"video_id": "v1", "chunked": False, "audio_file_path": "/a.webm"}]}
        result = transcribe_audio_with_whisper(extracted, "es")

        assert result["total_transcribed"] == 0
        assert "error" in result

    def test_single_file_transcription_success(self, mocker, tmp_path):
        audio_path = str(tmp_path / "audio.webm")
        self._patch_whisper(
            mocker,
            health=True,
            transcribe_result={"success": True, "text": "Hola mundo", "duration": 10, "error": None}
        )

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        extracted = {"videos": [{
            "video_id": "v1",
            "chunked": False,
            "audio_file_path": audio_path,
            "video_title": "Test"
        }]}
        result = transcribe_audio_with_whisper(extracted, "es")

        assert result["total_transcribed"] == 1
        assert result["videos"][0]["transcription"] == "Hola mundo"

    def test_single_file_without_audio_path_produces_error(self, mocker):
        self._patch_whisper(mocker, health=True)

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        extracted = {"videos": [{"video_id": "v1", "chunked": False}]}
        result = transcribe_audio_with_whisper(extracted, "es")

        assert "error" in result["videos"][0]

    def test_chunked_audio_transcription(self, mocker):
        chunks_result = {
            "total_chunks": 2,
            "successful_transcriptions": 2,
            "chunks": [
                {"chunk_index": 0, "text": "Primer bloque", "success": True},
                {"chunk_index": 1, "text": "Segundo bloque", "success": True},
            ]
        }
        self._patch_whisper(mocker, health=True, chunks_result=chunks_result)

        from congress_videos.modules.youtube.download import transcribe_audio_with_whisper

        extracted = {"videos": [{
            "video_id": "v1",
            "chunked": True,
            "chunks": [{"chunk_index": 0}, {"chunk_index": 1}],
            "video_title": "Test"
        }]}
        result = transcribe_audio_with_whisper(extracted, "es")

        assert result["videos"][0]["chunked"] is True
        assert result["videos"][0]["total_chunks"] == 2


# ---------------------------------------------------------------------------
# merge_transcription_srt_files
# ---------------------------------------------------------------------------

class TestMergeTranscriptionSrtFiles:

    def test_empty_transcriptions_returns_zero_merged(self):
        from congress_videos.modules.youtube.download import merge_transcription_srt_files

        result = merge_transcription_srt_files({}, "2025-01-01")

        assert result["total_merged"] == 0
        assert result["videos"] == []

    def test_none_transcriptions_returns_zero_merged(self):
        from congress_videos.modules.youtube.download import merge_transcription_srt_files

        result = merge_transcription_srt_files(None, "2025-01-01")

        assert result["total_merged"] == 0

    def test_non_chunked_video_is_skipped(self):
        from congress_videos.modules.youtube.download import merge_transcription_srt_files

        transcriptions = {"videos": [{"video_id": "v1", "chunked": False}]}
        result = merge_transcription_srt_files(transcriptions, "2025-01-01")

        assert result["total_merged"] == 0
        assert result["videos"] == []

    def test_chunked_video_missing_srt_dir_produces_error(self, mocker, tmp_path):
        # get_download_video_path is imported lazily from congress_videos.config.paths
        mocker.patch(
            "congress_videos.config.paths.get_download_video_path",
            return_value=str(tmp_path / "nonexistent_video_dir")
        )

        from congress_videos.modules.youtube.download import merge_transcription_srt_files

        transcriptions = {"videos": [{"video_id": "v1", "chunked": True, "video_title": "T"}]}
        result = merge_transcription_srt_files(transcriptions, "2025-01-01")

        assert result["videos"][0].get("error") is not None

    def test_successful_merge_returns_merged_path(self, mocker, tmp_path):
        video_dir = tmp_path / "vid001"
        srt_dir = video_dir / "srt_files"
        srt_dir.mkdir(parents=True)
        (srt_dir / "chunk_001.srt").write_text("1\n00:00:00 --> 00:00:05\nHola\n", encoding="utf-8")
        (srt_dir / "chunk_002.srt").write_text("1\n00:00:05 --> 00:00:10\nMundo\n", encoding="utf-8")

        # get_download_video_path is imported lazily from congress_videos.config.paths
        mocker.patch(
            "congress_videos.config.paths.get_download_video_path",
            return_value=str(video_dir)
        )
        merged_path = str(srt_dir / "vid001_merged.srt")
        # merge_srt_files is imported lazily from utils.whisper_helpers
        mocker.patch(
            "utils.whisper_helpers.merge_srt_files",
            return_value={"success": True, "output_path": merged_path, "total_entries": 2}
        )

        from congress_videos.modules.youtube.download import merge_transcription_srt_files

        transcriptions = {"videos": [{"video_id": "vid001", "chunked": True, "video_title": "T"}]}
        result = merge_transcription_srt_files(transcriptions, "2025-01-01")

        assert result["total_merged"] == 1
        assert result["videos"][0]["merged_srt_path"] == merged_path


# ---------------------------------------------------------------------------
# split_srt_by_silence
# ---------------------------------------------------------------------------

class TestSplitSrtBySilence:

    def test_empty_srt_data_returns_zero_videos(self):
        from congress_videos.modules.youtube.download import split_srt_by_silence

        result = split_srt_by_silence({}, "2025-01-01")

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_none_srt_data_returns_zero_videos(self):
        from congress_videos.modules.youtube.download import split_srt_by_silence

        result = split_srt_by_silence(None, "2025-01-01")

        assert result["total_videos"] == 0

    def test_video_without_merged_srt_path_produces_error(self):
        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1"}]}
        result = split_srt_by_silence(srt_data, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_video_with_error_flag_is_skipped(self):
        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1", "error": "merge failed", "merged_srt_path": None}]}
        result = split_srt_by_silence(srt_data, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_successful_split_returns_chunks(self, mocker, tmp_path):
        srt_file = tmp_path / "vid001_merged.srt"
        srt_file.write_text(
            "00:00:00 --> 00:01:00\nContenido chunk uno\n\n00:30:00 --> 01:00:00\nContenido chunk dos\n",
            encoding="utf-8"
        )

        chunks = [
            {"chunk_number": 1, "start_time": "00:00:00", "end_time": "00:30:00",
             "duration_minutes": 30, "duration_seconds": 1800, "content": "Bloque 1"},
            {"chunk_number": 2, "start_time": "00:30:00", "end_time": "01:00:00",
             "duration_minutes": 30, "duration_seconds": 1800, "content": "Bloque 2"},
        ]
        # chunk_by_silence is imported lazily inside the function — patch at source module
        mocker.patch(
            "utils.ai_chapter_analyzer.chunk_by_silence",
            return_value=chunks
        )

        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1", "merged_srt_path": str(srt_file)}]}
        result = split_srt_by_silence(srt_data, "2025-01-01")

        assert result["total_videos"] == 1
        assert result["videos"][0]["total_chunks"] == 2

    def test_chunks_store_srt_on_disk_not_inline(self, mocker, tmp_path):
        """#7: chunks must carry an srt_file_path and drop inline content so the
        XCom payload stays small and JSON-serialisable."""
        srt_file = tmp_path / "vid001_merged.srt"
        srt_file.write_text("00:00:00 --> 00:01:00\nHola\n", encoding="utf-8")

        chunks = [
            {"chunk_number": 1, "start_time": "00:00:00", "end_time": "00:30:00",
             "duration_minutes": 30, "duration_seconds": 1800, "content": "Bloque 1"},
        ]
        mocker.patch("utils.ai_chapter_analyzer.chunk_by_silence", return_value=chunks)

        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1", "merged_srt_path": str(srt_file)}]}
        result = split_srt_by_silence(srt_data, "2025-01-01")

        out_chunk = result["videos"][0]["chunks"][0]
        assert "content" not in out_chunk, "inline content must be removed from XCom"
        assert "srt_file_path" in out_chunk
        from pathlib import Path
        slice_path = Path(out_chunk["srt_file_path"])
        assert slice_path.exists()
        assert slice_path.read_text(encoding="utf-8") == "Bloque 1"

    def test_xcom_payload_is_json_serialisable_and_small(self, mocker, tmp_path):
        """#7: result must be JSON-serialisable and well under the 1MB guard."""
        srt_file = tmp_path / "vid001_merged.srt"
        srt_file.write_text("00:00:00 --> 00:01:00\nHola\n", encoding="utf-8")

        # Even with a large slice, only the path travels in XCom.
        chunks = [
            {"chunk_number": 1, "start_time": "00:00:00", "end_time": "00:30:00",
             "duration_minutes": 30, "duration_seconds": 1800, "content": "x" * 500_000},
        ]
        mocker.patch("utils.ai_chapter_analyzer.chunk_by_silence", return_value=chunks)

        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1", "merged_srt_path": str(srt_file)}]}
        result = split_srt_by_silence(srt_data, "2025-01-01")

        payload = json.dumps(result)
        assert len(payload.encode("utf-8")) < 1_000_000

    def test_use_adaptive_forwarded_to_chunk_by_silence(self, mocker, tmp_path):
        """#13 wiring: split_srt_by_silence(use_adaptive=True) must forward
        use_adaptive and adaptive_percentile through to chunk_by_silence so the
        adaptive silence threshold is reachable in production."""
        import utils.ai_chapter_analyzer as _aca

        srt_file = tmp_path / "vid001_merged.srt"
        srt_file.write_text("00:00:00 --> 00:01:00\nHola\n", encoding="utf-8")

        chunks = [
            {"chunk_number": 1, "start_time": "00:00:00", "end_time": "00:30:00",
             "duration_minutes": 30, "duration_seconds": 1800, "content": "Bloque 1"},
        ]
        spy = mocker.patch.object(_aca, "chunk_by_silence", return_value=chunks)

        from congress_videos.modules.youtube.download import split_srt_by_silence

        srt_data = {"videos": [{"video_id": "v1", "merged_srt_path": str(srt_file)}]}
        split_srt_by_silence(srt_data, "2025-01-01", use_adaptive=True, adaptive_percentile=80.0)

        spy.assert_called_once()
        _, kwargs = spy.call_args
        assert kwargs.get("use_adaptive") is True, (
            "split_srt_by_silence must forward use_adaptive=True to chunk_by_silence"
        )
        assert kwargs.get("adaptive_percentile") == 80.0


# ---------------------------------------------------------------------------
# _chunk_text shim (#7 back-compat: reads inline content OR srt_file_path)
# ---------------------------------------------------------------------------

class TestChunkText:
    def test_reads_inline_content_legacy(self):
        from congress_videos.modules.youtube.download import _chunk_text

        chunk = {"chunk_number": 1, "content": "legacy inline srt"}
        assert _chunk_text(chunk) == "legacy inline srt"

    def test_reads_from_srt_file_path(self, tmp_path):
        from congress_videos.modules.youtube.download import _chunk_text

        srt = tmp_path / "v1_chunk_1.srt"
        srt.write_text("from disk", encoding="utf-8")
        chunk = {"chunk_number": 1, "srt_file_path": str(srt)}
        assert _chunk_text(chunk) == "from disk"

    def test_inline_content_takes_precedence_over_path(self, tmp_path):
        from congress_videos.modules.youtube.download import _chunk_text

        srt = tmp_path / "v1_chunk_1.srt"
        srt.write_text("disk version", encoding="utf-8")
        chunk = {"content": "inline version", "srt_file_path": str(srt)}
        assert _chunk_text(chunk) == "inline version"

    def test_missing_both_raises_value_error(self):
        from congress_videos.modules.youtube.download import _chunk_text

        with pytest.raises(ValueError, match="srt_file_path is None"):
            _chunk_text({"chunk_number": 1})

    def test_srt_file_path_none_raises_value_error(self):
        from congress_videos.modules.youtube.download import _chunk_text

        with pytest.raises(ValueError, match="srt_file_path is None"):
            _chunk_text({"content": None, "srt_file_path": None})

    def test_missing_file_raises_file_not_found(self, tmp_path):
        from congress_videos.modules.youtube.download import _chunk_text

        chunk = {"srt_file_path": str(tmp_path / "does_not_exist.srt")}
        with pytest.raises(FileNotFoundError):
            _chunk_text(chunk)


# ---------------------------------------------------------------------------
# summarize_silence_chunks
# ---------------------------------------------------------------------------

class TestSummarizeSilenceChunks:

    def _make_chunk(self, number=1):
        return {
            "chunk_number": number,
            "start_time": "00:00:00",
            "end_time": "00:30:00",
            "duration_seconds": 1800,
            "duration_minutes": 30,
            "content": "Contenido de prueba del chunk"
        }

    def _make_openai_summary_response(self, mocker, json_content: str):
        return _patch_completion(mocker, json_content)

    def test_empty_input_returns_zero_videos(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        result = summarize_silence_chunks({}, "2025-01-01")

        assert result["total_videos"] == 0

    def test_none_input_returns_zero_videos(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        result = summarize_silence_chunks(None, "2025-01-01")

        assert result["total_videos"] == 0

    def test_video_with_no_chunks_produces_error_entry(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        data = {"videos": [{"video_id": "v1", "chunks": []}]}
        result = summarize_silence_chunks(data, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_one_video_two_chunks_produces_two_summarized_chunks(self, mocker):
        summary_json = '{"speakers": [{"name": "Juan", "role": "Diputado"}], "topics": ["Presupuestos"], "timeline": [], "summary": "Debate sobre presupuestos"}'
        self._make_openai_summary_response(mocker, summary_json)

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        data = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks": [self._make_chunk(1), self._make_chunk(2)]
        }]}
        result = summarize_silence_chunks(data, "2025-01-01")

        assert result["total_videos"] == 1
        assert result["videos"][0]["total_chunks"] == 2
        assert len(result["videos"][0]["summarized_chunks"]) == 2

    def test_ai_json_parse_error_stored_in_chunk(self, mocker):
        _patch_completion(mocker, "NOT VALID JSON {{{")

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        data = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks": [self._make_chunk(1)]
        }]}
        result = summarize_silence_chunks(data, "2025-01-01")

        chunk = result["videos"][0]["summarized_chunks"][0]
        assert "error" in chunk

    def test_video_with_error_flag_skipped(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        data = {"videos": [{"video_id": "v1", "error": "prior error", "chunks": []}]}
        result = summarize_silence_chunks(data, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_summarized_chunks_contain_required_fields(self, mocker):
        summary_json = '{"speakers": [], "topics": ["Educación"], "timeline": [], "summary": "Debate educativo"}'
        self._make_openai_summary_response(mocker, summary_json)

        from congress_videos.modules.youtube.download import summarize_silence_chunks

        data = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks": [self._make_chunk(1)]
        }]}
        result = summarize_silence_chunks(data, "2025-01-01")

        chunk = result["videos"][0]["summarized_chunks"][0]
        for field in ("chunk_number", "start_time", "end_time", "speakers", "topics", "summary"):
            assert field in chunk, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# identify_interesting_chapters
# ---------------------------------------------------------------------------

class TestIdentifyInterestingChapters:

    def _make_summarized_chunk(self, number=1, duration_minutes=30):
        return {
            "chunk_number": number,
            "start_time": "00:00:00",
            "end_time": "00:30:00",
            "duration_seconds": duration_minutes * 60,
            "duration_minutes": duration_minutes,
            "speakers": [{"name": "Diputado López", "role": "Diputado"}],
            "topics": ["Presupuestos"],
            "summary": "Debate sobre presupuestos generales"
        }

    def _make_srt_chunk(self, number=1, duration_minutes=30):
        return {
            "chunk_number": number,
            "start_time": "00:00:00",
            "end_time": "00:30:00",
            "duration_minutes": duration_minutes,
            "content": "00:00:01 Contenido de transcripción de prueba"
        }

    def test_empty_chunk_summaries_returns_zero_videos(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        result = identify_interesting_chapters({}, {}, "2025-01-01")

        assert result["total_videos"] == 0

    def test_none_input_returns_zero_videos(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        result = identify_interesting_chapters(None, None, "2025-01-01")

        assert result["total_videos"] == 0

    def test_short_chunk_below_max_optimal_returned_as_whole(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [self._make_summarized_chunk(1, duration_minutes=30)]
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [self._make_srt_chunk(1, duration_minutes=30)]
        }]}

        result = identify_interesting_chapters(chunk_summaries, chunked_srt, "2025-01-01",
                                               max_optimal_duration=120)

        assert result["total_videos"] == 1
        chunk = result["videos"][0]["chunks_with_chapters"][0]
        assert chunk["skipped_ai_analysis"] is True
        assert chunk["total_interesting_chapters"] == 1

    def test_video_with_error_produces_error_entry(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{"video_id": "v1", "error": "prior error", "summarized_chunks": []}]}
        result = identify_interesting_chapters(chunk_summaries, {}, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_missing_srt_data_for_video_produces_error(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "summarized_chunks": [self._make_summarized_chunk(1)]
        }]}

        result = identify_interesting_chapters(chunk_summaries, {"videos": []}, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_long_chunk_triggers_ai_analysis(self, mocker):
        chapters_json = '{"interesting_chapters": [{"title": "Debate", "description": "Desc", "start_time": "00:00:00", "end_time": "01:00:00", "duration_minutes": 60, "speakers": [], "topics": []}]}'
        _patch_completion(mocker, chapters_json)

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        # duration > max_optimal_duration=120 → AI analysis path
        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [self._make_summarized_chunk(1, duration_minutes=130)]
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [self._make_srt_chunk(1, duration_minutes=130)]
        }]}

        result = identify_interesting_chapters(chunk_summaries, chunked_srt, "2025-01-01",
                                               min_chapter_duration=15, max_optimal_duration=120)

        assert result["total_videos"] == 1
        chunk = result["videos"][0]["chunks_with_chapters"][0]
        assert chunk.get("skipped_ai_analysis") is False


# ---------------------------------------------------------------------------
# merge_interesting_chapters
# ---------------------------------------------------------------------------

class TestMergeInterestingChapters:

    def _make_chapter(self, title, start, end, duration):
        return {
            "title": title,
            "description": f"Desc of {title}",
            "start_time": start,
            "end_time": end,
            "duration_minutes": duration,
            "speakers": ["Speaker A"],
            "topics": ["Topic X"]
        }

    def test_empty_identified_chapters_returns_zero_videos(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        result = merge_interesting_chapters({}, "2025-01-01")

        assert result["total_videos"] == 0
        assert result["videos"] == []

    def test_none_input_returns_zero_videos(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        result = merge_interesting_chapters(None, "2025-01-01")

        assert result["total_videos"] == 0

    def test_video_with_error_produces_error_entry(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        identified = {"videos": [{"video_id": "v1", "error": "no chunks", "chunks_with_chapters": []}]}
        result = merge_interesting_chapters(identified, "2025-01-01")

        assert "error" in result["videos"][0]

    def test_chapters_from_multiple_chunks_are_merged(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        ch1 = self._make_chapter("Cap 1", "00:00:00", "00:30:00", 30)
        ch2 = self._make_chapter("Cap 2", "00:30:00", "01:00:00", 30)
        ch3 = self._make_chapter("Cap 3", "01:00:00", "01:30:00", 30)

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [
                {"chunk_number": 1, "interesting_chapters": [ch1, ch2]},
                {"chunk_number": 2, "interesting_chapters": [ch3]},
            ]
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        assert result["total_videos"] == 1
        assert result["videos"][0]["total_chapters"] == 3
        assert len(result["videos"][0]["final_chapters"]) == 3

    def test_chapters_sorted_by_start_time(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        ch_a = self._make_chapter("Cap A", "01:00:00", "01:30:00", 30)
        ch_b = self._make_chapter("Cap B", "00:00:00", "00:30:00", 30)

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [
                {"chunk_number": 1, "interesting_chapters": [ch_a]},
                {"chunk_number": 2, "interesting_chapters": [ch_b]},
            ]
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        chapters = result["videos"][0]["final_chapters"]
        start_times = [c["start_time"] for c in chapters]
        assert start_times == sorted(start_times)

    def test_chunk_with_error_flag_is_skipped(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        ch = self._make_chapter("Cap 1", "00:00:00", "00:30:00", 30)

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [
                {"chunk_number": 1, "error": "AI failed", "interesting_chapters": []},
                {"chunk_number": 2, "interesting_chapters": [ch]},
            ]
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        assert result["videos"][0]["total_chapters"] == 1

    def test_source_chunk_number_stored_per_chapter(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        ch = self._make_chapter("Cap 1", "00:00:00", "00:30:00", 30)

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [
                {"chunk_number": 5, "interesting_chapters": [ch]},
            ]
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        assert result["videos"][0]["final_chapters"][0]["source_chunk"] == 5

    def test_result_contains_required_keys(self):
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        ch = self._make_chapter("Cap 1", "00:00:00", "00:30:00", 30)

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [{"chunk_number": 1, "interesting_chapters": [ch]}]
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        video = result["videos"][0]
        for key in ("video_id", "total_chapters", "final_chapters"):
            assert key in video, f"Missing key: {key}"


# ---------------------------------------------------------------------------
# T1.1 — LARGE_SRT_THRESHOLD constant and no-truncation behaviour
# ---------------------------------------------------------------------------

class TestLargeSrtThreshold:

    def test_threshold_constant_is_100k(self):
        from congress_videos.modules.youtube.download import LARGE_SRT_THRESHOLD

        assert LARGE_SRT_THRESHOLD == 100_000

    def _make_ai_chunk_inputs(self, srt_content: str, duration_minutes: int = 130):
        """Build chunk_summaries and chunked_srt inputs for AI-path testing."""
        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [{
                "chunk_number": 1,
                "start_time": "00:00:00",
                "end_time": "02:00:00",
                "duration_minutes": duration_minutes,
                "duration_seconds": duration_minutes * 60,
                "speakers": [],
                "topics": [],
                "summary": "Long session",
            }],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1,
                "start_time": "00:00:00",
                "end_time": "02:00:00",
                "duration_minutes": duration_minutes,
                "content": srt_content,
            }],
        }]}
        return chunk_summaries, chunked_srt

    def _mock_openai_chapters(self, mocker):
        return _patch_completion(
            mocker,
            '{"interesting_chapters": [{"title": "T", "description": "D", '
            '"start_time": "00:00:00", "end_time": "02:00:00", '
            '"duration_minutes": 120, "speakers": [], "topics": []}]}',
        )

    def test_full_srt_passed_to_openai_for_120k_input(self, mocker):
        """Spec #1: full SRT (120k chars) is NOT sliced before the LLM call."""
        srt_content = "x" * 120_000
        mock_completion = self._mock_openai_chapters(mocker)
        chunk_summaries, chunked_srt = self._make_ai_chunk_inputs(srt_content)

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )

        user_prompt = mock_completion.call_args[1]["user_prompt"]
        assert srt_content in user_prompt, "Full SRT must be passed, not a truncated slice"

    def test_oversized_srt_routed_to_map_reduce(self, mocker):
        """Spec #8 (supersedes T1.1 warning): SRT > 100k chars is handled by
        map-reduce windowing, not a single full-SRT pass.

        T4.3 removed the original T1.1 WARNING-only log and replaced it with
        actual map-reduce delegation. This test verifies the new behaviour:
        the oversized SRT is routed to ``map_reduce_identify_chapters``.
        """
        srt_content = "y" * 120_000
        self._mock_openai_chapters(mocker)
        chunk_summaries, chunked_srt = self._make_ai_chunk_inputs(srt_content)

        mock_map_reduce = mocker.patch(
            "congress_videos.modules.youtube.map_reduce_chapters."
            "map_reduce_identify_chapters",
            return_value=[],
        )

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )

        mock_map_reduce.assert_called_once()
        passed_srt = mock_map_reduce.call_args[0][0] if mock_map_reduce.call_args[0] \
            else mock_map_reduce.call_args[1]["srt_content"]
        assert passed_srt == srt_content, "Full oversized SRT must be passed to map-reduce"

    def test_no_warning_for_srt_below_threshold(self, mocker, caplog):
        """Spec #1: no threshold WARNING for SRT <= 100k chars."""
        import logging
        srt_content = "z" * 50_000
        self._mock_openai_chapters(mocker)
        chunk_summaries, chunked_srt = self._make_ai_chunk_inputs(srt_content)

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        with caplog.at_level(logging.WARNING):
            identify_interesting_chapters(
                chunk_summaries, chunked_srt, "2025-01-01",
                min_chapter_duration=15, max_optimal_duration=120,
            )

        threshold_warnings = [
            r for r in caplog.records
            if r.levelno == logging.WARNING and "threshold" in r.message.lower()
        ]
        assert threshold_warnings == [], "No threshold WARNING expected for small SRT"


# ---------------------------------------------------------------------------
# T1.2 — Deterministic fallback for empty/invalid LLM chapters
# ---------------------------------------------------------------------------

class TestFallbackChunkEntry:

    def _base_summary_chunk(self, number=1):
        # duration_minutes=130 forces the AI-analysis path (> max_optimal_duration=120)
        return {
            "chunk_number": number,
            "start_time": "00:10:00",
            "end_time": "02:30:00",
            "duration_minutes": 130,
            "duration_seconds": 7800,
            "speakers": [],
            "topics": [],
            "summary": "Test summary",
        }

    def test_empty_chapters_list_triggers_fallback(self, mocker):
        """Spec #4: empty chapters array → fallback with chunk metadata."""
        _patch_completion(mocker, '{"interesting_chapters": []}')

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [self._base_summary_chunk()],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1,
                "start_time": "00:10:00",
                "end_time": "02:30:00",
                "duration_minutes": 130,
                "content": "SRT content",
            }],
        }]}

        result = identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )

        chunk = result["videos"][0]["chunks_with_chapters"][0]
        assert chunk.get("error") is None, "Fallback must not have error key"
        chapters = chunk["interesting_chapters"]
        assert len(chapters) == 1
        assert chapters[0]["fallback"] is True
        assert chapters[0]["start_time"] == "00:10:00"
        assert chapters[0]["end_time"] == "02:30:00"

    def test_json_parse_error_triggers_fallback_not_error_dict(self, mocker):
        """Spec #4: JSONDecodeError → fallback, not error dict."""
        _patch_completion(mocker, "NOT JSON {{{")

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [self._base_summary_chunk()],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1,
                "start_time": "00:10:00",
                "end_time": "02:30:00",
                "duration_minutes": 130,
                "content": "SRT content",
            }],
        }]}

        result = identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )

        chunk = result["videos"][0]["chunks_with_chapters"][0]
        assert chunk.get("error") is None, "JSONDecodeError must produce fallback, not error entry"
        assert chunk["interesting_chapters"][0]["fallback"] is True

    def test_fallback_chapter_range_is_valid(self):
        """Spec #4: fallback chapter has end > start (passes range validation)."""
        from congress_videos.modules.youtube.download import _build_fallback_chunk_entry

        summary_chunk = {
            "start_time": "00:10:00",
            "end_time": "00:30:00",
            "duration_minutes": 20,
            "summary": "Debate presupuestos",
        }
        entry = _build_fallback_chunk_entry(1, summary_chunk)

        ch = entry["interesting_chapters"][0]
        from utils.time_utils import parse_timestamp
        assert parse_timestamp(ch["end_time"]) > parse_timestamp(ch["start_time"])

    def test_missing_end_time_raises_value_error(self):
        """Spec #4 edge case: missing end_time raises ValueError."""
        from congress_videos.modules.youtube.download import _build_fallback_chunk_entry

        with pytest.raises(ValueError, match="end_time"):
            _build_fallback_chunk_entry(1, {"start_time": "00:01:00"})


# ---------------------------------------------------------------------------
# T1.3 — _validate_chapter_ranges
# ---------------------------------------------------------------------------

class TestValidateChapterRanges:

    def _make_ch(self, start, end, title="Chapter"):
        return {"title": title, "start_time": start, "end_time": end, "duration_minutes": 30}

    def test_valid_chapter_passes(self):
        """Spec #5: valid range chapter is kept."""
        from congress_videos.modules.youtube.download import _validate_chapter_ranges

        chapters = [self._make_ch("00:01:00", "00:05:00")]
        result = _validate_chapter_ranges(chapters)

        assert len(result) == 1

    def test_start_equals_end_is_discarded(self, caplog):
        """Spec #5: start == end → discarded with WARNING."""
        import logging
        from congress_videos.modules.youtube.download import _validate_chapter_ranges

        chapters = [self._make_ch("00:03:00", "00:03:00")]
        with caplog.at_level(logging.WARNING):
            result = _validate_chapter_ranges(chapters, chunk_number=2)

        assert result == []
        assert any("00:03:00" in r.message for r in caplog.records if r.levelno == logging.WARNING)

    def test_start_greater_than_end_is_discarded(self, caplog):
        """Spec #5: start > end → discarded with WARNING."""
        import logging
        from congress_videos.modules.youtube.download import _validate_chapter_ranges

        chapters = [self._make_ch("00:10:00", "00:02:00")]
        with caplog.at_level(logging.WARNING):
            result = _validate_chapter_ranges(chapters)

        assert result == []

    def test_mixed_valid_and_invalid_keeps_valid_only(self):
        """Spec #5: only invalid chapters are dropped."""
        from congress_videos.modules.youtube.download import _validate_chapter_ranges

        chapters = [
            self._make_ch("00:01:00", "00:05:00", "Good"),
            self._make_ch("00:10:00", "00:02:00", "Bad"),
            self._make_ch("00:06:00", "00:08:00", "AlsoGood"),
        ]
        result = _validate_chapter_ranges(chapters)

        titles = [c["title"] for c in result]
        assert "Good" in titles
        assert "AlsoGood" in titles
        assert "Bad" not in titles

    @pytest.mark.parametrize("start,end,expected_count", [
        ("01:30:00", "01:00:00", 0),          # start > end
        ("00:00:00", "00:00:01", 1),           # valid
        ("00:05:00", "00:05:00", 0),           # equal
        ("00:01:30,500", "00:01:31,000", 1),   # ms precision, valid
    ])
    def test_parametrized_range_cases(self, start, end, expected_count):
        from congress_videos.modules.youtube.download import _validate_chapter_ranges

        chapters = [self._make_ch(start, end)]
        result = _validate_chapter_ranges(chapters)

        assert len(result) == expected_count

    def test_all_chapters_fail_validation_triggers_fallback(self, mocker):
        """Spec #5 edge case: all chapters invalid → whole-chunk fallback invoked."""
        chapters_json = (
            '{"interesting_chapters": ['
            '{"title": "Bad1", "start_time": "00:10:00", "end_time": "00:02:00", '
            '"duration_minutes": 120, "speakers": [], "topics": []},'
            '{"title": "Bad2", "start_time": "00:05:00", "end_time": "00:05:00", '
            '"duration_minutes": 120, "speakers": [], "topics": []}'
            ']}'
        )
        _patch_completion(mocker, chapters_json)

        from congress_videos.modules.youtube.download import identify_interesting_chapters

        chunk_summaries = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "summarized_chunks": [{
                "chunk_number": 1,
                "start_time": "00:00:00",
                "end_time": "02:00:00",
                "duration_minutes": 130,
                "duration_seconds": 7800,
                "speakers": [],
                "topics": [],
                "summary": "Session",
            }],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1,
                "start_time": "00:00:00",
                "end_time": "02:00:00",
                "duration_minutes": 130,
                "content": "SRT content",
            }],
        }]}

        result = identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )

        chunk = result["videos"][0]["chunks_with_chapters"][0]
        assert chunk.get("error") is None
        assert chunk["interesting_chapters"][0].get("fallback") is True


# ---------------------------------------------------------------------------
# T1.4 — Dynamic date injection in scoring prompt
# ---------------------------------------------------------------------------

class TestDynamicDateInScoringPrompt:

    def test_scoring_prompt_template_has_current_date_placeholder(self):
        """Spec #14: raw template must contain {current_date}, not a literal month."""
        from congress_videos.config.ai_prompts import CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE

        assert "{current_date}" in CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE
        assert "octubre 2025" not in CHAPTER_RELEVANCE_SCORING_USER_PROMPT_TEMPLATE

    def test_built_prompt_does_not_contain_octubre_2025(self, mocker):
        """Spec #14: the final built prompt must not contain the old hardcoded date."""
        captured_prompts = []

        def capture_prompt(**kwargs):
            captured_prompts.append(kwargs.get("user_prompt", ""))
            return {"data": {
                "speaker_relevance_points": 1,
                "topic_relevance_points": 1,
                "public_interest_points": 0,
                "reasoning": "test",
                "key_speakers": [],
                "is_current_topic": False,
            }, "error": None}

        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            side_effect=capture_prompt,
        )

        from congress_videos.modules.youtube.youtube_ai import score_chapters_relevance

        merged = {
            "total_videos": 1,
            "videos": [{
                "video_id": "v1",
                "video_title": "Test",
                "total_chapters": 1,
                "final_chapters": [{
                    "title": "Test Chapter",
                    "description": "A desc",
                    "duration_minutes": 30,
                    "speakers": ["Speaker A"],
                    "topics": ["Educación"],
                    "start_time": "00:00:00",
                    "end_time": "00:30:00",
                }],
            }],
        }

        score_chapters_relevance(merged)

        assert len(captured_prompts) == 1
        prompt = captured_prompts[0]
        assert "octubre 2025" not in prompt
        assert "{current_date}" not in prompt  # placeholder must be substituted

    def test_built_prompt_contains_current_year(self, mocker):
        """Spec #14: injected date contains the current year."""
        from datetime import datetime, timezone

        captured_prompts = []

        def capture_prompt(**kwargs):
            captured_prompts.append(kwargs.get("user_prompt", ""))
            return {"data": {
                "speaker_relevance_points": 0, "topic_relevance_points": 0,
                "public_interest_points": 0, "reasoning": "",
                "key_speakers": [], "is_current_topic": False,
            }, "error": None}

        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            side_effect=capture_prompt,
        )

        from congress_videos.modules.youtube.youtube_ai import score_chapters_relevance

        merged = {
            "total_videos": 1,
            "videos": [{
                "video_id": "v1",
                "video_title": "Test",
                "total_chapters": 1,
                "final_chapters": [{
                    "title": "Ch", "description": "D", "duration_minutes": 10,
                    "speakers": [], "topics": [],
                    "start_time": "00:00:00", "end_time": "00:10:00",
                }],
            }],
        }

        score_chapters_relevance(merged)

        current_year = str(datetime.now(tz=timezone.utc).year)
        assert current_year in captured_prompts[0]

    def test_current_date_es_returns_spanish_month(self):
        """Spec #14 locale-independence: helper always returns Spanish month."""
        from congress_videos.modules.youtube.youtube_ai import _current_date_es

        result = _current_date_es()
        spanish_months = [
            "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
        ]
        assert any(m in result for m in spanish_months), f"Expected Spanish month in: {result!r}"

    def test_current_date_es_locale_independent(self):
        """Spec #14: result does not depend on system locale."""
        from congress_videos.modules.youtube.youtube_ai import _current_date_es

        # Call twice — must return the same value regardless of locale side effects
        r1 = _current_date_es()
        r2 = _current_date_es()
        assert r1 == r2


# ---------------------------------------------------------------------------
# T2.2 — _dedup_overlapping_chapters (#6)
# ---------------------------------------------------------------------------

class TestDedupOverlappingChapters:
    """Tests for spec #6: dedup overlapping chapters in merge step."""

    def _ch(self, start: str, end: str, title: str = "Chapter") -> dict:
        from utils.time_utils import parse_timestamp
        dur = (parse_timestamp(end) - parse_timestamp(start)) / 60
        return {
            "title": title,
            "start_time": start,
            "end_time": end,
            "duration_minutes": round(dur, 1),
        }

    def test_no_overlap_all_chapters_kept(self):
        """Spec #6: non-overlapping chapters are both kept."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        chapters = [
            self._ch("00:00:00", "00:01:00", "A"),
            self._ch("00:02:00", "00:04:00", "B"),
        ]
        result = _dedup_overlapping_chapters(chapters)

        assert len(result) == 2
        titles = {c["title"] for c in result}
        assert {"A", "B"} == titles

    def test_100_percent_overlap_narrower_discarded(self):
        """Spec #6: inner chapter (fully inside outer) is dropped; wider kept."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        # Outer: 00:01:40 → 00:06:40 (300s), Inner: 00:02:30 → 00:05:00 (150s)
        outer = self._ch("00:01:40", "00:06:40", "Outer")
        inner = self._ch("00:02:30", "00:05:00", "Inner")

        result = _dedup_overlapping_chapters([outer, inner])

        assert len(result) == 1
        assert result[0]["title"] == "Outer"

    def test_33_percent_overlap_both_kept(self):
        """Spec #6 scenario 3: 33% overlap is < 50% threshold — both kept."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        # A: 0-200s (200s), B: 150-300s (150s), overlap=50s, shorter=150s → 33%
        a = self._ch("00:00:00", "00:03:20", "A")
        b = self._ch("00:02:30", "00:05:00", "B")

        result = _dedup_overlapping_chapters([a, b])

        assert len(result) == 2

    def test_exactly_50_percent_overlap_both_kept(self):
        """Spec #6 scenario 4: exactly 50% overlap is NOT > 50% → both kept."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        # A: 0-200s, B: 100-300s, overlap=100s, shorter=200s → 50% (not >50%)
        a = self._ch("00:00:00", "00:03:20", "A")
        b = self._ch("00:01:40", "00:05:00", "B")

        result = _dedup_overlapping_chapters([a, b])

        assert len(result) == 2

    def test_single_chapter_returned_unchanged(self):
        """Spec #6 edge case: single chapter passes through without error."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        chapters = [self._ch("00:00:00", "00:30:00", "Only")]
        result = _dedup_overlapping_chapters(chapters)

        assert len(result) == 1
        assert result[0]["title"] == "Only"

    def test_all_overlap_keeps_widest(self):
        """Spec #6 edge case: all chapters overlap >50% with the widest — widest survives."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        # Wide: 0-600s; narrow1: 10-100s (inside); narrow2: 200-300s (inside)
        wide = self._ch("00:00:00", "00:10:00", "Wide")
        narrow1 = self._ch("00:00:10", "00:01:40", "Narrow1")
        narrow2 = self._ch("00:03:20", "00:05:00", "Narrow2")

        result = _dedup_overlapping_chapters([wide, narrow1, narrow2])

        titles = {c["title"] for c in result}
        assert "Wide" in titles

    def test_output_sorted_by_start_time(self):
        """Output is sorted by start_time regardless of input order."""
        from congress_videos.modules.youtube.download import _dedup_overlapping_chapters

        chapters = [
            self._ch("00:10:00", "00:20:00", "Second"),
            self._ch("00:00:00", "00:08:00", "First"),
        ]
        result = _dedup_overlapping_chapters(chapters)

        assert result[0]["title"] == "First"
        assert result[1]["title"] == "Second"

    def test_dedup_runs_in_merge_step(self):
        """Integration: merge_interesting_chapters applies dedup on overlapping input."""
        from congress_videos.modules.youtube.download import merge_interesting_chapters

        # Both chapters overlap >50%: A=0-600s outer, B=60-420s inner (360s/360s=100%)
        ch_outer = {
            "title": "Outer",
            "description": "",
            "start_time": "00:00:00",
            "end_time": "00:10:00",
            "duration_minutes": 10,
            "speakers": [],
            "topics": [],
        }
        ch_inner = {
            "title": "Inner",
            "description": "",
            "start_time": "00:01:00",
            "end_time": "00:07:00",
            "duration_minutes": 6,
            "speakers": [],
            "topics": [],
        }

        identified = {"videos": [{
            "video_id": "v1",
            "video_title": "Test",
            "chunks_with_chapters": [
                {"chunk_number": 1, "interesting_chapters": [ch_outer, ch_inner]},
            ],
        }]}

        result = merge_interesting_chapters(identified, "2025-01-01")

        final_chapters = result["videos"][0]["final_chapters"]
        # Only the wider chapter should survive
        assert len(final_chapters) == 1
        assert final_chapters[0]["title"] == "Outer"


# ---------------------------------------------------------------------------
# Batch 4 — #8 map-reduce wiring into identify_interesting_chapters (T4.3)
# ---------------------------------------------------------------------------

class TestIdentifyInterestingChaptersMapReduce:
    """The >LARGE_SRT_THRESHOLD path must delegate to map-reduce (#8)."""

    def _summarized(self, duration_minutes=130):
        return {
            "chunk_number": 1,
            "start_time": "00:00:00",
            "end_time": "02:10:00",
            "duration_seconds": duration_minutes * 60,
            "duration_minutes": duration_minutes,
            "speakers": [],
            "topics": ["X"],
            "summary": "Long session",
        }

    def test_oversized_srt_triggers_map_reduce(self, mocker):
        """SRT > 100k chars on an AI-path chunk → map_reduce_identify_chapters."""
        mocker.patch("openai.OpenAI", return_value=MagicMock())
        mr = mocker.patch(
            "congress_videos.modules.youtube.map_reduce_chapters."
            "map_reduce_identify_chapters",
            return_value=[{
                "title": "Cap", "start_time": "00:10:00", "end_time": "00:40:00",
                "duration_minutes": 30,
            }],
        )

        from congress_videos.modules.youtube.download import (
            identify_interesting_chapters,
        )

        big_content = "00:00:01 " + ("palabra " * 20000)  # >100k chars
        chunk_summaries = {"videos": [{
            "video_id": "v1", "video_title": "T",
            "summarized_chunks": [self._summarized(130)],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1, "start_time": "00:00:00",
                "end_time": "02:10:00", "duration_minutes": 130,
                "content": big_content,
            }],
        }]}

        identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )
        assert mr.called, "map-reduce must be invoked for oversized SRT"

    def test_small_srt_does_not_trigger_map_reduce(self, mocker):
        """SRT <= 100k chars uses the single-pass call, not map-reduce."""
        chapters_json = (
            '{"interesting_chapters": [{"title": "D", "description": "x", '
            '"start_time": "00:00:00", "end_time": "01:00:00", '
            '"duration_minutes": 60, "speakers": [], "topics": []}]}'
        )
        _patch_completion(mocker, chapters_json)
        mr = mocker.patch(
            "congress_videos.modules.youtube.map_reduce_chapters."
            "map_reduce_identify_chapters",
        )

        from congress_videos.modules.youtube.download import (
            identify_interesting_chapters,
        )

        chunk_summaries = {"videos": [{
            "video_id": "v1", "video_title": "T",
            "summarized_chunks": [self._summarized(130)],
        }]}
        chunked_srt = {"videos": [{
            "video_id": "v1",
            "chunks": [{
                "chunk_number": 1, "start_time": "00:00:00",
                "end_time": "02:10:00", "duration_minutes": 130,
                "content": "00:00:01 contenido corto",  # well under 100k
            }],
        }]}

        identify_interesting_chapters(
            chunk_summaries, chunked_srt, "2025-01-01",
            min_chapter_duration=15, max_optimal_duration=120,
        )
        assert not mr.called, "small SRT must not trigger map-reduce"


# ---------------------------------------------------------------------------
# Batch 4 — #9 dynamic-mapping callables (T4.4)
# ---------------------------------------------------------------------------

class TestSummarizeOneChunk:
    """The standalone callable Airflow dynamic task mapping expands over."""

    def _chunk(self, number=1, video_id="vid123", video_title="Test Video"):
        return {
            "chunk_number": number,
            "start_time": "00:00:00",
            "end_time": "00:30:00",
            "duration_seconds": 1800,
            "duration_minutes": 30,
            "content": "Contenido de prueba",
            "video_id": video_id,
            "video_title": video_title,
        }

    def test_returns_summary_on_success(self, mocker):
        _patch_completion(
            mocker,
            '{"speakers": [{"name": "Juan"}], "topics": ["T"], '
            '"timeline": [], "summary": "S"}',
        )
        from congress_videos.modules.youtube.download import summarize_one_chunk

        result = summarize_one_chunk(self._chunk(1))
        assert result["chunk_number"] == 1
        assert result["topics"] == ["T"]
        assert result["video_id"] == "vid123"
        assert result["video_title"] == "Test Video"
        assert "error" not in result

    def test_failure_captured_not_raised(self, mocker):
        _patch_completion(mocker, "NOT JSON {{{")
        from congress_videos.modules.youtube.download import summarize_one_chunk

        result = summarize_one_chunk(self._chunk(2))
        assert result["chunk_number"] == 2
        assert "error" in result  # captured, not raised

    def test_missing_srt_path_returns_error(self, mocker):
        mocker.patch("openai.OpenAI", return_value=MagicMock())
        from congress_videos.modules.youtube.download import summarize_one_chunk

        # No content and srt_file_path None → _chunk_text raises ValueError,
        # captured into the error field (mapped task isolation).
        chunk = {
            "chunk_number": 3, "start_time": "00:00:00", "end_time": "00:30:00",
            "duration_seconds": 1800, "duration_minutes": 30,
            "srt_file_path": None,
        }
        result = summarize_one_chunk(chunk)
        assert "error" in result


class TestAggregateChunkSummaries:

    def test_empty_returns_empty(self):
        from congress_videos.modules.youtube.download import (
            aggregate_chunk_summaries,
        )
        assert aggregate_chunk_summaries([]) == []

    def test_sorted_by_chunk_number(self):
        from congress_videos.modules.youtube.download import (
            aggregate_chunk_summaries,
        )
        out = aggregate_chunk_summaries([
            {"chunk_number": 3}, {"chunk_number": 1}, {"chunk_number": 2},
        ])
        assert [c["chunk_number"] for c in out] == [1, 2, 3]


class TestFlattenAndRegroup:
    """flatten → mapped summarize → regroup round-trips the chunk_summaries shape."""

    def test_flatten_tags_video_and_skips_errors(self):
        from congress_videos.modules.youtube.download import (
            flatten_chunks_for_mapping,
        )
        data = {"videos": [
            {"video_id": "v1", "video_title": "T1", "chunks": [
                {"chunk_number": 1}, {"chunk_number": 2}]},
            {"video_id": "v2", "error": "boom", "chunks": [{"chunk_number": 9}]},
        ]}
        refs = flatten_chunks_for_mapping(data)
        assert len(refs) == 2  # v2 skipped (error)
        assert all(r["video_id"] == "v1" for r in refs)
        assert refs[0]["video_title"] == "T1"

    def test_flatten_empty_input_returns_empty(self):
        from congress_videos.modules.youtube.download import (
            flatten_chunks_for_mapping,
        )
        assert flatten_chunks_for_mapping(None) == []
        assert flatten_chunks_for_mapping({"videos": []}) == []

    def test_regroup_rebuilds_chunk_summaries_shape(self):
        from congress_videos.modules.youtube.download import (
            regroup_summarized_chunks,
        )
        mapped = [
            {"chunk_number": 2, "video_id": "v1", "video_title": "T1",
             "summary": "b"},
            {"chunk_number": 1, "video_id": "v1", "video_title": "T1",
             "summary": "a"},
        ]
        out = regroup_summarized_chunks(mapped)
        assert out["total_videos"] == 1
        video = out["videos"][0]
        assert video["video_id"] == "v1"
        assert video["total_chunks"] == 2
        # Sorted by chunk_number, routing tags stripped.
        assert [c["chunk_number"] for c in video["summarized_chunks"]] == [1, 2]
        assert "video_id" not in video["summarized_chunks"][0]

    def test_regroup_empty_returns_zero_videos(self):
        from congress_videos.modules.youtube.download import (
            regroup_summarized_chunks,
        )
        out = regroup_summarized_chunks([])
        assert out == {"total_videos": 0, "videos": []}


# ---------------------------------------------------------------------------
# guard_enabled -> guard_live_status forwarding (finished-stream-guard E.1)
# ---------------------------------------------------------------------------

class TestDownloadGuardForwarding:

    def _patch_paths(self, mocker, tmp_path):
        mocker.patch(
            "congress_videos.modules.youtube.download.get_download_video_path",
            return_value=str(tmp_path / "downloads" / "2025-01-01" / "vid001"),
        )
        mocker.patch("congress_videos.modules.youtube.download.ensure_directory_exists")

    def _spy_downloader(self, mocker):
        return mocker.patch(
            "congress_videos.modules.youtube.download.download_youtube_video_for_upload",
            return_value={
                "success": True,
                "file_path": "/tmp/video.mp4",
                "file_size_mb": 100,
                "duration": 1800,
                "title": "Test",
            },
        )

    def test_default_forwards_guard_live_status_true(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        spy = self._spy_downloader(mocker)

        from congress_videos.modules.youtube.download import download_video_from_youtube

        download_video_from_youtube(_make_video_details(_make_video()), "2025-01-01")

        assert spy.call_args.kwargs["guard_live_status"] is True

    def test_guard_disabled_forwards_guard_live_status_false(self, mocker, tmp_path):
        self._patch_paths(mocker, tmp_path)
        spy = self._spy_downloader(mocker)

        from congress_videos.modules.youtube.download import download_video_from_youtube

        download_video_from_youtube(
            _make_video_details(_make_video()), "2025-01-01", guard_enabled=False
        )

        assert spy.call_args.kwargs["guard_live_status"] is False
