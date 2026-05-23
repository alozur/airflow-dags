"""Tests for congress_videos.modules.youtube.download module."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

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
        fake_client = MagicMock()
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = json_content
        fake_client.chat.completions.create.return_value = fake_response
        mocker.patch("openai.OpenAI", return_value=fake_client)
        return fake_client

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
        fake_client = MagicMock()
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = "NOT VALID JSON {{{"
        fake_client.chat.completions.create.return_value = fake_response
        mocker.patch("openai.OpenAI", return_value=fake_client)

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
        fake_client = MagicMock()
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = chapters_json
        fake_client.chat.completions.create.return_value = fake_response
        mocker.patch("openai.OpenAI", return_value=fake_client)

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
