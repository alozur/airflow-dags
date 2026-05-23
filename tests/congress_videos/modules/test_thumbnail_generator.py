"""Tests for congress_videos.modules.thumbnail_generator module."""

from __future__ import annotations

from unittest.mock import MagicMock, call

import pytest


# ---------------------------------------------------------------------------
# truncate_to_complete_words
# ---------------------------------------------------------------------------

class TestTruncateToCompleteWords:
    """Pure-function tests — no mocking needed."""

    def _call(self, text, max_length):
        from congress_videos.modules.thumbnail_generator import truncate_to_complete_words
        return truncate_to_complete_words(text, max_length)

    @pytest.mark.parametrize("text,max_length,expected", [
        # Text shorter than limit → unchanged
        ("Hello world", 50, "Hello world"),
        # Truncate at word boundary — slice is "The quick brown" (15 chars),
        # rfind(' ') finds space at index 9 → truncates to "The quick"
        ("The quick brown fox jumps", 15, "The quick"),
        # Exactly at limit → unchanged
        ("ABCDE", 5, "ABCDE"),
        # Empty string → empty string
        ("", 10, ""),
        # No spaces → truncate by character (first word only)
        ("ABCDEFGHIJ", 5, "ABCDE"),
    ])
    def test_parametrized_cases(self, text, max_length, expected):
        assert self._call(text, max_length) == expected

    def test_truncates_at_last_complete_word(self):
        result = self._call("One two three four five", 14)
        # Result must be a subset of complete words and never exceed limit
        assert len(result) <= 14
        for word in result.split():
            assert word in "One two three four five".split()

    def test_result_never_exceeds_max_length(self):
        result = self._call("Palabra superlarga sinespacio", 10)
        assert len(result) <= 10

    def test_no_leading_or_trailing_spaces(self):
        result = self._call("Hello world again", 11)
        assert result == result.strip()


# ---------------------------------------------------------------------------
# generate_thumbnail_text  (mocks OpenAI)
# ---------------------------------------------------------------------------

class TestGenerateThumbnailText:

    def _make_openai_response(self, content: str):
        response = MagicMock()
        response.choices = [MagicMock()]
        response.choices[0].message.content = content
        return response

    def test_short_response_returns_success(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response("DEBATE PRESUPUESTOS")
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Test title", "Test description", max_length=40)

        assert result["success"] is True
        assert result["error"] is None
        assert "thumbnail_text" in result
        assert result["thumbnail_text"] == "DEBATE PRESUPUESTOS"

    def test_strips_surrounding_double_quotes(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response('"MOCIÓN CENSURA"')
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc", max_length=40)

        assert '"' not in result["thumbnail_text"]
        assert result["success"] is True

    def test_strips_single_quotes(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response("'VOTO DE CONFIANZA'")
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc", max_length=40)

        assert "'" not in result["thumbnail_text"]

    def test_api_error_returns_fallback_with_success_false(self, mocker):
        mocker.patch(
            "congress_videos.modules.thumbnail_generator.OpenAI",
            side_effect=Exception("API connection error")
        )

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Sesión Plenaria Extraordinaria 2025", "Desc", max_length=40)

        assert result["success"] is False
        assert result["error"] is not None
        assert result["thumbnail_text"]  # fallback is non-empty

    def test_fallback_text_is_uppercased(self, mocker):
        mocker.patch(
            "congress_videos.modules.thumbnail_generator.OpenAI",
            side_effect=RuntimeError("network timeout")
        )

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Debate Presupuestos Generales Estado", "Desc")

        assert result["thumbnail_text"] == result["thumbnail_text"].upper()

    def test_very_long_response_is_truncated_to_max_length(self, mocker):
        long_text = "TEXTO MUY LARGO QUE SUPERA CON CRECES EL LIMITE MAXIMO ESTABLECIDO"
        assert len(long_text) > 40

        fake_client = MagicMock()
        # All attempts return the long text → forces truncation path
        fake_client.chat.completions.create.return_value = self._make_openai_response(long_text)
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc", max_length=40, max_attempts=2)

        assert result["success"] is True
        assert len(result["thumbnail_text"]) <= 40

    def test_result_contains_required_keys(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response("OK TEXT")
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc")

        for key in ("thumbnail_text", "word_count", "char_count", "success", "error"):
            assert key in result, f"Missing key: {key}"

    def test_thumbnail_text_is_uppercased_on_success(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response("debate fiscal")
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc", max_length=40)

        assert result["thumbnail_text"] == result["thumbnail_text"].upper()

    def test_word_count_matches_text(self, mocker):
        fake_client = MagicMock()
        fake_client.chat.completions.create.return_value = self._make_openai_response("TRES PALABRAS MAS")
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text
        result = generate_thumbnail_text("Title", "Desc", max_length=40)

        assert result["word_count"] == len(result["thumbnail_text"].split())


# ---------------------------------------------------------------------------
# split_text_into_lines  (mocks PIL internals)
# ---------------------------------------------------------------------------

class TestSplitTextIntoLines:

    def _setup_draw_mock(self, mocker, width_fn):
        """Patch PIL so draw.textbbox uses width_fn(text) -> int for widths."""
        mock_draw = MagicMock()
        mock_draw.textbbox.side_effect = lambda pos, text, font=None: (0, 0, width_fn(text), 20)
        mocker.patch("congress_videos.modules.thumbnail_generator.Image.new", return_value=MagicMock())
        mocker.patch("congress_videos.modules.thumbnail_generator.ImageDraw.Draw", return_value=mock_draw)
        return MagicMock()  # font

    def test_single_word_returns_single_line(self, mocker):
        font = self._setup_draw_mock(mocker, lambda t: 100)

        from congress_videos.modules.thumbnail_generator import split_text_into_lines
        result = split_text_into_lines("HOLA", font, max_width=500)

        assert result == ["HOLA"]

    def test_text_fitting_max_width_is_single_line(self, mocker):
        font = self._setup_draw_mock(mocker, lambda t: 200)  # always fits

        from congress_videos.modules.thumbnail_generator import split_text_into_lines
        result = split_text_into_lines("HOLA MUNDO", font, max_width=500)

        assert len(result) == 1
        assert result[0] == "HOLA MUNDO"

    def test_wide_text_splits_into_multiple_lines(self, mocker):
        # Multi-word strings exceed max_width; single words do not
        font = self._setup_draw_mock(mocker, lambda t: 600 if " " in t else 200)

        from congress_videos.modules.thumbnail_generator import split_text_into_lines
        result = split_text_into_lines("PRIMERA SEGUNDA TERCERA", font, max_width=300)

        assert len(result) >= 2

    def test_never_returns_empty_list(self, mocker):
        font = self._setup_draw_mock(mocker, lambda t: 50)

        from congress_videos.modules.thumbnail_generator import split_text_into_lines
        result = split_text_into_lines("OK", font, max_width=200)

        assert result  # always non-empty

    def test_all_original_words_present_in_output(self, mocker):
        font = self._setup_draw_mock(mocker, lambda t: 600 if " " in t else 200)

        from congress_videos.modules.thumbnail_generator import split_text_into_lines
        original = "ALPHA BETA GAMMA DELTA"
        result = split_text_into_lines(original, font, max_width=300)

        all_words_in_output = " ".join(result).split()
        assert sorted(all_words_in_output) == sorted(original.split())


# ---------------------------------------------------------------------------
# create_thumbnail  (smoke test — all PIL/IO mocked)
# ---------------------------------------------------------------------------

class TestCreateThumbnail:

    def _patch_thumbnail_deps(self, mocker, tmp_path):
        mock_img = MagicMock()
        mock_img.size = (1280, 720)
        mock_img.convert.return_value = mock_img
        mock_img.resize.return_value = mock_img

        mocker.patch("congress_videos.modules.thumbnail_generator.Image.open", return_value=mock_img)
        mocker.patch("congress_videos.modules.thumbnail_generator.Image.new", return_value=mock_img)

        mock_enhancer = MagicMock()
        mock_enhancer.enhance.return_value = mock_img
        mocker.patch(
            "congress_videos.modules.thumbnail_generator.ImageEnhance.Brightness",
            return_value=mock_enhancer
        )

        mock_draw = MagicMock()
        mock_draw.textbbox.return_value = (0, 0, 100, 30)
        mocker.patch("congress_videos.modules.thumbnail_generator.ImageDraw.Draw", return_value=mock_draw)

        mock_font = MagicMock()
        mocker.patch("congress_videos.modules.thumbnail_generator.ImageFont.truetype", return_value=mock_font)
        mocker.patch("congress_videos.modules.thumbnail_generator.ImageFont.load_default", return_value=mock_font)

        # Make font paths appear to exist so truetype branch is taken
        mocker.patch("congress_videos.modules.thumbnail_generator.os.path.exists", return_value=True)

        output = str(tmp_path / "thumbnail.png")
        return mock_img, output

    def test_returns_dict_with_success_and_error_keys(self, mocker, tmp_path):
        _, output = self._patch_thumbnail_deps(mocker, tmp_path)

        from congress_videos.modules.thumbnail_generator import create_thumbnail
        result = create_thumbnail(
            background_image_path="/fake/bg.png",
            text="DEBATE FISCAL",
            output_path=output,
            session_number=133
        )

        assert "success" in result
        assert "error" in result

    def test_success_result_contains_output_path(self, mocker, tmp_path):
        _, output = self._patch_thumbnail_deps(mocker, tmp_path)

        from congress_videos.modules.thumbnail_generator import create_thumbnail
        result = create_thumbnail(
            background_image_path="/fake/bg.png",
            text="PRESUPUESTOS",
            output_path=output,
            session_number=133
        )

        if result["success"]:
            assert result["output_path"] == output

    def test_failure_when_image_open_raises(self, mocker, tmp_path):
        mocker.patch(
            "congress_videos.modules.thumbnail_generator.Image.open",
            side_effect=OSError("file not found")
        )
        output = str(tmp_path / "thumb.png")

        from congress_videos.modules.thumbnail_generator import create_thumbnail
        result = create_thumbnail(
            background_image_path="/nonexistent/bg.png",
            text="TEXTO",
            output_path=output,
            session_number=1
        )

        assert result["success"] is False
        assert result["error"] is not None
        assert result["output_path"] is None

    def test_session_number_stored_in_successful_result(self, mocker, tmp_path):
        _, output = self._patch_thumbnail_deps(mocker, tmp_path)

        from congress_videos.modules.thumbnail_generator import create_thumbnail
        result = create_thumbnail(
            background_image_path="/fake/bg.png",
            text="MOCIÓN",
            output_path=output,
            session_number=999
        )

        if result["success"]:
            assert result["session_number"] == 999

    def test_text_stored_in_successful_result(self, mocker, tmp_path):
        _, output = self._patch_thumbnail_deps(mocker, tmp_path)

        from congress_videos.modules.thumbnail_generator import create_thumbnail
        result = create_thumbnail(
            background_image_path="/fake/bg.png",
            text="VOTO CONFIANZA",
            output_path=output,
            session_number=10
        )

        if result["success"]:
            assert result["text"] == "VOTO CONFIANZA"


# ---------------------------------------------------------------------------
# generate_thumbnail_text_for_videos
# ---------------------------------------------------------------------------

class TestGenerateThumbnailTextForVideos:

    def test_empty_queued_videos_returns_empty_results(self):
        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text_for_videos
        result = generate_thumbnail_text_for_videos([], {"topic_metadata": []})
        assert result == {"results": []}

    def test_none_metadata_returns_empty_results(self):
        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text_for_videos
        result = generate_thumbnail_text_for_videos([{"chapter_id": 1}], None)
        assert result == {"results": []}

    def test_missing_topic_metadata_key_returns_empty(self):
        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text_for_videos
        result = generate_thumbnail_text_for_videos([{"chapter_id": 1}], {})
        assert result == {"results": []}

    def test_matching_chapter_generates_text_entry(self, mocker):
        fake_client = MagicMock()
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = "DEBATE FISCAL"
        fake_client.chat.completions.create.return_value = fake_response
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text_for_videos

        queued = [{"chapter_id": 42, "video_id": 7}]
        metadata = {
            "topic_metadata": [{
                "chapter_id": 42,
                "title": {"title": "Debate sobre presupuestos"},
                "description": {"description": "Se debaten los presupuestos generales"}
            }]
        }

        result = generate_thumbnail_text_for_videos(queued, metadata)
        assert len(result["results"]) == 1
        assert result["results"][0]["chapter_id"] == 42

    def test_no_matching_chapter_produces_no_result(self, mocker):
        fake_client = MagicMock()
        fake_response = MagicMock()
        fake_response.choices = [MagicMock()]
        fake_response.choices[0].message.content = "TEXT"
        fake_client.chat.completions.create.return_value = fake_response
        mocker.patch("congress_videos.modules.thumbnail_generator.OpenAI", return_value=fake_client)

        from congress_videos.modules.thumbnail_generator import generate_thumbnail_text_for_videos

        queued = [{"chapter_id": 99, "video_id": 7}]
        metadata = {"topic_metadata": [{"chapter_id": 1, "title": {}, "description": {}}]}

        result = generate_thumbnail_text_for_videos(queued, metadata)
        assert result["results"] == []


# ---------------------------------------------------------------------------
# generate_video_thumbnails
# ---------------------------------------------------------------------------

class TestGenerateVideoThumbnails:

    def test_empty_queued_videos_returns_empty(self):
        from congress_videos.modules.thumbnail_generator import generate_video_thumbnails
        result = generate_video_thumbnails([], {}, {}, "/tmp/data")
        assert result == {"results": []}

    def test_none_queued_videos_returns_empty(self):
        from congress_videos.modules.thumbnail_generator import generate_video_thumbnails
        result = generate_video_thumbnails(None, {}, {}, "/tmp/data")
        assert result == {"results": []}
