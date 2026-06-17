"""Tests for congress_videos.modules.youtube.youtube_ai."""

from __future__ import annotations

from congress_videos.modules.youtube.youtube_ai import (
    build_youtube_chapters_block,
    generate_youtube_description,
    generate_youtube_title,
    score_chapters_relevance,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_merged_chapters(videos: list[dict]) -> dict:
    return {"total_videos": len(videos), "videos": videos}


def _make_video(video_id: str, chapters: list[dict]) -> dict:
    return {
        "video_id": video_id,
        "video_title": f"Title {video_id}",
        "total_chapters": len(chapters),
        "final_chapters": chapters,
    }


def _make_chapter(title: str = "Test Chapter", speakers: list | None = None,
                  topics: list | None = None, duration: float = 10.0) -> dict:
    return {
        "title": title,
        "description": "Chapter description",
        "duration_minutes": duration,
        "speakers": speakers or ["Speaker One"],
        "topics": topics or ["Education"],
        "start_time": "00:00:00,000",
        "end_time": "00:10:00,000",
    }


def _make_chat_result(content: str = "Generated content") -> dict:
    return {"content": content, "error": None}


def _make_chat_error(msg: str = "API failure") -> dict:
    return {"content": None, "error": msg}


def _make_json_result(data: dict) -> dict:
    return {"data": data, "error": None}


def _make_json_error(msg: str = "API failure") -> dict:
    return {"data": None, "error": msg}


# ---------------------------------------------------------------------------
# generate_youtube_title
# ---------------------------------------------------------------------------

class TestGenerateYoutubeTitle:
    def test_success_returns_generated_title(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result("Debate sobre Educación Nacional"),
        )
        result = generate_youtube_title("Reforma educativa", [])
        assert result["title"] == "Debate sobre Educación Nacional"
        assert result["error"] is None

    def test_success_returns_correct_character_count(self, mocker):
        title = "Título corto"
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result(title),
        )
        result = generate_youtube_title("Contenido", [])
        assert result["character_count"] == len(title)

    def test_success_within_limit_true_for_short_title(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result("Short title"),
        )
        result = generate_youtube_title("Content", [], max_length=100)
        assert result["within_limit"] is True

    def test_api_error_returns_fallback_title(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_error("Rate limit exceeded"),
        )
        result = generate_youtube_title("Some content", [])
        assert result["title"] is not None
        assert len(result["title"]) > 0
        assert result["error"] is not None

    def test_api_error_fallback_title_contains_congreso_or_gobierno(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_error("Connection error"),
        )
        result = generate_youtube_title("Some content", [])
        assert "Congreso" in result["title"] or "Gobierno" in result["title"]

    def test_title_stripped_of_surrounding_double_quotes(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result('"Quoted Title"'),
        )
        result = generate_youtube_title("Content", [])
        assert not result["title"].startswith('"')
        assert not result["title"].endswith('"')

    def test_title_truncated_when_exceeds_max_length(self, mocker):
        long_title = "A" * 200
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result(long_title),
        )
        result = generate_youtube_title("Content", [], max_length=50)
        assert result["character_count"] <= 50


# ---------------------------------------------------------------------------
# generate_youtube_description
# ---------------------------------------------------------------------------

class TestGenerateYoutubeDescription:
    def test_success_returns_description_without_error(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result("A rich description of the debate"),
        )
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.construct_session_link",
            return_value="https://www.congreso.es/link",
        )
        result = generate_youtube_description(
            "Topic content", [], {"duration_estimated": "10 minutos"}, 42
        )
        assert result["error"] is None
        assert len(result["description"]) > 0

    def test_success_result_has_all_required_keys(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_result("Some description"),
        )
        result = generate_youtube_description("Content", [], {}, 1)
        for key in ["description", "character_count", "word_count", "error"]:
            assert key in result, f"Missing key: {key}"

    def test_api_error_returns_non_empty_fallback_description(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_error("Timeout"),
        )
        result = generate_youtube_description("Content", [], {}, 1)
        assert result["description"] is not None
        assert len(result["description"]) > 0
        assert result["error"] is not None

    def test_fallback_description_includes_session_number(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.generate_chat_completion",
            return_value=_make_chat_error("Error"),
        )
        result = generate_youtube_description("Content", [], {}, 99)
        assert "99" in result["description"]


# ---------------------------------------------------------------------------
# score_chapters_relevance
# ---------------------------------------------------------------------------

class TestScoreChaptersRelevance:
    def test_none_input_returns_zero_totals(self):
        result = score_chapters_relevance(None)
        assert result["total_videos"] == 0
        assert result["total_chapters_scored"] == 0
        assert result["successful_scores"] == 0
        assert result["failed_scores"] == 0
        assert result["videos"] == []

    def test_empty_videos_list_returns_zero_totals(self):
        result = score_chapters_relevance({"videos": []})
        assert result["total_videos"] == 0
        assert result["total_chapters_scored"] == 0

    def test_single_chapter_scored_correctly(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            return_value=_make_json_result({
                "speaker_relevance_points": 2,
                "topic_relevance_points": 2,
                "public_interest_points": 1,
                "reasoning": "High-profile politicians discussing hot topic",
                "key_speakers": ["Speaker One"],
                "is_current_topic": True,
            }),
        )
        merged = _make_merged_chapters([_make_video("vid1", [_make_chapter()])])
        result = score_chapters_relevance(merged)

        assert result["total_videos"] == 1
        assert result["total_chapters_scored"] == 1
        assert result["successful_scores"] == 1
        assert result["failed_scores"] == 0

        scored_chapter = result["videos"][0]["scored_chapters"][0]
        assert scored_chapter["relevance_score"] == 5
        assert scored_chapter["speaker_relevance_points"] == 2
        assert scored_chapter["topic_relevance_points"] == 2
        assert scored_chapter["public_interest_points"] == 1
        assert scored_chapter["scoring_error"] is None

    def test_ai_error_returns_default_middle_score_of_two(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            return_value=_make_json_error("Model unavailable"),
        )
        merged = _make_merged_chapters([_make_video("vid1", [_make_chapter()])])
        result = score_chapters_relevance(merged)

        assert result["failed_scores"] == 1
        scored_chapter = result["videos"][0]["scored_chapters"][0]
        # Default fallback: speaker=1, topic=1, interest=0 -> sum=2
        assert scored_chapter["relevance_score"] == 2
        assert scored_chapter["scoring_error"] is not None

    def test_scores_clamped_to_valid_ranges(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            return_value=_make_json_result({
                "speaker_relevance_points": 10,   # exceeds max of 2
                "topic_relevance_points": -1,     # below min of 0
                "public_interest_points": 5,      # exceeds max of 1
                "reasoning": "Test",
                "key_speakers": [],
                "is_current_topic": False,
            }),
        )
        merged = _make_merged_chapters([_make_video("vid1", [_make_chapter()])])
        result = score_chapters_relevance(merged)

        scored = result["videos"][0]["scored_chapters"][0]
        assert 0 <= scored["speaker_relevance_points"] <= 2
        assert 0 <= scored["topic_relevance_points"] <= 2
        assert 0 <= scored["public_interest_points"] <= 1

    def test_chapters_sorted_by_relevance_score_descending(self, mocker):
        call_count = 0

        def side_effect(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return _make_json_result({
                    "speaker_relevance_points": 2,
                    "topic_relevance_points": 2,
                    "public_interest_points": 1,
                    "reasoning": "High",
                    "key_speakers": [],
                    "is_current_topic": True,
                })
            return _make_json_result({
                "speaker_relevance_points": 0,
                "topic_relevance_points": 1,
                "public_interest_points": 0,
                "reasoning": "Low",
                "key_speakers": [],
                "is_current_topic": False,
            })

        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            side_effect=side_effect,
        )

        chapters = [_make_chapter("Chapter A"), _make_chapter("Chapter B")]
        merged = _make_merged_chapters([_make_video("vid1", chapters)])
        result = score_chapters_relevance(merged)

        scores = [c["relevance_score"] for c in result["videos"][0]["scored_chapters"]]
        assert scores == sorted(scores, reverse=True)

    def test_video_with_error_field_is_skipped_from_scoring(self, mocker):
        video_with_error = {
            "video_id": "vidX",
            "video_title": "Error video",
            "error": "Download failed",
            "final_chapters": [],
        }
        merged = _make_merged_chapters([video_with_error])
        result = score_chapters_relevance(merged)

        assert result["total_chapters_scored"] == 0
        assert result["videos"][0].get("error") is not None

    def test_multiple_videos_all_chapters_scored(self, mocker):
        mocker.patch(
            "congress_videos.modules.youtube.youtube_ai.cached_json_completion",
            return_value=_make_json_result({
                "speaker_relevance_points": 1,
                "topic_relevance_points": 1,
                "public_interest_points": 0,
                "reasoning": "Mid",
                "key_speakers": [],
                "is_current_topic": False,
            }),
        )
        merged = _make_merged_chapters([
            _make_video("vid1", [_make_chapter("Ch1"), _make_chapter("Ch2")]),
            _make_video("vid2", [_make_chapter("Ch3")]),
        ])
        result = score_chapters_relevance(merged)

        assert result["total_videos"] == 2
        assert result["total_chapters_scored"] == 3
        assert result["successful_scores"] == 3


# ---------------------------------------------------------------------------
# build_youtube_chapters_block
# ---------------------------------------------------------------------------

def _make_timeline_moment(time: str, speaker: str = "", content: str = "x") -> dict:
    return {"time": time, "speaker": speaker, "content": content}


def test_build_youtube_chapters_block_rebases_to_clip_start():
    timeline = [
        _make_timeline_moment("01:10:00", "Pedro Sánchez"),
        _make_timeline_moment("01:12:30", "Alberto Núñez Feijóo"),
        _make_timeline_moment("01:15:00", "Yolanda Díaz"),
    ]
    block = build_youtube_chapters_block(timeline, "01:10:00,000")

    assert block.splitlines() == [
        "00:00 Pedro Sánchez",
        "02:30 Alberto Núñez Feijóo",
        "05:00 Yolanda Díaz",
    ]


def test_build_youtube_chapters_block_first_marker_is_zero():
    # Clip starts before the first key moment — first marker must still be 00:00.
    timeline = [
        _make_timeline_moment("00:00:30", "A"),
        _make_timeline_moment("00:01:00", "B"),
        _make_timeline_moment("00:02:00", "C"),
    ]
    block = build_youtube_chapters_block(timeline, "00:00:00")

    assert block.splitlines()[0] == "00:00 A"


def test_build_youtube_chapters_block_speaker_fallback_to_topic():
    timeline = [
        _make_timeline_moment("00:00:00", "Speaker One"),
        _make_timeline_moment("00:01:00", ""),  # no speaker -> topic
        _make_timeline_moment("00:02:00", "   "),  # blank speaker -> topic
    ]
    block = build_youtube_chapters_block(
        timeline, "00:00:00", topics=["Sanidad", "Economía", "Vivienda"]
    )

    lines = block.splitlines()
    assert lines[0] == "00:00 Speaker One"
    assert lines[1] == "01:00 Economía"
    assert lines[2] == "02:00 Vivienda"


def test_build_youtube_chapters_block_fallback_to_generic_when_no_topic():
    timeline = [
        _make_timeline_moment("00:00:00", ""),
        _make_timeline_moment("00:01:00", ""),
        _make_timeline_moment("00:02:00", ""),
    ]
    block = build_youtube_chapters_block(timeline, "00:00:00", topics=[])

    assert block.splitlines() == [
        "00:00 Intervención 1",
        "01:00 Intervención 2",
        "02:00 Intervención 3",
    ]


def test_build_youtube_chapters_block_enforces_min_spacing():
    # The 8s-apart moment is dropped (< 10s minimum chapter length).
    timeline = [
        _make_timeline_moment("00:00:00", "A"),
        _make_timeline_moment("00:00:08", "B"),  # too close -> dropped
        _make_timeline_moment("00:00:30", "C"),
        _make_timeline_moment("00:01:00", "D"),
    ]
    block = build_youtube_chapters_block(timeline, "00:00:00")

    lines = block.splitlines()
    assert "B" not in block
    assert lines == ["00:00 A", "00:30 C", "01:00 D"]


def test_build_youtube_chapters_block_below_min_chapters_returns_empty():
    timeline = [
        _make_timeline_moment("00:00:00", "A"),
        _make_timeline_moment("00:01:00", "B"),
    ]
    assert build_youtube_chapters_block(timeline, "00:00:00") == ""


def test_build_youtube_chapters_block_empty_timeline_returns_empty():
    assert build_youtube_chapters_block([], "00:00:00") == ""


def test_build_youtube_chapters_block_unparseable_start_defaults_to_zero():
    timeline = [
        _make_timeline_moment("00:00:00", "A"),
        _make_timeline_moment("00:01:00", "B"),
        _make_timeline_moment("00:02:00", "C"),
    ]
    block = build_youtube_chapters_block(timeline, "not-a-timestamp")

    assert block.splitlines()[0] == "00:00 A"


def test_build_youtube_chapters_block_drops_unparseable_moment_times():
    timeline = [
        _make_timeline_moment("00:00:00", "A"),
        _make_timeline_moment("bogus", "B"),  # dropped
        _make_timeline_moment("00:01:00", "C"),
        _make_timeline_moment("00:02:00", "D"),
    ]
    block = build_youtube_chapters_block(timeline, "00:00:00")

    assert "B" not in block
    assert block.splitlines() == ["00:00 A", "01:00 C", "02:00 D"]


def test_build_youtube_chapters_block_long_clip_uses_hour_format():
    timeline = [
        _make_timeline_moment("00:00:00", "A"),
        _make_timeline_moment("00:30:00", "B"),
        _make_timeline_moment("01:05:30", "C"),
    ]
    block = build_youtube_chapters_block(timeline, "00:00:00")

    assert block.splitlines()[2] == "1:05:30 C"
