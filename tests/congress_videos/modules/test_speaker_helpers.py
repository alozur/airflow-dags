"""Tests for congress_videos.modules.speaker_helpers module."""

from __future__ import annotations

import pytest

from congress_videos.modules.speaker_helpers import format_speaker_context, format_speaker_list


# ---------------------------------------------------------------------------
# format_speaker_list
# ---------------------------------------------------------------------------

class TestFormatSpeakerList:

    def test_empty_list_returns_no_participants_message(self):
        result = format_speaker_list([])
        assert result == "No hay participantes especificados"

    def test_single_speaker_with_role(self):
        speakers = [{"speaker_name": "Ana García", "role": "Presidente"}]
        result = format_speaker_list(speakers)
        assert "Ana García" in result
        assert "Presidente" in result
        assert result == "- Ana García (Presidente)"

    def test_single_speaker_without_role_key(self):
        speakers = [{"speaker_name": "Ana García"}]
        result = format_speaker_list(speakers)
        assert result == "- Ana García"

    def test_single_speaker_with_empty_role(self):
        speakers = [{"speaker_name": "Ana García", "role": ""}]
        result = format_speaker_list(speakers)
        assert result == "- Ana García"

    def test_single_speaker_missing_name_key_uses_desconocido(self):
        speakers = [{"role": "Diputado"}]
        result = format_speaker_list(speakers)
        assert "Desconocido" in result
        assert "Diputado" in result

    def test_multiple_speakers_each_on_own_line(self):
        speakers = [
            {"speaker_name": "Ana", "role": "Presidenta"},
            {"speaker_name": "Juan", "role": "Diputado"},
        ]
        result = format_speaker_list(speakers)
        lines = result.split("\n")
        assert len(lines) == 2
        assert "Ana" in lines[0]
        assert "Juan" in lines[1]

    def test_include_role_false_omits_roles(self):
        speakers = [{"speaker_name": "Ana García", "role": "Presidenta"}]
        result = format_speaker_list(speakers, include_role=False)
        assert "Presidenta" not in result
        assert "Ana García" in result

    def test_max_speakers_limits_output(self):
        speakers = [{"speaker_name": f"Speaker{i}", "role": "Rol"} for i in range(5)]
        result = format_speaker_list(speakers, max_speakers=3)
        lines = [line for line in result.split("\n") if line.startswith("-")]
        assert len(lines) == 3

    def test_overflow_singular_one_more(self):
        # Exactly max_speakers + 1 → "1 participante más" (singular)
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(4)]
        result = format_speaker_list(speakers, max_speakers=3)
        assert "y 1 participante más" in result

    def test_overflow_plural_multiple_more(self):
        # max_speakers + 2 → "2 participantes más" (plural)
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(5)]
        result = format_speaker_list(speakers, max_speakers=3)
        assert "y 2 participantes más" in result

    def test_no_overflow_message_when_exactly_at_max(self):
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(3)]
        result = format_speaker_list(speakers, max_speakers=3)
        assert "más" not in result

    def test_overflow_message_with_default_max_10(self):
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(12)]
        result = format_speaker_list(speakers)
        assert "y 2 participantes más" in result

    @pytest.mark.parametrize("remaining,expected_word", [
        (1, "participante"),
        (2, "participantes"),
        (10, "participantes"),
    ])
    def test_overflow_singular_plural_parametrized(self, remaining: int, expected_word: str):
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(3 + remaining)]
        result = format_speaker_list(speakers, max_speakers=3)
        assert expected_word in result


# ---------------------------------------------------------------------------
# format_speaker_context
# ---------------------------------------------------------------------------

class TestFormatSpeakerContext:

    def test_empty_list_returns_empty_string(self):
        result = format_speaker_context([])
        assert result == ""

    def test_single_speaker_with_role(self):
        speakers = [{"speaker_name": "Ana García", "role": "Presidenta"}]
        result = format_speaker_context(speakers)
        assert "Ana García (Presidenta)" in result

    def test_single_speaker_without_role(self):
        speakers = [{"speaker_name": "Ana García", "role": ""}]
        result = format_speaker_context(speakers)
        assert "Ana García" in result
        assert "()" not in result

    def test_single_speaker_missing_name_key_uses_unknown(self):
        speakers = [{"role": "Diputado"}]
        result = format_speaker_context(speakers)
        assert "Unknown" in result

    def test_prefix_is_included(self):
        speakers = [{"speaker_name": "Ana", "role": "R"}]
        result = format_speaker_context(speakers, prefix="Participan")
        assert result.startswith("Participan:")

    def test_custom_prefix(self):
        speakers = [{"speaker_name": "Ana", "role": "R"}]
        result = format_speaker_context(speakers, prefix="Speakers")
        assert result.startswith("Speakers:")

    def test_max_speakers_limits_output(self):
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(5)]
        result = format_speaker_context(speakers, max_speakers=2)
        assert "S2" not in result
        assert "S3" not in result

    def test_multiple_speakers_joined_by_comma(self):
        speakers = [
            {"speaker_name": "Ana", "role": "Presidenta"},
            {"speaker_name": "Juan", "role": "Diputado"},
        ]
        result = format_speaker_context(speakers)
        assert "Ana (Presidenta), Juan (Diputado)" in result

    def test_default_max_speakers_is_3(self):
        speakers = [{"speaker_name": f"S{i}", "role": "R"} for i in range(5)]
        result = format_speaker_context(speakers)
        assert "S3" not in result
        assert "S4" not in result

    def test_speaker_without_role_no_parentheses(self):
        speakers = [{"speaker_name": "Ana"}, {"speaker_name": "Juan"}]
        result = format_speaker_context(speakers)
        assert "()" not in result
        assert "Ana, Juan" in result
