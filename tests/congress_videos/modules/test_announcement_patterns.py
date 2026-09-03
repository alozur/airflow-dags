"""Tests for congress_videos.modules.announcement_patterns.

Shared president-announcement phrase detection used by both speaker_turns
and speaker_resolution. Pure regex module: no DB, no ffmpeg, no orchestrator
imports.
"""

from __future__ import annotations

from congress_videos.modules.announcement_patterns import (
    RE_GRACIAS_SENORIA,
    RE_NAMED,
    RE_SU_SENORIA,
    has_announcement_phrase,
)


class TestHasAnnouncementPhrasePerPattern:
    def test_named_pattern_hits(self):
        """'Tiene la palabra el señor <name>' must match."""
        assert has_announcement_phrase("Tiene la palabra el señor Sánchez.") is True

    def test_su_senoria_pattern_hits(self):
        """'Tiene la palabra su señoría' must match."""
        assert has_announcement_phrase("Tiene la palabra su señoría.") is True

    def test_gracias_senoria_pattern_hits(self):
        """'Gracias, señoría' must match."""
        assert has_announcement_phrase("Gracias, señoría.") is True


class TestHasAnnouncementPhraseMiss:
    def test_unrelated_text_misses(self):
        """Ordinary transcript text with no announcement phrase must not match."""
        assert has_announcement_phrase("El Gobierno presentará el proyecto mañana.") is False

    def test_similar_but_non_matching_phrase_misses(self):
        """'Tiene la palabra el diputado' (no señor/señora) must not match."""
        assert has_announcement_phrase("Tiene la palabra el diputado Fernández.") is False


class TestHasAnnouncementPhraseToleranceAndEdgeCases:
    def test_accent_and_case_tolerant(self):
        """Accent-stripped, uppercase variant must still match (case/accent tolerant)."""
        assert has_announcement_phrase("TIENE LA PALABRA SU SENORIA") is True

    def test_none_returns_false(self):
        """None input must return False, never raise."""
        assert has_announcement_phrase(None) is False

    def test_empty_string_returns_false(self):
        """Empty string input must return False."""
        assert has_announcement_phrase("") is False


class TestExportedRegexIdentity:
    """Both consumer modules must share the exact same compiled pattern objects."""

    def test_re_named_is_same_object_as_speaker_turns(self):
        from congress_videos.modules.speaker_turns import _RE_NAMED

        assert _RE_NAMED is RE_NAMED

    def test_re_su_senoria_is_same_object_as_speaker_turns(self):
        from congress_videos.modules.speaker_turns import _RE_SU_SENORIA

        assert _RE_SU_SENORIA is RE_SU_SENORIA

    def test_re_gracias_senoria_is_same_object_as_speaker_turns(self):
        from congress_videos.modules.speaker_turns import _RE_GRACIAS_SENORIA

        assert _RE_GRACIAS_SENORIA is RE_GRACIAS_SENORIA
