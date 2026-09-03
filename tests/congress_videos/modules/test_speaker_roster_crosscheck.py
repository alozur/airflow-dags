"""Tests for the Gate B roster cross-check pure module (issue #321).

TDD RED cycle: written before implementation.
Tests: chapter_roster_mentions() mention extraction + crosscheck_slug()
accept/reject/no_opinion classification.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# chapter_roster_mentions
# ---------------------------------------------------------------------------


class TestChapterRosterMentions:
    """chapter_roster_mentions(key_speakers, speakers) filters placeholders and dedups."""

    def test_filters_placeholder_entries(self):
        """Placeholder-only entries (e.g. 'Ministra', 'El Presidente') are dropped."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        mentions = chapter_roster_mentions(
            key_speakers=["Ministra", "Félix Bolaños García"],
            speakers=["Presidente"],
        )

        assert mentions == ["Félix Bolaños García"]

    def test_dedups_case_insensitively(self):
        """The same mention appearing in both arrays (any case) is returned once."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        mentions = chapter_roster_mentions(
            key_speakers=["Félix Bolaños García"],
            speakers=["félix bolaños garcía"],
        )

        assert mentions == ["Félix Bolaños García"]

    def test_accepts_dict_entries_with_name_key(self):
        """Entries may be dicts with a 'name' key, mirroring thumbnail_generation._real_speakers."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        mentions = chapter_roster_mentions(
            key_speakers=[{"name": "Señor Carazo"}],
            speakers=None,
        )

        assert mentions == ["Señor Carazo"]

    def test_empty_arrays_yield_empty_mentions(self):
        """Both arrays empty -> zero mentions (fail-open precondition)."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        assert chapter_roster_mentions(key_speakers=[], speakers=[]) == []

    def test_none_arrays_never_raise(self):
        """None for either array must not raise — treated as empty."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        assert chapter_roster_mentions(key_speakers=None, speakers=None) == []

    def test_malformed_entries_never_raise(self):
        """Non-str/non-dict entries (e.g. int, None) are skipped, not raised."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            chapter_roster_mentions,
        )

        mentions = chapter_roster_mentions(
            key_speakers=[123, None, "Félix Bolaños García"],
            speakers=[{"unexpected": "shape"}],
        )

        assert mentions == ["Félix Bolaños García"]


# ---------------------------------------------------------------------------
# crosscheck_slug
# ---------------------------------------------------------------------------


class TestCrosscheckSlug:
    """crosscheck_slug(display_name, mentions) -> 'accept' | 'reject' | 'no_opinion'."""

    def test_rejects_display_name_absent_from_mentions(self):
        """felix-bolanos-garcia / 'Félix Bolaños García' vs an unrelated roster -> reject."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        verdict = crosscheck_slug("Félix Bolaños García", ["Señor Carazo", "Señora Funez"])

        assert verdict == "reject"

    def test_accepts_display_name_matching_a_mention(self):
        """A legitimate speaker whose display_name shares tokens with a mention -> accept."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        verdict = crosscheck_slug("Félix Bolaños García", ["Félix Bolaños García"])

        assert verdict == "accept"

    def test_accepts_partial_name_mention(self):
        """A shortened mention (e.g. surname only) sharing a real token -> accept."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        verdict = crosscheck_slug("Félix Bolaños García", ["Bolaños"])

        assert verdict == "accept"

    def test_no_opinion_when_mentions_empty(self):
        """Empty mentions list -> no_opinion (fail-open)."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        assert crosscheck_slug("Félix Bolaños García", []) == "no_opinion"

    def test_no_opinion_when_display_name_empty(self):
        """Falsy display_name -> no_opinion (fail-open)."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        assert crosscheck_slug("", ["Señor Carazo"]) == "no_opinion"

    def test_never_raises_on_malformed_mentions(self):
        """Non-str entries inside mentions must be skipped, not raised."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        verdict = crosscheck_slug("Félix Bolaños García", [123, None, "Bolaños"])

        assert verdict == "accept"

    def test_courtesy_tokens_alone_do_not_cause_a_false_accept(self):
        """Shared courtesy tokens only (e.g. 'señor'/'de') must not satisfy the match."""
        from congress_videos.modules.speaker_roster_crosscheck import crosscheck_slug

        verdict = crosscheck_slug("Juan de la Torre", ["Señor de la Peña"])

        assert verdict == "reject"


# ---------------------------------------------------------------------------
# _mention_name_from_entry (issue #272 slice 2: extracted from
# chapter_roster_mentions to reduce cyclomatic complexity)
# ---------------------------------------------------------------------------


class TestMentionNameFromEntry:
    """_mention_name_from_entry(entry) -> real mention name, or None."""

    def test_dict_entry_with_name_key_returns_stripped_name(self):
        """A dict entry with a 'name' key returns that name, stripped."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry({"name": "  Félix Bolaños García  "}) == "Félix Bolaños García"

    def test_str_entry_returns_stripped_name(self):
        """A plain string entry returns itself, stripped."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry(" Señor Carazo ") == "Señor Carazo"

    def test_other_type_entry_returns_none(self):
        """A non-str/non-dict entry (e.g. int) returns None."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry(123) is None

    def test_none_entry_returns_none(self):
        """A None entry returns None, never raises."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry(None) is None

    def test_empty_string_entry_returns_none(self):
        """An empty (or whitespace-only) string entry returns None."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry("   ") is None

    def test_placeholder_entry_returns_none(self):
        """A known placeholder name (e.g. 'Desconocido') returns None."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry("Desconocido") is None

    def test_dict_entry_without_name_key_returns_none(self):
        """A dict entry missing the 'name' key falls back to '', which is None after strip."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _mention_name_from_entry,
        )

        assert _mention_name_from_entry({"unexpected": "shape"}) is None


# ---------------------------------------------------------------------------
# _dedupe_case_insensitive (issue #272 slice 2)
# ---------------------------------------------------------------------------


class TestDedupeCaseInsensitive:
    """_dedupe_case_insensitive(names) -> order-preserving, casefold-deduped list."""

    def test_keeps_first_casing_of_duplicate(self):
        """The first-seen casing of a case-insensitive duplicate is kept."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _dedupe_case_insensitive,
        )

        result = _dedupe_case_insensitive(["Félix Bolaños García", "félix bolaños garcía"])

        assert result == ["Félix Bolaños García"]

    def test_preserves_input_order_for_distinct_names(self):
        """Distinct names are returned in their original order, untouched."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _dedupe_case_insensitive,
        )

        result = _dedupe_case_insensitive(["Señor Carazo", "Señora Funez"])

        assert result == ["Señor Carazo", "Señora Funez"]

    def test_empty_list_returns_empty_list(self):
        """An empty input list returns an empty list."""
        from congress_videos.modules.speaker_roster_crosscheck import (
            _dedupe_case_insensitive,
        )

        assert _dedupe_case_insensitive([]) == []
