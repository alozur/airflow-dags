"""Tests for congress_videos.modules.speaker_placeholders (Task 1.1 RED).

Covers:
- PLACEHOLDER_SPEAKERS frozenset membership (casefolded exact set)
- is_placeholder: 5 spec scenarios (exact match, single-token, role-only,
  role+propername fail-open, unrecognised fail-open)
"""

from __future__ import annotations

import pytest


class TestPlaceholderSpeakersSet:
    """PLACEHOLDER_SPEAKERS is a frozenset of casefolded exact placeholder strings."""

    def test_frozenset_is_importable(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert isinstance(PLACEHOLDER_SPEAKERS, frozenset)

    def test_desconocido_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "desconocido" in PLACEHOLDER_SPEAKERS

    def test_unknown_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "unknown" in PLACEHOLDER_SPEAKERS

    def test_interviniente_no_especificado_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "interviniente no especificado" in PLACEHOLDER_SPEAKERS

    def test_no_especificado_parenthesized_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "(no especificado)" in PLACEHOLDER_SPEAKERS

    def test_no_especificado_plain_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "no especificado" in PLACEHOLDER_SPEAKERS

    def test_empty_string_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "" in PLACEHOLDER_SPEAKERS

    def test_real_name_not_in_set(self):
        from congress_videos.modules.speaker_placeholders import PLACEHOLDER_SPEAKERS

        assert "pedro sánchez" not in PLACEHOLDER_SPEAKERS
        assert "Ana Martínez" not in PLACEHOLDER_SPEAKERS


class TestIsPlaceholderExactMatch:
    """Scenario: Known placeholder exact match → is_placeholder returns True."""

    @pytest.mark.parametrize(
        "name",
        [
            "Desconocido",
            "desconocido",
            "DESCONOCIDO",
            "Unknown",
            "unknown",
            "UNKNOWN",
            "Interviniente no especificado",
            "INTERVINIENTE NO ESPECIFICADO",
            "(No especificado)",
            "(NO ESPECIFICADO)",
            "No especificado",
            "NO ESPECIFICADO",
            "",
        ],
    )
    def test_exact_placeholder_returns_true(self, name: str):
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder(name) is True, f"Expected is_placeholder({name!r}) == True"


class TestIsPlaceholderSingleToken:
    """Scenario: Single-token string with no whitespace → is_placeholder returns True."""

    @pytest.mark.parametrize(
        "name",
        [
            "Portavoz",
            "PP",
            "PSOE",
            "Diputado",
            "Diputada",
            "Senador",
            "Presidente",
            "Presidenta",
            "Ministro",
            "Ministra",
            "Secretario",
            "Vocal",
            "Interveniente",
            "Interviniente",
        ],
    )
    def test_single_token_returns_true(self, name: str):
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder(name) is True, f"Expected is_placeholder({name!r}) == True"


class TestIsPlaceholderRoleOnly:
    """Scenario: Role-keyword prefix without a proper name → returns True."""

    @pytest.mark.parametrize(
        "name",
        [
            "Presidenta",
            "Ministro",
            "Portavoz del grupo",
            "Diputado del PP",
        ],
    )
    def test_role_only_returns_true(self, name: str):
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder(name) is True, f"Expected is_placeholder({name!r}) == True"


class TestIsPlaceholderRoleWithProperName:
    """Scenario: Role prefix followed by a proper name → fail-open, returns False."""

    @pytest.mark.parametrize(
        "name",
        [
            "Presidente Sánchez",
            "Ministra González",
            "Portavoz López",
            "Diputado Martínez García",
        ],
    )
    def test_role_plus_propername_returns_false(self, name: str):
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder(name) is False, f"Expected is_placeholder({name!r}) == False"


class TestIsPlaceholderUnrecognisedFailOpen:
    """Scenario: Unrecognised shape → fail-open, returns False (real name, not filtered)."""

    @pytest.mark.parametrize(
        "name",
        [
            "Ana Martínez",
            "Pedro López",
            "Laura Gómez",
            "María José Rodríguez",
            "José Antonio Fernández",
            "Xi Jinping",
            "Joe Biden",
            "Cualquier Nombre Real",
        ],
    )
    def test_real_name_returns_false(self, name: str):
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder(name) is False, f"Expected is_placeholder({name!r}) == False"

    def test_whitespace_only_treated_as_placeholder(self):
        """Whitespace-only strings collapse to empty string → placeholder."""
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder("   ") is True

    def test_none_equivalent_empty_string_is_placeholder(self):
        """Empty string is in PLACEHOLDER_SPEAKERS."""
        from congress_videos.modules.speaker_placeholders import is_placeholder

        assert is_placeholder("") is True
