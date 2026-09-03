"""Tests for congress_videos.modules.thumbnail_prompt (TDD — RED first).

Test groups:
    T-01  build_pikzels_prompt layout A/B/C golden rules
    T-02  logo line behaviour
    T-03  error cases (unknown layout, missing keys)
    T-04  http prohibition (no 'http' in output even when input contains it)
    T-05  input immutability
"""

from __future__ import annotations

import pytest

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

_BRIEF = {
    "text": "Lo que no te cuentan",
    "background": "una calle española con manifestantes",
    "person": "un ciudadano de mediana edad con expresión indignada",
    "mood": "indignación y urgencia",
}


# ---------------------------------------------------------------------------
# T-01: Golden rules for layouts A / B / C
# ---------------------------------------------------------------------------


class TestBuildPikzelsPrompt:
    """build_pikzels_prompt must produce correct prompts for layouts A, B, C."""

    def test_layout_a_contains_gold_color(self) -> None:
        """Layout A output must contain the gold colour hex #C9A84C."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        result = build_pikzels_prompt(_BRIEF, "A")
        assert "#C9A84C" in result

    def test_layout_b_contains_gold_color(self) -> None:
        """Layout B output must contain the gold colour hex #C9A84C."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        result = build_pikzels_prompt(_BRIEF, "B")
        assert "#C9A84C" in result

    def test_layout_c_contains_gold_color(self) -> None:
        """Layout C output must contain the gold colour hex #C9A84C."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        result = build_pikzels_prompt(_BRIEF, "C")
        assert "#C9A84C" in result

    def test_text_is_uppercased(self) -> None:
        """The text value must appear uppercased in the output."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "text": "lo que no te cuentan"}
        result = build_pikzels_prompt(brief, "A")
        assert "LO QUE NO TE CUENTAN" in result

    def test_prohibitions_present_no_logos(self) -> None:
        """All layouts must include 'NO logos' prohibition."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "NO logos" in result, f"Layout {layout} missing 'NO logos'"

    def test_prohibitions_present_no_data_charts(self) -> None:
        """All layouts must include 'NO data charts' prohibition."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "NO data charts" in result, f"Layout {layout} missing 'NO data charts'"

    def test_prohibitions_present_no_government_chambers(self) -> None:
        """All layouts must include 'NO government chambers or parliamentary hemiciclo'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "NO government chambers or parliamentary hemiciclo" in result, (
                f"Layout {layout} missing government-chambers prohibition"
            )

    def test_16_9_youtube_thumbnail_present(self) -> None:
        """Output must contain '16:9 YouTube thumbnail'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "16:9 YouTube thumbnail" in result, f"Layout {layout} missing '16:9 YouTube thumbnail'"

    def test_no_http_in_output_even_when_brief_contains_http(self) -> None:
        """If any brief value contains 'http', the output must NOT contain 'http'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief_with_http = {
            "text": "LO QUE OCULTAN",
            "background": "http://example.com fondo de calle",
            "person": "un ciudadano serio",
            "mood": "tensión http://evil.com",
        }
        result = build_pikzels_prompt(brief_with_http, "A")
        assert "http" not in result, "Output must not contain 'http' even when input brief values contain URLs"


# ---------------------------------------------------------------------------
# T-02: Logo line behaviour
# ---------------------------------------------------------------------------


class TestLogoLine:
    """Logo line present when logo set; absent when empty/missing."""

    def test_logo_line_present_when_logo_set_layout_a(self) -> None:
        """When logo='RTVE', layout A output contains 'TOP-LEFT corner'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "logo": "RTVE"}
        result = build_pikzels_prompt(brief, "A")
        assert "TOP-LEFT corner" in result
        assert "RTVE" in result

    def test_logo_line_present_when_logo_set_layout_b(self) -> None:
        """When logo='COPE', layout B output contains 'TOP-RIGHT corner'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "logo": "COPE"}
        result = build_pikzels_prompt(brief, "B")
        assert "TOP-RIGHT corner" in result
        assert "COPE" in result

    def test_logo_line_present_when_logo_set_layout_c(self) -> None:
        """When logo='TVE', layout C output contains 'BOTTOM-RIGHT corner'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "logo": "TVE"}
        result = build_pikzels_prompt(brief, "C")
        assert "BOTTOM-RIGHT corner" in result
        assert "TVE" in result

    def test_logo_line_absent_when_logo_empty(self) -> None:
        """When logo='', the logo corner line must not appear."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "logo": ""}
        result = build_pikzels_prompt(brief, "A")
        assert "TOP-LEFT corner" not in result

    def test_logo_line_absent_when_logo_key_missing(self) -> None:
        """When 'logo' key is absent, the logo corner line must not appear."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {k: v for k, v in _BRIEF.items() if k != "logo"}
        result = build_pikzels_prompt(brief, "B")
        assert "TOP-RIGHT corner" not in result


# ---------------------------------------------------------------------------
# T-03: Error cases
# ---------------------------------------------------------------------------


class TestErrorCases:
    """Unknown layout raises ValueError; missing required key raises KeyError."""

    def test_unknown_layout_raises_value_error(self) -> None:
        """Layout value not in {A, B, C} must raise ValueError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        with pytest.raises(ValueError, match="layout"):
            build_pikzels_prompt(_BRIEF, "D")

    def test_lowercase_layout_raises_value_error(self) -> None:
        """Layout check is case-sensitive — 'a' must raise ValueError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        with pytest.raises(ValueError):
            build_pikzels_prompt(_BRIEF, "a")

    def test_missing_text_key_raises_key_error(self) -> None:
        """Missing 'text' in brief raises KeyError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {k: v for k, v in _BRIEF.items() if k != "text"}
        with pytest.raises(KeyError):
            build_pikzels_prompt(brief, "A")

    def test_missing_background_key_raises_key_error(self) -> None:
        """Missing 'background' in brief raises KeyError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {k: v for k, v in _BRIEF.items() if k != "background"}
        with pytest.raises(KeyError):
            build_pikzels_prompt(brief, "A")

    def test_missing_person_key_raises_key_error(self) -> None:
        """Missing 'person' in brief raises KeyError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {k: v for k, v in _BRIEF.items() if k != "person"}
        with pytest.raises(KeyError):
            build_pikzels_prompt(brief, "A")

    def test_missing_mood_key_raises_key_error(self) -> None:
        """Missing 'mood' in brief raises KeyError."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {k: v for k, v in _BRIEF.items() if k != "mood"}
        with pytest.raises(KeyError):
            build_pikzels_prompt(brief, "A")


# ---------------------------------------------------------------------------
# T-04: http prohibition
# ---------------------------------------------------------------------------


class TestHttpProhibition:
    """No 'http' in output regardless of input values."""

    def test_background_with_url_stripped_from_output(self) -> None:
        """URL in background value must not survive into the Pikzels prompt."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {
            "text": "CRISIS POLÍTICA",
            "background": "https://example.com/img una calle",
            "person": "ciudadano serio",
            "mood": "indignación",
        }
        result = build_pikzels_prompt(brief, "B")
        assert "http" not in result

    def test_person_with_url_stripped_from_output(self) -> None:
        """URL in person value must not survive into the Pikzels prompt."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {
            "text": "CRISIS POLÍTICA",
            "background": "calle española",
            "person": "http://avatar.com persona seria",
            "mood": "urgencia",
        }
        result = build_pikzels_prompt(brief, "C")
        assert "http" not in result


# ---------------------------------------------------------------------------
# T-05: Input immutability
# ---------------------------------------------------------------------------


class TestInputImmutability:
    """build_pikzels_prompt must not mutate the input dict."""

    def test_input_dict_not_mutated(self) -> None:
        """Calling build_pikzels_prompt must not alter the original brief dict."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {
            "text": "original text",
            "background": "original background",
            "person": "original person",
            "mood": "original mood",
        }
        original_copy = dict(brief)
        build_pikzels_prompt(brief, "A")

        assert brief == original_copy, "build_pikzels_prompt must not mutate the input brief dict"

    def test_text_uppercasing_does_not_mutate_input(self) -> None:
        """Text uppercasing must produce a new string, not mutate brief['text']."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        original_text = "lowercase text"
        brief = {
            "text": original_text,
            "background": "fondo",
            "person": "persona",
            "mood": "mood",
        }
        build_pikzels_prompt(brief, "B")

        assert brief["text"] == original_text, "brief['text'] must remain unchanged after build_pikzels_prompt call"


# ---------------------------------------------------------------------------
# T-06: Safe-zone constraint (REQ-1)
# ---------------------------------------------------------------------------


class TestSafeZone:
    """build_pikzels_prompt must include a bottom-right safe-zone prohibition in all layouts."""

    def test_safe_zone_line_present_layout_a(self) -> None:
        """Layout A output must contain 'SAFE ZONE'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        assert "SAFE ZONE" in build_pikzels_prompt(_BRIEF, "A")

    def test_safe_zone_line_present_layout_b(self) -> None:
        """Layout B output must contain 'SAFE ZONE'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        assert "SAFE ZONE" in build_pikzels_prompt(_BRIEF, "B")

    def test_safe_zone_line_present_layout_c(self) -> None:
        """Layout C output must contain 'SAFE ZONE'."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        assert "SAFE ZONE" in build_pikzels_prompt(_BRIEF, "C")

    def test_bottom_right_corner_substring_all_layouts(self) -> None:
        """All layouts must reference 'bottom-right corner' in the safe-zone line."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "bottom-right corner" in result, f"Layout {layout} missing 'bottom-right corner' safe-zone substring"

    def test_logo_is_exempt_substring_all_layouts(self) -> None:
        """All layouts must state 'the logo is exempt' in the safe-zone line."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        for layout in ("A", "B", "C"):
            result = build_pikzels_prompt(_BRIEF, layout)
            assert "the logo is exempt" in result, f"Layout {layout} missing 'the logo is exempt' safe-zone substring"

    def test_logo_line_and_safe_zone_coexist_layout_c(self) -> None:
        """Layout C with a logo must contain both BOTTOM-RIGHT (logo) and SAFE ZONE lines."""
        from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

        brief = {**_BRIEF, "logo": "TVE"}
        result = build_pikzels_prompt(brief, "C")
        assert "BOTTOM-RIGHT corner" in result, "Layout C with logo must still render _LOGO_LINE_C"
        assert "SAFE ZONE" in result, "Layout C with logo must also contain the safe-zone line"
