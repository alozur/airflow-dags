"""Pure prompt builder for Pikzels thumbnail generation.

Converts an art-direction brief dict into a Pikzels text prompt using one of
the three layout templates from the congress-thumbnail skill (Template A/B/C).

No I/O, no external calls — pure function, safe to unit-test in isolation.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Layout templates verbatim from congress-thumbnail skill (Template A/B/C).
# Placeholders use {field} syntax; <...> → {field} substitution applied.
# ---------------------------------------------------------------------------

_TEMPLATE_A = """\
A thick gold (#C9A84C) border frames the entire image edge.

BACKGROUND: {background}.

SUBJECT (RIGHT HALF): {person}. Face fills 35-40%. Looks at camera or left toward text.

TEXT (LEFT HALF, 42% from top): Bold ALL-CAPS white (#FFFFFF) '{text}'.
Font: bold compressed sans-serif (Impact / Bebas Neue).
3-4px black (#000000) outline + drop shadow (4px down, 6px blur, 80% opacity).

{logo_line}NO logos, icons, watermarks, play buttons, channel symbols, or brand marks.
NO data charts, infographic overlays, or economic graphs.
NO government chambers or parliamentary hemiciclo.

Overall mood: {mood}.
16:9 YouTube thumbnail."""

_TEMPLATE_B = """\
A thick gold (#C9A84C) border frames the entire image edge.

BACKGROUND: {background}.

SUBJECT (LEFT HALF): {person}. Face fills 35-40%. Looks toward right (toward text).

TEXT (RIGHT HALF, 42% from top): Bold ALL-CAPS white (#FFFFFF) '{text}'.
Font: bold compressed sans-serif (Impact / Bebas Neue).
3-4px black (#000000) outline + drop shadow (4px down, 6px blur, 80% opacity).

{logo_line}NO logos, icons, watermarks, play buttons, channel symbols, or brand marks.
NO data charts, infographic overlays, or economic graphs.
NO government chambers or parliamentary hemiciclo.

Overall mood: {mood}.
16:9 YouTube thumbnail."""

_TEMPLATE_C = """\
A thick gold (#C9A84C) border frames the entire image edge.

BACKGROUND: {background}.

SUBJECT (UPPER HALF, centered): {person}. Face fills 30-35%. Looks directly at camera.

TEXT (LOWER THIRD, centered horizontally): One or two GIANT words in bold ALL-CAPS white (#FFFFFF): '{text}'. \
The text dominates the bottom third of the image, filling most of its width. \
Font: bold compressed sans-serif (Impact / Bebas Neue). \
3-4px black (#000000) outline + drop shadow (4px down, 6px blur, 80% opacity).

{logo_line}NO logos, icons, watermarks, play buttons, channel symbols, or brand marks.
NO data charts, infographic overlays, or economic graphs.
NO government chambers or parliamentary hemiciclo.

Overall mood: {mood}.
16:9 YouTube thumbnail."""

# Logo line per layout (inserted only when brief["logo"] is non-empty).
_LOGO_LINE_A = "TOP-LEFT corner, small {logo} logo.\n\n"
_LOGO_LINE_B = "TOP-RIGHT corner, small {logo} logo.\n\n"
_LOGO_LINE_C = "BOTTOM-RIGHT corner, small {logo} logo.\n\n"

_TEMPLATES = {"A": _TEMPLATE_A, "B": _TEMPLATE_B, "C": _TEMPLATE_C}
_LOGO_LINES = {"A": _LOGO_LINE_A, "B": _LOGO_LINE_B, "C": _LOGO_LINE_C}

_REQUIRED_KEYS = ("text", "background", "person", "mood")


def build_pikzels_prompt(art_brief: dict, layout: str) -> str:
    """Build a Pikzels text prompt from an art-direction brief and a layout choice.

    Args:
        art_brief: Dict with keys ``text``, ``background``, ``person``, ``mood``.
                   Optional key ``logo`` adds a corner logo line when non-empty.
                   The dict is never mutated.
        layout:    One of ``"A"``, ``"B"``, or ``"C"`` (case-sensitive).

    Returns:
        Formatted prompt string safe to pass to ``PikzelsClient.thumbnail_from_text``.
        Guaranteed to contain no ``"http"`` substring.

    Raises:
        ValueError: When ``layout`` is not in ``{"A", "B", "C"}``.
        KeyError:   When any of the required keys is missing from ``art_brief``.
    """
    if layout not in _TEMPLATES:
        raise ValueError(
            f"Unknown layout {layout!r}. Valid values are: 'A', 'B', 'C'."
        )

    # Access required keys — raises KeyError naturally if missing.
    for key in _REQUIRED_KEYS:
        _ = art_brief[key]

    # Build values without mutating input.
    text_upper = art_brief["text"].upper()
    background = art_brief["background"]
    person = art_brief["person"]
    mood = art_brief["mood"]

    logo = art_brief.get("logo", "")
    logo_line = _LOGO_LINES[layout].format(logo=logo) if logo else ""

    rendered = _TEMPLATES[layout].format(
        text=text_upper,
        background=background,
        person=person,
        mood=mood,
        logo_line=logo_line,
    )

    # Defensive: strip any accidental "http" that may have leaked from brief values.
    rendered = rendered.replace("http", "")

    return rendered
