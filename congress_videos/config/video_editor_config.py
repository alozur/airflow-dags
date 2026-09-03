"""Per-domain configuration for generic video overlay editing.

This module provides the domain-keyed config dict used by the video editor
module. Each domain entry defines per-tipo overlay style keys used to
construct ffmpeg drawtext or Pillow-rendered overlay arguments.

Two renderers are supported:
- ``"drawtext"`` (default): ffmpeg drawtext filter; keys mirror ffmpeg options.
- ``"pillow"``: Pillow-rendered transparent PNG composited via ffmpeg overlay;
  keys are Python/Pillow-native (RGB tuples, ints).

Usage::

    from congress_videos.config.video_editor_config import ConfigError, get_domain_config

    cfg = get_domain_config("congreso")
    style = cfg["tipos"]["extracto_sesion"]
"""

from __future__ import annotations

from congress_videos.config.paths import FONT_BOLD, FONT_REGULAR


class ConfigError(Exception):
    """Raised when a requested domain key is absent from the video editor config."""


_VIDEO_EDITOR_CONFIG: dict = {
    "congreso": {
        "tipos": {
            # ------------------------------------------------------------------
            # Pillow tipos: rendered as transparent PNG, composited via ffmpeg.
            # Color values are (R, G, B) or (R, G, B, A) tuples.
            # ------------------------------------------------------------------
            # Bottom-centered 90% bar: session extract label.
            # titulo      → main extract text (large)
            # descripcion → optional subtitle (small, optional)
            "extracto_sesion": {
                "renderer": "pillow",
                "fontfile": FONT_BOLD,
                "fontfile_sub": FONT_REGULAR,
                "fontsize_title": 30,
                "fontsize_sub": 18,
                "bg_color": (12, 36, 97, 215),
                "accent_color": (0, 120, 255, 255),
                "title_color": (255, 255, 255, 255),
                "sub_color": (180, 210, 255, 240),
                "width_pct": 0.90,
                "height": 104,
                "margin_y": 60,
            },
            # Lower-left card: speaker name + role/party.
            # titulo   → speaker name (large)
            # descripcion → role · party (small, optional)
            "speaker_id": {
                "renderer": "pillow",
                "fontfile": FONT_BOLD,
                "fontfile_sub": FONT_REGULAR,
                "fontsize_title": 34,
                "fontsize_sub": 20,
                "bg_color": (12, 36, 97, 215),
                "accent_color": (0, 120, 255, 255),
                "title_color": (255, 255, 255, 255),
                "sub_color": (180, 210, 255, 240),
                "width_pct": 0.46,
                "height": 104,
                "margin_x": 64,
                "margin_y": 60,
            },
            # Centered large quote highlight.
            # titulo      → quote body (large, white)
            # descripcion → attribution, e.g. "— Juan Pérez" (small, gold)
            "cita_destacada": {
                "renderer": "pillow",
                "fontfile": FONT_BOLD,
                "fontfile_sub": FONT_REGULAR,
                "fontsize_title": 32,
                "fontsize_sub": 20,
                "bg_color": (0, 0, 0, 155),
                "accent_color": (255, 190, 0, 255),
                "title_color": (255, 255, 255, 255),
                "sub_color": (255, 190, 0, 230),
                "width_pct": 0.86,
                "padding": 28,
            },
            # Top red breaking-news banner.
            # titulo      → news text
            # descripcion → optional detail line (smaller)
            "urgente": {
                "renderer": "pillow",
                "fontfile": FONT_BOLD,
                "fontfile_sub": FONT_REGULAR,
                "fontsize_label": 18,
                "fontsize_title": 26,
                "fontsize_sub": 17,
                "bg_color": (180, 15, 15, 235),
                "label_bg_color": (220, 0, 0, 255),
                "accent_color": (255, 230, 0, 255),
                "label_color": (255, 230, 0, 255),
                "title_color": (255, 255, 255, 255),
                "sub_color": (255, 210, 210, 230),
                "label": "URGENTE",
                "width_pct": 0.90,
                "height": 62,
                "margin_y": 24,
            },
            # Bottom-right compact data/context box.
            # titulo      → headline or number (large)
            # descripcion → context sentence (small, optional)
            "dato_contexto": {
                "renderer": "pillow",
                "fontfile": FONT_BOLD,
                "fontfile_sub": FONT_REGULAR,
                "fontsize_header": 14,
                "fontsize_title": 30,
                "fontsize_sub": 16,
                "bg_color": (12, 36, 97, 215),
                "accent_color": (255, 190, 0, 255),
                "header_color": (255, 190, 0, 255),
                "title_color": (255, 255, 255, 255),
                "sub_color": (180, 210, 255, 230),
                "label": "DATO",
                "width": 300,
                "padding": 18,
                "margin_x": 64,
                "margin_y": 60,
            },
        },
    },
}


def get_domain_config(domain: str) -> dict:
    """Return the config dict for *domain*, raising ConfigError if absent.

    Args:
        domain: Domain key (e.g. ``"congreso"``).

    Returns:
        Per-domain configuration dict containing a ``tipos`` sub-dict.

    Raises:
        ConfigError: When *domain* is not present in the video editor config.
    """
    if domain not in _VIDEO_EDITOR_CONFIG:
        raise ConfigError(
            f"Unknown video editor domain: {domain!r}. Available domains: {list(_VIDEO_EDITOR_CONFIG.keys())}"
        )
    return _VIDEO_EDITOR_CONFIG[domain]
