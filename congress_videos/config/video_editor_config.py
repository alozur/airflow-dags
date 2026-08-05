"""Per-domain configuration for generic video overlay editing.

This module provides the domain-keyed config dict used by the video editor
module. Each domain entry defines per-tipo overlay style keys used to
construct ffmpeg drawtext filter arguments.

Usage::

    from congress_videos.config.video_editor_config import ConfigError, get_domain_config

    cfg = get_domain_config("congreso")
    style = cfg["tipos"]["extracto_sesion"]
"""

from __future__ import annotations

from congress_videos.config.paths import FONT_BOLD


class ConfigError(Exception):
    """Raised when a requested domain key is absent from the video editor config."""


_VIDEO_EDITOR_CONFIG: dict = {
    "congreso": {
        # Per-tipo overlay style dicts.  Each entry maps a tipo label to the
        # ffmpeg drawtext arguments needed to render that overlay style.
        "tipos": {
            "extracto_sesion": {
                # Absolute path to the font file on the Airflow image.
                "fontfile": FONT_BOLD,
                # Font size in pixels.
                "fontsize": 42,
                # Font colour (ffmpeg colour name or hex).
                "fontcolor": "white",
                # Draw a filled box behind the text (1 = enabled).
                "box": 1,
                # Box fill colour and opacity.
                "boxcolor": "black@0.5",
                # Padding around the text inside the box (pixels).
                "boxborderw": 8,
                # Horizontal position expression (ffmpeg geometry).
                # (w-text_w)/2 centres the text horizontally.
                "x": "(w-text_w)/2",
                # Vertical position expression.
                # h-th-60 places the text 60px from the bottom edge.
                "y": "h-th-60",
                # Minimum overlay duration in seconds (informational).
                "min_duration_secs": 1.0,
                # Maximum overlay duration in seconds (informational).
                "max_duration_secs": 15.0,
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
            f"Unknown video editor domain: {domain!r}. "
            f"Available domains: {list(_VIDEO_EDITOR_CONFIG.keys())}"
        )
    return _VIDEO_EDITOR_CONFIG[domain]
