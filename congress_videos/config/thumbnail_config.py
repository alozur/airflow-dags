"""Per-domain configuration for generic thumbnail generation.

This module provides the domain-keyed config dict used by the thumbnail
generation module. Each domain entry defines the visual styles, participant
lookup function, and optional fallback party logo.

Usage::

    from congress_videos.config.thumbnail_config import THUMBNAIL_CONFIG, ConfigError

    cfg = THUMBNAIL_CONFIG["congreso"]
"""

from __future__ import annotations


class ConfigError(Exception):
    """Raised when a requested domain key is absent from THUMBNAIL_CONFIG."""


def _get_thumbnail_config() -> dict:
    """Build and return the full per-domain thumbnail configuration dict.

    Defined as a function to defer import of ``lookup_participant_by_slug``
    until call time (avoids circular imports at module load).

    Returns:
        Mapping from domain key to per-domain config dict.
    """
    from congress_videos.modules.participants_db import lookup_participant_by_slug

    return {
        "congreso": {
            # Visual styles / personas for Pikzels thumbnail generation.
            # Exactly 2 entries — one per generated option.
            "styles": [
                {
                    "label": "option_a",
                    "style": (
                        "Dramatic political documentary style. High-contrast lighting. "
                        "Spanish parliament chamber background. Bold typography overlay."
                    ),
                    "persona": (
                        "Senior political journalist covering the Spanish Congress. "
                        "Authoritative, serious, newsroom aesthetic."
                    ),
                },
                {
                    "label": "option_b",
                    "style": (
                        "Modern editorial news style. Clean, professional composition. "
                        "Gradient overlay with official Spanish flag colours."
                    ),
                    "persona": (
                        "Digital-native political correspondent. "
                        "Dynamic, impactful, social-media-ready framing."
                    ),
                },
            ],
            # Callable that resolves a participant by exact stable slug.
            # Signature: (slug: str) -> dict | None
            "participants_lookup": lookup_participant_by_slug,
            # Optional absolute path to a fallback party logo image.
            # Set to None when no logo file is available.
            "party_logo_map": None,
        }
    }


def get_domain_config(domain: str) -> dict:
    """Return the config dict for *domain*, raising ConfigError if absent.

    Args:
        domain: Domain key (e.g. ``"congreso"``).

    Returns:
        Per-domain configuration dict.

    Raises:
        ConfigError: When *domain* is not present in ``THUMBNAIL_CONFIG``.
    """
    config = _get_thumbnail_config()
    if domain not in config:
        raise ConfigError(
            f"Unknown thumbnail domain: {domain!r}. "
            f"Available domains: {list(config.keys())}"
        )
    return config[domain]


# Public module-level constant — lazy-evaluated to avoid circular imports.
# Access individual domains via ``get_domain_config(domain)`` or index directly.
# The dict is rebuilt on each access through ``get_domain_config``; for batch
# usage, call ``_get_thumbnail_config()`` once and cache the result locally.
THUMBNAIL_CONFIG = _get_thumbnail_config()
