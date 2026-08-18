"""YouTube channel and OAuth token registry.

Tokens are organized per channel and per purpose so each purpose holds only
the OAuth scopes it needs (least privilege). The ``analytics`` purpose is
read-only, so an analytics token cannot upload, edit, or delete anything.

On-disk layout (runtime, under the Airflow data dir)::

    {YOUTUBE_TOKENS_DIR}/{channel}/{purpose}.pickle

Examples::

    .../youtube_tokens/congreso/upload.pickle      # upload + manage scopes
    .../youtube_tokens/congreso/analytics.pickle    # yt-analytics.readonly

Onboard a new channel by adding an entry to ``CHANNELS``; add a new token
purpose by adding an entry to ``TOKEN_SCOPES``.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from congress_videos.config.paths import YOUTUBE_TOKEN_FILE, YOUTUBE_TOKENS_DIR


@dataclass(frozen=True)
class ChannelConfig:
    """Static metadata for a registered YouTube channel."""

    slug: str
    display_name: str
    channel_id: str | None = None


# Registered channels, keyed by slug. Add an entry to onboard a channel.
CHANNELS: dict[str, ChannelConfig] = {
    "congreso": ChannelConfig(
        slug="congreso",
        display_name="Congreso",
        channel_id=None,  # populate once known
    ),
}

# Channel assumed when a caller does not specify one (the original channel).
DEFAULT_CHANNEL = "congreso"

# OAuth scopes per token purpose. Least privilege: analytics is read-only and
# deliberately excludes every write/upload scope.
TOKEN_SCOPES: dict[str, tuple[str, ...]] = {
    "upload": (
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube",
        "https://www.googleapis.com/auth/youtube.force-ssl",
    ),
    "analytics": (
        "https://www.googleapis.com/auth/yt-analytics.readonly",
    ),
}

# Valid token purposes.
PURPOSES: tuple[str, ...] = tuple(TOKEN_SCOPES)


def _validate_channel(channel: str) -> None:
    if channel not in CHANNELS:
        raise ValueError(
            f"Unknown channel '{channel}'. Registered channels: {sorted(CHANNELS)}"
        )


def _validate_purpose(purpose: str) -> None:
    if purpose not in TOKEN_SCOPES:
        raise ValueError(
            f"Unknown token purpose '{purpose}'. Valid purposes: {sorted(TOKEN_SCOPES)}"
        )


def get_token_scopes(purpose: str) -> tuple[str, ...]:
    """Return the OAuth scopes for a token purpose.

    Raises:
        ValueError: If ``purpose`` is not a registered purpose.
    """
    _validate_purpose(purpose)
    return TOKEN_SCOPES[purpose]


def get_token_path(
    channel: str,
    purpose: str,
    *,
    tokens_dir: str = YOUTUBE_TOKENS_DIR,
) -> str:
    """Return the on-disk path for a channel+purpose token pickle.

    This does not check for existence. Use :func:`resolve_token_path` when a
    backward-compatible fallback to the legacy single-token file is wanted.

    Raises:
        ValueError: If the channel or purpose is not registered.
    """
    _validate_channel(channel)
    _validate_purpose(purpose)
    return f"{tokens_dir}/{channel}/{purpose}.pickle"


def resolve_token_path(
    channel: str,
    purpose: str,
    *,
    tokens_dir: str = YOUTUBE_TOKENS_DIR,
    legacy_path: str = YOUTUBE_TOKEN_FILE,
) -> str:
    """Resolve the token path to use, with backward compatibility.

    Prefers the per-channel/per-purpose path. If that file does not exist yet
    but the legacy single-token file does, returns the legacy path so existing
    deployments keep working until their tokens are migrated.

    The legacy fallback applies ONLY to the default channel's ``upload``
    purpose, because the legacy token predates the per-purpose split and was an
    upload token for the default channel. The read-only ``analytics`` purpose
    never falls back — the legacy token lacks the analytics scope.

    Raises:
        ValueError: If the channel or purpose is not registered.
    """
    new_path = get_token_path(channel, purpose, tokens_dir=tokens_dir)
    if os.path.exists(new_path):
        return new_path
    if (
        channel == DEFAULT_CHANNEL
        and purpose == "upload"
        and os.path.exists(legacy_path)
    ):
        return legacy_path
    return new_path
