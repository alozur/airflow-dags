"""YouTube channel and OAuth token registry.

Tokens are organized per channel and per purpose so each purpose holds only
the OAuth scopes it needs (least privilege). The ``analytics`` purpose is
read-only, so an analytics token cannot upload, edit, or delete anything.

Tokens are stored as portable JSON (``Credentials.to_json()``), which is
stable across google-auth versions — unlike pickle, which breaks when the
writer and reader run different google-auth majors.

On-disk layout (runtime, under the Airflow data dir)::

    {YOUTUBE_TOKENS_DIR}/{channel}/{purpose}.json

Examples::

    .../youtube_tokens/congreso-es-tv/upload.json      # upload + manage scopes
    .../youtube_tokens/congreso-es-tv/analytics.json    # yt-analytics.readonly

Onboard a new channel by adding an entry to ``CHANNELS``; add a new token
purpose by adding an entry to ``TOKEN_SCOPES``.
"""

from __future__ import annotations

from dataclasses import dataclass

from congress_videos.config.paths import YOUTUBE_TOKENS_DIR


@dataclass(frozen=True)
class ChannelConfig:
    """Static metadata for a registered YouTube channel."""

    slug: str
    display_name: str
    channel_id: str | None = None


# Registered channels, keyed by slug. Add an entry to onboard a channel.
CHANNELS: dict[str, ChannelConfig] = {
    "congreso-es-tv": ChannelConfig(
        slug="congreso-es-tv",
        display_name="Congreso ES TV",
        channel_id=None,  # populate once known
    ),
}

# Channel assumed when a caller does not specify one (the original channel).
DEFAULT_CHANNEL = "congreso-es-tv"

# OAuth scopes per token purpose. Least privilege: analytics is read-only and
# deliberately excludes every write/upload scope.
TOKEN_SCOPES: dict[str, tuple[str, ...]] = {
    "upload": (
        "https://www.googleapis.com/auth/youtube.upload",
        "https://www.googleapis.com/auth/youtube",
        "https://www.googleapis.com/auth/youtube.force-ssl",
    ),
    "analytics": ("https://www.googleapis.com/auth/yt-analytics.readonly",),
}

# Valid token purposes.
PURPOSES: tuple[str, ...] = tuple(TOKEN_SCOPES)


def _validate_channel(channel: str) -> None:
    if channel not in CHANNELS:
        raise ValueError(f"Unknown channel '{channel}'. Registered channels: {sorted(CHANNELS)}")


def _validate_purpose(purpose: str) -> None:
    if purpose not in TOKEN_SCOPES:
        raise ValueError(f"Unknown token purpose '{purpose}'. Valid purposes: {sorted(TOKEN_SCOPES)}")


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
    """Return the on-disk path for a channel+purpose token (JSON).

    This does not check for existence. Use :func:`resolve_token_path` when a
    backward-compatible fallback to the legacy single-token file is wanted.

    Raises:
        ValueError: If the channel or purpose is not registered.
    """
    _validate_channel(channel)
    _validate_purpose(purpose)
    return f"{tokens_dir}/{channel}/{purpose}.json"


def resolve_token_path(
    channel: str,
    purpose: str,
    *,
    tokens_dir: str = YOUTUBE_TOKENS_DIR,
) -> str:
    """Resolve the on-disk token path for a channel+purpose (JSON).

    Historically this fell back to the legacy single-token pickle for the
    default channel's ``upload`` purpose; that fallback was retired in issue
    #197 together with the pickle format itself.

    Raises:
        ValueError: If the channel or purpose is not registered.
    """
    return get_token_path(channel, purpose, tokens_dir=tokens_dir)
