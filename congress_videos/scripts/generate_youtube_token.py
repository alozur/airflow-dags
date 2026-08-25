#!/usr/bin/env python3
"""
Generate a YouTube OAuth token (per channel, per purpose).

Run this locally (not in Docker) — it opens a browser for the OAuth consent.

Tokens are organized per channel and per purpose so each purpose holds only the
OAuth scopes it needs (least privilege). Analytics tokens are read-only.

Local layout (mirrors the runtime youtube_tokens/ tree)::

    youtube_tokens/{channel}/{purpose}.json

Usage::

    python scripts/generate_youtube_token.py --channel congreso --purpose upload
    python scripts/generate_youtube_token.py --channel congreso --purpose analytics

Prerequisites:
1. OAuth 2.0 Client ID (Desktop application) JSON saved as 'client_secrets.json'
   in the project root (congress_videos/).
   https://console.cloud.google.com/apis/credentials
2. Enable the APIs the purpose needs:
   - upload    -> YouTube Data API v3
   - analytics -> YouTube Analytics API
"""

import argparse
import sys
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# Make the project importable when run as a standalone script.
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent            # congress_videos/
REPO_ROOT = PROJECT_ROOT.parent             # repository root
sys.path.insert(0, str(REPO_ROOT))

from congress_videos.config.youtube_channels import (  # noqa: E402  (path setup above)
    CHANNELS,
    DEFAULT_CHANNEL,
    PURPOSES,
    get_token_scopes,
)

CLIENT_SECRETS_FILE = PROJECT_ROOT / "client_secrets.json"

# Local mirror of the runtime youtube_tokens/ layout. Copy this tree to the
# Airflow data directory after generating tokens.
LOCAL_TOKENS_DIR = PROJECT_ROOT / "youtube_tokens"


def local_token_path(channel: str, purpose: str) -> Path:
    """Return the local JSON path for a channel+purpose token."""
    return LOCAL_TOKENS_DIR / channel / f"{purpose}.json"


def _save(credentials: Credentials, token_file: Path) -> None:
    """Write credentials as portable JSON (google-auth version independent)."""
    token_file.write_text(credentials.to_json())


def generate_token(channel: str, purpose: str):
    """Generate (or refresh) an OAuth token for one channel+purpose.

    Returns:
        Tuple of (credentials, token_file). ``credentials`` is ``None`` on
        failure.
    """
    scopes = list(get_token_scopes(purpose))
    token_file = local_token_path(channel, purpose)
    token_file.parent.mkdir(parents=True, exist_ok=True)

    credentials = None
    if token_file.exists():
        print(f"Loading existing token from {token_file}")
        credentials = Credentials.from_authorized_user_file(str(token_file), scopes)

    if credentials and credentials.valid:
        print("Existing token is still valid!")
        return credentials, token_file

    if credentials and credentials.expired and credentials.refresh_token:
        print("Token expired. Attempting to refresh...")
        try:
            credentials.refresh(Request())
            _save(credentials, token_file)
            print(f"Refreshed token saved to {token_file}")
            return credentials, token_file
        except Exception as e:  # noqa: BLE001 - surface any refresh failure and re-auth
            print(f"Failed to refresh token: {e}")
            print("Will generate a new token...")
            credentials = None

    if not CLIENT_SECRETS_FILE.exists():
        print(f"\nERROR: Client secrets file not found: {CLIENT_SECRETS_FILE}")
        print("\nTo create it:")
        print("1. Go to https://console.cloud.google.com/apis/credentials")
        print("2. Create an OAuth 2.0 Client ID (Desktop application)")
        print("3. Download the JSON and save it as 'client_secrets.json' in the project root")
        return None, token_file

    print(f"\nStarting OAuth flow for channel='{channel}' purpose='{purpose}'")
    print(f"Scopes: {scopes}")
    print("A browser window will open for authentication...")

    flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRETS_FILE), scopes)
    credentials = flow.run_local_server(
        port=8090,
        prompt="consent",
        access_type="offline",  # required for a refresh token
    )

    _save(credentials, token_file)
    print(f"\nNew token saved to {token_file}")
    return credentials, token_file


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a YouTube OAuth token per channel and purpose."
    )
    parser.add_argument(
        "--channel",
        default=DEFAULT_CHANNEL,
        choices=sorted(CHANNELS),
        help="Channel slug (default: %(default)s).",
    )
    parser.add_argument(
        "--purpose",
        default="upload",
        choices=sorted(PURPOSES),
        help="Token purpose / scope set (default: %(default)s).",
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)

    print("=" * 60)
    print(f"YouTube OAuth Token Generator — {args.channel} / {args.purpose}")
    print("=" * 60)

    credentials, token_file = generate_token(args.channel, args.purpose)

    if not credentials:
        print("\nFailed to generate token.")
        return 1

    print("\n" + "=" * 60)
    print("SUCCESS!")
    print("=" * 60)
    print(f"\nToken file: {token_file}")
    print(f"Token valid: {credentials.valid}")
    print(f"Token expired: {credentials.expired}")
    print(f"Has refresh token: {bool(credentials.refresh_token)}")
    print("\nCopy the youtube_tokens/ tree to your Airflow data directory:")
    print(f"  cp -r {LOCAL_TOKENS_DIR} /path/to/airflow/data/congress_videos/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
