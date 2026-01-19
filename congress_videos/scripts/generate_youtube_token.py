#!/usr/bin/env python3
"""
Generate YouTube OAuth Token

This script generates a new OAuth token for YouTube API access.
Run this locally (not in Docker) as it requires browser access.

Prerequisites:
1. Download OAuth 2.0 Client ID credentials from Google Cloud Console
   - Go to: https://console.cloud.google.com/apis/credentials
   - Create OAuth 2.0 Client ID (Desktop application)
   - Download JSON and save as 'client_secrets.json'

2. Enable YouTube Data API v3:
   - Go to: https://console.cloud.google.com/apis/library/youtube.googleapis.com

Usage:
    python scripts/generate_youtube_token.py

The token will be saved to: congress_youtube_token.pickle
Copy this file to your Airflow data directory.
"""

import os
import pickle
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow

# YouTube API scopes required for uploading
SCOPES = [
    'https://www.googleapis.com/auth/youtube.upload',
    'https://www.googleapis.com/auth/youtube',
    'https://www.googleapis.com/auth/youtube.force-ssl'
]

# File paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
CLIENT_SECRETS_FILE = PROJECT_ROOT / 'client_secrets.json'
TOKEN_FILE = PROJECT_ROOT / 'congress_youtube_token.pickle'


def generate_token():
    """Generate new OAuth token through browser-based flow."""

    credentials = None

    # Check if token already exists
    if TOKEN_FILE.exists():
        print(f"Loading existing token from {TOKEN_FILE}")
        with open(TOKEN_FILE, 'rb') as token:
            credentials = pickle.load(token)

    # Check if credentials are valid
    if credentials and credentials.valid:
        print("Existing token is still valid!")
        return credentials

    # Try to refresh if expired
    if credentials and credentials.expired and credentials.refresh_token:
        print("Token expired. Attempting to refresh...")
        try:
            credentials.refresh(Request())
            print("Token refreshed successfully!")

            # Save refreshed token
            with open(TOKEN_FILE, 'wb') as token:
                pickle.dump(credentials, token)
            print(f"Refreshed token saved to {TOKEN_FILE}")
            return credentials
        except Exception as e:
            print(f"Failed to refresh token: {e}")
            print("Will generate new token...")
            credentials = None

    # Generate new token
    if not credentials:
        if not CLIENT_SECRETS_FILE.exists():
            print(f"\nERROR: Client secrets file not found: {CLIENT_SECRETS_FILE}")
            print("\nTo create this file:")
            print("1. Go to https://console.cloud.google.com/apis/credentials")
            print("2. Create OAuth 2.0 Client ID (Desktop application)")
            print("3. Download JSON and save as 'client_secrets.json' in project root")
            return None

        print(f"\nStarting OAuth flow using {CLIENT_SECRETS_FILE}")
        print("A browser window will open for authentication...")

        flow = InstalledAppFlow.from_client_secrets_file(
            str(CLIENT_SECRETS_FILE),
            SCOPES
        )

        # Run local server for OAuth callback (use port 8090 to avoid conflicts)
        credentials = flow.run_local_server(
            port=8090,
            prompt='consent',
            access_type='offline'  # Required for refresh token
        )

        # Save new token
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(credentials, token)
        print(f"\nNew token saved to {TOKEN_FILE}")

    return credentials


def main():
    print("=" * 60)
    print("YouTube OAuth Token Generator")
    print("=" * 60)

    credentials = generate_token()

    if credentials:
        print("\n" + "=" * 60)
        print("SUCCESS!")
        print("=" * 60)
        print(f"\nToken file: {TOKEN_FILE}")
        print(f"Token valid: {credentials.valid}")
        print(f"Token expired: {credentials.expired}")
        print(f"Has refresh token: {bool(credentials.refresh_token)}")
        print(f"\nCopy the token file to your Airflow data directory:")
        print(f"  cp {TOKEN_FILE} /path/to/airflow/data/congress_videos/")
    else:
        print("\nFailed to generate token.")
        return 1

    return 0


if __name__ == '__main__':
    exit(main())
