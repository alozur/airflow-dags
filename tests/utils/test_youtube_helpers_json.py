"""Tests for JSON token storage in youtube_helpers.

JSON storage (Credentials.to_json / from_authorized_user_info) is portable
across google-auth versions, unlike pickle.
"""

import pickle
from datetime import datetime, timedelta

from google.oauth2.credentials import Credentials

from utils.youtube_helpers import _load_credentials, _save_credentials


def _make_creds(scopes):
    # Future expiry so the credential is valid and loading never triggers a
    # network refresh.
    return Credentials(
        token="access-token",
        refresh_token="refresh-token",
        token_uri="https://oauth2.googleapis.com/token",
        client_id="client-id",
        client_secret="client-secret",
        scopes=scopes,
        expiry=datetime.utcnow() + timedelta(hours=1),
    )


class TestSaveCredentials:
    def test_json_path_writes_valid_json(self, tmp_path):
        creds = _make_creds(["https://www.googleapis.com/auth/yt-analytics.readonly"])
        dst = tmp_path / "analytics.json"

        _save_credentials(creds, str(dst))

        text = dst.read_text()
        assert '"refresh_token": "refresh-token"' in text
        # Must be JSON, not a pickle byte stream.
        assert text.lstrip().startswith("{")

    def test_pickle_path_writes_pickle(self, tmp_path):
        creds = _make_creds(["https://www.googleapis.com/auth/youtube.upload"])
        dst = tmp_path / "legacy.pickle"

        _save_credentials(creds, str(dst))

        with open(dst, "rb") as fh:
            loaded = pickle.load(fh)
        assert loaded.refresh_token == "refresh-token"


class TestLoadCredentials:
    def test_json_round_trip_preserves_refresh_token_and_scopes(self, tmp_path):
        scopes = ["https://www.googleapis.com/auth/yt-analytics.readonly"]
        dst = tmp_path / "analytics.json"
        _save_credentials(_make_creds(scopes), str(dst))

        loaded = _load_credentials(str(dst))

        assert loaded.refresh_token == "refresh-token"
        assert loaded.scopes == scopes

    def test_missing_file_raises(self, tmp_path):
        import pytest

        with pytest.raises(FileNotFoundError):
            _load_credentials(str(tmp_path / "nope.json"))

    def test_legacy_pickle_still_loads(self, tmp_path):
        dst = tmp_path / "legacy.pickle"
        _save_credentials(_make_creds(["https://www.googleapis.com/auth/youtube"]), str(dst))

        loaded = _load_credentials(str(dst))

        assert loaded.refresh_token == "refresh-token"
