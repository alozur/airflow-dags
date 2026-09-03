"""Tests for the per-channel/per-purpose YouTube token registry."""

import pytest

from congress_videos.config.youtube_channels import (
    CHANNELS,
    DEFAULT_CHANNEL,
    PURPOSES,
    get_token_path,
    get_token_scopes,
    resolve_token_path,
)


class TestRegistry:
    def test_default_channel_is_registered(self):
        assert DEFAULT_CHANNEL in CHANNELS

    def test_purposes_are_upload_and_analytics(self):
        assert set(PURPOSES) == {"upload", "analytics"}


class TestGetTokenScopes:
    def test_analytics_scope_is_read_only_single(self):
        scopes = get_token_scopes("analytics")
        assert scopes == ("https://www.googleapis.com/auth/yt-analytics.readonly",)

    def test_upload_scopes_include_upload_and_force_ssl(self):
        scopes = get_token_scopes("upload")
        assert "https://www.googleapis.com/auth/youtube.upload" in scopes
        assert "https://www.googleapis.com/auth/youtube.force-ssl" in scopes

    def test_analytics_has_no_write_scopes(self):
        # Least privilege: an analytics token must not carry any upload/manage scope.
        scopes = get_token_scopes("analytics")
        assert not any("upload" in s or "force-ssl" in s for s in scopes)

    def test_unknown_purpose_raises(self):
        with pytest.raises(ValueError):
            get_token_scopes("delete")


class TestGetTokenPath:
    def test_builds_channel_purpose_path(self):
        path = get_token_path("congreso-es-tv", "analytics", tokens_dir="/data/youtube_tokens")
        assert path == "/data/youtube_tokens/congreso-es-tv/analytics.json"

    def test_upload_path(self):
        path = get_token_path("congreso-es-tv", "upload", tokens_dir="/data/youtube_tokens")
        assert path == "/data/youtube_tokens/congreso-es-tv/upload.json"

    def test_unknown_channel_raises(self):
        with pytest.raises(ValueError):
            get_token_path("unknown", "upload")

    def test_unknown_purpose_raises(self):
        with pytest.raises(ValueError):
            get_token_path("congreso-es-tv", "delete")


class TestResolveTokenPath:
    def test_returns_new_path_when_it_exists(self, tmp_path):
        tokens_dir = tmp_path / "youtube_tokens"
        new_path = tokens_dir / "congreso-es-tv" / "upload.json"
        new_path.parent.mkdir(parents=True)
        new_path.write_bytes(b"x")

        resolved = resolve_token_path("congreso-es-tv", "upload", tokens_dir=str(tokens_dir))
        assert resolved == str(new_path)

    def test_no_legacy_fallback_when_new_missing(self, tmp_path):
        # Issue #197: the legacy single-token pickle fallback was retired —
        # the per-channel JSON path is returned even when the file is absent.
        tokens_dir = tmp_path / "youtube_tokens"

        resolved = resolve_token_path("congreso-es-tv", "upload", tokens_dir=str(tokens_dir))
        assert resolved == str(tokens_dir / "congreso-es-tv" / "upload.json")

    def test_legacy_path_kwarg_removed(self):
        import pytest

        with pytest.raises(TypeError):
            resolve_token_path("congreso-es-tv", "upload", legacy_path="/tmp/x")

    def test_analytics_resolves_to_json_path(self, tmp_path):
        tokens_dir = tmp_path / "youtube_tokens"

        resolved = resolve_token_path("congreso-es-tv", "analytics", tokens_dir=str(tokens_dir))
        assert resolved == str(tokens_dir / "congreso-es-tv" / "analytics.json")
