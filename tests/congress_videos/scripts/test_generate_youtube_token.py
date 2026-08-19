"""Tests for the per-channel/per-purpose token generator CLI.

The OAuth browser flow is not exercised; these tests cover argument parsing,
the local path layout, and that scopes come from the registry.
"""

import importlib.util
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[3]
    / "congress_videos"
    / "scripts"
    / "generate_youtube_token.py"
)


def _load_script():
    spec = importlib.util.spec_from_file_location("generate_youtube_token", _SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


gen = _load_script()


class TestParseArgs:
    def test_defaults_to_congreso_es_tv_upload(self):
        args = gen.parse_args([])
        assert args.channel == "congreso-es-tv"
        assert args.purpose == "upload"

    def test_accepts_analytics_purpose(self):
        args = gen.parse_args(["--channel", "congreso-es-tv", "--purpose", "analytics"])
        assert args.purpose == "analytics"

    def test_rejects_unknown_purpose(self):
        with pytest.raises(SystemExit):
            gen.parse_args(["--purpose", "delete"])

    def test_rejects_unknown_channel(self):
        with pytest.raises(SystemExit):
            gen.parse_args(["--channel", "nope"])


class TestLocalTokenPath:
    def test_builds_channel_purpose_json(self):
        path = gen.local_token_path("congreso-es-tv", "analytics")
        assert path.name == "analytics.json"
        assert path.parent.name == "congreso-es-tv"
        assert path.parent.parent.name == "youtube_tokens"


class TestScopeWiring:
    def test_analytics_uses_read_only_scope_from_registry(self):
        from congress_videos.config.youtube_channels import get_token_scopes

        assert gen.get_token_scopes is get_token_scopes
        assert get_token_scopes("analytics") == (
            "https://www.googleapis.com/auth/yt-analytics.readonly",
        )
