"""Tests for SidecarApiError — the single structured failure type for both sidecar wrappers."""
from __future__ import annotations

import pytest


class TestSidecarApiError:
    def test_is_runtime_error_subclass(self):
        from congress_videos.modules.sidecar_api_error import SidecarApiError

        assert issubclass(SidecarApiError, RuntimeError)

    def test_message_is_preserved(self):
        from congress_videos.modules.sidecar_api_error import SidecarApiError

        err = SidecarApiError("diarize-api unreachable at http://diarize-api:8080")
        assert "diarize-api unreachable" in str(err)
        assert "http://diarize-api:8080" in str(err)

    def test_can_be_caught_as_runtime_error(self):
        from congress_videos.modules.sidecar_api_error import SidecarApiError

        with pytest.raises(RuntimeError):
            raise SidecarApiError("test error")

    def test_can_be_caught_as_sidecar_api_error(self):
        from congress_videos.modules.sidecar_api_error import SidecarApiError

        with pytest.raises(SidecarApiError):
            raise SidecarApiError("test error")
