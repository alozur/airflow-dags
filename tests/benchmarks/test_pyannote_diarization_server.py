"""Tests for benchmarks/pyannote_diarization/server.py — diarize-api FastAPI service.

Heavy deps (torch, pyannote) are never imported: the model-loader callable is
injected so the test suite runs without GPU/model downloads.
"""
from __future__ import annotations

import io
import os

import pytest
from fastapi.testclient import TestClient


def _make_client(inference_callable=None):
    """Create a TestClient with an injectable inference callable.

    The TestClient is used as a context manager so the FastAPI lifespan
    (which loads the model into ``_state``) runs before any request.
    Returns the entered context manager directly so callers use it like a
    regular client.
    """
    if inference_callable is None:
        def inference_callable(wav_path: str) -> list[dict]:
            return []

    import sys
    # Ensure fresh import so each call gets its own _state dict.
    if "benchmarks.pyannote_diarization.server" in sys.modules:
        del sys.modules["benchmarks.pyannote_diarization.server"]

    import benchmarks.pyannote_diarization.server as srv
    app = srv.create_app(model_loader=lambda: inference_callable)
    # Enter the lifespan so _state["infer"] is populated before requests.
    client = TestClient(app)
    client.__enter__()
    return client


class TestHealthEndpoint:
    def test_health_returns_200(self):
        client = _make_client()
        resp = client.get("/health")
        assert resp.status_code == 200

    def test_health_body_is_exactly_status_ok(self):
        client = _make_client()
        resp = client.get("/health")
        assert resp.json() == {"status": "ok"}


class TestDiarizeEndpoint:
    def _stub_inference(self, changes: list[dict]):
        """Return a stub that ignores wav_path and returns canned changes."""
        def inference(wav_path: str) -> list[dict]:
            return changes

        return inference

    def test_missing_audio_file_returns_422(self):
        client = _make_client()
        resp = client.post("/diarize", data={"chapter_offset": "0.0"})
        assert resp.status_code == 422

    def test_happy_path_returns_200_with_speaker_changes(self):
        changes = [
            {
                "start_seconds": 5.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 10.0,
            }
        ]
        client = _make_client(self._stub_inference(changes))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "speaker_changes" in body
        assert body["speaker_changes"][0]["from_speaker"] == "SPEAKER_00"
        assert body["speaker_changes"][0]["to_speaker"] == "SPEAKER_01"

    def test_chapter_offset_is_added_to_start_seconds(self):
        changes = [
            {
                "start_seconds": 10.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 5.0,
            }
        ]
        client = _make_client(self._stub_inference(changes))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "100.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        # 10.0 + 100.0 = 110.0
        assert body["speaker_changes"][0]["start_seconds"] == pytest.approx(110.0)

    def test_zero_offset_leaves_start_seconds_unchanged(self):
        changes = [
            {
                "start_seconds": 7.5,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 3.0,
            }
        ]
        client = _make_client(self._stub_inference(changes))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["speaker_changes"][0]["start_seconds"] == pytest.approx(7.5)

    def test_speaker_changes_mapping_preserves_all_fields(self):
        changes = [
            {
                "start_seconds": 3.0,
                "from_speaker": "SPEAKER_00",
                "to_speaker": "SPEAKER_01",
                "confirmed_block_duration_seconds": 12.0,
            }
        ]
        client = _make_client(self._stub_inference(changes))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )
        assert resp.status_code == 200
        sc = resp.json()["speaker_changes"][0]
        assert "start_seconds" in sc
        assert "from_speaker" in sc
        assert "to_speaker" in sc
        assert "confirmed_block_duration_seconds" in sc
        assert sc["confirmed_block_duration_seconds"] == pytest.approx(12.0)


class TestTempfileCleanup:
    def test_tempfile_cleaned_after_successful_inference(self, tmp_path, monkeypatch):
        """No tempfile should remain after a successful /diarize call."""
        import tempfile as _tf

        created_files: list[str] = []
        original_named_temporary_file = _tf.NamedTemporaryFile

        def tracking_named_temporary_file(*args, **kwargs):
            f = original_named_temporary_file(*args, **kwargs)
            created_files.append(f.name)
            return f

        monkeypatch.setattr(_tf, "NamedTemporaryFile", tracking_named_temporary_file)

        changes = [
            {
                "start_seconds": 1.0,
                "from_speaker": "A",
                "to_speaker": "B",
                "confirmed_block_duration_seconds": 2.0,
            }
        ]

        import benchmarks.pyannote_diarization.server as srv
        import importlib
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        def stub_inference(wav_path: str) -> list[dict]:
            return changes

        app = srv.create_app(model_loader=lambda: stub_inference)
        client = TestClient(app)
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )

        # All created tempfiles must have been cleaned up
        for fpath in created_files:
            assert not os.path.exists(fpath), f"Tempfile not cleaned up: {fpath}"

    def test_tempfile_cleaned_after_failed_inference(self, tmp_path, monkeypatch):
        """Tempfile must be cleaned up even when inference raises."""
        import tempfile as _tf

        created_files: list[str] = []
        original_ntf = _tf.NamedTemporaryFile

        def tracking_ntf(*args, **kwargs):
            f = original_ntf(*args, **kwargs)
            created_files.append(f.name)
            return f

        monkeypatch.setattr(_tf, "NamedTemporaryFile", tracking_ntf)

        def failing_inference(wav_path: str) -> list[dict]:
            raise RuntimeError("inference failed")

        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        app = srv.create_app(model_loader=lambda: failing_inference)
        client = TestClient(app, raise_server_exceptions=False)
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )

        for fpath in created_files:
            assert not os.path.exists(fpath), f"Tempfile not cleaned after failure: {fpath}"
