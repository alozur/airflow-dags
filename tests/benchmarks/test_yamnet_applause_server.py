"""Tests for benchmarks/yamnet_applause/server.py — yamnet-api FastAPI service.

Heavy deps (tflite, soundfile, numpy) are never imported: the inference callable
is injected so the test suite runs without model downloads.
"""
from __future__ import annotations

import io
import os

import pytest
from fastapi.testclient import TestClient


def _make_client(inference_callable=None):
    """Create a TestClient with an injectable inference callable.

    The TestClient is entered as a context manager so the FastAPI lifespan
    (which populates ``_state``) runs before any request.
    """
    if inference_callable is None:
        def inference_callable(wav_path: str) -> list[dict]:
            return []

    import sys
    if "benchmarks.yamnet_applause.server" in sys.modules:
        del sys.modules["benchmarks.yamnet_applause.server"]

    import benchmarks.yamnet_applause.server as srv
    app = srv.create_app(model_loader=lambda: inference_callable)
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


class TestDetectEndpoint:
    def _stub_inference(self, intervals: list[dict]):
        def inference(wav_path: str) -> list[dict]:
            return intervals
        return inference

    def test_missing_audio_file_returns_422(self):
        client = _make_client()
        resp = client.post("/detect", data={"offset": "0.0"})
        assert resp.status_code == 422

    def test_happy_path_returns_200_with_applause_intervals(self):
        intervals = [{"start": 5.0, "end": 20.0, "max_score": 0.88}]
        client = _make_client(self._stub_inference(intervals))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/detect",
            files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"offset": "0.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert "applause_intervals" in body
        assert body["applause_intervals"][0]["max_score"] == pytest.approx(0.88)

    def test_offset_is_added_to_start_and_end(self):
        intervals = [{"start": 5.0, "end": 20.0, "max_score": 0.75}]
        client = _make_client(self._stub_inference(intervals))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/detect",
            files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"offset": "100.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        iv = body["applause_intervals"][0]
        # 5.0 + 100.0 = 105.0; 20.0 + 100.0 = 120.0
        assert iv["start"] == pytest.approx(105.0)
        assert iv["end"] == pytest.approx(120.0)

    def test_zero_offset_leaves_intervals_unchanged(self):
        intervals = [{"start": 3.0, "end": 8.0, "max_score": 0.6}]
        client = _make_client(self._stub_inference(intervals))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/detect",
            files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"offset": "0.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
        iv = body["applause_intervals"][0]
        assert iv["start"] == pytest.approx(3.0)
        assert iv["end"] == pytest.approx(8.0)

    def test_max_score_field_preserved(self):
        intervals = [{"start": 0.0, "end": 10.0, "max_score": 0.95}]
        client = _make_client(self._stub_inference(intervals))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/detect",
            files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"offset": "0.0"},
        )
        assert resp.status_code == 200
        iv = resp.json()["applause_intervals"][0]
        assert "max_score" in iv
        assert iv["max_score"] == pytest.approx(0.95)

    def test_empty_intervals_returns_empty_list(self):
        client = _make_client(self._stub_inference([]))
        wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
        resp = client.post(
            "/detect",
            files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
            data={"offset": "0.0"},
        )
        assert resp.status_code == 200
        assert resp.json()["applause_intervals"] == []


class TestTempfileCleanup:
    def test_tempfile_cleaned_after_successful_inference(self, monkeypatch):
        import tempfile as _tf

        created_files: list[str] = []
        original_ntf = _tf.NamedTemporaryFile

        def tracking_ntf(*args, **kwargs):
            f = original_ntf(*args, **kwargs)
            created_files.append(f.name)
            return f

        monkeypatch.setattr(_tf, "NamedTemporaryFile", tracking_ntf)

        import sys
        if "benchmarks.yamnet_applause.server" in sys.modules:
            del sys.modules["benchmarks.yamnet_applause.server"]
        import benchmarks.yamnet_applause.server as srv

        intervals = [{"start": 1.0, "end": 4.0, "max_score": 0.8}]

        def stub(wav_path: str) -> list[dict]:
            return intervals

        app = srv.create_app(model_loader=lambda: stub)
        client = TestClient(app)
        with client:
            wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
            client.post(
                "/detect",
                files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
                data={"offset": "0.0"},
            )

        for fpath in created_files:
            assert not os.path.exists(fpath), f"Tempfile not cleaned: {fpath}"

    def test_tempfile_cleaned_after_failed_inference(self, monkeypatch):
        import tempfile as _tf

        created_files: list[str] = []
        original_ntf = _tf.NamedTemporaryFile

        def tracking_ntf(*args, **kwargs):
            f = original_ntf(*args, **kwargs)
            created_files.append(f.name)
            return f

        monkeypatch.setattr(_tf, "NamedTemporaryFile", tracking_ntf)

        def failing_inference(wav_path: str) -> list[dict]:
            raise RuntimeError("inference boom")

        import sys
        if "benchmarks.yamnet_applause.server" in sys.modules:
            del sys.modules["benchmarks.yamnet_applause.server"]
        import benchmarks.yamnet_applause.server as srv

        app = srv.create_app(model_loader=lambda: failing_inference)
        client = TestClient(app, raise_server_exceptions=False)
        with client:
            wav_bytes = b"RIFF\x00\x00\x00\x00WAVEfmt "
            client.post(
                "/detect",
                files={"audio_file": ("turn.wav", io.BytesIO(wav_bytes), "audio/wav")},
                data={"offset": "0.0"},
            )

        for fpath in created_files:
            assert not os.path.exists(fpath), f"Tempfile not cleaned after failure: {fpath}"
