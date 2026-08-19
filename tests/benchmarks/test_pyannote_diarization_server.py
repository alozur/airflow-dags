"""Tests for benchmarks/pyannote_diarization/server.py — diarize-api FastAPI service.

Heavy deps (torch, pyannote) are never imported: the model-loader callable is
injected so the test suite runs without GPU/model downloads.

Covers:
- Health endpoint (always-light: no model load, no last_activity stamp)
- Lazy model load (first request loads; subsequent reuse; concurrent first-callers load once)
- 422 validation error stamps last_activity even before route body runs
- Watchdog: exits at idle threshold (inflight==0); blocked by in-flight; timer reset by activity
- Watchdog disabled when idle_timeout <= 0
- Lifespan: watchdog task created (enabled) / absent (disabled); model NOT loaded at startup
"""
from __future__ import annotations

import asyncio
import io
import os

import pytest
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_client(
    inference_callable=None,
    loader_call_count: list | None = None,
    *,
    idle_timeout: int = 900,
    clock=None,
    sleep=None,
    exit_signal=None,
):
    """Create a TestClient with an injectable inference callable.

    Returns the entered TestClient so callers receive a ready client.
    ``loader_call_count`` is an optional mutable list used to track how many
    times the model_loader itself is called (as opposed to the inference fn).
    """
    if inference_callable is None:
        def inference_callable(wav_path: str) -> list[dict]:
            return []

    if loader_call_count is None:
        loader_call_count = []

    def counting_loader():
        loader_call_count.append(1)
        return inference_callable

    import sys
    if "benchmarks.pyannote_diarization.server" in sys.modules:
        del sys.modules["benchmarks.pyannote_diarization.server"]

    import benchmarks.pyannote_diarization.server as srv

    kwargs: dict = dict(
        model_loader=counting_loader,
        idle_timeout=idle_timeout,
    )
    if clock is not None:
        kwargs["clock"] = clock
    if sleep is not None:
        kwargs["sleep"] = sleep
    if exit_signal is not None:
        kwargs["exit_signal"] = exit_signal

    app = srv.create_app(**kwargs)
    client = TestClient(app)
    client.__enter__()
    return client


_WAV_STUB = b"RIFF\x00\x00\x00\x00WAVEfmt "


# ---------------------------------------------------------------------------
# Existing tests (safety-net)
# ---------------------------------------------------------------------------

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
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
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
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
            data={"chapter_offset": "100.0"},
        )
        assert resp.status_code == 200
        body = resp.json()
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
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
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
        resp = client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
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

        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        def stub_inference(wav_path: str) -> list[dict]:
            return changes

        app = srv.create_app(model_loader=lambda: stub_inference)
        client = TestClient(app)
        client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )

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
        client.post(
            "/diarize",
            files={"audio_file": ("chapter.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )

        for fpath in created_files:
            assert not os.path.exists(fpath), f"Tempfile not cleaned after failure: {fpath}"


# ---------------------------------------------------------------------------
# Phase 1 — NEW: Idle-exit / lazy-load tests (RED → GREEN after Phase 2)
# ---------------------------------------------------------------------------

class TestLazyLoad:
    """Model must NOT be loaded at startup; loaded on first inference only."""

    def test_health_never_loads_model(self):
        """GET /health must not trigger the model loader."""
        count: list[int] = []
        client = _make_client(loader_call_count=count, idle_timeout=0)
        client.get("/health")
        client.get("/health")
        assert count == [], "model_loader must not be called by /health"

    def test_lifespan_does_not_load_model(self):
        """After lifespan startup only, loader call count must be 0."""
        count: list[int] = []
        _make_client(loader_call_count=count, idle_timeout=0)
        # client entered (lifespan ran) but no request issued yet
        assert count == [], "model must not be loaded during lifespan startup"

    def test_first_diarize_loads_model_once(self):
        """First POST /diarize triggers exactly one model load."""
        count: list[int] = []
        client = _make_client(loader_call_count=count, idle_timeout=0)
        client.post(
            "/diarize",
            files={"audio_file": ("c.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
            data={"chapter_offset": "0.0"},
        )
        assert len(count) == 1, f"Expected exactly 1 load call, got {len(count)}"

    def test_second_diarize_skips_load(self):
        """Second POST /diarize must reuse cached model (loader not called again)."""
        count: list[int] = []
        client = _make_client(loader_call_count=count, idle_timeout=0)
        for _ in range(2):
            client.post(
                "/diarize",
                files={"audio_file": ("c.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
                data={"chapter_offset": "0.0"},
            )
        assert len(count) == 1, f"Expected exactly 1 load call across 2 requests, got {len(count)}"

    def test_concurrent_first_requests_load_model_once(self):
        """N concurrent POST /diarize while model unloaded → model loaded exactly once."""
        import threading

        count: list[int] = []
        load_started = threading.Event()
        load_proceed = threading.Event()

        def slow_inference(wav_path: str) -> list[dict]:
            return []

        def slow_loader():
            load_started.set()
            load_proceed.wait(timeout=5)
            count.append(1)
            return slow_inference

        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv
        app = srv.create_app(model_loader=slow_loader, idle_timeout=0)

        results: list[int] = []

        # All threads share ONE entered client (one lifespan context).
        with TestClient(app) as client:
            def make_request():
                resp = client.post(
                    "/diarize",
                    files={"audio_file": ("c.wav", io.BytesIO(_WAV_STUB), "audio/wav")},
                    data={"chapter_offset": "0.0"},
                )
                results.append(resp.status_code)

            threads = [threading.Thread(target=make_request) for _ in range(3)]
            for t in threads:
                t.start()
            load_started.wait(timeout=5)
            load_proceed.set()
            for t in threads:
                t.join(timeout=10)

        assert all(s == 200 for s in results), f"Not all 200: {results}"
        assert len(count) == 1, f"Expected 1 loader call, got {len(count)}"


class TestActivityStamping:
    """last_activity must be stamped by /diarize (entry+exit) and 422 handler; never by /health."""

    def test_422_stamps_last_activity(self):
        """Missing audio_file → HTTP 422 AND last_activity advances."""
        clock_val: list[float] = [0.0]

        def fake_clock() -> float:
            return clock_val[0]

        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        app = srv.create_app(model_loader=lambda: (lambda p: []), idle_timeout=0, clock=fake_clock)
        with TestClient(app) as client:
            clock_val[0] = 42.0

            resp = client.post("/diarize", data={"chapter_offset": "0.0"})
            assert resp.status_code == 422
            assert app.extra["_state"]["last_activity"] == 42.0

    def test_health_does_not_stamp_last_activity(self):
        """GET /health must not change last_activity; confirmed via watchdog not firing."""
        fired: list[bool] = []
        clock_val: list[float] = [0.0]

        def fake_clock() -> float:
            return clock_val[0]

        def fake_exit():
            fired.append(True)

        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        # Timeout=900; if /health stamped, advancing clock by 500 wouldn't trigger exit.
        # We test by driving one watchdog tick after /health and checking exit not fired.
        app = srv.create_app(
            model_loader=lambda: (lambda p: []),
            idle_timeout=900,
            clock=fake_clock,
            exit_signal=fake_exit,
        )
        with TestClient(app) as client:
            # Record initial last_activity (set at startup)
            clock_val[0] = 1000.0  # advance past threshold from 0
            # GET /health — must NOT stamp last_activity
            client.get("/health")
            # Now manually tick the watchdog: idle > 900s from startup, inflight=0
            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(app.extra["_state"], 900, fake_clock, fake_exit)
            )
            # Should exit because /health didn't reset the timer
            assert fired, "/health must not stamp last_activity (watchdog should fire)"


class TestWatchdog:
    """Watchdog tick behavior: exit on idle, block on inflight, reset on activity."""

    def _make_app_and_state(self, idle_timeout: int, clock, exit_fn):
        """Helper: create app + extract _state from app.extra."""
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        app = srv.create_app(
            model_loader=lambda: (lambda p: []),
            idle_timeout=idle_timeout,
            clock=clock,
            exit_signal=exit_fn,
        )
        return app, srv

    def test_watchdog_exits_at_idle_threshold(self):
        """Watchdog tick with now >= last_activity+timeout and inflight==0 → exit_signal called."""
        fired: list[bool] = []
        clock_val: list[float] = [0.0]

        def fake_clock():
            return clock_val[0]

        def fake_exit():
            fired.append(True)

        app, srv = self._make_app_and_state(900, fake_clock, fake_exit)
        with TestClient(app):
            state = app.extra["_state"]
            # Set last_activity to 0.0, advance clock past threshold
            state["last_activity"] = 0.0
            state["inflight"] = 0
            clock_val[0] = 900.0

            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(state, 900, fake_clock, fake_exit)
            )
            assert fired, "exit_signal must be called when idle >= threshold and inflight==0"

    def test_watchdog_blocked_by_inflight(self):
        """Watchdog must NOT exit while inflight > 0, even past threshold."""
        fired: list[bool] = []
        clock_val: list[float] = [0.0]

        def fake_clock():
            return clock_val[0]

        def fake_exit():
            fired.append(True)

        app, srv = self._make_app_and_state(900, fake_clock, fake_exit)
        with TestClient(app):
            state = app.extra["_state"]
            state["last_activity"] = 0.0
            state["inflight"] = 1  # in-flight request
            clock_val[0] = 1000.0  # past threshold

            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(state, 900, fake_clock, fake_exit)
            )
            assert not fired, "exit_signal must NOT be called while inflight > 0"

            # Now drain inflight and tick again
            state["inflight"] = 0
            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(state, 900, fake_clock, fake_exit)
            )
            assert fired, "exit_signal must be called after inflight drops to 0 past threshold"

    def test_watchdog_activity_resets_timer(self):
        """A new last_activity stamp resets the idle window; watchdog should not exit."""
        fired: list[bool] = []
        clock_val: list[float] = [0.0]

        def fake_clock():
            return clock_val[0]

        def fake_exit():
            fired.append(True)

        app, srv = self._make_app_and_state(900, fake_clock, fake_exit)
        with TestClient(app):
            state = app.extra["_state"]
            state["inflight"] = 0

            # Simulate: idle for 800s, then a new request stamps at t=800
            state["last_activity"] = 800.0
            clock_val[0] = 900.0  # only 100s elapsed since stamp → threshold not met

            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(state, 900, fake_clock, fake_exit)
            )
            assert not fired, "watchdog must not exit when only 100s elapsed since last_activity"

    def test_watchdog_does_not_exit_just_below_threshold(self):
        """Clock at exactly last_activity + timeout - 1 → no exit."""
        fired: list[bool] = []
        clock_val: list[float] = [0.0]

        def fake_clock():
            return clock_val[0]

        def fake_exit():
            fired.append(True)

        app, srv = self._make_app_and_state(900, fake_clock, fake_exit)
        with TestClient(app):
            state = app.extra["_state"]
            state["last_activity"] = 0.0
            state["inflight"] = 0
            clock_val[0] = 899.0  # one second short

            asyncio.get_event_loop().run_until_complete(
                srv._watchdog_tick(state, 900, fake_clock, fake_exit)
            )
            assert not fired, "watchdog must not exit when elapsed < threshold"


class TestWatchdogDisabled:
    """idle_timeout <= 0 → no watchdog task created; sleep mode disabled."""

    def test_watchdog_disabled_idle_timeout_zero(self):
        """create_app(idle_timeout=0) → lifespan starts NO watchdog task."""
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        app = srv.create_app(model_loader=lambda: (lambda p: []), idle_timeout=0)
        with TestClient(app):
            state = app.extra["_state"]
            assert state.get("watchdog_task") is None, \
                "watchdog_task must be None when idle_timeout=0"

    def test_watchdog_disabled_idle_timeout_negative(self):
        """create_app(idle_timeout=-1) → lifespan starts NO watchdog task."""
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        app = srv.create_app(model_loader=lambda: (lambda p: []), idle_timeout=-1)
        with TestClient(app):
            state = app.extra["_state"]
            assert state.get("watchdog_task") is None, \
                "watchdog_task must be None when idle_timeout=-1"

    def test_watchdog_disabled_logs_sleep_mode_disabled(self, caplog):
        """When idle_timeout <= 0, log must contain 'sleep mode disabled'."""
        import logging
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        with caplog.at_level(logging.INFO):
            app = srv.create_app(model_loader=lambda: (lambda p: []), idle_timeout=0)
            with TestClient(app):
                pass

        assert any("sleep mode disabled" in r.message for r in caplog.records), \
            "Expected 'sleep mode disabled' log message when idle_timeout=0"


class TestLifespanWatchdogEnabled:
    """When idle_timeout > 0, lifespan must start a watchdog task but NOT load the model."""

    def test_lifespan_starts_watchdog_loads_no_model(self):
        """Enter lifespan with idle_timeout=900: watchdog_task created, loader uncalled."""
        count: list[int] = []
        import sys
        if "benchmarks.pyannote_diarization.server" in sys.modules:
            del sys.modules["benchmarks.pyannote_diarization.server"]
        import benchmarks.pyannote_diarization.server as srv

        def counting_loader():
            count.append(1)
            return lambda p: []

        # Use a fake clock and a no-op sleep to avoid real 30s waits
        app = srv.create_app(
            model_loader=counting_loader,
            idle_timeout=900,
            clock=lambda: 0.0,
            sleep=lambda _: asyncio.sleep(0),  # yield but don't wait
        )
        with TestClient(app):
            state = app.extra["_state"]
            assert count == [], "model must NOT be loaded during lifespan startup"
            assert state.get("watchdog_task") is not None, \
                "watchdog_task must be created when idle_timeout > 0"
