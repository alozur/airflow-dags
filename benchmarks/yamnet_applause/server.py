"""FastAPI sidecar service for YAMNet TFLite applause detection.

Exposes ``POST /detect`` accepting a WAV upload (multipart form) and an
``offset`` float. Applies the offset to every ``start`` and ``end`` timestamp
before returning so the caller receives absolute session timestamps.

The model is loaded **lazily** on the first inference request (not at startup).
A concurrency lock ensures concurrent first-callers await a single load.
A background watchdog task monitors ``last_activity`` and exits the process
after ``IDLE_TIMEOUT_SECONDS`` of inactivity (compose ``restart: unless-stopped``
relaunches the server light, without reloading the model).

Set ``IDLE_TIMEOUT_SECONDS=0`` (or any negative integer) to disable the watchdog
and keep the model resident once loaded (v1 behavior).

Usage (production)::

    uvicorn server:app --host 0.0.0.0 --port ${YAMNET_API_PORT:-8081}

For testing, use ``create_app(model_loader=...)`` to inject a stub.
The ``clock``, ``sleep``, and ``exit_signal`` parameters are additional seams
for unit tests that need deterministic timing without real sleeps.

Note: this machinery is intentionally duplicated from the diarize-api server.
Compose build contexts are isolated per directory; a shared module would require
expanding both contexts to the repo root, rebuilding the whole repo into each
image and breaking the trivial ``COPY server.py`` paths.
"""
from __future__ import annotations

import asyncio
import csv
import logging
import os
import signal
import tempfile
import time
from contextlib import asynccontextmanager
from typing import Annotated, Callable

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

_YAMNET_API_PORT = int(os.environ.get("YAMNET_API_PORT", "8081"))


def _default_model_loader() -> Callable[[str], list[dict]]:
    """Load the YAMNet TFLite model and return a callable inference function.

    This is the real production loader. It imports tflite_runtime, numpy and
    soundfile only when called (at first inference), so the module is
    importable without those deps present.
    """
    import os as _os

    import numpy as np
    import soundfile as sf
    from tflite_runtime.interpreter import Interpreter

    cache = _os.environ.get("MODEL_CACHE_DIR", "/model-cache")
    model_path = _os.path.join(cache, "yamnet_classification.tflite")
    class_map_path = _os.path.join(cache, "yamnet_class_map.csv")

    # Find the applause class index.
    applause_idx: int | None = None
    with open(class_map_path, newline="") as fh:
        for row in csv.DictReader(fh):
            if row["display_name"] == "Applause":
                applause_idx = int(row["index"])
                break
    if applause_idx is None:
        raise RuntimeError("Applause class not found in YAMNet class map")

    TARGET_SR = 16000
    WINDOW_SAMPLES = 15600
    HOP_SECONDS = 0.48
    FRAME_SECONDS = WINDOW_SAMPLES / TARGET_SR
    HOP_SAMPLES = int(round(HOP_SECONDS * TARGET_SR))

    def _run(wav_path: str) -> list[dict]:
        data, sr = sf.read(wav_path, dtype="float32", always_2d=True)
        if sr != TARGET_SR:
            raise RuntimeError(f"Expected {TARGET_SR} Hz WAV, got {sr} Hz")
        waveform = data.mean(axis=1).astype(np.float32)
        duration = len(waveform) / TARGET_SR

        interp = Interpreter(model_path=model_path)
        in_idx = interp.get_input_details()[0]["index"]
        out_idx = interp.get_output_details()[0]["index"]
        interp.allocate_tensors()

        scores: list[np.ndarray] = []
        start = 0
        while start < len(waveform):
            window = waveform[start: start + WINDOW_SAMPLES]
            if len(window) < WINDOW_SAMPLES:
                window = np.pad(window, (0, WINDOW_SAMPLES - len(window)))
            interp.set_tensor(in_idx, window.astype(np.float32))
            interp.invoke()
            scores.append(interp.get_tensor(out_idx)[0])
            start += HOP_SAMPLES

        score_arr = np.asarray(scores, dtype=np.float32)
        applause = score_arr[:, applause_idx]

        # Simple threshold-based interval detection.
        threshold = 0.5
        min_duration = 3.0
        active = applause >= threshold

        intervals: list[dict] = []
        in_run = False
        run_start = 0
        for i, is_active in enumerate(active):
            if is_active and not in_run:
                in_run = True
                run_start = i
            elif not is_active and in_run:
                in_run = False
                s = run_start * HOP_SECONDS
                e = min(i * HOP_SECONDS + FRAME_SECONDS, duration)
                if (e - s) >= min_duration:
                    window_scores = applause[run_start:i]
                    intervals.append({
                        "start": round(s, 3),
                        "end": round(e, 3),
                        "max_score": round(float(window_scores.max()), 4),
                    })
        if in_run:
            s = run_start * HOP_SECONDS
            e = duration
            if (e - s) >= min_duration:
                window_scores = applause[run_start:]
                intervals.append({
                    "start": round(s, 3),
                    "end": round(e, 3),
                    "max_score": round(float(window_scores.max()), 4),
                })

        return intervals

    return _run


async def _watchdog_tick(
    state: dict,
    idle_timeout: int,
    clock: Callable[[], float],
    exit_signal: Callable[[], None],
) -> None:
    """Execute one watchdog check: exit if idle >= timeout and no requests in-flight.

    Exposed as a module-level async function so tests can call one tick directly
    without running the sleep loop.
    """
    now = clock()
    elapsed = now - state["last_activity"]
    if elapsed >= idle_timeout and state["inflight"] == 0:
        logger.info(
            "yamnet-api: idle %.0fs >= %ds, inflight=0 — exiting for restart",
            elapsed,
            idle_timeout,
        )
        exit_signal()


async def _watchdog_loop(
    state: dict,
    idle_timeout: int,
    watchdog_interval: float,
    clock: Callable[[], float],
    sleep: Callable,
    exit_signal: Callable[[], None],
) -> None:
    """Background loop: sleep interval then call one watchdog tick."""
    while True:
        await sleep(watchdog_interval)
        await _watchdog_tick(state, idle_timeout, clock, exit_signal)


def create_app(
    *,
    model_loader: Callable[[], Callable[[str], list[dict]]] = _default_model_loader,
    idle_timeout: int = int(os.environ.get("IDLE_TIMEOUT_SECONDS", "900")),
    watchdog_interval: float = float(os.environ.get("WATCHDOG_INTERVAL_SECONDS", "30")),
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable = asyncio.sleep,
    exit_signal: Callable[[], None] | None = None,
) -> FastAPI:
    """Construct and return the FastAPI application.

    Args:
        model_loader: Zero-argument callable that returns the inference
            function ``(wav_path: str) -> list[dict]``.  The default loads
            the real YAMNet TFLite model.  Pass a stub for tests.
        idle_timeout: Seconds of inactivity before the watchdog exits the
            process.  ``<= 0`` disables the watchdog entirely.
        watchdog_interval: How often (seconds) the watchdog loop wakes up.
        clock: Callable returning a monotonic timestamp (seam for tests).
        sleep: Awaitable sleep function (seam for tests).
        exit_signal: Callable invoked when the watchdog decides to exit.
            Defaults to a flag on ``_state``; production binds
            ``server.should_exit = True`` on the uvicorn Server instance.
    """
    _state: dict = {
        "infer": None,
        "load_lock": asyncio.Lock(),
        "last_activity": clock(),
        "inflight": 0,
        "watchdog_task": None,
    }

    # Default exit_signal: send SIGTERM to own process so uvicorn's signal
    # handler triggers a graceful shutdown (drains in-flight, runs lifespan
    # cleanup, exits 0).  Tests inject a fake callable instead.
    def _default_exit_signal() -> None:
        _state["should_exit"] = True
        os.kill(os.getpid(), signal.SIGTERM)

    _exit_signal = exit_signal if exit_signal is not None else _default_exit_signal

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        # Model is NOT loaded at startup — lazy load happens on first /detect.
        if idle_timeout > 0:
            _state["watchdog_task"] = asyncio.create_task(
                _watchdog_loop(
                    _state,
                    idle_timeout,
                    watchdog_interval,
                    clock,
                    sleep,
                    _exit_signal,
                )
            )
        else:
            logger.info(
                "yamnet-api: IDLE_TIMEOUT_SECONDS<=0 — sleep mode disabled, model stays resident"
            )
        yield
        # Cleanup: cancel watchdog on shutdown.
        task = _state.get("watchdog_task")
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        _state.clear()

    application = FastAPI(title="yamnet-api", lifespan=lifespan)
    # Expose _state via app.extra so tests can inspect it.
    application.extra["_state"] = _state

    @application.exception_handler(RequestValidationError)
    async def _stamp_on_validation_error(request, exc: RequestValidationError):
        """Stamp last_activity for malformed inference POSTs (FastAPI raises before route body)."""
        _state["last_activity"] = clock()
        return JSONResponse(
            status_code=422,
            content={"detail": jsonable_encoder(exc.errors())},
        )

    @application.get("/health")
    async def health():
        # Intentionally untouched: no load, no stamp, no wake.
        return {"status": "ok"}

    @application.post("/detect")
    async def detect(
        audio_file: Annotated[UploadFile, File(description="WAV audio bytes")],
        offset: Annotated[float, Form(description="Turn start offset in seconds")] = 0.0,
    ):
        # Stamp activity at entry (long inferences must not let the timer fire mid-run).
        _state["last_activity"] = clock()
        _state["inflight"] += 1
        try:
            # Lazy model load — only on the first request; lock prevents double-load.
            if _state["infer"] is None:
                async with _state["load_lock"]:
                    if _state["infer"] is None:  # double-checked inside lock
                        logger.info("yamnet-api: loading model …")
                        t0 = clock()
                        _state["infer"] = model_loader()
                        logger.info("yamnet-api: model ready in %.1fs", clock() - t0)

            wav_bytes = await audio_file.read()

            tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
            try:
                tmp.write(wav_bytes)
                tmp.flush()
                tmp.close()

                intervals: list[dict] = _state["infer"](tmp.name)
            except Exception as exc:
                logger.exception("yamnet-api: inference failed")
                raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc
            finally:
                try:
                    os.unlink(tmp.name)
                except OSError:
                    pass
        finally:
            _state["inflight"] -= 1
            # Stamp exit time: measures idle from when work completed.
            _state["last_activity"] = clock()

        if offset:
            intervals = [
                {**iv, "start": float(iv["start"]) + offset, "end": float(iv["end"]) + offset}
                for iv in intervals
            ]

        return {"applause_intervals": intervals}

    return application


# Production app — loaded when uvicorn starts this module directly.
app = create_app()
