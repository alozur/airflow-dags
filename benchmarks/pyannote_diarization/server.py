"""FastAPI sidecar service for pyannote speaker diarization.

Exposes ``POST /diarize`` accepting a WAV upload (multipart form) and a
``chapter_offset`` float. Applies the offset to every ``start_seconds``
timestamp before returning so the caller receives absolute session timestamps.

The model is loaded **lazily** on the first inference request (not at startup).
A concurrency lock ensures concurrent first-callers await a single load.
A background watchdog task monitors ``last_activity`` and exits the process
after ``IDLE_TIMEOUT_SECONDS`` of inactivity (compose ``restart: unless-stopped``
relaunches the server light, without reloading the model).

Set ``IDLE_TIMEOUT_SECONDS=0`` (or any negative integer) to disable the watchdog
and keep the model resident once loaded (v1 behavior).

Usage (production)::

    uvicorn server:app --host 0.0.0.0 --port ${DIARIZE_API_PORT:-8080}

For testing, use ``create_app(model_loader=...)`` to inject a stub.
The ``clock``, ``sleep``, and ``exit_signal`` parameters are additional seams
for unit tests that need deterministic timing without real sleeps.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import tempfile
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

_DIARIZE_API_PORT = int(os.environ.get("DIARIZE_API_PORT", "8080"))


def _default_model_loader() -> Callable[[str], list[dict]]:
    """Load the pyannote pipeline and return a callable inference function.

    This is the real production loader. It imports torch and pyannote only
    when called (at first inference), so the module is importable without
    those heavy deps present.
    """
    import os as _os

    model_cache = _os.environ.get("MODEL_CACHE_DIR") or _os.environ.get("HF_HOME")
    if model_cache:
        _os.environ["HF_HOME"] = model_cache

    token = _os.environ.get("HF_TOKEN")
    if not token:
        raise RuntimeError("HF_TOKEN must be set for the diarize-api")

    logging.getLogger("pyannote").setLevel(logging.ERROR)
    logging.getLogger("pytorch_lightning").setLevel(logging.ERROR)

    import torch
    from pyannote.audio import Pipeline

    model_id = "pyannote/speaker-diarization-community-1"
    pipeline = Pipeline.from_pretrained(model_id, token=token)
    pipeline.to(torch.device("cpu"))

    def _infer(wav_path: str) -> list[dict]:
        output = pipeline(wav_path)
        turns = (
            (turn.start, turn.end, speaker)
            for turn, _, speaker in output.speaker_diarization.itertracks(yield_label=True)
        )
        return _to_speaker_changes(turns)

    return _infer


def _to_speaker_changes(
    turns: list[tuple[float, float, str]],
) -> list[dict]:
    """Convert raw (start, end, label) diarization turns to speaker-change records.

    Returns a list of ``{start_seconds, from_speaker, to_speaker,
    confirmed_block_duration_seconds}`` dicts representing transitions between
    consecutive speaker segments.
    """
    segments: list[dict] = []
    previous: dict | None = None

    for start, end, label in turns:
        seg = {"start": float(start), "end": float(end), "label": label}
        if previous is not None and previous["label"] != label:
            segments.append({
                "start_seconds": seg["start"],
                "from_speaker": previous["label"],
                "to_speaker": label,
                "confirmed_block_duration_seconds": round(
                    previous["end"] - previous["start"], 6
                ),
            })
        previous = seg

    return segments


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
            "diarize-api: idle %.0fs >= %ds, inflight=0 — exiting for restart",
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
            the real pyannote pipeline.  Pass a stub for tests.
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
        "infer_lock": asyncio.Lock(),
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
        # Model is NOT loaded at startup — lazy load happens on first /diarize.
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
                "diarize-api: IDLE_TIMEOUT_SECONDS<=0 — sleep mode disabled, model stays resident"
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

    application = FastAPI(title="diarize-api", lifespan=lifespan)
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

    @application.post("/diarize")
    async def diarize(
        audio_file: Annotated[UploadFile, File(description="WAV audio bytes")],
        chapter_offset: Annotated[float, Form(description="Chapter start offset in seconds")] = 0.0,
    ):
        # Stamp activity at entry (long inferences must not let the timer fire mid-run).
        _state["last_activity"] = clock()
        _state["inflight"] += 1
        try:
            # Lazy model load — only on the first request; lock prevents double-load.
            if _state["infer"] is None:
                async with _state["load_lock"]:
                    if _state["infer"] is None:  # double-checked inside lock
                        logger.info("diarize-api: loading model …")
                        t0 = clock()
                        _state["infer"] = model_loader()
                        logger.info("diarize-api: model ready in %.1fs", clock() - t0)

            wav_bytes = await audio_file.read()

            tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
            try:
                tmp.write(wav_bytes)
                tmp.flush()
                tmp.close()

                async with _state["infer_lock"]:
                    changes: list[dict] = await asyncio.to_thread(_state["infer"], tmp.name)
            except Exception as exc:
                logger.exception("diarize-api: inference failed")
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

        if chapter_offset:
            changes = [
                {**ch, "start_seconds": float(ch["start_seconds"]) + chapter_offset}
                for ch in changes
            ]

        return {"speaker_changes": changes}

    return application


# Production app — loaded when uvicorn starts this module directly.
app = create_app()
