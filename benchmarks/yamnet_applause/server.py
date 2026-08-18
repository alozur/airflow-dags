"""FastAPI sidecar service for YAMNet TFLite applause detection.

Exposes ``POST /detect`` accepting a WAV upload (multipart form) and an
``offset`` float. Applies the offset to every ``start`` and ``end`` timestamp
before returning so the caller receives absolute session timestamps.
The model is loaded once at startup via the ``lifespan`` context; a
``model_loader`` callable can be injected for testing so no tflite/numpy
import occurs during the unit test suite.

Usage (production)::

    uvicorn server:app --host 0.0.0.0 --port ${YAMNET_API_PORT:-8081}

For testing, use ``create_app(model_loader=...)`` to inject a stub.
"""
from __future__ import annotations

import logging
import os
import tempfile
from contextlib import asynccontextmanager
from typing import Annotated, Callable

from fastapi import FastAPI, File, Form, HTTPException, UploadFile

logger = logging.getLogger(__name__)

_YAMNET_API_PORT = int(os.environ.get("YAMNET_API_PORT", "8081"))


def _default_model_loader() -> Callable[[str], list[dict]]:
    """Load the YAMNet TFLite model and return a callable inference function.

    This is the real production loader. It imports tflite_runtime, numpy and
    soundfile only when called (at lifespan startup), so the module is
    importable without those deps present.
    """
    import csv
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


def create_app(
    *,
    model_loader: Callable[[], Callable[[str], list[dict]]] = _default_model_loader,
) -> FastAPI:
    """Construct and return the FastAPI application.

    Args:
        model_loader: Zero-argument callable that returns the inference
            function ``(wav_path: str) -> list[dict]``.  The default loads
            the real YAMNet TFLite model.  Pass a stub for tests.
    """
    _state: dict = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("yamnet-api: loading model …")
        _state["infer"] = model_loader()
        logger.info("yamnet-api: model ready")
        yield
        _state.clear()

    application = FastAPI(title="yamnet-api", lifespan=lifespan)

    @application.get("/health")
    async def health():
        return {"status": "ok"}

    @application.post("/detect")
    async def detect(
        audio_file: Annotated[UploadFile, File(description="WAV audio bytes")],
        offset: Annotated[float, Form(description="Turn start offset in seconds")] = 0.0,
    ):
        wav_bytes = await audio_file.read()

        tmp = tempfile.NamedTemporaryFile(suffix=".wav", delete=False)
        try:
            tmp.write(wav_bytes)
            tmp.flush()
            tmp.close()

            infer = _state.get("infer")
            if infer is None:
                raise HTTPException(status_code=503, detail="model not loaded")

            intervals: list[dict] = infer(tmp.name)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("yamnet-api: inference failed")
            raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

        if offset:
            intervals = [
                {**iv, "start": float(iv["start"]) + offset, "end": float(iv["end"]) + offset}
                for iv in intervals
            ]

        return {"applause_intervals": intervals}

    return application


# Production app — loaded when uvicorn starts this module directly.
app = create_app()
