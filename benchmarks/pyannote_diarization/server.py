"""FastAPI sidecar service for pyannote speaker diarization.

Exposes ``POST /diarize`` accepting a WAV upload (multipart form) and a
``chapter_offset`` float. Applies the offset to every ``start_seconds``
timestamp before returning so the caller receives absolute session timestamps.
The model is loaded once at startup via the ``lifespan`` context; a
``model_loader`` callable can be injected for testing so no real torch/pyannote
import occurs during the unit test suite.

Usage (production)::

    uvicorn server:app --host 0.0.0.0 --port ${DIARIZE_API_PORT:-8080}

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

_DIARIZE_API_PORT = int(os.environ.get("DIARIZE_API_PORT", "8080"))


def _default_model_loader() -> Callable[[str], list[dict]]:
    """Load the pyannote pipeline and return a callable inference function.

    This is the real production loader. It imports torch and pyannote only
    when called (at lifespan startup), so the module is importable without
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
    pipeline = Pipeline.from_pretrained(model_id, use_auth_token=token)
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


def create_app(
    *,
    model_loader: Callable[[], Callable[[str], list[dict]]] = _default_model_loader,
) -> FastAPI:
    """Construct and return the FastAPI application.

    Args:
        model_loader: Zero-argument callable that returns the inference
            function ``(wav_path: str) -> list[dict]``.  The default loads
            the real pyannote pipeline.  Pass a stub for tests.
    """
    _state: dict = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logger.info("diarize-api: loading model …")
        _state["infer"] = model_loader()
        logger.info("diarize-api: model ready")
        yield
        _state.clear()

    application = FastAPI(title="diarize-api", lifespan=lifespan)

    @application.get("/health")
    async def health():
        return {"status": "ok"}

    @application.post("/diarize")
    async def diarize(
        audio_file: Annotated[UploadFile, File(description="WAV audio bytes")],
        chapter_offset: Annotated[float, Form(description="Chapter start offset in seconds")] = 0.0,
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

            changes: list[dict] = infer(tmp.name)
        except HTTPException:
            raise
        except Exception as exc:
            logger.exception("diarize-api: inference failed")
            raise HTTPException(status_code=500, detail={"error": str(exc)}) from exc
        finally:
            try:
                os.unlink(tmp.name)
            except OSError:
                pass

        if chapter_offset:
            changes = [
                {**ch, "start_seconds": float(ch["start_seconds"]) + chapter_offset}
                for ch in changes
            ]

        return {"speaker_changes": changes}

    return application


# Production app — loaded when uvicorn starts this module directly.
app = create_app()
