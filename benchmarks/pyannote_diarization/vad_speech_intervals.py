#!/usr/bin/env python3
"""Extract transcript-free speech intervals with an offline pyannote.audio v4 VAD pipeline."""

from __future__ import annotations

import argparse
import json
import os
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import Any


class VADDependencyError(ValueError):
    """Raised when the pinned offline VAD dependency contract is unavailable."""


def _load_parameters(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise VADDependencyError("pipeline parameters must be a regular file in the model cache")
    try:
        parameters = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise VADDependencyError("pipeline parameters must contain a JSON object") from error
    if not isinstance(parameters, dict):
        raise VADDependencyError("pipeline parameters must contain a JSON object")
    unsupported = [parameter for parameter in ("onset", "offset") if parameter in parameters]
    if unsupported:
        raise VADDependencyError(
            "unsupported pyannote.audio v4 powerset parameters: "
            f"{', '.join(unsupported)}; onset and offset are fixed at 0.5, so use only "
            "min_duration_on and min_duration_off"
        )
    return parameters


def _load_vad_pipeline(model_id: str, parameters: dict[str, Any]):
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"
    try:
        installed_version = version("pyannote.audio")
    except PackageNotFoundError as error:
        raise VADDependencyError("pyannote.audio v4 is not installed in this image") from error
    if not installed_version.startswith("4."):
        raise VADDependencyError(
            f"pyannote.audio v4 is required; installed version is {installed_version}"
        )

    try:
        import torch
        from pyannote.audio import Model
        from pyannote.audio.pipelines import VoiceActivityDetection

        model = Model.from_pretrained(model_id)
        pipeline = VoiceActivityDetection(segmentation=model)
        pipeline.instantiate(parameters)
        pipeline.to(torch.device("cpu"))
    except Exception as error:
        raise VADDependencyError(
            "offline VAD dependency/model-cache contract failed; provide an already cached "
            "pyannote.audio v4 segmentation model and its verified VoiceActivityDetection "
            "parameter JSON"
        ) from error
    return pipeline


def extract_speech_intervals(audio_path: Path, model_id: str, parameters: dict[str, Any]) -> list[dict[str, float]]:
    """Run the verified VAD pipeline and return sorted speech activity intervals."""
    if not audio_path.is_file():
        raise VADDependencyError("audio must be a regular file")
    pipeline = _load_vad_pipeline(model_id, parameters)
    try:
        result = pipeline(str(audio_path))
        timeline = result.get_timeline() if hasattr(result, "get_timeline") else result
        intervals = [
            {"start_seconds": round(float(segment.start), 6), "end_seconds": round(float(segment.end), 6)}
            for segment in timeline.support()
            if segment.end > segment.start
        ]
    except Exception as error:
        raise VADDependencyError(
            "offline VAD inference failed; the cached model and parameters must be compatible "
            "with pyannote.audio v4 VoiceActivityDetection"
        ) from error
    return sorted(intervals, key=lambda interval: (interval["start_seconds"], interval["end_seconds"]))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audio", required=True, help="read-only full-run WAV")
    parser.add_argument("--model-id", required=True, help="pretrained segmentation model already cached")
    parser.add_argument(
        "--pipeline-parameters",
        required=True,
        help="verified VoiceActivityDetection parameter JSON in the read-only model cache",
    )
    parser.add_argument("--output", required=True, help="separate VAD speech-interval JSON artifact")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    audio_path = Path(args.audio)
    parameters_path = Path(args.pipeline_parameters)
    output_path = Path(args.output)
    try:
        if audio_path.resolve() == output_path.resolve():
            raise VADDependencyError("output must be separate from audio")
        intervals = extract_speech_intervals(audio_path, args.model_id, _load_parameters(parameters_path))
        artifact = {
            "source_audio_path": str(audio_path),
            "model_id": args.model_id,
            "intervals": intervals,
            "rules": {
                "scope": "VAD distinguishes voice activity versus no voice only; applause classification remains separate.",
                "offline": True,
            },
        }
        output_path.write_text(json.dumps(artifact, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, VADDependencyError) as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
