#!/usr/bin/env python3
"""Run an offline, transcript-free Pyannote speaker-diarization calibration."""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from collections.abc import Iterable
from pathlib import Path

MODEL_ID = "pyannote/speaker-diarization-community-1"


class BenchmarkConfigurationError(ValueError):
    """Raised when the benchmark cannot safely start."""


def estimate_full_runtime_seconds(
    *,
    diarization_seconds: float,
    sample_duration_seconds: float,
    full_video_duration_seconds: float,
) -> float:
    """Linearly extrapolate inference time from a fixed audio calibration window."""
    values = (diarization_seconds, sample_duration_seconds, full_video_duration_seconds)
    if not all(math.isfinite(value) for value in values):
        raise BenchmarkConfigurationError("runtime values must be finite")
    if diarization_seconds < 0 or full_video_duration_seconds < 0:
        raise BenchmarkConfigurationError("runtime values must not be negative")
    if sample_duration_seconds <= 0:
        raise BenchmarkConfigurationError("sample duration must be greater than zero")
    return round(diarization_seconds * full_video_duration_seconds / sample_duration_seconds, 6)


def summarize_turns(turns: Iterable[tuple[float, float, str]]) -> dict[str, object]:
    """Return timing-only turns and deterministic anonymous cluster-label changes."""
    labels: dict[str, str] = {}
    raw_turns: list[dict[str, object]] = []
    changes: list[dict[str, object]] = []
    previous_label: str | None = None

    for start, end, source_label in turns:
        start_seconds = round(float(start), 3)
        end_seconds = round(float(end), 3)
        if not (math.isfinite(start_seconds) and math.isfinite(end_seconds)):
            raise BenchmarkConfigurationError("turn timestamps must be finite")
        if end_seconds < start_seconds:
            raise BenchmarkConfigurationError("turn end must not precede turn start")

        source_label = str(source_label)
        anonymous_label = labels.setdefault(source_label, f"SPEAKER_{len(labels):02d}")
        raw_turns.append(
            {
                "start_seconds": start_seconds,
                "end_seconds": end_seconds,
                "speaker_label": anonymous_label,
            }
        )
        if previous_label is not None and anonymous_label != previous_label:
            changes.append(
                {
                    "timestamp_seconds": start_seconds,
                    "previous_speaker_label": previous_label,
                    "speaker_label": anonymous_label,
                }
            )
        previous_label = anonymous_label

    return {
        "speaker_cluster_count": len(labels),
        "raw_turns": raw_turns,
        "speaker_change_count": len(changes),
        "timestamp_anonymous_label_changes": changes,
    }


def _configure_model_cache() -> None:
    model_cache = os.environ.get("MODEL_CACHE_DIR") or os.environ.get("HF_HOME")
    if model_cache:
        os.environ["HF_HOME"] = model_cache


def _read_token() -> str:
    token = os.environ.get("HF_TOKEN")
    if not token:
        raise BenchmarkConfigurationError("HF_TOKEN must be set in the environment")
    return token


def _load_pipeline():
    _configure_model_cache()
    token = _read_token()
    logging.getLogger("pyannote").setLevel(logging.ERROR)
    logging.getLogger("pytorch_lightning").setLevel(logging.ERROR)

    import torch
    from pyannote.audio import Pipeline

    pipeline = Pipeline.from_pretrained(MODEL_ID, token=token)
    pipeline.to(torch.device("cpu"))
    return pipeline


def _prepare_model() -> int:
    _load_pipeline()
    return 0


def _run_diarization(args: argparse.Namespace) -> int:
    audio_path = Path(args.audio)
    if not audio_path.is_file():
        raise BenchmarkConfigurationError("audio must be a regular file")

    pipeline = _load_pipeline()
    started_at = time.monotonic()
    output = pipeline(str(audio_path))
    diarization_seconds = round(time.monotonic() - started_at, 6)

    turns = (
        (turn.start, turn.end, speaker) for turn, _, speaker in output.speaker_diarization.itertracks(yield_label=True)
    )
    summary = {
        "model_id": MODEL_ID,
        "sample_duration_seconds": args.sample_duration_seconds,
        "full_video_duration_seconds": args.full_video_duration_seconds,
        "diarization_seconds": diarization_seconds,
        "extrapolated_full_video_diarization_seconds": estimate_full_runtime_seconds(
            diarization_seconds=diarization_seconds,
            sample_duration_seconds=args.sample_duration_seconds,
            full_video_duration_seconds=args.full_video_duration_seconds,
        ),
        **summarize_turns(turns),
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, sort_keys=True))
    return 0


def _positive_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--prepare-model", action="store_true")
    parser.add_argument("--audio")
    parser.add_argument("--output")
    parser.add_argument("--sample-duration-seconds", type=_positive_float)
    parser.add_argument("--full-video-duration-seconds", type=_positive_float)
    args = parser.parse_args(argv)

    if args.prepare_model:
        if any(
            value is not None
            for value in (
                args.audio,
                args.output,
                args.sample_duration_seconds,
                args.full_video_duration_seconds,
            )
        ):
            parser.error("--prepare-model cannot be combined with inference arguments")
    elif any(
        value is None
        for value in (
            args.audio,
            args.output,
            args.sample_duration_seconds,
            args.full_video_duration_seconds,
        )
    ):
        parser.error(
            "--audio, --output, --sample-duration-seconds, and --full-video-duration-seconds are required for inference"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.prepare_model:
            return _prepare_model()
        return _run_diarization(args)
    except BenchmarkConfigurationError as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    except Exception:
        # Do not surface upstream request details because they can contain HF credentials.
        print("diarization failed; inspect container logs without exposing HF_TOKEN", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
