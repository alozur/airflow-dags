#!/usr/bin/env python3
"""Pure report construction for the standalone whisper.cpp TinyDiarize benchmark."""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

SAMPLE_DURATION_SECONDS = 1_800


def _finite_number(value: object, field_name: str) -> float:
    """Return a finite numeric value or raise a contract-focused error."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a finite number")
    try:
        parsed = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{field_name} must be a finite number") from error
    if not math.isfinite(parsed):
        raise ValueError(f"{field_name} must be a finite number")
    return parsed


def validate_full_duration(value: object) -> float:
    """Ensure the source can supply the fixed 30-minute benchmark sample."""
    try:
        duration = _finite_number(value, "full video duration")
    except ValueError as error:
        raise ValueError("full video duration must be at least 1,800 seconds for this benchmark") from error
    if duration < SAMPLE_DURATION_SECONDS:
        raise ValueError("full video duration must be at least 1,800 seconds for this benchmark")
    return duration


def build_report(
    *,
    full_video_duration_seconds: object,
    audio_extraction_seconds: object,
    diarization_seconds: object,
    container_end_to_end_seconds: object,
    speaker_turn_marker_count: int,
    diarization_exit_status: int,
    image_identity: str,
    model_identity: str,
    timestamp: str,
    audio_path: str,
    raw_cli_output_path: str,
    report_path: str,
    image_build_seconds: object,
) -> dict[str, Any]:
    """Build a transcript-free report; non-zero CLI exits are always failures."""
    if (
        isinstance(speaker_turn_marker_count, bool)
        or not isinstance(speaker_turn_marker_count, int)
        or speaker_turn_marker_count < 0
    ):
        raise ValueError("speaker turn marker count must be a non-negative integer")
    if isinstance(diarization_exit_status, bool) or not isinstance(diarization_exit_status, int):
        raise ValueError("diarization exit status must be an integer")

    return {
        "schema_version": 1,
        "status": "success" if diarization_exit_status == 0 else "failed",
        "full_video_duration_seconds": validate_full_duration(full_video_duration_seconds),
        "processed_sample_duration_seconds": SAMPLE_DURATION_SECONDS,
        "timing_seconds": {
            "image_build_host": _finite_number(image_build_seconds, "image build seconds"),
            "audio_extraction": _finite_number(audio_extraction_seconds, "audio extraction seconds"),
            "diarization": _finite_number(diarization_seconds, "diarization seconds"),
            "container_end_to_end": _finite_number(container_end_to_end_seconds, "container end-to-end seconds"),
        },
        "speaker_turn_marker_count": speaker_turn_marker_count,
        "diarization_exit_status": diarization_exit_status,
        "image_identity": image_identity,
        "model_identity": model_identity,
        "timestamp": timestamp,
        "output_paths": {
            "audio": audio_path,
            "raw_cli_output": raw_cli_output_path,
            "report": report_path,
        },
    }


def _arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Write a TinyDiarize benchmark report.")
    parser.add_argument("--full-video-duration-seconds", required=True)
    parser.add_argument("--audio-extraction-seconds", required=True)
    parser.add_argument("--diarization-seconds", required=True)
    parser.add_argument("--container-end-to-end-seconds", required=True)
    parser.add_argument("--speaker-turn-marker-count", required=True, type=int)
    parser.add_argument("--diarization-exit-status", required=True, type=int)
    parser.add_argument("--image-identity", required=True)
    parser.add_argument("--model-identity", required=True)
    parser.add_argument("--timestamp", required=True)
    parser.add_argument("--audio-path", required=True)
    parser.add_argument("--raw-cli-output-path", required=True)
    parser.add_argument("--report-path", required=True)
    parser.add_argument("--image-build-seconds", required=True)
    parser.add_argument("--output", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write one report without exposing the raw transcript in its JSON payload."""
    args = _arguments(argv)
    report = build_report(
        full_video_duration_seconds=args.full_video_duration_seconds,
        audio_extraction_seconds=args.audio_extraction_seconds,
        diarization_seconds=args.diarization_seconds,
        container_end_to_end_seconds=args.container_end_to_end_seconds,
        speaker_turn_marker_count=args.speaker_turn_marker_count,
        diarization_exit_status=args.diarization_exit_status,
        image_identity=args.image_identity,
        model_identity=args.model_identity,
        timestamp=args.timestamp,
        audio_path=args.audio_path,
        raw_cli_output_path=args.raw_cli_output_path,
        report_path=args.report_path,
        image_build_seconds=args.image_build_seconds,
    )
    args.output.write_text(
        json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
