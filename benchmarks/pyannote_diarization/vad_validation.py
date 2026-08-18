#!/usr/bin/env python3
"""Validate NO_DIARIZED_SPEECH candidates against offline VAD speech intervals."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any


CANDIDATE_LABEL = "NO_DIARIZED_SPEECH"
REJECTED_LABEL = "REJECTED_VOICE_PRESENT"
CONFIRMED_LABEL = "CONFIRMED_NO_VOICE"
SCOPE_NOTE = (
    "VAD validates voice activity versus no voice only; applause classification remains separate."
)


class ValidationError(ValueError):
    """Raised when candidate or VAD artifacts cannot be safely compared."""


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise ValidationError(f"{field_name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise ValidationError(f"{field_name} must be a finite number") from error
    if not math.isfinite(number):
        raise ValidationError(f"{field_name} must be a finite number")
    return number


def _read_artifact(path: Path, name: str) -> dict[str, Any]:
    if not path.is_file():
        raise ValidationError(f"{name} must be a regular file")
    try:
        artifact = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ValidationError(f"{name} must contain valid JSON") from error
    if not isinstance(artifact, dict):
        raise ValidationError(f"{name} must be a JSON object")
    return artifact


def _intervals(artifact: dict[str, Any], field_name: str) -> list[dict[str, Any]]:
    intervals = artifact.get("intervals")
    if not isinstance(intervals, list):
        raise ValidationError(f"{field_name}.intervals must be a list")
    validated: list[dict[str, Any]] = []
    for index, interval in enumerate(intervals):
        if not isinstance(interval, dict):
            raise ValidationError(f"{field_name}.intervals[{index}] must be an object")
        start = _finite_number(interval.get("start_seconds"), f"{field_name}.intervals[{index}].start_seconds")
        end = _finite_number(interval.get("end_seconds"), f"{field_name}.intervals[{index}].end_seconds")
        if end <= start:
            raise ValidationError(
                f"{field_name}.intervals[{index}].end_seconds must be greater than start_seconds"
            )
        validated.append({**interval, "start_seconds": start, "end_seconds": end})
    return validated


def validate_candidate_intervals(
    candidate_artifact: dict[str, Any], vad_speech_artifact: dict[str, Any]
) -> list[dict[str, object]]:
    """Label each candidate from positive-duration overlap with VAD speech activity."""
    candidates = _intervals(candidate_artifact, "candidates")
    speech_intervals = _intervals(vad_speech_artifact, "vad_speech_intervals")
    results: list[dict[str, object]] = []

    for candidate_index, candidate in enumerate(candidates):
        if candidate.get("label") != CANDIDATE_LABEL:
            raise ValidationError(
                f"candidates.intervals[{candidate_index}] must have label {CANDIDATE_LABEL}"
            )
        start = candidate["start_seconds"]
        end = candidate["end_seconds"]
        overlap_indexes = [
            speech_index
            for speech_index, speech in enumerate(speech_intervals)
            if max(start, speech["start_seconds"]) < min(end, speech["end_seconds"])
        ]
        results.append(
            {
                "start_seconds": start,
                "end_seconds": end,
                "label": REJECTED_LABEL if overlap_indexes else CONFIRMED_LABEL,
                "evidence": {
                    "candidate_interval_index": candidate_index,
                    "candidate_source_label": CANDIDATE_LABEL,
                    "overlapping_vad_speech_interval_indexes": overlap_indexes,
                },
            }
        )
    return results


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidates", required=True, help="candidate-interval JSON artifact")
    parser.add_argument(
        "--vad-speech-intervals", required=True, help="offline VAD speech-interval JSON artifact"
    )
    parser.add_argument("--output", required=True, help="separate VAD-validated JSON artifact")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    candidate_path = Path(args.candidates)
    vad_speech_path = Path(args.vad_speech_intervals)
    output_path = Path(args.output)
    try:
        if output_path.resolve() in {candidate_path.resolve(), vad_speech_path.resolve()}:
            raise ValidationError("output must be separate from input artifacts")
        candidates = _read_artifact(candidate_path, "candidates")
        vad_speech = _read_artifact(vad_speech_path, "vad_speech_intervals")
        intervals = validate_candidate_intervals(candidates, vad_speech)
        artifact = {
            "source_candidate_intervals_path": str(candidate_path),
            "source_vad_speech_intervals_path": str(vad_speech_path),
            "source_summary_path": candidates.get("source_summary_path"),
            "source_vad_audio_path": vad_speech.get("source_audio_path"),
            "source_vad_model_id": vad_speech.get("model_id"),
            "rules": {
                "scope": SCOPE_NOTE,
                "applause_classification": "not performed",
                "overlap": "positive-duration intersection only; touching endpoints do not overlap",
            },
            "candidate_count": len(intervals),
            "intervals": intervals,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(artifact, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, ValidationError) as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
