#!/usr/bin/env python3
"""Derive transcript-free candidate cuts from an existing Pyannote summary."""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

CANDIDATE_LABEL = "NO_DIARIZED_SPEECH"
SCOPE_NOTE = (
    "Raw diarization gaps are candidate cuts only; audio classification and validation are "
    "required. They do not classify applause or non-speech."
)


class SummaryValidationError(ValueError):
    """Raised when a Pyannote summary cannot safely produce candidate intervals."""


def _finite_number(value: Any, field_name: str) -> float:
    if isinstance(value, bool):
        raise SummaryValidationError(f"{field_name} must be a finite number")
    try:
        number = float(value)
    except (TypeError, ValueError) as error:
        raise SummaryValidationError(f"{field_name} must be a finite number") from error
    if not math.isfinite(number):
        raise SummaryValidationError(f"{field_name} must be a finite number")
    return number


def _rounded(value: float) -> float:
    return round(value, 6)


def derive_candidate_intervals(summary: dict[str, Any], min_gap_seconds: float) -> list[dict[str, object]]:
    """Return gaps in raw diarization activity that meet the requested minimum duration."""
    duration = _finite_number(summary.get("full_video_duration_seconds"), "full_video_duration_seconds")
    if duration < 0:
        raise SummaryValidationError("full_video_duration_seconds must not be negative")

    raw_turns = summary.get("raw_turns")
    if not isinstance(raw_turns, list):
        raise SummaryValidationError("raw_turns must be a list")

    active_intervals: list[tuple[float, float]] = []
    for index, turn in enumerate(raw_turns):
        if not isinstance(turn, dict):
            raise SummaryValidationError(f"raw_turns[{index}] must be an object")
        start = _finite_number(turn.get("start_seconds"), f"raw_turns[{index}].start_seconds")
        end = _finite_number(turn.get("end_seconds"), f"raw_turns[{index}].end_seconds")
        if end < start:
            raise SummaryValidationError(f"raw_turns[{index}].end_seconds must not precede start_seconds")
        clamped_start = max(0.0, start)
        clamped_end = min(duration, end)
        if clamped_end > clamped_start:
            active_intervals.append((clamped_start, clamped_end))

    merged_intervals: list[list[float]] = []
    for start, end in sorted(active_intervals):
        if merged_intervals and start <= merged_intervals[-1][1]:
            merged_intervals[-1][1] = max(merged_intervals[-1][1], end)
        else:
            merged_intervals.append([start, end])

    gaps: list[dict[str, object]] = []
    cursor = 0.0
    for start, end in merged_intervals:
        if start - cursor >= min_gap_seconds:
            gaps.append(
                {
                    "start_seconds": _rounded(cursor),
                    "end_seconds": _rounded(start),
                    "label": CANDIDATE_LABEL,
                }
            )
        cursor = end
    if duration - cursor >= min_gap_seconds:
        gaps.append(
            {
                "start_seconds": _rounded(cursor),
                "end_seconds": _rounded(duration),
                "label": CANDIDATE_LABEL,
            }
        )
    return gaps


def _positive_finite_float(value: str) -> float:
    try:
        parsed = float(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive finite number") from error
    if not math.isfinite(parsed) or parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive finite number")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", required=True, help="existing transcript-free Pyannote summary.json")
    parser.add_argument("--output", required=True, help="separate candidate-interval JSON artifact")
    parser.add_argument("--min-gap-seconds", type=_positive_finite_float, default=3.0)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary_path = Path(args.summary)
    output_path = Path(args.output)
    try:
        if not summary_path.is_file():
            raise SummaryValidationError("summary must be a regular file")
        if summary_path.resolve() == output_path.resolve():
            raise SummaryValidationError("output must be separate from summary")
        try:
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as error:
            raise SummaryValidationError("summary must contain valid JSON") from error
        if not isinstance(summary, dict):
            raise SummaryValidationError("summary must be a JSON object")

        intervals = derive_candidate_intervals(summary, args.min_gap_seconds)
        artifact = {
            "source_summary_path": str(summary_path),
            "rules": {
                "candidate_label": CANDIDATE_LABEL,
                "min_gap_seconds": args.min_gap_seconds,
                "scope": SCOPE_NOTE,
            },
            "candidate_count": len(intervals),
            "total_candidate_seconds": _rounded(
                sum(interval["end_seconds"] - interval["start_seconds"] for interval in intervals)
            ),
            "intervals": intervals,
        }
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(artifact, sort_keys=True) + "\n", encoding="utf-8")
    except (OSError, SummaryValidationError) as error:
        print(f"configuration error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
