"""Public CLI behavior tests for Pyannote diarization gap candidates."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

from benchmarks.pyannote_diarization.candidate_intervals import (
    SummaryValidationError,
    derive_candidate_intervals,
)

CLI = Path("benchmarks/pyannote_diarization/candidate_intervals.py")


# ---------------------------------------------------------------------------
# Direct characterization tests for derive_candidate_intervals — pinned from
# literals already proven by the CLI-subprocess tests below (issue #272,
# slice 4, PR4). Written and run GREEN before the C901 refactor lands, so
# they document current behavior rather than the post-lift shape.
# ---------------------------------------------------------------------------


def test_derive_candidate_intervals_clamps_merges_and_reports_interior_and_tail_gaps():
    summary = {
        "full_video_duration_seconds": 20.0,
        "raw_turns": [
            {"start_seconds": -2.0, "end_seconds": 4.0},
            {"start_seconds": 3.0, "end_seconds": 6.0},
            {"start_seconds": 10.0, "end_seconds": 12.0},
        ],
    }

    intervals = derive_candidate_intervals(summary, min_gap_seconds=3.0)

    assert intervals == [
        {"start_seconds": 6.0, "end_seconds": 10.0, "label": "NO_DIARIZED_SPEECH"},
        {"start_seconds": 12.0, "end_seconds": 20.0, "label": "NO_DIARIZED_SPEECH"},
    ]


def test_derive_candidate_intervals_rejects_both_leading_and_tail_gaps_under_minimum():
    summary = {
        "full_video_duration_seconds": 10.0,
        "raw_turns": [{"start_seconds": 4.0, "end_seconds": 6.0}],
    }

    intervals = derive_candidate_intervals(summary, min_gap_seconds=5)

    assert intervals == []


def test_derive_candidate_intervals_rejects_end_before_start():
    summary = {
        "full_video_duration_seconds": 10.0,
        "raw_turns": [{"start_seconds": 4.0, "end_seconds": 2.0}],
    }

    with pytest.raises(SummaryValidationError, match="end_seconds must not precede start_seconds"):
        derive_candidate_intervals(summary, min_gap_seconds=3.0)


def test_cli_writes_clamped_union_of_diarization_gaps_as_candidates(tmp_path: Path):
    source = tmp_path / "summary.json"
    output = tmp_path / "candidates.json"
    source.write_text(
        json.dumps(
            {
                "full_video_duration_seconds": 20.0,
                "raw_turns": [
                    {"start_seconds": -2.0, "end_seconds": 4.0},
                    {"start_seconds": 3.0, "end_seconds": 6.0},
                    {"start_seconds": 10.0, "end_seconds": 12.0},
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(CLI), "--summary", str(source), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "candidate_count": 2,
        "intervals": [
            {
                "end_seconds": 10.0,
                "label": "NO_DIARIZED_SPEECH",
                "start_seconds": 6.0,
            },
            {
                "end_seconds": 20.0,
                "label": "NO_DIARIZED_SPEECH",
                "start_seconds": 12.0,
            },
        ],
        "rules": {
            "candidate_label": "NO_DIARIZED_SPEECH",
            "min_gap_seconds": 3.0,
            "scope": (
                "Raw diarization gaps are candidate cuts only; audio classification and validation are "
                "required. They do not classify applause or non-speech."
            ),
        },
        "source_summary_path": str(source),
        "total_candidate_seconds": 12.0,
    }


def test_cli_honors_configured_minimum_gap(tmp_path: Path):
    source = tmp_path / "summary.json"
    output = tmp_path / "candidates.json"
    source.write_text(
        json.dumps(
            {
                "full_video_duration_seconds": 10.0,
                "raw_turns": [{"start_seconds": 4.0, "end_seconds": 6.0}],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(CLI),
            "--summary",
            str(source),
            "--output",
            str(output),
            "--min-gap-seconds",
            "5",
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text(encoding="utf-8"))["intervals"] == []


def test_cli_rejects_invalid_raw_intervals_without_writing_output(tmp_path: Path):
    source = tmp_path / "summary.json"
    output = tmp_path / "candidates.json"
    source.write_text(
        json.dumps(
            {
                "full_video_duration_seconds": 10.0,
                "raw_turns": [{"start_seconds": 4.0, "end_seconds": 2.0}],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [sys.executable, str(CLI), "--summary", str(source), "--output", str(output)],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "end_seconds must not precede start_seconds" in result.stderr
    assert not output.exists()
