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


# ---------------------------------------------------------------------------
# RED-first quirk tests for the helpers to be lifted out of
# derive_candidate_intervals (issue #272, slice 4, PR4). Written before the
# helpers exist — importing them fails until the lift lands.
# ---------------------------------------------------------------------------


def test_merge_active_intervals_sorts_unsorted_input():
    from benchmarks.pyannote_diarization.candidate_intervals import _merge_active_intervals

    merged = _merge_active_intervals([(10.0, 12.0), (0.0, 2.0)])

    assert merged == [[0.0, 2.0], [10.0, 12.0]]


def test_merge_active_intervals_merges_touching_intervals():
    from benchmarks.pyannote_diarization.candidate_intervals import _merge_active_intervals

    merged = _merge_active_intervals([(0.0, 4.0), (4.0, 6.0)])

    assert merged == [[0.0, 6.0]]


def test_merge_active_intervals_does_not_shrink_end_for_contained_interval():
    from benchmarks.pyannote_diarization.candidate_intervals import _merge_active_intervals

    merged = _merge_active_intervals([(0.0, 10.0), (2.0, 4.0)])

    assert merged == [[0.0, 10.0]]


def test_intervals_to_gaps_emits_gap_at_exact_minimum():
    from benchmarks.pyannote_diarization.candidate_intervals import _intervals_to_gaps

    gaps = _intervals_to_gaps([[5.0, 10.0]], duration=10.0, min_gap_seconds=5.0)

    assert gaps == [{"start_seconds": 0.0, "end_seconds": 5.0, "label": "NO_DIARIZED_SPEECH"}]


def test_intervals_to_gaps_tail_gap_uses_duration_minus_cursor():
    from benchmarks.pyannote_diarization.candidate_intervals import _intervals_to_gaps

    gaps = _intervals_to_gaps([[0.0, 2.0]], duration=8.0, min_gap_seconds=3.0)

    assert gaps == [{"start_seconds": 2.0, "end_seconds": 8.0, "label": "NO_DIARIZED_SPEECH"}]


def test_intervals_to_gaps_rounds_to_six_decimals():
    from benchmarks.pyannote_diarization.candidate_intervals import _intervals_to_gaps

    gaps = _intervals_to_gaps([], duration=1.0 / 3.0, min_gap_seconds=0.0)

    assert gaps == [{"start_seconds": 0.0, "end_seconds": 0.333333, "label": "NO_DIARIZED_SPEECH"}]


def test_intervals_to_gaps_zero_minimum_emits_zero_length_gaps():
    from benchmarks.pyannote_diarization.candidate_intervals import _intervals_to_gaps

    gaps = _intervals_to_gaps([[0.0, 5.0]], duration=5.0, min_gap_seconds=0.0)

    assert gaps == [
        {"start_seconds": 0.0, "end_seconds": 0.0, "label": "NO_DIARIZED_SPEECH"},
        {"start_seconds": 5.0, "end_seconds": 5.0, "label": "NO_DIARIZED_SPEECH"},
    ]


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
