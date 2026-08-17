"""Pure-summary tests for the isolated Pyannote diarization benchmark."""

from __future__ import annotations

from benchmarks.pyannote_diarization.entrypoint import (
    estimate_full_runtime_seconds,
    summarize_turns,
)


def test_summary_keeps_only_timestamps_and_anonymous_speaker_labels():
    summary = summarize_turns(
        [
            (0.0, 1.25, "SPEAKER_00"),
            (1.25, 2.5, "SPEAKER_00"),
            (2.5, 4.0, "SPEAKER_01"),
        ]
    )

    assert summary == {
        "speaker_cluster_count": 2,
        "raw_turns": [
            {"start_seconds": 0.0, "end_seconds": 1.25, "speaker_label": "SPEAKER_00"},
            {"start_seconds": 1.25, "end_seconds": 2.5, "speaker_label": "SPEAKER_00"},
            {"start_seconds": 2.5, "end_seconds": 4.0, "speaker_label": "SPEAKER_01"},
        ],
        "speaker_change_count": 1,
        "timestamp_anonymous_label_changes": [
            {
                "timestamp_seconds": 2.5,
                "previous_speaker_label": "SPEAKER_00",
                "speaker_label": "SPEAKER_01",
            }
        ],
    }


def test_full_runtime_estimate_scales_the_fixed_calibration_window():
    assert estimate_full_runtime_seconds(
        diarization_seconds=150.0,
        sample_duration_seconds=600.0,
        full_video_duration_seconds=3_600.0,
    ) == 900.0
