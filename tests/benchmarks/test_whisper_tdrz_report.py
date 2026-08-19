"""Behavior tests for the standalone whisper.cpp TinyDiarize report contract."""

from __future__ import annotations

import pytest

from benchmarks.whisper_tdrz.benchmark_report import (
    SAMPLE_DURATION_SECONDS,
    build_report,
    validate_full_duration,
)


def _report(*, diarization_exit_status: int = 0) -> dict:
    return build_report(
        full_video_duration_seconds=3_721.25,
        audio_extraction_seconds=12.5,
        diarization_seconds=34.75,
        container_end_to_end_seconds=48.0,
        speaker_turn_marker_count=3,
        diarization_exit_status=diarization_exit_status,
        image_identity="whisper-tdrz-benchmark:local@sha256:example",
        model_identity="ggml-org/whisper.cpp small.en-tdrz",
        timestamp="2025-03-08T12:00:00Z",
        audio_path="/output/20250308T120000Z-sample.wav",
        raw_cli_output_path="/output/20250308T120000Z-whisper-cli.log",
        report_path="/output/20250308T120000Z-report.json",
        image_build_seconds=91.25,
    )


def test_successful_report_separates_full_media_sample_and_timing_scopes():
    report = _report()

    assert report["status"] == "success"
    assert report["full_video_duration_seconds"] == 3_721.25
    assert report["processed_sample_duration_seconds"] == SAMPLE_DURATION_SECONDS
    assert report["timing_seconds"] == {
        "image_build_host": 91.25,
        "audio_extraction": 12.5,
        "diarization": 34.75,
        "container_end_to_end": 48.0,
    }
    assert report["output_paths"] == {
        "audio": "/output/20250308T120000Z-sample.wav",
        "raw_cli_output": "/output/20250308T120000Z-whisper-cli.log",
        "report": "/output/20250308T120000Z-report.json",
    }
    assert "transcript" not in report


def test_nonzero_diarization_exit_cannot_become_a_successful_report():
    report = _report(diarization_exit_status=1)

    assert report["status"] == "failed"
    assert report["diarization_exit_status"] == 1
    assert report["status"] != "success"


@pytest.mark.parametrize("duration", [0, 1_799.99, "not-a-number"])
def test_full_duration_must_cover_the_exact_thirty_minute_sample(duration):
    with pytest.raises(ValueError, match="1,800"):
        validate_full_duration(duration)
