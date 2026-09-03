"""Public CLI behavior tests for VAD validation of diarization-gap candidates."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

CLI = Path("benchmarks/pyannote_diarization/vad_validation.py")


def test_cli_marks_only_candidates_with_positive_voice_overlap_as_rejected(tmp_path: Path):
    candidates = tmp_path / "candidate-intervals.json"
    speech = tmp_path / "vad-speech-intervals.json"
    output = tmp_path / "vad-validated-candidates.json"
    candidates.write_text(
        json.dumps(
            {
                "source_summary_path": "/nas/run.1/summary.json",
                "intervals": [
                    {"start_seconds": 1.0, "end_seconds": 3.0, "label": "NO_DIARIZED_SPEECH"},
                    {"start_seconds": 5.0, "end_seconds": 7.0, "label": "NO_DIARIZED_SPEECH"},
                ],
            }
        ),
        encoding="utf-8",
    )
    speech.write_text(
        json.dumps(
            {
                "source_audio_path": "/nas/run.1/calibration-600s.wav",
                "model_id": "verified-vad-model",
                "intervals": [
                    {"start_seconds": 2.5, "end_seconds": 4.0},
                    {"start_seconds": 7.0, "end_seconds": 8.0},
                ],
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(CLI),
            "--candidates",
            str(candidates),
            "--vad-speech-intervals",
            str(speech),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert json.loads(output.read_text(encoding="utf-8")) == {
        "candidate_count": 2,
        "intervals": [
            {
                "end_seconds": 3.0,
                "evidence": {
                    "candidate_interval_index": 0,
                    "candidate_source_label": "NO_DIARIZED_SPEECH",
                    "overlapping_vad_speech_interval_indexes": [0],
                },
                "label": "REJECTED_VOICE_PRESENT",
                "start_seconds": 1.0,
            },
            {
                "end_seconds": 7.0,
                "evidence": {
                    "candidate_interval_index": 1,
                    "candidate_source_label": "NO_DIARIZED_SPEECH",
                    "overlapping_vad_speech_interval_indexes": [],
                },
                "label": "CONFIRMED_NO_VOICE",
                "start_seconds": 5.0,
            },
        ],
        "rules": {
            "applause_classification": "not performed",
            "overlap": "positive-duration intersection only; touching endpoints do not overlap",
            "scope": "VAD validates voice activity versus no voice only; applause classification remains separate.",
        },
        "source_candidate_intervals_path": str(candidates),
        "source_summary_path": "/nas/run.1/summary.json",
        "source_vad_audio_path": "/nas/run.1/calibration-600s.wav",
        "source_vad_model_id": "verified-vad-model",
        "source_vad_speech_intervals_path": str(speech),
    }


def test_cli_rejects_non_candidate_labels_without_writing_output(tmp_path: Path):
    candidates = tmp_path / "candidate-intervals.json"
    speech = tmp_path / "vad-speech-intervals.json"
    output = tmp_path / "vad-validated-candidates.json"
    candidates.write_text(
        json.dumps(
            {"intervals": [{"start_seconds": 1, "end_seconds": 2, "label": "APPLAUSE"}]}
        ),
        encoding="utf-8",
    )
    speech.write_text(json.dumps({"intervals": []}), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(CLI),
            "--candidates",
            str(candidates),
            "--vad-speech-intervals",
            str(speech),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "must have label NO_DIARIZED_SPEECH" in result.stderr
    assert not output.exists()
