"""Fail-closed behavior tests for the offline VAD adapter."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


ADAPTER = Path("benchmarks/pyannote_diarization/vad_speech_intervals.py")


def test_adapter_rejects_unsupported_powerset_thresholds_before_inference(tmp_path: Path):
    audio = tmp_path / "full-run.wav"
    parameters = tmp_path / "vad-parameters.json"
    output = tmp_path / "vad-speech-intervals.json"
    audio.write_bytes(b"RIFF")
    parameters.write_text(
        json.dumps(
            {
                "onset": 0.6,
                "offset": 0.4,
                "min_duration_on": 0.0,
                "min_duration_off": 0.0,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            str(ADAPTER),
            "--audio",
            str(audio),
            "--model-id",
            "verified/segmentation-model",
            "--pipeline-parameters",
            str(parameters),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "unsupported pyannote.audio v4 powerset parameters: onset, offset" in result.stderr
    assert "pyannote.audio v4 is not installed" not in result.stderr
    assert not output.exists()


def test_adapter_fails_closed_when_pyannote_audio_v4_is_unavailable(tmp_path: Path):
    audio = tmp_path / "full-run.wav"
    parameters = tmp_path / "vad-parameters.json"
    output = tmp_path / "vad-speech-intervals.json"
    audio.write_bytes(b"RIFF")
    parameters.write_text(json.dumps({}), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            str(ADAPTER),
            "--audio",
            str(audio),
            "--model-id",
            "verified/segmentation-model",
            "--pipeline-parameters",
            str(parameters),
            "--output",
            str(output),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 2
    assert "pyannote.audio v4 is not installed" in result.stderr
    assert not output.exists()
