"""Behavior tests for the Pyannote NAS benchmark runner."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

RUNNER = Path("benchmarks/pyannote_diarization/run_nas_benchmark.sh")


@pytest.mark.parametrize("sample_duration", ["0", "-1", "not-a-number", "nan", "inf"])
def test_runner_rejects_invalid_sample_duration_before_docker(tmp_path: Path, sample_duration: str):
    docker = tmp_path / "docker"
    docker_called = tmp_path / "docker-called"
    docker.write_text(
        "#!/bin/sh\nprintf 'called\\n' > \"$DOCKER_CALLED\"\nexit 99\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(RUNNER), "https://example.invalid/video", str(tmp_path / "output")],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "BENCHMARK_CPU": "0",
            "HF_TOKEN": "test-token",
            "SAMPLE_DURATION_SECONDS": sample_duration,
            "DOCKER_BIN": str(docker),
            "DOCKER_CALLED": str(docker_called),
        },
        text=True,
    )

    assert result.returncode == 2
    assert "SAMPLE_DURATION_SECONDS must be a positive finite number" in result.stderr
    assert not docker_called.exists()
