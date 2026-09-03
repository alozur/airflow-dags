"""Behavior tests for the TinyDiarize NAS runner's CPU isolation contract."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

RUNNER = Path("benchmarks/whisper_tdrz/run_nas_benchmark.sh")


@pytest.mark.parametrize("cpu", [None, "", "-1", "1.5", "0,1", "cpu0"])
def test_runner_rejects_missing_or_non_integer_cpu_selection(tmp_path: Path, cpu: str | None):
    source = tmp_path / "source.mp4"
    source.touch()
    environment = os.environ | {
        "OUTPUT_BASE_DIRECTORY": str(tmp_path / "output"),
        "DOCKER_BIN": str(tmp_path / "docker"),
    }
    if cpu is not None:
        environment["BENCHMARK_CPU"] = cpu

    result = subprocess.run(
        ["bash", str(RUNNER), str(source)],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 2
    assert "BENCHMARK_CPU must be a single non-negative integer" in result.stderr


def test_runner_uses_the_selected_cpu_only_as_a_cpuset(tmp_path: Path):
    source = tmp_path / "source.mp4"
    source.touch()
    docker = tmp_path / "docker"
    arguments = tmp_path / "docker-run-arguments.txt"
    docker.write_text(
        """#!/bin/sh
set -eu
if [ "$1" = build ]; then
    while [ "$#" -gt 0 ]; do
        if [ "$1" = --iidfile ]; then
            printf 'sha256:test-image\\n' > "$2"
            exit 0
        fi
        shift
    done
fi
if [ "$1" = run ]; then
    shift
    printf '%s\\n' "$@" > "$DOCKER_ARGUMENTS"
fi
""",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(RUNNER), str(source)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "BENCHMARK_CPU": "7",
            "DOCKER_ARGUMENTS": str(arguments),
            "DOCKER_BIN": str(docker),
            "OUTPUT_BASE_DIRECTORY": str(tmp_path / "output"),
        },
        text=True,
    )

    assert result.returncode == 0
    docker_arguments = arguments.read_text(encoding="utf-8").splitlines()
    assert docker_arguments.count("--cpuset-cpus") == 1
    assert docker_arguments[docker_arguments.index("--cpuset-cpus") + 1] == "7"
    assert "--cpus" not in docker_arguments
