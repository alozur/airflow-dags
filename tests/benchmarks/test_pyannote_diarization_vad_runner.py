"""Offline-container contract tests for the VAD speech-interval runner."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

RUNNER = Path("benchmarks/pyannote_diarization/run_vad_speech_intervals.sh")
DOCKERFILE = Path("benchmarks/pyannote_diarization/Dockerfile")


def test_dockerfile_installs_vad_speech_intervals_runner():
    dockerfile = DOCKERFILE.read_text(encoding="utf-8")

    assert "COPY vad_speech_intervals.py /app/vad_speech_intervals.py" in dockerfile


def test_runner_uses_an_isolated_cpu_limited_container_with_read_only_inputs(tmp_path: Path):
    docker = tmp_path / "docker"
    docker_args = tmp_path / "docker-args"
    audio = tmp_path / "full-run.wav"
    cache = tmp_path / "model-cache"
    parameters = cache / "vad-parameters.json"
    output = tmp_path / "vad-speech-intervals.json"
    audio.write_bytes(b"RIFF")
    cache.mkdir()
    parameters.write_text("{}", encoding="utf-8")
    docker.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DOCKER_ARGS\"\nprintf '{}\\n' > \"$FAKE_VAD_OUTPUT\"\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(RUNNER), str(audio), str(cache), str(output)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "BENCHMARK_CPU": "0",
            "VAD_SEGMENTATION_MODEL_ID": "verified/segmentation-model",
            "VAD_PIPELINE_PARAMETERS": str(parameters),
            "DOCKER_BIN": str(docker),
            "DOCKER_ARGS": str(docker_args),
            "FAKE_VAD_OUTPUT": str(output),
            "SUDO_UID": "1026",
            "SUDO_GID": "100",
        },
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert output.read_text(encoding="utf-8") == "{}\n"
    args = docker_args.read_text(encoding="utf-8")
    assert "--network\nnone\n" in args
    assert "--cpuset-cpus\n0\n" in args
    assert "--memory\n4g\n" in args
    assert "--user\n1026:100\n" in args
    assert "source=" + str(audio) + ",target=/source/input.wav,readonly" in args
    assert "source=" + str(cache) + ",target=/model-cache,readonly" in args
    assert "source=" + str(parameters) + ",target=/vad-parameters.json,readonly" in args


def test_runner_maps_non_sudo_host_identity_to_container_user(tmp_path: Path):
    docker = tmp_path / "docker"
    docker_args = tmp_path / "docker-args"
    audio = tmp_path / "full-run.wav"
    cache = tmp_path / "model-cache"
    parameters = cache / "vad-parameters.json"
    output = tmp_path / "vad-speech-intervals.json"
    audio.write_bytes(b"RIFF")
    cache.mkdir()
    parameters.write_text("{}", encoding="utf-8")
    docker.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DOCKER_ARGS\"\nprintf '{}\\n' > \"$FAKE_VAD_OUTPUT\"\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    environment = os.environ | {
        "BENCHMARK_CPU": "0",
        "VAD_SEGMENTATION_MODEL_ID": "verified/segmentation-model",
        "VAD_PIPELINE_PARAMETERS": str(parameters),
        "DOCKER_BIN": str(docker),
        "DOCKER_ARGS": str(docker_args),
        "FAKE_VAD_OUTPUT": str(output),
    }
    environment.pop("SUDO_UID", None)
    environment.pop("SUDO_GID", None)

    result = subprocess.run(
        ["bash", str(RUNNER), str(audio), str(cache), str(output)],
        check=False,
        capture_output=True,
        env=environment,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert f"--user\n{os.getuid()}:{os.getgid()}\n" in docker_args.read_text(encoding="utf-8")


def test_runner_prioritizes_system_docker_paths_before_path_lookup():
    runner = RUNNER.read_text(encoding="utf-8")

    assert runner.index('"/usr/local/bin/docker"') < runner.index('"/usr/bin/docker"')
    assert runner.index('"/usr/bin/docker"') < runner.index("command -v docker")


@pytest.mark.parametrize("name, value", [("SUDO_UID", "not-a-number"), ("SUDO_GID", "10.5")])
def test_runner_rejects_nonnumeric_sudo_identity_values(
    tmp_path: Path, name: str, value: str
):
    docker = tmp_path / "docker"
    docker_was_invoked = tmp_path / "docker-was-invoked"
    audio = tmp_path / "full-run.wav"
    cache = tmp_path / "model-cache"
    parameters = cache / "vad-parameters.json"
    output = tmp_path / "vad-speech-intervals.json"
    audio.write_bytes(b"RIFF")
    cache.mkdir()
    parameters.write_text("{}", encoding="utf-8")
    docker.write_text("#!/bin/sh\ntouch \"$DOCKER_WAS_INVOKED\"\nexit 99\n", encoding="utf-8")
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(RUNNER), str(audio), str(cache), str(output)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "BENCHMARK_CPU": "0",
            "VAD_SEGMENTATION_MODEL_ID": "verified/segmentation-model",
            "VAD_PIPELINE_PARAMETERS": str(parameters),
            "DOCKER_BIN": str(docker),
            "SUDO_UID": "1026",
            "SUDO_GID": "100",
            "DOCKER_WAS_INVOKED": str(docker_was_invoked),
            name: value,
        },
        text=True,
    )

    assert result.returncode == 2
    assert f"{name} must be a non-negative integer" in result.stderr
    assert not docker_was_invoked.exists()
