"""No-Docker contract tests for the VAD model preparation script."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

PREPARER = Path("benchmarks/pyannote_diarization/prepare_vad_model.sh")


@pytest.mark.parametrize(
    ("cache_path", "token", "expected_error"),
    [
        ("relative-cache", "test-token", "model cache directory must be an absolute path"),
        ("/absolute/model-cache", "", "HF_TOKEN must be set in the environment"),
    ],
)
def test_preparer_rejects_invalid_input_before_docker(
    tmp_path: Path, cache_path: str, token: str, expected_error: str
):
    docker = tmp_path / "docker"
    docker_called = tmp_path / "docker-called"
    docker.write_text(
        "#!/bin/sh\nprintf 'called\\n' > \"$DOCKER_CALLED\"\nexit 99\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PREPARER), cache_path],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "HF_TOKEN": token,
            "DOCKER_BIN": str(docker),
            "DOCKER_CALLED": str(docker_called),
        },
        text=True,
    )

    assert result.returncode == 2
    assert expected_error in result.stderr
    assert not docker_called.exists()


def test_preparer_defaults_to_the_reviewed_segmentation_model(tmp_path: Path):
    docker = tmp_path / "docker"
    prepared_model = tmp_path / "prepared-model"
    cache = tmp_path / "model-cache"
    docker.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$VAD_SEGMENTATION_MODEL_ID\" > \"$PREPARED_MODEL\"\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)
    env = os.environ | {
        "HF_TOKEN": "test-token",
        "DOCKER_BIN": str(docker),
        "PREPARED_MODEL": str(prepared_model),
    }
    env.pop("VAD_SEGMENTATION_MODEL_ID", None)

    result = subprocess.run(
        ["bash", str(PREPARER), str(cache)],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert prepared_model.read_text(encoding="utf-8") == "pyannote/segmentation-3.0\n"


def test_preparer_loads_the_selected_model_in_a_hardened_staging_container(tmp_path: Path):
    docker = tmp_path / "docker"
    docker_args = tmp_path / "docker-args"
    cache = tmp_path / "model-cache"
    token = "test-token-that-must-not-be-printed"
    docker.write_text(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$DOCKER_ARGS\"\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PREPARER), str(cache)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "HF_TOKEN": token,
            "VAD_SEGMENTATION_MODEL_ID": "verified/segmentation-model",
            "DOCKER_BIN": str(docker),
            "DOCKER_ARGS": str(docker_args),
            "SUDO_UID": "1234",
            "SUDO_GID": "5678",
        },
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert token not in result.stdout
    assert token not in result.stderr
    assert (cache / "vad-parameters.json").read_text(encoding="utf-8") == (
        '{"min_duration_off":0.0,"min_duration_on":0.0}\n'
    )
    args = docker_args.read_text(encoding="utf-8")
    assert "--network\nbridge\n" in args
    assert "--read-only\n" in args
    assert "--cap-drop\nALL\n" in args
    assert "--user\n1234:5678\n" in args
    assert "--env\nHF_TOKEN\n" in args
    assert "--env\nVAD_SEGMENTATION_MODEL_ID\n" in args
    assert "verified/segmentation-model" not in args
    assert "Model.from_pretrained" in args
    assert "VoiceActivityDetection" in args
    assert "source=" + str(cache) + ",target=/model-cache" in args
    assert "--model-id" not in args


def test_preparer_uses_only_powerset_supported_parameters(tmp_path: Path):
    docker = tmp_path / "docker"
    cache = tmp_path / "model-cache"
    package_root = tmp_path / "python-packages"
    (package_root / "pyannote" / "audio").mkdir(parents=True)
    (package_root / "pyannote" / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "pyannote" / "audio" / "__init__.py").write_text(
        "class Model:\n"
        "    @classmethod\n"
        "    def from_pretrained(cls, model_id, token):\n"
        "        return object()\n",
        encoding="utf-8",
    )
    (package_root / "pyannote" / "audio" / "pipelines.py").write_text(
        "class VoiceActivityDetection:\n"
        "    def __init__(self, segmentation):\n"
        "        pass\n"
        "    def instantiate(self, parameters):\n"
        "        if 'onset' in parameters:\n"
        "            raise ValueError(\"parameter 'onset' does not exist\")\n"
        "        assert parameters == {'min_duration_on': 0.0, 'min_duration_off': 0.0}\n",
        encoding="utf-8",
    )
    docker.write_text(
        "#!/bin/sh\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "  if [ \"$1\" = \"-c\" ]; then shift; exec python3 -c \"$1\"; fi\n"
        "  shift\n"
        "done\n"
        "exit 99\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PREPARER), str(cache)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "HF_TOKEN": "test-token",
            "DOCKER_BIN": str(docker),
            "PYTHONPATH": str(package_root),
        },
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert (cache / "vad-parameters.json").read_text(encoding="utf-8") == (
        '{"min_duration_off":0.0,"min_duration_on":0.0}\n'
    )


def test_preparer_reports_sanitized_model_preparation_failure(tmp_path: Path):
    docker = tmp_path / "docker"
    cache = tmp_path / "model-cache"
    package_root = tmp_path / "python-packages"
    token = "hf_super_secret_token"
    (package_root / "pyannote" / "audio").mkdir(parents=True)
    (package_root / "pyannote" / "__init__.py").write_text("", encoding="utf-8")
    (package_root / "pyannote" / "audio" / "__init__.py").write_text(
        "class Model:\n"
        "    @classmethod\n"
        "    def from_pretrained(cls, model_id, token):\n"
        "        raise RuntimeError(f'upstream rejected {token}; session hf_another_token')\n",
        encoding="utf-8",
    )
    (package_root / "pyannote" / "audio" / "pipelines.py").write_text(
        "class VoiceActivityDetection:\n    pass\n", encoding="utf-8"
    )
    docker.write_text(
        "#!/bin/sh\n"
        "while [ \"$#\" -gt 0 ]; do\n"
        "  if [ \"$1\" = \"-c\" ]; then shift; exec python3 -c \"$1\"; fi\n"
        "  shift\n"
        "done\n"
        "exit 99\n",
        encoding="utf-8",
    )
    docker.chmod(0o755)

    result = subprocess.run(
        ["bash", str(PREPARER), str(cache)],
        check=False,
        capture_output=True,
        env=os.environ
        | {
            "HF_TOKEN": token,
            "DOCKER_BIN": str(docker),
            "PYTHONPATH": str(package_root),
        },
        text=True,
    )

    assert result.returncode == 1
    assert (
        "RuntimeError: upstream rejected [REDACTED]; session [REDACTED]"
        in result.stderr
    )
    assert token not in result.stderr
    assert "hf_" not in result.stderr
