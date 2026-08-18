"""Docker-subprocess diarization backend for speaker-turn detection.

Provides the default production ``diarize_fn`` used by
:func:`congress_videos.modules.speaker_turns.detect_turns`. It runs the
isolated pyannote diarization container and parses its speaker-change JSON.

Kept in a separate module so ``speaker_turns.py`` stays pure — it imports no
subprocess/Docker machinery and is fully unit-testable with a stubbed
``diarize_fn``. Here the ``runner`` (``subprocess.run`` by default) is injected
so tests never launch a real container.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Callable

logger = logging.getLogger(__name__)

# The committed benchmark diarization image (see benchmarks/pyannote_diarization).
DIARIZE_IMAGE = os.environ.get(
    "DIARIZE_IMAGE", "pyannote/speaker-diarization-community-1"
)
# NAS ops: docker lives at /usr/local/bin/docker with no sudo; override via env.
DOCKER_BIN = os.environ.get("DOCKER_BIN", "docker")
DEFAULT_MEMORY = "4g"
DEFAULT_CPUSET = "0-1"
# Diarization runs ~4.6x realtime; cap generously to avoid a runaway container.
_DEFAULT_TIMEOUT_SECONDS = 6 * 60 * 60


def build_diarize_command(
    wav_path: str,
    out_dir: str,
    *,
    docker_bin: str = DOCKER_BIN,
    image: str = DIARIZE_IMAGE,
    memory: str = DEFAULT_MEMORY,
    cpuset: str = DEFAULT_CPUSET,
) -> list[str]:
    """Build the ``docker run`` argument vector for one diarization.

    The WAV's parent directory is mounted read-only at ``/in`` and ``out_dir``
    read-write at ``/out``. Returns an arg list (never a shell string) so the
    caller runs it without ``shell=True``.
    """
    wav = Path(wav_path)
    return [
        docker_bin, "run", "--rm",
        "--network", "none",
        "--memory", memory,
        "--cpuset-cpus", cpuset,
        "-v", f"{wav.parent}:/in:ro",
        "-v", f"{out_dir}:/out",
        image,
        "--input", f"/in/{wav.name}",
        "--output", "/out/changes.json",
    ]


def docker_diarize_fn(
    wav_path: str,
    chapter_offset: float = 0.0,
    *,
    timeout: int | None = _DEFAULT_TIMEOUT_SECONDS,
    runner: Callable[..., object] = subprocess.run,
) -> list[dict]:
    """Diarize ``wav_path`` in an isolated container; return speaker changes.

    Runs the pyannote container (no ``shell=True``), reads the container-written
    ``changes.json`` from a private output directory, and returns the
    ``speaker_changes`` list. ``chapter_offset`` (the chapter's absolute start in
    the source video) is added to every ``start_seconds`` so the returned
    timestamps are absolute session seconds.

    Matches the :data:`congress_videos.modules.speaker_turns.DiarizeFn` contract.
    """
    with tempfile.TemporaryDirectory(prefix="diarize-out-") as out_dir:
        cmd = build_diarize_command(wav_path, out_dir)
        logger.info("Running diarization: %s", " ".join(cmd))
        result = runner(cmd, capture_output=True, shell=False, timeout=timeout)

        if getattr(result, "returncode", 0) != 0:
            stderr = getattr(result, "stderr", b"") or b""
            if isinstance(stderr, bytes):
                stderr = stderr.decode("utf-8", "replace")
            raise RuntimeError(
                f"Diarization container failed (rc={result.returncode}): {stderr}"
            )

        changes_path = Path(out_dir, "changes.json")
        if not changes_path.exists():
            raise RuntimeError(
                f"Diarization produced no output at {changes_path}"
            )
        payload = json.loads(changes_path.read_text())

    changes = payload.get("speaker_changes", [])
    if chapter_offset:
        changes = [
            {**ch, "start_seconds": float(ch["start_seconds"]) + chapter_offset}
            for ch in changes
        ]
    return changes
