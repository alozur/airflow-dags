"""Docker-subprocess YAMNet backend for applause detection in speaker turns.

Provides the default production ``yamnet_fn`` used by
:func:`congress_videos.modules.trim_proposals.generate_trim_proposals`. It runs
an isolated YAMNet TFLite container and parses its applause-interval JSON.

Kept separate so ``trim_proposals.py`` stays pure — it imports no
subprocess/Docker machinery and is fully unit-testable with a stubbed
``yamnet_fn``. Here the ``runner`` (``subprocess.run`` by default) is injected
so tests never launch a real container.

Threat-matrix coverage:
- Shell injection: arg-list ``subprocess.run`` (no ``shell=True``); paths are
  module-built temp/derived paths, never user-supplied text interpolated into a
  shell string.
- Untrusted input: ``offset`` must be a real number; validated before the
  subprocess is started.
- Resource exhaustion: ``--memory`` and ``--cpuset-cpus`` caps set on every run.
- Network egress: ``--network none`` always present.
"""
from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Callable

logger = logging.getLogger(__name__)

# YAMNet TFLite Docker image; override via environment for local builds.
YAMNET_IMAGE = os.environ.get(
    "YAMNET_IMAGE", "congress-yamnet-tflite:latest"
)
# NAS ops: docker lives at /usr/local/bin/docker with no sudo; override via env.
DOCKER_BIN = os.environ.get("DOCKER_BIN", "docker")
DEFAULT_MEMORY = "4g"
DEFAULT_CPUSET = "0-1"
# YAMNet runs faster than pyannote; cap at 30 min to avoid runaway containers.
_DEFAULT_TIMEOUT_SECONDS = 30 * 60


def build_yamnet_command(
    wav_path: str,
    offset: float,
    *,
    docker_bin: str = DOCKER_BIN,
    image: str = YAMNET_IMAGE,
    memory: str = DEFAULT_MEMORY,
    cpuset: str = DEFAULT_CPUSET,
) -> list[str]:
    """Build the ``docker run`` argument vector for one YAMNet applause scan.

    Validates ``offset`` is a real number before building the command. Returns
    an arg list (never a shell string) so the caller runs it without
    ``shell=True``.

    Args:
        wav_path:   Absolute path to the turn's WAV file.
        offset:     Turn start in absolute session seconds (added to container
                    output timestamps so results are session-absolute).
        docker_bin: Docker binary path (default ``DOCKER_BIN``).
        image:      YAMNet container image (default ``YAMNET_IMAGE``).
        memory:     Docker memory cap string (default ``"4g"``).
        cpuset:     CPU affinity string (default ``"0-1"``).

    Returns:
        List[str] arg vector suitable for :func:`subprocess.run`.

    Raises:
        TypeError:  If ``offset`` cannot be converted to ``float``.
        ValueError: If ``offset`` is not a valid numeric string.
    """
    # Validate offset — must be numeric before we build any command.
    float(offset)  # raises TypeError for non-numeric, ValueError for bad strings

    return [
        docker_bin, "run", "--rm",
        "--network", "none",
        "--memory", memory,
        "--cpuset-cpus", cpuset,
        image,
        wav_path,
        str(float(offset)),
    ]


def docker_yamnet_fn(
    wav_path: str,
    offset: float,
    *,
    timeout: int | None = _DEFAULT_TIMEOUT_SECONDS,
    runner: Callable[..., object] = subprocess.run,
) -> list[dict]:
    """Run YAMNet applause detection in an isolated container; return intervals.

    Runs the YAMNet container (no ``shell=True``), reads the JSON written to
    stdout, and returns the ``applause_intervals`` list. ``offset`` (the turn's
    absolute start in the source session) is added to every ``start`` and
    ``end`` so the returned timestamps are absolute session seconds.

    Matches the :data:`congress_videos.modules.trim_proposals.YamnetFn` contract.

    Args:
        wav_path: Absolute path to the turn's extracted WAV.
        offset:   Turn start in absolute session seconds.
        timeout:  Subprocess timeout in seconds.
        runner:   Callable with the same signature as :func:`subprocess.run`
                  (injectable for tests).

    Returns:
        List of ``{start, end, max_score}`` dicts with absolute timestamps.

    Raises:
        TypeError/ValueError: If ``offset`` is not numeric (caught before runner).
        RuntimeError:          If the container exits non-zero.
    """
    cmd = build_yamnet_command(wav_path, offset)
    logger.info("Running YAMNet: %s", " ".join(cmd))

    result = runner(cmd, capture_output=True, shell=False, timeout=timeout)

    if getattr(result, "returncode", 0) != 0:
        stderr = getattr(result, "stderr", b"") or b""
        if isinstance(stderr, bytes):
            stderr = stderr.decode("utf-8", "replace")
        raise RuntimeError(
            f"YAMNet container failed (rc={result.returncode}): {stderr}"
        )

    stdout = getattr(result, "stdout", b"") or b""
    if isinstance(stdout, bytes):
        stdout = stdout.decode("utf-8", "replace")

    payload = json.loads(stdout)
    intervals: list[dict] = payload.get("applause_intervals", [])

    if offset:
        intervals = [
            {**iv, "start": float(iv["start"]) + offset, "end": float(iv["end"]) + offset}
            for iv in intervals
        ]

    return intervals
