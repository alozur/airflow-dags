"""NAS archival helpers for completed congress-video raw material (issue: NAS archive).

Pure module: no Airflow imports, no live subprocess/network calls. Every
side-effecting collaborator (process runner, filesystem) is either an
injectable callable or plain ``pathlib``/``shutil`` I/O, so this module is
directly unit-testable without Docker or a real NAS.

Archival lifecycle, orchestrated by ``congress_videos/nas_archive_dag.py``:

1. ``video_paths``            — locate every local directory holding a
                                 video's raw/derived material.
2. ``remote_mkdir_command``   — ensure the remote parent exists (rsync's
                                 receiver is an old 3.1.2 build with no
                                 ``--mkpath`` support server-side).
3. ``rsync_command``          — push one local path to the NAS mirror.
4. ``verify_synced``          — dry-run rsync; only a byte-identical mirror
                                 clears the way for local deletion.
5. ``prune_local``            — safety-checked local deletion (never touches
                                 shared/protected roots).
6. ``write_marker`` /
   ``is_archived``            — idempotency marker so a video is archived
                                 at most once.
7. ``mirror_paths``           — every ``MIRROR_ONLY_DIRS`` entry that exists
                                 under ``PROJECT_DATA_DIR`` (e.g.
                                 ``thumbnails/``): rsynced every run like any
                                 other path above, but NEVER pruned — those
                                 directories are keyed by an identifier
                                 (``youtube_video_id``) that cannot be
                                 attributed to one source video, so there is
                                 no safe per-video deletion rule for them.
"""

from __future__ import annotations

import json
import shlex
import shutil
from dataclasses import dataclass
from pathlib import Path

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

_REQUIRED_SSH_FILES = ("id_ed25519", "known_hosts")

_DEFAULT_PORT = "22"
_DEFAULT_MIN_AGE_DAYS = "14"
_DEFAULT_SSH_DIR = "/opt/airflow/nas_sync"


def _parse_int(name: str, raw: str) -> int:
    try:
        return int(raw)
    except (TypeError, ValueError):
        raise ValueError(f"{name} must be an integer, got {raw!r}") from None


@dataclass(frozen=True)
class ArchiveSettings:
    """NAS archive configuration, read from the container environment.

    Attributes:
        host:         Tailnet IPv4 of the NAS. Empty string disables archiving.
        port:         SSH port.
        user:         SSH user.
        root:         Remote absolute directory mirroring ``PROJECT_DATA_DIR``.
        min_age_days: Minimum local age (days) before a video is eligible.
        ssh_dir:      Directory holding ``id_ed25519`` and ``known_hosts``
                       (bind-mounted read-only from ``NAS_SYNC_HOST_DIR``).
    """

    host: str
    port: int
    user: str
    root: str
    min_age_days: int
    ssh_dir: Path

    @property
    def enabled(self) -> bool:
        """Archiving is enabled only when a NAS host is configured."""
        return bool(self.host)

    def validate(self) -> None:
        """Raise ``ValueError`` when the settings are unusable while enabled.

        A disabled configuration (empty host) is always considered valid —
        ``root``/``user`` being unset in that case is the expected default.
        """
        if not self.enabled:
            return
        if not self.root.startswith("/"):
            raise ValueError(f"NAS_ARCHIVE_ROOT must be an absolute path when archiving is enabled, got {self.root!r}")
        if not self.user:
            raise ValueError("NAS_ARCHIVE_USER must be set when archiving is enabled")
        missing = [name for name in _REQUIRED_SSH_FILES if not (self.ssh_dir / name).is_file()]
        if missing:
            raise ValueError(f"NAS_ARCHIVE_SSH_DIR={self.ssh_dir} is missing required file(s): {', '.join(missing)}")

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> ArchiveSettings:
        """Build settings from the container environment (or an injected mapping).

        Args:
            env: Mapping to read from instead of ``os.environ`` (tests).

        Raises:
            ValueError: If a numeric field is not an integer, or if the
                configuration is enabled but otherwise unusable (see
                :meth:`validate`).
        """
        import os  # noqa: PLC0415 -- local import keeps os.environ access explicit and mockable

        source = env if env is not None else os.environ

        host = source.get("NAS_ARCHIVE_HOST", "").strip()
        user = source.get("NAS_ARCHIVE_USER", "").strip()
        root = source.get("NAS_ARCHIVE_ROOT", "").strip()
        port = _parse_int("NAS_ARCHIVE_PORT", source.get("NAS_ARCHIVE_PORT", _DEFAULT_PORT))
        min_age_days = _parse_int(
            "NAS_ARCHIVE_MIN_AGE_DAYS", source.get("NAS_ARCHIVE_MIN_AGE_DAYS", _DEFAULT_MIN_AGE_DAYS)
        )
        if min_age_days < 0:
            raise ValueError(f"NAS_ARCHIVE_MIN_AGE_DAYS must be >= 0, got {min_age_days}")
        ssh_dir = Path(source.get("NAS_ARCHIVE_SSH_DIR", _DEFAULT_SSH_DIR))

        settings = cls(host=host, port=port, user=user, root=root, min_age_days=min_age_days, ssh_dir=ssh_dir)
        settings.validate()
        return settings


# ---------------------------------------------------------------------------
# Local path discovery
# ---------------------------------------------------------------------------


def video_paths(project_dir: Path | str, channel_slug: str, video_id: str) -> list[Path]:
    """Return every local directory holding a video's raw/derived material.

    Mirrors the date-less lookup used by ``speaker_turn_videos_dag`` and
    ``trim_proposals_dag`` for the raw download, plus the canonical
    per-channel artifact subtree.

    Args:
        project_dir:  ``PROJECT_DATA_DIR`` (or an override for tests).
        channel_slug: Channel slug (e.g. ``"congreso-es-tv"``).
        video_id:     Source YouTube video identifier.

    Returns:
        Every existing local directory for the video: zero or more
        ``downloads/{date}/{video_id}`` directories, plus
        ``{channel_slug}/{video_id}`` when it exists.

    Raises:
        FileNotFoundError: If no local path exists for the video.
    """
    project_dir = Path(project_dir)
    paths: list[Path] = []

    downloads_dir = project_dir / "downloads"
    if downloads_dir.is_dir():
        for date_dir in sorted(downloads_dir.iterdir()):
            candidate = date_dir / video_id
            if candidate.is_dir():
                paths.append(candidate)

    channel_dir = project_dir / channel_slug / video_id
    if channel_dir.is_dir():
        paths.append(channel_dir)

    if not paths:
        raise FileNotFoundError(
            f"No local paths found for channel_slug={channel_slug!r} video_id={video_id!r} under {project_dir}"
        )
    return paths


# Top-level directories under PROJECT_DATA_DIR that are mirrored to the NAS
# every run but are NEVER pruned from local disk (see ``mirror_paths``).
MIRROR_ONLY_DIRS: tuple[str, ...] = ("thumbnails",)


def mirror_paths(project_dir: Path | str) -> list[Path]:
    """Return every existing ``MIRROR_ONLY_DIRS`` directory under ``project_dir``.

    ``thumbnails/{youtube_video_id}/...`` holds small PNG/JSON files keyed by
    the *uploaded* YouTube video id, not the source ``video_id`` used
    elsewhere in this module — a single directory can't be attributed to one
    source video, so it is synced wholesale instead of per-video like
    :func:`video_paths`.

    A mirror directory that does not (yet) exist locally is simply omitted
    (nothing to sync), rather than raising like :func:`video_paths` does for
    a missing per-video path.

    Callers must sync these paths WITHOUT ``rsync --delete`` and must never
    pass them to :func:`prune_local` — every name in ``MIRROR_ONLY_DIRS`` is
    also listed in ``_PROTECTED_TOP_LEVEL_NAMES``, so ``prune_local`` refuses
    them outright. A file removed locally therefore stays archived on the
    NAS indefinitely; that is the intended behavior for shared material with
    no safe per-video deletion rule.
    """
    project_dir = Path(project_dir)
    return [project_dir / name for name in MIRROR_ONLY_DIRS if (project_dir / name).is_dir()]


# ---------------------------------------------------------------------------
# Command builders (pure — return argv lists, never execute anything)
# ---------------------------------------------------------------------------


def ssh_command(settings: ArchiveSettings) -> list[str]:
    """Return the base SSH argv shared by every NAS command.

    Locked down for unattended use: no interactive prompts (``BatchMode``),
    strict host-key verification against the pre-seeded ``known_hosts``, and
    a single explicit identity file (``IdentitiesOnly``) so the ssh-agent's
    default keys are never tried.
    """
    known_hosts = settings.ssh_dir / "known_hosts"
    identity = settings.ssh_dir / "id_ed25519"
    return [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=yes",
        "-o",
        f"UserKnownHostsFile={known_hosts}",
        "-o",
        "IdentitiesOnly=yes",
        "-i",
        str(identity),
        "-p",
        str(settings.port),
    ]


def remote_mkdir_command(settings: ArchiveSettings, remote_relative_dir: str) -> list[str]:
    """Return the argv that creates the remote parent directory over SSH.

    The NAS runs rsync 3.1.2; ``--mkpath`` (rsync_command below) requires
    protocol support the receiver may not have, so the remote directory is
    created explicitly as a defensive, independent step before the transfer.
    """
    remote_path = f"{settings.root}/{remote_relative_dir}"
    return [*ssh_command(settings), f"{settings.user}@{settings.host}", "mkdir", "-p", remote_path]


def rsync_command(
    settings: ArchiveSettings,
    local_path: Path | str,
    remote_relative_dir: str,
    dry_run: bool = False,
) -> list[str]:
    """Return the argv that mirrors ``local_path`` under the NAS archive root.

    ``remote_relative_dir`` is ``local_path``'s position relative to
    ``PROJECT_DATA_DIR`` (e.g. ``"downloads/2026-03-01/abc123"`` or
    ``"congreso-es-tv/abc123"``), so the archive mirrors the project layout.
    """
    local_path = Path(local_path)
    remote = f"{settings.user}@{settings.host}:{settings.root}/{remote_relative_dir}/"
    command = ["rsync", "-a", "--partial", "--mkpath", "--itemize-changes"]
    if dry_run:
        command.append("--dry-run")
    command += ["-e", shlex.join(ssh_command(settings)), f"{local_path}/", remote]
    return command


def verify_synced(
    settings: ArchiveSettings,
    local_path: Path | str,
    remote_relative_dir: str,
    runner,
) -> bool:
    """Return ``True`` only when the NAS mirror is byte-identical to ``local_path``.

    Runs the dry-run rsync (``-n``) and inspects the itemized-changes output:
    any line beginning with ``<`` (would send), ``>`` (would receive), or
    ``c`` (would create/change locally) means the mirror is not yet complete.

    Args:
        runner: Injectable ``subprocess.run``-shaped callable — takes the
            argv list and returns an object exposing ``.stdout``.
    """
    command = rsync_command(settings, local_path, remote_relative_dir, dry_run=True)
    result = runner(command)
    stdout = getattr(result, "stdout", "") or ""
    return all(line[:1] not in ("<", ">", "c") for line in stdout.splitlines())


# ---------------------------------------------------------------------------
# Local pruning (safety-checked deletion)
# ---------------------------------------------------------------------------

# Top-level names directly under PROJECT_DATA_DIR that must never be pruned.
# Includes every name in MIRROR_ONLY_DIRS (currently just "thumbnails"): those
# directories are synced to the NAS every run (see mirror_paths) but are
# shared/unattributable to a single video, so they are never deleted locally.
_PROTECTED_TOP_LEVEL_NAMES = frozenset({"assets", "youtube_tokens", "youtube_cookies.txt", *MIRROR_ONLY_DIRS})

_MARKER_NAME = ".nas_archived.json"


def _validate_prunable(path: Path, project_dir: Path) -> None:
    try:
        relative = path.relative_to(project_dir)
    except ValueError:
        raise ValueError(f"Refusing to prune path outside project_dir: {path}") from None
    if not relative.parts:
        raise ValueError(f"Refusing to prune project_dir itself: {path}")

    top = relative.parts[0]
    if top in _PROTECTED_TOP_LEVEL_NAMES:
        raise ValueError(f"Refusing to prune protected root: {path}")
    if top == "downloads" and len(relative.parts) < 2:
        raise ValueError(f"Refusing to prune the downloads/ root: {path}")
    if top == "downloads" and len(relative.parts) == 2:
        # downloads/{date} is shared across every video downloaded that day.
        raise ValueError(f"Refusing to prune the shared downloads/<date> directory: {path}")


def prune_local(paths: list[Path | str], project_dir: Path | str) -> list[str]:
    """Delete local material for one video, after every safety check passes.

    - ``downloads/{date}/{video_id}`` (the raw per-video download): removed
      wholesale via ``shutil.rmtree``.
    - ``{channel_slug}/{video_id}`` (the derived-artifact subtree): only
      ``*.mp4`` files are removed; sidecar files (SRT/JSON/PNG) are kept so
      downstream DAGs reading metadata are unaffected.

    Every path is checked with :func:`_validate_prunable` BEFORE any
    deletion happens; a failing check raises and leaves every path (in this
    call) untouched.

    Args:
        paths:       Local directories/files to prune (as returned by
            :func:`video_paths`).
        project_dir: ``PROJECT_DATA_DIR`` boundary paths must stay inside.

    Returns:
        Every path actually removed, as strings.
    """
    project_dir = Path(project_dir).resolve()
    resolved = [Path(path).resolve() for path in paths]

    downloads_dir = project_dir / "downloads"
    for path in resolved:
        _validate_prunable(path, project_dir)

    removed: list[str] = []
    for path in resolved:
        if path.parent.parent == downloads_dir:
            shutil.rmtree(path)
            removed.append(str(path))
            continue

        if path.is_dir():
            for mp4_file in sorted(path.rglob("*.mp4")):
                mp4_file.unlink()
                removed.append(str(mp4_file))
        elif path.suffix == ".mp4":
            path.unlink()
            removed.append(str(path))

    return removed


# ---------------------------------------------------------------------------
# Idempotency marker
# ---------------------------------------------------------------------------


def write_marker(channel_dir: Path | str, payload: dict) -> Path:
    """Write the ``.nas_archived.json`` marker recording an archive run.

    Args:
        channel_dir: The video's ``{channel_slug}/{video_id}`` directory.
        payload:     JSON-serializable summary, e.g.
            ``{"archived_at": ..., "host": ..., "root": ..., "removed": [...], "synced": [...]}``.

    Returns:
        Path to the written marker file.
    """
    channel_dir = Path(channel_dir)
    channel_dir.mkdir(parents=True, exist_ok=True)
    marker_path = channel_dir / _MARKER_NAME
    marker_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return marker_path


def is_archived(channel_dir: Path | str) -> bool:
    """Return ``True`` when ``channel_dir`` already holds the archive marker."""
    return (Path(channel_dir) / _MARKER_NAME).is_file()
