"""NAS fetch-back helpers for videos archived by ``nas_archive`` (issue: NAS archive).

Pure module: no Airflow imports, no live subprocess/network calls. Every
side-effecting collaborator (process runner, filesystem) is either an
injectable callable or plain ``pathlib``/``json``/``os`` I/O, so this module
is directly unit-testable without Docker or a real NAS.

This is the inverse of ``congress_videos/modules/nas_archive.py``: it restores
one previously-archived video's local material from the NAS so a downstream
DAG (``speaker_turns``, ``trim_proposals``, ``speaker_turn_videos``, ...) can
reprocess it, orchestrated by ``congress_videos/nas_fetch_dag.py``.

Fetch-back lifecycle:

1. ``read_marker``       — read and validate the ``.nas_archived.json``
                            marker written by ``nas_archive.write_marker``,
                            recovering exactly which project-relative
                            directories were pushed for this video.
2. ``fetch_rsync_command`` — pull one of those directories back from the NAS
                            mirror into its original local position.
3. ``verify_fetched``    — dry-run pull; only a byte-identical local copy
                            clears the way for the next step.
4. ``refresh_retention`` — reset the fetched media files' mtime to "now".
                            ``rsync -a`` preserves the NAS's original mtime,
                            so a naively-fetched video would still look old
                            to ``nas_archive``'s local age gate (see that
                            function's docstring for the exact rule it
                            matches).
5. ``remove_marker``     — delete the local idempotency marker so
                            ``nas_archive`` treats the video as a fresh
                            candidate again once it re-ages past
                            ``NAS_ARCHIVE_MIN_AGE_DAYS``. The NAS copy is
                            never touched by this module — only local state
                            changes.
6. ``is_archived_elsewhere`` — thin wrapper so a consumer DAG that only knows
                            ``(project_dir, channel_slug, video_id)`` can ask
                            "is this video's source on the NAS only?" without
                            importing ``nas_archive`` directly.
"""

from __future__ import annotations

import json
import os
import shlex
from datetime import datetime
from pathlib import Path, PurePosixPath

from congress_videos.modules import nas_archive
from congress_videos.modules.nas_archive import ArchiveSettings, ssh_command

# Marker filename must match congress_videos.modules.nas_archive._MARKER_NAME
# exactly. Duplicated here (rather than importing a private name across a
# module boundary) since it is an implementation detail of that module, not
# part of its public interface.
_MARKER_NAME = ".nas_archived.json"

# Media suffixes whose mtime nas_archive_dag._newest_mtime() uses to gate the
# local-age eligibility check — see refresh_retention() below.
_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm")


# ---------------------------------------------------------------------------
# Marker read/remove
# ---------------------------------------------------------------------------


def _validate_safe_relative_dir(relative_dir: object, channel_slug: str) -> None:
    """Raise ``ValueError`` unless ``relative_dir`` is a safe project-relative posix path.

    Safe means: a non-empty string, not absolute, containing no ``..``
    component, and rooted at either ``downloads/`` or ``{channel_slug}/`` —
    the only two prefixes ``nas_archive.write_marker`` ever records in
    ``synced`` (see ``nas_archive.video_paths``).
    """
    if not isinstance(relative_dir, str) or not relative_dir:
        raise ValueError(f"Unsafe entry in NAS archive marker 'synced' list: {relative_dir!r}")
    posix_path = PurePosixPath(relative_dir)
    if posix_path.is_absolute() or ".." in posix_path.parts:
        raise ValueError(f"Unsafe path in NAS archive marker 'synced' list: {relative_dir!r}")
    if not (relative_dir.startswith("downloads/") or relative_dir.startswith(f"{channel_slug}/")):
        raise ValueError(
            f"Unexpected root in NAS archive marker 'synced' entry: {relative_dir!r} "
            f"(must start with 'downloads/' or {channel_slug!r}/)"
        )


def read_marker(project_dir: Path | str, channel_slug: str, video_id: str) -> dict:
    """Read and validate the ``.nas_archived.json`` marker for one video.

    Args:
        project_dir:  ``PROJECT_DATA_DIR`` (or an override for tests).
        channel_slug: Channel slug (e.g. ``"congreso-es-tv"``).
        video_id:     Source YouTube video identifier.

    Returns:
        The parsed marker payload (``archived_at``, ``host``, ``root``,
        ``removed``, ``synced``).

    Raises:
        FileNotFoundError: If no marker exists for this video.
        ValueError: If the marker is not valid JSON, or its ``synced`` field
            is missing, empty, not a list, or contains an unsafe path (see
            :func:`_validate_safe_relative_dir`).
    """
    marker_path = Path(project_dir) / channel_slug / video_id / _MARKER_NAME
    if not marker_path.is_file():
        raise FileNotFoundError(
            f"No NAS archive marker for channel_slug={channel_slug!r} video_id={video_id!r} at {marker_path}"
        )

    try:
        payload = json.loads(marker_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"Malformed NAS archive marker at {marker_path}: {exc}") from exc

    synced = payload.get("synced")
    if not isinstance(synced, list) or not synced:
        raise ValueError(f"NAS archive marker at {marker_path} has no non-empty 'synced' list")

    for relative_dir in synced:
        _validate_safe_relative_dir(relative_dir, channel_slug)

    return payload


def remove_marker(project_dir: Path | str, channel_slug: str, video_id: str) -> bool:
    """Delete the local ``.nas_archived.json`` marker for one video.

    Only local idempotency state changes — the NAS copy is never touched.
    After this call, ``nas_archive.select_archive_candidates`` treats the
    video as a fresh candidate once it re-ages past
    ``NAS_ARCHIVE_MIN_AGE_DAYS`` (see :func:`refresh_retention`).

    Returns:
        ``True`` if a marker was removed, ``False`` if none existed (a no-op,
        not an error — the caller may be re-running after a partial fetch).
    """
    marker_path = Path(project_dir) / channel_slug / video_id / _MARKER_NAME
    if not marker_path.is_file():
        return False
    marker_path.unlink()
    return True


def is_archived_elsewhere(project_dir: Path | str, channel_slug: str, video_id: str) -> bool:
    """Return ``True`` when this video's source material lives only on the NAS.

    Thin wrapper around ``nas_archive.is_archived`` for consumer DAGs that
    only know ``(project_dir, channel_slug, video_id)`` and would otherwise
    need to import ``nas_archive`` directly to build the channel directory
    path themselves.
    """
    channel_dir = Path(project_dir) / channel_slug / video_id
    return nas_archive.is_archived(channel_dir)


# ---------------------------------------------------------------------------
# Command builders (pure — return argv lists, never execute anything)
# ---------------------------------------------------------------------------


def fetch_rsync_command(
    settings: ArchiveSettings,
    remote_relative_dir: str,
    local_path: Path | str,
    dry_run: bool = False,
) -> list[str]:
    """Return the argv that pulls one archived directory back from the NAS.

    The inverse of ``nas_archive.rsync_command``: source and destination are
    swapped, so ``{user}@{host}:{root}/{remote_relative_dir}/`` is pulled
    into ``{local_path}/``.

    ``--mkpath`` here applies to the LOCAL destination path, created by this
    container's own (modern) rsync — unlike the push side, there is no old
    receiver-side rsync involved in a pull, so no separate remote-mkdir step
    is needed (contrast ``nas_archive.remote_mkdir_command``, which exists
    only because the NAS's rsync 3.1.2 receiver can't honor ``--mkpath``
    itself when it is the one creating the destination directory).
    """
    local_path = Path(local_path)
    remote = f"{settings.user}@{settings.host}:{settings.root}/{remote_relative_dir}/"
    command = ["rsync", "-a", "--partial", "--mkpath", "--itemize-changes"]
    if dry_run:
        command.append("--dry-run")
    command += ["-e", shlex.join(ssh_command(settings)), remote, f"{local_path}/"]
    return command


def verify_fetched(
    settings: ArchiveSettings,
    remote_relative_dir: str,
    local_path: Path | str,
    runner,
) -> bool:
    """Return ``True`` only when ``local_path`` is byte-identical to the NAS mirror.

    Mirrors ``nas_archive.verify_synced``'s semantics: a dry-run pull whose
    itemized-changes output has no line starting with ``<``, ``>``, or ``c``
    means nothing is left to fetch.

    Args:
        runner: Injectable ``subprocess.run``-shaped callable — takes the
            argv list and returns an object exposing ``.stdout``.
    """
    command = fetch_rsync_command(settings, remote_relative_dir, local_path, dry_run=True)
    result = runner(command)
    stdout = getattr(result, "stdout", "") or ""
    return all(line[:1] not in ("<", ">", "c") for line in stdout.splitlines())


# ---------------------------------------------------------------------------
# Retention window refresh
# ---------------------------------------------------------------------------


def refresh_retention(paths: list[Path | str], now: datetime) -> list[Path]:
    """Reset the mtime of every fetched media file under ``paths`` to ``now``.

    ``nas_archive_dag._newest_mtime`` — the local-age gate feeding
    ``ArchiveSettings.min_age_days`` in ``select_archive_candidates`` — takes
    the MAX mtime across every file (not just media) under a video's local
    paths. ``rsync -a`` preserves the sender's (the NAS's) original mtimes,
    so a video fetched back without this step would still carry its
    pre-archive timestamp and could immediately re-qualify for archival
    instead of getting a fresh full ``NAS_ARCHIVE_MIN_AGE_DAYS`` window.

    Bumping only the media files (``*.mp4``/``*.mkv``/``*.webm``) to ``now``
    is sufficient: since the age gate takes the MAX across all files, any
    older sidecar file (SRT/JSON/PNG) left untouched cannot pull the video's
    computed age back down.

    Args:
        paths: Directories (or individual media files) restored by
            :func:`fetch_rsync_command`, e.g. as returned in a fetch
            summary's ``restored`` list resolved back to local paths.
        now:   Timestamp to set — pass the same instant used to log the
            fetch, so the recorded age window starts exactly then.

    Returns:
        Every media file path whose mtime was updated.
    """
    timestamp = now.timestamp()
    touched: list[Path] = []
    for raw_path in paths:
        path = Path(raw_path)
        if path.is_dir():
            media_files = [f for f in path.rglob("*") if f.is_file() and f.suffix in _MEDIA_SUFFIXES]
        elif path.is_file() and path.suffix in _MEDIA_SUFFIXES:
            media_files = [path]
        else:
            media_files = []

        for media_file in media_files:
            os.utime(media_file, (timestamp, timestamp))
            touched.append(media_file)

    return touched
