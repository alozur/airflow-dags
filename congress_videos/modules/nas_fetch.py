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
                            directories were pushed for this video. When no
                            marker exists, ``discover_fetch_source`` /
                            ``discover_remote_dirs`` fall back to a single
                            SSH discovery command against
                            ``ArchiveSettings.root`` and, when configured,
                            ``ArchiveSettings.legacy_root`` (a read-only
                            pre-migration production tree) — see
                            ``congress_videos.nas_fetch_dag.fetch_one_video``.
2. ``ensure_local_dir``  — create the local destination directory (a pruned
                            ``downloads/{date}/{video_id}`` tree may no
                            longer exist) before the pull runs.
3. ``fetch_rsync_command`` — pull one of those directories back from the NAS
                            mirror into its original local position.
4. ``verify_fetched``    — dry-run pull; only a byte-identical local copy
                            clears the way for the next step.
5. ``refresh_retention`` — reset the fetched media files' mtime to "now".
                            ``rsync -a`` preserves the NAS's original mtime,
                            so a naively-fetched video would still look old
                            to ``nas_archive``'s local age gate (see that
                            function's docstring for the exact rule it
                            matches).
6. ``remove_marker``     — delete the local idempotency marker so
                            ``nas_archive`` treats the video as a fresh
                            candidate again once it re-ages past
                            ``NAS_ARCHIVE_MIN_AGE_DAYS``. The NAS copy is
                            never touched by this module — only local state
                            changes.
7. ``is_archived_elsewhere`` — thin wrapper so a consumer DAG that only knows
                            ``(project_dir, channel_slug, video_id)`` can ask
                            "is this video's source on the NAS only?" without
                            importing ``nas_archive`` directly.
"""

from __future__ import annotations

import json
import os
import re
import shlex
from datetime import datetime
from pathlib import Path, PurePosixPath

from congress_videos.modules import nas_archive
from congress_videos.modules.nas_archive import ArchiveSettings, rsync_itemized_clean, ssh_command

# rsync/ssh subprocess timeout (seconds). Raw downloads are multi-GB, so this
# is a generous ceiling matching nas_archive's own transfer timeout. This
# module is the single source of truth (design D7); congress_videos.nas_fetch_dag
# imports this constant rather than redefining it.
RSYNC_TIMEOUT_SECS = 3600

# Marker filename must match congress_videos.modules.nas_archive._MARKER_NAME
# exactly. Duplicated here (rather than importing a private name across a
# module boundary) since it is an implementation detail of that module, not
# part of its public interface.
_MARKER_NAME = ".nas_archived.json"

# Media suffixes whose mtime nas_archive_dag._newest_mtime() uses to gate the
# local-age eligibility check — see refresh_retention() below.
_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm")

# Channel slugs are lowercase-alphanumeric-with-hyphens tokens (e.g.
# "congreso-es-tv"). Enforced before channel_slug is ever interpolated
# unquoted into a remote shell command — see discover_remote_dirs below.
_CHANNEL_SLUG_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}$")


def _validate_channel_slug(channel_slug: str) -> None:
    """Raise ``ValueError`` unless ``channel_slug`` looks like a safe slug.

    A defensive boundary check mirroring ``nas_archive.validate_video_id``:
    exists so a malformed or adversarial ``channel_slug`` can never reach an
    interpolated remote shell command unquoted.
    """
    if not isinstance(channel_slug, str) or not _CHANNEL_SLUG_PATTERN.fullmatch(channel_slug):
        raise ValueError(f"Invalid channel_slug: {channel_slug!r} (must match {_CHANNEL_SLUG_PATTERN.pattern})")


# ---------------------------------------------------------------------------
# Marker read/remove
# ---------------------------------------------------------------------------


def _validate_safe_relative_dir(relative_dir: object, channel_slug: str, video_id: str) -> None:
    """Raise ``ValueError`` unless ``relative_dir`` is a safe project-relative posix path.

    Safe means: a non-empty string, not absolute, containing no ``..``
    component, and either rooted at ``downloads/`` or ``{channel_slug}/``, or
    an exact match for ``video_id`` (the legacy top-level scheme — see
    ``nas_archive.video_paths``) — the only three shapes
    ``nas_archive.write_marker`` ever records in ``synced``, and the only
    three shapes ``discover_remote_dirs`` below ever discovers.
    """
    if not isinstance(relative_dir, str) or not relative_dir:
        raise ValueError(f"Unsafe entry in NAS archive marker 'synced' list: {relative_dir!r}")
    posix_path = PurePosixPath(relative_dir)
    if posix_path.is_absolute() or ".." in posix_path.parts:
        raise ValueError(f"Unsafe path in NAS archive marker 'synced' list: {relative_dir!r}")
    if relative_dir == video_id:
        return
    if not (relative_dir.startswith("downloads/") or relative_dir.startswith(f"{channel_slug}/")):
        raise ValueError(
            f"Unexpected root in NAS archive marker 'synced' entry: {relative_dir!r} "
            f"(must start with 'downloads/' or {channel_slug!r}/, or equal video_id {video_id!r})"
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
            :func:`_validate_safe_relative_dir` — a bare ``video_id`` entry,
            from the legacy top-level scheme, is accepted alongside
            ``downloads/...`` and ``{channel_slug}/...``).
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
        _validate_safe_relative_dir(relative_dir, channel_slug, video_id)

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
# Local destination directory
# ---------------------------------------------------------------------------


def ensure_local_dir(local_path: Path | str) -> Path:
    """Create ``local_path`` (and any missing parents) before a pull runs.

    A previously-archived video's local directory (e.g.
    ``downloads/{date}/{video_id}``) may have been pruned entirely by
    ``nas_archive.prune_local``, so it must be recreated locally before
    rsync can write into it — the sender side (this container) is on a
    modern rsync, but ``--mkpath`` is never used here either (see
    :func:`fetch_rsync_command`), so this is done as an explicit step, the
    pull-side mirror of ``nas_archive.remote_mkdir_command``.
    """
    local_path = Path(local_path)
    local_path.mkdir(parents=True, exist_ok=True)
    return local_path


# ---------------------------------------------------------------------------
# Command builders (pure — return argv lists, never execute anything)
# ---------------------------------------------------------------------------


def fetch_rsync_command(
    settings: ArchiveSettings,
    source_root: str,
    remote_relative_dir: str,
    local_path: Path | str,
    dry_run: bool = False,
) -> list[str]:
    """Return the argv that pulls one archived directory back from the NAS.

    The inverse of ``nas_archive.rsync_command``: source and destination are
    swapped, so ``{user}@{host}:{source_root}/{remote_relative_dir}/`` is
    pulled into ``{local_path}/``.

    ``source_root`` is an explicit parameter (rather than always reading
    ``settings.root``) because a video with no local archive marker may be
    pulled from ``settings.legacy_root`` instead — see
    :func:`discover_fetch_source`. Marker-mode callers simply pass
    ``settings.root``.

    ``--mkpath`` is deliberately never passed. rsync forwards it to the
    remote side of the transfer for negotiation regardless of which side is
    the sender, and the NAS (rsync 3.1.2) rejects it outright even when it
    is only the source here — so the local destination directory is instead
    created ahead of time via :func:`ensure_local_dir`.
    """
    local_path = Path(local_path)
    remote = f"{settings.user}@{settings.host}:{source_root}/{remote_relative_dir}/"
    command = ["rsync", "-a", "--partial", "--itemize-changes"]
    if dry_run:
        command.append("--dry-run")
    command += ["-e", shlex.join(ssh_command(settings)), remote, f"{local_path}/"]
    return command


def verify_fetched(
    settings: ArchiveSettings,
    source_root: str,
    remote_relative_dir: str,
    local_path: Path | str,
    runner,
) -> bool:
    """Return ``True`` only when ``local_path`` is byte-identical to the NAS mirror.

    Mirrors ``nas_archive.verify_synced``'s semantics: the dry-run pull's
    itemized-changes/returncode interpretation is shared via
    :func:`nas_archive.rsync_itemized_clean`.

    Args:
        source_root: Same meaning as in :func:`fetch_rsync_command`.
        runner: Injectable ``subprocess.run``-shaped callable — takes the
            argv list and returns an object exposing ``.stdout`` and
            ``.returncode``.
    """
    command = fetch_rsync_command(settings, source_root, remote_relative_dir, local_path, dry_run=True)
    result = runner(command)
    return rsync_itemized_clean(result)


# ---------------------------------------------------------------------------
# Marker-less fallback: remote discovery
# ---------------------------------------------------------------------------


def discover_remote_dirs(
    settings: ArchiveSettings,
    source_root: str,
    channel_slug: str,
    video_id: str,
    runner,
) -> list[str]:
    """Discover which of a video's three possible directory shapes exist under ``source_root``.

    Used only when no local ``.nas_archived.json`` marker exists for
    ``video_id`` — the caller doesn't know in advance which of
    ``downloads/{date}/{video_id}``, ``{channel_slug}/{video_id}``, or the
    legacy top-level ``{video_id}`` actually exist under ``source_root`` (the
    NAS archive root or the read-only legacy production root — see
    :func:`discover_fetch_source`).

    Runs exactly ONE SSH command: a POSIX ``sh`` snippet that globs each
    candidate shape and prints only the ones that exist as real directories.
    ``video_id`` is validated via ``nas_archive.validate_video_id`` and
    ``channel_slug`` via :func:`_validate_channel_slug` before either is
    interpolated unquoted into the remote command — both patterns forbid
    shell metacharacters, quotes, and whitespace. ``source_root`` (an
    already-validated absolute path, but not guaranteed free of spaces or
    quotes) is ``shlex.quote``-d; the ``*`` glob segment is deliberately left
    unquoted so the remote shell expands it.

    ssh already runs its trailing argv through the remote user's login
    shell: passing the command as several argv elements (e.g. ``"sh"``,
    ``"-c"``, ``snippet``) makes ssh naively space-join them into one
    string that the remote shell then re-parses from scratch, destroying
    any quoting the snippet relied on. So — exactly like
    ``nas_archive.remote_mkdir_command`` appends its command directly after
    ``user@host`` with no ``sh -c`` wrapper — the fully-built snippet is
    appended as the single trailing argv element here.

    Args:
        source_root: Absolute remote root to search under.
        runner: Injectable ``subprocess.run``-shaped callable — takes the
            argv list and returns an object exposing ``.stdout`` and
            ``.returncode``.

    Returns:
        Every discovered directory, as a project-relative posix path (e.g.
        ``"downloads/2026-03-01/abc123"``, ``"congreso-es-tv/abc123"``, or
        ``"abc123"``), safety-validated the same way as marker-mode
        ``synced`` entries. Empty list when nothing exists under
        ``source_root`` for this video.

    Raises:
        ValueError: ``video_id`` fails ``nas_archive.validate_video_id``,
            ``channel_slug`` fails :func:`_validate_channel_slug`, the
            remote discovery command fails, or a discovered path is unsafe
            or escapes ``source_root``.
    """
    nas_archive.validate_video_id(video_id)
    _validate_channel_slug(channel_slug)

    quoted_root = shlex.quote(source_root)
    candidates = (
        f"{quoted_root}/downloads/*/{video_id}",
        f"{quoted_root}/{channel_slug}/{video_id}",
        f"{quoted_root}/{video_id}",
    )
    # Trailing ": " (POSIX no-op, always exits 0) matters: without it, the
    # `for` loop's exit status is whatever its LAST executed `[ -d ... ]`
    # test returned, so the whole command would spuriously fail whenever the
    # last candidate (the legacy top-level shape) happens not to exist even
    # though an earlier candidate matched and was printed — the common case.
    remote_command = "for d in " + " ".join(candidates) + '; do [ -d "$d" ] && printf \'%s\\n\' "$d"; done; :'
    command = [*ssh_command(settings), f"{settings.user}@{settings.host}", remote_command]

    result = runner(command)
    if getattr(result, "returncode", 0) != 0:
        raise ValueError(
            f"nas_fetch: remote discovery failed under {source_root!r} for video_id={video_id!r}: "
            f"{getattr(result, 'stderr', '')!r}"
        )

    prefix = f"{source_root}/"
    relative_dirs: list[str] = []
    for raw_line in (getattr(result, "stdout", "") or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if not line.startswith(prefix):
            raise ValueError(f"nas_fetch: discovered path outside source_root {source_root!r}: {line!r}")
        relative_dir = line[len(prefix) :]
        _validate_safe_relative_dir(relative_dir, channel_slug, video_id)
        relative_dirs.append(relative_dir)
    return relative_dirs


def discover_fetch_source(
    settings: ArchiveSettings,
    channel_slug: str,
    video_id: str,
    runner,
) -> tuple[str, list[str]]:
    """Return ``(source_root, relative_dirs)`` for a video with no local marker.

    Tries ``settings.root`` (the NAS archive root) first, then
    ``settings.legacy_root`` (the read-only pre-migration production tree)
    when configured — the first root that yields at least one directory
    wins. Never touches ``settings.legacy_root`` for anything but reading:
    the legacy root is pull-only, never written, deleted, or rsync-pushed to.

    Raises:
        FileNotFoundError: Neither root has any directory for this video.
        ValueError: See :func:`discover_remote_dirs`.
    """
    for source_root in (settings.root, settings.legacy_root):
        if not source_root:
            continue
        relative_dirs = discover_remote_dirs(settings, source_root, channel_slug, video_id, runner)
        if relative_dirs:
            return source_root, relative_dirs
    raise FileNotFoundError(
        f"nas_fetch: no remote directories found for video_id={video_id!r} channel_slug={channel_slug!r} "
        "under the archive root or legacy root"
    )


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
