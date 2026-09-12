"""NAS reclaim gate module: prune local material already safe on the NAS (issue: NAS reclaim).

Pure module: no Airflow imports, no live subprocess/network calls, no DB
access — every side-effecting collaborator (process runner, filesystem,
injected ``now``) is an injectable callable or plain ``pathlib`` I/O, so this
is directly unit-testable without Docker or a real NAS — orchestrated by
``congress_videos/nas_reclaim_dag.py``.

Four independent gates guard every deletion (design D3): (1) the per-video
``nas_fetch.fetch_lock`` must be free; (2) newest local media mtime must be
older than ``ArchiveSettings.reclaim_grace_hours`` (``refresh_retention``
resets it on every fetch); (3) ``video_id`` must be in the caller's
``nas_completeness.complete_video_ids`` result — this module never queries
the DB itself; (4) the real push-direction dry-run rsync
(``nas_archive.verify_synced``) must report byte-identical, refusing on any
non-clean result (returncode or itemized-changes, D4). Gate 4 is the sole
"is it really on the NAS" check — an ``.nas_archived.json`` marker is
neither required nor consulted as a gate, since the primary target here
(material a consumer fetched back via ``nas_fetch``) has its marker removed
for the duration of its local lease.

:func:`select_reclaim_candidates` is advisory only — it checks gates 1-3 (no
``runner`` for gate 4). Every gate is re-evaluated INSIDE the lock by
:func:`reclaim_one_video`, the only place gate 4 and deletion happen. On
success the marker is written (if absent) or left as-is, satisfying "later
consumers know the NAS has it" without ever being a precondition.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from congress_videos.modules import nas_archive
from congress_videos.modules.nas_archive import ArchiveSettings
from congress_videos.modules.nas_fetch import FetchLockBusy, fetch_lock

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Shared gate predicates (used by both selection and the in-lock re-check —
# no duplicated gate logic between the two entrypoints below).
# ---------------------------------------------------------------------------


def _local_paths_or_none(project_dir: Path, channel_slug: str, video_id: str) -> list[Path] | None:
    """Return the video's local paths, or ``None`` when nothing local exists."""
    try:
        return nas_archive.video_paths(project_dir, channel_slug, video_id)
    except FileNotFoundError:
        return None


# Matches nas_fetch._MEDIA_SUFFIXES (duplicated, not imported — private
# cross-module symbol). Narrower than nas_archive_dag._newest_mtime's
# "every file" scan on purpose: reclaim_one_video acquires fetch_lock
# (writing a fresh-mtime lock file in the same dir) before the grace check,
# so counting non-media bookkeeping files would make every video look
# freshly touched and never eligible.
_MEDIA_SUFFIXES = (".mp4", ".mkv", ".webm")


def _newest_mtime(paths: list[Path]) -> float | None:
    """Return the newest mtime (epoch seconds) across every media file under ``paths``."""
    newest: float | None = None
    for path in paths:
        files = [path] if path.is_file() else list(path.rglob("*"))
        for file_path in files:
            if not file_path.is_file() or file_path.suffix not in _MEDIA_SUFFIXES:
                continue
            mtime = file_path.stat().st_mtime
            if newest is None or mtime > newest:
                newest = mtime
    return newest


def _grace_elapsed(paths: list[Path], now: datetime, grace_hours: int) -> bool:
    """Return ``True`` only when every path's newest mtime is at least ``grace_hours`` old."""
    newest_mtime = _newest_mtime(paths)
    if newest_mtime is None:
        return False
    age = now - datetime.fromtimestamp(newest_mtime, tz=UTC)
    return age >= timedelta(hours=grace_hours)


def _lock_free(project_dir: Path, channel_slug: str, video_id: str, now: datetime) -> bool:
    """Non-blocking peek at the fetch lock — advisory only (see :func:`reclaim_one_video`)."""
    try:
        with fetch_lock(project_dir, channel_slug, video_id, now=now):
            return True
    except FetchLockBusy:
        return False


# ---------------------------------------------------------------------------
# Selection (advisory)
# ---------------------------------------------------------------------------


def select_reclaim_candidates(
    settings: ArchiveSettings,
    project_dir: Path | str,
    channel_slug: str,
    complete_video_ids: list[str],
    *,
    now: datetime,
    batch: int,
) -> list[dict]:
    """Return up to ``batch`` videos eligible for reclaim this run (advisory — see module docstring).

    Gates: ``video_id`` in ``complete_video_ids`` (3), local material exists
    and its grace window elapsed (2), fetch lock free (1, advisory peek).

    Returns:
        ``[{"channel_slug": ..., "video_id": ...}, ...]``, in
        ``complete_video_ids`` order, capped at ``batch`` entries.
    """
    project_dir = Path(project_dir)
    candidates: list[dict] = []
    for video_id in complete_video_ids:
        if len(candidates) >= batch:
            break
        paths = _local_paths_or_none(project_dir, channel_slug, video_id)
        if not paths or not _grace_elapsed(paths, now, settings.reclaim_grace_hours):
            continue
        if not _lock_free(project_dir, channel_slug, video_id, now):
            continue
        candidates.append({"channel_slug": channel_slug, "video_id": video_id})
    logger.info("nas_reclaim: %d candidate(s) selected for reclaim (batch=%d)", len(candidates), batch)
    return candidates


# ---------------------------------------------------------------------------
# Reclaim (in-lock re-check + deletion)
# ---------------------------------------------------------------------------


def reclaim_one_video(
    settings: ArchiveSettings,
    project_dir: Path | str,
    channel_slug: str,
    video_id: str,
    *,
    runner,
    now: datetime,
) -> dict:
    """Re-check every gate INSIDE the fetch lock, then prune (design D3).

    Never deletes anything ``nas_archive.verify_synced``'s real dry-run
    rsync doesn't confirm byte-identical on the NAS. Writes
    ``.nas_archived.json`` when absent, else leaves it untouched.

    Args:
        runner: Injectable ``subprocess.run``-shaped callable, passed
            through to ``nas_archive.verify_synced``.

    Returns:
        ``{"status": "reclaimed"|"blocked"|"skipped", "video_id": ..., ...}``
        — ``"blocked"``/``"skipped"`` carry a ``"reason"``; only
        ``"reclaimed"`` carries ``"removed"``.
    """
    project_dir = Path(project_dir)
    try:
        with fetch_lock(project_dir, channel_slug, video_id, now=now):
            paths = _local_paths_or_none(project_dir, channel_slug, video_id)
            if not paths:
                return {"status": "blocked", "reason": "no_local_material", "video_id": video_id}
            if not _grace_elapsed(paths, now, settings.reclaim_grace_hours):
                return {"status": "blocked", "reason": "grace_window", "video_id": video_id}

            verified = all(
                nas_archive.verify_synced(settings, path, path.relative_to(project_dir).as_posix(), runner)
                for path in paths
            )
            if not verified:
                return {"status": "blocked", "reason": "unverified", "video_id": video_id}

            removed = nas_archive.prune_local(paths, project_dir)
            channel_dir = project_dir / channel_slug / video_id
            if not nas_archive.is_archived(channel_dir):
                nas_archive.write_marker(
                    channel_dir,
                    {
                        "archived_at": now.isoformat(),
                        "host": settings.host,
                        "root": settings.root,
                        "removed": removed,
                        "synced": [path.relative_to(project_dir).as_posix() for path in paths],
                    },
                )
            logger.info("nas_reclaim: video_id=%s reclaimed — %d local item(s) removed", video_id, len(removed))
            return {"status": "reclaimed", "video_id": video_id, "removed": removed}
    except FetchLockBusy:
        return {"status": "skipped", "reason": "locked", "video_id": video_id}
