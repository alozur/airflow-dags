"""Tests for congress_videos.nas_archive_dag.

All I/O collaborators (DB, subprocess, filesystem) are mocked or monkeypatched
— no real Airflow execution, Docker, DB, or network.

Test organisation:
  TestDagLoads             — DAG import smoke test, schedule, task graph shape.
  TestCheckEnabled          — check_enabled ShortCircuitOperator callable.
  TestSelectArchiveCandidates — eligibility pool -> filtered candidate batch.
  TestArchiveOneVideo       — sync -> verify -> prune -> marker happy path and
                              the abort-before-delete failure path.
"""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

MODULE = "congress_videos.nas_archive_dag"


def _fresh():
    if MODULE in sys.modules:
        del sys.modules[MODULE]
    return importlib.import_module(MODULE)


# ---------------------------------------------------------------------------
# DAG load + task graph shape
# ---------------------------------------------------------------------------


class TestDagLoads:
    def test_dag_imports_cleanly(self):
        mod = _fresh()
        assert mod.dag is not None

    def test_dag_id(self):
        mod = _fresh()
        assert mod.dag.dag_id == "nas_archive"

    def test_schedule_is_daily_cron(self):
        mod = _fresh()
        assert mod.dag.schedule_interval == "0 4 * * *"

    def test_catchup_is_false(self):
        mod = _fresh()
        assert mod.dag.catchup is False

    def test_max_active_runs_and_tasks_are_one(self):
        mod = _fresh()
        assert mod.dag.max_active_runs == 1
        assert mod.dag.max_active_tasks == 1

    def test_is_paused_upon_creation(self):
        mod = _fresh()
        assert mod.dag.is_paused_upon_creation is True

    def test_expected_task_ids_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert task_ids == {"check_enabled", "select_candidates", "archive_videos"}

    def test_task_order(self):
        mod = _fresh()
        check_enabled = mod.dag.get_task("check_enabled")
        select_candidates = mod.dag.get_task("select_candidates")
        archive_videos = mod.dag.get_task("archive_videos")
        assert select_candidates.task_id in check_enabled.downstream_task_ids
        assert archive_videos.task_id in select_candidates.downstream_task_ids

    def test_no_pool_declared(self):
        """DAG-level max_active_tasks=1 already serializes; no extra pool needed
        (adding pool="default_pool" would break test_contract.py's pool-scan)."""
        mod = _fresh()
        for task in mod.dag.tasks:
            assert task.pool == "default_pool"


# ---------------------------------------------------------------------------
# check_enabled
# ---------------------------------------------------------------------------


class TestCheckEnabled:
    def test_disabled_by_default_returns_false(self, monkeypatch):
        mod = _fresh()
        monkeypatch.delenv("NAS_ARCHIVE_HOST", raising=False)
        assert mod._check_enabled() is False

    def test_invalid_config_returns_false(self, monkeypatch):
        mod = _fresh()
        monkeypatch.setenv("NAS_ARCHIVE_HOST", "100.64.0.1")
        monkeypatch.setenv("NAS_ARCHIVE_PORT", "not-a-number")
        assert mod._check_enabled() is False

    def test_enabled_and_valid_returns_true(self, monkeypatch, tmp_path):
        mod = _fresh()
        ssh_dir = tmp_path / "nas_sync"
        ssh_dir.mkdir()
        (ssh_dir / "id_ed25519").write_text("key")
        (ssh_dir / "known_hosts").write_text("hosts")
        monkeypatch.setenv("NAS_ARCHIVE_HOST", "100.64.0.1")
        monkeypatch.setenv("NAS_ARCHIVE_USER", "nas-archive")
        monkeypatch.setenv("NAS_ARCHIVE_ROOT", "/volume1/congress_archive")
        monkeypatch.setenv("NAS_ARCHIVE_SSH_DIR", str(ssh_dir))
        assert mod._check_enabled() is True


# ---------------------------------------------------------------------------
# select_archive_candidates
# ---------------------------------------------------------------------------


class TestSelectArchiveCandidates:
    def _settings(self, tmp_path, min_age_days=14):
        from congress_videos.modules.nas_archive import ArchiveSettings

        ssh_dir = tmp_path / "nas_sync"
        ssh_dir.mkdir()
        (ssh_dir / "id_ed25519").write_text("key")
        (ssh_dir / "known_hosts").write_text("hosts")
        return ArchiveSettings.from_env(
            {
                "NAS_ARCHIVE_HOST": "100.64.0.1",
                "NAS_ARCHIVE_USER": "nas-archive",
                "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
                "NAS_ARCHIVE_MIN_AGE_DAYS": str(min_age_days),
                "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
            }
        )

    def test_skips_already_archived_videos(self, monkeypatch, tmp_path):
        mod = _fresh()
        monkeypatch.setattr(mod, "_query_complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: True)

        candidates = mod.select_archive_candidates(self._settings(tmp_path), tmp_path, "congreso-es-tv")

        assert candidates == []

    def test_skips_videos_with_no_local_paths(self, monkeypatch, tmp_path):
        mod = _fresh()
        monkeypatch.setattr(mod, "_query_complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)

        def _raise(*a, **k):
            raise FileNotFoundError("no local paths")

        monkeypatch.setattr(mod.nas_archive, "video_paths", _raise)

        candidates = mod.select_archive_candidates(self._settings(tmp_path), tmp_path, "congreso-es-tv")

        assert candidates == []

    def test_skips_videos_younger_than_min_age(self, monkeypatch, tmp_path):
        import time

        mod = _fresh()
        video_dir = tmp_path / "downloads" / "2026-03-01" / "abc123"
        video_dir.mkdir(parents=True)
        fresh_file = video_dir / "video.mp4"
        fresh_file.write_bytes(b"data")

        monkeypatch.setattr(mod, "_query_complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(mod.nas_archive, "video_paths", lambda *a, **k: [video_dir])
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: time.time())  # brand new

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == []

    def test_accepts_old_enough_video_and_stops_at_batch_cap(self, monkeypatch, tmp_path):
        import time

        mod = _fresh()
        monkeypatch.setattr(mod, "NAS_ARCHIVE_BATCH", 1)
        monkeypatch.setattr(mod, "_query_complete_video_ids", lambda limit: ["abc123", "def456"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(
            mod.nas_archive, "video_paths", lambda project_dir, channel, video_id: [tmp_path / video_id]
        )
        old_mtime = time.time() - (30 * 86400)
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: old_mtime)

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == [{"channel_slug": "congreso-es-tv", "video_id": "abc123"}]


# ---------------------------------------------------------------------------
# archive_one_video
# ---------------------------------------------------------------------------


class TestArchiveOneVideo:
    def _settings(self, tmp_path):
        from congress_videos.modules.nas_archive import ArchiveSettings

        ssh_dir = tmp_path / "nas_sync"
        ssh_dir.mkdir()
        (ssh_dir / "id_ed25519").write_text("key")
        (ssh_dir / "known_hosts").write_text("hosts")
        return ArchiveSettings.from_env(
            {
                "NAS_ARCHIVE_HOST": "100.64.0.1",
                "NAS_ARCHIVE_USER": "nas-archive",
                "NAS_ARCHIVE_ROOT": "/volume1/congress_archive",
                "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
            }
        )

    def _make_video(self, tmp_path):
        video_dir = tmp_path / "downloads" / "2026-03-01" / "abc123"
        video_dir.mkdir(parents=True)
        (video_dir / "video.mp4").write_bytes(b"raw video data")
        return video_dir

    def test_happy_path_syncs_verifies_prunes_and_writes_marker(self, monkeypatch, tmp_path):
        mod = _fresh()
        video_dir = self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=ok_result))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: ok_result)

        result = mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert result["video_id"] == "abc123"
        assert not video_dir.exists()  # raw download dir fully removed
        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert marker_path.exists()

    def test_verification_mismatch_aborts_before_any_deletion(self, monkeypatch, tmp_path):
        mod = _fresh()
        video_dir = self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        mismatch_result = SimpleNamespace(returncode=0, stdout=">f+++++++++ video.mp4\n", stderr="")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=ok_result))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: mismatch_result)

        with pytest.raises(AirflowException, match="verification failed"):
            mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert video_dir.exists()  # nothing was deleted
        assert (video_dir / "video.mp4").exists()

    def test_rsync_failure_aborts_before_any_deletion(self, monkeypatch, tmp_path):
        mod = _fresh()
        video_dir = self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        mkdir_ok = SimpleNamespace(returncode=0, stdout="", stderr="")
        rsync_failed = SimpleNamespace(returncode=1, stdout="", stderr="connection refused")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=mkdir_ok))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: rsync_failed)

        with pytest.raises(AirflowException, match="rsync failed"):
            mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert video_dir.exists()

    def test_run_archive_videos_aggregates_summary(self, monkeypatch, mock_task_instance):
        mod = _fresh()
        candidates = [
            {"channel_slug": "congreso-es-tv", "video_id": "abc123"},
            {"channel_slug": "congreso-es-tv", "video_id": "def456"},
        ]
        mock_task_instance.xcom_store["candidates"] = candidates
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())
        monkeypatch.setattr(
            mod,
            "archive_one_video",
            lambda settings, project_dir, channel_slug, video_id: {
                "video_id": video_id,
                "synced": [],
                "removed": [],
                "bytes_freed": 100,
            },
        )

        summary = mod._run_archive_videos(ti=mock_task_instance)

        assert summary == {"archived": 2, "bytes_freed": 200}
