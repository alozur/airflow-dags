"""Tests for congress_videos.nas_archive_dag.

All I/O collaborators (DB, subprocess, filesystem) are mocked or monkeypatched
— no real Airflow execution, Docker, DB, or network.

Test organisation:
  TestDagLoads             — DAG import smoke test, schedule, task graph shape.
  TestCheckEnabled          — check_enabled ShortCircuitOperator callable.
  TestQueryCompleteVideoIds — approval test pinning complete_video_ids()'s SQL
                              shape/return value (moved to modules/nas_completeness.py).
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

from congress_videos.modules import nas_completeness

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
        assert task_ids == {"check_enabled", "select_candidates", "archive_videos", "mirror_shared"}

    def test_task_order(self):
        mod = _fresh()
        check_enabled = mod.dag.get_task("check_enabled")
        select_candidates = mod.dag.get_task("select_candidates")
        archive_videos = mod.dag.get_task("archive_videos")
        mirror_shared = mod.dag.get_task("mirror_shared")
        assert select_candidates.task_id in check_enabled.downstream_task_ids
        assert archive_videos.task_id in select_candidates.downstream_task_ids
        assert mirror_shared.task_id in archive_videos.downstream_task_ids

    def test_mirror_shared_uses_default_trigger_rule(self):
        """mirror_shared must run every enabled run, including zero-candidate
        runs — archive_videos already succeeds on an empty candidate loop, so
        the default "all_success" trigger rule is sufficient; no override
        needed."""
        mod = _fresh()
        assert mod.dag.get_task("mirror_shared").trigger_rule == "all_success"

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
# complete_video_ids (approval test — pins behavior before the move to
# congress_videos/modules/nas_completeness.py)
# ---------------------------------------------------------------------------


class TestQueryCompleteVideoIds:
    """Approval test: pins the query shape and return value unchanged across
    the move into ``modules/nas_completeness.py``. References the imported
    ``complete_video_ids`` name on the DAG module, not a private helper."""

    def _pg_mock(self, monkeypatch, rows):
        cur = MagicMock()
        cur.fetchall.return_value = [{"video_id": vid} for vid in rows]
        conn = MagicMock()
        conn.cursor.return_value.__enter__.return_value = cur
        pg = MagicMock()
        pg.get_qualified_table.side_effect = lambda name: f"test.{name}"
        pg.get_connection.return_value.__enter__.return_value = conn
        monkeypatch.setattr(nas_completeness, "PostgresConnection", lambda: pg)
        return cur

    def test_returns_video_ids_from_query_result(self, monkeypatch):
        mod = _fresh()
        cur = self._pg_mock(monkeypatch, ["abc123", "def456"])

        result = mod.complete_video_ids(50)

        assert result == ["abc123", "def456"]
        cur.execute.assert_called_once()
        (query, params) = cur.execute.call_args.args
        assert params == (50,)

    def test_query_references_every_completeness_table(self, monkeypatch):
        mod = _fresh()
        cur = self._pg_mock(monkeypatch, [])

        mod.complete_video_ids(10)

        query = cur.execute.call_args.args[0]
        for table in (
            "test.video_chapters",
            "test.uploadable_chapters",
            "test.uploadable_turns",
            "test.speaker_turn_videos",
            "test.speaker_turns",
        ):
            assert table in query

    def test_empty_result_returns_empty_list(self, monkeypatch):
        mod = _fresh()
        self._pg_mock(monkeypatch, [])

        assert mod.complete_video_ids(10) == []


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
        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: True)

        candidates = mod.select_archive_candidates(self._settings(tmp_path), tmp_path, "congreso-es-tv")

        assert candidates == []

    def test_skips_videos_with_no_local_paths(self, monkeypatch, tmp_path):
        mod = _fresh()
        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)

        def _raise(*a, **k):
            raise FileNotFoundError("no local paths")

        monkeypatch.setattr(mod.nas_archive, "video_paths", _raise)

        candidates = mod.select_archive_candidates(self._settings(tmp_path), tmp_path, "congreso-es-tv")

        assert candidates == []

    def test_young_video_is_synced_but_not_marked_for_prune(self, monkeypatch, tmp_path):
        """Early sync, late prune: a video too young to prune is still a sync
        candidate (rsync is incremental — re-syncing an unchanged video is
        cheap), just with prune=False."""
        import time

        mod = _fresh()
        video_dir = tmp_path / "downloads" / "2026-03-01" / "abc123"
        video_dir.mkdir(parents=True)
        fresh_file = video_dir / "video.mp4"
        fresh_file.write_bytes(b"data")

        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(mod.nas_archive, "video_paths", lambda *a, **k: [video_dir])
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: time.time())  # brand new

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == [{"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": False}]

    def test_old_enough_video_is_synced_and_marked_for_prune(self, monkeypatch, tmp_path):
        import time

        mod = _fresh()
        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(
            mod.nas_archive, "video_paths", lambda project_dir, channel, video_id: [tmp_path / video_id]
        )
        old_mtime = time.time() - (30 * 86400)
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: old_mtime)

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == [{"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True}]

    def test_sync_cap_limits_the_number_of_candidates(self, monkeypatch, tmp_path):
        import time

        mod = _fresh()
        monkeypatch.setattr(mod, "NAS_ARCHIVE_SYNC_BATCH", 1)
        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123", "def456"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(
            mod.nas_archive, "video_paths", lambda project_dir, channel, video_id: [tmp_path / video_id]
        )
        old_mtime = time.time() - (30 * 86400)
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: old_mtime)

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == [{"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True}]

    def test_prune_cap_is_independent_of_the_sync_cap(self, monkeypatch, tmp_path):
        """Both videos are old enough to prune, but NAS_ARCHIVE_BATCH=1 caps
        prunes at one per run — the second stays synced (prune=False) rather
        than being dropped from the candidate list entirely."""
        import time

        mod = _fresh()
        monkeypatch.setattr(mod, "NAS_ARCHIVE_BATCH", 1)
        monkeypatch.setattr(mod, "complete_video_ids", lambda limit: ["abc123", "def456"])
        monkeypatch.setattr(mod.nas_archive, "is_archived", lambda channel_dir: False)
        monkeypatch.setattr(
            mod.nas_archive, "video_paths", lambda project_dir, channel, video_id: [tmp_path / video_id]
        )
        old_mtime = time.time() - (30 * 86400)
        monkeypatch.setattr(mod, "_newest_mtime", lambda paths: old_mtime)

        candidates = mod.select_archive_candidates(
            self._settings(tmp_path, min_age_days=14), tmp_path, "congreso-es-tv"
        )

        assert candidates == [
            {"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True},
            {"channel_slug": "congreso-es-tv", "video_id": "def456", "prune": False},
        ]


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

    def test_remote_mkdir_runs_before_rsync_and_mkpath_is_never_sent(self, monkeypatch, tmp_path):
        mod = _fresh()
        self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        calls: list[str] = []
        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")

        def fake_subprocess_run(command, **kwargs):
            assert "--mkpath" not in command
            calls.append("mkdir")
            return ok_result

        def fake_runner(command):
            assert "--mkpath" not in command
            calls.append("rsync")
            return ok_result

        monkeypatch.setattr(mod.subprocess, "run", fake_subprocess_run)
        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)

        mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        # remote mkdir precedes both the real rsync push and the verify dry-run.
        assert calls[0] == "mkdir"
        assert "rsync" in calls
        assert calls.index("mkdir") < calls.index("rsync")

    def test_synced_not_pruned_when_prune_is_false(self, monkeypatch, tmp_path):
        """A video that isn't old enough (or lost the prune-cap race) is
        rsynced and verified like any other, but nothing local is deleted
        and no marker is written."""
        mod = _fresh()
        video_dir = self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=ok_result))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: ok_result)

        result = mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123", prune=False)

        assert result == {
            "video_id": "abc123",
            "synced": ["downloads/2026-03-01/abc123"],
            "removed": [],
            "bytes_freed": 0,
            "pruned": False,
        }
        assert video_dir.exists()  # nothing was deleted
        assert (video_dir / "video.mp4").exists()
        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert not marker_path.exists()

    def test_verification_mismatch_aborts_even_when_prune_is_false(self, monkeypatch, tmp_path):
        mod = _fresh()
        video_dir = self._make_video(tmp_path)
        settings = self._settings(tmp_path)

        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        mismatch_result = SimpleNamespace(returncode=0, stdout=">f+++++++++ video.mp4\n", stderr="")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=ok_result))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: mismatch_result)

        with pytest.raises(AirflowException, match="verification failed"):
            mod.archive_one_video(settings, tmp_path, "congreso-es-tv", "abc123", prune=False)

        assert video_dir.exists()
        assert (video_dir / "video.mp4").exists()

    def test_run_archive_videos_aggregates_summary(self, monkeypatch, mock_task_instance):
        mod = _fresh()
        candidates = [
            {"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True},
            {"channel_slug": "congreso-es-tv", "video_id": "def456", "prune": False},
        ]
        mock_task_instance.xcom_store["candidates"] = candidates
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())
        monkeypatch.setattr(
            mod,
            "archive_one_video",
            lambda settings, project_dir, channel_slug, video_id, prune: {
                "video_id": video_id,
                "synced": [],
                "removed": [],
                "bytes_freed": 100,
                "pruned": prune,
            },
        )

        summary = mod._run_archive_videos(ti=mock_task_instance)

        assert summary == {"synced": 2, "pruned": 1, "bytes_freed": 200, "failed": []}

    def test_run_archive_videos_isolates_per_candidate_failure(self, monkeypatch, mock_task_instance):
        """One candidate raising AirflowException must not abort the batch —
        it is recorded in summary["failed"] and the rest still proceed
        (mirrors nas_fetch_dag._run_fetch_videos' per-video isolation)."""
        mod = _fresh()
        candidates = [
            {"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True},
            {"channel_slug": "congreso-es-tv", "video_id": "def456", "prune": False},
        ]
        mock_task_instance.xcom_store["candidates"] = candidates
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        def _archive_one_video(settings, project_dir, channel_slug, video_id, prune):
            if video_id == "abc123":
                raise AirflowException("nas_archive: verification failed for video_id=abc123")
            return {"video_id": video_id, "synced": [], "removed": [], "bytes_freed": 100, "pruned": prune}

        monkeypatch.setattr(mod, "archive_one_video", _archive_one_video)

        summary = mod._run_archive_videos(ti=mock_task_instance)

        assert summary == {
            "synced": 1,
            "pruned": 0,
            "bytes_freed": 100,
            "failed": [{"video_id": "abc123", "error": "nas_archive: verification failed for video_id=abc123"}],
        }

    def test_run_archive_videos_raises_when_every_candidate_fails(self, monkeypatch, mock_task_instance):
        mod = _fresh()
        candidates = [
            {"channel_slug": "congreso-es-tv", "video_id": "abc123", "prune": True},
            {"channel_slug": "congreso-es-tv", "video_id": "def456", "prune": True},
        ]
        mock_task_instance.xcom_store["candidates"] = candidates
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        def _archive_one_video(settings, project_dir, channel_slug, video_id, prune):
            raise AirflowException(f"nas_archive: rsync failed for video_id={video_id}")

        monkeypatch.setattr(mod, "archive_one_video", _archive_one_video)

        with pytest.raises(AirflowException, match="all 2 candidate video"):
            mod._run_archive_videos(ti=mock_task_instance)

    def test_run_archive_videos_succeeds_with_empty_candidate_list(self, monkeypatch, mock_task_instance):
        """Zero candidates must not be misclassified as an all-failed run."""
        mod = _fresh()
        mock_task_instance.xcom_store["candidates"] = []
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        summary = mod._run_archive_videos(ti=mock_task_instance)

        assert summary == {"synced": 0, "pruned": 0, "bytes_freed": 0, "failed": []}


# ---------------------------------------------------------------------------
# mirror_shared_dirs
# ---------------------------------------------------------------------------


class TestMirrorSharedDirs:
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

    def test_no_mirror_dirs_returns_empty_summary(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(side_effect=AssertionError("must not be called")))
        monkeypatch.setattr(mod, "_subprocess_runner", MagicMock(side_effect=AssertionError("must not be called")))

        summary = mod.mirror_shared_dirs(settings, tmp_path)

        assert summary == {"mirrored": [], "changed_lines": 0}

    def test_mirrors_thumbnails_and_counts_changed_lines(self, monkeypatch, tmp_path):
        mod = _fresh()
        (tmp_path / "thumbnails").mkdir()
        settings = self._settings(tmp_path)

        mkdir_ok = SimpleNamespace(returncode=0, stdout="", stderr="")
        rsync_ok = SimpleNamespace(returncode=0, stdout=">f+++++++++ abc.png\n>f+++++++++ abc.json\n", stderr="")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=mkdir_ok))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: rsync_ok)

        summary = mod.mirror_shared_dirs(settings, tmp_path)

        assert summary == {"mirrored": ["thumbnails"], "changed_lines": 2}

    def test_remote_mkdir_failure_raises(self, monkeypatch, tmp_path):
        mod = _fresh()
        (tmp_path / "thumbnails").mkdir()
        settings = self._settings(tmp_path)

        mkdir_failed = SimpleNamespace(returncode=1, stdout="", stderr="permission denied")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=mkdir_failed))

        with pytest.raises(AirflowException, match="remote mkdir failed"):
            mod.mirror_shared_dirs(settings, tmp_path)

    def test_remote_mkdir_runs_before_rsync_and_mkpath_is_never_sent(self, monkeypatch, tmp_path):
        mod = _fresh()
        (tmp_path / "thumbnails").mkdir()
        settings = self._settings(tmp_path)

        calls: list[str] = []
        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")

        def fake_subprocess_run(command, **kwargs):
            assert "--mkpath" not in command
            calls.append("mkdir")
            return ok_result

        def fake_runner(command):
            assert "--mkpath" not in command
            calls.append("rsync")
            return ok_result

        monkeypatch.setattr(mod.subprocess, "run", fake_subprocess_run)
        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)

        mod.mirror_shared_dirs(settings, tmp_path)

        assert calls == ["mkdir", "rsync"]

    def test_rsync_failure_raises(self, monkeypatch, tmp_path):
        mod = _fresh()
        (tmp_path / "thumbnails").mkdir()
        settings = self._settings(tmp_path)

        mkdir_ok = SimpleNamespace(returncode=0, stdout="", stderr="")
        rsync_failed = SimpleNamespace(returncode=1, stdout="", stderr="connection refused")
        monkeypatch.setattr(mod.subprocess, "run", MagicMock(return_value=mkdir_ok))
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: rsync_failed)

        with pytest.raises(AirflowException, match="rsync failed"):
            mod.mirror_shared_dirs(settings, tmp_path)

    def test_run_mirror_shared_reads_settings_and_project_dir(self, monkeypatch, tmp_path):
        mod = _fresh()
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())
        monkeypatch.setattr(mod, "PROJECT_DATA_DIR", str(tmp_path))
        monkeypatch.setattr(
            mod, "mirror_shared_dirs", lambda settings, project_dir: {"mirrored": ["thumbnails"], "changed_lines": 3}
        )

        summary = mod._run_mirror_shared()

        assert summary == {"mirrored": ["thumbnails"], "changed_lines": 3}
