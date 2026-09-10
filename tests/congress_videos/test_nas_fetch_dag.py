"""Tests for congress_videos.nas_fetch_dag.

All I/O collaborators (subprocess, filesystem) are mocked or monkeypatched —
no real Airflow execution, Docker, DB, or network.

Test organisation:
  TestDagLoads          — DAG import smoke test, schedule, task graph shape.
  TestCheckEnabled       — check_enabled ShortCircuitOperator callable.
  TestRequestedVideoIds  — conf -> deduplicated video_id list.
  TestFetchOneVideo      — marker-mode fetch -> verify -> refresh ->
                            remove-marker happy path, the marker-less
                            fallback (archive root / legacy root, no marker
                            to remove, never writes to the legacy root), and
                            the abort-before-marker-removal failure paths.
  TestRunFetchVideos     — per-video isolation across a batch.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from airflow.exceptions import AirflowException

MODULE = "congress_videos.nas_fetch_dag"


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
        assert mod.dag.dag_id == "nas_fetch"

    def test_schedule_is_none(self):
        mod = _fresh()
        assert mod.dag.schedule_interval is None

    def test_catchup_is_false(self):
        mod = _fresh()
        assert mod.dag.catchup is False

    def test_max_active_runs_is_one(self):
        mod = _fresh()
        assert mod.dag.max_active_runs == 1

    def test_expected_task_ids_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert task_ids == {"check_enabled", "fetch_videos"}

    def test_task_order(self):
        mod = _fresh()
        check_enabled = mod.dag.get_task("check_enabled")
        fetch_videos = mod.dag.get_task("fetch_videos")
        assert fetch_videos.task_id in check_enabled.downstream_task_ids


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
# _requested_video_ids
# ---------------------------------------------------------------------------


class TestRequestedVideoIds:
    def test_single_video_id(self):
        mod = _fresh()
        assert mod._requested_video_ids({"video_id": "abc123"}) == ["abc123"]

    def test_video_ids_list(self):
        mod = _fresh()
        assert mod._requested_video_ids({"video_ids": ["abc123", "def456"]}) == ["abc123", "def456"]

    def test_both_keys_combined_and_deduplicated(self):
        mod = _fresh()
        conf = {"video_id": "abc123", "video_ids": ["abc123", "def456"]}
        assert mod._requested_video_ids(conf) == ["abc123", "def456"]

    def test_empty_conf_returns_empty_list(self):
        mod = _fresh()
        assert mod._requested_video_ids({}) == []


# ---------------------------------------------------------------------------
# fetch_one_video
# ---------------------------------------------------------------------------


class TestFetchOneVideo:
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

    def _write_marker(self, tmp_path, channel_slug="congreso-es-tv", video_id="abc123"):
        from congress_videos.modules.nas_archive import write_marker

        channel_dir = tmp_path / channel_slug / video_id
        payload = {
            "archived_at": "2026-08-01T00:00:00+00:00",
            "host": "100.64.0.1",
            "root": "/volume1/congress_archive",
            "removed": [],
            "synced": [f"downloads/2026-08-01/{video_id}", f"{channel_slug}/{video_id}"],
        }
        write_marker(channel_dir, payload)
        return channel_dir

    def test_happy_path_fetches_verifies_refreshes_and_removes_marker(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)
        self._write_marker(tmp_path)

        ok_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: ok_result)
        refreshed = []
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: refreshed.extend(paths) or refreshed)

        result = mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert result["video_id"] == "abc123"
        assert result["restored"] == ["downloads/2026-08-01/abc123", "congreso-es-tv/abc123"]
        assert result["source"] == "marker"
        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert not marker_path.exists()

    def test_local_dir_is_created_before_each_pull_and_mkpath_is_never_sent(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)
        self._write_marker(tmp_path)

        calls: list[str] = []
        created_dirs: list = []
        original_ensure_local_dir = mod.nas_fetch.ensure_local_dir

        def fake_ensure_local_dir(local_path):
            calls.append("mkdir")
            result = original_ensure_local_dir(local_path)
            created_dirs.append(result)
            return result

        def fake_runner(command):
            assert "--mkpath" not in command
            calls.append("rsync")
            # The local destination directory must already exist by the time
            # any rsync (the real pull or its verify dry-run) runs.
            assert created_dirs[-1].is_dir()
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod.nas_fetch, "ensure_local_dir", fake_ensure_local_dir)
        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: paths)

        mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        # mkdir precedes both rsync calls (the real pull + the verify dry-run)
        # for each of the two synced directories in the marker.
        assert calls == ["mkdir", "rsync", "rsync", "mkdir", "rsync", "rsync"]

    def test_missing_marker_falls_back_to_discovery_and_raises_when_nothing_found(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)

        empty_result = SimpleNamespace(returncode=0, stdout="", stderr="")
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: empty_result)

        with pytest.raises(FileNotFoundError):
            mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

    def _settings_with_legacy(self, tmp_path):
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
                "NAS_FETCH_LEGACY_ROOT": "/volume1/docker/airflow/congress_videos",
                "NAS_ARCHIVE_SSH_DIR": str(ssh_dir),
            }
        )

    def test_fallback_pulls_from_the_archive_root_when_it_has_a_match(self, monkeypatch, tmp_path):
        """No local marker, but the video exists under NAS_ARCHIVE_ROOT — the
        video is fetched from there, and there is no marker to remove."""
        mod = _fresh()
        settings = self._settings_with_legacy(tmp_path)

        def fake_runner(command):
            if "sh" in command and "-c" in command:
                assert "--mkpath" not in command
                return SimpleNamespace(returncode=0, stdout=f"{settings.root}/congreso-es-tv/abc123\n", stderr="")
            assert "--mkpath" not in command
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: paths)

        result = mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert result["source"] == "archive-root"
        assert result["restored"] == ["congreso-es-tv/abc123"]
        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert not marker_path.exists()  # nothing to remove — none was ever written by this test

    def test_fallback_pulls_from_the_legacy_root_when_archive_root_has_nothing(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings_with_legacy(tmp_path)

        def fake_runner(command):
            snippet = command[-1] if ("sh" in command and "-c" in command) else ""
            if settings.legacy_root in snippet:
                return SimpleNamespace(
                    returncode=0, stdout=f"{settings.legacy_root}/congreso-es-tv/abc123\n", stderr=""
                )
            if "sh" in command and "-c" in command:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            # rsync pull / verify dry-run against the legacy root
            assert settings.legacy_root in command[-2] or settings.legacy_root in command[-1]
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: paths)

        result = mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        assert result["source"] == "legacy-root"
        assert result["restored"] == ["congreso-es-tv/abc123"]

    def test_fallback_never_writes_deletes_or_pushes_to_the_legacy_root(self, monkeypatch, tmp_path):
        """Every command issued while pulling from the legacy root is a read
        (discovery sh -c, or an rsync PULL where the legacy root is the
        source, never the destination)."""
        mod = _fresh()
        settings = self._settings_with_legacy(tmp_path)
        commands = []

        def fake_runner(command):
            commands.append(command)
            snippet = command[-1] if ("sh" in command and "-c" in command) else ""
            if settings.legacy_root in snippet:
                return SimpleNamespace(
                    returncode=0, stdout=f"{settings.legacy_root}/congreso-es-tv/abc123\n", stderr=""
                )
            if "sh" in command and "-c" in command:
                return SimpleNamespace(returncode=0, stdout="", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: paths)

        mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        for command in commands:
            if "rsync" not in command:
                continue
            # rsync argv is [..., source, destination]: the legacy root must
            # only ever appear as the remote SOURCE (second-to-last, prefixed
            # with the ssh user@host), never as the destination (last token,
            # always a local filesystem path).
            assert settings.legacy_root not in command[-1]
            if settings.legacy_root in command[-2]:
                assert command[-2].startswith(f"nas-archive@100.64.0.1:{settings.legacy_root}")

    def test_rsync_failure_aborts_before_marker_removal(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)
        self._write_marker(tmp_path)

        rsync_failed = SimpleNamespace(returncode=1, stdout="", stderr="connection refused")
        monkeypatch.setattr(mod, "_subprocess_runner", lambda command: rsync_failed)

        with pytest.raises(AirflowException, match="rsync failed"):
            mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert marker_path.exists()

    def test_verification_mismatch_aborts_before_marker_removal(self, monkeypatch, tmp_path):
        mod = _fresh()
        settings = self._settings(tmp_path)
        self._write_marker(tmp_path)

        results = {
            "call": 0,
        }

        def fake_runner(command):
            results["call"] += 1
            if "--dry-run" in command:
                return SimpleNamespace(returncode=0, stdout=">f+++++++++ video.mp4\n", stderr="")
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)

        with pytest.raises(AirflowException, match="verification failed"):
            mod.fetch_one_video(settings, tmp_path, "congreso-es-tv", "abc123")

        marker_path = tmp_path / "congreso-es-tv" / "abc123" / ".nas_archived.json"
        assert marker_path.exists()


# ---------------------------------------------------------------------------
# _run_fetch_videos
# ---------------------------------------------------------------------------


class TestRunFetchVideos:
    def test_raises_when_conf_has_no_video_ids(self, monkeypatch):
        mod = _fresh()
        dag_run = MagicMock()
        dag_run.conf = {}

        with pytest.raises(AirflowException, match="video_id"):
            mod._run_fetch_videos(dag_run=dag_run)

    def test_one_failure_does_not_block_the_other_video(self, monkeypatch):
        mod = _fresh()
        dag_run = MagicMock()
        dag_run.conf = {"video_ids": ["good", "bad"]}
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        def fake_fetch(settings, project_dir, channel_slug, video_id):
            if video_id == "bad":
                raise AirflowException("nas_fetch: rsync failed")
            return {"video_id": video_id, "channel_slug": channel_slug, "restored": ["x"], "media_refreshed": 1}

        monkeypatch.setattr(mod, "fetch_one_video", fake_fetch)

        summary = mod._run_fetch_videos(dag_run=dag_run)

        assert len(summary["restored"]) == 1
        assert summary["restored"][0]["video_id"] == "good"
        assert len(summary["failed"]) == 1
        assert summary["failed"][0]["video_id"] == "bad"

    def test_uses_default_channel_when_conf_omits_it(self, monkeypatch):
        mod = _fresh()
        dag_run = MagicMock()
        dag_run.conf = {"video_id": "abc123"}
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        captured = {}

        def fake_fetch(settings, project_dir, channel_slug, video_id):
            captured["channel_slug"] = channel_slug
            return {"video_id": video_id, "channel_slug": channel_slug, "restored": [], "media_refreshed": 0}

        monkeypatch.setattr(mod, "fetch_one_video", fake_fetch)

        mod._run_fetch_videos(dag_run=dag_run)

        assert captured["channel_slug"] == mod.DEFAULT_CHANNEL

    def test_honors_explicit_channel_slug(self, monkeypatch):
        mod = _fresh()
        dag_run = MagicMock()
        dag_run.conf = {"video_id": "abc123", "channel_slug": "other-channel"}
        monkeypatch.setattr(mod, "ArchiveSettings", MagicMock())

        captured = {}

        def fake_fetch(settings, project_dir, channel_slug, video_id):
            captured["channel_slug"] = channel_slug
            return {"video_id": video_id, "channel_slug": channel_slug, "restored": [], "media_refreshed": 0}

        monkeypatch.setattr(mod, "fetch_one_video", fake_fetch)

        mod._run_fetch_videos(dag_run=dag_run)

        assert captured["channel_slug"] == "other-channel"

    def test_runner_timeout_isolates_the_failing_video_and_leaves_its_marker(self, monkeypatch, tmp_path):
        """subprocess.TimeoutExpired from the rsync runner (HIGH finding): a hung/slow
        rsync for one video must not abort the rest of the batch, and the failing
        video's marker must stay in place — same contract as the other failure modes
        in test_one_failure_does_not_block_the_other_video, but exercised through the
        real fetch_one_video (not a fake), via the actual runner injection point."""
        from congress_videos.modules.nas_archive import write_marker

        mod = _fresh()

        ssh_dir = tmp_path / "nas_sync"
        ssh_dir.mkdir()
        (ssh_dir / "id_ed25519").write_text("key")
        (ssh_dir / "known_hosts").write_text("hosts")
        monkeypatch.setenv("NAS_ARCHIVE_HOST", "100.64.0.1")
        monkeypatch.setenv("NAS_ARCHIVE_USER", "nas-archive")
        monkeypatch.setenv("NAS_ARCHIVE_ROOT", "/volume1/congress_archive")
        monkeypatch.setenv("NAS_ARCHIVE_SSH_DIR", str(ssh_dir))
        monkeypatch.setattr(mod, "PROJECT_DATA_DIR", tmp_path)

        channel_slug = "congreso-es-tv"
        video_ids = ["vid1", "vid2", "vid3"]
        for video_id in video_ids:
            write_marker(
                tmp_path / channel_slug / video_id,
                {
                    "archived_at": "2026-08-01T00:00:00+00:00",
                    "host": "100.64.0.1",
                    "root": "/volume1/congress_archive",
                    "removed": [],
                    "synced": [f"{channel_slug}/{video_id}"],
                },
            )

        def fake_runner(command):
            if "vid2" in " ".join(command):
                raise subprocess.TimeoutExpired(cmd=command, timeout=mod._RSYNC_TIMEOUT_SECS)
            return SimpleNamespace(returncode=0, stdout="", stderr="")

        monkeypatch.setattr(mod, "_subprocess_runner", fake_runner)
        monkeypatch.setattr(mod.nas_fetch, "refresh_retention", lambda paths, now: paths)

        dag_run = MagicMock()
        dag_run.conf = {"video_ids": video_ids}

        summary = mod._run_fetch_videos(dag_run=dag_run)

        restored_ids = {r["video_id"] for r in summary["restored"]}
        failed_ids = {f["video_id"] for f in summary["failed"]}
        assert restored_ids == {"vid1", "vid3"}
        assert failed_ids == {"vid2"}

        marker_path = tmp_path / channel_slug / "vid2" / ".nas_archived.json"
        assert marker_path.exists(), "vid2's marker must stay in place after an aborted fetch"
