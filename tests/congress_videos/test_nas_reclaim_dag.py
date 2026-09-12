"""Tests for congress_videos.nas_reclaim_dag.

All I/O collaborators (DB, subprocess, filesystem, locking) are mocked or
monkeypatched — no real Airflow execution, Docker, DB, or network. Gate
logic lives in ``congress_videos.modules.nas_reclaim`` and is covered by
``test_nas_reclaim.py``; this file exercises only the thin DAG wiring.
"""

from __future__ import annotations

import importlib
import sys

import pytest
from airflow.exceptions import AirflowException

MODULE = "congress_videos.nas_reclaim_dag"


def _fresh():
    if MODULE in sys.modules:
        del sys.modules[MODULE]
    return importlib.import_module(MODULE)


def _reclaim_by_id(results_by_id: dict):
    """Fake ``reclaim_one_video`` returning a pre-built result keyed by video_id."""

    def _fake(settings, project_dir, channel_slug, video_id, *, runner, now):
        return results_by_id[video_id]

    return _fake


def _fake_settings(monkeypatch, mod) -> None:
    monkeypatch.setattr(mod, "ArchiveSettings", type("S", (), {"from_env": staticmethod(lambda: object())}))


TWO_CANDIDATES = [
    {"channel_slug": "congreso-es-tv", "video_id": "abc123"},
    {"channel_slug": "congreso-es-tv", "video_id": "def456"},
]


# ---------------------------------------------------------------------------
# DAG load + task graph shape
# ---------------------------------------------------------------------------


class TestDagLoads:
    def test_dag_imports_cleanly(self):
        mod = _fresh()
        assert mod.dag is not None

    def test_dag_id(self):
        mod = _fresh()
        assert mod.dag.dag_id == "nas_reclaim"

    def test_schedule_is_every_four_hours(self):
        mod = _fresh()
        assert mod.dag.schedule_interval == "0 */4 * * *"

    def test_catchup_is_false(self):
        mod = _fresh()
        assert mod.dag.catchup is False

    def test_max_active_runs_is_one(self):
        mod = _fresh()
        assert mod.dag.max_active_runs == 1

    def test_is_paused_upon_creation(self):
        mod = _fresh()
        assert mod.dag.is_paused_upon_creation is True

    def test_expected_task_ids_present(self):
        mod = _fresh()
        task_ids = {t.task_id for t in mod.dag.tasks}
        assert task_ids == {"check_enabled", "select_candidates", "reclaim_videos"}

    def test_task_order(self):
        mod = _fresh()
        check_enabled = mod.dag.get_task("check_enabled")
        select_candidates = mod.dag.get_task("select_candidates")
        reclaim_videos = mod.dag.get_task("reclaim_videos")
        assert select_candidates.task_id in check_enabled.downstream_task_ids
        assert reclaim_videos.task_id in select_candidates.downstream_task_ids


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
# _run_select_candidates — thin wiring: DB pool -> select_reclaim_candidates
# ---------------------------------------------------------------------------


class TestRunSelectCandidates:
    def test_wires_db_pool_into_select_reclaim_candidates(self, monkeypatch, mock_task_instance):
        """The task pulls a DB pool sized off NAS_RECLAIM_BATCH and passes it
        straight through to select_reclaim_candidates with batch=NAS_RECLAIM_BATCH
        — no gate logic duplicated in the DAG module (7.5)."""
        mod = _fresh()
        monkeypatch.setattr(mod, "NAS_RECLAIM_BATCH", 3)
        seen: dict = {}

        def _fake_complete_video_ids(limit):
            seen["pool_limit"] = limit
            return ["abc123", "def456"]

        def _fake_select(settings, project_dir, channel_slug, complete_ids, *, now, batch):
            seen.update({"complete_ids": complete_ids, "batch": batch})
            return [{"channel_slug": channel_slug, "video_id": "abc123"}]

        _fake_settings(monkeypatch, mod)
        monkeypatch.setattr(mod, "complete_video_ids", _fake_complete_video_ids)
        monkeypatch.setattr(mod, "select_reclaim_candidates", _fake_select)

        result = mod._run_select_candidates(ti=mock_task_instance)

        assert seen["pool_limit"] == 3 * mod._CANDIDATE_POOL_MULTIPLIER
        assert seen["complete_ids"] == ["abc123", "def456"]
        assert seen["batch"] == 3
        assert (
            mock_task_instance.xcom_store["candidates"]
            == result
            == [{"channel_slug": "congreso-es-tv", "video_id": "abc123"}]
        )


# ---------------------------------------------------------------------------
# _run_reclaim_videos — per-candidate isolation + all-failed raise
# ---------------------------------------------------------------------------


class TestRunReclaimVideos:
    def test_aggregates_summary_by_status(self, monkeypatch, mock_task_instance):
        mod = _fresh()
        candidates = [*TWO_CANDIDATES, {"channel_slug": "congreso-es-tv", "video_id": "ghi789"}]
        mock_task_instance.xcom_store["candidates"] = candidates
        _fake_settings(monkeypatch, mod)

        results_by_id = {
            "abc123": {"status": "reclaimed", "video_id": "abc123", "removed": ["video.mp4"]},
            "def456": {"status": "blocked", "reason": "grace_window", "video_id": "def456"},
            "ghi789": {"status": "skipped", "reason": "locked", "video_id": "ghi789"},
        }
        monkeypatch.setattr(mod, "reclaim_one_video", _reclaim_by_id(results_by_id))

        summary = mod._run_reclaim_videos(ti=mock_task_instance)

        assert summary == {"reclaimed": 1, "blocked": 1, "skipped": 1, "failed": []}

    def test_isolates_per_candidate_failure(self, monkeypatch, mock_task_instance):
        """One candidate raising an unexpected error (e.g. prune_local's
        protected-path guard) must not abort the batch — it is recorded in
        summary["failed"] and the rest still proceed."""
        mod = _fresh()
        mock_task_instance.xcom_store["candidates"] = TWO_CANDIDATES
        _fake_settings(monkeypatch, mod)

        def _reclaim_one_video(settings, project_dir, channel_slug, video_id, *, runner, now):
            if video_id == "abc123":
                raise ValueError("refusing to prune protected path")
            return {"status": "reclaimed", "video_id": video_id, "removed": ["video.mp4"]}

        monkeypatch.setattr(mod, "reclaim_one_video", _reclaim_one_video)

        summary = mod._run_reclaim_videos(ti=mock_task_instance)

        assert summary == {
            "reclaimed": 1,
            "blocked": 0,
            "skipped": 0,
            "failed": [{"video_id": "abc123", "error": "refusing to prune protected path"}],
        }

    def test_raises_when_every_candidate_fails(self, monkeypatch, mock_task_instance):
        mod = _fresh()
        mock_task_instance.xcom_store["candidates"] = TWO_CANDIDATES
        _fake_settings(monkeypatch, mod)

        def _reclaim_one_video(settings, project_dir, channel_slug, video_id, *, runner, now):
            raise ValueError(f"refusing to prune protected path for video_id={video_id}")

        monkeypatch.setattr(mod, "reclaim_one_video", _reclaim_one_video)

        with pytest.raises(AirflowException, match="all 2 candidate video"):
            mod._run_reclaim_videos(ti=mock_task_instance)

    def test_all_blocked_or_skipped_is_not_treated_as_all_failed(self, monkeypatch, mock_task_instance):
        """Blocked/skipped are normal gate outcomes, not errors — a run where
        every candidate is blocked or skipped is a quiet, successful no-op,
        not an all-failed run (mirrors D6: only real failures raise)."""
        mod = _fresh()
        mock_task_instance.xcom_store["candidates"] = TWO_CANDIDATES
        _fake_settings(monkeypatch, mod)

        results_by_id = {
            "abc123": {"status": "blocked", "reason": "grace_window", "video_id": "abc123"},
            "def456": {"status": "skipped", "reason": "locked", "video_id": "def456"},
        }
        monkeypatch.setattr(mod, "reclaim_one_video", _reclaim_by_id(results_by_id))

        summary = mod._run_reclaim_videos(ti=mock_task_instance)

        assert summary == {"reclaimed": 0, "blocked": 1, "skipped": 1, "failed": []}

    def test_succeeds_with_empty_candidate_list(self, monkeypatch, mock_task_instance):
        """Zero candidates must not be misclassified as an all-failed run."""
        mod = _fresh()
        mock_task_instance.xcom_store["candidates"] = []
        _fake_settings(monkeypatch, mod)

        summary = mod._run_reclaim_videos(ti=mock_task_instance)

        assert summary == {"reclaimed": 0, "blocked": 0, "skipped": 0, "failed": []}
