"""[RED] Tests for congress_videos.config.analytics_config.

Verifies:
- CHECKPOINTS is a plain dict with exactly five keys and the correct hour values
- MAX_WINDOW_HOURS equals 2160 (90 days in hours)
- METRIC_FIELDS is a list of the ten API-supported metric names
- No Airflow imports leak into this module (import succeeds in isolation)
"""

from __future__ import annotations

import importlib
import sys

import pytest


@pytest.fixture(autouse=True)
def fresh_import():
    """Force fresh import of analytics_config for each test."""
    module_name = "congress_videos.config.analytics_config"
    if module_name in sys.modules:
        del sys.modules[module_name]
    yield
    if module_name in sys.modules:
        del sys.modules[module_name]


def _load():
    return importlib.import_module("congress_videos.config.analytics_config")


class TestCheckpoints:

    def test_checkpoints_has_exactly_five_keys(self):
        cfg = _load()
        assert set(cfg.CHECKPOINTS.keys()) == {"24h", "48h", "7d", "30d", "90d"}

    def test_checkpoint_24h_is_24_hours(self):
        cfg = _load()
        assert cfg.CHECKPOINTS["24h"] == 24

    def test_checkpoint_48h_is_48_hours(self):
        cfg = _load()
        assert cfg.CHECKPOINTS["48h"] == 48

    def test_checkpoint_7d_is_168_hours(self):
        cfg = _load()
        assert cfg.CHECKPOINTS["7d"] == 168

    def test_checkpoint_30d_is_720_hours(self):
        cfg = _load()
        assert cfg.CHECKPOINTS["30d"] == 720

    def test_checkpoint_90d_is_2160_hours(self):
        cfg = _load()
        assert cfg.CHECKPOINTS["90d"] == 2160

    def test_checkpoints_is_plain_dict(self):
        cfg = _load()
        assert isinstance(cfg.CHECKPOINTS, dict)


class TestMaxWindowHours:

    def test_max_window_hours_equals_2160(self):
        cfg = _load()
        assert cfg.MAX_WINDOW_HOURS == 2160

    def test_max_window_hours_matches_90d_checkpoint(self):
        cfg = _load()
        assert cfg.MAX_WINDOW_HOURS == cfg.CHECKPOINTS["90d"]


class TestMetricFields:

    # Every name here must be a metric the YouTube Analytics API accepts for a
    # per-video channel report; impressions/impressionClickThroughRate/
    # watchTimeMinutes are intentionally absent (unsupported by the API).
    _EXPECTED_METRICS = [
        "views",
        "estimatedMinutesWatched",
        "averageViewDuration",
        "averageViewPercentage",
        "likes",
        "dislikes",
        "comments",
        "shares",
        "subscribersGained",
        "subscribersLost",
    ]

    def test_metric_fields_matches_supported_set(self):
        cfg = _load()
        assert cfg.METRIC_FIELDS == self._EXPECTED_METRICS

    def test_metric_fields_has_ten_entries(self):
        cfg = _load()
        assert len(cfg.METRIC_FIELDS) == 10

    def test_metric_fields_excludes_unsupported_metrics(self):
        cfg = _load()
        for unsupported in (
            "impressions",
            "impressionClickThroughRate",
            "watchTimeMinutes",
        ):
            assert unsupported not in cfg.METRIC_FIELDS

    def test_metric_fields_is_a_list(self):
        cfg = _load()
        assert isinstance(cfg.METRIC_FIELDS, list)


class TestNoAirflowImports:

    def test_module_imports_without_airflow(self, monkeypatch):
        """analytics_config must not require Airflow to import."""
        # Block airflow from being importable; module must still load
        monkeypatch.setitem(sys.modules, "airflow", None)
        monkeypatch.setitem(sys.modules, "airflow.models", None)
        # Should not raise even if airflow is absent
        cfg = _load()
        assert cfg.CHECKPOINTS is not None


class TestUnderperformanceActionConstants:
    """[RED] Spec: Per-checkpoint underperformance evaluation / Lifetime action
    cap per video / Checkpoint-scoped action types (issue #102).

    New constants consumed by evaluate_action() (modules/video_analytics.py).
    """

    def test_underperform_ratio_is_half(self):
        cfg = _load()
        assert cfg.UNDERPERFORM_RATIO == 0.5

    def test_min_prior_snapshots_is_ten(self):
        cfg = _load()
        assert cfg.MIN_PRIOR_SNAPSHOTS == 10

    def test_title_update_checkpoints_is_24h_only(self):
        cfg = _load()
        assert cfg.TITLE_UPDATE_CHECKPOINTS == ("24h",)

    def test_max_thumbnail_actions_per_video_is_one(self):
        cfg = _load()
        assert cfg.MAX_THUMBNAIL_ACTIONS_PER_VIDEO == 1

    def test_max_title_actions_per_video_is_one(self):
        cfg = _load()
        assert cfg.MAX_TITLE_ACTIONS_PER_VIDEO == 1

    def test_action_values_mirrors_migration_041_check_constraint(self):
        cfg = _load()
        assert cfg.ACTION_VALUES == {
            "cold_start",
            "ok",
            "capped",
            "in_progress",
            "thumbnail_regenerated",
            "thumbnail_and_title_regenerated",
            "failed",
        }
