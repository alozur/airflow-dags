"""[RED] Tests for congress_videos.config.analytics_config.

Verifies:
- CHECKPOINTS is a plain dict with exactly five keys and the correct hour values
- MAX_WINDOW_HOURS equals 2160 (90 days in hours)
- METRIC_FIELDS is a list of exactly five field names
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

    def test_metric_fields_has_exactly_five_entries(self):
        cfg = _load()
        assert len(cfg.METRIC_FIELDS) == 5

    def test_metric_fields_contains_views(self):
        cfg = _load()
        assert "views" in cfg.METRIC_FIELDS

    def test_metric_fields_contains_impressions(self):
        cfg = _load()
        assert "impressions" in cfg.METRIC_FIELDS

    def test_metric_fields_contains_impression_ctr(self):
        cfg = _load()
        assert "impressionClickThroughRate" in cfg.METRIC_FIELDS

    def test_metric_fields_contains_average_view_duration(self):
        cfg = _load()
        assert "averageViewDuration" in cfg.METRIC_FIELDS

    def test_metric_fields_contains_watch_time_minutes(self):
        cfg = _load()
        assert "watchTimeMinutes" in cfg.METRIC_FIELDS

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
