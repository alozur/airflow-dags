"""Tests for congress_videos.generic_thumbnail_generator_dag (Slice 3).

Strict TDD: all tests below are written BEFORE the production DAG file exists.
First run must produce ImportError failures.

Test groups:
    T-01  DAG imports cleanly (no import errors)
    T-02  schedule is None
    T-03  task-id set (exact, no more, no fewer)
    T-04  task dependency graph shape
    T-05  no Congreso-specific string literals in source
    T-06  validate_input callable behaviour
    T-07  TRIANGULATE — PIKZELS_API_KEY absent, ConfigError on unknown domain
"""

from __future__ import annotations

import importlib
import os
import re
import sys
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# T-01  DAG imports cleanly
# ---------------------------------------------------------------------------


class TestDagImport:
    """T-01: The DAG module must import without raising any exception."""

    def test_dag_imports_cleanly(self) -> None:
        """Importing the DAG module must not raise any exception."""
        # Force a fresh import so this test works regardless of import order.
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        # This must not raise.
        mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        assert mod is not None


# ---------------------------------------------------------------------------
# T-02  schedule is None
# ---------------------------------------------------------------------------


class TestDagSchedule:
    """T-02: The loaded DAG must have schedule=None (on-demand only)."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_schedule_is_none(self) -> None:
        dag = self.mod.dag
        # Airflow 2.x exposes the schedule via schedule_interval; schedule=None → schedule_interval is None.
        assert dag.schedule_interval is None


# ---------------------------------------------------------------------------
# T-03  Exact task-id set
# ---------------------------------------------------------------------------


EXPECTED_TASK_IDS = {
    "validate_input",
    "resolve_participant_photo",
    "generate_thumbnail_option_a",
    "download_option_a",
    "score_option_a",
    "generate_thumbnail_option_b",
    "download_option_b",
    "score_option_b",
    "choose_best_option",
    "generate_title",
    "persist_results",
}


class TestDagTaskIds:
    """T-03: DAG must contain exactly the expected task IDs — no more, no fewer."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_exact_task_id_set(self) -> None:
        dag = self.mod.dag
        actual_ids = {task.task_id for task in dag.tasks}
        assert actual_ids == EXPECTED_TASK_IDS, (
            f"Task ID mismatch.\n"
            f"  Extra: {actual_ids - EXPECTED_TASK_IDS}\n"
            f"  Missing: {EXPECTED_TASK_IDS - actual_ids}"
        )


# ---------------------------------------------------------------------------
# T-04  Task dependency graph shape
# ---------------------------------------------------------------------------


class TestDagDependencies:
    """T-04: Verify the exact upstream sets for key DAG tasks."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")
        self.dag = self.mod.dag

    def _upstream_ids(self, task_id: str) -> set[str]:
        task = self.dag.get_task(task_id)
        return {t.task_id for t in task.upstream_list}

    def test_resolve_participant_photo_upstream_is_validate_input(self) -> None:
        assert self._upstream_ids("resolve_participant_photo") == {"validate_input"}

    def test_generate_thumbnail_option_a_upstream_is_resolve_participant_photo(self) -> None:
        assert self._upstream_ids("generate_thumbnail_option_a") == {"resolve_participant_photo"}

    def test_generate_thumbnail_option_b_upstream_is_resolve_participant_photo(self) -> None:
        assert self._upstream_ids("generate_thumbnail_option_b") == {"resolve_participant_photo"}

    def test_download_option_a_upstream_is_generate_thumbnail_option_a(self) -> None:
        assert self._upstream_ids("download_option_a") == {"generate_thumbnail_option_a"}

    def test_download_option_b_upstream_is_generate_thumbnail_option_b(self) -> None:
        assert self._upstream_ids("download_option_b") == {"generate_thumbnail_option_b"}

    def test_score_option_a_upstream_is_download_option_a(self) -> None:
        assert self._upstream_ids("score_option_a") == {"download_option_a"}

    def test_score_option_b_upstream_is_download_option_b(self) -> None:
        assert self._upstream_ids("score_option_b") == {"download_option_b"}

    def test_choose_best_option_upstream_is_both_score_tasks(self) -> None:
        assert self._upstream_ids("choose_best_option") == {"score_option_a", "score_option_b"}

    def test_generate_title_upstream_is_choose_best_option(self) -> None:
        assert self._upstream_ids("generate_title") == {"choose_best_option"}

    def test_persist_results_upstream_is_generate_title(self) -> None:
        assert self._upstream_ids("persist_results") == {"generate_title"}


# ---------------------------------------------------------------------------
# T-05  No Congreso-specific string literals in DAG source
# ---------------------------------------------------------------------------


class TestNoCongresoBranding:
    """T-05: DAG source must not contain Congreso-specific literals outside imports."""

    def test_no_congreso_literals_in_dag_source(self) -> None:
        import congress_videos.generic_thumbnail_generator_dag as dag_mod
        source_path = dag_mod.__file__
        assert source_path is not None, "Module has no __file__"
        with open(source_path, encoding="utf-8") as fh:
            source = fh.read()

        # Strip import lines to allow 'congress_videos' package paths in import statements.
        non_import_lines = [
            line for line in source.splitlines()
            if not re.match(r"\s*(import|from)\s+", line)
        ]
        body = "\n".join(non_import_lines)

        forbidden = re.compile(r'\b(congreso|diputado|CONGRESO)\b', re.IGNORECASE)
        matches = forbidden.findall(body)
        assert not matches, (
            f"Found Congreso-specific literals in DAG task logic: {matches}"
        )


# ---------------------------------------------------------------------------
# T-06  validate_input callable behaviour
# ---------------------------------------------------------------------------


VALID_CONF = {
    "youtube_video_id": "abc123",
    "chapter_id": 7,
    "debate_summary": "El debate sobre pensiones fue intenso.",
    "session": "Pleno 2026-06-10",
    "domain": "congreso",
    "normalized_name": "garcia_lopez_maria",
}


class TestValidateInput:
    """T-06: validate_input must accept valid conf and reject missing/empty keys."""

    @pytest.fixture(autouse=True)
    def _dag_mod(self) -> None:
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        self.mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

    def test_valid_conf_does_not_raise(self) -> None:
        """All 6 required keys present → no exception."""
        # Call the underlying function directly (not via Airflow trigger).
        self.mod.validate_input(VALID_CONF)

    @pytest.mark.parametrize("missing_key", list(VALID_CONF.keys()))
    def test_missing_key_raises_value_error(self, missing_key: str) -> None:
        """Omitting any required key must raise ValueError."""
        conf = {k: v for k, v in VALID_CONF.items() if k != missing_key}
        with pytest.raises(ValueError, match=missing_key):
            self.mod.validate_input(conf)

    @pytest.mark.parametrize("empty_key", list(VALID_CONF.keys()))
    def test_empty_string_raises_value_error(self, empty_key: str) -> None:
        """Empty string for any required key must raise ValueError."""
        conf = {**VALID_CONF, empty_key: ""}
        with pytest.raises(ValueError, match=empty_key):
            self.mod.validate_input(conf)


# ---------------------------------------------------------------------------
# T-07  TRIANGULATE — PIKZELS_API_KEY absent, ConfigError on unknown domain
# ---------------------------------------------------------------------------


class TestTriangulate:
    """T-07: Edge cases — missing API key and unknown domain."""

    def test_pikzels_api_key_absent_raises_environment_error(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """When PIKZELS_API_KEY is absent, PikzelsClient construction raises EnvironmentError."""
        # Remove key from environment; force fresh module import.
        monkeypatch.delenv("PIKZELS_API_KEY", raising=False)

        # Clear cached pikzels_client so it re-reads env on import.
        for mod_name in list(sys.modules.keys()):
            if "pikzels_client" in mod_name:
                del sys.modules[mod_name]

        with pytest.raises(EnvironmentError, match="PIKZELS_API_KEY"):
            from congress_videos.modules import pikzels_client as _pc  # noqa: F401
            _pc.PikzelsClient()

    def test_config_error_on_unknown_domain(self) -> None:
        """validate_input with an unknown domain raises ConfigError via get_domain_config."""
        if "congress_videos.generic_thumbnail_generator_dag" in sys.modules:
            del sys.modules["congress_videos.generic_thumbnail_generator_dag"]
        mod = importlib.import_module("congress_videos.generic_thumbnail_generator_dag")

        from congress_videos.config.thumbnail_config import ConfigError

        conf = {**VALID_CONF, "domain": "unknown_domain_xyz"}
        # The DAG's validate_input must call get_domain_config and propagate ConfigError.
        with pytest.raises(ConfigError):
            mod.validate_input(conf)
