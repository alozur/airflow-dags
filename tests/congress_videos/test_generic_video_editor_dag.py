"""Tests for congress_videos.generic_video_editor_dag (T-09 — REQ-09).

Strict TDD — all tests are written BEFORE the production DAG file exists.
First run must produce ImportError failures.

Test groups:
    TestDagImport           — T-09a DAG imports cleanly
    TestDagSchedule         — T-09b schedule is None
    TestDagTaskIds          — T-09c exact task-id set
    TestDagDependencies     — T-09d task dependency graph shape
    TestTaskValidateInput   — T-09e _task_validate_input callable behaviour
    TestTaskEditorResult    — T-09f _task_editor_result XCom shape
"""

from __future__ import annotations

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Shared helpers and fixtures
# ---------------------------------------------------------------------------

_VALID_CONF = {
    "domain": "congreso",
    "source_path": "/data/chapter_video.mp4",
    "overlays": [
        {
            "tipo": "extracto_sesion",
            "tiempo_inicio": 0.0,
            "tiempo_fin": 5.0,
            "titulo": "Extracto de sesión",
        }
    ],
}


def _get_dag_mod():
    """Force a fresh import of the DAG module."""
    mod_name = "congress_videos.generic_video_editor_dag"
    if mod_name in sys.modules:
        del sys.modules[mod_name]
    return importlib.import_module(mod_name)


def _make_fake_ti(xcom_map: dict) -> MagicMock:
    """Return a TaskInstance double whose xcom_pull dispatches by task_ids kwarg."""
    ti = MagicMock(name="TaskInstance")
    xcom_store: dict = {}

    def _pull(task_ids=None, key="return_value", **_kw):
        return xcom_map.get(task_ids)

    def _push(key: str, value, **_kw):
        xcom_store[key] = value

    ti.xcom_pull.side_effect = _pull
    ti.xcom_push.side_effect = _push
    ti.xcom_store = xcom_store
    return ti


# ---------------------------------------------------------------------------
# T-09a: DAG imports cleanly
# ---------------------------------------------------------------------------


class TestDagImport:
    """T-09a: The DAG module must import without raising any exception."""

    def test_dag_imports_cleanly(self) -> None:
        """Importing the DAG module must not raise any exception."""
        mod = _get_dag_mod()
        assert mod is not None


# ---------------------------------------------------------------------------
# T-09b: schedule is None
# ---------------------------------------------------------------------------


class TestDagSchedule:
    """T-09b: The DAG must have schedule=None (trigger-only)."""

    def test_schedule_is_none(self) -> None:
        """dag.schedule_interval must be None."""
        mod = _get_dag_mod()
        dag = mod.dag
        assert dag.schedule_interval is None


# ---------------------------------------------------------------------------
# T-09c: Exact task-id set
# ---------------------------------------------------------------------------

_EXPECTED_TASK_IDS = {"validate_input", "apply_overlays", "editor_result"}


class TestDagTaskIds:
    """T-09c: DAG must contain exactly three tasks — no more, no fewer."""

    def test_exact_task_id_set(self) -> None:
        """Task IDs must be exactly {validate_input, apply_overlays, editor_result}."""
        mod = _get_dag_mod()
        dag = mod.dag
        actual_ids = {task.task_id for task in dag.tasks}
        assert actual_ids == _EXPECTED_TASK_IDS, (
            f"Task ID mismatch.\n"
            f"  Extra:   {actual_ids - _EXPECTED_TASK_IDS}\n"
            f"  Missing: {_EXPECTED_TASK_IDS - actual_ids}"
        )


# ---------------------------------------------------------------------------
# T-09d: Task dependency graph
# ---------------------------------------------------------------------------


class TestDagDependencies:
    """T-09d: Verify the task dependency chain."""

    def _upstream_ids(self, dag, task_id: str) -> set[str]:
        task = dag.get_task(task_id)
        return {t.task_id for t in task.upstream_list}

    def test_apply_overlays_upstream_is_validate_input(self) -> None:
        """apply_overlays must have validate_input as its upstream task."""
        mod = _get_dag_mod()
        assert self._upstream_ids(mod.dag, "apply_overlays") == {"validate_input"}

    def test_editor_result_upstream_is_apply_overlays(self) -> None:
        """editor_result must have apply_overlays as its upstream task."""
        mod = _get_dag_mod()
        assert self._upstream_ids(mod.dag, "editor_result") == {"apply_overlays"}

    def test_validate_input_has_no_upstream(self) -> None:
        """validate_input must be the entry task with no upstream dependencies."""
        mod = _get_dag_mod()
        assert self._upstream_ids(mod.dag, "validate_input") == set()


# ---------------------------------------------------------------------------
# T-09e: _task_validate_input callable behaviour
# ---------------------------------------------------------------------------


class TestTaskValidateInput:
    """T-09e: _task_validate_input must read dag_run.conf and call validate_editor_input."""

    def test_valid_conf_returns_conf_dict(self, mocker) -> None:
        """A valid conf must be returned from the task wrapper."""
        mod = _get_dag_mod()
        mocker.patch.object(mod, "validate_editor_input")

        dag_run = MagicMock()
        dag_run.conf = _VALID_CONF.copy()

        result = mod._task_validate_input(dag_run=dag_run)
        assert result == _VALID_CONF

    def test_calls_validate_editor_input(self, mocker) -> None:
        """The task wrapper must call validate_editor_input with the conf dict."""
        mod = _get_dag_mod()
        mock_validate = mocker.patch.object(mod, "validate_editor_input")

        dag_run = MagicMock()
        dag_run.conf = _VALID_CONF.copy()

        mod._task_validate_input(dag_run=dag_run)
        mock_validate.assert_called_once_with(_VALID_CONF)

    def test_empty_conf_defaults_to_empty_dict(self, mocker) -> None:
        """When dag_run.conf is None, an empty dict is used."""
        mod = _get_dag_mod()
        mock_validate = mocker.patch.object(mod, "validate_editor_input")

        dag_run = MagicMock()
        dag_run.conf = None

        # Should not raise — validate_editor_input handles the error
        try:
            mod._task_validate_input(dag_run=dag_run)
        except Exception:
            pass  # validate_editor_input may raise; that's fine

        # Must have been called with an empty dict, not None
        mock_validate.assert_called_once_with({})


# ---------------------------------------------------------------------------
# T-09f: _task_editor_result XCom shape
# ---------------------------------------------------------------------------


class TestTaskEditorResult:
    """T-09f: _task_editor_result must return the correct result shape via XCom."""

    def test_result_shape_contains_success_output_path_domain(self, mocker) -> None:
        """The return dict must contain success, output_path, and domain."""
        mod = _get_dag_mod()

        ti = _make_fake_ti(
            {
                "validate_input": _VALID_CONF,
                "apply_overlays": {"output_path": "/data/out_edited.mp4", "success": True},
            }
        )

        result = mod._task_editor_result(ti=ti)
        assert result["success"] is True
        assert result["output_path"] == "/data/out_edited.mp4"
        assert result["domain"] == "congreso"

    def test_result_output_path_matches_apply_overlays(self, mocker) -> None:
        """output_path in result must come from the apply_overlays XCom."""
        mod = _get_dag_mod()

        ti = _make_fake_ti(
            {
                "validate_input": _VALID_CONF,
                "apply_overlays": {"output_path": "/custom/path/edited.mp4", "success": True},
            }
        )

        result = mod._task_editor_result(ti=ti)
        assert result["output_path"] == "/custom/path/edited.mp4"

    def test_domain_comes_from_validate_input_xcom(self, mocker) -> None:
        """domain in result must come from the validate_input XCom conf."""
        mod = _get_dag_mod()

        conf = {**_VALID_CONF, "domain": "congreso"}
        ti = _make_fake_ti(
            {
                "validate_input": conf,
                "apply_overlays": {"output_path": "/out.mp4", "success": True},
            }
        )

        result = mod._task_editor_result(ti=ti)
        assert result["domain"] == "congreso"


# ---------------------------------------------------------------------------
# T-09g: _REQUIRED_CONF_KEYS module-level constant
# ---------------------------------------------------------------------------


class TestRequiredConfKeys:
    """T-09g: _REQUIRED_CONF_KEYS must expose the expected keys at module level."""

    def test_required_conf_keys_contains_domain_and_overlays(self) -> None:
        """_REQUIRED_CONF_KEYS must include 'domain' and 'overlays'."""
        mod = _get_dag_mod()
        assert "domain" in mod._REQUIRED_CONF_KEYS
        assert "overlays" in mod._REQUIRED_CONF_KEYS

    def test_required_conf_keys_is_tuple_or_sequence(self) -> None:
        """_REQUIRED_CONF_KEYS must be a tuple or other sequence."""
        mod = _get_dag_mod()
        assert hasattr(mod._REQUIRED_CONF_KEYS, "__iter__")
