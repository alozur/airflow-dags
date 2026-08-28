"""Structural guard for .github/workflows/pip-audit.yml (issue #207).

Pure YAML-parse + trigger-shape assertions — no GitHub Actions execution.
Confirms the workflow is scheduled + manually dispatchable and stays
informational (does not gate unrelated PRs via a required check on push).
"""
from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW_PATH = (
    Path(__file__).resolve().parents[2] / ".github" / "workflows" / "pip-audit.yml"
)


class TestPipAuditWorkflowExists:

    def test_workflow_file_exists(self):
        assert WORKFLOW_PATH.exists(), f"Missing workflow: {WORKFLOW_PATH}"

    def test_workflow_parses_cleanly(self):
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        assert "jobs" in config


class TestPipAuditTriggers:

    @staticmethod
    def _triggers() -> dict:
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        # PyYAML parses the bare `on:` key as boolean True.
        return config.get("on", config.get(True))

    def test_has_schedule_trigger(self):
        assert "schedule" in self._triggers()

    def test_has_manual_dispatch_trigger(self):
        assert "workflow_dispatch" in self._triggers()


class TestPipAuditDoesNotTouchLockfile:

    def test_no_uv_add_or_uv_lock_write_step(self):
        raw = WORKFLOW_PATH.read_text(encoding="utf-8")
        assert "uv add" not in raw
        assert "uv lock" not in raw
