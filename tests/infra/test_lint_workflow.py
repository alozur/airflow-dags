"""Structural guard for .github/workflows/lint.yml (issue #486).

Pure YAML-parse + trigger-shape assertions — no GitHub Actions execution.
Confirms the `pull_request` trigger re-fires on a base-branch retarget
(`gh pr edit --base dev`) without a `branches:` filter blocking coverage for
stacked PRs, that the concurrency group is scoped per activity type to avoid
same-SHA cancellation of the required `ruff check`, and that the structurally
fixed parts of the workflow (job name, push trigger) stay unchanged.
"""

from __future__ import annotations

from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "lint.yml"


class TestLintWorkflowExists:
    def test_workflow_file_exists(self):
        assert WORKFLOW_PATH.exists(), f"Missing workflow: {WORKFLOW_PATH}"

    def test_workflow_parses_cleanly(self):
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        assert "jobs" in config


class TestLintWorkflowTriggers:
    @staticmethod
    def _triggers() -> dict:
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        # PyYAML parses the bare `on:` key as boolean True.
        return config.get("on", config.get(True))

    def test_pull_request_types_cover_edited(self):
        pr = self._triggers()["pull_request"]
        types = pr.get("types")
        assert types is not None
        assert sorted(types) == sorted(["opened", "synchronize", "reopened", "edited"])

    def test_pull_request_has_no_branch_filter(self):
        pr = self._triggers()["pull_request"]
        assert "branches" not in pr
        assert "branches-ignore" not in pr

    def test_push_branches_unchanged(self):
        assert self._triggers()["push"]["branches"] == ["dev", "main"]


class TestLintWorkflowConcurrency:
    def test_concurrency_group_is_per_activity_type(self):
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        assert config["concurrency"]["group"] == "lint-${{ github.ref }}-${{ github.event.action }}"

    def test_cancel_in_progress_enabled(self):
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        assert config["concurrency"]["cancel-in-progress"] is True


class TestLintWorkflowRequiredCheck:
    def test_ruff_job_name_is_the_required_check_name(self):
        config = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
        assert config["jobs"]["ruff"]["name"] == "ruff check"


class TestLintWorkflowDocumentation:
    def test_header_cites_the_retarget_issue(self):
        raw = WORKFLOW_PATH.read_text(encoding="utf-8")
        assert "#486" in raw
