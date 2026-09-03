"""Tests for congress_videos.video_analytics_actions_dag (issue #102).

Spec: Action DAG dispatch and token isolation / Claim-before-act retry
semantics / action_taken vocabulary and audit snapshot / Lifetime action
cap per video.
"""

from __future__ import annotations

from contextlib import ExitStack
from datetime import UTC, datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# DAG shape (8.1)
# ---------------------------------------------------------------------------


class TestVideoAnalyticsActionsDagLoads:
    def test_dag_is_importable(self):
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "video_analytics_actions" not in bag.import_errors

    def test_dag_id(self):
        from congress_videos.video_analytics_actions_dag import dag

        assert dag.dag_id == "video_analytics_actions"

    def test_schedule_is_none(self):
        from congress_videos.video_analytics_actions_dag import dag

        schedule = getattr(dag, "schedule_interval", None) or getattr(dag, "schedule", None)
        assert schedule is None

    def test_max_active_runs_is_one(self):
        from congress_videos.video_analytics_actions_dag import dag

        assert dag.max_active_runs == 1

    def test_has_exactly_four_tasks(self):
        from congress_videos.video_analytics_actions_dag import dag

        assert len(dag.tasks) == 4

    def test_expected_task_ids_present(self):
        from congress_videos.video_analytics_actions_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert task_ids == {
            "select_candidates",
            "evaluate_candidates",
            "record_no_ops",
            "apply_actions",
        }

    def test_task_chain_order(self):
        from congress_videos.video_analytics_actions_dag import dag

        select = dag.get_task("select_candidates")
        evaluate = dag.get_task("evaluate_candidates")
        record = dag.get_task("record_no_ops")
        apply_ = dag.get_task("apply_actions")

        assert evaluate in select.downstream_list
        assert record in evaluate.downstream_list
        assert apply_ in record.downstream_list

    def test_apply_actions_has_zero_retries(self):
        """Spec: Claim-before-act retry semantics — apply_actions MUST run
        with retries: 0 (claim-before-act makes Airflow-level retry unsafe)."""
        from congress_videos.video_analytics_actions_dag import dag

        apply_task = dag.get_task("apply_actions")
        assert apply_task.retries == 0


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _decision_row(
    snapshot_id=1,
    chapter_id=5,
    youtube_video_id="vid123",
    checkpoint="48h",
    decision="thumbnail_regenerated",
    views=100,
    median_views=1000,
    sample_size=15,
):
    return {
        "snapshot_id": snapshot_id,
        "chapter_id": chapter_id,
        "youtube_video_id": youtube_video_id,
        "checkpoint": checkpoint,
        "metrics": {"views": views},
        "chapter_title": "Título",
        "description": "Desc",
        "session_number": 1,
        "session_date": None,
        "key_speakers": [],
        "resolved_participant_slug": None,
        "decision": decision,
        "views": views,
        "median_views": median_views,
        "sample_size": sample_size,
    }


def _thumbnail_dag_run(state="success"):
    dag_run = MagicMock(name="thumbnail_dag_run")
    dag_run.run_id = "child_run_1"
    dag_run.state = state
    dag_run.refresh_from_db.return_value = None
    return dag_run


# ---------------------------------------------------------------------------
# select_candidates / evaluate_candidates / record_no_ops
# ---------------------------------------------------------------------------


class TestSelectCandidates:
    def test_pushes_db_result_to_candidates_xcom(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_select_candidates

        rows = [_decision_row()]
        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_unactioned_snapshots",
            return_value=rows,
        ):
            result = _run_select_candidates(ti=mock_task_instance)

        assert result == rows
        assert mock_task_instance.xcom_store["candidates"] == rows


class TestEvaluateCandidates:
    def test_no_candidates_pushes_empty_decisions(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_evaluate_candidates

        mock_task_instance.xcom_store["candidates"] = []
        result = _run_evaluate_candidates(ti=mock_task_instance)

        assert result == []
        assert mock_task_instance.xcom_store["decisions"] == []

    def test_decisions_use_evaluate_action_and_history(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_evaluate_candidates

        candidate = _decision_row(decision=None)
        del candidate["decision"]
        del candidate["views"]
        del candidate["median_views"]
        del candidate["sample_size"]
        mock_task_instance.xcom_store["candidates"] = [candidate]

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_checkpoint_view_medians",
                return_value={"48h": {"median_views": 1000, "sample_size": 15}},
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_video_action_history",
                return_value={"vid123": {"thumbnail": 0, "title": 0}},
            ),
        ):
            result = _run_evaluate_candidates(ti=mock_task_instance)

        assert len(result) == 1
        assert result[0]["decision"] == "thumbnail_regenerated"  # 100 << 50% of 1000


class TestRecordNoOps:
    def test_no_op_decisions_call_mark_action_taken(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_record_no_ops

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(snapshot_id=1, decision="ok"),
            _decision_row(snapshot_id=2, decision="cold_start"),
            _decision_row(snapshot_id=3, decision="capped"),
            _decision_row(snapshot_id=4, decision="thumbnail_regenerated"),
        ]

        with patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken") as mock_mark:
            _run_record_no_ops(ti=mock_task_instance)

        marked_ids = {c.kwargs.get("snapshot_id", c.args[0] if c.args else None) for c in mock_mark.call_args_list}
        assert mock_mark.call_count == 3
        for sid in (1, 2, 3):
            assert sid in marked_ids
        assert 4 not in marked_ids

    def test_no_op_rows_never_call_youtube(self, mock_task_instance):
        """Spec: no-op rows never call YouTube."""
        from congress_videos.video_analytics_actions_dag import _run_record_no_ops

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(snapshot_id=1, decision="ok"),
        ]

        with (
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"),
            patch("utils.youtube_helpers.get_authenticated_youtube_service") as mock_youtube_svc,
        ):
            _run_record_no_ops(ti=mock_task_instance)

        mock_youtube_svc.assert_not_called()


# ---------------------------------------------------------------------------
# apply_actions (8.2 - 8.5)
# ---------------------------------------------------------------------------


class TestApplyActionsClaimBeforeAct:
    """Spec: Claim-before-act retry semantics."""

    def test_claims_row_before_any_external_call(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        call_order = []

        def fake_claim(snapshot_id):
            call_order.append("claim")
            return True

        def fake_trigger(dag_id, conf, run_id):
            call_order.append("trigger")
            return _thumbnail_dag_run()

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                side_effect=fake_claim,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                side_effect=fake_trigger,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert call_order[0] == "claim"
        assert "trigger" in call_order
        assert call_order.index("claim") < call_order.index("trigger")

    def test_concurrent_claim_rejected_row_skipped(self, mock_task_instance):
        """A row that fails to claim (already in_progress) is skipped —
        no trigger, no external call."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=False,
            ),
            patch("congress_videos.video_analytics_actions_dag.trigger_dag_api") as mock_trigger,
        ):
            _run_apply_actions(ti=mock_task_instance)

        mock_trigger.assert_not_called()


class TestApplyActionsPriorSnapshotBeforeTrigger:
    """Spec: Snapshot before trigger."""

    def test_snapshots_prior_archetype_and_title_into_action_detail(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        chosen_row = {
            "archetype": "denuncia",
            "openai_title": "Título anterior",
            "local_path": "/opt/prior.png",
        }

        captured_detail = {}

        def fake_mark(snapshot_id, action, detail):
            captured_detail.update(detail)

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=chosen_row,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured_detail["prior"]["archetype"] == "denuncia"
        assert captured_detail["prior"]["title"] == "Título anterior"


class TestApplyActionsTokenIsolation:
    """Spec: Action DAG dispatch and token isolation."""

    def test_uses_upload_purpose_token_never_analytics(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
            ) as mock_get_service,
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
        ):
            _run_apply_actions(ti=mock_task_instance)

        mock_get_service.assert_called_once()
        token_file_arg = mock_get_service.call_args.args[0]
        assert "upload" in token_file_arg or "analytics" not in token_file_arg


class TestApplyActionsNoOpNeverCallsYoutube:
    """Spec: no-op rows never call YouTube (apply_actions only processes
    regenerate decisions; no-ops are handled entirely by record_no_ops)."""

    def test_apply_actions_ignores_no_op_decisions(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(snapshot_id=1, decision="ok"),
            _decision_row(snapshot_id=2, decision="capped"),
            _decision_row(snapshot_id=3, decision="cold_start"),
        ]

        with (
            patch("congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action") as mock_claim,
            patch("utils.youtube_helpers.get_authenticated_youtube_service") as mock_svc,
        ):
            _run_apply_actions(ti=mock_task_instance)

        mock_claim.assert_not_called()
        mock_svc.assert_not_called()


class TestApplyActionsFailurePath:
    """Spec: failure path sets action_taken='failed' with error in
    action_detail, AND (issue #311, D1/D5) the gate raises exactly once
    after every row is recorded — the row is durably 'failed' in the DB
    before the exception ever reaches Airflow."""

    def test_thumbnail_dag_failure_marks_failed_with_error(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(state="failed"),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert captured["detail"].get("error")

    def test_youtube_publish_failure_marks_failed_with_error(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": False, "error": "thumbnail size exceeded"},
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert "thumbnail size exceeded" in captured["detail"].get("error", "")

    def test_title_checkpoint_calls_update_video_title(self, mock_task_instance):
        """24h regeneration must also call update_video_title after the
        thumbnail publish succeeds."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(checkpoint="24h", decision="thumbnail_and_title_regenerated")
        ]
        mock_task_instance.run_id = "manual_run_1"

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
            patch(
                "utils.youtube_helpers.update_video_title",
                return_value={"success": True, "error": None},
            ) as mock_update_title,
        ):
            _run_apply_actions(ti=mock_task_instance)

        mock_update_title.assert_called_once()

    def test_non_title_checkpoint_does_not_call_update_video_title(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row(checkpoint="48h", decision="thumbnail_regenerated")]
        mock_task_instance.run_id = "manual_run_1"

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"),
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
            patch(
                "utils.youtube_helpers.update_video_title",
            ) as mock_update_title,
        ):
            _run_apply_actions(ti=mock_task_instance)

        mock_update_title.assert_not_called()


def _patched_apply(
    *,
    set_thumbnail_result=None,
    set_thumbnail_side_effect=None,
    update_title_result=None,
    update_title_side_effect=None,
):
    """ExitStack of the standard apply_actions success-path patch set, with
    the thumbnail/title publish results overridable per test (D-1..D-3)."""
    stack = ExitStack()
    stack.enter_context(
        patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        )
    )
    stack.enter_context(
        patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        )
    )
    stack.enter_context(
        patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        )
    )
    stack.enter_context(patch("congress_videos.video_analytics_actions_dag.time.sleep", return_value=None))
    stack.enter_context(
        patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        )
    )
    stack.enter_context(patch("utils.youtube_helpers.get_authenticated_youtube_service", return_value=MagicMock()))
    if set_thumbnail_side_effect is not None:
        stack.enter_context(
            patch("utils.youtube_helpers.set_thumbnail_for_video", side_effect=set_thumbnail_side_effect)
        )
    else:
        stack.enter_context(
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value=set_thumbnail_result or {"success": True, "error": None},
            )
        )
    if update_title_side_effect is not None:
        stack.enter_context(patch("utils.youtube_helpers.update_video_title", side_effect=update_title_side_effect))
    else:
        stack.enter_context(
            patch(
                "utils.youtube_helpers.update_video_title",
                return_value=update_title_result or {"success": True, "error": None},
            )
        )
    return stack


class TestApplyActionsAppliedField:
    """Issue #317 (D1/D4/D5): per-row `applied` quad-value + per-row isolation.
    Issue #311 (D1/D5): a "failed" row still raises the gate exactly once,
    AFTER every row is recorded."""

    def test_title_failure_records_applied_thumbnail_true_title_false(self, mock_task_instance):
        """D-1: ordinary title publish failure -> failed, applied ==
        {"thumbnail": True, "title": False}."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(checkpoint="24h", decision="thumbnail_and_title_regenerated")
        ]
        mock_task_instance.run_id = "manual_run_1"
        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            _patched_apply(update_title_result={"success": False, "error": "quotaExceeded"}),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert captured["detail"]["applied"] == {"thumbnail": True, "title": False}

    def test_blank_title_guard_raise_is_recorded_not_escaped(self, mock_task_instance):
        """D-2: update_video_title raises ValueError (blank guard) -> recorded
        failed with applied.title is False. The ValueError itself never
        escapes _apply_one_action's per-row isolation; the DIFFERENT,
        deliberate exception raised by the #311 gate below it is what
        reaches Airflow, naming this row, only after mark_action_taken ran."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(checkpoint="24h", decision="thumbnail_and_title_regenerated")
        ]
        mock_task_instance.run_id = "manual_run_1"
        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            _patched_apply(
                update_title_side_effect=ValueError("update_video_title: refusing to publish a blank title")
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert captured["detail"]["applied"]["title"] is False
        assert "blank title refused" in captured["detail"]["error"]

    def test_one_row_guard_failure_does_not_abort_remaining_rows(self, mock_task_instance):
        """D-3 + issue #311 D1: 2 rows, row 1's title guard raises, row 2
        succeeds -> BOTH mark_action_taken calls happen (loop integrity,
        every row claimed and recorded) BEFORE the gate raises once at the
        end naming only the failed row."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(snapshot_id=1, checkpoint="24h", decision="thumbnail_and_title_regenerated"),
            _decision_row(snapshot_id=2, checkpoint="24h", decision="thumbnail_and_title_regenerated"),
        ]
        mock_task_instance.run_id = "manual_run_1"
        marked = []

        def fake_mark(snapshot_id, action, detail):
            marked.append((snapshot_id, action))

        call_count = {"n": 0}

        def _title_side_effect(*_args, **_kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise ValueError("blank title")
            return {"success": True, "error": None}

        with (
            _patched_apply(update_title_side_effect=_title_side_effect),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="snapshot_id=1"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        # Pins D1: both rows were claimed and mark_action_taken-recorded
        # BEFORE the exception below propagated — the raise happens only
        # after the full results list comprehension finishes.
        assert len(marked) == 2
        assert {a for _, a in marked} == {"failed", "thumbnail_and_title_regenerated"}

    def test_trigger_failure_applied_both_none(self, mock_task_instance):
        """D-6: trigger DAG exception -> early return, applied ==
        {"thumbnail": None, "title": None} (pins init-at-build)."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"
        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["detail"] = detail

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                side_effect=RuntimeError("dag not found"),
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["detail"]["applied"] == {"thumbnail": None, "title": None}


def _permanent_thumbnail_failure(video_id: str) -> dict:
    return {
        "success": False,
        "error": f"Thumbnail upload failed: forbidden for {video_id}",
        "permanent": True,
        "status": 403,
        "reason": "forbidden",
        "domain": "youtube.thumbnail",
        "location": "videoId",
        "message": "The caller does not have permission.",
    }


class TestApplyActionsFailLoudGate:
    """Integration (issue #311, B8.1): the gate raises exactly once, naming
    every failed row, only after every claimed row is recorded. All-success
    batches staying green is proven implicitly by the many other passing
    success-path tests elsewhere in this file — none of them are wrapped in
    pytest.raises, so a spurious raise there would already fail loudly."""

    def test_mixed_batch_raises_naming_only_failed_snapshots(self, mock_task_instance):
        """Covers both required B8.1 scenarios in one pass: a success row
        mixed with failures is never named, and multiple permanent failures
        (snapshot 1 and 3) join into a SINGLE sentence, not one each."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(snapshot_id=1, youtube_video_id="vid-forbidden-1"),
            _decision_row(snapshot_id=2, youtube_video_id="vid-ok"),
            _decision_row(snapshot_id=3, youtube_video_id="vid-forbidden-3"),
        ]
        mock_task_instance.run_id = "manual_run_1"

        def fake_set_thumbnail(youtube, video_id, path):
            if video_id == "vid-ok":
                return {"success": True, "error": None}
            return _permanent_thumbnail_failure(video_id)

        with (
            _patched_apply(set_thumbnail_side_effect=fake_set_thumbnail),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken") as mock_mark,
            pytest.raises(Exception) as exc_info,
        ):
            _run_apply_actions(ti=mock_task_instance)

        message = str(exc_info.value)
        assert message.count("PERMANENTLY") == 1
        assert "snapshot_id=1" in message
        assert "snapshot_id=3" in message
        assert "snapshot_id=2" not in message
        # Pins D1: all 3 rows still got mark_action_taken before the raise.
        assert mock_mark.call_count == 3


class TestActionFailureProblems:
    """Spec: apply_actions fails loud with every finding — pure-dict tests,
    no TI double (issue #311, D5)."""

    def _failed_row(self, snapshot_id=1, **failure_overrides):
        failure = {"stage": "youtube_thumbnail", "permanent": True, "status": 403, "reason": "forbidden"}
        failure.update(failure_overrides)
        return {"snapshot_id": snapshot_id, "action": "failed", "failure": failure}

    def _success_row(self, snapshot_id=1, action="thumbnail_regenerated"):
        return {"snapshot_id": snapshot_id, "action": action, "failure": None}

    def test_empty_and_non_failure_rows_produce_no_problems(self):
        from congress_videos.video_analytics_actions_dag import _action_failure_problems

        assert _action_failure_problems([]) == []
        assert (
            _action_failure_problems([self._success_row(1), self._success_row(2, "thumbnail_and_title_regenerated")])
            == []
        )
        assert _action_failure_problems([{"snapshot_id": 1, "action": "skipped_already_claimed"}]) == []

    def test_permanent_failure_sentence_excludes_transient_word(self):
        from congress_videos.video_analytics_actions_dag import _action_failure_problems

        problems = _action_failure_problems([self._failed_row(1)])

        assert len(problems) == 1
        assert "PERMANENTLY" in problems[0]
        assert "transient" not in problems[0]
        assert "snapshot_id=1" in problems[0]
        assert "stage=youtube_thumbnail" in problems[0]
        assert "status=403" in problems[0]
        assert "reason=forbidden" in problems[0]

    @pytest.mark.parametrize(
        "overrides",
        [
            {"permanent": False, "status": 503, "reason": "backendError"},
            {"permanent": None, "status": None, "reason": None},
        ],
        ids=["transient", "permanent_none"],
    )
    def test_transient_or_unknown_failure_produces_only_second_sentence(self, overrides):
        from congress_videos.video_analytics_actions_dag import _action_failure_problems

        problems = _action_failure_problems([self._failed_row(1, **overrides)])

        assert len(problems) == 1
        assert "transiently or unclassified" in problems[0]
        assert f"status={overrides['status']}" in problems[0]

    def test_permanent_and_transient_together_produce_two_sentences(self):
        from congress_videos.video_analytics_actions_dag import _action_failure_problems

        rows = [
            self._failed_row(1),
            self._failed_row(2, permanent=False, status=503, reason="backendError"),
        ]
        problems = _action_failure_problems(rows)

        assert len(problems) == 2
        assert "PERMANENTLY" in problems[0]
        assert "transiently or unclassified" in problems[1]

    def test_missing_failure_key_on_failed_row_still_reported_unclassified(self):
        from congress_videos.video_analytics_actions_dag import _action_failure_problems

        row = {"snapshot_id": 9, "action": "failed"}
        problems = _action_failure_problems([row])

        assert len(problems) == 1
        assert "transiently or unclassified" in problems[0]
        assert "snapshot_id=9" in problems[0]


class TestApplyActionsAdditionalFailureBranches:
    """Triangulation: trigger exception and invalid-result branches."""

    def test_trigger_dag_api_exception_marks_failed(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                side_effect=RuntimeError("dag not found"),
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert "dag not found" in captured["detail"]["error"]

    def test_invalid_thumbnail_result_marks_failed(self, mock_task_instance):
        """thumbnail_result XCom missing output_path -> failed, not a crash."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=_thumbnail_dag_run(),
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={"success": True, "chapter_id": 5, "output_path": "", "title": ""},
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
                side_effect=fake_mark,
            ),
            pytest.raises(Exception, match="failed"),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert captured["action"] == "failed"
        assert "no valid result" in captured["detail"]["error"]

    def test_still_pending_polls_again_then_succeeds(self, mock_task_instance):
        """dag_run.state stays 'running' for one poll cycle before settling —
        exercises the poll-loop 'continue' branch."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        dag_run = _thumbnail_dag_run(state="running")
        states = iter(["running", "success"])

        def fake_refresh():
            dag_run.state = next(states)

        dag_run.refresh_from_db.side_effect = fake_refresh

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                return_value=True,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                return_value=None,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                return_value=dag_run,
            ),
            patch(
                "congress_videos.video_analytics_actions_dag.time.sleep",
                return_value=None,
            ),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/thumb.png",
                    "title": "Nuevo título",
                },
            ),
            patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken") as mock_mark,
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "utils.youtube_helpers.set_thumbnail_for_video",
                return_value={"success": True, "error": None},
            ),
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert dag_run.refresh_from_db.call_count == 2
        mock_mark.assert_called_once()
        assert mock_mark.call_args.kwargs["action"] == "thumbnail_regenerated"


# ---------------------------------------------------------------------------
# Poll-loop progress visibility (issue #311, D7)
# ---------------------------------------------------------------------------


class TestPollThumbnailDagRunProgress:
    """A ~30-minute bounded poll loop that logs nothing is indistinguishable
    from a hung task while it is happening. These pin that the loop announces
    itself on entry and reports progress periodically, so an operator can tell
    "waiting" from "stuck" without waiting for the timeout."""

    def test_entry_line_names_run_and_snapshot(self, caplog):
        from congress_videos.video_analytics_actions_dag import _poll_thumbnail_dag_run

        dag_run = _thumbnail_dag_run(state="success")

        with (
            caplog.at_level("INFO"),
            patch("congress_videos.video_analytics_actions_dag.time.sleep", return_value=None),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/t.png",
                    "title": "x",
                },
            ),
        ):
            _poll_thumbnail_dag_run(dag_run, snapshot_id=42, checkpoint="7d", snapshot_age_days=13)

        entry = [r.message for r in caplog.records if "polling thumbnail DAG run" in r.message]
        assert len(entry) == 1
        assert "child_run_1" in entry[0]
        assert "snapshot_id=42" in entry[0]
        assert "checkpoint=7d" in entry[0]
        # The first production run applied 13-day-old measurements; staleness
        # of the decision's input belongs in the log next to the wait.
        assert "13" in entry[0]

    def test_progress_line_every_sixth_poll_with_elapsed_and_state(self, caplog):
        from congress_videos.video_analytics_actions_dag import (
            _POLL_PROGRESS_EVERY,
            _THUMBNAIL_POLL_INTERVAL_SECONDS,
            _poll_thumbnail_dag_run,
        )

        dag_run = _thumbnail_dag_run(state="running")
        # Stay pending for two full progress cycles, then settle.
        states = iter(["running"] * (_POLL_PROGRESS_EVERY * 2) + ["success"])
        dag_run.refresh_from_db.side_effect = lambda: setattr(dag_run, "state", next(states))

        with (
            caplog.at_level("INFO"),
            patch("congress_videos.video_analytics_actions_dag.time.sleep", return_value=None),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/t.png",
                    "title": "x",
                },
            ),
        ):
            _poll_thumbnail_dag_run(dag_run, snapshot_id=42)

        progress = [r.message for r in caplog.records if "still waiting" in r.message]
        assert len(progress) == 2, progress
        assert "state=running" in progress[0]
        # Elapsed must be real seconds, not a poll count.
        first_elapsed = _POLL_PROGRESS_EVERY * _THUMBNAIL_POLL_INTERVAL_SECONDS
        assert f"elapsed={first_elapsed}s" in progress[0]
        assert f"elapsed={first_elapsed * 2}s" in progress[1]

    def test_quiet_when_run_settles_before_first_progress_cycle(self, caplog):
        """No progress spam on the common fast path."""
        from congress_videos.video_analytics_actions_dag import _poll_thumbnail_dag_run

        dag_run = _thumbnail_dag_run(state="success")

        with (
            caplog.at_level("INFO"),
            patch("congress_videos.video_analytics_actions_dag.time.sleep", return_value=None),
            patch(
                "airflow.models.XCom.get_one",
                return_value={
                    "success": True,
                    "chapter_id": 5,
                    "output_path": "/tmp/t.png",
                    "title": "x",
                },
            ),
        ):
            _poll_thumbnail_dag_run(dag_run, snapshot_id=42)

        assert not [r for r in caplog.records if "still waiting" in r.message]


class TestSnapshotAgeDays:
    """`collected_at` is newly projected; rows built before it — including every
    hand-built test fixture — must degrade to None rather than crash."""

    def test_none_collected_at_returns_none(self):
        from congress_videos.video_analytics_actions_dag import _snapshot_age_days

        assert _snapshot_age_days(None) is None

    def test_unusable_value_returns_none_instead_of_raising(self):
        from congress_videos.video_analytics_actions_dag import _snapshot_age_days

        assert _snapshot_age_days("not-a-datetime") is None

    @pytest.mark.parametrize("tzinfo", [None, UTC])
    def test_age_in_whole_days_for_naive_and_aware(self, tzinfo):
        from congress_videos.video_analytics_actions_dag import _snapshot_age_days

        now = datetime.now(tzinfo)
        assert _snapshot_age_days(now - timedelta(days=13, hours=2)) == 13


class TestApplyActionsPreviousBriefForwarded:
    """Spec: previous_brief steering (migration 043, #292) — apply_actions
    forwards the chosen row's persisted art_direction_brief verbatim as
    child_conf["previous_brief"] when it is a non-empty dict, and omits the
    key otherwise. Forwarding is independent of the title checkpoint."""

    def _run_with_chosen(self, mock_task_instance, chosen_row, checkpoint="48h"):
        """Drive _run_apply_actions through the standard 8-patch stack and
        return the mock used for trigger_dag_api, so callers can inspect
        call_args.kwargs["conf"]."""
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row(checkpoint=checkpoint)]
        mock_task_instance.run_id = "manual_run_1"

        with ExitStack() as stack:
            stack.enter_context(
                patch(
                    "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
                    return_value=True,
                )
            )
            stack.enter_context(
                patch(
                    "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
                    return_value=chosen_row,
                )
            )
            mock_trigger = stack.enter_context(
                patch(
                    "congress_videos.video_analytics_actions_dag.trigger_dag_api",
                    return_value=_thumbnail_dag_run(),
                )
            )
            stack.enter_context(
                patch(
                    "congress_videos.video_analytics_actions_dag.time.sleep",
                    return_value=None,
                )
            )
            stack.enter_context(
                patch(
                    "airflow.models.XCom.get_one",
                    return_value={
                        "success": True,
                        "chapter_id": 5,
                        "output_path": "/tmp/thumb.png",
                        "title": "Nuevo título",
                    },
                )
            )
            stack.enter_context(patch("congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"))
            stack.enter_context(
                patch(
                    "utils.youtube_helpers.get_authenticated_youtube_service",
                    return_value=MagicMock(),
                )
            )
            stack.enter_context(
                patch(
                    "utils.youtube_helpers.set_thumbnail_for_video",
                    return_value={"success": True, "error": None},
                )
            )
            _run_apply_actions(ti=mock_task_instance)

        return mock_trigger

    def test_brief_dict_present_is_forwarded_verbatim(self, mock_task_instance):
        brief = {"text": "PENSIÓN", "background": "hemiciclo"}
        chosen_row = {"art_direction_brief": brief}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row)

        conf = mock_trigger.call_args.kwargs["conf"]
        assert conf["previous_brief"] == brief

    def test_brief_none_key_is_omitted(self, mock_task_instance):
        chosen_row = {"art_direction_brief": None}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row)

        conf = mock_trigger.call_args.kwargs["conf"]
        assert "previous_brief" not in conf

    def test_brief_empty_dict_is_omitted(self, mock_task_instance):
        chosen_row = {"art_direction_brief": {}}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row)

        conf = mock_trigger.call_args.kwargs["conf"]
        assert "previous_brief" not in conf

    def test_brief_legacy_string_is_omitted(self, mock_task_instance):
        """Historical pre-migration-043 rows never had a real dict brief;
        a stray non-dict value must never leak into child_conf."""
        chosen_row = {"art_direction_brief": "legacy string"}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row)

        conf = mock_trigger.call_args.kwargs["conf"]
        assert "previous_brief" not in conf

    def test_previous_archetype_still_forwarded_alongside_brief(self, mock_task_instance):
        """Regression: adding previous_brief must not remove the existing
        previous_archetype steering."""
        brief = {"text": "brief"}
        chosen_row = {"art_direction_brief": brief, "archetype": "denuncia"}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row)

        conf = mock_trigger.call_args.kwargs["conf"]
        assert conf["previous_brief"] == brief
        assert conf["previous_archetype"] == "denuncia"

    def test_forwarded_at_non_title_checkpoint(self, mock_task_instance):
        """Pins checkpoint-independence: the brief forwards even at 48h,
        which is not a title-update checkpoint."""
        brief = {"text": "brief at 48h"}
        chosen_row = {"art_direction_brief": brief}

        mock_trigger = self._run_with_chosen(mock_task_instance, chosen_row, checkpoint="48h")

        conf = mock_trigger.call_args.kwargs["conf"]
        assert conf["previous_brief"] == brief
