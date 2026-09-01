"""Tests for congress_videos.video_analytics_actions_dag (issue #102).

Spec: Action DAG dispatch and token isolation / Claim-before-act retry
semantics / action_taken vocabulary and audit snapshot / Lifetime action
cap per video.
"""

from __future__ import annotations

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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_checkpoint_view_medians",
            return_value={"48h": {"median_views": 1000, "sample_size": 15}},
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_video_action_history",
            return_value={"vid123": {"thumbnail": 0, "title": 0}},
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ) as mock_mark:
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service"
        ) as mock_youtube_svc:
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            side_effect=fake_claim,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            side_effect=fake_trigger,
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=False,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api"
        ) as mock_trigger:
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=chosen_row,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
            side_effect=fake_mark,
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
        ) as mock_get_service, patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action"
        ) as mock_claim, patch(
            "utils.youtube_helpers.get_authenticated_youtube_service"
        ) as mock_svc:
            _run_apply_actions(ti=mock_task_instance)

        mock_claim.assert_not_called()
        mock_svc.assert_not_called()


class TestApplyActionsFailurePath:
    """Spec: failure path sets action_taken='failed' with error in action_detail."""

    def test_thumbnail_dag_failure_marks_failed_with_error(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [_decision_row()]
        mock_task_instance.run_id = "manual_run_1"

        captured = {}

        def fake_mark(snapshot_id, action, detail):
            captured["action"] = action
            captured["detail"] = detail

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(state="failed"),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
            side_effect=fake_mark,
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
            side_effect=fake_mark,
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": False, "error": "thumbnail size exceeded"},
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
        ), patch(
            "utils.youtube_helpers.update_video_title",
            return_value={"success": True, "error": None},
        ) as mock_update_title:
            _run_apply_actions(ti=mock_task_instance)

        mock_update_title.assert_called_once()

    def test_non_title_checkpoint_does_not_call_update_video_title(self, mock_task_instance):
        from congress_videos.video_analytics_actions_dag import _run_apply_actions

        mock_task_instance.xcom_store["decisions"] = [
            _decision_row(checkpoint="48h", decision="thumbnail_regenerated")
        ]
        mock_task_instance.run_id = "manual_run_1"

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ), patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
        ), patch(
            "utils.youtube_helpers.update_video_title",
        ) as mock_update_title:
            _run_apply_actions(ti=mock_task_instance)

        mock_update_title.assert_not_called()


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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            side_effect=RuntimeError("dag not found"),
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
            side_effect=fake_mark,
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=_thumbnail_dag_run(),
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={"success": True, "chapter_id": 5, "output_path": "", "title": ""},
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken",
            side_effect=fake_mark,
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

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.claim_snapshot_action",
            return_value=True,
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_chosen_thumbnail",
            return_value=None,
        ), patch(
            "congress_videos.video_analytics_actions_dag.trigger_dag_api",
            return_value=dag_run,
        ), patch(
            "congress_videos.video_analytics_actions_dag.time.sleep",
            return_value=None,
        ), patch(
            "airflow.models.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 5,
                "output_path": "/tmp/thumb.png",
                "title": "Nuevo título",
            },
        ), patch(
            "congress_videos.modules.database.CongressionalVideoDB.mark_action_taken"
        ) as mock_mark, patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=MagicMock(),
        ), patch(
            "utils.youtube_helpers.set_thumbnail_for_video",
            return_value={"success": True, "error": None},
        ):
            _run_apply_actions(ti=mock_task_instance)

        assert dag_run.refresh_from_db.call_count == 2
        mock_mark.assert_called_once()
        assert mock_mark.call_args.kwargs["action"] == "thumbnail_regenerated"
