"""Tests for congress_videos.post_upload_verification_dag.

Covers DAG load hygiene, task graph shape, per-video error isolation,
and the MAX_API_CALLS_PER_RUN cap enforcement.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# DAG load + hygiene
# ---------------------------------------------------------------------------


class TestPostUploadVerificationDagLoads:
    """DAG must load without import errors and appear in DagBag."""

    def test_dag_has_no_import_errors(self):
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "post_upload_verification" not in bag.import_errors

    def test_dag_object_is_defined(self):
        from congress_videos.post_upload_verification_dag import dag

        assert dag is not None

    def test_dag_id(self):
        from congress_videos.post_upload_verification_dag import dag

        assert dag.dag_id == "post_upload_verification"

    def test_schedule_is_hourly(self):
        from congress_videos.post_upload_verification_dag import dag

        assert dag.schedule_interval == "@hourly"

    def test_catchup_is_false(self):
        from congress_videos.post_upload_verification_dag import dag

        assert dag.catchup is False


# ---------------------------------------------------------------------------
# Task graph shape
# ---------------------------------------------------------------------------


class TestPostUploadVerificationDagGraph:
    """Task graph: staleness_guard → select_unverified_uploads → verify_and_record."""

    def test_has_three_tasks(self):
        from congress_videos.post_upload_verification_dag import dag

        assert len(dag.tasks) == 3

    def test_expected_task_ids_present(self):
        from congress_videos.post_upload_verification_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "staleness_guard" in task_ids
        assert "select_unverified_uploads" in task_ids
        assert "verify_and_record" in task_ids

    def test_staleness_guard_upstream_of_select(self):
        from congress_videos.post_upload_verification_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        guard = tasks_by_id["staleness_guard"]
        select = tasks_by_id["select_unverified_uploads"]
        assert select.task_id in {t.task_id for t in guard.downstream_list}

    def test_select_upstream_of_verify(self):
        from congress_videos.post_upload_verification_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        select = tasks_by_id["select_unverified_uploads"]
        verify = tasks_by_id["verify_and_record"]
        assert verify.task_id in {t.task_id for t in select.downstream_list}


# ---------------------------------------------------------------------------
# verify_and_record callable logic
# ---------------------------------------------------------------------------


class TestVerifyAndRecordCallable:
    """Per-video error isolation and API-cap enforcement."""

    def _get_callable(self):
        """Import the verify_and_record callable from the DAG module."""
        from congress_videos.post_upload_verification_dag import _verify_and_record

        return _verify_and_record

    def test_one_bad_video_does_not_abort_others(self, mock_task_instance):
        """A single check_video_status exception must not prevent processing other videos."""
        candidates = [
            {"item_type": "chapter", "id": 1, "youtube_video_id": "bad_vid", "output_path": None},
            {"item_type": "chapter", "id": 2, "youtube_video_id": "good_vid", "output_path": None},
        ]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        def _flaky_check(video_id, *, http_get, youtube_service=None):
            if video_id == "bad_vid":
                raise RuntimeError("network error")
            return ("ok", "oembed_200")

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                side_effect=_flaky_check,
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            # good_vid must still be processed — mark_upload_verified called once
            mock_db.mark_upload_verified.assert_called_once_with("chapter", 2)

    def test_api_cap_stops_data_api_calls_early(self, mock_task_instance):
        """When MAX_API_CALLS_PER_RUN=2, only 2 Data-API calls must be made for non-200 oembed."""
        # 5 candidates that all hit the Data API (oembed non-200)
        candidates = [
            {"item_type": "chapter", "id": i, "youtube_video_id": f"vid{i}", "output_path": None} for i in range(1, 6)
        ]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        api_call_count = {"n": 0}

        def _capped_check(video_id, *, http_get, youtube_service=None):
            # Simulate oembed non-200 + Data API call
            api_call_count["n"] += 1
            return ("ok", "api_ok")

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                side_effect=_capped_check,
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            with patch(
                "congress_videos.post_upload_verification_dag.MAX_API_CALLS_PER_RUN",
                2,
            ):
                callable_fn = self._get_callable()
                callable_fn(ti=mock_task_instance)

            # With cap=2, only 2 check_video_status calls should have been made
            assert api_call_count["n"] == 2

    def test_verified_video_calls_mark_upload_verified(self, mock_task_instance):
        """ok status → mark_upload_verified must be called with correct args."""
        candidates = [{"item_type": "turn", "id": 10, "youtube_video_id": "turnvid", "output_path": "/p/t.mp4"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                return_value=("ok", "oembed_200"),
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            mock_db.mark_upload_verified.assert_called_once_with("turn", "/p/t.mp4")

    def test_abandoned_video_calls_record_failure(self, mock_task_instance):
        """abandoned status → record_upload_verification_failure must be called."""
        candidates = [{"item_type": "chapter", "id": 99, "youtube_video_id": "gonevid", "output_path": None}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                return_value=("abandoned", "empty_items"),
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            mock_db.record_upload_verification_failure.assert_called_once()
            call_args = mock_db.record_upload_verification_failure.call_args[0]
            assert call_args[0] == "chapter"
            assert call_args[1] == 99

    def test_processing_status_makes_no_db_write(self, mock_task_instance):
        """processing status → no DB write must be performed."""
        candidates = [{"item_type": "chapter", "id": 5, "youtube_video_id": "procvid", "output_path": None}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                return_value=("processing", "processingStatus=processing"),
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            mock_db.mark_upload_verified.assert_not_called()
            mock_db.record_upload_verification_failure.assert_not_called()

    def test_empty_candidates_is_noop(self, mock_task_instance):
        """No candidates → no check_video_status calls and no DB writes."""
        mock_task_instance.xcom_pull.return_value = []

        with (
            patch("congress_videos.post_upload_verification_dag.check_video_status") as mock_check,
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            mock_check.assert_not_called()
            mock_db.mark_upload_verified.assert_not_called()

    def test_characterizes_full_summary_dict_for_mixed_batch(self, mock_task_instance):
        """Characterization (pinned before extraction): the exact 5-key summary dict
        for a mixed batch — one verified (no API charge), one abandoned (charged),
        one erroring (unincremented) — including the api_calls_made tally.
        """
        candidates = [
            {"item_type": "chapter", "id": 1, "youtube_video_id": "okvid", "output_path": None},
            {"item_type": "chapter", "id": 2, "youtube_video_id": "abandonedvid", "output_path": None},
            {"item_type": "chapter", "id": 3, "youtube_video_id": "badvid", "output_path": None},
        ]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        def _mixed_check(video_id, *, http_get, youtube_service=None):
            if video_id == "okvid":
                return ("ok", "oembed_200")
            if video_id == "abandonedvid":
                return ("abandoned", "empty_items")
            raise RuntimeError("network error")

        with (
            patch(
                "congress_videos.post_upload_verification_dag.check_video_status",
                side_effect=_mixed_check,
            ),
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db_cls.return_value = MagicMock()

            callable_fn = self._get_callable()
            result = callable_fn(ti=mock_task_instance)

            assert result == {
                "verified": 1,
                "failures": 1,
                "skipped": 0,
                "errors": 1,
                "api_calls_made": 1,
            }

    def test_characterizes_empty_candidates_four_key_dict(self, mock_task_instance):
        """Characterization (pinned before extraction): the empty-candidates path
        keeps its current 4-key dict shape — no api_calls_made key.
        """
        mock_task_instance.xcom_pull.return_value = []

        with (
            patch("congress_videos.post_upload_verification_dag.check_video_status") as mock_check,
            patch("congress_videos.post_upload_verification_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db_cls.return_value = MagicMock()

            callable_fn = self._get_callable()
            result = callable_fn(ti=mock_task_instance)

            assert result == {"verified": 0, "failures": 0, "skipped": 0, "errors": 0}
            mock_check.assert_not_called()
