"""Tests for congress_videos.thumbnail_republish_dag (issue #331).

Covers DAG load hygiene, task graph shape, the staleness guard's 180m
tolerance (DD6), per-candidate error isolation, and the
MAX_THUMBNAIL_CALLS_PER_RUN cap.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# DAG load + hygiene
# ---------------------------------------------------------------------------


class TestThumbnailRepublishDagLoads:
    """DAG must load without import errors and appear in DagBag."""

    def test_dag_has_no_import_errors(self):
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "thumbnail_republish" not in bag.import_errors

    def test_dag_object_is_defined(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag is not None

    def test_dag_id(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.dag_id == "thumbnail_republish"

    def test_schedule_is_daily_at_15_utc(self):
        """DD6: schedule is a literal 15:00 UTC cron, not bare @daily."""
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.schedule_interval == "0 15 * * *"

    def test_catchup_is_false(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert dag.catchup is False

    def test_tags_present(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert "congress" in dag.tags
        assert "youtube" in dag.tags


# ---------------------------------------------------------------------------
# Task graph shape
# ---------------------------------------------------------------------------


class TestThumbnailRepublishDagGraph:
    """Task graph: staleness_guard -> select -> heal."""

    def test_has_three_tasks(self):
        from congress_videos.thumbnail_republish_dag import dag

        assert len(dag.tasks) == 3

    def test_expected_task_ids_present(self):
        from congress_videos.thumbnail_republish_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "staleness_guard" in task_ids
        assert "select_thumbnail_republish_candidates" in task_ids
        assert "heal_thumbnails" in task_ids

    def test_staleness_guard_upstream_of_select(self):
        from congress_videos.thumbnail_republish_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        guard = tasks_by_id["staleness_guard"]
        select = tasks_by_id["select_thumbnail_republish_candidates"]
        assert select.task_id in {t.task_id for t in guard.downstream_list}

    def test_select_upstream_of_heal(self):
        from congress_videos.thumbnail_republish_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        select = tasks_by_id["select_thumbnail_republish_candidates"]
        heal = tasks_by_id["heal_thumbnails"]
        assert heal.task_id in {t.task_id for t in select.downstream_list}


# ---------------------------------------------------------------------------
# _staleness_guard: 180m tolerance (DD6), not the hourly DAG's 30m
# ---------------------------------------------------------------------------


class TestStalenessGuard:
    """Spec: staleness_guard skips replays stale beyond 180m tolerance."""

    def test_fresh_run_returns_true(self):
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        assert _staleness_guard(data_interval_end=datetime.now(UTC)) is True

    def test_no_data_interval_end_returns_true(self):
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        assert _staleness_guard() is True

    def test_within_180m_tolerance_returns_true(self):
        """150 minutes ago is still within the 180m tolerance -- must proceed."""
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        recent = datetime.now(UTC) - timedelta(minutes=150)
        assert _staleness_guard(data_interval_end=recent) is True

    def test_beyond_180m_tolerance_returns_false(self):
        """4 hours ago exceeds the 180m tolerance -- must skip."""
        from congress_videos.thumbnail_republish_dag import _staleness_guard

        stale = datetime.now(UTC) - timedelta(hours=4)
        assert _staleness_guard(data_interval_end=stale) is False


# ---------------------------------------------------------------------------
# _heal_thumbnails callable logic
# ---------------------------------------------------------------------------


class TestHealThumbnailsCallable:
    """Per-candidate error isolation and MAX_THUMBNAIL_CALLS_PER_RUN cap."""

    def _get_callable(self):
        from congress_videos.thumbnail_republish_dag import _heal_thumbnails

        return _heal_thumbnails

    def test_empty_candidates_is_noop(self, mock_task_instance):
        """No candidates -> no service build, no DB writes."""
        mock_task_instance.xcom_pull.return_value = []

        with patch("utils.youtube_helpers.get_authenticated_youtube_service") as mock_get_service:
            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_get_service.assert_not_called()
            assert summary == {
                "healed": 0,
                "retried": 0,
                "abandoned": 0,
                "skipped": 0,
                "errors": 0,
                "calls_made": 0,
            }

    def test_healed_candidate_calls_mark_republished(self, mock_task_instance):
        candidates = [{"output_path": "/p/v1.mp4", "youtube_video_id": "vid1"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                return_value=("healed", "success"),
            ),
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_db.mark_turn_thumbnail_republished.assert_called_once_with("/p/v1.mp4")
            mock_db.record_turn_thumbnail_republish_failure.assert_not_called()
            assert summary["healed"] == 1
            assert summary["calls_made"] == 1

    def test_retry_candidate_records_failure_without_abandon(self, mock_task_instance):
        candidates = [{"output_path": "/p/v2.mp4", "youtube_video_id": "vid2"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                return_value=("retry", "transient error"),
            ),
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_db.record_turn_thumbnail_republish_failure.assert_called_once_with(
                "/p/v2.mp4", "transient error", abandon=False
            )
            assert summary["retried"] == 1

    def test_abandon_candidate_records_failure_with_abandon_true(self, mock_task_instance):
        candidates = [{"output_path": "/p/v3.mp4", "youtube_video_id": "vid3"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                return_value=("abandon", "Thumbnail file not found: /p/thumbnail.png"),
            ),
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_db.record_turn_thumbnail_republish_failure.assert_called_once_with(
                "/p/v3.mp4",
                "Thumbnail file not found: /p/thumbnail.png",
                abandon=True,
            )
            assert summary["abandoned"] == 1

    def test_one_bad_candidate_does_not_abort_others(self, mock_task_instance):
        """A single attempt_thumbnail_republish exception must not prevent the rest."""
        candidates = [
            {"output_path": "/p/bad.mp4", "youtube_video_id": "vidbad"},
            {"output_path": "/p/good.mp4", "youtube_video_id": "vidgood"},
        ]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        def _flaky_attempt(output_path, *, set_thumbnail_fn):
            if output_path == "/p/bad.mp4":
                raise RuntimeError("unexpected crash")
            return ("healed", "success")

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                side_effect=_flaky_attempt,
            ),
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_db.mark_turn_thumbnail_republished.assert_called_once_with("/p/good.mp4")
            assert summary["errors"] == 1
            assert summary["healed"] == 1

    def test_cap_leaves_remainder_for_next_run(self, mock_task_instance):
        """With MAX_THUMBNAIL_CALLS_PER_RUN=2, only 2 candidates get attempted."""
        candidates = [{"output_path": f"/p/v{i}.mp4", "youtube_video_id": f"vid{i}"} for i in range(1, 6)]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                return_value=("healed", "success"),
            ) as mock_attempt,
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            with patch(
                "congress_videos.thumbnail_republish_dag.MAX_THUMBNAIL_CALLS_PER_RUN",
                2,
            ):
                callable_fn = self._get_callable()
                summary = callable_fn(ti=mock_task_instance)

            assert mock_attempt.call_count == 2
            assert summary["calls_made"] == 2
            assert summary["skipped"] == 3

    def test_service_build_failure_skips_all_candidates(self, mock_task_instance):
        """If the YouTube service cannot be built, no candidate is attempted."""
        candidates = [{"output_path": "/p/v1.mp4", "youtube_video_id": "vid1"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                side_effect=FileNotFoundError("token missing"),
            ),
            patch("congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish") as mock_attempt,
        ):
            callable_fn = self._get_callable()
            summary = callable_fn(ti=mock_task_instance)

            mock_attempt.assert_not_called()
            assert summary["skipped"] == 1
            assert summary["calls_made"] == 0

    def test_recorded_error_has_no_token_path(self, mock_task_instance):
        """Threat matrix: the recorded error string never contains a token path."""
        candidates = [{"output_path": "/p/v1.mp4", "youtube_video_id": "vid1"}]
        mock_task_instance.xcom_push(key="candidates", value=candidates)

        detail = "Thumbnail file not found: /p/thumbnail.png"

        with (
            patch(
                "utils.youtube_helpers.get_authenticated_youtube_service",
                return_value=MagicMock(),
            ),
            patch(
                "congress_videos.thumbnail_republish_dag.attempt_thumbnail_republish",
                return_value=("abandon", detail),
            ),
            patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls,
        ):
            mock_db = MagicMock()
            mock_db_cls.return_value = mock_db

            callable_fn = self._get_callable()
            callable_fn(ti=mock_task_instance)

            recorded_error = mock_db.record_turn_thumbnail_republish_failure.call_args[0][1]
            assert "token" not in recorded_error.lower()
            assert recorded_error == detail


# ---------------------------------------------------------------------------
# _run_select_candidates callable
# ---------------------------------------------------------------------------


class TestRunSelectCandidates:
    def test_passes_candidate_limit_and_pushes_xcom(self, mock_task_instance):
        from congress_videos.modules.thumbnail_republish import CANDIDATE_LIMIT
        from congress_videos.thumbnail_republish_dag import _run_select_candidates

        with patch("congress_videos.thumbnail_republish_dag.CongressionalVideoDB") as mock_db_cls:
            mock_db = MagicMock()
            mock_db.select_turns_needing_thumbnail_republish.return_value = [{"output_path": "/p/v1.mp4"}]
            mock_db_cls.return_value = mock_db

            result = _run_select_candidates(ti=mock_task_instance)

            mock_db.select_turns_needing_thumbnail_republish.assert_called_once_with(limit=CANDIDATE_LIMIT)
            assert result == [{"output_path": "/p/v1.mp4"}]
            assert mock_task_instance.xcom_store["candidates"] == [{"output_path": "/p/v1.mp4"}]
