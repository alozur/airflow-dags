"""Tests for congress_videos.video_analytics_dag.

Spec: DAG Shape and Scheduling / No Public YouTube Writes.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import pytest

# ---------------------------------------------------------------------------
# DAG load + structure
# ---------------------------------------------------------------------------


class TestVideoAnalyticsDagLoads:
    """DAG must load without errors and appear in DagBag."""

    def test_dag_is_importable(self):
        """DagBag must have zero import errors for video_analytics_dag."""
        from airflow.models import DagBag

        bag = DagBag(include_examples=False)
        assert "video_analytics" not in bag.import_errors

    def test_dag_object_is_defined(self):
        """The module must expose a 'dag' object."""
        from congress_videos.video_analytics_dag import dag

        assert dag is not None

    def test_dag_id(self):
        """dag_id must be 'video_analytics'."""
        from congress_videos.video_analytics_dag import dag

        assert dag.dag_id == "video_analytics"

    def test_schedule_is_hourly(self):
        """schedule_interval must be '@hourly'."""
        from congress_videos.video_analytics_dag import dag

        assert dag.schedule_interval == "@hourly"

    def test_catchup_is_false(self):
        """catchup must be False to avoid backfill storms."""
        from congress_videos.video_analytics_dag import dag

        assert dag.catchup is False


# ---------------------------------------------------------------------------
# Task graph shape
# ---------------------------------------------------------------------------


class TestVideoAnalyticsDagGraph:
    """Task graph must have staleness guard → pending select → fetch → record."""

    def test_has_five_tasks(self):
        """DAG must have exactly 5 tasks: t0, t1, t2, t3, t4 (issue #102 adds
        the terminal trigger_action_dag task)."""
        from congress_videos.video_analytics_dag import dag

        assert len(dag.tasks) == 5

    def test_expected_task_ids_present(self):
        """Required task IDs must be present in the DAG."""
        from congress_videos.video_analytics_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "staleness_guard" in task_ids
        assert "get_pending_checkpoints" in task_ids
        assert "fetch_analytics" in task_ids
        assert "record_snapshots" in task_ids

    def test_staleness_guard_is_upstream_of_get_pending(self):
        """staleness_guard → get_pending_checkpoints."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t0 = tasks_by_id["staleness_guard"]
        t1 = tasks_by_id["get_pending_checkpoints"]

        downstream_ids = {t.task_id for t in t0.downstream_list}
        assert t1.task_id in downstream_ids

    def test_get_pending_is_upstream_of_fetch(self):
        """get_pending_checkpoints → fetch_analytics."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t1 = tasks_by_id["get_pending_checkpoints"]
        t2 = tasks_by_id["fetch_analytics"]

        downstream_ids = {t.task_id for t in t1.downstream_list}
        assert t2.task_id in downstream_ids

    def test_fetch_is_upstream_of_record(self):
        """fetch_analytics → record_snapshots."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t2 = tasks_by_id["fetch_analytics"]
        t3 = tasks_by_id["record_snapshots"]

        downstream_ids = {t.task_id for t in t2.downstream_list}
        assert t3.task_id in downstream_ids


# ---------------------------------------------------------------------------
# _fetch_analytics wiring: collected pairs from DB
# ---------------------------------------------------------------------------


class TestFetchAnalyticsUsesCollectedPairs:
    """_fetch_analytics must query the DB for already-collected pairs and pass
    them to pending_checkpoints() instead of using an always-empty set.

    This saves Analytics API quota by skipping pairs we already have.
    """

    def test_fetch_analytics_calls_get_collected_pairs(self, monkeypatch):
        """GIVEN candidate rows with youtube_video_ids
        WHEN _fetch_analytics executes
        THEN get_collected_analytics_pairs is called with those ids."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "abc123",
                "youtube_upload_date": None,
            }
        ]

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        get_collected_calls = []

        def fake_get_collected(ids):
            get_collected_calls.append(ids)
            return {("abc123", "24h")}  # pretend 24h already collected

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                side_effect=fake_get_collected,
            ),
            patch(
                "congress_videos.modules.video_analytics.pending_checkpoints",
                return_value=[],
            ) as mock_pending,
        ):
            _fetch_analytics(ti=mock_ti)

        # get_collected_analytics_pairs must have been called
        assert len(get_collected_calls) == 1
        assert "abc123" in get_collected_calls[0]

    def test_fetch_analytics_passes_collected_set_to_pending_checkpoints(self, monkeypatch):
        """The collected set from DB must be forwarded as the 'collected'
        argument to pending_checkpoints(), NOT an empty set."""
        from unittest.mock import MagicMock, call, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 2,
                "youtube_video_id": "xyz999",
                "youtube_upload_date": None,
            }
        ]
        already_collected = {("xyz999", "24h"), ("xyz999", "48h")}

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        captured_pending_calls = []

        def fake_pending(now, videos, collected):
            captured_pending_calls.append(collected)
            return []

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=already_collected,
            ),
            patch(
                "congress_videos.modules.video_analytics.pending_checkpoints",
                side_effect=fake_pending,
            ),
        ):
            _fetch_analytics(ti=mock_ti)

        # pending_checkpoints must have received the real collected set, not set()
        assert len(captured_pending_calls) == 1
        assert captured_pending_calls[0] == already_collected


# ---------------------------------------------------------------------------
# _fetch_analytics API-call loop coverage (issue #185)
#
# Spec: Analytics collector test coverage — happy path, API-error path,
# missing youtube_video_id, DAG-level idempotency.
# ---------------------------------------------------------------------------


def _make_fake_service(response: dict):
    """Build a fake Analytics service whose reports().query().execute()
    returns the given response dict."""
    from unittest.mock import MagicMock

    fake_service = MagicMock(name="analytics_service")
    fake_service.reports.return_value.query.return_value.execute.return_value = response
    return fake_service


def _full_response(values: dict) -> dict:
    """Build a real-shaped Analytics API response covering every
    METRIC_FIELDS column, using `values` for named overrides (default 0)."""
    from congress_videos.config.analytics_config import METRIC_FIELDS

    row = [values.get(field, 0) for field in METRIC_FIELDS]
    return {
        "columnHeaders": [{"name": name} for name in METRIC_FIELDS],
        "rows": [row],
    }


class TestFetchAnalyticsHappyPath:
    """Spec: Analytics collector test coverage / Happy path."""

    def test_real_shaped_response_lands_in_collected_xcom(self):
        """GIVEN a pending checkpoint candidate with a valid youtube_video_id
        WHEN _fetch_analytics calls service.reports().query().execute() and
             receives a real-shaped response
        THEN the parsed metrics land in the 'collected' XCom key."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 7,
                "youtube_video_id": "happy123",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            }
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        response = _full_response({"views": 500, "likes": 40})
        fake_service = _make_fake_service(response)

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=set(),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        assert len(result) == 1
        item = result[0]
        assert item["chapter_id"] == 7
        assert item["youtube_video_id"] == "happy123"
        assert item["metrics"]["views"] == 500
        assert item["metrics"]["likes"] == 40
        mock_ti.xcom_push.assert_any_call(key="collected", value=result)


class TestFetchAnalyticsApiErrorPath:
    """Spec: Analytics collector test coverage / Quota-exceeded-API error path."""

    def test_execute_raises_item_skipped_loop_continues(self):
        """GIVEN the Analytics API raises on .execute() for one candidate but
             succeeds for another
        WHEN _fetch_analytics processes both
        THEN the failing item is skipped (no partial XCom entry) and the loop
             continues to collect the succeeding item without crashing."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "fails111",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            },
            {
                "chapter_id": 2,
                "youtube_video_id": "ok222",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            },
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        good_response = _full_response({"views": 10})
        fake_service = MagicMock(name="analytics_service")

        def fake_execute():
            filters = fake_service.reports.return_value.query.call_args.kwargs.get("filters", "")
            if "fails111" in filters:
                raise RuntimeError("quotaExceeded")
            return good_response

        fake_service.reports.return_value.query.return_value.execute.side_effect = fake_execute

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=set(),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        yt_ids = {item["youtube_video_id"] for item in result}
        assert "ok222" in yt_ids
        assert "fails111" not in yt_ids
        assert len(result) == 1


class TestFetchAnalyticsMissingVideoId:
    """Spec: Analytics collector test coverage / Missing video id."""

    def test_missing_youtube_video_id_excluded_from_api_call_and_collected(self):
        """GIVEN a candidate row with a missing/None youtube_video_id alongside
             a valid one
        WHEN _fetch_analytics runs (real pending_checkpoints, not mocked)
        THEN the row without an id is never sent to the Analytics API and is
             excluded from 'collected'."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": None,
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            },
            {
                "chapter_id": 2,
                "youtube_video_id": "valid456",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            },
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        response = _full_response({"views": 25})
        fake_service = _make_fake_service(response)

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=set(),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        # Only one (video, checkpoint) API call — the None-id row never reaches it.
        assert fake_service.reports.return_value.query.call_count == 1
        assert len(result) == 1
        assert result[0]["youtube_video_id"] == "valid456"


class TestFetchAnalyticsDagLevelIdempotency:
    """Spec: Analytics collector test coverage / DAG-level idempotency."""

    def test_two_full_runs_do_not_duplicate_a_recorded_checkpoint(self, mock_task_instance):
        """GIVEN a checkpoint already recorded in video_analytics_snapshots
        WHEN the DAG-callable path (_fetch_analytics -> _run_record_snapshots)
             runs twice for the same checkpoint
        THEN no duplicate row is written — ON CONFLICT DO NOTHING holds
             end-to-end, not just at the DB-method level."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import (
            _fetch_analytics,
            _run_record_snapshots,
        )

        candidate_rows = [
            {
                "chapter_id": 9,
                "youtube_video_id": "idem789",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            }
        ]

        # In-memory store simulating the UNIQUE(youtube_video_id, checkpoint)
        # constraint + ON CONFLICT DO NOTHING semantics.
        store: dict[tuple[str, str], dict] = {}

        def fake_get_collected(youtube_video_ids):
            return {key for key in store if key[0] in youtube_video_ids}

        def fake_record(chapter_id, youtube_video_id, checkpoint, metrics):
            store.setdefault((youtube_video_id, checkpoint), metrics)

        response = _full_response({"views": 100})
        fake_service = _make_fake_service(response)

        mock_task_instance.xcom_store["candidates"] = candidate_rows

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                side_effect=fake_get_collected,
            ),
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.record_analytics_snapshot",
                side_effect=fake_record,
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            # Run 1: nothing collected yet -> fetch + record one snapshot.
            _fetch_analytics(ti=mock_task_instance)
            _run_record_snapshots(ti=mock_task_instance)

            assert len(store) == 1

            # Run 2: same checkpoint, now already collected -> no re-fetch,
            # no duplicate record.
            _fetch_analytics(ti=mock_task_instance)
            _run_record_snapshots(ti=mock_task_instance)

        assert len(store) == 1
        assert store[("idem789", "24h")]["views"] == 100


# ---------------------------------------------------------------------------
# Remaining coverage gaps: staleness_guard, empty-candidates short-circuit,
# get_collected_analytics_pairs failure, Analytics service build failure,
# should_persist skip-and-retry branch, _run_get_pending_checkpoints.
# ---------------------------------------------------------------------------


class TestStalenessGuard:
    """Spec: staleness_guard skips stale data_interval_end replays."""

    def test_fresh_run_returns_true(self):
        """GIVEN data_interval_end is now (fresh)
        WHEN _staleness_guard runs
        THEN it returns True (proceed)."""
        from congress_videos.video_analytics_dag import _staleness_guard

        assert _staleness_guard(data_interval_end=datetime.now(UTC)) is True

    def test_no_data_interval_end_returns_true(self):
        """GIVEN no data_interval_end in context
        WHEN _staleness_guard runs
        THEN it returns True (proceed)."""
        from congress_videos.video_analytics_dag import _staleness_guard

        assert _staleness_guard() is True

    def test_stale_run_returns_false(self):
        """GIVEN data_interval_end is far in the past (beyond tolerance)
        WHEN _staleness_guard runs
        THEN it returns False (skip)."""
        from congress_videos.video_analytics_dag import _staleness_guard

        stale = datetime.now(UTC) - timedelta(hours=6)
        assert _staleness_guard(data_interval_end=stale) is False


class TestFetchAnalyticsNoCandidates:
    """Spec: empty-candidates short-circuit."""

    def test_no_candidate_rows_returns_empty_and_pushes_empty_xcom(self):
        """GIVEN no candidate rows on the 'candidates' XCom
        WHEN _fetch_analytics runs
        THEN it returns [] and pushes an empty 'collected' XCom without
             calling the Analytics API."""
        from unittest.mock import MagicMock

        from congress_videos.video_analytics_dag import _fetch_analytics

        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = []

        result = _fetch_analytics(ti=mock_ti)

        assert result == []
        mock_ti.xcom_push.assert_any_call(key="collected", value=[])


class TestFetchAnalyticsCollectedPairsFailure:
    """Spec: DB read for already-collected pairs degrades gracefully."""

    def test_get_collected_pairs_raises_proceeds_with_empty_set(self):
        """GIVEN get_collected_analytics_pairs raises
        WHEN _fetch_analytics runs
        THEN it logs a warning and proceeds as if nothing was collected yet
             (idempotency stays DB-enforced via ON CONFLICT DO NOTHING)."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "dbfail1",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            }
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        response = _full_response({"views": 5})
        fake_service = _make_fake_service(response)

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                side_effect=RuntimeError("connection refused"),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        assert len(result) == 1
        assert result[0]["youtube_video_id"] == "dbfail1"


class TestFetchAnalyticsServiceBuildFailure:
    """Spec: Analytics service construction failure degrades gracefully."""

    def test_service_build_raises_returns_empty_collected(self):
        """GIVEN get_youtube_analytics_service raises
        WHEN _fetch_analytics runs
        THEN it logs a warning, pushes an empty 'collected' XCom, and returns
             [] without crashing the task."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "svcfail1",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            }
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=set(),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                side_effect=RuntimeError("token missing"),
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        assert result == []
        mock_ti.xcom_push.assert_any_call(key="collected", value=[])


class TestFetchAnalyticsSkipAndRetry:
    """Spec: should_persist() False branch — all-None/all-zero metrics."""

    def test_all_zero_metrics_skipped_not_collected(self):
        """GIVEN the Analytics API returns all-zero metrics for a candidate
        WHEN _fetch_analytics runs
        THEN the pair is skipped (skip-and-retry) and excluded from
             'collected', leaving it pending for the next hourly run."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _fetch_analytics

        candidate_rows = [
            {
                "chapter_id": 1,
                "youtube_video_id": "zeroed1",
                "youtube_upload_date": datetime.now(UTC) - timedelta(hours=30),
            }
        ]
        mock_ti = MagicMock()
        mock_ti.xcom_pull.return_value = candidate_rows

        all_zero_response = _full_response({})
        fake_service = _make_fake_service(all_zero_response)

        with (
            patch(
                "congress_videos.modules.database.CongressionalVideoDB.get_collected_analytics_pairs",
                return_value=set(),
            ),
            patch(
                "utils.youtube_helpers.get_youtube_analytics_service",
                return_value=fake_service,
            ),
        ):
            result = _fetch_analytics(ti=mock_ti)

        assert result == []


class TestRunGetPendingCheckpoints:
    """Spec: _run_get_pending_checkpoints callable pushes 'candidates' XCom."""

    def test_pushes_db_result_to_candidates_xcom(self):
        """GIVEN CongressionalVideoDB.get_pending_analytics_checkpoints returns rows
        WHEN _run_get_pending_checkpoints runs
        THEN it pushes those rows to the 'candidates' XCom key and returns them."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _run_get_pending_checkpoints

        db_rows = [{"chapter_id": 3, "youtube_video_id": "cand3", "youtube_upload_date": None}]
        mock_ti = MagicMock()

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_pending_analytics_checkpoints",
            return_value=db_rows,
        ):
            result = _run_get_pending_checkpoints(ti=mock_ti)

        assert result == db_rows
        mock_ti.xcom_push.assert_any_call(key="candidates", value=db_rows)

    def test_pushed_candidates_survive_xcom_round_trip(self):
        """Bug regression (issue #303): a non-UTC fixed-offset
        youtube_upload_date must survive Airflow's REAL XCom serializer
        round-trip. Before the fix this raises
        ValueError: ZoneInfo keys must be normalized relative paths, got:"""
        import json
        from unittest.mock import MagicMock, patch

        from airflow.utils.json import XComDecoder, XComEncoder

        from congress_videos.video_analytics_dag import _run_get_pending_checkpoints

        def _xcom_round_trip(value):
            return json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)

        db_rows = [
            {
                "chapter_id": 42,
                "youtube_video_id": "abc123",
                "youtube_upload_date": datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2))),
            }
        ]
        mock_ti = MagicMock()

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_pending_analytics_checkpoints",
            return_value=db_rows,
        ):
            _run_get_pending_checkpoints(ti=mock_ti)

        pushed_value = mock_ti.xcom_push.call_args.kwargs["value"]
        result = _xcom_round_trip(pushed_value)

        pushed_date = result[0]["youtube_upload_date"]
        assert isinstance(pushed_date, datetime)
        assert pushed_date.utcoffset() == timedelta(0)
        assert pushed_date == db_rows[0]["youtube_upload_date"]

    def test_pushed_value_is_the_returned_object(self):
        """The explicit ti.xcom_push(key='candidates', ...) and the implicit
        return_value auto-push must share the SAME normalized object."""
        from unittest.mock import MagicMock, patch

        from congress_videos.video_analytics_dag import _run_get_pending_checkpoints

        db_rows = [{"chapter_id": 3, "youtube_video_id": "cand3", "youtube_upload_date": None}]
        mock_ti = MagicMock()

        with patch(
            "congress_videos.modules.database.CongressionalVideoDB.get_pending_analytics_checkpoints",
            return_value=db_rows,
        ):
            result = _run_get_pending_checkpoints(ti=mock_ti)

        assert mock_ti.xcom_push.call_args.kwargs["value"] is result


# ---------------------------------------------------------------------------
# Terminal trigger to video_analytics_actions (issue #102)
#
# Spec: video-analytics capability / Terminal trigger to action DAG.
# ---------------------------------------------------------------------------


class TestTerminalTriggerToActionDag:
    """record_snapshots -> TriggerDagRunOperator(trigger_dag_id='video_analytics_actions')."""

    def test_dag_has_five_tasks(self):
        """A terminal trigger task is appended: 4 existing + 1 new = 5."""
        from congress_videos.video_analytics_dag import dag

        assert len(dag.tasks) == 5

    def test_trigger_action_dag_task_present(self):
        from congress_videos.video_analytics_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "trigger_action_dag" in task_ids

    def test_trigger_task_is_trigger_dag_run_operator(self):
        from airflow.operators.trigger_dagrun import TriggerDagRunOperator

        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        trigger_task = tasks_by_id["trigger_action_dag"]
        assert isinstance(trigger_task, TriggerDagRunOperator)

    def test_trigger_task_targets_video_analytics_actions(self):
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        trigger_task = tasks_by_id["trigger_action_dag"]
        assert trigger_task.trigger_dag_id == "video_analytics_actions"

    def test_trigger_task_does_not_wait_for_completion(self):
        """Fire-and-forget: the hourly collector must not block on the
        action DAG's (potentially long) regeneration work."""
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        trigger_task = tasks_by_id["trigger_action_dag"]
        assert trigger_task.wait_for_completion is False

    def test_record_snapshots_is_upstream_of_trigger(self):
        from congress_videos.video_analytics_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t3 = tasks_by_id["record_snapshots"]
        trigger_task = tasks_by_id["trigger_action_dag"]

        downstream_ids = {t.task_id for t in t3.downstream_list}
        assert trigger_task.task_id in downstream_ids

    def test_docstring_states_writes_occur_only_in_child_dag(self):
        """The collector docstring must no longer claim no writes anywhere
        downstream — writes now occur in the triggered child DAG."""
        import congress_videos.video_analytics_dag as dag_mod

        docstring = dag_mod.__doc__ or ""
        assert "child" in docstring.lower() or "video_analytics_actions" in docstring
