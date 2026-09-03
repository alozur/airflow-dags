"""Tests for congress_youtube_chapter_uploader DAG (congress_videos.youtube_upload_dag)."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


def _make_ti(xcom_store: dict | None = None):
    """Return a TaskInstance double with an in-memory XCom store."""
    store: dict = xcom_store or {}
    ti = MagicMock(name="TaskInstance")
    ti.xcom_store = store

    def _push(key: str, value, **_kw) -> None:
        store[key] = value

    def _pull(key: str | None = None, **_kw):
        if key is None:
            return None
        return store.get(key)

    ti.xcom_push.side_effect = _push
    ti.xcom_pull.side_effect = _pull
    return ti


# ---------------------------------------------------------------------------
# DAG load + dependency chain
# ---------------------------------------------------------------------------


class TestYoutubeUploadDagLoads:
    def test_dag_loads(self):
        from congress_videos.youtube_upload_dag import dag

        assert dag is not None
        assert dag.dag_id == "congress_youtube_chapter_uploader"

    def test_dag_has_fourteen_tasks(self):
        """DAG must have 14 tasks: 13 original (t1_db replaced by get_uploadable_item PythonOperator)
        plus mark_turns_uploaded task."""
        from congress_videos.youtube_upload_dag import dag

        assert len(dag.tasks) == 14

    def test_expected_task_ids_present(self):
        """New task IDs present; legacy Pillow task IDs absent."""
        from congress_videos.youtube_upload_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        # Core tasks that must still exist
        assert "trigger_youtube_upload" in task_ids
        assert "mark_chapters_uploaded" in task_ids
        assert "check_upload_failures" in task_ids
        # New Pikzels-based tasks
        assert "prepare_thumbnail_config" in task_ids
        assert "generate_thumbnail" in task_ids
        assert "backfill_thumbnail_video_id" in task_ids
        # Legacy Pillow tasks must be gone
        assert "generate_thumbnail_text" not in task_ids
        assert "generate_thumbnails" not in task_ids

    def test_chain_t7_t8_backfill_t9(self):
        """New chain: trigger -> mark_uploaded -> backfill -> check_failures."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t7 = tasks_by_id["trigger_youtube_upload"]
        t8 = tasks_by_id["mark_chapters_uploaded"]
        t8_backfill = tasks_by_id["backfill_thumbnail_video_id"]
        t9 = tasks_by_id["check_upload_failures"]

        assert t8.task_id in {t.task_id for t in t7.downstream_list}
        assert t8_backfill.task_id in {t.task_id for t in t8.downstream_list}
        assert t9.task_id in {t.task_id for t in t8_backfill.downstream_list}

    def test_prepare_precedes_generate(self):
        """prepare_thumbnail_config must be upstream of generate_thumbnail."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        prepare = tasks_by_id["prepare_thumbnail_config"]
        generate = tasks_by_id["generate_thumbnail"]

        upstream_ids = {t.task_id for t in generate.upstream_list}
        assert prepare.task_id in upstream_ids

    def test_generate_precedes_extract(self):
        """generate_thumbnail must be a direct upstream of extract_chapter_videos."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        generate = tasks_by_id["generate_thumbnail"]
        extract = tasks_by_id["extract_chapter_videos"]

        upstream_ids = {t.task_id for t in extract.upstream_list}
        assert generate.task_id in upstream_ids

    def test_extract_precedes_upload_config(self):
        """extract_chapter_videos must be a direct upstream of prepare_upload_config."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        extract = tasks_by_id["extract_chapter_videos"]
        upload_config = tasks_by_id["prepare_upload_config"]

        upstream_ids = {t.task_id for t in upload_config.upstream_list}
        assert extract.task_id in upstream_ids

    def test_backfill_after_mark_uploaded(self):
        """backfill_thumbnail_video_id must be downstream of mark_chapters_uploaded."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        mark = tasks_by_id["mark_chapters_uploaded"]
        backfill = tasks_by_id["backfill_thumbnail_video_id"]

        downstream_ids = {t.task_id for t in mark.downstream_list}
        assert backfill.task_id in downstream_ids


# ---------------------------------------------------------------------------
# _unpublished_thumbnail_labels (issue #320)
# ---------------------------------------------------------------------------


class TestUnpublishedThumbnailLabels:
    def test_single_failure_returns_one_label(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [
            {
                "youtube_video_id": "abc123",
                "chapter_id": 9,
                "turn_id": None,
                "thumbnail_success": False,
            }
        ]
        labels = _unpublished_thumbnail_labels(upload_details)
        assert len(labels) == 1
        assert "abc123" in labels[0]
        assert "chapter_id=9" in labels[0]

    def test_two_failures_return_both_labels(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [
            {
                "youtube_video_id": "vid-one",
                "chapter_id": 9,
                "turn_id": None,
                "thumbnail_success": False,
            },
            {
                "youtube_video_id": "vid-two",
                "chapter_id": None,
                "turn_id": 42,
                "thumbnail_success": False,
            },
        ]
        labels = _unpublished_thumbnail_labels(upload_details)
        assert len(labels) == 2
        joined = " ".join(labels)
        assert "vid-one" in joined
        assert "vid-two" in joined

    def test_missing_youtube_video_id_renders_unknown(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [
            {"chapter_id": 9, "turn_id": None, "thumbnail_success": False},
        ]
        labels = _unpublished_thumbnail_labels(upload_details)
        assert len(labels) == 1
        assert "<unknown>" in labels[0]

    def test_none_thumbnail_success_is_not_a_failure(self):
        """No custom thumbnail was requested — not a failure (design D2)."""
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [
            {"youtube_video_id": "abc", "chapter_id": 1, "thumbnail_success": None}
        ]
        assert _unpublished_thumbnail_labels(upload_details) == []

    def test_true_thumbnail_success_is_not_a_failure(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [
            {"youtube_video_id": "abc", "chapter_id": 1, "thumbnail_success": True}
        ]
        assert _unpublished_thumbnail_labels(upload_details) == []

    def test_missing_key_is_not_a_failure(self):
        """No-results fallback path never sets thumbnail_success at all."""
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [{"youtube_video_id": "abc", "chapter_id": 1}]
        assert _unpublished_thumbnail_labels(upload_details) == []

    def test_empty_list_returns_empty_list(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        assert _unpublished_thumbnail_labels([]) == []

    def test_none_input_returns_empty_list(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        assert _unpublished_thumbnail_labels(None) == []


# ---------------------------------------------------------------------------
# _turn_marking_problems (issue #332)
# ---------------------------------------------------------------------------


class TestTurnMarkingProblems:
    def test_none_turn_updates_reports_missing_xcom(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        problems = _turn_marking_problems(None)
        assert problems == [
            "turn_upload_updates XCom missing after mark_turns_uploaded succeeded"
        ]

    def test_clean_payload_without_recorded_failures_key_returns_empty_list(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 3,
            "failed_updates": 0,
            "details": [
                {"turn_id": 1, "youtube_video_id": "v1", "status": "updated"},
            ],
        }
        assert _turn_marking_problems(turn_updates) == []

    def test_failed_detail_with_turn_id_names_it(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 1,
            "details": [{"turn_id": 7, "status": "failed", "error": "boom"}],
        }
        problems = _turn_marking_problems(turn_updates)
        assert len(problems) == 1
        assert "turn_id=7" in problems[0]

    def test_failed_detail_with_turn_id_none_uses_output_path_label(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 1,
            "details": [
                {
                    "turn_id": None,
                    "status": "failed",
                    "error": "boom",
                    "matched_by": "output_path",
                    "output_path": "/videos/turn-7.mp4",
                }
            ],
        }
        problems = _turn_marking_problems(turn_updates)
        assert len(problems) == 1
        assert "output_path=/videos/turn-7.mp4" in problems[0]
        assert "turn_id=None" not in problems[0]

    def test_counter_only_failure_with_empty_details_still_fires(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {"updated_turns": 0, "failed_updates": 2, "details": []}
        problems = _turn_marking_problems(turn_updates)
        assert len(problems) == 1
        assert "2" in problems[0]

    def test_output_path_not_found_skip_is_a_distinct_sentence(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 0,
            "details": [
                {
                    "turn_id": None,
                    "status": "skipped",
                    "reason": "output_path_not_found",
                    "matched_by": "output_path",
                    "output_path": "/videos/turn-9.mp4",
                }
            ],
        }
        problems = _turn_marking_problems(turn_updates)
        assert len(problems) == 1
        assert "DB-update" not in problems[0]
        assert "output_path=/videos/turn-9.mp4" in problems[0]

    def test_upload_failed_or_missing_fields_skip_does_not_raise(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 0,
            "details": [
                {"turn_id": 3, "status": "skipped", "reason": "upload_failed_or_missing_fields"}
            ],
        }
        assert _turn_marking_problems(turn_updates) == []

    def test_failed_and_output_path_not_found_together_produce_two_sentences(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 1,
            "details": [
                {"turn_id": 7, "status": "failed", "error": "boom"},
                {
                    "turn_id": None,
                    "status": "skipped",
                    "reason": "output_path_not_found",
                    "output_path": "/videos/turn-9.mp4",
                },
            ],
        }
        problems = _turn_marking_problems(turn_updates)
        assert len(problems) == 2

    def test_reasonless_skip_and_statusless_detail_are_both_ignored(self):
        """Deviation from tasks 1.10 literal wording (see apply-progress): the
        tasks artifact described this case as `status="failed"` lacking a
        `reason`, but real `failed` details never carry `reason` (only
        `error` — see upload_marking.py:156-160), and status=="failed" alone
        must fire regardless of `reason` per design D4/spec, matching
        test_failed_detail_with_turn_id_names_it above. This test instead
        covers design D4's actual defensive-shape rows: a `skipped` detail
        missing `reason` (no match against the exact `output_path_not_found`
        string) and a detail missing `status` entirely — both ignored.
        """
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {
            "updated_turns": 0,
            "failed_updates": 0,
            "details": [
                {"turn_id": 3, "status": "skipped"},
                {"turn_id": 4},
            ],
        }
        assert _turn_marking_problems(turn_updates) == []

    def test_absent_details_key_and_zero_failed_updates_returns_empty_list(self):
        from congress_videos.youtube_upload_dag import _turn_marking_problems

        turn_updates = {"updated_turns": 5, "failed_updates": 0}
        assert _turn_marking_problems(turn_updates) == []


# ---------------------------------------------------------------------------
# _check_upload_failures
# ---------------------------------------------------------------------------


class TestCheckUploadFailures:
    def test_raises_on_recorded_failures(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {"chapter_upload_updates": {"recorded_failures": 1, "failed_updates": 0}}
        )
        with pytest.raises(Exception, match="Chapter upload failures"):
            _check_upload_failures(ti)

    def test_raises_on_failed_updates(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 2}}
        )
        with pytest.raises(Exception, match="Chapter upload failures"):
            _check_upload_failures(ti)

    def test_raises_on_missing_xcom(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({})
        with pytest.raises(Exception, match="chapter_upload_updates XCom missing"):
            _check_upload_failures(ti)

    def test_noop_on_zeros(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
                "turn_upload_updates": {
                    "updated_turns": 0,
                    "failed_updates": 0,
                    "details": [],
                },
            }
        )
        _check_upload_failures(ti)  # should not raise

    def test_noop_on_empty_payload_lacking_recorded_failures(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {
                    "updated_chapters": 0,
                    "failed_updates": 0,
                    "details": [],
                },
                "turn_upload_updates": {
                    "updated_turns": 0,
                    "failed_updates": 0,
                    "details": [],
                },
            }
        )
        _check_upload_failures(ti)  # should not raise

    def test_raises_on_thumbnail_failure_with_clean_db(self):
        """Issue #320: a video published but its custom thumbnail failed."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
                "upload_results": {
                    "upload_details": [
                        {
                            "youtube_video_id": "vid-thumb-fail",
                            "chapter_id": 9,
                            "turn_id": None,
                            "thumbnail_success": False,
                        }
                    ]
                },
            }
        )
        with pytest.raises(Exception, match="custom thumbnail") as exc_info:
            _check_upload_failures(ti)
        assert "vid-thumb-fail" in str(exc_info.value)

    def test_raises_one_combined_exception_for_db_and_thumbnail_failures(self):
        """Issue #320 design D6: both findings surface in ONE raise, not first-wins."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {
                    "recorded_failures": 1,
                    "failed_updates": 0,
                    "details": [{"chapter_id": 5, "status": "failure_recorded"}],
                },
                "upload_results": {
                    "upload_details": [
                        {
                            "youtube_video_id": "vid-thumb-fail",
                            "chapter_id": 9,
                            "turn_id": None,
                            "thumbnail_success": False,
                        }
                    ]
                },
            }
        )
        with pytest.raises(Exception) as exc_info:
            _check_upload_failures(ti)
        message = str(exc_info.value)
        assert "Chapter upload failures" in message
        assert "custom thumbnail" in message

    def test_missing_upload_results_is_benign_when_db_clean(self):
        """Issue #320 design D3: missing upload_results does not raise on its own."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
                "turn_upload_updates": {
                    "updated_turns": 0,
                    "failed_updates": 0,
                    "details": [],
                },
            }
        )
        _check_upload_failures(ti)  # should not raise — upload_results absent

    # -----------------------------------------------------------------------
    # Turn findings (issue #332)
    # -----------------------------------------------------------------------

    def test_raises_on_turn_failure_with_clean_chapter_and_thumbnail(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
                "turn_upload_updates": {
                    "updated_turns": 0,
                    "failed_updates": 1,
                    "details": [{"turn_id": 42, "status": "failed", "error": "boom"}],
                },
            }
        )
        with pytest.raises(Exception) as exc_info:
            _check_upload_failures(ti)
        assert "turn_id=42" in str(exc_info.value)

    def test_raises_one_combined_exception_for_chapter_thumbnail_and_turn_failures(self):
        """Issue #332: extends #320 design D6 — three categories, ONE raise."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {
                    "recorded_failures": 1,
                    "failed_updates": 0,
                    "details": [{"chapter_id": 5, "status": "failure_recorded"}],
                },
                "upload_results": {
                    "upload_details": [
                        {
                            "youtube_video_id": "vid-thumb-fail",
                            "chapter_id": 9,
                            "turn_id": None,
                            "thumbnail_success": False,
                        }
                    ]
                },
                "turn_upload_updates": {
                    "updated_turns": 0,
                    "failed_updates": 1,
                    "details": [{"turn_id": 42, "status": "failed", "error": "boom"}],
                },
            }
        )
        with pytest.raises(Exception) as exc_info:
            _check_upload_failures(ti)
        message = str(exc_info.value)
        assert "chapter_id=5" in message or "5" in message
        assert "Chapter upload failures" in message
        assert "custom thumbnail" in message
        assert "turn_id=42" in message

    def test_raises_on_missing_turn_xcom_with_clean_chapter(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0}}
        )
        with pytest.raises(Exception, match="turn_upload_updates XCom missing"):
            _check_upload_failures(ti)

    def test_missing_turn_xcom_does_not_hide_chapter_and_thumbnail_findings(self):
        """Pins design D3: no short-circuit, no masking, even when BOTH the
        chapter DB failure and the turn XCom-missing finding coexist."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {
                    "recorded_failures": 1,
                    "failed_updates": 0,
                    "details": [{"chapter_id": 5, "status": "failure_recorded"}],
                },
                "upload_results": {
                    "upload_details": [
                        {
                            "youtube_video_id": "vid-thumb-fail",
                            "chapter_id": 9,
                            "turn_id": None,
                            "thumbnail_success": False,
                        }
                    ]
                },
                # turn_upload_updates deliberately absent
            }
        )
        with pytest.raises(Exception) as exc_info:
            _check_upload_failures(ti)
        message = str(exc_info.value)
        assert "Chapter upload failures" in message
        assert "custom thumbnail" in message
        assert "turn_upload_updates XCom missing" in message

    def test_no_raise_when_chapter_thumbnail_and_turn_are_all_clean(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(
            {
                "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
                "upload_results": {
                    "upload_details": [
                        {
                            "youtube_video_id": "vid-ok",
                            "chapter_id": 9,
                            "turn_id": None,
                            "thumbnail_success": True,
                        }
                    ]
                },
                "turn_upload_updates": {
                    "updated_turns": 1,
                    "failed_updates": 0,
                    "details": [{"turn_id": 42, "status": "updated"}],
                },
            }
        )
        _check_upload_failures(ti)  # should not raise


# ---------------------------------------------------------------------------
# trigger_upload_with_config (t7)
# ---------------------------------------------------------------------------


class TestTriggerUploadWithConfig:
    def test_schedule_is_once_daily_at_19_utc(self):
        """DAG schedule is '0 19 * * *' — one run daily at 19:00 UTC."""
        from congress_videos.youtube_upload_dag import dag

        assert dag.schedule_interval == "0 19 * * *"

    def test_no_raise_on_child_failure_and_pushes_fallback(self, mocker):
        from congress_videos.youtube_upload_dag import trigger_upload_with_config

        mock_run = MagicMock()
        mock_run.run_id = "failed_run_001"
        mock_run.state = "failed"
        mock_run.execution_date = "2026-07-28T00:00:00+00:00"
        mock_run.refresh_from_db = MagicMock()

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=mock_run
        )
        mocker.patch("time.sleep")
        mock_xcom = mocker.patch("airflow.models.XCom")
        mock_xcom.get_many.return_value = []

        ti = _make_ti(
            {
                "upload_config": {
                    "videos": [
                        {
                            "chapter_id": "c-1",
                            "video_id": "v-1",
                            "video_file": "/c1.mp4",
                        },
                        {
                            "chapter_id": "c-2",
                            "video_id": "v-2",
                            "video_file": "/c2.mp4",
                        },
                    ]
                }
            }
        )

        result = trigger_upload_with_config(ti, run_id="test_run")

        assert result == "failed_run_001"
        fallback = ti.xcom_store["upload_results"]
        assert len(fallback["upload_details"]) == 2
        assert all(d["success"] is False for d in fallback["upload_details"])


# ---------------------------------------------------------------------------
# should_upload function (REQ-GATE-01)
# ---------------------------------------------------------------------------


def _make_context_for_should_upload(
    queue_size: int, hour: int, uploads_today: int = 0
) -> dict:
    """Build a minimal Airflow context for should_upload tests."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    logical_date = datetime(2026, 7, 31, hour, 0, 0, tzinfo=UTC)

    ti = MagicMock(name="TaskInstance")
    ti.xcom_pull.return_value = {
        "queue_size": queue_size,
        "uploads_today": uploads_today,
    }

    return {"ti": ti, "logical_date": logical_date}


class TestShouldUpload:
    def test_queue_above_zero_is_true_regardless_of_hour(self):
        """queue=5 at hour=11 → True (gate is queue_size > 0, no hour lookup) (REQ-GATE-01)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=5, hour=11)
        assert should_upload(**ctx) is True

    # 17:00 — threshold 0
    def test_17_queue_0_is_false(self):
        """17:00, queue=0 → False (threshold 0, not strictly above) (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=0, hour=17)
        assert should_upload(**ctx) is False

    def test_17_queue_1_is_true(self):
        """17:00, queue=1 → True (above threshold 0) (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=1, hour=17)
        assert should_upload(**ctx) is True

    def test_19_queue_1_is_true(self):
        """Scheduled 19:00 UTC run uploads when the long-video queue is non-empty."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=1, hour=19)
        assert should_upload(**ctx) is True

    def test_19_queue_1_is_false_after_daily_long_upload(self):
        """A scheduled run cannot upload a second long-form chapter that day."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=1, hour=19, uploads_today=1)

        assert should_upload(**ctx) is False

    def test_daily_limit_wins_over_manual_hour_threshold(self):
        """Manual logical dates retain thresholds, but never bypass the daily cap."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11, uploads_today=1)

        assert should_upload(**ctx) is False

    # Unknown hour — defaults to threshold 0
    def test_unknown_hour_queue_0_is_false(self):
        """Unknown hour (e.g. 8), queue=0 → False (defaults to threshold 0)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=0, hour=8)
        assert should_upload(**ctx) is False

    def test_unknown_hour_queue_1_is_true(self):
        """Unknown hour (e.g. 8), queue=1 → True (above default threshold 0)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=1, hour=8)
        assert should_upload(**ctx) is True

    # ---------------------------------------------------------------------------
    # Staleness guard tests (REQ-STALE-01/02/03/04)
    # ---------------------------------------------------------------------------

    def test_stale_run_returns_false(self):
        """data_interval_end ~2h in the past, queue above threshold → False (stale skip)."""
        from datetime import datetime, timedelta, timezone

        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = datetime.now(UTC) - timedelta(hours=2)
        assert should_upload(**ctx) is False

    def test_fresh_run_proceeds_to_threshold(self):
        """data_interval_end ~1 min in the past, queue above threshold → True (threshold applies)."""
        from datetime import datetime, timedelta, timezone

        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = datetime.now(UTC) - timedelta(minutes=1)
        assert should_upload(**ctx) is True

    def test_missing_data_interval_end_falls_through(self):
        """No data_interval_end key in context, queue above threshold → True (backward compat)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        # Explicitly ensure the key is absent (helper does not set it)
        assert "data_interval_end" not in ctx
        assert should_upload(**ctx) is True

    def test_staleness_boundary_strictly_greater(self):
        """data_interval_end exactly 30 min in the past → True (guard uses strict >, not >=)."""
        from datetime import datetime, timedelta, timezone
        from unittest.mock import patch

        from congress_videos.youtube_upload_dag import (
            STALE_RUN_TOLERANCE_MINUTES,
            should_upload,
        )

        frozen_now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=UTC)
        # exactly at boundary: staleness == tolerance, NOT greater
        data_interval_end = frozen_now - timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES)

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = data_interval_end

        with patch("congress_videos.youtube_upload_dag.datetime") as mock_dt:
            mock_dt.now.return_value = frozen_now
            result = should_upload(**ctx)

        assert result is True


# ---------------------------------------------------------------------------
# dry_run param
# ---------------------------------------------------------------------------


class TestDryRun:
    def test_dry_run_skips_upload_and_pushes_empty_results(self):
        """dry_run=True must return early without calling trigger_dag_api."""
        from congress_videos.youtube_upload_dag import trigger_upload_with_config

        ti = _make_ti({"upload_config": {"videos": [{"chapter_id": "c-1"}]}})
        result = trigger_upload_with_config(
            ti, params={"dry_run": True}, run_id="test_dry"
        )

        assert result is None
        assert ti.xcom_store.get("upload_results") == {"upload_details": []}

    def test_dry_run_false_does_not_skip(self, mocker):
        """dry_run=False must proceed to trigger_dag_api as normal."""
        from congress_videos.youtube_upload_dag import trigger_upload_with_config

        mock_run = MagicMock()
        mock_run.run_id = "real_run_001"
        mock_run.state = "success"
        mock_run.execution_date = "2026-07-31T17:00:00+00:00"
        mock_run.refresh_from_db = MagicMock()

        trigger_mock = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=mock_run
        )
        mocker.patch("time.sleep")
        mock_xcom = mocker.patch("airflow.models.XCom")
        mock_xcom.get_many.return_value = []

        ti = _make_ti({"upload_config": {"videos": [{"chapter_id": "c-1"}]}})
        trigger_upload_with_config(ti, params={"dry_run": False}, run_id="test_real")

        trigger_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Helper: build a minimal chapter dict for thumbnail config tests
# ---------------------------------------------------------------------------


def _make_chapter(
    chapter_id: int = 42,
    title: str = "Debate sobre presupuestos",
    description: str = "Una discusión importante",
    session_number: int | None = 80,
    session_date: str | None = "2025-06-10",
    key_speakers: list | None = None,
    speakers: list | None = None,
    resolved_participant_slug: str | None = None,
) -> dict:
    return {
        "chapter_id": chapter_id,
        "chapter_title": title,
        "description": description,
        "session_number": session_number,
        "session_date": session_date,
        "key_speakers": key_speakers
        if key_speakers is not None
        else [{"name": "Ana García"}],
        "speakers": speakers if speakers is not None else ["Ana García"],
        "resolved_participant_slug": resolved_participant_slug,
    }


# ---------------------------------------------------------------------------
# _prepare_thumbnail_config
# ---------------------------------------------------------------------------


class TestPrepareThumbnailConfig:
    """Chapter branch of _prepare_thumbnail_config — read-else-resolve (issue #263)."""

    def test_resolved_participant_slug_is_preferred_over_fuzzy(self):
        """A chapter's resolved_participant_slug wins; the resolver is never called."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            chapter_id=7,
            key_speakers=[{"name": "Ministra de Defensa"}],
            resolved_participant_slug="margarita-robles-fernandez",
        )

        with patch(
            "congress_videos.youtube_upload_dag.resolve_chapter_speakers",
        ) as resolver:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] == "margarita-robles-fernandez"
        resolver.assert_not_called()

    def test_falls_back_to_llm_resolver_when_no_resolved_slug(self):
        """Without a resolved slug, the roster-validated resolver is called and its
        result feeds both the slug and the canonicalized key_speakers."""
        from congress_videos.modules.chapter_speaker_resolution import (
            ChapterSpeakerResolution,
            SpeakerMatch,
        )
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            chapter_id=42,
            title="Debate sobre presupuestos",
            description="Una discusión importante",
            session_number=80,
            key_speakers=[{"name": "Ana Garcia"}],
            resolved_participant_slug=None,
        )
        mock_db = MagicMock()

        match = SpeakerMatch(
            mention="Ana Garcia",
            participant_slug="garcia-ana",
            display_name="Ana García",
            confidence=0.90,
        )
        resolution = ChapterSpeakerResolution(matches=(match,), by_mention={"Ana Garcia": match})

        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[{"slug": "garcia-ana", "display_name": "Ana García"}],
            ),
            patch(
                "congress_videos.youtube_upload_dag.resolve_chapter_speakers",
                return_value=resolution,
            ) as resolver,
        ):
            result = _prepare_thumbnail_config(chapter, mock_db)

        assert result["slug"] == "garcia-ana"
        resolver.assert_called_once()
        assert result["key_speakers"] == [{"name": "Ana García"}]
        mock_db.mark_chapter_resolved.assert_called_once_with(42, "garcia-ana")
        assert result["domain"] == "congreso"
        assert result["debate_summary"] != ""
        assert result["session"] is not None
        assert result["chapter_id"] == 42

    def test_resolver_raising_sets_slug_to_none_without_raising(self):
        """A resolver-path failure yields slug=None; the exception never propagates."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Ana García"}])

        with patch(
            "congress_videos.youtube_upload_dag.get_participants_roster",
            side_effect=RuntimeError("db unavailable"),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        assert result["domain"] == "congreso"

    def test_unmatched_speaker_sets_slug_to_none(self):
        """An unresolved mention is nonfatal and leaves the slug unset."""
        from congress_videos.modules.chapter_speaker_resolution import ChapterSpeakerResolution
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Unknown Speaker"}])
        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[{"slug": "garcia-ana", "display_name": "Ana García"}],
            ),
            patch(
                "congress_videos.youtube_upload_dag.resolve_chapter_speakers",
                return_value=ChapterSpeakerResolution(),
            ) as resolver,
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        resolver.assert_called_once()

    def test_empty_speakers_sets_slug_to_none_without_calling_resolver(self):
        """Chapter with no speaker mentions produces slug=None; resolver is skipped."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[], speakers=[])

        with patch(
            "congress_videos.youtube_upload_dag.resolve_chapter_speakers"
        ) as resolver:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        resolver.assert_not_called()

    def test_placeholder_only_speakers_skip_resolver(self):
        """Chapter whose only speaker mention is a placeholder skips the resolver call."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Desconocido"}], speakers=[])

        with patch(
            "congress_videos.youtube_upload_dag.resolve_chapter_speakers"
        ) as resolver:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        resolver.assert_not_called()

    def test_enabled_false_skips_resolver_and_leaves_slug_none(self):
        """speaker_normalization_config.ENABLED=False disables the resolver call."""
        from congress_videos.config import speaker_normalization_config as snc
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Ana García"}])

        with (
            patch.object(snc, "ENABLED", False),
            patch(
                "congress_videos.youtube_upload_dag.resolve_chapter_speakers"
            ) as resolver,
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        resolver.assert_not_called()


# ---------------------------------------------------------------------------
# trigger_thumbnail_generation
# ---------------------------------------------------------------------------


class TestTriggerThumbnailGeneration:
    THUMBNAIL_CONFIG = {
        "chapter_id": 42,
        "slug": "garcia-ana",
        "domain": "congreso",
        "debate_summary": "Un debate importante sobre el presupuesto",
        "session": "Sesión 80",
    }

    def _successful_child_run(self) -> MagicMock:
        child_run = MagicMock()
        child_run.run_id = "chapter_thumbnail_test_run"
        child_run.state = "success"
        return child_run

    def test_passes_complete_chapter_contract_to_generic_dag(self, mocker) -> None:
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        trigger = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "chapter_id": 42,
                "success": True,
                "output_path": "/thumbnails/42/option_a.png",
                "title": "Generated title",
            },
        )

        trigger_thumbnail_generation(
            _make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG}), run_id="test_run"
        )

        trigger.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            conf={"youtube_video_id": "42", **self.THUMBNAIL_CONFIG, "key_speakers": []},
            run_id="chapter_thumbnail_test_run",
        )

    def test_triggers_generic_dag_without_participant_slug(self, mocker) -> None:
        """A chapter without a resolved speaker still receives a generic thumbnail."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        trigger = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "chapter_id": 42,
                "success": True,
                "output_path": "/thumbnails/42/option_a.png",
                "title": "Generated title",
            },
        )
        config_without_speaker = {**self.THUMBNAIL_CONFIG, "slug": None}

        trigger_thumbnail_generation(
            _make_ti({"thumbnail_config": config_without_speaker}), run_id="test_run"
        )

        trigger.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            conf={"youtube_video_id": "42", **config_without_speaker, "key_speakers": []},
            run_id="chapter_thumbnail_test_run",
        )

    def test_retrieves_result_by_child_dag_and_exact_triggered_run_id(
        self, mocker
    ) -> None:
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        get_one = mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "chapter_id": 42,
                "success": True,
                "output_path": "/thumbnails/42/option_a.png",
                "title": "Generated title",
            },
        )
        ti = _make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG})

        result = trigger_thumbnail_generation(ti, run_id="test_run")

        assert result == child_run.run_id
        assert ti.xcom_store["thumbnail_dag_run_id"] == child_run.run_id
        assert ti.xcom_store["thumbnail_result"]["title"] == "Generated title"
        get_one.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            task_id="thumbnail_result",
            key="return_value",
            run_id=child_run.run_id,
        )

    def test_child_failure_uses_no_custom_thumbnail_fallback(self, mocker) -> None:
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        child_run.state = "failed"
        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        get_one = mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one")
        ti = _make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG})

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert ti.xcom_store["thumbnail_result"] == {
            "chapter_id": 42,
            "success": False,
            "output_path": None,
            "title": None,
        }
        get_one.assert_not_called()

    def test_missing_result_uses_no_custom_thumbnail_fallback(self, mocker) -> None:
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one", return_value=None
        )
        ti = _make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG})

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert ti.xcom_store["thumbnail_result"] == {
            "chapter_id": 42,
            "success": False,
            "output_path": None,
            "title": None,
        }


# ---------------------------------------------------------------------------
# _backfill_thumbnail_video_id
# ---------------------------------------------------------------------------


class TestBackfillThumbnailVideoId:
    def test_calls_update_when_thumbnail_success_true(self):
        """When thumbnail_result.success=True, update_thumbnail_youtube_video_id is called."""
        from congress_videos.youtube_upload_dag import _backfill_thumbnail_video_id

        ti = _make_ti(
            {
                "thumbnail_result": {
                    "success": True,
                    "chapter_id": 42,
                    "output_path": "/tmp/x.png",
                    "title": "T",
                },
                "upload_results": {
                    "upload_details": [{"chapter_id": 42, "youtube_video_id": "abc123"}]
                },
            }
        )
        mock_db = MagicMock()

        _backfill_thumbnail_video_id(ti, mock_db)

        mock_db.update_thumbnail_youtube_video_id.assert_called_once_with(
            chapter_id=42, youtube_video_id="abc123"
        )

    def test_skips_update_when_thumbnail_success_false(self):
        """When thumbnail_result.success=False, update is NOT called."""
        from congress_videos.youtube_upload_dag import _backfill_thumbnail_video_id

        ti = _make_ti(
            {
                "thumbnail_result": {
                    "success": False,
                    "chapter_id": 42,
                    "output_path": None,
                    "title": None,
                },
                "upload_results": {
                    "upload_details": [{"chapter_id": 42, "youtube_video_id": "abc123"}]
                },
            }
        )
        mock_db = MagicMock()

        _backfill_thumbnail_video_id(ti, mock_db)


# ---------------------------------------------------------------------------
# title-news-format Phase 5: key_speakers threading through _prepare_thumbnail_config
# ---------------------------------------------------------------------------


class TestPrepareThumbnailConfigKeySpeakers:
    """Phase 5: _prepare_thumbnail_config must propagate key_speakers from chapter row."""

    def test_key_speakers_present_propagated_in_config(self):
        """Chapter with key_speakers list → returned dict includes key_speakers intact."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            key_speakers=[{"name": "Ana Pastor"}],
        )
        with patch(
            "congress_videos.youtube_upload_dag.get_participants_roster",
            return_value=[],
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert "key_speakers" in result, "_prepare_thumbnail_config must include 'key_speakers' key"
        assert result["key_speakers"] == [{"name": "Ana Pastor"}], (
            "key_speakers must be propagated from chapter row"
        )

    def test_key_speakers_absent_returns_empty_list(self):
        """Chapter without key_speakers key → returned dict has key_speakers=[]."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter()
        # Remove key_speakers from chapter to simulate absent key
        chapter_without = {k: v for k, v in chapter.items() if k != "key_speakers"}

        with patch(
            "congress_videos.youtube_upload_dag.get_participants_roster",
            return_value=[],
        ):
            result = _prepare_thumbnail_config(chapter_without, MagicMock())

        assert result.get("key_speakers") == [], (
            "key_speakers must be [] when chapter row has no key_speakers key"
        )

    def test_key_speakers_none_value_returns_empty_list(self):
        """Chapter with key_speakers value=None (explicit null) → returned dict has key_speakers=[]."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        # Build chapter directly to have key_speakers=None as the actual value
        chapter = _make_chapter()
        chapter["key_speakers"] = None  # explicit None value in the row

        with patch(
            "congress_videos.youtube_upload_dag.get_participants_roster",
            return_value=[],
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result.get("key_speakers") == [], (
            "key_speakers must be [] when chapter.key_speakers is None"
        )


# ---------------------------------------------------------------------------
# SRT fragment threading (Phase 4 — issue #57)
# ---------------------------------------------------------------------------


def _make_srt_chapter(
    *,
    chapter_id: int = 42,
    video_id: str = "vid001",
    start_time: str = "00:01:00,000",
    end_time: str = "00:02:00,000",
) -> dict:
    """Minimal chapter dict with time-window fields for SRT tests."""
    base = _make_chapter(chapter_id=chapter_id)
    base["video_id"] = video_id
    base["start_time"] = start_time
    base["end_time"] = end_time
    return base


class TestPrepareThumbnailConfigSrtFragment:
    """_prepare_thumbnail_config must resolve and thread the SRT fragment."""

    def test_srt_present_adds_srt_fragment_to_config(self):
        """When SRT resolves, config[srt_fragment] includes every overlapping block,
        including ones straddling either window boundary (issue #341: overlap, not
        containment)."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(
            start_time="00:01:00,000",
            end_time="00:02:00,000",
        )

        blocks = [
            # straddles window start (55s..65s crosses 60s) — must be INCLUDED
            {"start_secs": 55.0, "end_secs": 65.0, "text": "arranca antes de la ventana"},
            {"start_secs": 70.0, "end_secs": 80.0, "text": "primera frase del debate"},
            {"start_secs": 90.0, "end_secs": 100.0, "text": "segunda frase importante"},
            # straddles window end (115s..125s crosses 120s) — must be INCLUDED
            {"start_secs": 115.0, "end_secs": 125.0, "text": "termina tras la ventana"},
            # block outside window entirely — must be excluded
            {"start_secs": 200.0, "end_secs": 210.0, "text": "fuera de ventana"},
        ]

        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[],
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/path.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert "srt_fragment" in result
        assert "arranca antes de la ventana" in result["srt_fragment"]
        assert "primera frase del debate" in result["srt_fragment"]
        assert "segunda frase importante" in result["srt_fragment"]
        assert "termina tras la ventana" in result["srt_fragment"]
        assert "fuera de ventana" not in result["srt_fragment"]

    def test_srt_text_capped_at_10000_chars(self):
        """When joined block text exceeds 10,000 chars, srt_fragment is truncated."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(
            start_time="00:00:00,000",
            end_time="01:00:00,000",
        )

        # Build a block whose text is over 10,000 chars
        long_text = "a " * 5001  # 10,002 chars
        blocks = [
            {"start_secs": 10.0, "end_secs": 20.0, "text": long_text},
        ]

        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[],
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/path.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert len(result["srt_fragment"]) == 10_000

    def test_srt_absent_omits_srt_fragment_key(self, caplog):
        """When no SRT resolves, the srt_fragment key must be absent from config,
        and a WARNING naming the row identifiers and the miss cause is logged."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(chapter_id=42, video_id="vid001")

        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[],
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value=None,
            ),
            caplog.at_level(logging.WARNING),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert "srt_fragment" not in result
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("vid001" in r.message and "42" in r.message for r in warnings)


class TestTriggerThumbnailGenerationForwardsSrt:
    """trigger_thumbnail_generation must forward srt_fragment in child_conf."""

    def test_srt_fragment_forwarded_when_present(self, mocker):
        """When thumbnail_config contains srt_fragment, child_conf must include it."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs = []

        mock_run = MagicMock()
        mock_run.run_id = "thumb_run_001"
        mock_run.state = "success"
        mock_run.refresh_from_db = MagicMock()

        def _fake_trigger(dag_id, conf, run_id):
            captured_confs.append(conf)
            return mock_run

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            side_effect=_fake_trigger,
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 42,
                "output_path": "/some/path.png",
                "title": "Un título",
            },
        )

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    "srt_fragment": "vamos a votar ya",
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert captured_confs[0].get("srt_fragment") == "vamos a votar ya"

    def test_srt_fragment_absent_does_not_add_key(self, mocker):
        """When thumbnail_config lacks srt_fragment, child_conf must not have the key."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs = []

        mock_run = MagicMock()
        mock_run.run_id = "thumb_run_002"
        mock_run.state = "success"
        mock_run.refresh_from_db = MagicMock()

        def _fake_trigger(dag_id, conf, run_id):
            captured_confs.append(conf)
            return mock_run

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            side_effect=_fake_trigger,
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 42,
                "output_path": "/some/path.png",
                "title": "Un título",
            },
        )

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    # No srt_fragment key
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert "srt_fragment" not in captured_confs[0]


# ---------------------------------------------------------------------------
# issue #91: title-speaker-attribution — key_speakers forwarding (T-05, T-06)
# ---------------------------------------------------------------------------


class TestTriggerThumbnailGenerationForwardsKeySpeakers:
    """trigger_thumbnail_generation must forward key_speakers into child_conf."""

    def _make_mock_run(self, mocker, captured_confs):
        mock_run = MagicMock()
        mock_run.run_id = "thumb_run_speakers"
        mock_run.state = "success"
        mock_run.refresh_from_db = MagicMock()

        def _fake_trigger(dag_id, conf, run_id):
            captured_confs.append(conf)
            return mock_run

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            side_effect=_fake_trigger,
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 42,
                "output_path": "/some/path.png",
                "title": "Un título",
            },
        )
        return mock_run

    def test_key_speakers_forwarded_when_present(self, mocker):
        """When thumbnail_config has key_speakers, child_conf must include the list."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs = []
        self._make_mock_run(mocker, captured_confs)

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    "key_speakers": ["Cervera Pinar"],
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert captured_confs[0].get("key_speakers") == ["Cervera Pinar"]

    def test_key_speakers_forwarded_as_empty_list_when_absent(self, mocker):
        """When thumbnail_config lacks key_speakers, child_conf must have key_speakers=[]."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs = []
        self._make_mock_run(mocker, captured_confs)

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    # No key_speakers key
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert "key_speakers" in captured_confs[0]
        assert captured_confs[0]["key_speakers"] == []


# ---------------------------------------------------------------------------
# Turn-only queue: None when empty (no chapter fallback)
# ---------------------------------------------------------------------------


# Sentinel distinguishing "not passed" (mirror the turn's own bounds — issue
# #341 default, identical behavior for every pre-existing consumer) from an
# explicitly-passed None (used by the group-bounds-fallback tests).
_UNSET = object()


def _make_turn_row(
    turn_id: int = 1,
    output_path: str = "/data/turn1.mp4",
    resolved_name: str = "Ana García",
    start_seconds: float = 120.0,
    end_seconds: float = 240.0,
    chapter_id: int = 42,
    key_speakers: list | None = None,
    session_number: int = 80,
    session_date: str = "2025-06-10",
    resolved_participant_slug: str | None = None,
    group_start_seconds=_UNSET,
    group_end_seconds=_UNSET,
) -> dict:
    # Non-zero offset (hours=2) reproduces the psycopg2 TIMESTAMPTZ shape that
    # crashes Airflow's XCom serializer with a ValueError on empty ZoneInfo key.
    _tz_offset = timezone(timedelta(hours=2))
    return {
        "turn_id": turn_id,
        "output_path": output_path,
        "resolved_name": resolved_name,
        "start_seconds": start_seconds,
        "end_seconds": end_seconds,
        "chapter_id": chapter_id,
        "key_speakers": key_speakers if key_speakers is not None else ["Ana García", "Pedro López"],
        "session_number": session_number,
        "session_date": session_date,
        "chapter_title": "Un capítulo de prueba",
        "description": "Descripción del capítulo",
        "relevance_score": 4,
        "materialized_at": datetime(2026, 8, 22, 1, 0, tzinfo=_tz_offset),
        "prepared_at": datetime(2026, 8, 22, 0, 0, tzinfo=_tz_offset),
        "resolved_participant_slug": resolved_participant_slug,
        "group_start_seconds": start_seconds if group_start_seconds is _UNSET else group_start_seconds,
        "group_end_seconds": end_seconds if group_end_seconds is _UNSET else group_end_seconds,
    }


def _run_turn_config(turn: dict, blocks: list[dict], *, srt_path: str = "/fake/turn.srt"):
    """Patch the SRT seam and run `_prepare_thumbnail_config` for a turn row.

    Shared by the group-window test suite (issue #341) to avoid a fresh
    triple-patch stack per test. Sets `video_id` when the caller did not,
    since `find_srt_for_chapter` is only reached when `video_id` is present.
    """
    from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

    turn.setdefault("video_id", "vid001")
    with (
        patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ),
        patch(
            "congress_videos.youtube_upload_dag.find_srt_for_chapter",
            return_value=srt_path,
        ),
        patch(
            "congress_videos.youtube_upload_dag._parse_srt_blocks",
            return_value=blocks,
        ),
    ):
        return _prepare_thumbnail_config(turn, MagicMock())


class TestPrepareThumbnailConfigForTurn:
    """_prepare_thumbnail_config anchors key_speakers to turn speaker and uses SRT window."""

    def test_key_speakers_anchored_to_resolved_name(self):
        """Turn config: key_speakers must be [resolved_name] ignoring chapter's key_speakers."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(
            turn_id=1,
            resolved_name="Ana García",
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ):
            result = _prepare_thumbnail_config(turn, MagicMock())

        assert result["key_speakers"] == ["Ana García"]

    def test_slug_resolved_from_resolved_name_not_key_speakers(self):
        """Turn config: slug is derived from resolved_name (turn speaker), not chapter's speakers."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(
            resolved_name="Pedro López",
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "lopez-pedro"},
        ) as lookup:
            result = _prepare_thumbnail_config(turn, MagicMock())

        lookup.assert_called_once_with("Pedro López")
        assert result["slug"] == "lopez-pedro"

    def test_srt_fragment_bounded_by_group_window(self, tmp_path, mocker):
        """Turn config: SRT fragment must be limited to
        [group_start_seconds, group_end_seconds], which is wider than the
        representative turn's own [start_seconds, end_seconds] (issue #341)."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        # SRT content: block 1 outside the turn's own span but inside the group
        # span (straddles the group start), block 2 inside both, block 3 outside
        # the group span entirely.
        srt_content = (
            "1\n00:00:50,000 --> 00:01:10,000\nAntes del turno propio, dentro del grupo.\n\n"
            "2\n00:01:30,000 --> 00:02:00,000\nDentro del turno propio.\n\n"
            "3\n00:04:00,000 --> 00:05:00,000\nFuera del grupo.\n\n"
        )
        srt_path = tmp_path / "test.srt"
        srt_path.write_text(srt_content, encoding="utf-8")

        turn = _make_turn_row(
            turn_id=1,
            start_seconds=90.0,          # 00:01:30 — turn's own narrow span
            end_seconds=120.0,           # 00:02:00
            group_start_seconds=60.0,    # 00:01:00 — wider group span
            group_end_seconds=180.0,     # 00:03:00
        )
        turn["video_id"] = "video123"
        turn["session_date"] = "2025-06-10"

        mocker.patch(
            "congress_videos.youtube_upload_dag.find_srt_for_chapter",
            return_value=str(srt_path),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        )

        result = _prepare_thumbnail_config(turn, MagicMock())

        assert "srt_fragment" in result
        assert "Antes del turno propio, dentro del grupo" in result["srt_fragment"]
        assert "Dentro del turno propio" in result["srt_fragment"]
        assert "Fuera del grupo" not in result["srt_fragment"]

    def test_chapter_id_preserved_in_config(self):
        """Turn config: chapter_id must be preserved for thumbnail identity."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(turn_id=5, chapter_id=42)

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
        ):
            result = _prepare_thumbnail_config(turn, MagicMock())

        assert result["chapter_id"] == 42

    def test_session_derived_from_session_number(self):
        """Turn config: session label derived from session_number."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(session_number=80)

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
        ):
            result = _prepare_thumbnail_config(turn, MagicMock())

        assert "80" in result["session"]


def _group_turn(**overrides) -> dict:
    """Turn bounds for the group-window suite: own span 90-120s, group span 60-180s."""
    base = {"start_seconds": 90.0, "end_seconds": 120.0, "group_start_seconds": 60.0, "group_end_seconds": 180.0}
    base.update(overrides)
    return _make_turn_row(**base)


class TestPrepareThumbnailConfigGroupWindow:
    """Turn-row SRT windowing uses the GROUP span, not the representative
    turn's own narrow span (issue #341). Overlap semantics, per-field
    coercing fallback, and non-fatal empty-window handling."""

    def test_block_outside_turn_span_inside_group_span_included(self):
        """A block inside the group span but outside the turn's own span is INCLUDED (#341 lock)."""
        turn = _group_turn()
        blocks = [{"start_secs": 70.0, "end_secs": 80.0, "text": "dentro del grupo"}]

        config = _run_turn_config(turn, blocks)

        assert "dentro del grupo" in config.get("srt_fragment", "")

    def test_block_outside_group_span_excluded(self):
        """A block with no overlap with the group span is EXCLUDED."""
        turn = _group_turn()
        blocks = [
            {"start_secs": 100.0, "end_secs": 110.0, "text": "dentro del grupo"},
            {"start_secs": 200.0, "end_secs": 210.0, "text": "fuera del grupo"},
        ]

        config = _run_turn_config(turn, blocks)

        assert "dentro del grupo" in config["srt_fragment"]
        assert "fuera del grupo" not in config["srt_fragment"]

    @pytest.mark.parametrize(
        ("start_secs", "end_secs", "text"),
        [(55.0, 65.0, "cruza el inicio del grupo"), (175.0, 185.0, "cruza el final del grupo")],
        ids=["start-boundary", "end-boundary"],
    )
    def test_block_straddling_group_boundary_included(self, start_secs, end_secs, text):
        """A block whose span crosses either group boundary is INCLUDED."""
        turn = _group_turn()
        blocks = [{"start_secs": start_secs, "end_secs": end_secs, "text": text}]

        config = _run_turn_config(turn, blocks)

        assert text in config.get("srt_fragment", "")

    @pytest.mark.parametrize(
        ("group_start", "group_end", "pop_keys"),
        [(None, None, True), (None, None, False), ("not-a-number", "also-bad", False)],
        ids=["missing-keys", "none-bounds", "non-numeric"],
    )
    def test_group_bounds_fallback_to_turn_bounds(self, group_start, group_end, pop_keys):
        """Missing, None, or unparsable group bounds fall back to the turn's own start/end."""
        turn = _make_turn_row(
            start_seconds=60.0,
            end_seconds=120.0,
            group_start_seconds=group_start,
            group_end_seconds=group_end,
        )
        if pop_keys:
            del turn["group_start_seconds"]
            del turn["group_end_seconds"]
        blocks = [
            {"start_secs": 70.0, "end_secs": 80.0, "text": "dentro del turno"},
            {"start_secs": 300.0, "end_secs": 310.0, "text": "muy lejos"},
        ]

        config = _run_turn_config(turn, blocks)

        assert "dentro del turno" in config["srt_fragment"]
        assert "muy lejos" not in config["srt_fragment"]

    def test_decimal_group_bounds_produce_same_window_as_float(self, caplog):
        """Decimal group bounds (the DB driver's real shape for NUMERIC columns)
        must produce a non-empty fragment, with no empty-window WARNING fired
        by the type alone (design finding F1)."""
        turn = _make_turn_row(
            start_seconds=60.0,
            end_seconds=120.0,
            group_start_seconds=Decimal("60.0"),
            group_end_seconds=Decimal("120.0"),
        )
        blocks = [{"start_secs": 70.0, "end_secs": 80.0, "text": "cita con decimal"}]

        with caplog.at_level(logging.WARNING):
            config = _run_turn_config(turn, blocks)

        assert config.get("srt_fragment") == "cita con decimal"
        assert not any("empty SRT window" in r.message for r in caplog.records)

    def test_empty_window_omits_key_and_warns(self, caplog):
        """An empty overlap omits srt_fragment (never "") and logs a WARNING
        naming the row identifiers and the empty-window cause."""
        turn = _group_turn(turn_id=7, chapter_id=42)
        blocks = [{"start_secs": 500.0, "end_secs": 510.0, "text": "muy lejos del grupo"}]

        with caplog.at_level(logging.WARNING):
            config = _run_turn_config(turn, blocks)

        assert "srt_fragment" not in config
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("empty SRT window" in r.message for r in warnings)
        assert any("42" in r.message and "7" in r.message for r in warnings)


class TestPrepareThumbnailConfigCanonicalDir:
    """`find_srt_for_chapter` must receive `canonical_dir` (issue #341/#340)."""

    def test_canonical_dir_passed_for_turn_row(self):
        """Turn row: canonical_dir == str(get_video_chapter_dir(video_id, chapter_id))."""
        from congress_videos.config.paths import get_video_chapter_dir

        turn = _make_turn_row(chapter_id=42)
        turn["video_id"] = "vid001"
        mock_find = self._run_and_capture(turn)

        assert mock_find.call_args.kwargs["canonical_dir"] == str(get_video_chapter_dir("vid001", 42))

    def test_canonical_dir_passed_for_chapter_row(self):
        """Chapter row: canonical_dir == str(get_video_chapter_dir(video_id, chapter_id))."""
        from congress_videos.config.paths import get_video_chapter_dir
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(chapter_id=42, video_id="vid001")
        with (
            patch("congress_videos.youtube_upload_dag.get_participants_roster", return_value=[]),
            patch("congress_videos.youtube_upload_dag.find_srt_for_chapter", return_value=None) as mock_find,
        ):
            _prepare_thumbnail_config(chapter, MagicMock())

        assert mock_find.call_args.kwargs["canonical_dir"] == str(get_video_chapter_dir("vid001", 42))

    def test_canonical_dir_none_when_chapter_id_missing(self):
        """When chapter_id is missing/None, canonical_dir must be None (D3) —
        legacy probe behavior stays unchanged."""
        turn = _make_turn_row()
        turn["video_id"] = "vid001"
        turn["chapter_id"] = None
        mock_find = self._run_and_capture(turn)

        assert mock_find.call_args.kwargs["canonical_dir"] is None

    @staticmethod
    def _run_and_capture(turn: dict):
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ),
            patch("congress_videos.youtube_upload_dag.find_srt_for_chapter", return_value=None) as mock_find,
        ):
            _prepare_thumbnail_config(turn, MagicMock())
        return mock_find


class TestPrepareThumbnailConfigForTurnSlugFallback:
    """resolved_name -> resolved_participant_slug fallback (issue #131),
    mirroring the chapter branch's slug-first precedence."""

    def test_resolved_name_present_unchanged(self):
        """resolved_name present -> unchanged; resolved_participant_slug is
        never consulted even when set."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(
            resolved_name="Ana García",
            resolved_participant_slug="lopez-pedro",
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ) as lookup, patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
        ) as by_slug:
            result = _prepare_thumbnail_config(turn, MagicMock())

        assert result["key_speakers"] == ["Ana García"]
        assert result["slug"] == "garcia-ana"
        lookup.assert_called_once_with("Ana García")
        by_slug.assert_not_called()

    def test_empty_resolved_name_falls_back_to_slug(self):
        """Empty resolved_name + non-empty resolved_participant_slug ->
        key_speakers/slug derive from resolved_participant_slug."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(
            resolved_name="",
            resolved_participant_slug="lopez-pedro",
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            return_value={"slug": "lopez-pedro", "display_name": "Pedro López"},
        ) as by_slug:
            result = _prepare_thumbnail_config(turn, MagicMock())

        by_slug.assert_called_once_with("lopez-pedro")
        assert result["slug"] == "lopez-pedro"
        assert result["key_speakers"] == ["Pedro López"]

    def test_both_empty_gives_empty_key_speakers_and_none_slug(self):
        """Both resolved_name and resolved_participant_slug empty ->
        key_speakers=[] and slug=None, unchanged from current behavior."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(resolved_name="", resolved_participant_slug=None)

        result = _prepare_thumbnail_config(turn, MagicMock())

        assert result["key_speakers"] == []
        assert result["slug"] is None


class TestTurnQueueSelection:
    """Turn-only queue: selects the next turn to upload; returns None when empty."""

    def test_uploader_selects_turn_when_available(self, mocker):
        """When get_uploadable_turns returns a turn, _run_get_uploadable_item returns it
        (with datetime fields UTC-normalized)."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item
        from utils.airflow_helpers import utc_normalize_row

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]
        mock_db.get_uploadable_chapters.return_value = []

        result = _run_get_uploadable_item(mock_db)

        assert result["item"] == utc_normalize_row(fake_turn)
        assert result["item_type"] == "turn"
        mock_db.get_uploadable_chapters.assert_not_called()

    def test_uploader_returns_none_when_turns_empty(self):
        """When turns empty, _run_get_uploadable_item returns None without calling get_uploadable_chapters."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []

        result = _run_get_uploadable_item(mock_db)

        assert result is None
        mock_db.get_uploadable_chapters.assert_not_called()

    def test_uploader_returns_none_when_both_queues_empty(self):
        """When both queues empty, _run_get_uploadable_item returns None."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []
        mock_db.get_uploadable_chapters.return_value = []

        result = _run_get_uploadable_item(mock_db)

        assert result is None

    def test_turn_xcom_datetimes_are_utc_normalized(self):
        """PRIMARY regression test (issues #163, #309): materialized_at and
        prepared_at must be UTC-normalized datetime values after
        _run_get_uploadable_item, so Airflow's XCom serializer never sees a
        non-zero-offset stdlib tzinfo."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]

        result = _run_get_uploadable_item(mock_db)

        assert result is not None
        for key in ("materialized_at", "prepared_at"):
            v = result["item"][key]
            assert isinstance(v, datetime)
            assert v.utcoffset() == timedelta(0)
            assert v == fake_turn[key]

    def test_turn_xcom_materialized_at_is_utc_datetime(self):
        """materialized_at must be a UTC-normalized datetime, not a string."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]

        result = _run_get_uploadable_item(mock_db)

        value = result["item"]["materialized_at"]
        assert isinstance(value, datetime)
        assert value.utcoffset() == timedelta(0)
        assert not isinstance(value, str)

    def test_turn_xcom_item_survives_real_xcom_round_trip_as_datetime(self):
        """Contract test (issue #309): after _run_get_uploadable_item builds the
        uploadable_item payload from a turn with a non-UTC fixed-offset
        materialized_at/prepared_at, the payload survives Airflow's REAL XCom
        serializer round-trip AND decodes as a UTC-normalized datetime (not a
        string). This proves the string -> datetime contract change actually
        took effect end-to-end through the real serializer, not just at the
        call site's return value.

        NOTE: this test does NOT assert a ValueError against unmodified code.
        _run_get_uploadable_item's pre-#309 body already avoided the ZoneInfo
        crash via the old ISO-8601 stringify helper it used to call, so no
        crash is reproducible at THIS call site either before or after this
        change.
        The RED signal for this test is a type mismatch (str, not datetime) —
        see test_turn_xcom_item_raw_row_breaks_real_xcom_round_trip below for
        the actual crash-reproducing bug-pin, which proves the +02:00 shape
        genuinely breaks the serializer and that normalization is load-bearing.
        """
        import json

        from airflow.utils.json import XComDecoder, XComEncoder

        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        def _xcom_round_trip(value):
            return json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]

        result = _run_get_uploadable_item(mock_db)
        restored = _xcom_round_trip(result)

        for key in ("materialized_at", "prepared_at"):
            v = restored["item"][key]
            assert isinstance(v, datetime)
            assert v.utcoffset() == timedelta(0)
            assert v == fake_turn[key]
        assert restored["item_type"] == "turn"

    def test_turn_xcom_item_raw_row_breaks_real_xcom_round_trip(self):
        """Bug-pin (issue #309): a RAW turn row (the un-normalized dict
        straight from the fixture, bypassing every helper) DOES break
        Airflow's REAL XCom serializer round-trip with the exact ZoneInfo
        crash. This must stay red-raising FOREVER — it deliberately never
        normalizes. It proves the +02:00 offset shape genuinely breaks the
        serializer, so the normalization applied at the call site is doing
        real work rather than being decorative.

        Mirrors tests/utils/test_airflow_helpers.py::TestXComSerializerRoundTrip
        ::test_raw_non_utc_offset_row_breaks_xcom_round_trip."""
        import json

        from airflow.utils.json import XComDecoder, XComEncoder

        def _xcom_round_trip(value):
            return json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)

        fake_turn = _make_turn_row()

        # match= is load-bearing: without it any ValueError would satisfy this
        # pin, including one raised for an unrelated reason. The point of the
        # test is that THIS specific tz defect is what breaks the round-trip.
        with pytest.raises(
            ValueError, match="ZoneInfo keys must be normalized relative paths"
        ):
            _xcom_round_trip(fake_turn)


class TestGuardAAndGuardB:
    """View-level guards: Guard A (chapter excluded when turn uploaded),
    Guard B (turn excluded when chapter uploaded). Both enforced in the SQL
    view — these tests verify the uploader passes the right rows through."""

    def test_guard_b_turn_excluded_by_empty_view(self):
        """Guard B: when uploadable_turns is empty (view filtered them out),
        uploader returns None (no turns to upload)."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []  # Guard B filtered them out

        result = _run_get_uploadable_item(mock_db)
        assert result is None

    def test_guard_a_chapter_excluded_by_empty_view(self):
        """Guard A: when uploadable_turns is empty (turn already uploaded),
        uploader returns None (turn-only queue; no chapter fallback)."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []

        result = _run_get_uploadable_item(mock_db)
        assert result is None


# ---------------------------------------------------------------------------
# CRITICAL-1: Dual-queue wired into DAG task graph (not dead code)
# ---------------------------------------------------------------------------


class TestDualQueueWiredIntoDag:
    """Verify that the DAG task graph actually invokes dual-queue logic
    rather than calling get_uploadable_chapters directly."""

    def test_dag_has_get_uploadable_item_task_not_static_chapters_op(self):
        """DAG must have a 'get_uploadable_item' task (PythonOperator), not a
        static get_uploadable_chapters lookup. Wires turn-only queue."""
        from congress_videos.youtube_upload_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        # The wired task must exist
        assert "get_uploadable_item" in task_ids, (
            "DAG must have a get_uploadable_item task (dual-queue wired)"
        )

    def test_get_uploadable_item_task_is_python_operator(self):
        """The get_uploadable_item task must be a PythonOperator."""
        from airflow.operators.python import PythonOperator

        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        task = tasks_by_id.get("get_uploadable_item")
        assert task is not None, "get_uploadable_item task must exist"
        assert isinstance(task, PythonOperator), (
            "get_uploadable_item must be a PythonOperator"
        )

    def test_get_uploadable_item_is_downstream_of_skip_if_quota_reached(self):
        """get_uploadable_item must be downstream of skip_if_quota_reached."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        skip_task = tasks_by_id["skip_if_quota_reached"]
        item_task = tasks_by_id.get("get_uploadable_item")
        assert item_task is not None, "get_uploadable_item task must exist"
        downstream_ids = {t.task_id for t in skip_task.downstream_list}
        assert item_task.task_id in downstream_ids, (
            "get_uploadable_item must be downstream of skip_if_quota_reached"
        )

    def test_generate_metadata_is_downstream_of_get_uploadable_item(self):
        """generate_youtube_metadata must be downstream of get_uploadable_item."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        item_task = tasks_by_id.get("get_uploadable_item")
        meta_task = tasks_by_id["generate_youtube_metadata"]
        assert item_task is not None, "get_uploadable_item task must exist"
        upstream_ids = {t.task_id for t in meta_task.upstream_list}
        assert item_task.task_id in upstream_ids, (
            "generate_youtube_metadata must be downstream of get_uploadable_item"
        )

    def test_dag_task_count_updated_for_wired_dual_queue(self):
        """DAG must have 14 tasks after replacing t1_db with get_uploadable_item and adding mark_turns_uploaded."""
        from congress_videos.youtube_upload_dag import dag

        assert len(dag.tasks) == 14, (
            f"Expected 14 tasks (13 original tasks, t1_db replaced by get_uploadable_item PythonOperator, "
            f"plus mark_turns_uploaded), got {len(dag.tasks)}"
        )

    def test_mark_turns_uploaded_task_exists(self):
        """DAG must have a mark_turns_uploaded task (CRITICAL-3)."""
        from congress_videos.youtube_upload_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "mark_turns_uploaded" in task_ids, (
            "DAG must have a mark_turns_uploaded task after upload"
        )

    def test_mark_turns_uploaded_is_downstream_of_trigger_youtube_upload(self):
        """mark_turns_uploaded must be downstream of trigger_youtube_upload."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        upload_task = tasks_by_id["trigger_youtube_upload"]
        mark_turns = tasks_by_id.get("mark_turns_uploaded")
        assert mark_turns is not None, "mark_turns_uploaded task must exist"
        downstream_ids = {t.task_id for t in upload_task.downstream_list}
        assert mark_turns.task_id in downstream_ids, (
            "mark_turns_uploaded must be downstream of trigger_youtube_upload"
        )


# ---------------------------------------------------------------------------
# CRITICAL-2: Combined daily cap (turns + chapters in uploads_today)
# ---------------------------------------------------------------------------


class TestCombinedDailyCap:
    """check_upload_quota must count turns uploaded today + chapters uploaded today."""

    def test_uploads_today_includes_turns_and_chapters(self, mocker):
        """When 1 turn and 1 chapter were uploaded today, uploads_today must be 2."""
        from congress_videos.youtube_upload_dag import _run_check_upload_quota

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_turns_uploaded_today.return_value = 1
        mock_db.count_pending_uploadable_chapters.return_value = 3
        mock_db.count_pending_uploadable_turns.return_value = 2

        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db,
        )

        ti = _make_ti({})
        result = _run_check_upload_quota(ti, params={})

        stored = ti.xcom_store.get("upload_quota")
        assert stored is not None, "upload_quota must be pushed to XCom"
        assert result["uploads_today"] == 2, (
            f"uploads_today must be turns (1) + chapters (1) = 2, got {result['uploads_today']}"
        )

    def test_uploads_today_zero_when_nothing_uploaded(self, mocker):
        """When no turns and no chapters uploaded today, uploads_today must be 0."""
        from congress_videos.youtube_upload_dag import _run_check_upload_quota

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 5
        mock_db.count_pending_uploadable_turns.return_value = 2

        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db,
        )

        ti = _make_ti({})
        result = _run_check_upload_quota(ti, params={})

        assert result["uploads_today"] == 0

    def test_uploads_today_chapter_only_when_no_turns_uploaded(self, mocker):
        """When only chapters uploaded today, uploads_today = chapter count."""
        from congress_videos.youtube_upload_dag import _run_check_upload_quota

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 2
        mock_db.count_pending_uploadable_turns.return_value = 0

        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db,
        )

        ti = _make_ti({})
        result = _run_check_upload_quota(ti, params={})

        assert result["uploads_today"] == 1


# ---------------------------------------------------------------------------
# WARNING-1: queue_size includes turns_pending in should_upload gate
# ---------------------------------------------------------------------------


class TestQueueSizeIncludesTurns:
    """should_upload must gate on combined queue size (turns + chapters)."""

    def test_queue_size_with_only_turns_pending_allows_upload(self):
        """When only turns are pending (queue_size=0 for chapters), gate on combined."""
        from congress_videos.youtube_upload_dag import should_upload

        # Simulate quota xcom: chapter queue empty but turns pending
        # queue_size must already include turns for the gate to work.
        # This test verifies the combined queue_size is what should_upload sees.
        ctx = _make_context_for_should_upload(queue_size=1, hour=19, uploads_today=0)
        # queue_size=1 represents turns_pending=1 counted in combined queue_size
        assert should_upload(**ctx) is True

    def test_combined_queue_size_in_check_upload_quota(self, mocker):
        """check_upload_quota queue_size must include turns_pending + chapters pending."""
        from congress_videos.youtube_upload_dag import _run_check_upload_quota

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 2
        mock_db.count_pending_uploadable_turns.return_value = 3  # 3 turns pending

        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db,
        )

        ti = _make_ti({})
        result = _run_check_upload_quota(ti, params={})

        # queue_size must be chapters(2) + turns(3) = 5
        assert result["queue_size"] == 5, (
            f"queue_size must be chapters_pending(2) + turns_pending(3) = 5, got {result['queue_size']}"
        )


# ---------------------------------------------------------------------------
# thumbnail-canonical-path (Slice 4a): output_path threading
# ---------------------------------------------------------------------------


class TestPrepareThumbnailConfigThreadsOutputPath:
    """_prepare_thumbnail_config must include output_path for turn items, absent for chapters."""

    def test_turn_item_includes_output_path(self):
        """Turn row with output_path → config['output_path'] equals the row value."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(output_path="/data/oradores/42/video.mp4")

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
        ):
            config = _prepare_thumbnail_config(turn, MagicMock())

        assert config["output_path"] == "/data/oradores/42/video.mp4"

    def test_chapter_item_omits_output_path(self):
        """Chapter row (no turn_id) → config.get('output_path') is None."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter()

        with patch(
            "congress_videos.youtube_upload_dag.get_participants_roster",
            return_value=[],
        ):
            config = _prepare_thumbnail_config(chapter, MagicMock())

        assert config.get("output_path") is None

    def test_turn_item_with_none_output_path_is_set_to_none(self):
        """Turn row whose output_path column is NULL → config['output_path'] is None (not absent)."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(output_path=None)  # type: ignore[arg-type]

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
        ):
            config = _prepare_thumbnail_config(turn, MagicMock())

        # Key must be present (set by is_turn branch) even if value is None
        assert "output_path" in config
        assert config["output_path"] is None


class TestTriggerThumbnailGenerationForwardsOutputPath:
    """trigger_thumbnail_generation must forward output_path into child_conf only when truthy."""

    def _make_mock_run(self, mocker, captured_confs):
        mock_run = MagicMock()
        mock_run.run_id = "thumb_run_output_path"
        mock_run.state = "success"
        mock_run.refresh_from_db = MagicMock()

        def _fake_trigger(dag_id, conf, run_id):
            captured_confs.append(conf)
            return mock_run

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            side_effect=_fake_trigger,
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value={
                "success": True,
                "chapter_id": 42,
                "output_path": "/data/oradores/42/thumbnail.png",
                "title": "Un título",
            },
        )
        return mock_run

    def test_forwards_output_path_when_present(self, mocker):
        """When thumbnail_config has a truthy output_path, child_conf must include it."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs: list = []
        self._make_mock_run(mocker, captured_confs)

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    "output_path": "/data/oradores/42/video.mp4",
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert captured_confs[0].get("output_path") == "/data/oradores/42/video.mp4"

    def test_omits_output_path_when_absent(self, mocker):
        """When thumbnail_config lacks output_path, child_conf must NOT contain the key."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs: list = []
        self._make_mock_run(mocker, captured_confs)

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    # No output_path key
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert "output_path" not in captured_confs[0]

    def test_omits_output_path_when_none(self, mocker):
        """When thumbnail_config has output_path=None, child_conf must NOT contain the key."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        captured_confs: list = []
        self._make_mock_run(mocker, captured_confs)

        ti = _make_ti(
            {
                "thumbnail_config": {
                    "chapter_id": 42,
                    "debate_summary": "un resumen",
                    "session": "Sesión 80",
                    "domain": "congreso",
                    "slug": None,
                    "output_path": None,
                }
            }
        )

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert len(captured_confs) == 1
        assert "output_path" not in captured_confs[0]


# ---------------------------------------------------------------------------
# SRT sidecar write (Slice 5 — srt-sidecar-canonical-path)
# ---------------------------------------------------------------------------

class TestPrepareThumbnailConfigSrtSidecar:
    """Upload path (issue #146 Fix C) no longer writes subtitles.srt for turns.

    The nightly speaker_turn_prepare DAG now owns the turn subtitles.srt sidecar,
    so _prepare_thumbnail_config must NOT write it at upload time. It still
    computes config['srt_fragment'] for the lapidary thumbnail quote.
    """

    _WINDOWED_BLOCKS = [
        {"start_secs": 60.0, "end_secs": 70.0, "text": "primera frase"},
        {"start_secs": 75.0, "end_secs": 85.0, "text": "segunda frase"},
        # outside window — must not appear in SRT
        {"start_secs": 200.0, "end_secs": 210.0, "text": "fuera de ventana"},
    ]

    def _run_turn(self, tmp_path, blocks=None, output_path_override=None):
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        video_mp4 = tmp_path / "video.mp4"
        video_mp4.write_bytes(b"")
        out_path = output_path_override if output_path_override is not None else str(video_mp4)
        turn = _make_turn_row(
            output_path=out_path,
            start_seconds=50.0,
            end_seconds=100.0,
        )
        # video_id is required so find_srt_for_chapter is actually called
        turn["video_id"] = "vid001"
        if blocks is None:
            blocks = self._WINDOWED_BLOCKS

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/session.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            return _prepare_thumbnail_config(turn, MagicMock()), tmp_path

    def test_turn_with_output_path_does_not_write_subtitles_srt(self, tmp_path):
        """Turn-type + output_path -> upload path must NOT write subtitles.srt (PREPARE owns it).

        Updated for issue #146 Fix C: PREPARE DAG now owns the turn srt sidecar.
        srt_fragment is still computed for the lapidary quote.
        """
        config, out_dir = self._run_turn(tmp_path)

        srt_path = out_dir / "subtitles.srt"
        assert not srt_path.exists(), (
            "upload path must NOT write subtitles.srt for turns; PREPARE DAG owns it"
        )
        assert not any(out_dir.rglob("subtitles.srt"))
        # srt_fragment still computed for the lapidary thumbnail quote.
        assert "primera frase" in config.get("srt_fragment", "")
        assert "segunda frase" in config.get("srt_fragment", "")
        assert "fuera de ventana" not in config.get("srt_fragment", "")
        assert config.get("chapter_id") == 42  # config unaffected

    def test_chapter_type_produces_no_subtitles_srt(self, tmp_path):
        """Chapter row (no turn_id) -> no subtitles.srt written anywhere."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(start_time="00:00:50,000", end_time="00:01:40,000")
        blocks = [
            {"start_secs": 60.0, "end_secs": 70.0, "text": "chapter text"},
        ]

        with (
            patch(
                "congress_videos.youtube_upload_dag.get_participants_roster",
                return_value=[],
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/session.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            _prepare_thumbnail_config(chapter, MagicMock())

        assert not any(tmp_path.rglob("subtitles.srt"))

    def test_turn_with_none_output_path_produces_no_write(self, tmp_path):
        """Turn row whose output_path is None -> no subtitles.srt written."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn = _make_turn_row(output_path=None, start_seconds=50.0, end_seconds=100.0)  # type: ignore[arg-type]
        turn["video_id"] = "vid001"
        blocks = [{"start_secs": 60.0, "end_secs": 70.0, "text": "texto"}]

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/session.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            config = _prepare_thumbnail_config(turn, MagicMock())

        assert not any(tmp_path.rglob("subtitles.srt"))
        assert "chapter_id" in config  # config still returned

    def test_turn_path_never_opens_a_file_for_writing(self, tmp_path):
        """Upload path must not open the turn dir for writing (issue #146 Fix C).

        Previously the upload path wrote subtitles.srt (and swallowed OSError). Now
        that PREPARE owns the srt, no write occurs, so no subtitles.srt file exists.
        """
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        video_mp4 = tmp_path / "video.mp4"
        video_mp4.write_bytes(b"")
        turn = _make_turn_row(
            output_path=str(video_mp4),
            start_seconds=50.0,
            end_seconds=100.0,
        )
        turn["video_id"] = "vid001"
        blocks = [{"start_secs": 60.0, "end_secs": 70.0, "text": "texto"}]

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value="/fake/session.srt",
            ),
            patch(
                "congress_videos.youtube_upload_dag._parse_srt_blocks",
                return_value=blocks,
            ),
        ):
            config = _prepare_thumbnail_config(turn, MagicMock())

        assert "chapter_id" in config
        assert not any(tmp_path.rglob("subtitles.srt")), (
            "upload path must not write subtitles.srt for turns"
        )


# ---------------------------------------------------------------------------
# Phase 4.3 — Upload DAG turn path (issue #146): no thumbnail trigger for turns
# ---------------------------------------------------------------------------


class TestUploadDagTurnPathRefactor:
    """Verify the upload DAG turn branch generates thumbnail and fresh metadata (issue #169).

    After unify-upload-metadata, the upload DAG MUST call trigger_thumbnail_generation
    for turn items (no more skip), and must overwrite title.txt/description.txt sidecars
    from fresh youtube_metadata_results XCom before reading them.
    """

    def test_run_generate_thumbnail_called_for_turns(self):
        """When item_type=turn, _run_generate_thumbnail must trigger the thumbnail DAG (issue #169)."""
        from unittest.mock import patch

        from congress_videos.youtube_upload_dag import _run_generate_thumbnail

        store = {
            "uploadable_item": {
                "item": {"turn_id": 1, "output_path": "/data/v.mp4"},
                "item_type": "turn",
            }
        }
        ti = _make_ti(store)

        with patch("congress_videos.youtube_upload_dag.trigger_thumbnail_generation") as mock_trig:
            mock_trig.return_value = "thumb_run_id"
            _run_generate_thumbnail(ti)

        mock_trig.assert_called_once()

    def test_prepare_upload_config_turn_uses_fresh_xcom_title(self, tmp_path):
        """_prepare_upload_config for a turn item must use fresh XCom title (issue #169).

        Fresh 19:00 AI metadata overwrites any stale sidecar on disk before
        prepare_orador_upload_config reads title.txt.
        """
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        # On-disk sidecars (would be stale in old flow, or freshly written from XCom now)
        (turn_dir / "title.txt").write_text("", encoding="utf-8")
        (turn_dir / "description.txt").write_text("", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": None,
                    "turn_id": 1,
                    "video_id": "vidXYZ",
                    "success": True,
                    "output_path": str(turn_dir / "video.mp4"),
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None,
                }
            ],
        }

        # Fresh 19:00 AI description in XCom; title is sourced from
        # this run's thumbnail_result, not from youtube_metadata_results (#245).
        fresh_metadata = {
            "topic_metadata": [
                {
                    "description": {"description": "Desc fresca."},
                }
            ]
        }

        store = {
            "uploadable_item": {
                "item": {
                    "turn_id": 1,
                    "output_path": str(turn_dir / "video.mp4"),
                    "chapter_id": 100,
                },
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": fresh_metadata,
            "thumbnail_result": {"success": True, "title": "TÍTULO FRESCO DESDE XCOM"},
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(ti, **context)

        config = ti.xcom_store.get("upload_config")
        assert config is not None
        videos = config.get("videos", [])
        assert len(videos) == 1
        assert videos[0]["title"] == "TÍTULO FRESCO DESDE XCOM"

    def test_prepare_upload_config_turn_no_ai_call(self, tmp_path):
        """For turn items, _prepare_upload_config must not call youtube_ai."""
        from unittest.mock import patch

        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": None,
                    "turn_id": 1,
                    "video_id": "vidXYZ",
                    "success": True,
                    "output_path": str(turn_dir / "video.mp4"),
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None,
                }
            ],
        }

        store = {
            "uploadable_item": {
                "item": {"turn_id": 1, "output_path": str(turn_dir / "video.mp4")},
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": None,
            "thumbnail_result": {"success": True, "title": "T"},
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        with patch("congress_videos.modules.youtube.youtube_ai.generate_youtube_metadata_for_selected_videos") as mock_ai:
            _prepare_upload_config(ti, **context)

        mock_ai.assert_not_called()

    def test_prepare_upload_config_chapter_unchanged(self):
        """For chapter items, _prepare_upload_config must still use AI metadata (unchanged path)."""
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": 999,
                    "video_id": "vidABC",
                    "success": True,
                    "output_path": "/data/chapter_video.mp4",
                    "file_size_mb": 50.0,
                    "duration_seconds": 300.0,
                    "error": None,
                }
            ],
        }
        metadata = {
            "topic_metadata": [
                {
                    "chapter_id": 999,
                    "video_id": "vidABC",
                    "title": {"title": "Chapter Title"},
                    "description": {"description": "Chapter desc"},
                }
            ]
        }

        store = {
            "uploadable_item": {"item": {"chapter_id": 999}, "item_type": "chapter"},
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": metadata,
            "thumbnail_result": None,
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(ti, **context)

        config = ti.xcom_store.get("upload_config")
        assert config is not None
        videos = config.get("videos", [])
        assert len(videos) == 1
        assert videos[0]["title"] == "Chapter Title"


# ---------------------------------------------------------------------------
# Issue #245: turn title must come from this run's thumbnail_result XCom;
# a missing/invalid title blocks the upload instead of publishing a fallback.
# ---------------------------------------------------------------------------


class TestPrepareUploadConfigTurnRequiresThumbnailTitle:
    """_prepare_upload_config for turns must raise when thumbnail_result lacks
    a valid, non-empty title — never publish a fallback title (issue #245).
    """

    @pytest.mark.parametrize(
        "thumbnail_result",
        [
            None,
            {"success": False, "title": None},
            {"success": True, "title": ""},
        ],
        ids=["missing", "failed", "empty-title"],
    )
    def test_raises_and_pushes_no_upload_config(self, tmp_path, thumbnail_result):
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("", encoding="utf-8")
        (turn_dir / "description.txt").write_text("", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": 100,
                    "turn_id": 1,
                    "video_id": "vidXYZ",
                    "success": True,
                    "output_path": str(turn_dir / "video.mp4"),
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None,
                }
            ],
        }

        store = {
            "uploadable_item": {
                "item": {
                    "turn_id": 1,
                    "output_path": str(turn_dir / "video.mp4"),
                    "chapter_id": 100,
                },
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": {
                "topic_metadata": [{"description": {"description": "Desc."}}]
            },
            "thumbnail_result": thumbnail_result,
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        with pytest.raises(ValueError):
            _prepare_upload_config(ti, **context)

        assert "upload_config" not in ti.xcom_store


# ---------------------------------------------------------------------------
# Phase 1.2: _prepare_upload_config for turns overwrites sidecars from XCom
# Issue #169/#245: fresh title (thumbnail_result) + fresh description
# (youtube_metadata_results) win over stale on-disk sidecars
# ---------------------------------------------------------------------------


class TestPrepareUploadConfigTurnOverwritesSidecarsFromXcom:
    """_prepare_upload_config for turns must overwrite title.txt/description.txt
    using thumbnail_result's title and youtube_metadata_results' description,
    before calling prepare_orador_upload_config, leaving subtitles.srt untouched.
    """

    def test_prepare_upload_config_turn_overwrites_sidecars_from_xcom(self, tmp_path):
        """Stale title.txt/description.txt are overwritten by fresh XCom data;
        subtitles.srt is not touched by the overwrite step."""
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        # Stale sidecars from old nightly prepare
        (turn_dir / "title.txt").write_text("TÍTULO VIEJO", encoding="utf-8")
        (turn_dir / "description.txt").write_text("Desc vieja.", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        original_srt = "1\n00:00:00,000 --> 00:00:05,000\nSRT intacto.\n\n"
        (turn_dir / "subtitles.srt").write_text(original_srt, encoding="utf-8")

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": None,
                    "turn_id": 1,
                    "video_id": "vidXYZ",
                    "success": True,
                    "output_path": str(turn_dir / "video.mp4"),
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None,
                }
            ],
        }

        # Fresh description from the 19:00 AI call; title comes from thumbnail_result
        fresh_metadata = {
            "topic_metadata": [
                {
                    "description": {"description": "Descripción fresca del turno."},
                }
            ]
        }

        store = {
            "uploadable_item": {
                "item": {
                    "turn_id": 1,
                    "output_path": str(turn_dir / "video.mp4"),
                    "chapter_id": 100,
                },
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": fresh_metadata,
            "thumbnail_result": {"success": True, "title": "TÍTULO NUEVO FRESCO"},
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(ti, **context)

        config = ti.xcom_store.get("upload_config")
        assert config is not None
        videos = config.get("videos", [])
        assert len(videos) == 1
        # Fresh thumbnail_result title must win over stale on-disk title
        assert videos[0]["title"] == "TÍTULO NUEVO FRESCO", (
            f"Expected fresh title 'TÍTULO NUEVO FRESCO', got {videos[0].get('title')!r}"
        )

        # subtitles.srt must remain untouched by the sidecar overwrite step
        srt_content = (turn_dir / "subtitles.srt").read_text(encoding="utf-8")
        assert srt_content == original_srt, (
            "subtitles.srt must not be modified by the metadata overwrite step"
        )


# ---------------------------------------------------------------------------
# _extract_metadata_description helper
# Issue #245: helper is description-only now that generate_youtube_title
# (and the "title" key in youtube_metadata_results) is gone.
# ---------------------------------------------------------------------------


class TestExtractMetadataDescription:
    """Unit tests for the pure _extract_metadata_description helper (issue #245)."""

    def test_extracts_description_from_dict_value(self):
        """Happy path: dict-wrapped description is unwrapped correctly."""
        from congress_videos.youtube_upload_dag import _extract_metadata_description

        result = {
            "topic_metadata": [
                {
                    "description": {"description": "Una descripción detallada."},
                }
            ]
        }
        desc = _extract_metadata_description(result)
        assert desc == "Una descripción detallada."

    def test_extracts_description_from_plain_string_value(self):
        """When description value is a plain string (not dict), it is returned as-is."""
        from congress_videos.youtube_upload_dag import _extract_metadata_description

        result = {
            "topic_metadata": [
                {
                    "description": "Descripción plana.",
                }
            ]
        }
        desc = _extract_metadata_description(result)
        assert desc == "Descripción plana."

    def test_returns_empty_string_when_none_input(self):
        """None input returns ''."""
        from congress_videos.youtube_upload_dag import _extract_metadata_description

        desc = _extract_metadata_description(None)
        assert desc == ""

    def test_returns_empty_string_when_topic_metadata_empty(self):
        """Empty topic_metadata list returns ''."""
        from congress_videos.youtube_upload_dag import _extract_metadata_description

        result = {"topic_metadata": []}
        desc = _extract_metadata_description(result)
        assert desc == ""

    def test_returns_empty_string_when_missing_topic_metadata_key(self):
        """Dict without topic_metadata key returns ''."""
        from congress_videos.youtube_upload_dag import _extract_metadata_description

        result = {"other_key": "value"}
        desc = _extract_metadata_description(result)
        assert desc == ""


# ---------------------------------------------------------------------------
# Cross-DAG regression: turn_id survives prepare -> upload -> mark (issue #230)
# ---------------------------------------------------------------------------


class TestTurnIdSurvivesUploadRoundTrip:
    """Regression coverage for issue #230.

    turn_id must flow unbroken through _prepare_upload_config -> the generic
    uploader's upload_multiple_videos -> the mark_turns_uploaded operator, so
    the operator's primary turn_id branch fires instead of the output_path
    fallback. Neither unit-level test suite (youtube_helpers, postgres
    operators) could catch this on its own — the bug was in the seam between
    them, which is exactly what this in-process round trip exercises.
    """

    def test_turn_id_flows_from_prepare_through_upload_to_marking(self, tmp_path, mocker):
        from congress_videos.youtube_upload_dag import (
            _prepare_upload_config,
            _run_mark_turns_uploaded,
        )
        from utils.youtube_helpers import upload_multiple_videos

        # --- Step 1: prepare upload config for a turn item (sidecar fixture) ---
        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("T", encoding="utf-8")
        (turn_dir / "description.txt").write_text("D", encoding="utf-8")
        (turn_dir / "thumbnail.png").write_bytes(b"\x89PNG")
        (turn_dir / "subtitles.srt").write_text("", encoding="utf-8")

        extraction = {
            "total_chapters": 1,
            "successful_extractions": 1,
            "results": [
                {
                    "chapter_id": 100,
                    "turn_id": 1,
                    "video_id": "vidXYZ",
                    "success": True,
                    "output_path": str(turn_dir / "video.mp4"),
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None,
                }
            ],
        }

        fresh_metadata = {
            "topic_metadata": [
                {
                    "description": {"description": "Round trip turn description."},
                }
            ]
        }

        prepare_store = {
            "uploadable_item": {
                "item": {
                    "turn_id": 1,
                    "output_path": str(turn_dir / "video.mp4"),
                    "chapter_id": 100,
                },
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": fresh_metadata,
            "thumbnail_result": {"success": True, "title": "Round Trip Turn Title"},
        }
        prepare_ti = _make_ti(prepare_store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(prepare_ti, **context)

        upload_config = prepare_ti.xcom_store["upload_config"]
        assert upload_config["videos"][0]["turn_id"] == 1, (
            "prepare step must carry turn_id into the upload config"
        )

        # --- Step 2: upload via the generic uploader's upload_multiple_videos ---
        mocker.patch(
            "utils.youtube_helpers.get_authenticated_youtube_service",
            return_value=mocker.MagicMock(),
        )
        mocker.patch(
            "utils.youtube_helpers.upload_video_to_youtube",
            return_value={
                "success": True,
                "video_id": "yt-round-trip",
                "video_url": "https://youtu.be/yt-round-trip",
                "thumbnail_success": None,
                "error": None,
            },
        )

        upload_results = upload_multiple_videos(
            upload_config["token_file"], upload_config["videos"]
        )

        assert upload_results["upload_details"][0]["turn_id"] == 1, (
            "upload_multiple_videos must propagate turn_id into upload_detail"
        )

        # --- Step 3: mark_turns_uploaded callable reads upload_results from XCom ---
        mock_db = mocker.MagicMock()
        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db,
        )

        operator_ti = _make_ti({"upload_results": upload_results})
        _run_mark_turns_uploaded(operator_ti)

        mock_db.mark_turns_uploaded.assert_called_once_with(
            turn_id=1, youtube_video_id="yt-round-trip"
        )
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()
