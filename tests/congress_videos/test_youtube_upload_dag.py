"""Tests for congress_youtube_chapter_uploader DAG (congress_videos.youtube_upload_dag)."""

from __future__ import annotations

import logging
import os
import re
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DAGS_DOC = REPO_ROOT / "docs" / "DAGS.md"
DAG_ID = "congress_youtube_chapter_uploader"


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

    def test_dag_has_fifteen_tasks(self):
        """DAG must have 16 tasks: the prior 15 plus apply_intro_overlay (issue #558)."""
        from congress_videos.youtube_upload_dag import dag

        assert len(dag.tasks) == 16

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
        # Issue #512
        assert "verify_final_copy" in task_ids
        # Issue #558
        assert "apply_intro_overlay" in task_ids
        # Legacy Pillow tasks must be gone
        assert "generate_thumbnail_text" not in task_ids
        assert "generate_thumbnails" not in task_ids

    def test_verify_final_copy_between_prepare_upload_config_and_trigger_upload(self):
        """t6b sits directly between t6 (prepare_upload_config) and t7 (trigger_youtube_upload)."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        prepare = tasks_by_id["prepare_upload_config"]
        verify = tasks_by_id["verify_final_copy"]
        trigger = tasks_by_id["trigger_youtube_upload"]

        assert verify.task_id in {t.task_id for t in prepare.downstream_list}
        assert trigger.task_id in {t.task_id for t in verify.downstream_list}

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
        """extract_chapter_videos must precede prepare_upload_config (issue #558:
        apply_intro_overlay (t5b) now sits directly between them, so the
        relationship is ancestor, not direct-upstream — see
        TestApplyIntroOverlayWiring for the direct t5 > t5b > t6 chain)."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        extract = tasks_by_id["extract_chapter_videos"]
        upload_config = tasks_by_id["prepare_upload_config"]

        ancestor_ids = upload_config.get_flat_relative_ids(upstream=True)
        assert extract.task_id in ancestor_ids

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

        upload_details = [{"youtube_video_id": "abc", "chapter_id": 1, "thumbnail_success": None}]
        assert _unpublished_thumbnail_labels(upload_details) == []

    def test_true_thumbnail_success_is_not_a_failure(self):
        from congress_videos.youtube_upload_dag import _unpublished_thumbnail_labels

        upload_details = [{"youtube_video_id": "abc", "chapter_id": 1, "thumbnail_success": True}]
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
        assert problems == ["turn_upload_updates XCom missing after mark_turns_uploaded succeeded"]

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
            "details": [{"turn_id": 3, "status": "skipped", "reason": "upload_failed_or_missing_fields"}],
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
# _copy_verification_problems (issue #512)
# ---------------------------------------------------------------------------


class TestCopyVerificationProblems:
    """Shaped like _turn_marking_problems: finished sentences appended to the
    _check_upload_failures accumulator (design.md D7)."""

    def _clean_payload(self, **overrides) -> dict:
        payload = {
            "verdict": "pass",
            "findings": [],
            "corrected_applied": False,
            "persisted": True,
            "content_version": "v1",
        }
        payload.update(overrides)
        return payload

    def test_none_payload_reports_missing_xcom(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        assert _copy_verification_problems(None) == [
            "copy_verification XCom missing after prepare_upload_config succeeded"
        ]

    def test_clean_pass_payload_returns_empty_list(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        assert _copy_verification_problems(self._clean_payload()) == []

    def test_successful_correction_returns_empty_list(self):
        """A silently auto-corrected typo is a success story, not a finding."""
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="pass",
            findings=[{"field": "title", "category": "spelling", "severity": "low"}],
            corrected_applied=True,
        )
        assert _copy_verification_problems(payload) == []

    def test_inconclusive_verdict_is_a_finding(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(verdict="inconclusive", persisted=False)
        problems = _copy_verification_problems(payload)
        assert len(problems) == 1
        assert "inconclusive" in problems[0].lower()

    def test_description_reject_is_a_finding(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="reject",
            findings=[{"field": "description", "category": "unsupported_claim", "severity": "high"}],
        )
        problems = _copy_verification_problems(payload)
        assert len(problems) == 2  # reject + the same finding also flags as a discarded correction
        assert any("reject" in p.lower() and "description" in p for p in problems)

    def test_discarded_unsupported_correction_is_a_finding(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="correctable",
            findings=[
                {
                    "field": "title",
                    "category": "unsupported_claim",
                    "severity": "high",
                    "detail": "Correction discarded: not derivable from the supplied evidence.",
                }
            ],
            corrected_applied=False,
        )
        problems = _copy_verification_problems(payload)
        assert len(problems) == 1
        assert "unsupported" in problems[0].lower()

    def test_persistence_skip_is_a_finding(self):
        """Stale-copy guard skip (design.md D3) on an otherwise ok verdict."""
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(verdict="pass", persisted=False)
        problems = _copy_verification_problems(payload)
        assert len(problems) == 1
        assert "audit write" in problems[0].lower() or "stale" in problems[0].lower()

    def test_reject_and_persistence_skip_both_fire(self):
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="reject",
            findings=[{"field": "description", "category": "unsupported_claim"}],
            persisted=False,
        )
        problems = _copy_verification_problems(payload)
        assert len(problems) == 3

    def test_copy_verification_problems_reports_unlanded_thumbnail_regen(self):
        """3.11 (issue #545, design.md D4): a thumbnail_text finding that did
        NOT land a regeneration is an operator-facing, non-blocking signal."""
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
            thumbnail_regen_landed=False,
        )
        problems = _copy_verification_problems(payload)
        assert any("thumbnail" in p.lower() and "regeneration" in p.lower() for p in problems)

    def test_copy_verification_problems_landed_regen_is_not_a_finding(self):
        """A landed regeneration is a success story — no extra finding."""
        from congress_videos.youtube_upload_dag import _copy_verification_problems

        payload = self._clean_payload(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
            thumbnail_regen_landed=True,
        )
        problems = _copy_verification_problems(payload)
        assert not any("regeneration did not land" in p for p in problems)


# ---------------------------------------------------------------------------
# _check_upload_failures
# ---------------------------------------------------------------------------


class TestCheckUploadFailures:
    def test_raises_on_recorded_failures(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 1, "failed_updates": 0}})
        with pytest.raises(Exception, match="Chapter upload failures"):
            _check_upload_failures(ti)

    def test_raises_on_failed_updates(self):
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 2}})
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
                "copy_verification": {
                    "verdict": "pass",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
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
                "copy_verification": {
                    "verdict": "pass",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
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
                "copy_verification": {
                    "verdict": "pass",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
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

        ti = _make_ti({"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0}})
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
                "copy_verification": {
                    "verdict": "pass",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
                },
            }
        )
        _check_upload_failures(ti)  # should not raise

    # -----------------------------------------------------------------------
    # Non-blocking copy-verification findings (issue #604)
    # -----------------------------------------------------------------------

    def _clean_chapter_and_turn_xcoms(self) -> dict:
        return {
            "chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0},
            "turn_upload_updates": {
                "updated_turns": 1,
                "failed_updates": 0,
                "details": [{"turn_id": 42, "status": "updated"}],
            },
        }

    @pytest.mark.parametrize(
        ("case_id", "copy_payload"),
        [
            (
                "inconclusive",
                {
                    "verdict": "inconclusive",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": False,
                    "content_version": "v1",
                },
            ),
            (
                "description_reject",
                {
                    "verdict": "reject",
                    "findings": [{"field": "description", "category": "unsupported_claim", "severity": "high"}],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
                },
            ),
            (
                "discarded_correction",
                {
                    "verdict": "correctable",
                    "findings": [
                        {
                            "field": "title",
                            "category": "unsupported_claim",
                            "severity": "high",
                            "detail": "Correction discarded: not derivable from the supplied evidence.",
                        }
                    ],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
                },
            ),
            (
                "audit_skip",
                {
                    "verdict": "pass",
                    "findings": [],
                    "corrected_applied": False,
                    "persisted": False,
                    "content_version": "v1",
                },
            ),
            (
                "unlanded_thumbnail_regen",
                {
                    "verdict": "reject",
                    "findings": [{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
                    "corrected_applied": False,
                    "persisted": True,
                    "content_version": "v1",
                    "thumbnail_regen_landed": False,
                },
            ),
        ],
    )
    def test_each_soft_copy_category_alone_does_not_raise(self, case_id, copy_payload):
        """3.7 (issue #604): each verifier-produced finding category is
        non-blocking on its own — no raise, and every finding lands on the
        dedicated `copy_verification_warnings` XCom."""
        from congress_videos.youtube_upload_dag import _check_upload_failures, _copy_verification_problems

        xcoms = self._clean_chapter_and_turn_xcoms()
        xcoms["copy_verification"] = copy_payload
        ti = _make_ti(xcoms)

        _check_upload_failures(ti)  # should not raise

        expected = _copy_verification_problems(copy_payload)
        assert expected  # sanity: this category IS a finding
        assert ti.xcom_store["copy_verification_warnings"] == expected

    def test_soft_copy_findings_are_each_logged_at_warning(self, caplog):
        """3.8 (issue #604): each non-blocking finding gets its own WARNING log."""
        from congress_videos.youtube_upload_dag import _check_upload_failures, _copy_verification_problems

        copy_payload = {
            "verdict": "reject",
            "findings": [{"field": "description", "category": "unsupported_claim", "severity": "high"}],
            "corrected_applied": False,
            "persisted": True,
            "content_version": "v1",
        }
        xcoms = self._clean_chapter_and_turn_xcoms()
        xcoms["copy_verification"] = copy_payload
        ti = _make_ti(xcoms)
        expected = _copy_verification_problems(copy_payload)
        assert len(expected) == 2  # reject + the same finding also flags as a discarded correction

        with caplog.at_level(logging.WARNING):
            _check_upload_failures(ti)  # should not raise

        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warning_records) == 2
        for finding in expected:
            assert any(finding in r.message for r in warning_records)

    def test_blocking_and_soft_findings_raise_with_blocking_text_only(self, caplog):
        """3.9 (issue #604): a blocking finding raises with ONLY the blocking
        text; the soft finding is still logged and pushed to XCom."""
        from congress_videos.youtube_upload_dag import _check_upload_failures, _copy_verification_problems

        copy_payload = {
            "verdict": "correctable",
            "findings": [
                {
                    "field": "title",
                    "category": "unsupported_claim",
                    "severity": "high",
                    "detail": "Correction discarded: not derivable from the supplied evidence.",
                }
            ],
            "corrected_applied": False,
            "persisted": True,
            "content_version": "v1",
        }
        ti = _make_ti(
            {
                "chapter_upload_updates": {
                    "recorded_failures": 1,
                    "failed_updates": 0,
                    "details": [{"chapter_id": 5, "status": "failure_recorded"}],
                },
                "turn_upload_updates": {
                    "updated_turns": 1,
                    "failed_updates": 0,
                    "details": [{"turn_id": 42, "status": "updated"}],
                },
                "copy_verification": copy_payload,
            }
        )
        expected = _copy_verification_problems(copy_payload)

        with caplog.at_level(logging.WARNING), pytest.raises(Exception) as exc_info:
            _check_upload_failures(ti)

        message = str(exc_info.value)
        assert "Chapter upload failures" in message
        assert "Final-copy verification" not in message
        assert ti.xcom_store["copy_verification_warnings"] == expected
        warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(expected[0] in r.message for r in warning_records)

    def test_missing_copy_verification_xcom_still_raises(self):
        """3.10 (issue #604): a missing `copy_verification` payload stays
        blocking even alone, and still pushes an empty warnings list."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        ti = _make_ti(self._clean_chapter_and_turn_xcoms())  # copy_verification absent

        with pytest.raises(Exception, match="copy_verification XCom missing after prepare_upload_config succeeded"):
            _check_upload_failures(ti)

        assert ti.xcom_store["copy_verification_warnings"] == []

    def test_clean_run_pushes_empty_warning_list(self):
        """3.11 (issue #604): a fully clean run does not raise and pushes an
        empty `copy_verification_warnings` list."""
        from congress_videos.youtube_upload_dag import _check_upload_failures

        xcoms = self._clean_chapter_and_turn_xcoms()
        xcoms["copy_verification"] = {
            "verdict": "pass",
            "findings": [],
            "corrected_applied": False,
            "persisted": True,
            "content_version": "v1",
        }
        ti = _make_ti(xcoms)

        _check_upload_failures(ti)  # should not raise

        assert ti.xcom_store["copy_verification_warnings"] == []


# ---------------------------------------------------------------------------
# trigger_upload_with_config (t7)
# ---------------------------------------------------------------------------


class TestTriggerUploadWithConfig:
    def test_no_raise_on_child_failure_and_pushes_fallback(self, mocker):
        from congress_videos.youtube_upload_dag import trigger_upload_with_config

        mock_run = MagicMock()
        mock_run.run_id = "failed_run_001"
        mock_run.state = "failed"
        mock_run.execution_date = "2026-07-28T00:00:00+00:00"
        mock_run.refresh_from_db = MagicMock()

        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=mock_run)
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
# _verify_final_copy (t6b, issue #512)
# ---------------------------------------------------------------------------


def _make_upload_config(*, title="Título original", description="Descripción original", chapter_id=100, turn_id=1):
    return {
        "token_file": "/tokens/x.pickle",
        "videos": [
            {
                "video_file": "/data/turn1/video.mp4",
                "title": title,
                "description": description,
                "thumbnail_file": "/data/turn1/thumbnail.png",
                "chapter_id": chapter_id,
                "turn_id": turn_id,
                "video_id": "vidXYZ",
            }
        ],
    }


class TestVerifyFinalCopy:
    """Long-form seam wiring: verify_final_copy() runs on the LAST mutable
    representation (upload_config["videos"][0]), never the upstream
    thumbnail_result/_extract_metadata_description XComs (design.md)."""

    def _patch_db(self, mocker, chapter_row=None, speaker_row=None, thumbnail_row=None):
        mock_db = MagicMock()
        mock_db.get_chapter_metadata.return_value = chapter_row
        mock_db.get_turn_speaker_slug.return_value = speaker_row
        mock_db.get_chosen_thumbnail.return_value = thumbnail_row
        mocker.patch("congress_videos.modules.database.CongressionalVideoDB", return_value=mock_db)
        return mock_db

    def _patch_matching_content_version(self, mocker, version="matching-hash"):
        """Patch compute_content_version so the stale-copy guard's recompute
        always matches whatever the mocked verdict declares — tests that are
        not exercising the guard itself (3.6) shouldn't have to replicate the
        real evidence bundle by hand."""
        mocker.patch("congress_videos.modules.final_copy_verification.compute_content_version", return_value=version)

    def _make_verdict(self, **overrides):
        from congress_videos.modules.final_copy_verification import CopyFinding, CopyVerdict

        findings = overrides.pop("findings", ())
        parsed_findings = tuple(CopyFinding(**f) if isinstance(f, dict) else f for f in findings)
        defaults = {
            "ok": True,
            "verdict": "pass",
            "findings": parsed_findings,
            "title": "Título original",
            "description": "Descripción original",
            "correction_applied": False,
            "content_version": "",
            "rounds": 1,
        }
        defaults.update(overrides)
        return CopyVerdict(**defaults)

    def test_title_reject_raises_value_error(self, mocker):
        """3.2 — title reject (no correction) raises; description/thumbnail-only
        rejects never do (locked hard-rejection asymmetry)."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        self._patch_db(mocker)
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "title", "category": "person_name", "severity": "high"}],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        ti = _make_ti({"upload_config": _make_upload_config()})
        with pytest.raises(ValueError, match="rejected the title"):
            _verify_final_copy(ti)

    def test_description_reject_persists_and_does_not_raise(self, mocker):
        """3.3 — description reject persists the audit row, publishes original,
        surfaces via the copy_verification XCom (accumulator reads it), never raises."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker)
        self._patch_matching_content_version(mocker, version="hash-1")
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "description", "category": "unsupported_claim", "severity": "high"}],
            content_version="hash-1",
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        ti = _make_ti({"upload_config": _make_upload_config()})
        _verify_final_copy(ti)  # should not raise

        mock_db.record_copy_verification_turn.assert_called_once()
        payload = ti.xcom_store["copy_verification"]
        assert payload["verdict"] == "reject"
        assert payload["persisted"] is True

    def test_inconclusive_verdict_publishes_unchanged_and_writes_nothing(self, mocker):
        """3.4 — inconclusive (verifier failure/timeout/malformed): publish
        upload_config unchanged, no DB write, surfaces via accumulator."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker)
        verdict_fn = mocker.patch(
            "congress_videos.modules.final_copy_verification.verify_final_copy",
            return_value=self._make_verdict(
                ok=False, verdict="", title="Título original", description="Descripción original"
            ),
        )

        config = _make_upload_config()
        ti = _make_ti({"upload_config": config})
        _verify_final_copy(ti)  # should not raise

        verdict_fn.assert_called_once()
        mock_db.record_copy_verification_turn.assert_not_called()
        assert ti.xcom_store["upload_config"]["videos"][0]["title"] == "Título original"
        assert ti.xcom_store["copy_verification"]["verdict"] == "inconclusive"
        assert ti.xcom_store["copy_verification"]["persisted"] is False

    def test_correction_patches_config_and_rewrites_sidecars(self, mocker):
        """3.5 — correctable+contained correction patches upload_config AND
        rewrites the sidecars via _write_orador_sidecars."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        self._patch_db(mocker)
        self._patch_matching_content_version(mocker, version="hash-2")
        verdict = self._make_verdict(
            verdict="pass",
            title="Título corregido",
            description="Descripción corregida",
            correction_applied=True,
            content_version="hash-2",
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)
        mock_write = mocker.patch("congress_videos.modules.youtube.youtube_upload._write_orador_sidecars")

        config = _make_upload_config()
        ti = _make_ti({"upload_config": config})
        _verify_final_copy(ti)

        mock_write.assert_called_once_with("/data/turn1/video.mp4", "Título corregido", "Descripción corregida")
        pushed_config = ti.xcom_store["upload_config"]
        assert pushed_config["videos"][0]["title"] == "Título corregido"
        assert pushed_config["videos"][0]["description"] == "Descripción corregida"
        assert ti.xcom_store["copy_verification"]["corrected_applied"] is True

    def test_stale_copy_guard_skips_write_and_emits_finding(self, mocker):
        """3.6 — recomputed content_version mismatch (about-to-publish values)
        skips the write entirely; never persists a correction against copy
        that changed after verification."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker)
        verdict = self._make_verdict(verdict="pass", content_version="stale-hash-does-not-match-anything")
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        ti = _make_ti({"upload_config": _make_upload_config()})
        _verify_final_copy(ti)  # should not raise

        mock_db.record_copy_verification_turn.assert_not_called()
        assert ti.xcom_store["copy_verification"]["persisted"] is False

    def test_verifies_upload_config_values_not_thumbnail_result_xcom(self, mocker):
        """3.9 — verification reads upload_config["videos"][0], the last
        mutable (sidecar-round-tripped) representation, never thumbnail_result
        or the pre-strip _extract_metadata_description XCom."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        self._patch_db(mocker)
        verify_fn = mocker.patch(
            "congress_videos.modules.final_copy_verification.verify_final_copy",
            return_value=self._make_verdict(),
        )

        config = _make_upload_config(title="Config title", description="Config description")
        ti = _make_ti(
            {
                "upload_config": config,
                # Deliberately different: proves the upstream XComs are never read here.
                "thumbnail_result": {"success": True, "title": "Stale thumbnail title"},
                "youtube_metadata_results": {"topic_metadata": [{"description": {"description": "Stale desc"}}]},
            }
        )
        _verify_final_copy(ti)

        _, kwargs = verify_fn.call_args
        assert kwargs["title"] == "Config title"
        assert kwargs["description"] == "Config description"

    def test_no_upload_config_skips_verification_without_raising(self, mocker):
        """No videos to verify (upstream skip/failure) — a no-op, not an anomaly
        this task should flag; _check_upload_failures already covers upstream skips."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        self._patch_db(mocker)
        verify_fn = mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy")

        ti = _make_ti({"upload_config": None})
        _verify_final_copy(ti)  # should not raise

        verify_fn.assert_not_called()
        assert "copy_verification" not in ti.xcom_store

    # -----------------------------------------------------------------
    # Issue #545: bounded, non-blocking thumbnail-text regeneration
    # branch, wired strictly after the title-reject raise above.
    # -----------------------------------------------------------------

    def test_verify_final_copy_thumbnail_text_finding_triggers_one_claim_and_trigger(self, mocker):
        """3.1 — a thumbnail_text finding triggers exactly one claim and,
        once claimed, exactly one call into _regenerate_flagged_thumbnail —
        never a loop, never more than one attempt per t6b execution. A
        landed regeneration also swaps thumbnail_file and pushes the
        mutated upload_config."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker, thumbnail_row={"art_direction_brief": {"text": "old brief"}})
        mock_db.claim_thumbnail_text_regeneration.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }
        regen = mocker.patch(
            "congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail",
            return_value=_regen_valid_result(output_path="/data/turn1/thumbnail.png"),
        )
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        thumbnail_config = _regen_thumbnail_config()
        config = _make_upload_config()
        ti = _make_ti({"upload_config": config, "thumbnail_config": thumbnail_config})

        _verify_final_copy(ti, run_id="run_1")

        mock_db.claim_thumbnail_text_regeneration.assert_called_once_with(
            "/data/turn1/video.mp4", prior_brief={"text": "old brief"}
        )
        regen.assert_called_once_with(
            thumbnail_config, "/data/turn1/video.mp4", {"text": "old brief"}, "run_1", db=mock_db
        )
        ti.xcom_push.assert_any_call(key="upload_config", value=config)
        assert config["videos"][0]["thumbnail_file"] == "/data/turn1/thumbnail.png"

    def test_verify_final_copy_no_thumbnail_text_finding_zero_claims(self, mocker):
        """3.2 — no thumbnail_text finding, no regeneration. Mutation check
        (manually verified): temporarily always calling claim makes this
        test fail."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker)
        self._patch_matching_content_version(mocker)
        verdict = self._make_verdict(verdict="pass", findings=[])
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        ti = _make_ti({"upload_config": _make_upload_config(), "thumbnail_config": _regen_thumbnail_config()})
        _verify_final_copy(ti)

        mock_db.claim_thumbnail_text_regeneration.assert_not_called()

    def test_verify_final_copy_hoisted_xcom_push_fires_without_correction(self, mocker):
        """3.4 — NON-NEGOTIABLE regression guard: the hoisted push must fire
        for a landed regeneration even when NO title/description correction
        was applied. Before the hoist, this push lived only inside
        `if verdict.correction_applied:` and would silently drop this
        exact case."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker, thumbnail_row={"art_direction_brief": {"text": "old brief"}})
        mock_db.claim_thumbnail_text_regeneration.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }
        mocker.patch(
            "congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail",
            return_value=_regen_valid_result(output_path="/data/turn1/thumbnail.png"),
        )
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
            correction_applied=False,
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        config = _make_upload_config()
        ti = _make_ti({"upload_config": config, "thumbnail_config": _regen_thumbnail_config()})

        _verify_final_copy(ti)

        ti.xcom_push.assert_any_call(key="upload_config", value=config)
        assert config["videos"][0]["thumbnail_file"] == "/data/turn1/thumbnail.png"

    def test_verify_final_copy_sibling_isolation_by_output_path(self, mocker):
        """3.7 (design.md D6) — the triggered child conf["output_path"] is
        turn A's own video_file, never the shared chapter_id and never a
        sibling turn B's path. Exercises the real (unmocked)
        _regenerate_flagged_thumbnail so the actual conf sent to
        trigger_dag_api is inspectable end to end."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker, thumbnail_row={"art_direction_brief": {"text": "old brief"}})
        mock_db.claim_thumbnail_text_regeneration.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }
        trigger = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            return_value=_regen_dag_run(state="success"),
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value=_regen_valid_result(output_path="/data/turn-A/thumbnail.png"),
        )
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        # Turn A's own config — a sibling turn B would share chapter_id=100
        # but have a distinct video_file/output_path, never referenced here.
        config = _make_upload_config(chapter_id=100)
        config["videos"][0]["video_file"] = "/data/turn-A/video.mp4"
        thumbnail_config = _regen_thumbnail_config(chapter_id=100)
        ti = _make_ti({"upload_config": config, "thumbnail_config": thumbnail_config})

        _verify_final_copy(ti, run_id="run_1")

        _, kwargs = trigger.call_args
        assert kwargs["conf"]["output_path"] == "/data/turn-A/video.mp4"
        assert kwargs["conf"]["output_path"] != "/data/turn-B/video.mp4"
        assert kwargs["conf"]["output_path"] != str(100)

    @pytest.mark.parametrize(
        "mode",
        ["timeout", "trigger_failed", "child_failed", "invalid_result", "not_claimed", "claim_exception"],
    )
    def test_verify_final_copy_every_failure_mode_returns_none_never_raises(self, mocker, mode):
        """3.8 — No Code Path May Block Or Indefinitely Delay Publication:
        every regeneration failure mode still returns None from t6b, never
        pushes upload_config (no correction landed, no regen landed), and
        never raises. Mutation check (manually verified): letting one
        branch re-raise makes this test fail for that parametrized mode."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker, thumbnail_row={"art_direction_brief": {"text": "old brief"}})
        if mode == "not_claimed":
            mock_db.claim_thumbnail_text_regeneration.return_value = None
        elif mode == "claim_exception":
            mock_db.claim_thumbnail_text_regeneration.side_effect = RuntimeError("db is down")
        else:
            mock_db.claim_thumbnail_text_regeneration.return_value = {
                "thumbnail_regen_attempts": 1,
                "thumbnail_regen_exhausted": False,
            }
            mocker.patch(
                "congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail",
                return_value={"outcome": mode, "error": "boom"},
            )

        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        config = _make_upload_config()
        ti = _make_ti({"upload_config": config, "thumbnail_config": _regen_thumbnail_config()})

        try:
            result = _verify_final_copy(ti)
        except Exception as exc:  # pragma: no cover - assertion below is the real check
            pytest.fail(f"_verify_final_copy raised {exc!r} instead of returning None")

        assert result is None
        upload_config_pushes = [c for c in ti.xcom_push.call_args_list if c.kwargs.get("key") == "upload_config"]
        assert upload_config_pushes == []

    def test_verify_final_copy_title_reject_still_raises_before_any_claim(self, mocker):
        """3.9 — Title hard-rejection remains the only blocking path: a
        verdict carrying BOTH a thumbnail_text finding and a title reject
        still raises at the existing line, and
        claim_thumbnail_text_regeneration is never called."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker)
        verdict = self._make_verdict(
            verdict="reject",
            findings=[
                {"field": "title", "category": "person_name", "severity": "high"},
                {"field": "thumbnail_text", "category": "person_name", "severity": "high"},
            ],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        ti = _make_ti({"upload_config": _make_upload_config(), "thumbnail_config": _regen_thumbnail_config()})
        with pytest.raises(ValueError, match="rejected the title"):
            _verify_final_copy(ti)

        mock_db.claim_thumbnail_text_regeneration.assert_not_called()

    def test_verify_final_copy_chapter_item_no_row_intentionally_publishes_as_is(self, mocker):
        """Non-negotiable #4 (issue #545): chapter items have no
        speaker_turn_videos row, so the claim naturally returns None and
        the chapter publishes as-is. This is INTENTIONAL — not a bug for a
        future contributor to "fix" by special-casing item_type here."""
        from congress_videos.youtube_upload_dag import _verify_final_copy

        mock_db = self._patch_db(mocker, thumbnail_row={"art_direction_brief": {"text": "old brief"}})
        mock_db.claim_thumbnail_text_regeneration.return_value = None  # no speaker_turn_videos row
        regen = mocker.patch("congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail")
        verdict = self._make_verdict(
            verdict="reject",
            findings=[{"field": "thumbnail_text", "category": "person_name", "severity": "high"}],
        )
        mocker.patch("congress_videos.modules.final_copy_verification.verify_final_copy", return_value=verdict)

        config = _make_upload_config()
        config["videos"][0]["turn_id"] = None  # chapter item — no turn_id
        config["videos"][0]["video_file"] = "/data/chapter100/video.mp4"
        ti = _make_ti({"upload_config": config, "thumbnail_config": _regen_thumbnail_config()})

        result = _verify_final_copy(ti)

        assert result is None
        mock_db.claim_thumbnail_text_regeneration.assert_called_once_with(
            "/data/chapter100/video.mp4", prior_brief={"text": "old brief"}
        )
        regen.assert_not_called()


def _lookup_stub(roster: dict):
    """Stub lookup_participant_by_slug: slug -> participant dict | None
    (design.md D2 — mirrors test_reap_uploader_dag.py's `_lookup_stub`
    shape; not imported across test modules by design)."""

    def _fn(slug):
        return roster.get(slug)

    return _fn


class TestCopyVerificationEvidenceNameSplit:
    """Issue #544: `_copy_verification_evidence` must keep the raw roster
    `display_name` and the canonical `short_name` (#511) in two distinct,
    never-conflated fields — for the resolved speaker and for every
    `mencionados` entry (design.md D1/D2)."""

    def test_resolvable_slug_splits_raw_and_canonical(self, mocker):
        from congress_videos.youtube_upload_dag import _copy_verification_evidence

        db = MagicMock()
        db.get_chapter_metadata.return_value = {"mentioned_participant_slugs": None}
        db.get_turn_speaker_slug.return_value = {"resolved_participant_slug": "known-slug"}

        roster = {"known-slug": {"display_name": "RAW Foo"}}
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            side_effect=_lookup_stub(roster),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.canonical_display_name",
            return_value="CANON-X",
        )

        evidence = _copy_verification_evidence(db, chapter_id=1, turn_id=2)

        assert evidence["speaker"]["display_name"] == "RAW Foo"
        assert evidence["speaker"]["short_name"] == "CANON-X"
        assert evidence["speaker"]["display_name"] != evidence["speaker"]["short_name"]

    def test_unmapped_slug_keeps_raw_and_nulls_canonical(self, mocker):
        from congress_videos.youtube_upload_dag import _copy_verification_evidence

        db = MagicMock()
        db.get_chapter_metadata.return_value = {"mentioned_participant_slugs": None}
        db.get_turn_speaker_slug.return_value = {"resolved_participant_slug": "known-slug"}

        roster = {"known-slug": {"display_name": "RAW Foo"}}
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            side_effect=_lookup_stub(roster),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.canonical_display_name",
            return_value=None,
        )

        evidence = _copy_verification_evidence(db, chapter_id=1, turn_id=2)

        assert evidence["speaker"]["short_name"] is None
        assert evidence["speaker"]["display_name"] == "RAW Foo"

    def test_mentioned_entries_split_raw_and_canonical(self, mocker):
        from congress_videos.youtube_upload_dag import _copy_verification_evidence

        db = MagicMock()
        db.get_chapter_metadata.return_value = {
            "mentioned_participant_slugs": ["mentioned-a", "mentioned-b"],
        }
        db.get_turn_speaker_slug.return_value = {"resolved_participant_slug": "speaker-slug"}

        roster = {
            "speaker-slug": {"display_name": "RAW Speaker"},
            "mentioned-a": {"display_name": "RAW Mentioned A"},
            "mentioned-b": {"display_name": "RAW Mentioned B"},
        }
        canonical = {
            "speaker-slug": "CANON Speaker",
            "mentioned-a": "CANON Mentioned A",
            "mentioned-b": "CANON Mentioned B",
        }
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            side_effect=_lookup_stub(roster),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.canonical_display_name",
            side_effect=lambda slug: canonical.get(slug),
        )

        evidence = _copy_verification_evidence(db, chapter_id=1, turn_id=2)

        by_slug = {entry["slug"]: entry for entry in evidence["mencionados"]}
        assert by_slug["mentioned-a"]["display_name"] == "RAW Mentioned A"
        assert by_slug["mentioned-a"]["short_name"] == "CANON Mentioned A"
        assert by_slug["mentioned-b"]["display_name"] == "RAW Mentioned B"
        assert by_slug["mentioned-b"]["short_name"] == "CANON Mentioned B"
        assert by_slug["mentioned-a"]["display_name"] != evidence["speaker"]["display_name"]
        assert by_slug["mentioned-b"]["display_name"] != evidence["speaker"]["display_name"]


# ---------------------------------------------------------------------------
# should_upload function (REQ-GATE-01)
# ---------------------------------------------------------------------------


def _make_context_for_should_upload(queue_size: int, hour: int, uploads_today: int = 0) -> dict:
    """Build a minimal Airflow context for should_upload tests."""
    from datetime import datetime
    from unittest.mock import MagicMock

    logical_date = datetime(2026, 7, 31, hour, 0, 0, tzinfo=UTC)

    ti = MagicMock(name="TaskInstance")
    ti.xcom_pull.return_value = {
        "queue_size": queue_size,
        "uploads_today": uploads_today,
    }

    return {"ti": ti, "logical_date": logical_date}


class TestShouldUpload:
    def test_only_scheduled_runs_consume_the_daily_quota(self):
        from congress_videos.youtube_upload_dag import _counts_toward_daily_quota

        scheduled = MagicMock(run_type="scheduled")
        manual = MagicMock(run_type="manual")

        assert _counts_toward_daily_quota(scheduled) is True
        assert _counts_toward_daily_quota(manual) is False

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

    def test_manual_run_bypasses_the_daily_cap(self):
        """A recovery is allowed even after a scheduled upload consumed the daily slot."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11, uploads_today=1)
        ctx["dag_run"] = MagicMock(run_type="manual")

        assert should_upload(**ctx) is True

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
        from datetime import datetime, timedelta

        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = datetime.now(UTC) - timedelta(hours=2)
        assert should_upload(**ctx) is False

    def test_stale_scheduled_run_returns_false(self):
        """An explicitly scheduled run with a stale data_interval_end is still dropped."""
        from datetime import datetime, timedelta

        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = datetime.now(UTC) - timedelta(hours=2)
        ctx["dag_run"] = MagicMock(run_type="scheduled")
        assert should_upload(**ctx) is False

    def test_stale_manual_run_is_not_dropped(self):
        """A manual run inherits the previous cron interval; the staleness guard must not reject it."""
        from datetime import datetime, timedelta

        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11, uploads_today=1)
        ctx["data_interval_end"] = datetime.now(UTC) - timedelta(hours=3)
        ctx["dag_run"] = MagicMock(run_type="manual")
        assert should_upload(**ctx) is True

    def test_fresh_run_proceeds_to_threshold(self):
        """data_interval_end ~1 min in the past, queue above threshold → True (threshold applies)."""
        from datetime import datetime, timedelta

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
        from datetime import datetime, timedelta
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
        result = trigger_upload_with_config(ti, params={"dry_run": True}, run_id="test_dry")

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

        trigger_mock = mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=mock_run)
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
        "key_speakers": key_speakers if key_speakers is not None else [{"name": "Ana García"}],
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

        with patch("congress_videos.youtube_upload_dag.resolve_chapter_speakers") as resolver:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        resolver.assert_not_called()

    def test_placeholder_only_speakers_skip_resolver(self):
        """Chapter whose only speaker mention is a placeholder skips the resolver call."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Desconocido"}], speakers=[])

        with patch("congress_videos.youtube_upload_dag.resolve_chapter_speakers") as resolver:
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
            patch("congress_videos.youtube_upload_dag.resolve_chapter_speakers") as resolver,
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
        trigger = mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
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

        trigger_thumbnail_generation(_make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG}), run_id="test_run")

        trigger.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            conf={"youtube_video_id": "42", **self.THUMBNAIL_CONFIG, "key_speakers": []},
            run_id="chapter_thumbnail_test_run",
        )

    def test_triggers_generic_dag_without_participant_slug(self, mocker) -> None:
        """A chapter without a resolved speaker still receives a generic thumbnail."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        trigger = mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
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

        trigger_thumbnail_generation(_make_ti({"thumbnail_config": config_without_speaker}), run_id="test_run")

        trigger.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            conf={"youtube_video_id": "42", **config_without_speaker, "key_speakers": []},
            run_id="chapter_thumbnail_test_run",
        )

    def test_retrieves_result_by_child_dag_and_exact_triggered_run_id(self, mocker) -> None:
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        child_run = self._successful_child_run()
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
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
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
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
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=None)
        ti = _make_ti({"thumbnail_config": self.THUMBNAIL_CONFIG})

        trigger_thumbnail_generation(ti, run_id="test_run")

        assert ti.xcom_store["thumbnail_result"] == {
            "chapter_id": 42,
            "success": False,
            "output_path": None,
            "title": None,
        }


# ---------------------------------------------------------------------------
# _regenerate_flagged_thumbnail (issue #545 — bounded thumbnail-text
# regeneration; wired into t6b via _claim_and_regenerate_thumbnail, PR3)
# ---------------------------------------------------------------------------


def _regen_dag_run(state="success", run_id="thumbnail_text_regen_test_run"):
    dag_run = MagicMock()
    dag_run.run_id = run_id
    dag_run.state = state
    return dag_run


def _regen_valid_result(output_path="/videos/turn-1/thumbnail.png"):
    return {
        "success": True,
        "output_path": output_path,
        "title": "Nuevo título",
        "title_generation_input": None,
    }


def _regen_thumbnail_config(**overrides) -> dict:
    """A complete thumbnail_config XCom — the same shape t4
    (trigger_thumbnail_generation) reads, and the same shape
    _prepare_thumbnail_config (t3) pushes for both turn and chapter items.
    All four scalar values required by generic_thumbnail_generator's own
    validate_input are present by default; tests exercising the guard
    override one to a falsy value."""
    config = {
        "chapter_id": 42,
        "debate_summary": "Debate summary",
        "session": "Sesión 1",
        "domain": "congreso",
        "slug": "some-slug",
        "key_speakers": ["Some Speaker"],
    }
    config.update(overrides)
    return config


class TestRegenerateFlaggedThumbnail:
    """Modeled on video_analytics_actions_dag.py::_poll_thumbnail_dag_run's
    BOUNDED loop shape (design.md D2) — NEVER trigger_thumbnail_generation's
    unbounded ``while True`` above, which is a pre-existing risk, not a
    template. The measured max (3989s) exceeds this helper's own 1000s
    bound, so the timeout path is routinely exercised in production, not an
    edge case."""

    def test_completes_within_bound_returns_regenerated_result(self, mocker):
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        dag_run = _regen_dag_run(state="running")
        # Settles on the 3rd poll — well under the 100-poll bound.
        states = iter(["running", "running", "success"])
        dag_run.refresh_from_db.side_effect = lambda: setattr(dag_run, "state", next(states))
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        sleep = mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        valid_result = _regen_valid_result()
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=valid_result)
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(),
            "/videos/turn-1/video.mp4",
            {"archetype": "closeup"},
            "run_1",
            db=mock_db,
        )

        assert result == valid_result
        assert sleep.call_count == 3
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="applied",
            error=None,
            regenerated_brief=valid_result,
        )

    def test_forwards_full_child_conf_with_output_path_and_prior_brief(self, mocker):
        """The child conf mirrors t4's own shape (youtube_video_id derived
        from chapter_id + the four required scalars + slug/key_speakers),
        PLUS previous_brief, PLUS an output_path always overridden to the
        triggering turn's own file (design.md D6 sibling isolation)."""
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        trigger = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            return_value=_regen_dag_run(state="success"),
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=_regen_valid_result())

        _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(output_path="/some/other/path.mp4"),
            "/videos/turn-1/video.mp4",
            {"archetype": "closeup"},
            "run_1",
            db=MagicMock(),
        )

        trigger.assert_called_once_with(
            dag_id="generic_thumbnail_generator",
            conf={
                "youtube_video_id": "42",
                "chapter_id": 42,
                "debate_summary": "Debate summary",
                "session": "Sesión 1",
                "domain": "congreso",
                "slug": "some-slug",
                "key_speakers": ["Some Speaker"],
                "previous_brief": {"archetype": "closeup"},
                # Overridden to the parameter, never thumbnail_config's own value.
                "output_path": "/videos/turn-1/video.mp4",
            },
            run_id="thumbnail_text_regen_run_1",
        )

    def test_missing_prior_brief_omits_previous_brief_key(self, mocker):
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        trigger = mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            return_value=_regen_dag_run(state="success"),
        )
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=_regen_valid_result())

        _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=MagicMock()
        )

        _, kwargs = trigger.call_args
        assert "previous_brief" not in kwargs["conf"]
        assert kwargs["conf"]["output_path"] == "/videos/turn-1/video.mp4"

    @pytest.mark.parametrize("missing_key", ["chapter_id", "debate_summary", "session", "domain"])
    def test_incomplete_thumbnail_config_never_triggers_records_trigger_failed(self, mocker, missing_key):
        """t4's own guard idiom (trigger_thumbnail_generation): any missing
        or empty required scalar means generic_thumbnail_generator's
        validate_input would reject the conf — so this function must never
        even call trigger_dag_api, and must record the non-attempt via the
        SAME 'trigger_failed' outcome as a real trigger exception."""
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        trigger = mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api")
        mock_db = MagicMock()
        incomplete_config = _regen_thumbnail_config(**{missing_key: None})

        result = _regenerate_flagged_thumbnail(incomplete_config, "/videos/turn-1/video.mp4", None, "run_1", db=mock_db)

        trigger.assert_not_called()
        assert result == {"outcome": "trigger_failed", "error": mocker.ANY}
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="trigger_failed",
            error=mocker.ANY,
            regenerated_brief=None,
        )

    def test_times_out_after_exactly_max_polls(self, mocker):
        """Mutation check (tasks.md 2.4): the loop count must be EXACTLY
        _THUMBNAIL_REGEN_MAX_POLLS (100), never >=100 or an off-by-one —
        pinned via time.sleep's exact call count."""
        from congress_videos.youtube_upload_dag import (
            _THUMBNAIL_REGEN_MAX_POLLS,
            _regenerate_flagged_thumbnail,
        )

        dag_run = _regen_dag_run(state="running")
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        sleep = mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
        )

        assert result == {"outcome": "timeout", "error": mocker.ANY}
        assert sleep.call_count == _THUMBNAIL_REGEN_MAX_POLLS == 100
        assert dag_run.refresh_from_db.call_count == _THUMBNAIL_REGEN_MAX_POLLS
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="timeout",
            error=mocker.ANY,
            regenerated_brief=None,
        )

    def test_trigger_exception_returns_trigger_failed_never_raises(self, mocker):
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        mocker.patch(
            "congress_videos.youtube_upload_dag.trigger_dag_api",
            side_effect=RuntimeError("could not reach the scheduler API"),
        )
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
        )

        assert result == {"outcome": "trigger_failed", "error": "could not reach the scheduler API"}
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="trigger_failed",
            error="could not reach the scheduler API",
            regenerated_brief=None,
        )

    def test_child_dag_failed_state_returns_child_failed(self, mocker):
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        dag_run = _regen_dag_run(state="failed")
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        get_one = mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one")
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
        )

        assert result == {"outcome": "child_failed", "error": mocker.ANY}
        get_one.assert_not_called()
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="child_failed",
            error=mocker.ANY,
            regenerated_brief=None,
        )

    @pytest.mark.parametrize(
        "xcom_result",
        [
            None,
            {"success": True, "output_path": "", "title": "x"},
            {"success": True, "title": "x"},
            {"success": False, "output_path": "/videos/turn-1/thumbnail.png", "title": "x"},
        ],
        ids=["none", "empty_output_path", "missing_output_path", "success_false"],
    )
    def test_malformed_xcom_returns_invalid_result(self, mocker, xcom_result):
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        dag_run = _regen_dag_run(state="success")
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=xcom_result)
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
        )

        assert result == {"outcome": "invalid_result", "error": mocker.ANY}
        mock_db.record_thumbnail_text_regeneration_outcome.assert_called_once_with(
            "/videos/turn-1/video.mp4",
            outcome="invalid_result",
            error=mocker.ANY,
            regenerated_brief=None,
        )

    def test_valid_success_shape_but_nonexistent_path_is_invalid_result(self, mocker):
        """design.md D5: a returned path that does not exist on disk is
        recorded as invalid_result and never swapped in — even when every
        other field of the child's result is well-formed."""
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        dag_run = _regen_dag_run(state="success")
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=False)
        mocker.patch(
            "congress_videos.youtube_upload_dag.XCom.get_one",
            return_value=_regen_valid_result(),
        )
        mock_db = MagicMock()

        result = _regenerate_flagged_thumbnail(
            _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
        )

        assert result == {"outcome": "invalid_result", "error": mocker.ANY}

    def test_outcome_recording_failure_is_swallowed(self, mocker, caplog):
        """design.md D4 point 3: the outcome write uses the
        _write_title_provenance failure-isolation shape — a DB outage on the
        bookkeeping write cannot become a publication outage."""
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        dag_run = _regen_dag_run(state="success")
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mocker.patch("congress_videos.youtube_upload_dag.os.path.exists", return_value=True)
        valid_result = _regen_valid_result()
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=valid_result)
        mock_db = MagicMock()
        mock_db.record_thumbnail_text_regeneration_outcome.side_effect = RuntimeError("db is down")

        with caplog.at_level("ERROR"):
            result = _regenerate_flagged_thumbnail(
                _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
            )

        assert result == valid_result
        assert any("db is down" in r.message for r in caplog.records)

    @pytest.mark.parametrize(
        "setup",
        [
            "trigger_raises",
            "child_failed",
            "invalid_result",
            "timeout",
            "unexpected_poll_exception",
        ],
    )
    def test_no_path_ever_raises(self, mocker, setup):
        """NON-NEGOTIABLE (issue #545): every failure mode converges on ONE
        behaviour — publish as-is, record the outcome, never raise. This is
        what makes #512's non-blocking asymmetry structural rather than
        aspirational. Every branch, including a genuinely unexpected
        mid-poll exception, must return a dict rather than propagate."""
        from congress_videos.youtube_upload_dag import _regenerate_flagged_thumbnail

        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        mock_db = MagicMock()

        if setup == "trigger_raises":
            mocker.patch(
                "congress_videos.youtube_upload_dag.trigger_dag_api",
                side_effect=RuntimeError("boom"),
            )
        elif setup == "child_failed":
            mocker.patch(
                "congress_videos.youtube_upload_dag.trigger_dag_api",
                return_value=_regen_dag_run(state="failed"),
            )
        elif setup == "invalid_result":
            mocker.patch(
                "congress_videos.youtube_upload_dag.trigger_dag_api",
                return_value=_regen_dag_run(state="success"),
            )
            mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=None)
        elif setup == "timeout":
            mocker.patch(
                "congress_videos.youtube_upload_dag.trigger_dag_api",
                return_value=_regen_dag_run(state="running"),
            )
        elif setup == "unexpected_poll_exception":
            dag_run = _regen_dag_run(state="running")
            dag_run.refresh_from_db.side_effect = RuntimeError("scheduler DB unreachable")
            mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=dag_run)

        try:
            result = _regenerate_flagged_thumbnail(
                _regen_thumbnail_config(), "/videos/turn-1/video.mp4", None, "run_1", db=mock_db
            )
        except Exception as exc:  # pragma: no cover - the assertion below is the real check
            pytest.fail(f"_regenerate_flagged_thumbnail raised {exc!r} instead of returning a dict")

        assert isinstance(result, dict)
        assert "outcome" in result or result.get("success") is True


# ---------------------------------------------------------------------------
# _claim_and_regenerate_thumbnail (issue #545, PR3)
# ---------------------------------------------------------------------------


class TestClaimAndRegenerateThumbnail:
    def test_claimed_attempt_calls_regenerate_flagged_thumbnail(self, mocker):
        from congress_videos.youtube_upload_dag import _claim_and_regenerate_thumbnail

        mock_db = MagicMock()
        mock_db.claim_thumbnail_text_regeneration.return_value = {
            "thumbnail_regen_attempts": 1,
            "thumbnail_regen_exhausted": False,
        }
        regen = mocker.patch(
            "congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail",
            return_value=_regen_valid_result(),
        )
        thumbnail_config = _regen_thumbnail_config()

        result = _claim_and_regenerate_thumbnail(
            mock_db,
            output_path="/videos/turn-1/video.mp4",
            thumbnail_config=thumbnail_config,
            prior_brief={"archetype": "closeup"},
            run_id="run_1",
        )

        mock_db.claim_thumbnail_text_regeneration.assert_called_once_with(
            "/videos/turn-1/video.mp4", prior_brief={"archetype": "closeup"}
        )
        regen.assert_called_once_with(
            thumbnail_config, "/videos/turn-1/video.mp4", {"archetype": "closeup"}, "run_1", db=mock_db
        )
        assert result == _regen_valid_result()

    def test_exhausted_or_no_row_returns_none_without_triggering(self, mocker):
        """design.md D3 / spec note 8: exhausted budget AND chapter items
        with no speaker_turn_videos row both surface as a falsy claim —
        both are INTENTIONAL, not errors, and must never trigger."""
        from congress_videos.youtube_upload_dag import _claim_and_regenerate_thumbnail

        mock_db = MagicMock()
        mock_db.claim_thumbnail_text_regeneration.return_value = None
        regen = mocker.patch("congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail")

        result = _claim_and_regenerate_thumbnail(
            mock_db,
            output_path="/videos/chapter-only/video.mp4",
            thumbnail_config=_regen_thumbnail_config(),
            prior_brief=None,
            run_id="run_1",
        )

        assert result is None
        regen.assert_not_called()

    def test_claim_exception_returns_none_never_raises(self, mocker):
        """Non-negotiable: no code path in the regeneration seam may raise —
        including a DB outage at claim time, before any paid call."""
        from congress_videos.youtube_upload_dag import _claim_and_regenerate_thumbnail

        mock_db = MagicMock()
        mock_db.claim_thumbnail_text_regeneration.side_effect = RuntimeError("db is down")
        regen = mocker.patch("congress_videos.youtube_upload_dag._regenerate_flagged_thumbnail")

        try:
            result = _claim_and_regenerate_thumbnail(
                mock_db,
                output_path="/videos/turn-1/video.mp4",
                thumbnail_config=_regen_thumbnail_config(),
                prior_brief=None,
                run_id="run_1",
            )
        except Exception as exc:  # pragma: no cover - the assertion below is the real check
            pytest.fail(f"_claim_and_regenerate_thumbnail raised {exc!r} instead of returning None")

        assert result is None
        regen.assert_not_called()


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
                "upload_results": {"upload_details": [{"chapter_id": 42, "youtube_video_id": "abc123"}]},
            }
        )
        mock_db = MagicMock()

        _backfill_thumbnail_video_id(ti, mock_db)

        mock_db.update_thumbnail_youtube_video_id.assert_called_once_with(chapter_id=42, youtube_video_id="abc123")

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
                "upload_results": {"upload_details": [{"chapter_id": 42, "youtube_video_id": "abc123"}]},
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
        assert result["key_speakers"] == [{"name": "Ana Pastor"}], "key_speakers must be propagated from chapter row"

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

        assert result.get("key_speakers") == [], "key_speakers must be [] when chapter row has no key_speakers key"

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

        assert result.get("key_speakers") == [], "key_speakers must be [] when chapter.key_speakers is None"


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
            start_seconds=90.0,  # 00:01:30 — turn's own narrow span
            end_seconds=120.0,  # 00:02:00
            group_start_seconds=60.0,  # 00:01:00 — wider group span
            group_end_seconds=180.0,  # 00:03:00
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

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ) as lookup,
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            ) as by_slug,
        ):
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
        with pytest.raises(ValueError, match="ZoneInfo keys must be normalized relative paths"):
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
        assert "get_uploadable_item" in task_ids, "DAG must have a get_uploadable_item task (dual-queue wired)"

    def test_get_uploadable_item_task_is_python_operator(self):
        """The get_uploadable_item task must be a PythonOperator."""
        from airflow.operators.python import PythonOperator

        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        task = tasks_by_id.get("get_uploadable_item")
        assert task is not None, "get_uploadable_item task must exist"
        assert isinstance(task, PythonOperator), "get_uploadable_item must be a PythonOperator"

    def test_get_uploadable_item_is_downstream_of_skip_if_quota_reached(self):
        """get_uploadable_item must be downstream of skip_if_quota_reached."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        skip_task = tasks_by_id["skip_if_quota_reached"]
        item_task = tasks_by_id.get("get_uploadable_item")
        assert item_task is not None, "get_uploadable_item task must exist"
        downstream_ids = {t.task_id for t in skip_task.downstream_list}
        assert item_task.task_id in downstream_ids, "get_uploadable_item must be downstream of skip_if_quota_reached"

    def test_generate_metadata_is_downstream_of_get_uploadable_item(self):
        """generate_youtube_metadata must be downstream of get_uploadable_item."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        item_task = tasks_by_id.get("get_uploadable_item")
        meta_task = tasks_by_id["generate_youtube_metadata"]
        assert item_task is not None, "get_uploadable_item task must exist"
        upstream_ids = {t.task_id for t in meta_task.upstream_list}
        assert item_task.task_id in upstream_ids, "generate_youtube_metadata must be downstream of get_uploadable_item"

    def test_dag_task_count_updated_for_wired_dual_queue(self):
        """DAG must have 16 tasks: 13 original (t1_db replaced by get_uploadable_item
        PythonOperator), plus mark_turns_uploaded, plus verify_final_copy (issue #512),
        plus apply_intro_overlay (issue #558)."""
        from congress_videos.youtube_upload_dag import dag

        assert len(dag.tasks) == 16, (
            f"Expected 16 tasks (13 original tasks, t1_db replaced by get_uploadable_item PythonOperator, "
            f"plus mark_turns_uploaded, plus verify_final_copy, plus apply_intro_overlay), got {len(dag.tasks)}"
        )

    def test_mark_turns_uploaded_task_exists(self):
        """DAG must have a mark_turns_uploaded task (CRITICAL-3)."""
        from congress_videos.youtube_upload_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "mark_turns_uploaded" in task_ids, "DAG must have a mark_turns_uploaded task after upload"

    def test_mark_turns_uploaded_is_downstream_of_trigger_youtube_upload(self):
        """mark_turns_uploaded must be downstream of trigger_youtube_upload."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        upload_task = tasks_by_id["trigger_youtube_upload"]
        mark_turns = tasks_by_id.get("mark_turns_uploaded")
        assert mark_turns is not None, "mark_turns_uploaded task must exist"
        downstream_ids = {t.task_id for t in upload_task.downstream_list}
        assert mark_turns.task_id in downstream_ids, "mark_turns_uploaded must be downstream of trigger_youtube_upload"


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
# title_generation_input persistence hook (issue #549, Requirement 3/5)
# ---------------------------------------------------------------------------


class TestTriggerThumbnailGenerationTitleProvenance:
    """trigger_thumbnail_generation persists title_generation_input keyed by
    thumbnail_config["output_path"] (the turn's own video.mp4), never the
    child result's output_path (the reconciled thumbnail.png), and never
    lets a persistence failure block publication."""

    _TITLE_PAYLOAD = {
        "generator": "turn_title",
        "schema_version": 1,
        "summary": "un resumen",
        "best": {"label": "option_a", "style": "A", "prompt": "p"},
        "sibling_titles": None,
        "key_speakers": None,
        "forbidden_title": None,
        "participant_slug": None,
        "title": "Un título",
    }

    def _ti_with_output_path(self) -> object:
        return _make_ti(
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

    def _mock_success(self, mocker, extra_result: dict | None = None) -> None:
        child_run = MagicMock()
        child_run.run_id = "thumb_run_provenance"
        child_run.state = "success"
        mocker.patch("congress_videos.youtube_upload_dag.trigger_dag_api", return_value=child_run)
        mocker.patch("congress_videos.youtube_upload_dag.time.sleep")
        result = {
            "success": True,
            "chapter_id": 42,
            # D3: the child's reconciled output_path is thumbnail.png, distinct
            # from thumbnail_config["output_path"] (video.mp4) used as the write key.
            "output_path": "/data/oradores/42/thumbnail.png",
            "title": "Un título",
        }
        if extra_result:
            result.update(extra_result)
        mocker.patch("congress_videos.youtube_upload_dag.XCom.get_one", return_value=result)

    def test_write_key_is_thumbnail_config_output_path_not_child_result_output_path(self, mocker) -> None:
        """Scenario 3.1/D3: the write key must be thumbnail_config['output_path']."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        self._mock_success(mocker, {"title_generation_input": self._TITLE_PAYLOAD})
        fake_db = MagicMock()
        fake_db.record_title_generation_input_turn.return_value = 1
        ti = self._ti_with_output_path()

        trigger_thumbnail_generation(ti, db=fake_db, run_id="test_run")

        fake_db.record_title_generation_input_turn.assert_called_once()
        call_args = fake_db.record_title_generation_input_turn.call_args
        key_used = call_args.args[0] if call_args.args else call_args.kwargs.get("output_path")
        assert key_used == "/data/oradores/42/video.mp4"
        assert key_used != "/data/oradores/42/thumbnail.png"
        assert call_args.kwargs["payload"] == self._TITLE_PAYLOAD

    def test_zero_rows_is_no_row_not_success(self, mocker, caplog) -> None:
        """Scenario 3.2b: rowcount == 0 is a loud no_row outcome, never success."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        self._mock_success(mocker, {"title_generation_input": self._TITLE_PAYLOAD})
        fake_db = MagicMock()
        fake_db.record_title_generation_input_turn.return_value = 0
        ti = self._ti_with_output_path()

        with caplog.at_level("WARNING"):
            trigger_thumbnail_generation(ti, db=fake_db, run_id="test_run")

        assert ti.xcom_store["title_provenance"] == {"status": "no_row", "rows": 0, "error": None}
        assert any("0 rows" in r.message for r in caplog.records)

    def test_db_exception_is_caught_and_publication_continues(self, mocker) -> None:
        """Scenario 5.1: a persistence failure is caught, logged, and never
        propagates — the upload task still completes with a valid thumbnail_result."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        self._mock_success(mocker, {"title_generation_input": self._TITLE_PAYLOAD})
        fake_db = MagicMock()
        fake_db.record_title_generation_input_turn.side_effect = RuntimeError("db unreachable")
        ti = self._ti_with_output_path()

        result = trigger_thumbnail_generation(ti, db=fake_db, run_id="test_run")

        assert result == "thumb_run_provenance"
        assert ti.xcom_store["title_provenance"] == {
            "status": "failed",
            "rows": 0,
            "error": "db unreachable",
        }
        assert ti.xcom_store["thumbnail_result"]["success"] is True
        assert ti.xcom_store["thumbnail_result"]["title"] == "Un título"

    def test_missing_payload_records_skipped_never_fails_strict_validation(self, mocker) -> None:
        """D2: title_generation_input absent from the result must never
        degrade a valid title into a thumbnail failure — the strict
        validation conjunction never sees this key."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        self._mock_success(mocker)  # no title_generation_input key at all
        fake_db = MagicMock()
        ti = self._ti_with_output_path()

        trigger_thumbnail_generation(ti, db=fake_db, run_id="test_run")

        fake_db.record_title_generation_input_turn.assert_not_called()
        assert ti.xcom_store["title_provenance"] == {"status": "skipped", "rows": 0, "error": None}
        assert ti.xcom_store["thumbnail_result"]["success"] is True

    def test_default_db_none_constructs_congressional_video_db(self, mocker) -> None:
        """No db= injected -> trigger_thumbnail_generation creates its own."""
        from congress_videos.youtube_upload_dag import trigger_thumbnail_generation

        self._mock_success(mocker, {"title_generation_input": self._TITLE_PAYLOAD})
        mock_db_instance = MagicMock()
        mock_db_instance.record_title_generation_input_turn.return_value = 1
        mocker.patch(
            "congress_videos.modules.database.CongressionalVideoDB",
            return_value=mock_db_instance,
        )
        ti = self._ti_with_output_path()

        trigger_thumbnail_generation(ti, run_id="test_run")

        mock_db_instance.record_title_generation_input_turn.assert_called_once()
        assert ti.xcom_store["title_provenance"]["status"] == "written"


# ---------------------------------------------------------------------------
# SRT sidecar write (Slice 5 — srt-sidecar-canonical-path)
# ---------------------------------------------------------------------------


class TestPrepareThumbnailConfigSrtSidecar:
    """Upload path (issue #146 Fix C) no longer writes subtitles.srt for turns.

    The speaker_turn_prepare DAG now owns the turn subtitles.srt sidecar,
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
        assert not srt_path.exists(), "upload path must NOT write subtitles.srt for turns; PREPARE DAG owns it"
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
        assert not any(tmp_path.rglob("subtitles.srt")), "upload path must not write subtitles.srt for turns"


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

        with patch(
            "congress_videos.modules.youtube.youtube_ai.generate_youtube_metadata_for_selected_videos"
        ) as mock_ai:
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
            "youtube_metadata_results": {"topic_metadata": [{"description": {"description": "Desc."}}]},
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
        assert srt_content == original_srt, "subtitles.srt must not be modified by the metadata overwrite step"


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
# _analyze_chapter_content — mentioned-people + topics upload hook (issue #432)
# ---------------------------------------------------------------------------


class TestAnalyzeChapterContentUsesChapterWindow:
    """`_analyze_chapter_content` must derive its text from the chapter's own
    SRT window (db.get_chapter_srt_context), never from the turn/group span
    (design F1/D7) — `uploadable_turns` does not expose start_time/end_time."""

    def test_analysis_uses_chapter_window_not_turn_window(self, tmp_path, mocker):
        from congress_videos.modules.mentioned_people_resolution import MentionedPeopleResult
        from congress_videos.modules.topic_extraction import TopicsResult
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        # Block 1 is outside the turn's own/group span but inside the wider
        # chapter span; block 2 is inside both. Both must reach the analyses.
        srt_content = (
            "1\n00:00:10,000 --> 00:00:20,000\nFuera del grupo, dentro del capitulo.\n\n"
            "2\n00:01:30,000 --> 00:02:00,000\nDentro del turno y del grupo.\n\n"
        )
        srt_path = tmp_path / "test.srt"
        srt_path.write_text(srt_content, encoding="utf-8")

        turn = _make_turn_row(turn_id=1, chapter_id=42, start_seconds=90.0, end_seconds=120.0)
        turn["video_id"] = "video123"

        mock_db = MagicMock()
        mock_db.get_chapter_srt_context.return_value = {
            "video_id": "video123",
            "start_time": "00:00:00,000",
            "end_time": "00:03:00,000",
            "session_date": "2025-06-10",
        }

        mocker.patch("congress_videos.youtube_upload_dag.find_srt_for_chapter", return_value=str(srt_path))
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        )
        mocker.patch("congress_videos.youtube_upload_dag.get_participants_roster", return_value=[])
        mock_resolve = mocker.patch(
            "congress_videos.youtube_upload_dag.resolve_mentioned_people",
            return_value=MentionedPeopleResult(),
        )
        mock_extract = mocker.patch(
            "congress_videos.youtube_upload_dag.extract_topics",
            return_value=TopicsResult(),
        )

        _prepare_thumbnail_config(turn, mock_db)

        mock_db.get_chapter_srt_context.assert_called_once_with(42)
        mock_resolve.assert_called_once()
        chapter_text = mock_resolve.call_args[0][0]
        assert "Fuera del grupo, dentro del capitulo" in chapter_text
        assert "Dentro del turno y del grupo" in chapter_text

        mock_extract.assert_called_once()
        assert "Fuera del grupo, dentro del capitulo" in mock_extract.call_args[0][0]


class TestAnalyzeChapterContentMissingContext:
    """`ctx is None` or `get_chapter_srt_context` raising must skip BOTH
    analyses and persist nothing (design F1/D7)."""

    @pytest.mark.parametrize("ctx_behavior", ["returns_none", "raises"])
    def test_missing_chapter_context_skips_both_analyses(self, ctx_behavior, mocker, tmp_path):
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        srt_path = tmp_path / "test.srt"
        srt_path.write_text("1\n00:00:10,000 --> 00:00:20,000\nTexto cualquiera.\n\n", encoding="utf-8")

        turn = _make_turn_row(turn_id=1, chapter_id=42)
        turn["video_id"] = "video123"

        mock_db = MagicMock()
        if ctx_behavior == "returns_none":
            mock_db.get_chapter_srt_context.return_value = None
        else:
            mock_db.get_chapter_srt_context.side_effect = RuntimeError("db unavailable")

        mocker.patch("congress_videos.youtube_upload_dag.find_srt_for_chapter", return_value=str(srt_path))
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        )
        mock_resolve = mocker.patch("congress_videos.youtube_upload_dag.resolve_mentioned_people")
        mock_extract = mocker.patch("congress_videos.youtube_upload_dag.extract_topics")

        _prepare_thumbnail_config(turn, mock_db)

        mock_resolve.assert_not_called()
        mock_extract.assert_not_called()
        mock_db.update_chapter_content_analysis.assert_not_called()


class TestAnalyzeChapterContentFailureIsolation:
    """A failure in one analysis MUST NOT discard the other's persisted
    result (design D9/M7/T5)."""

    def _setup(self, mocker, tmp_path):
        srt_path = tmp_path / "test.srt"
        srt_path.write_text("1\n00:00:10,000 --> 00:00:20,000\nTexto cualquiera.\n\n", encoding="utf-8")

        turn = _make_turn_row(turn_id=1, chapter_id=42)
        turn["video_id"] = "video123"

        mock_db = MagicMock()
        mock_db.get_chapter_srt_context.return_value = {
            "video_id": "video123",
            "start_time": "00:00:00,000",
            "end_time": "00:03:00,000",
            "session_date": "2025-06-10",
        }

        mocker.patch("congress_videos.youtube_upload_dag.find_srt_for_chapter", return_value=str(srt_path))
        mocker.patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        )
        mocker.patch("congress_videos.youtube_upload_dag.get_participants_roster", return_value=[])
        return turn, mock_db

    def test_one_analysis_failing_persists_the_other(self, mocker, tmp_path):
        from congress_videos.modules.topic_extraction import TopicsResult
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn, mock_db = self._setup(mocker, tmp_path)

        mocker.patch(
            "congress_videos.youtube_upload_dag.resolve_mentioned_people",
            side_effect=RuntimeError("llm failure"),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.extract_topics",
            return_value=TopicsResult(ok=True, topics=("sanidad",)),
        )

        _prepare_thumbnail_config(turn, mock_db)

        mock_db.update_chapter_content_analysis.assert_called_once_with(42, mentioned_slugs=None, topics=["sanidad"])

    def test_topics_failing_persists_mentioned_slugs(self, mocker, tmp_path):
        from congress_videos.modules.mentioned_people_resolution import MentionedPeopleResult
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn, mock_db = self._setup(mocker, tmp_path)

        mocker.patch(
            "congress_videos.youtube_upload_dag.resolve_mentioned_people",
            return_value=MentionedPeopleResult(ok=True),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.extract_topics",
            side_effect=RuntimeError("llm failure"),
        )

        _prepare_thumbnail_config(turn, mock_db)

        mock_db.update_chapter_content_analysis.assert_called_once_with(42, mentioned_slugs=[], topics=None)

    def test_empty_topics_does_not_overwrite(self, mocker, tmp_path):
        """ok=True with zero topics must be OMITTED from the UPDATE kwargs
        (topics stays None), never written as an empty array (design D9)."""
        from congress_videos.modules.mentioned_people_resolution import MentionedPeopleResult
        from congress_videos.modules.topic_extraction import TopicsResult
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn, mock_db = self._setup(mocker, tmp_path)

        mocker.patch(
            "congress_videos.youtube_upload_dag.resolve_mentioned_people",
            return_value=MentionedPeopleResult(ok=True),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.extract_topics",
            return_value=TopicsResult(ok=True, topics=()),
        )

        _prepare_thumbnail_config(turn, mock_db)

        mock_db.update_chapter_content_analysis.assert_called_once_with(42, mentioned_slugs=[], topics=None)

    def test_db_failure_does_not_fail_the_upload(self, mocker, tmp_path):
        from congress_videos.modules.mentioned_people_resolution import MentionedPeopleResult
        from congress_videos.modules.topic_extraction import TopicsResult
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        turn, mock_db = self._setup(mocker, tmp_path)

        mocker.patch(
            "congress_videos.youtube_upload_dag.resolve_mentioned_people",
            return_value=MentionedPeopleResult(ok=True, people=()),
        )
        mocker.patch(
            "congress_videos.youtube_upload_dag.extract_topics",
            return_value=TopicsResult(ok=True, topics=("sanidad",)),
        )
        mock_db.update_chapter_content_analysis.side_effect = RuntimeError("db write failed")

        result = _prepare_thumbnail_config(turn, mock_db)

        assert result["chapter_id"] == 42


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
        assert upload_config["videos"][0]["turn_id"] == 1, "prepare step must carry turn_id into the upload config"

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

        upload_results = upload_multiple_videos(upload_config["token_file"], upload_config["videos"])

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

        mock_db.mark_turns_uploaded.assert_called_once_with(turn_id=1, youtube_video_id="yt-round-trip")
        mock_db.mark_turns_uploaded_by_output_path.assert_not_called()


# ---------------------------------------------------------------------------
# docs/DAGS.md <-> DAG schedule consistency guard (issue #422)
# ---------------------------------------------------------------------------


def _documented_schedule(markdown: str, dag_id: str) -> str:
    """Return the cron token documented for ``dag_id`` in a DAGS.md-shaped document.

    Anchors on the per-DAG ``## `` heading, never on line numbers, so section
    renumbering and surrounding prose are free to change. ``re.split`` on ``^## ``
    cannot split on ``### `` subheadings, so a DAG's subsections stay inside its
    own section.
    """
    sections = [s for s in re.split(r"^## ", markdown, flags=re.M)[1:] if dag_id in s.splitlines()[0]]
    assert len(sections) == 1, (
        f"docs/DAGS.md must contain exactly one '## ...' section whose heading names {dag_id}; found {len(sections)}."
    )
    crons = re.findall(r"^\*\*Schedule:\*\*\s*`([^`]+)`", sections[0], flags=re.M)
    assert len(crons) == 1, (
        f"Expected exactly one '**Schedule:** `<cron>`' line in the {dag_id} section "
        f"of docs/DAGS.md; found {len(crons)}."
    )
    return crons[0].strip()


class TestDocsScheduleConsistency:
    """The schedule stated in docs/DAGS.md must be the one the DAG declares.

    Documentation drift is what manufactured issues #325 and #328; this guard
    binds the single documented cron token to ``dag.schedule_interval``. Scope is
    deliberately one scalar fact — do not extend it to the task graph, the daily
    cap or the artifact layout, which fail on legitimate reformatting.
    """

    def test_schedule_is_once_daily_at_19_utc(self):
        """DAG schedule is '0 19 * * *' — one run daily at 19:00 UTC."""
        from congress_videos.youtube_upload_dag import dag

        assert dag.schedule_interval == "0 19 * * *"

    def test_documented_schedule_matches_dag_schedule(self):
        """docs/DAGS.md and the DAG must not drift apart."""
        from congress_videos.youtube_upload_dag import dag

        documented = _documented_schedule(DAGS_DOC.read_text(encoding="utf-8"), DAG_ID)

        assert documented == dag.schedule_interval, (
            f"docs/DAGS.md documents schedule {documented!r} for {DAG_ID}, but the DAG declares "
            f"{dag.schedule_interval!r} in congress_videos/youtube_upload_dag.py. Fix whichever "
            f"is wrong: the '**Schedule:**' line in the {DAG_ID} section of docs/DAGS.md, or the "
            f"schedule= argument of the DAG."
        )

    def test_extractor_ignores_other_dag_sections_and_subheadings(self):
        """Only the uploader's own section is read; '### ' subsections stay inside it."""
        markdown = (
            "# DAGs\n"
            "\n"
            "## 1. congress_youtube_channel_monitor\n"
            "\n"
            "**Schedule:** `0 22 * * *` (22:00 hora Madrid, diario)\n"
            "\n"
            "## 2. congress_youtube_chapter_uploader\n"
            "\n"
            "**Schedule:** `0 19 * * *` (19:00 UTC, diario)\n"
            "\n"
            "### Nomenclatura\n"
            "\n"
            "Texto libre que menciona `0 12 * * *` sin ser una linea de Schedule.\n"
        )

        assert _documented_schedule(markdown, DAG_ID) == "0 19 * * *"

    def test_extractor_rejects_two_schedule_lines_in_one_section(self):
        """Two schedules under one heading IS the drift — fail loudly, do not pick one."""
        markdown = "## 2. congress_youtube_chapter_uploader\n\n**Schedule:** `0 19 * * *`\n**Schedule:** `0 12 * * *`\n"

        with pytest.raises(AssertionError, match="exactly one '\\*\\*Schedule:\\*\\*"):
            _documented_schedule(markdown, DAG_ID)

    def test_extractor_rejects_missing_dag_section(self):
        """Losing the per-DAG heading loses the doc's index key — fail loudly."""
        markdown = "## 1. congress_youtube_channel_monitor\n\n**Schedule:** `0 22 * * *`\n"

        with pytest.raises(AssertionError, match="exactly one '## ...' section"):
            _documented_schedule(markdown, DAG_ID)


# ---------------------------------------------------------------------------
# _turn_speaker_fields (lifted out of _prepare_thumbnail_config, issue #272)
# ---------------------------------------------------------------------------


class TestTurnSpeakerFields:
    """Turn-branch speaker resolution: (key_speakers, slug) for one turn row."""

    def test_resolved_name_anchors_key_speakers_and_slug_comes_from_fuzzy_lookup(self):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(resolved_name="Ana García")

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ) as fuzzy:
            key_speakers, slug = _turn_speaker_fields(turn)

        fuzzy.assert_called_once_with("Ana García")
        assert key_speakers == ["Ana García"]
        assert slug == "garcia-ana"

    def test_fuzzy_lookup_returning_none_leaves_slug_none(self):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(resolved_name="Ana García")

        with patch("congress_videos.youtube_upload_dag.lookup_participant_fuzzy", return_value=None):
            assert _turn_speaker_fields(turn) == (["Ana García"], None)

    def test_fuzzy_lookup_raising_keeps_key_speakers_and_sets_slug_none(self, caplog):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(turn_id=9, resolved_name="Ana García")

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                side_effect=RuntimeError("db down"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            key_speakers, slug = _turn_speaker_fields(turn)

        assert key_speakers == ["Ana García"]
        assert slug is None
        assert any("turn speaker resolution failed" in r.message and "turn_id=9" in r.message for r in caplog.records)

    def test_empty_resolved_name_uses_fallback_slug_and_its_display_name(self):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(resolved_name="", resolved_participant_slug="lopez-pedro")

        with (
            patch("congress_videos.youtube_upload_dag.lookup_participant_fuzzy") as fuzzy,
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
                return_value={"slug": "lopez-pedro", "display_name": "Pedro López"},
            ) as by_slug,
        ):
            key_speakers, slug = _turn_speaker_fields(turn)

        fuzzy.assert_not_called()
        by_slug.assert_called_once_with("lopez-pedro")
        assert slug == "lopez-pedro"
        assert key_speakers == ["Pedro López"]

    def test_slug_lookup_raising_keeps_key_speakers_empty_and_slug_as_fallback(self, caplog):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(turn_id=9, resolved_name=None, resolved_participant_slug="lopez-pedro")

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
                side_effect=RuntimeError("db down"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            key_speakers, slug = _turn_speaker_fields(turn)

        assert key_speakers == []
        assert slug == "lopez-pedro"
        assert any("turn slug lookup failed" in r.message and "turn_id=9" in r.message for r in caplog.records)

    def test_no_resolved_name_and_no_fallback_slug_calls_no_lookup(self):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(resolved_name="", resolved_participant_slug="")

        with (
            patch("congress_videos.youtube_upload_dag.lookup_participant_fuzzy") as fuzzy,
            patch("congress_videos.youtube_upload_dag.lookup_participant_by_slug") as by_slug,
        ):
            result = _turn_speaker_fields(turn)

        assert result == ([], None)
        fuzzy.assert_not_called()
        by_slug.assert_not_called()

    def test_participant_without_display_name_leaves_key_speakers_empty(self):
        from congress_videos.youtube_upload_dag import _turn_speaker_fields

        turn = _make_turn_row(resolved_name="", resolved_participant_slug="lopez-pedro")

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_by_slug",
            return_value={"slug": "lopez-pedro", "display_name": ""},
        ):
            assert _turn_speaker_fields(turn) == ([], "lopez-pedro")


# ---------------------------------------------------------------------------
# Issue #558: session intro-card overlay — t5b apply_intro_overlay
# ---------------------------------------------------------------------------


class TestBuildIntroCardText:
    """_build_intro_card_text(session_number, session_date) — Spanish titulo/descripcion.

    The card names the institution, not just an ordinal — a long-form video reaches
    viewers with no surrounding context. descripcion carries the date as DD/MM/AAAA
    (issue #558's stated format). Both absent raises (nothing to render on the card).
    """

    def test_session_number_and_date_present(self):
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        titulo, descripcion = _build_intro_card_text(42, "2026-09-10")

        assert titulo == "Sesión 42 del Congreso de los Diputados"
        assert descripcion == "10/09/2026"

    def test_only_session_number_present(self):
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        titulo, descripcion = _build_intro_card_text(7, None)

        assert titulo == "Sesión 7 del Congreso de los Diputados"
        assert descripcion == ""

    def test_only_session_date_present(self):
        """No ordinal still identifies the institution, never a bare date as a title."""
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        titulo, descripcion = _build_intro_card_text(None, "2026-09-10")

        assert titulo == "Congreso de los Diputados"
        assert descripcion == "10/09/2026"

    def test_date_object_is_formatted_not_stringified(self):
        from datetime import date

        from congress_videos.youtube_upload_dag import _build_intro_card_text

        _titulo, descripcion = _build_intro_card_text(42, date(2026, 3, 4))

        assert descripcion == "04/03/2026"

    def test_unparseable_date_passes_through_rather_than_raising(self):
        """The intro card must never block a publication over a odd date value."""
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        _titulo, descripcion = _build_intro_card_text(42, "sesión extraordinaria")

        assert descripcion == "sesión extraordinaria"

    def test_both_absent_raises(self):
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        with pytest.raises(ValueError, match="session_number.*session_date"):
            _build_intro_card_text(None, None)

    def test_session_number_zero_is_not_treated_as_absent(self):
        """session_number=0 must use the number branch, not the None-fallback branch."""
        from congress_videos.youtube_upload_dag import _build_intro_card_text

        titulo, _descripcion = _build_intro_card_text(0, "2026-09-10")

        assert titulo == "Sesión 0 del Congreso de los Diputados"


def _make_intro_overlay_store(
    *,
    output_path: str,
    success: bool = True,
    session_number=42,
    session_date="2026-09-10",
    chapter_id: int = 100,
    turn_id: int | None = 1,
) -> dict:
    """Build the XCom store `_apply_intro_overlay` reads from (t5's output)."""
    return {
        "uploadable_item": {
            "item": {
                "chapter_id": chapter_id,
                "turn_id": turn_id,
                "session_number": session_number,
                "session_date": session_date,
                "output_path": output_path,
            },
            "item_type": "turn",
        },
        "chapter_extraction_results": {
            "total_chapters": 1,
            "successful_extractions": 1 if success else 0,
            "failed_extractions": 0 if success else 1,
            "results": [
                {
                    "chapter_id": chapter_id,
                    "turn_id": turn_id,
                    "video_id": "vid123",
                    "success": success,
                    "output_path": output_path if success else None,
                    "file_size_mb": None,
                    "duration_seconds": None,
                    "error": None if success else "turn output_path missing",
                }
            ],
        },
    }


def _patch_intro_overlay_fonts_ok(mocker):
    """Fonts are not installed in every dev/CI environment; `_validate_overlay`
    checks font-file existence via `os.path.exists`. `os` and `os.path` are
    shared singleton modules, so patching `os.path.exists` is inherently global
    regardless of which module's `os` attribute is used to reach it — a
    discriminating side_effect (real check for every other path) keeps
    unrelated real-file assertions in the same test working.
    """
    from congress_videos.config.paths import FONT_BOLD, FONT_REGULAR

    real_exists = os.path.exists

    def _fake_exists(path):
        if path in (FONT_BOLD, FONT_REGULAR):
            return True
        return real_exists(path)

    mocker.patch("os.path.exists", side_effect=_fake_exists)


def _patch_apply_overlays_writes_file(mocker):
    """Fake apply_overlays that actually writes the `_edited` sibling file, so the
    real (unpatched) `os.path.exists` post-check in `_apply_intro_overlay` — and
    any source-immutability / sidecar-directory assertions — see a real file.
    """

    def _fake_apply_overlays(source_path, output_path, overlays, domain_cfg, *, max_timeout=None):
        with open(output_path, "wb") as f:
            f.write(b"fake-edited-bytes")
        return {"success": True, "output_path": output_path}

    return mocker.patch(
        "congress_videos.modules.video_editor.apply_overlays",
        side_effect=_fake_apply_overlays,
    )


class TestApplyIntroOverlayPassThrough:
    """t5b mirrors t6's upstream-failure tolerance: never fail loud for upstream
    extraction problems that are not this task's own doing (D3)."""

    def test_missing_chapter_extraction_results_is_pass_through(self):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        ti = _make_ti({"uploadable_item": {"item": {}, "item_type": "turn"}})

        result = _apply_intro_overlay(ti)

        assert result is None
        assert "chapter_extraction_results" not in ti.xcom_store

    def test_failed_extraction_result_is_pass_through(self):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        store = _make_intro_overlay_store(output_path="/data/turn1/video.mp4", success=False)
        ti = _make_ti(store)

        result = _apply_intro_overlay(ti)

        assert result is None
        # XCom untouched: still the original (failed) extraction payload.
        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_empty_results_list_is_pass_through(self):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        store = {
            "uploadable_item": {"item": {}, "item_type": "turn"},
            "chapter_extraction_results": {"total_chapters": 0, "results": []},
        }
        ti = _make_ti(store)

        result = _apply_intro_overlay(ti)

        assert result is None
        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_missing_output_path_is_pass_through(self):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        store = {
            "uploadable_item": {"item": {}, "item_type": "turn"},
            "chapter_extraction_results": {
                "results": [{"chapter_id": 1, "success": True, "output_path": None}],
            },
        }
        original = dict(store["chapter_extraction_results"])
        ti = _make_ti(store)

        result = _apply_intro_overlay(ti)

        assert result is None
        assert ti.xcom_store["chapter_extraction_results"] == original


class TestApplyIntroOverlayFailLoud:
    """Every failure CAUSED by t5b itself must raise — never a silent skip,
    never publishing the un-overlaid source (D3)."""

    def test_missing_session_fields_raises_before_apply_overlays(self, tmp_path, mocker):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source), session_number=None, session_date=None)
        ti = _make_ti(store)

        apply_spy = mocker.patch("congress_videos.modules.video_editor.apply_overlays")

        with pytest.raises(ValueError, match="session_number.*session_date"):
            _apply_intro_overlay(ti)

        apply_spy.assert_not_called()
        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_missing_font_raises_filenotfounderror_before_apply_overlays(self, tmp_path, mocker):
        """D5: validate_editor_input runs BEFORE apply_overlays. A missing font
        must raise FileNotFoundError naming tipo/key/path before ffmpeg spawns."""
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        mocker.patch("os.path.exists", return_value=False)
        apply_spy = mocker.patch("congress_videos.modules.video_editor.apply_overlays")

        with pytest.raises(FileNotFoundError, match="intro_sesion"):
            _apply_intro_overlay(ti)

        apply_spy.assert_not_called()
        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_guard_trip_raises_and_leaves_xcom_untouched(self, tmp_path, mocker):
        """A ValueError from apply_overlays' own duration guard must propagate."""
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        mocker.patch(
            "congress_videos.modules.video_editor.apply_overlays",
            side_effect=ValueError("Source duration 9999.0s exceeds MAX_OVERLAY_SOURCE_SECONDS (3600s)."),
        )

        with pytest.raises(ValueError, match="MAX_OVERLAY_SOURCE_SECONDS"):
            _apply_intro_overlay(ti)

        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_ffmpeg_failure_raises_and_leaves_xcom_untouched(self, tmp_path, mocker):
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        mocker.patch(
            "congress_videos.modules.video_editor.apply_overlays",
            side_effect=RuntimeError("ffmpeg failed (rc=1): boom"),
        )

        with pytest.raises(RuntimeError, match="ffmpeg failed"):
            _apply_intro_overlay(ti)

        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]

    def test_missing_output_file_raises_and_leaves_xcom_untouched(self, tmp_path, mocker):
        """apply_overlays reports success but never actually wrote the file."""
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        mocker.patch(
            "congress_videos.modules.video_editor.apply_overlays",
            return_value={"success": True, "output_path": str(tmp_path / "video_edited.mp4")},
        )

        with pytest.raises(RuntimeError, match="missing"):
            _apply_intro_overlay(ti)

        assert ti.xcom_store["chapter_extraction_results"] == store["chapter_extraction_results"]


class TestApplyIntroOverlayCallOrder:
    def test_validate_editor_input_called_before_apply_overlays(self, tmp_path, mocker):
        """D5/D8 (task 4.8): a missing font must fail before ffmpeg ever spawns."""
        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        call_order: list[str] = []

        def _fake_validate(conf):
            call_order.append("validate")

        def _fake_apply(source_path, output_path, overlays, domain_cfg, *, max_timeout=None):
            call_order.append("apply")
            with open(output_path, "wb") as f:
                f.write(b"x")
            return {"success": True, "output_path": output_path}

        real_validate = mocker.patch(
            "congress_videos.modules.video_editor.validate_editor_input",
            side_effect=_fake_validate,
        )
        mocker.patch(
            "congress_videos.modules.video_editor.apply_overlays",
            side_effect=_fake_apply,
        )

        _apply_intro_overlay(ti)

        assert call_order == ["validate", "apply"]
        real_validate.assert_called_once()


class TestApplyIntroOverlaySuccess:
    """In-process overwrite, DB invariant, source immutability, sidecar
    resolution, and idempotent retry — the happy path (D2/D3/D4)."""

    def test_overwrites_output_path_in_memory_and_records_original(self, tmp_path, mocker):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        pushed = ti.xcom_store["chapter_extraction_results"]
        result0 = pushed["results"][0]
        assert result0["output_path"] == str(tmp_path / "video_edited.mp4")
        assert result0["original_output_path"] == str(source)

    def test_card_text_from_build_intro_card_text_reaches_the_overlay_conf(self, tmp_path, mocker):
        """Closes the verify WARNING: the pure text builder is well covered, but
        nothing pinned that its output actually reaches the overlay conf handed
        to `apply_overlays`. Without this, a refactor could silently drop the
        session label and still ship a card."""
        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source), session_number=77, session_date="2026-03-04")
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        apply_overlays_mock = _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import (
            _apply_intro_overlay,
            _build_intro_card_text,
        )

        _apply_intro_overlay(ti)

        expected_titulo, expected_descripcion = _build_intro_card_text(77, "2026-03-04")
        overlays = apply_overlays_mock.call_args.args[2]
        assert len(overlays) == 1
        assert overlays[0]["tipo"] == "intro_sesion"
        assert overlays[0]["titulo"] == expected_titulo
        assert overlays[0]["descripcion"] == expected_descripcion

    def test_no_database_module_imported_and_no_db_write(self, tmp_path, mocker):
        """t5b must import no database module and issue no db.* write (D4)."""
        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)
        db_ctor = mocker.patch("congress_videos.modules.database.CongressionalVideoDB")

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        db_ctor.assert_not_called()

    def test_speaker_turn_videos_output_path_never_updated(self, tmp_path, mocker):
        """DB invariant: no db.* write of any shape is issued (D4)."""
        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)
        mock_db = MagicMock()
        mocker.patch("congress_videos.modules.database.CongressionalVideoDB", return_value=mock_db)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        mock_db.mark_turns_uploaded.assert_not_called()
        assert mock_db.method_calls == []

    def test_source_file_bytes_and_path_unchanged(self, tmp_path, mocker):
        source = tmp_path / "video.mp4"
        original_bytes = b"source-bytes-unchanged"
        source.write_bytes(original_bytes)
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        assert source.exists()
        assert source.read_bytes() == original_bytes

    def test_edited_file_lands_beside_source_for_sidecar_resolution(self, tmp_path, mocker):
        """The 4 sidecars still resolve for prepare_orador_upload_config (D-Sidecar)."""
        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        source = turn_dir / "video.mp4"
        source.write_bytes(b"source-bytes")
        for name, content in (
            ("title.txt", "T"),
            ("description.txt", "D"),
            ("thumbnail.png", "\x89PNG"),
            ("subtitles.srt", ""),
        ):
            (turn_dir / name).write_text(content, encoding="utf-8")

        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        new_output_path = ti.xcom_store["chapter_extraction_results"]["results"][0]["output_path"]
        assert os.path.dirname(new_output_path) == str(turn_dir)
        for sidecar in ("title.txt", "description.txt", "thumbnail.png", "subtitles.srt"):
            assert (turn_dir / sidecar).exists()

    def test_retry_writes_same_deterministic_path_no_accumulation(self, tmp_path, mocker):
        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)
        first_output_path = ti.xcom_store["chapter_extraction_results"]["results"][0]["output_path"]

        # Retry: same run, XCom already carries the overlaid output_path from
        # the first attempt — mirrors what a task retry replays from t5's XCom
        # (t5 itself is idempotent and always pushes the same source path).
        retry_store = _make_intro_overlay_store(output_path=str(source))
        retry_ti = _make_ti(retry_store)
        _apply_intro_overlay(retry_ti)
        second_output_path = retry_ti.xcom_store["chapter_extraction_results"]["results"][0]["output_path"]

        assert first_output_path == second_output_path
        edited_files = list(tmp_path.glob("*_edited.mp4"))
        assert len(edited_files) == 1

    def test_default_window_used_when_no_override_supplied(self, tmp_path, mocker):
        """Default Intro Window requirement: [0, 5) read from INTRO_WINDOW_SECONDS."""
        from congress_videos.modules.video_editor import INTRO_WINDOW_SECONDS

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        apply_spy = _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        overlays_arg = apply_spy.call_args.args[2]
        assert overlays_arg[0]["tiempo_inicio"] == INTRO_WINDOW_SECONDS[0]
        assert overlays_arg[0]["tiempo_fin"] == INTRO_WINDOW_SECONDS[1]

    def test_max_timeout_passed_through_to_apply_overlays(self, tmp_path, mocker):
        from congress_videos.modules.video_editor import OVERLAY_MAX_TIMEOUT_SECONDS

        source = tmp_path / "video.mp4"
        source.write_bytes(b"source-bytes")
        store = _make_intro_overlay_store(output_path=str(source))
        ti = _make_ti(store)

        _patch_intro_overlay_fonts_ok(mocker)
        apply_spy = _patch_apply_overlays_writes_file(mocker)

        from congress_videos.youtube_upload_dag import _apply_intro_overlay

        _apply_intro_overlay(ti)

        assert apply_spy.call_args.kwargs["max_timeout"] == OVERLAY_MAX_TIMEOUT_SECONDS


class TestTurnIdPinnedThroughIntroOverlayEditedPath:
    """Regression pin (issue #558, D4 landmine): mark_turn_uploads' fallback
    (`mark_turns_uploaded_by_output_path`, `WHERE output_path = %s`) would match
    ZERO rows against an `_edited` path. It stays unreachable only because t6
    always sets `turn_config["turn_id"]` — this test pins that guarantee even
    when output_path has been rewritten to the overlaid `_edited` sibling.
    """

    def test_turn_id_present_in_upload_config_for_edited_output_path(self, tmp_path):
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        edited_path = turn_dir / "video_edited.mp4"
        edited_path.write_bytes(b"edited")
        for name, content in (
            ("title.txt", "T"),
            ("description.txt", "D"),
            ("thumbnail.png", "\x89PNG"),
            ("subtitles.srt", ""),
        ):
            (turn_dir / name).write_text(content, encoding="utf-8")

        extraction = {
            "results": [
                {
                    "chapter_id": 100,
                    "turn_id": 1,
                    "video_id": "vid123",
                    "success": True,
                    "output_path": str(edited_path),
                    "original_output_path": str(turn_dir / "video.mp4"),
                }
            ],
        }
        store = {
            "uploadable_item": {"item": {"turn_id": 1}, "item_type": "turn"},
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": {},
            "thumbnail_result": {"success": True, "title": "Overlaid Title"},
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(ti, **context)

        upload_config = ti.xcom_store["upload_config"]
        turn_config = upload_config["videos"][0]
        assert turn_config["turn_id"] == 1, (
            "turn_id must be present so mark_turn_uploads takes the primary "
            "turn_id branch, never the output_path fallback (which would match "
            "0 rows against an _edited path and silently re-publish tomorrow)"
        )


class TestApplyIntroOverlayWiring:
    def test_apply_intro_overlay_between_extract_and_prepare(self):
        """t5b sits directly between t5 (extract) and t6 (prepare_upload_config)."""
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        extract = tasks_by_id["extract_chapter_videos"]
        overlay = tasks_by_id["apply_intro_overlay"]
        prepare = tasks_by_id["prepare_upload_config"]

        assert overlay.task_id in {t.task_id for t in extract.downstream_list}
        assert prepare.task_id in {t.task_id for t in overlay.downstream_list}
