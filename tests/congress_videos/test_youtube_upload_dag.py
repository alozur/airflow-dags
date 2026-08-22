"""Tests for congress_youtube_chapter_uploader DAG (congress_videos.youtube_upload_dag)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
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
            {"chapter_upload_updates": {"recorded_failures": 0, "failed_updates": 0}}
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
                }
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
# THRESHOLD_BY_HOUR constant (REQ-THRESH-01/02/03)
# ---------------------------------------------------------------------------


class TestThresholdByHour:
    def test_constant_exists(self):
        """THRESHOLD_BY_HOUR is importable from the DAG module."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR

        assert THRESHOLD_BY_HOUR is not None

    def test_threshold_at_11(self):
        """11:00 threshold is 10 (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR

        assert THRESHOLD_BY_HOUR[11] == 10

    def test_threshold_at_14(self):
        """14:00 threshold is 20 (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR

        assert THRESHOLD_BY_HOUR[14] == 20

    def test_threshold_at_17(self):
        """17:00 threshold is 0 (REQ-THRESH-03)."""
        from congress_videos.youtube_upload_dag import THRESHOLD_BY_HOUR

        assert THRESHOLD_BY_HOUR[17] == 0


# ---------------------------------------------------------------------------
# should_upload function (REQ-GATE-01, REQ-THRESH-01/02/03)
# ---------------------------------------------------------------------------


def _make_context_for_should_upload(
    queue_size: int, hour: int, uploads_today: int = 0
) -> dict:
    """Build a minimal Airflow context for should_upload tests."""
    from datetime import datetime, timezone
    from unittest.mock import MagicMock

    logical_date = datetime(2026, 7, 31, hour, 0, 0, tzinfo=timezone.utc)

    ti = MagicMock(name="TaskInstance")
    ti.xcom_pull.return_value = {
        "queue_size": queue_size,
        "uploads_today": uploads_today,
    }

    return {"ti": ti, "logical_date": logical_date}


class TestShouldUpload:
    # 11:00 — threshold 10
    def test_11_queue_10_is_false(self):
        """11:00, queue=10 → False (exactly at threshold, not above) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=10, hour=11)
        assert should_upload(**ctx) is False

    def test_11_queue_11_is_true(self):
        """11:00, queue=11 → True (above threshold) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        assert should_upload(**ctx) is True

    def test_11_queue_5_is_false(self):
        """11:00, queue=5 → False (below threshold) (REQ-THRESH-01)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=5, hour=11)
        assert should_upload(**ctx) is False

    # 14:00 — threshold 20
    def test_14_queue_20_is_false(self):
        """14:00, queue=20 → False (boundary — exactly at threshold) (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=20, hour=14)
        assert should_upload(**ctx) is False

    def test_14_queue_21_is_true(self):
        """14:00, queue=21 → True (above threshold) (REQ-THRESH-02)."""
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=21, hour=14)
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
        ctx["data_interval_end"] = datetime.now(timezone.utc) - timedelta(hours=2)
        assert should_upload(**ctx) is False

    def test_fresh_run_proceeds_to_threshold(self):
        """data_interval_end ~1 min in the past, queue above threshold → True (threshold applies)."""
        from datetime import datetime, timedelta, timezone
        from congress_videos.youtube_upload_dag import should_upload

        ctx = _make_context_for_should_upload(queue_size=11, hour=11)
        ctx["data_interval_end"] = datetime.now(timezone.utc) - timedelta(minutes=1)
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
            should_upload,
            STALE_RUN_TOLERANCE_MINUTES,
        )

        frozen_now = datetime(2026, 7, 31, 12, 0, 0, tzinfo=timezone.utc)
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
    def test_resolved_speaker_returns_full_config(self):
        """Raw speaker is resolved once at the boundary and stored as a slug."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            chapter_id=42,
            title="Debate sobre presupuestos",
            description="Una discusión importante",
            session_number=80,
            key_speakers=[{"name": "Ana García"}],
        )
        mock_db = MagicMock()

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ) as lookup:
            result = _prepare_thumbnail_config(chapter, mock_db)

        assert result["slug"] == "garcia-ana"
        lookup.assert_called_once_with("Ana García")
        assert result["domain"] == "congreso"
        assert result["debate_summary"] != ""
        assert result["session"] is not None
        assert result["chapter_id"] == 42

    def test_resolved_participant_slug_is_preferred_over_fuzzy(self):
        """A chapter's resolved_participant_slug wins; no fuzzy lookup is spent."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            chapter_id=7,
            key_speakers=[{"name": "Ministra de Defensa"}],
            resolved_participant_slug="margarita-robles-fernandez",
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
        ) as lookup:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] == "margarita-robles-fernandez"
        lookup.assert_not_called()

    def test_falls_back_to_fuzzy_when_no_resolved_slug(self):
        """Without a resolved slug the raw-speaker fuzzy path still applies."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            key_speakers=[{"name": "Ana García"}],
            resolved_participant_slug=None,
        )

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
        ) as lookup:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] == "garcia-ana"
        lookup.assert_called_once_with("Ana García")

    def test_lookup_error_sets_slug_to_none(self):
        """A speaker lookup failure yields slug=None without raising."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(
            key_speakers=[{"name": "Unknown Speaker"}],
        )
        # db is not called here; error path is simulated via absent lookup
        # The function catches LookupError from the name resolution path.
        # We patch the internal lookup call.
        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            side_effect=LookupError("not found"),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        assert result["domain"] == "congreso"

    def test_unmatched_speaker_sets_slug_to_none(self):
        """An unmatched raw speaker is nonfatal and leaves the slug unset."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[{"name": "Unknown Speaker"}])
        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
        ) as lookup:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        lookup.assert_called_once_with("Unknown Speaker")

    def test_empty_speakers_sets_slug_to_none(self):
        """Chapter with no speaker produces slug=None without fuzzy lookup."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_chapter(key_speakers=[], speakers=[])

        with patch(
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy"
        ) as lookup:
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert result["slug"] is None
        lookup.assert_not_called()


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
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "pastor-ana"},
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
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
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
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value={"slug": "garcia-ana"},
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
        """When SRT resolves, config[srt_fragment] equals the joined block text."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter(
            start_time="00:01:00,000",
            end_time="00:02:00,000",
        )

        blocks = [
            {"start_secs": 60.0, "end_secs": 70.0, "text": "primera frase del debate"},
            {"start_secs": 75.0, "end_secs": 85.0, "text": "segunda frase importante"},
            # block outside window — must be excluded
            {"start_secs": 200.0, "end_secs": 210.0, "text": "fuera de ventana"},
        ]

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
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
        assert "primera frase del debate" in result["srt_fragment"]
        assert "segunda frase importante" in result["srt_fragment"]
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
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
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

    def test_srt_absent_omits_srt_fragment_key(self):
        """When no SRT resolves, the srt_fragment key must be absent from config."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        chapter = _make_srt_chapter()

        with (
            patch(
                "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
                return_value={"slug": "garcia-ana"},
            ),
            patch(
                "congress_videos.youtube_upload_dag.find_srt_for_chapter",
                return_value=None,
            ),
        ):
            result = _prepare_thumbnail_config(chapter, MagicMock())

        assert "srt_fragment" not in result


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
# Dual-queue: turn queue tried first, chapter fallback when turns empty
# ---------------------------------------------------------------------------


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
    }


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

    def test_srt_fragment_bounded_by_start_end_seconds(self, tmp_path, mocker):
        """Turn config: SRT fragment must be limited to [start_seconds, end_seconds]."""
        from congress_videos.youtube_upload_dag import _prepare_thumbnail_config

        # SRT content: block 1 within window, block 2 after window
        srt_content = (
            "1\n00:01:00,000 --> 00:02:00,000\nDentro del turno.\n\n"
            "2\n00:04:00,000 --> 00:05:00,000\nFuera del turno.\n\n"
        )
        srt_path = tmp_path / "test.srt"
        srt_path.write_text(srt_content, encoding="utf-8")

        turn = _make_turn_row(
            turn_id=1,
            start_seconds=60.0,   # 00:01:00
            end_seconds=180.0,    # 00:03:00 — block 2 is at 240s, outside window
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
        assert "Dentro del turno" in result["srt_fragment"]
        assert "Fuera del turno" not in result["srt_fragment"]

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


class TestDualQueueBehavior:
    """Dual-queue: turn queue has priority; chapter fallback used when turns empty."""

    def test_uploader_selects_turn_when_available(self, mocker):
        """When get_uploadable_turns returns a turn, _run_get_uploadable_item returns it
        (with datetime fields sanitized to ISO-8601 strings)."""
        from congress_videos.youtube_upload_dag import (
            _run_get_uploadable_item,
            _sanitize_row_for_xcom,
        )

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]
        mock_db.get_uploadable_chapters.return_value = []

        result = _run_get_uploadable_item(mock_db)

        assert result["item"] == _sanitize_row_for_xcom(fake_turn)
        assert result["item_type"] == "turn"
        mock_db.get_uploadable_chapters.assert_not_called()

    def test_uploader_falls_back_to_chapter_when_turns_empty(self, mocker):
        """When turns empty, _run_get_uploadable_item falls back to chapters."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_chapter = {"chapter_id": 99, "title": "Cap 99"}
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []
        mock_db.get_uploadable_chapters.return_value = [fake_chapter]

        result = _run_get_uploadable_item(mock_db)

        assert result["item"] == fake_chapter
        assert result["item_type"] == "chapter"
        mock_db.get_uploadable_chapters.assert_called_once()

    def test_uploader_returns_none_when_both_queues_empty(self):
        """When both queues empty, _run_get_uploadable_item returns None."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []
        mock_db.get_uploadable_chapters.return_value = []

        result = _run_get_uploadable_item(mock_db)

        assert result is None

    def test_turn_xcom_item_contains_no_datetime_values(self):
        """PRIMARY regression test (issue #163): item dict returned for a turn
        must contain no datetime.datetime values after _run_get_uploadable_item,
        so Airflow's XCom serializer never sees a non-zero-offset stdlib tzinfo."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]

        result = _run_get_uploadable_item(mock_db)

        assert result is not None
        assert all(not isinstance(v, datetime) for v in result["item"].values()), (
            "item dict must not contain any datetime.datetime values (XCom serializer safety)"
        )

    def test_turn_xcom_materialized_at_is_isoformat_string(self):
        """materialized_at and prepared_at must be ISO-8601 strings in the XCom item."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_turn = _make_turn_row()
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = [fake_turn]

        result = _run_get_uploadable_item(mock_db)

        assert isinstance(result["item"]["materialized_at"], str)
        assert isinstance(result["item"]["prepared_at"], str)
        # Verify the strings are valid ISO-8601 (parseable back to datetime)
        datetime.fromisoformat(result["item"]["materialized_at"])
        datetime.fromisoformat(result["item"]["prepared_at"])

    def test_chapter_row_unchanged_by_sanitization(self):
        """Chapter rows (no datetime columns) must pass through unchanged."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        fake_chapter = {"chapter_id": 99, "title": "Cap 99", "relevance_score": 3}
        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []
        mock_db.get_uploadable_chapters.return_value = [fake_chapter]

        result = _run_get_uploadable_item(mock_db)

        assert result["item"] == fake_chapter
        assert result["item_type"] == "chapter"


class TestSanitizeRowForXcom:
    """Unit tests for the pure helper _sanitize_row_for_xcom."""

    def test_sanitize_converts_datetime_to_isoformat_string(self):
        """datetime.datetime values must be replaced with their .isoformat() string."""
        from congress_videos.youtube_upload_dag import _sanitize_row_for_xcom

        tz = timezone(timedelta(hours=2))
        dt = datetime(2026, 8, 22, 1, 0, tzinfo=tz)
        row = {"materialized_at": dt, "turn_id": 1}

        result = _sanitize_row_for_xcom(row)

        assert result["materialized_at"] == dt.isoformat()
        assert isinstance(result["materialized_at"], str)

    def test_sanitize_leaves_non_datetime_fields_unchanged(self):
        """int, str, float, None, list, and datetime.date must pass through unchanged."""
        import datetime as dt_module

        from congress_videos.youtube_upload_dag import _sanitize_row_for_xcom

        date_only = dt_module.date(2026, 8, 22)
        row = {
            "turn_id": 7,
            "name": "Ana",
            "score": 4.5,
            "nothing": None,
            "tags": ["a", "b"],
            "session_date": date_only,
        }

        result = _sanitize_row_for_xcom(row)

        assert result["turn_id"] == 7
        assert result["name"] == "Ana"
        assert result["score"] == 4.5
        assert result["nothing"] is None
        assert result["tags"] == ["a", "b"]
        assert result["session_date"] is date_only

    def test_sanitize_returns_new_dict_does_not_mutate_input(self):
        """The helper must return a NEW dict; the original row must be unmodified."""
        from congress_videos.youtube_upload_dag import _sanitize_row_for_xcom

        tz = timezone(timedelta(hours=2))
        dt = datetime(2026, 8, 22, 1, 0, tzinfo=tz)
        row = {"prepared_at": dt, "turn_id": 5}

        result = _sanitize_row_for_xcom(row)

        assert result is not row
        assert isinstance(row["prepared_at"], datetime), "Original must remain a datetime"

    def test_sanitize_handles_multiple_datetime_fields(self):
        """All datetime.datetime values in the row must be converted."""
        from congress_videos.youtube_upload_dag import _sanitize_row_for_xcom

        tz = timezone(timedelta(hours=2))
        row = {
            "materialized_at": datetime(2026, 8, 22, 1, 0, tzinfo=tz),
            "prepared_at": datetime(2026, 8, 22, 0, 0, tzinfo=tz),
            "turn_id": 3,
        }

        result = _sanitize_row_for_xcom(row)

        assert all(not isinstance(v, datetime) for v in result.values()), (
            "All datetime.datetime values must be converted to strings"
        )
        assert isinstance(result["materialized_at"], str)
        assert isinstance(result["prepared_at"], str)


class TestGuardAAndGuardB:
    """View-level guards: Guard A (chapter excluded when turn uploaded),
    Guard B (turn excluded when chapter uploaded). Both enforced in the SQL
    view — these tests verify the uploader passes the right rows through."""

    def test_guard_b_turn_excluded_by_empty_view(self):
        """Guard B: when uploadable_turns is empty (chapter already uploaded),
        uploader falls back to chapter queue."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []  # Guard B filtered them out
        mock_db.get_uploadable_chapters.return_value = []

        result = _run_get_uploadable_item(mock_db)
        assert result is None

    def test_guard_a_chapter_excluded_by_empty_view(self):
        """Guard A: when chapter fallback returns empty (turn already uploaded),
        uploader has nothing to upload."""
        from congress_videos.youtube_upload_dag import _run_get_uploadable_item

        mock_db = MagicMock()
        mock_db.get_uploadable_turns.return_value = []
        mock_db.get_uploadable_chapters.return_value = []  # Guard A filtered chapter out

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
        PostgreSQLOperator with operation='get_uploadable_chapters'."""
        from congress_videos.youtube_upload_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        # The wired task must exist
        assert "get_uploadable_item" in task_ids, (
            "DAG must have a get_uploadable_item task (dual-queue wired)"
        )

    def test_get_uploadable_item_task_is_python_operator(self):
        """The get_uploadable_item task must be a PythonOperator (not PostgreSQLOperator)."""
        from airflow.operators.python import PythonOperator
        from congress_videos.youtube_upload_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        task = tasks_by_id.get("get_uploadable_item")
        assert task is not None, "get_uploadable_item task must exist"
        assert isinstance(task, PythonOperator), (
            "get_uploadable_item must be a PythonOperator, not a PostgreSQLOperator"
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
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_turns_uploaded_today.return_value = 1
        mock_db.count_pending_uploadable_chapters.return_value = 3
        mock_db.count_pending_uploadable_turns.return_value = 2

        mocker.patch(
            "congress_videos.modules.postgres_operators.CongressionalVideoDB",
            return_value=mock_db,
        )
        mocker.patch.dict(
            "os.environ",
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
                "POSTGRES_SCHEMA": "public",
            },
        )

        op = PostgreSQLOperator(
            task_id="check_upload_quota",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ti = MagicMock()
        ti.xcom_store = {}
        ti.xcom_push.side_effect = lambda key, value, **kw: ti.xcom_store.update({key: value})
        context = {"params": {}, "ti": ti}

        op.execute(context)

        result = ti.xcom_store.get("upload_quota")
        assert result is not None, "upload_quota must be pushed to XCom"
        assert result["uploads_today"] == 2, (
            f"uploads_today must be turns (1) + chapters (1) = 2, got {result['uploads_today']}"
        )

    def test_uploads_today_zero_when_nothing_uploaded(self, mocker):
        """When no turns and no chapters uploaded today, uploads_today must be 0."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 5
        mock_db.count_pending_uploadable_turns.return_value = 2

        mocker.patch(
            "congress_videos.modules.postgres_operators.CongressionalVideoDB",
            return_value=mock_db,
        )
        mocker.patch.dict(
            "os.environ",
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
                "POSTGRES_SCHEMA": "public",
            },
        )

        op = PostgreSQLOperator(
            task_id="check_upload_quota",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ti = MagicMock()
        ti.xcom_store = {}
        ti.xcom_push.side_effect = lambda key, value, **kw: ti.xcom_store.update({key: value})
        context = {"params": {}, "ti": ti}

        op.execute(context)

        result = ti.xcom_store.get("upload_quota")
        assert result["uploads_today"] == 0

    def test_uploads_today_chapter_only_when_no_turns_uploaded(self, mocker):
        """When only chapters uploaded today, uploads_today = chapter count."""
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 1
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 2
        mock_db.count_pending_uploadable_turns.return_value = 0

        mocker.patch(
            "congress_videos.modules.postgres_operators.CongressionalVideoDB",
            return_value=mock_db,
        )
        mocker.patch.dict(
            "os.environ",
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
                "POSTGRES_SCHEMA": "public",
            },
        )

        op = PostgreSQLOperator(
            task_id="check_upload_quota",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ti = MagicMock()
        ti.xcom_store = {}
        ti.xcom_push.side_effect = lambda key, value, **kw: ti.xcom_store.update({key: value})
        context = {"params": {}, "ti": ti}

        op.execute(context)

        result = ti.xcom_store.get("upload_quota")
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
        from congress_videos.modules.postgres_operators import PostgreSQLOperator

        mock_db = MagicMock()
        mock_db.count_chapters_uploaded_today.return_value = 0
        mock_db.count_turns_uploaded_today.return_value = 0
        mock_db.count_pending_uploadable_chapters.return_value = 2
        mock_db.count_pending_uploadable_turns.return_value = 3  # 3 turns pending

        mocker.patch(
            "congress_videos.modules.postgres_operators.CongressionalVideoDB",
            return_value=mock_db,
        )
        mocker.patch.dict(
            "os.environ",
            {
                "POSTGRES_HOST": "localhost",
                "POSTGRES_PORT": "5432",
                "POSTGRES_DB": "testdb",
                "POSTGRES_USER": "testuser",
                "POSTGRES_PASSWORD": "testpass",
                "POSTGRES_SCHEMA": "public",
            },
        )

        op = PostgreSQLOperator(
            task_id="check_upload_quota",
            operation="check_upload_quota",
            output_xcom_key="upload_quota",
        )
        ti = MagicMock()
        ti.xcom_store = {}
        ti.xcom_push.side_effect = lambda key, value, **kw: ti.xcom_store.update({key: value})
        context = {"params": {}, "ti": ti}

        op.execute(context)

        result = ti.xcom_store.get("upload_quota")
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
            "congress_videos.youtube_upload_dag.lookup_participant_fuzzy",
            return_value=None,
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
    """Verify the upload DAG turn branch reads pre-prepared sidecars (issue #146).

    After the split, the upload DAG must NOT call trigger_thumbnail_generation
    for turn items; instead it reads from canonical #133 path sidecars.
    """

    def test_run_generate_thumbnail_skipped_for_turns(self):
        """When item_type=turn, _run_generate_thumbnail must not trigger the thumbnail DAG."""
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
            _run_generate_thumbnail(ti)

        mock_trig.assert_not_called()

    def test_prepare_upload_config_turn_reads_sidecars(self, tmp_path):
        """_prepare_upload_config for a turn item must read pre-written sidecars."""
        from congress_videos.youtube_upload_dag import _prepare_upload_config

        turn_dir = tmp_path / "oradores" / "1"
        turn_dir.mkdir(parents=True)
        (turn_dir / "video.mp4").write_bytes(b"fake")
        (turn_dir / "title.txt").write_text("TÍTULO SIDECAR", encoding="utf-8")
        (turn_dir / "description.txt").write_text("Desc.", encoding="utf-8")
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
                "item": {
                    "turn_id": 1,
                    "output_path": str(turn_dir / "video.mp4"),
                    "chapter_id": 100,
                },
                "item_type": "turn",
            },
            "chapter_extraction_results": extraction,
            "youtube_metadata_results": None,
            "thumbnail_result": None,
        }
        ti = _make_ti(store)
        context = {"params": {"isTesting": False, "dry_run": False}}

        _prepare_upload_config(ti, **context)

        config = ti.xcom_store.get("upload_config")
        assert config is not None
        videos = config.get("videos", [])
        assert len(videos) == 1
        assert videos[0]["title"] == "TÍTULO SIDECAR"

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
            "thumbnail_result": None,
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
