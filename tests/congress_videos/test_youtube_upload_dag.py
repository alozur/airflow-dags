"""Tests for congress_youtube_chapter_uploader DAG (congress_videos.youtube_upload_dag)."""

from __future__ import annotations

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

    def test_dag_has_thirteen_tasks(self):
        """DAG must have 13 tasks after replacing t3/t4 with 3 new tasks (net +1)."""
        from congress_videos.youtube_upload_dag import dag

        assert len(dag.tasks) == 13

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
