"""Tests for congress_youtube_channel_monitor DAG
(congress_videos.youtube_channel_monitor_dag)."""

from __future__ import annotations

import inspect
from datetime import UTC, datetime, timezone

import pytest

# ---------------------------------------------------------------------------
# DAG load tests
# ---------------------------------------------------------------------------


class TestCongressYoutubeChannelMonitorDAGLoads:
    def test_dag_loads(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        assert dag is not None
        assert dag.dag_id == "congress_youtube_channel_monitor"

    def test_dag_has_correct_schedule(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        assert dag.schedule_interval == "0 * * * *"

    def test_dag_serializes_runs(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        assert dag.max_active_runs == 1

    def test_filter_unprocessed_videos_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "filter_unprocessed_videos" in task_ids


# ---------------------------------------------------------------------------
# Topology tests
# ---------------------------------------------------------------------------


class TestFilterUnprocessedVideosTopology:
    def test_sits_between_filter_plenary_and_finished_stream_guard(self):
        """filter_unprocessed_videos must be downstream of filter_plenary_sessions
        and upstream of the finished-stream guard
        (production path: t2 >> t2b >> t2_guard >> t2a)."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t2 = tasks_by_id["filter_plenary_sessions"]
        t2b = tasks_by_id["filter_unprocessed_videos"]
        guard = tasks_by_id["filter_finished_streams"]

        # t2 -> t2b
        assert t2b.task_id in {t.task_id for t in t2.downstream_list}
        # t2b -> t2_guard (guard now sits between t2b and check_if_plenary_found)
        assert guard.task_id in {t.task_id for t in t2b.downstream_list}

    def test_not_on_test_mode_path(self):
        """The test-mode path (create_test_video_data >> [t3a, t3b]) must NOT
        reach filter_unprocessed_videos."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t0_test = tasks_by_id["create_test_video_data"]
        t2b = tasks_by_id["filter_unprocessed_videos"]

        # filter_unprocessed_videos is not a direct downstream of the test task
        assert "filter_unprocessed_videos" not in {t.task_id for t in t0_test.downstream_list}
        # nor anywhere in the test task's transitive downstream set
        downstream_ids = {t.task_id for t in t0_test.get_flat_relatives(upstream=False)}
        assert "filter_unprocessed_videos" not in downstream_ids

        # and the test task is not upstream of t2b
        upstream_ids = {t.task_id for t in t2b.get_flat_relatives(upstream=True)}
        assert "create_test_video_data" not in upstream_ids


# ---------------------------------------------------------------------------
# Improvement #9 — dynamic task mapping for per-chunk summarization
# (t5f_flatten -> t5f_map (.partial().expand()) -> t5f aggregate)
# ---------------------------------------------------------------------------


class TestDynamicChunkSummarizationMapping:
    """Verify improvement #9: per-chunk summarization is fanned out via
    Airflow dynamic task mapping (.expand) instead of a single serial task."""

    def test_mapped_summarize_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "flatten_chunks_for_mapping" in task_ids
        assert "summarize_one_chunk" in task_ids
        assert "aggregate_chunk_summaries" in task_ids

    def test_summarize_one_chunk_is_a_mapped_task(self):
        """The summarization task must be an expanded/mapped operator, not a
        plain PythonOperator. Checked by class name to stay version-robust:
        Airflow 2.10 exposes MappedOperator under airflow.models.mappedoperator,
        Airflow 3.x under airflow.sdk.definitions.mappedoperator."""
        from congress_videos.youtube_channel_monitor_dag import dag

        mapped = dag.get_task("summarize_one_chunk")
        assert type(mapped).__name__ == "MappedOperator"

    def test_dynamic_mapping_wiring_is_present(self):
        """Structural wiring: flatten -> map -> aggregate."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        flatten = tasks_by_id["flatten_chunks_for_mapping"]
        mapped = tasks_by_id["summarize_one_chunk"]
        aggregate = tasks_by_id["aggregate_chunk_summaries"]

        # flatten -> map
        assert mapped.task_id in {t.task_id for t in flatten.downstream_list}
        # map -> aggregate
        assert aggregate.task_id in {t.task_id for t in mapped.downstream_list}


# ---------------------------------------------------------------------------
# finished-stream-guard (F.1) — filter_finished_streams topology + params
# ---------------------------------------------------------------------------


class TestFilterFinishedStreamsTopology:
    def test_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "filter_finished_streams" in task_ids

    def test_sits_between_filter_unprocessed_and_check_if_plenary_found(self):
        """Production path: t2b >> t2_guard >> t2a."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t2b = tasks_by_id["filter_unprocessed_videos"]
        guard = tasks_by_id["filter_finished_streams"]
        t2a = tasks_by_id["check_if_plenary_found"]

        # t2b -> guard
        assert guard.task_id in {t.task_id for t in t2b.downstream_list}
        # guard -> t2a
        assert t2a.task_id in {t.task_id for t in guard.downstream_list}

    def test_not_on_test_mode_path(self):
        """The test-mode path must NOT reach filter_finished_streams."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t0_test = tasks_by_id["create_test_video_data"]
        downstream_ids = {t.task_id for t in t0_test.get_flat_relatives(upstream=False)}
        assert "filter_finished_streams" not in downstream_ids

    def test_guard_params_present_with_defaults(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        params = dag.params
        assert "guard_enabled" in params
        assert "guard_floor_minutes" in params
        # ParamsDict.__getitem__ resolves to the raw value
        assert bool(params["guard_enabled"]) is True
        assert int(params["guard_floor_minutes"]) == 10

    def test_min_hours_since_end_param_default_is_12(self):
        """min_hours_since_end raised from 2 to 12 (fix-video-integrity #24)."""
        from congress_videos.youtube_channel_monitor_dag import dag

        assert int(dag.params["min_hours_since_end"]) == 12

    def test_empty_guard_result_routes_to_no_plenary_sessions(self):
        """When the guard drops every candidate (total_matches == 0), the
        downstream branch must route to 'no_plenary_sessions'."""
        from unittest.mock import MagicMock

        from congress_videos.youtube_channel_monitor_dag import dag

        branch = {t.task_id: t for t in dag.tasks}["check_if_plenary_found"]
        ti = MagicMock()
        ti.xcom_pull.return_value = {
            "total_matches": 0,
            "videos": [],
            "target_date": "2025-10-08",
        }

        assert branch.python_callable(ti) == "no_plenary_sessions"


# ---------------------------------------------------------------------------
# TASK 8 (RED) — t_normalize_speakers topology
# ---------------------------------------------------------------------------


class TestNormalizeSpeakersTask:
    """TASK 8 — t_normalize_speakers exists, is wired after save_chapters_to_db,
    and has trigger_rule='all_done'."""

    def test_normalize_speakers_task_exists(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "normalize_speakers" in task_ids, "Expected task_id 'normalize_speakers' in DAG tasks"

    def test_normalize_speakers_trigger_rule_is_all_done(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t = tasks_by_id["normalize_speakers"]
        assert str(t.trigger_rule) == "all_done", f"Expected trigger_rule='all_done', got {t.trigger_rule!r}"

    def test_normalize_speakers_is_downstream_of_save_chapters_to_db(self):
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}

        t9_db = tasks_by_id["save_chapters_to_db"]
        normalize = tasks_by_id["normalize_speakers"]

        downstream_ids = {t.task_id for t in t9_db.downstream_list}
        assert normalize.task_id in downstream_ids, (
            "'normalize_speakers' must be a direct downstream of 'save_chapters_to_db'"
        )


# ---------------------------------------------------------------------------
# Phase 4 — Monitor cleanup: trigger_refinement removed, normalize_speakers terminal
# ---------------------------------------------------------------------------


class TestMonitorTriggerRefinementRemoved:
    """Negative tests: trigger_refinement task must NOT exist; normalize_speakers is terminal."""

    def test_no_trigger_refinement_task(self):
        """trigger_refinement task must not exist in the monitor DAG after cleanup."""
        from congress_videos.youtube_channel_monitor_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "trigger_refinement" not in task_ids, (
            "trigger_refinement task must be deleted from the monitor DAG "
            "(was targeting non-existent dag_id='speaker_turns_dag')"
        )

    def test_normalize_speakers_is_terminal(self):
        """normalize_speakers must have no downstream tasks after trigger_refinement is removed."""
        from congress_videos.youtube_channel_monitor_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        normalize = tasks_by_id["normalize_speakers"]
        assert len(normalize.downstream_list) == 0, (
            f"normalize_speakers must be terminal (no downstream tasks); "
            f"found: {[t.task_id for t in normalize.downstream_list]}"
        )


# ---------------------------------------------------------------------------
# Issue #158 — _resolve_srt_input fail-fast helper
# ---------------------------------------------------------------------------


class TestSplitSrtResolveInput:
    """Unit tests for the module-level _resolve_srt_input(ti) helper.

    Uses the shared mock_task_instance fixture (tests/conftest.py) which
    provides an in-memory XCom store keyed by 'key=' kwarg.
    """

    def test_both_none_raises_value_error(self, mock_task_instance):
        """Both XCom keys return None -> ValueError with both branch states in message."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_srt_input

        ti = mock_task_instance
        # Both keys absent -> xcom_pull returns None

        with pytest.raises(ValueError) as exc_info:
            _resolve_srt_input(ti)

        message = str(exc_info.value)
        assert "merged_srt_files=None" in message
        assert "youtube_subtitles=None" in message

    def test_both_empty_dict_raises_value_error(self, mock_task_instance):
        """Truthy-but-empty dicts (videos=[]) must still raise ValueError (regression guard)."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_srt_input

        ti = mock_task_instance
        empty = {"total_downloaded": 0, "videos": []}
        ti.xcom_push(key="merged_srt_files", value=empty)
        ti.xcom_push(key="youtube_subtitles", value=empty)

        with pytest.raises(ValueError):
            _resolve_srt_input(ti)

    def test_merged_valid_returns_merged(self, mock_task_instance):
        """merged_srt_files with videos -> returns that dict unchanged, no exception."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_srt_input

        ti = mock_task_instance
        merged = {"videos": [{"id": 1}]}
        ti.xcom_push(key="merged_srt_files", value=merged)
        # youtube_subtitles left absent (None)

        result = _resolve_srt_input(ti)
        assert result is merged

    def test_subtitles_valid_returns_subtitles(self, mock_task_instance):
        """merged_srt_files empty, youtube_subtitles has videos -> returns subtitles dict unchanged."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_srt_input

        ti = mock_task_instance
        subtitles = {"videos": [{"id": 2}]}
        ti.xcom_push(key="youtube_subtitles", value=subtitles)
        # merged_srt_files left absent (None)

        result = _resolve_srt_input(ti)
        assert result is subtitles

    def test_plenary_absent_raises_without_secondary_exception(self, mock_task_instance):
        """Both SRT keys empty + plenary_videos absent -> ValueError raised, no AttributeError."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_srt_input

        ti = mock_task_instance
        # All keys absent; plenary_videos returns None

        # Must raise ValueError (not AttributeError / TypeError from message construction)
        with pytest.raises(ValueError) as exc_info:
            _resolve_srt_input(ti)

        message = str(exc_info.value)
        # Message must reference video ids safely (empty list or 'unknown')
        assert "unknown" in message or "[]" in message or "video_ids" in message


# ---------------------------------------------------------------------------
# Issue #206 — _resolve_target_date resolves at task runtime, not parse time
# ---------------------------------------------------------------------------


class TestResolveTargetDate:
    """Unit tests for the module-level _resolve_target_date(context) helper.

    target_date must resolve inside each task at execution time, using an
    explicit params override when present, else the DAG run's own
    logical_date — never a value computed once when the DAG file was parsed.
    """

    def test_explicit_override_wins(self):
        from congress_videos.youtube_channel_monitor_dag import _resolve_target_date

        context = {
            "params": {"target_date": "2026-08-20"},
            "logical_date": datetime(2026, 8, 25, tzinfo=UTC),
        }
        assert _resolve_target_date(context) == "2026-08-20"

    def test_no_override_uses_logical_date(self):
        from congress_videos.youtube_channel_monitor_dag import _resolve_target_date

        context = {
            "params": {"target_date": None},
            "logical_date": datetime(2026, 8, 24, 15, 30, tzinfo=UTC),
        }
        assert _resolve_target_date(context) == "2026-08-24"

    def test_no_logical_date_falls_back_to_data_interval_end(self):
        from congress_videos.youtube_channel_monitor_dag import _resolve_target_date

        context = {
            "params": {},
            "data_interval_end": datetime(2026, 8, 23, 9, 0, tzinfo=UTC),
        }
        assert _resolve_target_date(context) == "2026-08-23"

    def test_falls_back_to_ds_when_no_logical_date_available(self):
        from congress_videos.youtube_channel_monitor_dag import _resolve_target_date

        context = {"params": {}, "ds": "2026-08-22"}
        assert _resolve_target_date(context) == "2026-08-22"

    def test_last_resort_now_utc(self, monkeypatch):
        import congress_videos.youtube_channel_monitor_dag as mod

        class _FrozenDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime(2026, 8, 21, 3, 0, 0, tzinfo=tz)

        monkeypatch.setattr(mod, "datetime", _FrozenDatetime)

        assert mod._resolve_target_date({}) == "2026-08-21"

    def test_midnight_boundary_resolves_to_the_run_day(self):
        """@hourly run at 00:30 UTC on day D with lookback_days=1 must resolve
        target_date to day D — not a stale date computed at DAG-parse time
        (regression guard for issue #206)."""
        from congress_videos.youtube_channel_monitor_dag import _resolve_target_date

        context = {
            "params": {"target_date": None, "lookback_days": 1},
            "logical_date": datetime(2026, 8, 24, 0, 30, tzinfo=UTC),
        }
        assert _resolve_target_date(context) == "2026-08-24"


# ---------------------------------------------------------------------------
# Issue #202b — _slim_transcriptions_for_xcom trims the transcription
# payload at the XCom boundary (Postgres XCom backend stores every push).
# ---------------------------------------------------------------------------


class TestSlimTranscriptionsForXcom:
    def test_none_input_passes_through_unchanged(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        assert _slim_transcriptions_for_xcom(None) is None

    def test_non_dict_input_passes_through_unchanged(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        assert _slim_transcriptions_for_xcom("not a dict") == "not a dict"

    def test_preserves_total_transcribed_and_top_level_error(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        result = {"total_transcribed": 0, "videos": [], "error": "Whisper API unavailable"}
        slimmed = _slim_transcriptions_for_xcom(result)

        assert slimmed["total_transcribed"] == 0
        assert slimmed["error"] == "Whisper API unavailable"
        assert slimmed["videos"] == []

    def test_chunked_video_drops_chunk_text_keeps_srt_paths(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        result = {
            "total_transcribed": 1,
            "videos": [
                {
                    "video_id": "abc123",
                    "video_title": "Sesión Plenaria",
                    "chunked": True,
                    "total_chunks": 2,
                    "successful_transcriptions": 2,
                    "chunks": [
                        {
                            "success": True,
                            "text": "a" * 5000,
                            "srt_path": "/srt/c1.srt",
                            "chunk_number": 1,
                            "segments": [{"start": 0, "end": 1, "text": "..."}],
                        },
                        {
                            "success": True,
                            "text": "b" * 5000,
                            "srt_path": "/srt/c2.srt",
                            "chunk_number": 2,
                            "segments": [{"start": 1, "end": 2, "text": "..."}],
                        },
                    ],
                }
            ],
        }

        slimmed = _slim_transcriptions_for_xcom(result)
        video = slimmed["videos"][0]

        assert video["video_id"] == "abc123"
        assert video["chunked"] is True
        assert video["total_chunks"] == 2
        assert video["successful_transcriptions"] == 2
        assert video["srt_paths"] == ["/srt/c1.srt", "/srt/c2.srt"]
        assert "chunks" not in video
        assert "text" not in video
        assert "segments" not in video

    def test_unchunked_video_drops_full_text_keeps_srt_path(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        result = {
            "total_transcribed": 1,
            "videos": [
                {
                    "video_id": "xyz789",
                    "chunked": False,
                    "audio_file_path": "/audio/xyz789.webm",
                    "transcription": "c" * 20000,
                    "transcription_success": True,
                    "srt_path": "/srt/xyz789.srt",
                    "transcription_duration": 12.3,
                    "error": None,
                }
            ],
        }

        slimmed = _slim_transcriptions_for_xcom(result)
        video = slimmed["videos"][0]

        assert video["video_id"] == "xyz789"
        assert video["chunked"] is False
        assert video["transcription_success"] is True
        assert video["srt_path"] == "/srt/xyz789.srt"
        assert "transcription" not in video
        assert "audio_file_path" not in video

    def test_per_video_error_preserved(self):
        from congress_videos.youtube_channel_monitor_dag import _slim_transcriptions_for_xcom

        result = {
            "total_transcribed": 0,
            "videos": [{"video_id": "err1", "error": "No audio file path"}],
        }

        slimmed = _slim_transcriptions_for_xcom(result)

        assert slimmed["videos"][0]["error"] == "No audio file path"
        assert slimmed["videos"][0]["video_id"] == "err1"


class TestTargetDateGuardrail:
    """Source-scan guardrails for issue #206: every params-override read site
    must go through _resolve_target_date; the data-field read inside
    _normalize_speakers must survive untouched.
    """

    def _source(self) -> str:
        import congress_videos.youtube_channel_monitor_dag as mod

        return inspect.getsource(mod)

    def test_no_today_str_references(self):
        assert "today_str" not in self._source()

    def test_no_params_get_target_date_reads_remain(self):
        assert 'context["params"].get("target_date")' not in self._source()

    def test_entry_get_target_date_data_field_untouched(self):
        """entry.get('target_date') inside _normalize_speakers reads a video
        row's own date field, not the DAG params override — it must appear
        exactly once, unchanged."""
        assert self._source().count('entry.get("target_date")') == 1


_CHAPTERS_DB_RESULT = {
    "total_videos_saved": 1,
    "total_chapters_saved": 2,
    "total_videos_failed": 0,
    "videos": [
        {
            "video_id": "vidA",
            "chapters_saved": 2,
            "error": None,
            "chapters": [
                {"chapter_id": 11, "start_time": "00:01:00,000", "end_time": "00:02:00,000"},
                {"chapter_id": 12, "start_time": "00:05:00,000", "end_time": "00:06:00,000"},
            ],
        }
    ],
}


class TestRunSaveChaptersToDbWritesSrtSidecars:
    """Phase 3 (#340): writer invoked once per chapter in db_save_results,
    best-effort — a writer exception must not abort the loop or block XCom."""

    @staticmethod
    def _ti():
        from unittest.mock import MagicMock

        store: dict = {"scored_chapters": {"videos": [{"video_id": "vidA", "scored_chapters": []}]}}
        ti = MagicMock(name="TaskInstance")
        ti.xcom_store = store
        ti.xcom_pull.side_effect = lambda key=None, **_kw: store.get(key)
        ti.xcom_push.side_effect = lambda key, value, **_kw: store.__setitem__(key, value)
        return ti

    @pytest.fixture
    def mock_db(self, mocker):
        db = mocker.MagicMock()
        db.save_youtube_chapters_to_db.return_value = _CHAPTERS_DB_RESULT
        mocker.patch("congress_videos.modules.database.CongressionalVideoDB", return_value=db)
        return db

    def test_writer_called_once_per_chapter_with_correct_args(self, mock_db, mocker):
        from congress_videos.youtube_channel_monitor_dag import _run_save_chapters_to_db

        writer_spy = mocker.patch("congress_videos.srt_helpers.write_chapter_srt_sidecar")

        _run_save_chapters_to_db(self._ti(), params={})

        assert writer_spy.call_count == 2
        calls = writer_spy.call_args_list
        assert calls[0].args[:4] == ("vidA", 11, "00:01:00,000", "00:02:00,000")
        assert calls[1].args[:4] == ("vidA", 12, "00:05:00,000", "00:06:00,000")

    def test_writer_exception_does_not_abort_loop_or_xcom_push(self, mock_db, mocker):
        from congress_videos.youtube_channel_monitor_dag import _run_save_chapters_to_db

        writer_spy = mocker.patch(
            "congress_videos.srt_helpers.write_chapter_srt_sidecar",
            side_effect=[RuntimeError("boom"), None],
        )
        ti = self._ti()

        result = _run_save_chapters_to_db(ti, params={})

        # Both chapters attempted despite the first one raising; db_save_results
        # is still pushed to XCom, unaffected by the writer.
        assert writer_spy.call_count == 2
        assert ti.xcom_store["db_save_results"] == _CHAPTERS_DB_RESULT
        assert result == _CHAPTERS_DB_RESULT

    def test_pushed_payload_has_no_raw_datetime_or_time_values(self, mock_db, mocker):
        import datetime as datetime_mod

        from congress_videos.youtube_channel_monitor_dag import _run_save_chapters_to_db

        mocker.patch("congress_videos.srt_helpers.write_chapter_srt_sidecar")
        ti = self._ti()

        _run_save_chapters_to_db(ti, params={})

        for chapter in ti.xcom_store["db_save_results"]["videos"][0]["chapters"]:
            assert isinstance(chapter["start_time"], str)
            assert isinstance(chapter["end_time"], str)
            assert not isinstance(chapter["start_time"], (datetime_mod.date, datetime_mod.time))
