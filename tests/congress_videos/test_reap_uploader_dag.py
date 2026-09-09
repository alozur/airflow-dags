"""Tests for reap_shorts_uploader DAG (congress_videos.reap_shorts_uploader_dag)."""

from __future__ import annotations

import logging

import pytest

# ---------------------------------------------------------------------------
# 7.7 — DAG 2 load test (reap_shorts_uploader)
# ---------------------------------------------------------------------------


class TestReapShortsUploaderDAGLoads:
    def test_reap_shorts_uploader_dag_loads(self):
        from congress_videos.reap_shorts_uploader_dag import dag

        assert dag is not None
        assert dag.dag_id == "reap_shorts_uploader"

    def test_dag_has_correct_task_count(self):
        from congress_videos.reap_shorts_uploader_dag import dag

        # Tasks: get_pending_shorts, generate_metadata, trigger_youtube_upload,
        # mark_shorts_uploaded, check_short_upload_failures
        assert len(dag.tasks) == 5

    def test_dag_schedule(self):
        from congress_videos.reap_shorts_uploader_dag import dag

        assert dag.schedule_interval == "0 8,10,13,15,22 * * *"

    def test_dag_correct_task_ids(self):
        from congress_videos.reap_shorts_uploader_dag import dag

        task_ids = {t.task_id for t in dag.tasks}
        assert "get_pending_shorts" in task_ids
        assert "generate_metadata" in task_ids
        assert "trigger_youtube_upload" in task_ids
        assert "mark_shorts_uploaded" in task_ids
        assert "check_short_upload_failures" in task_ids

    def test_dag_correct_dependency_chain(self):
        from congress_videos.reap_shorts_uploader_dag import dag

        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t1 = tasks_by_id["get_pending_shorts"]
        t2 = tasks_by_id["generate_metadata"]
        t3 = tasks_by_id["trigger_youtube_upload"]
        t4 = tasks_by_id["mark_shorts_uploaded"]
        t5 = tasks_by_id["check_short_upload_failures"]

        assert t2.task_id in {t.task_id for t in t1.downstream_list}
        assert t3.task_id in {t.task_id for t in t2.downstream_list}
        assert t4.task_id in {t.task_id for t in t3.downstream_list}
        assert t5.task_id in {t.task_id for t in t4.downstream_list}


# ---------------------------------------------------------------------------
# 7.7 — Title truncation logic (unit test without Airflow context)
# ---------------------------------------------------------------------------


class TestTriggerYoutubeUploadTitleTruncation:
    def _build_title(self, raw_title: str, suffix: str = " #Shorts", max_len: int = 100) -> str:
        """Reproduce the exact truncation logic from _trigger_youtube_upload."""
        if len(raw_title) + len(suffix) > max_len:
            raw_title = raw_title[: max_len - len(suffix)]
        return raw_title + suffix

    def test_short_title_is_not_truncated(self):
        title = "Debate sobre ley de presupuestos"
        result = self._build_title(title)
        assert result == "Debate sobre ley de presupuestos #Shorts"

    def test_long_title_is_truncated_to_100_chars(self):
        # 95-char title + " #Shorts" (8 chars) = 103 → must truncate
        long_title = "A" * 95
        result = self._build_title(long_title)
        assert len(result) <= 100
        assert result.endswith(" #Shorts")

    def test_exactly_92_char_title_plus_suffix_equals_100(self):
        # 92 + 8 = 100 → no truncation needed
        title = "B" * 92
        result = self._build_title(title)
        assert len(result) == 100
        assert result.endswith(" #Shorts")

    def test_exactly_93_char_title_triggers_truncation(self):
        # 93 + 8 = 101 > 100 → must truncate
        title = "C" * 93
        result = self._build_title(title)
        assert len(result) == 100
        assert result.endswith(" #Shorts")

    def test_result_always_ends_with_shorts_suffix(self):
        for length in [10, 50, 92, 93, 100, 200]:
            result = self._build_title("X" * length)
            assert result.endswith(" #Shorts"), f"Failed for title length {length}"
            assert len(result) <= 100, f"Exceeded 100 chars for title length {length}"


# ---------------------------------------------------------------------------
# 7.8 — DAG 2 task functions (_get_pending_shorts, _mark_shorts_uploaded)
# ---------------------------------------------------------------------------


def _make_ti(xcom_store: dict | None = None):
    from unittest.mock import MagicMock

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


class TestGetPendingShorts:
    def test_empty_result_logs_and_pushes_empty_list(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _get_pending_shorts

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_pending_shorts.return_value = []

        ti = _make_ti()
        _get_pending_shorts(ti, params={"max_shorts_per_run": 2, "min_virality_score": 0.0})

        assert ti.xcom_store["pending_shorts"] == []

    def test_pushes_shorts_list_to_xcom(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _get_pending_shorts

        shorts = [
            {"id": 1, "reap_clip_id": "c-001", "local_file_path": "/data/c1.mp4"},
            {"id": 2, "reap_clip_id": "c-002", "local_file_path": "/data/c2.mp4"},
        ]
        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_pending_shorts.return_value = shorts

        ti = _make_ti()
        _get_pending_shorts(ti, params={"max_shorts_per_run": 3, "min_virality_score": 0.5})

        assert ti.xcom_store["pending_shorts"] == shorts

    def test_passes_limit_and_virality_to_db(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _get_pending_shorts

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_pending_shorts.return_value = []

        ti = _make_ti()
        _get_pending_shorts(ti, params={"max_shorts_per_run": 5, "min_virality_score": 0.7})

        mock_db.get_pending_shorts.assert_called_once_with(limit=5, min_virality_score=0.7)


class TestMarkShortsUploaded:
    def test_successful_upload_calls_mark_short_uploaded(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        upload_results = {
            "upload_details": [
                {
                    "reap_clip_id": "c-001",
                    "youtube_video_id": "yt-abc",
                    "success": True,
                }
            ]
        }

        ti = _make_ti({"upload_results": upload_results})
        _mark_shorts_uploaded(ti, params={})

        mock_db.mark_short_uploaded.assert_called_once_with("c-001", "yt-abc")

    def test_failed_upload_does_not_call_mark_short_uploaded(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        upload_results = {
            "upload_details": [
                {
                    "reap_clip_id": "c-fail",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "Upload failed",
                }
            ]
        }

        ti = _make_ti({"upload_results": upload_results})
        _mark_shorts_uploaded(ti, params={})

        mock_db.mark_short_uploaded.assert_not_called()

    def test_empty_upload_details_no_db_call(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        ti = _make_ti({"upload_results": {"upload_details": []}})
        _mark_shorts_uploaded(ti, params={})

        mock_db.mark_short_uploaded.assert_not_called()

    def test_missing_upload_results_xcom_no_error(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")

        ti = _make_ti({})
        _mark_shorts_uploaded(ti, params={})

    def test_failed_upload_calls_record_short_upload_failure(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        upload_results = {
            "upload_details": [
                {
                    "reap_clip_id": "c-fail",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "Upload failed",
                }
            ]
        }

        ti = _make_ti({"upload_results": upload_results})
        _mark_shorts_uploaded(ti, params={})

        mock_db.record_short_upload_failure.assert_called_once_with("c-fail", "Upload failed")
        mock_db.mark_short_uploaded.assert_not_called()

    def test_failed_upload_missing_reap_clip_id_warns_and_skips(self, mocker, caplog):
        from congress_videos.reap_shorts_uploader_dag import _mark_shorts_uploaded

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value

        upload_results = {
            "upload_details": [
                {
                    "reap_clip_id": None,
                    "youtube_video_id": None,
                    "success": False,
                    "error": "Upload failed",
                }
            ]
        }

        with caplog.at_level(logging.WARNING):
            ti = _make_ti({"upload_results": upload_results})
            _mark_shorts_uploaded(ti, params={})

        mock_db.record_short_upload_failure.assert_not_called()
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        assert "reap_clip_id" in warnings[0].message


# ---------------------------------------------------------------------------
# _resolve_speakers unit tests
# ---------------------------------------------------------------------------


class TestResolveSpeakers:
    def test_resolve_speakers_uses_key_speakers_first(self):
        """key_speakers take priority; speakers adds non-duplicate real names to the pool."""
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers(
            {
                "key_speakers": ["Ana García", "Pedro López"],
                "speakers": ["María Ruiz"],
            }
        )
        # Pool: [Ana García, Pedro López, María Ruiz] — key_speakers first, then new speakers
        assert result == ("Ana García", "Pedro López, María Ruiz")

    def test_resolve_speakers_falls_back_to_speakers(self):
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"speakers": ["Xose Fernández", "Yolanda Díaz"]})
        assert result == ("Xose Fernández", "Yolanda Díaz")

    def test_resolve_speakers_key_speakers_empty_list(self):
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": [], "speakers": ["Zoila Martínez"]})
        assert result == ("Zoila Martínez", "")

    def test_resolve_speakers_empty_both(self):
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        assert _resolve_speakers({}) == ("", "")
        assert _resolve_speakers({"key_speakers": [], "speakers": []}) == ("", "")

    def test_resolve_speakers_single_speaker(self):
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": ["Pedro Sánchez"]})
        assert result == ("Pedro Sánchez", "")

    def test_resolve_speakers_none_values(self):
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": None, "speakers": None})
        assert result == ("", "")

    # -------------------------------------------------------------------------
    # Placeholder-aware _resolve_speakers spec scenarios (Task 1.4 RED)
    # -------------------------------------------------------------------------

    def test_key_speakers_real_name_wins_over_speakers_placeholder(self):
        """Spec scenario 1: key_speakers has a real name; speakers[0] is placeholder.

        GIVEN key_speakers=["Ana Martínez"], speakers=["Desconocido", "Pedro López"]
        THEN  primary="Ana Martínez", secondary="Pedro López"
        """
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers(
            {
                "key_speakers": ["Ana Martínez"],
                "speakers": ["Desconocido", "Pedro López"],
            }
        )
        assert result == ("Ana Martínez", "Pedro López"), f"Expected ('Ana Martínez', 'Pedro López'), got {result!r}"

    def test_placeholder_at_speakers_index_0_is_skipped(self):
        """Spec scenario 2: key_speakers empty; speakers[1] is real, speakers[0] is placeholder.

        GIVEN key_speakers=[], speakers=["Portavoz", "Pedro López"]
        THEN  primary="Pedro López", secondary=""
        """
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers(
            {
                "key_speakers": [],
                "speakers": ["Portavoz", "Pedro López"],
            }
        )
        assert result == ("Pedro López", ""), f"Expected ('Pedro López', ''), got {result!r}"

    def test_all_placeholders_returns_empty_sentinel(self):
        """Spec scenario 3: All speakers are placeholders → ("", ""), no crash.

        GIVEN key_speakers=["Desconocido"], speakers=["(No especificado)"]
        THEN  returns ("", "")
        """
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers(
            {
                "key_speakers": ["Desconocido"],
                "speakers": ["(No especificado)"],
            }
        )
        assert result == ("", ""), f"Expected ('', ''), got {result!r}"

    def test_both_arrays_empty_returns_empty_sentinel(self):
        """Spec scenario 4: Both arrays empty → ("", "").

        GIVEN key_speakers=[], speakers=[]
        THEN  returns ("", "")
        """
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": [], "speakers": []})
        assert result == ("", "")

    def test_only_key_speakers_real_speakers_empty(self):
        """Spec scenario 5: Only key_speakers has a real name; speakers is empty.

        GIVEN key_speakers=["Laura Gómez"], speakers=[]
        THEN  primary="Laura Gómez", secondary=""
        """
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers(
            {
                "key_speakers": ["Laura Gómez"],
                "speakers": [],
            }
        )
        assert result == ("Laura Gómez", ""), f"Expected ('Laura Gómez', ''), got {result!r}"

    # -------------------------------------------------------------------------
    # T7 (issue #433, D4) — preferred_primary kwarg promotes the resolved turn
    # speaker ahead of the key_speakers/speakers pool, deduplicating.
    # -------------------------------------------------------------------------

    def test_preferred_primary_promotes_over_pool(self):
        """GIVEN preferred_primary + an unrelated pool THEN it becomes primary."""
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": ["Otro Diputado"]}, preferred_primary="Ana Pérez")
        assert result == ("Ana Pérez", "Otro Diputado")

    def test_preferred_primary_dedups_when_already_in_pool(self):
        """GIVEN preferred_primary already present in the pool THEN no duplicate entry."""
        from congress_videos.reap_shorts_uploader_dag import _resolve_speakers

        result = _resolve_speakers({"key_speakers": ["Ana Pérez", "Otro Diputado"]}, preferred_primary="Ana Pérez")
        assert result == ("Ana Pérez", "Otro Diputado")


# ---------------------------------------------------------------------------
# Prompt template regression tests
# ---------------------------------------------------------------------------


class TestPromptTemplates:
    def test_old_speakers_placeholder_not_in_template(self):
        from congress_videos.config.ai_prompts import (
            SHORTS_METADATA_USER_PROMPT_TEMPLATE,
        )

        assert "{speakers}" not in SHORTS_METADATA_USER_PROMPT_TEMPLATE

    def test_system_prompt_contains_siempre_and_taxonomy_rule(self):
        from congress_videos.config.ai_prompts import SHORTS_METADATA_SYSTEM_PROMPT

        assert "SIEMPRE" in SHORTS_METADATA_SYSTEM_PROMPT
        assert "Nivel 1" in SHORTS_METADATA_SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# _generate_metadata integration: prompt contains primary speaker
# ---------------------------------------------------------------------------


class TestGenerateMetadataPrompt:
    def test_generate_metadata_prompt_includes_primary_speaker(self, mocker):
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = {
            "key_speakers": ["Pedro Sánchez"],
            "title": "Test Chapter",
            "topics": [],
            "scoring_reasoning": "",
        }

        mocker.patch("os.path.exists", return_value=True)

        mock_subprocess = mocker.patch("subprocess.run")
        mock_subprocess.return_value.returncode = 0

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto de prueba transcrito"},
        )

        captured: dict = {}

        def fake_generate_json_completion(system_prompt, user_prompt, **kwargs):
            captured["user_prompt"] = user_prompt
            return {
                "data": {
                    "title": "Pedro Sánchez debate vivienda",
                    "description": "Desc #Shorts",
                }
            }

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            side_effect=fake_generate_json_completion,
        )

        pending_shorts = [{"id": 1, "chapter_id": 42, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        assert "user_prompt" in captured
        user_prompt = captured["user_prompt"]
        assert "Pedro Sánchez" in user_prompt
        transcript_pos = user_prompt.find("TRANSCRIPCIÓN")
        speaker_pos = user_prompt.find("Pedro Sánchez")
        assert speaker_pos < transcript_pos, "primary_speaker must appear before TRANSCRIPCIÓN block"


# ---------------------------------------------------------------------------
# Shared factory for chapter metadata dicts used across footer/session tests
# ---------------------------------------------------------------------------


def _make_chapter_metadata(**overrides) -> dict:
    base = {
        "chapter_id": 1,
        "title": "Debate presupuestos",
        "description": "Descripción original",
        "key_speakers": ["Ana García"],
        "speakers": ["Ana García"],
        "topics": ["presupuestos"],
        "scoring_reasoning": "Alta relevancia",
        "relevance_score": 4,
        "youtube_video_id": None,
        "source_video_title": None,
        "source_video_url": None,
        "session_number": None,
        "session_date": None,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# _format_own_channel_footer — unit tests
# ---------------------------------------------------------------------------


class TestFormatOwnChannelFooter:
    def test_returns_own_channel_url_when_id_present(self):
        """Happy path: youtube_video_id set → footer with own-channel URL."""
        from congress_videos.reap_shorts_uploader_dag import _format_own_channel_footer

        result = _format_own_channel_footer("abc123")

        assert result == "\n\n📺 Vídeo completo:\nhttps://www.youtube.com/watch?v=abc123"

    def test_returns_empty_string_when_id_is_none(self):
        """Null guard: youtube_video_id=None → no footer (hard contract)."""
        from congress_videos.reap_shorts_uploader_dag import _format_own_channel_footer

        result = _format_own_channel_footer(None)

        assert result == ""

    def test_returns_empty_string_when_id_is_empty_string(self):
        """Empty-string guard: youtube_video_id='' → no footer."""
        from congress_videos.reap_shorts_uploader_dag import _format_own_channel_footer

        result = _format_own_channel_footer("")

        assert result == ""

    def test_source_url_never_appears_in_footer(self):
        """No fallback to source URL — only own-channel URL is used."""
        from congress_videos.reap_shorts_uploader_dag import _format_own_channel_footer

        result = _format_own_channel_footer("yt-XYZ")

        assert "youtube.com/watch?v=yt-XYZ" in result
        assert "Extraído de:" not in result


# ---------------------------------------------------------------------------
# _generate_metadata — own-channel footer integration tests
# ---------------------------------------------------------------------------


class TestGenerateMetadataFooter:
    def test_generate_metadata_footer_appended_when_youtube_video_id_set(self, mocker):
        """AC#4 — description ends with own-channel footer when youtube_video_id is set."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            youtube_video_id="yt-own-abc",
        )

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 1, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        metadata = ti.xcom_store["shorts_metadata"]
        description = metadata[0]["description"]
        expected_suffix = "\n\n📺 Vídeo completo:\nhttps://www.youtube.com/watch?v=yt-own-abc"
        assert description.endswith(expected_suffix), f"Description was: {description!r}"

    def test_generate_metadata_no_footer_when_youtube_video_id_null(self, mocker):
        """AC#5 — description has NO footer and source URL does NOT appear when youtube_video_id is None."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            youtube_video_id=None,
            source_video_url="https://youtube.com/watch?v=source-should-not-appear",
        )

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 2, "chapter_id": 20, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        metadata = ti.xcom_store["shorts_metadata"]
        description = metadata[0]["description"]
        assert "📺 Vídeo completo:" not in description
        assert "source-should-not-appear" not in description

    def test_generate_metadata_source_fields_not_in_ai_prompt(self, mocker):
        """AC#6 — source_video_title and source_video_url must NOT appear in the AI user prompt."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            youtube_video_id="yt-prompt-test",
            source_video_title="Sesión con fuente",
            source_video_url="https://youtube.com/watch?v=xyz789",
        )

        mocker.patch("os.path.exists", return_value=True)
        mock_subprocess = mocker.patch("subprocess.run")
        mock_subprocess.return_value.returncode = 0

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto transcrito de prueba"},
        )

        captured: dict = {}

        def fake_generate_json_completion(system_prompt, user_prompt, **kwargs):
            captured["user_prompt"] = user_prompt
            return {"data": {"title": "Título generado", "description": "Descripción AI"}}

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            side_effect=fake_generate_json_completion,
        )

        pending_shorts = [{"id": 3, "chapter_id": 30, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        assert "user_prompt" in captured, "generate_json_completion was not called"
        user_prompt = captured["user_prompt"]
        assert "source_video_title" not in user_prompt
        assert "source_video_url" not in user_prompt
        assert "Sesión con fuente" not in user_prompt
        assert "https://youtube.com/watch?v=xyz789" not in user_prompt


# ---------------------------------------------------------------------------
# _format_session_line — unit tests for all 4 spec scenarios (task 4.1)
# ---------------------------------------------------------------------------


class TestFormatSessionLine:
    def test_both_present_returns_full_line(self):
        """Spec scenario: both session_number and session_date present."""
        from datetime import date

        from congress_videos.reap_shorts_uploader_dag import _format_session_line

        result = _format_session_line(150, date(2024, 3, 12))

        assert result == "\n\n🏛️ Sesión nº 150 del Congreso - 12 de marzo de 2024"

    def test_number_only_returns_session_line(self):
        """Spec scenario: only session_number present."""
        from congress_videos.reap_shorts_uploader_dag import _format_session_line

        result = _format_session_line(42, None)

        assert result == "\n\n🏛️ Sesión nº 42 del Congreso"

    def test_date_only_returns_date_line(self):
        """Spec scenario: only session_date present."""
        from datetime import date

        from congress_videos.reap_shorts_uploader_dag import _format_session_line

        result = _format_session_line(None, date(2025, 11, 5))

        assert result == "\n\n🏛️ 5 de noviembre de 2025"

    def test_both_none_returns_empty_string(self):
        """Spec scenario: both values absent."""
        from congress_videos.reap_shorts_uploader_dag import _format_session_line

        result = _format_session_line(None, None)

        assert result == ""

    def test_all_twelve_months_in_spanish(self):
        """All months produce the correct Spanish name."""
        from datetime import date

        from congress_videos.reap_shorts_uploader_dag import _format_session_line

        expected_months = [
            "enero",
            "febrero",
            "marzo",
            "abril",
            "mayo",
            "junio",
            "julio",
            "agosto",
            "septiembre",
            "octubre",
            "noviembre",
            "diciembre",
        ]
        for month_idx, month_name in enumerate(expected_months, start=1):
            result = _format_session_line(None, date(2024, month_idx, 1))
            assert month_name in result, f"Month {month_idx} expected '{month_name}' in '{result}'"


# ---------------------------------------------------------------------------
# _generate_metadata — session line integration tests (tasks 4.3 and 4.4)
# ---------------------------------------------------------------------------


class TestGenerateMetadataSessionLine:
    def test_description_ends_with_session_suffix_when_data_available(self, mocker):
        """Task 4.3 — AI success + session data: description ends with Spanish session line."""
        from datetime import date

        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            session_number=80,
            session_date=date(2024, 6, 10),
        )

        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.run").return_value.returncode = 0
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto transcrito de prueba"},
        )
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            return_value={"data": {"title": "Titulo AI", "description": "Descripcion AI generada"}},
        )

        pending_shorts = [{"id": 1, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        metadata = ti.xcom_store["shorts_metadata"]
        description = metadata[0]["description"]
        expected_suffix = "\n\n🏛️ Sesión nº 80 del Congreso - 10 de junio de 2024"
        assert description.endswith(expected_suffix), f"Description was: {description!r}"

    def test_description_unchanged_when_session_data_null(self, mocker):
        """Task 4.4 — session_number=None + session_date=None: no session suffix appended."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            session_number=None,
            session_date=None,
        )

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 2, "chapter_id": 20, "local_file_path": "/fake/clip2.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        metadata = ti.xcom_store["shorts_metadata"]
        description = metadata[0]["description"]
        assert "🏛️ Sesión" not in description, f"Unexpected session suffix in: {description!r}"


# ---------------------------------------------------------------------------
# _check_short_upload_failures (t5) — post-DB monitoring task
# ---------------------------------------------------------------------------


class TestCheckShortUploadFailures:
    def test_raises_on_failed_details(self):
        from congress_videos.reap_shorts_uploader_dag import (
            _check_short_upload_failures,
        )

        upload_results = {
            "upload_details": [
                {"reap_clip_id": "c-ok", "youtube_video_id": "yt-ok", "success": True},
                {
                    "reap_clip_id": "c-fail",
                    "youtube_video_id": None,
                    "success": False,
                    "error": "Upload failed",
                },
            ]
        }
        ti = _make_ti({"upload_results": upload_results})
        with pytest.raises(Exception, match=r"short\(s\) failed to upload"):
            _check_short_upload_failures(ti, params={})

    def test_raises_on_success_with_missing_youtube_id(self):
        from congress_videos.reap_shorts_uploader_dag import (
            _check_short_upload_failures,
        )

        upload_results = {
            "upload_details": [
                {"reap_clip_id": "c-ok", "youtube_video_id": None, "success": True},
            ]
        }
        ti = _make_ti({"upload_results": upload_results})
        with pytest.raises(Exception, match=r"short\(s\) failed to upload"):
            _check_short_upload_failures(ti, params={})

    def test_all_success_does_not_raise(self):
        from congress_videos.reap_shorts_uploader_dag import (
            _check_short_upload_failures,
        )

        upload_results = {
            "upload_details": [
                {"reap_clip_id": "c-1", "youtube_video_id": "yt-a", "success": True},
                {"reap_clip_id": "c-2", "youtube_video_id": "yt-b", "success": True},
            ]
        }
        ti = _make_ti({"upload_results": upload_results})
        _check_short_upload_failures(ti, params={})

    def test_empty_details_noop(self):
        from congress_videos.reap_shorts_uploader_dag import (
            _check_short_upload_failures,
        )

        ti = _make_ti({"upload_results": {"upload_details": []}})
        _check_short_upload_failures(ti, params={})

    def test_missing_xcom_raises(self):
        from congress_videos.reap_shorts_uploader_dag import (
            _check_short_upload_failures,
        )

        ti = _make_ti({})
        with pytest.raises(Exception, match="Upload results XCom missing"):
            _check_short_upload_failures(ti, params={})


# ---------------------------------------------------------------------------
# _generate_metadata — AI success vs. fallback-on-error DAG-boundary coverage
# (issue #365: no production change here, this DAG is a test subject only)
# ---------------------------------------------------------------------------


class TestGenerateMetadataAiOutcome:
    def test_ai_title_and_description_used(self, mocker):
        """AI metadata parses the expected JSON on success (issue #365 spec)."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata()

        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.run").return_value.returncode = 0
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto transcrito de prueba"},
        )
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            return_value={
                "data": {
                    "title": "Título generado por IA",
                    "description": "Descripción generada por IA",
                },
                "error": None,
            },
        )

        pending_shorts = [{"id": 1, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        metadata = ti.xcom_store["shorts_metadata"]
        assert metadata[0]["title"] == "Título generado por IA"
        assert metadata[0]["description"].startswith("Descripción generada por IA")

    def test_fallback_and_warning_on_ai_error(self, mocker, caplog):
        """AI failure preserves the pre-computed fallback and logs a warning
        (issue #365 spec) — this exercises the UNMODIFIED fallback branch at
        `reap_shorts_uploader_dag.py:252-256`. The task must still SUCCEED.
        """
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db_cls.return_value.get_chapter_metadata.return_value = _make_chapter_metadata(
            title="Debate presupuestos",
            key_speakers=["Ana García"],
        )

        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.run").return_value.returncode = 0
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto transcrito de prueba"},
        )
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            return_value={"data": None, "error": "boom"},
        )

        pending_shorts = [{"id": 7, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})

        with caplog.at_level(logging.WARNING):
            _generate_metadata(ti)  # must not raise — task succeeds on AI failure

        metadata = ti.xcom_store["shorts_metadata"]
        assert metadata[0]["title"] == "Ana García: Debate presupuestos #Shorts"
        assert metadata[0]["description"].startswith("🏛️ Debate en el Congreso de los Diputados.")
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("7" in w and "boom" in w for w in warnings)


# ---------------------------------------------------------------------------
# build_shorts_metadata_context — pure function unit tests (issue #433, T3/T4)
# ---------------------------------------------------------------------------


def _lookup_stub(roster: dict):
    """Stub participants_lookup: slug -> {"display_name": ...} | None.

    The returned callable records every slug it was asked to resolve on its
    `.calls` attribute, so tests can assert no-lookup-call behaviour.
    """
    calls: list[str] = []

    def _fn(slug):
        calls.append(slug)
        return roster.get(slug)

    _fn.calls = calls
    return _fn


class TestBuildShortsMetadataContext:
    def test_multi_mention_preserves_order_and_identity(self):
        """Spec: 'Multiple mentioned people preserve order and identity'."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {
            "ana-perez": {"display_name": "Ana Pérez"},
            "luis-gomez": {"display_name": "Luis Gómez"},
            "maria-ruiz": {"display_name": "María Ruiz"},
        }
        chapter = {"mentioned_participant_slugs": ["ana-perez", "luis-gomez", "maria-ruiz"]}

        result = build_shorts_metadata_context(chapter, None, _lookup_stub(roster))

        assert result["mentioned_display_names"] == ["Ana Pérez", "Luis Gómez", "María Ruiz"]

    def test_unknown_slug_dropped_not_rendered_raw(self):
        """Spec: 'Unresolvable slug dropped, not rendered raw'."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {"ana-perez": {"display_name": "Ana Pérez"}}
        chapter = {"mentioned_participant_slugs": ["ana-perez", "unknown-slug"]}

        result = build_shorts_metadata_context(chapter, None, _lookup_stub(roster))

        assert result["mentioned_display_names"] == ["Ana Pérez"]
        assert "unknown-slug" not in result["mentioned_display_names"]

    def test_speaker_excluded_from_mentioned_by_slug(self):
        """Spec: 'Speaker excluded from mentioned-people section'."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {
            "ana-perez": {"display_name": "Ana Pérez"},
            "luis-gomez": {"display_name": "Luis Gómez"},
        }
        chapter = {"mentioned_participant_slugs": ["ana-perez", "luis-gomez"]}

        result = build_shorts_metadata_context(chapter, "ana-perez", _lookup_stub(roster))

        assert result["speaker_display_name"] == "Ana Pérez"
        assert result["mentioned_display_names"] == ["Luis Gómez"]

    def test_lookup_raise_for_mentioned_slug_is_swallowed(self):
        """D4/D5: 'participants_lookup raises' — that slug becomes unresolvable, no raise."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        def _raising_lookup(slug):
            if slug == "boom-slug":
                raise RuntimeError("roster unavailable")
            return {"display_name": "Luis Gómez"} if slug == "luis-gomez" else None

        chapter = {"mentioned_participant_slugs": ["boom-slug", "luis-gomez"]}

        result = build_shorts_metadata_context(chapter, None, _raising_lookup)

        assert result["mentioned_display_names"] == ["Luis Gómez"]

    def test_null_mentioned_participant_slugs_yields_empty_list(self):
        """Spec: NULL mentioned_participant_slugs (never analysed) → []."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        chapter = {"mentioned_participant_slugs": None}

        result = build_shorts_metadata_context(chapter, None, _lookup_stub({}))

        assert result["mentioned_display_names"] == []

    def test_empty_mentioned_participant_slugs_yields_empty_list(self):
        """Spec: analysed-but-empty mentioned_participant_slugs behaves like NULL."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, None, _lookup_stub({}))

        assert result["mentioned_display_names"] == []

    def test_turn_speaker_slug_none_yields_empty_speaker_and_no_lookup_call(self):
        """D5: turn_speaker_slug=None → empty speaker name with no lookup call."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        lookup = _lookup_stub({})
        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, None, lookup)

        assert result["speaker_display_name"] == ""
        assert lookup.calls == []

    def test_unresolvable_speaker_slug_yields_empty_speaker(self):
        """Spec: 'Falls back to heuristic when slug is unresolvable' (speaker side)."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, "off-roster-slug", _lookup_stub({}))

        assert result["speaker_display_name"] == ""

    def test_topic_never_enters_mentioned_or_speaker(self):
        """T4 — spec: 'A topic never renders as a person'."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {"ana-perez": {"display_name": "Ana Pérez"}}
        chapter = {
            "mentioned_participant_slugs": ["ana-perez"],
            "topics": ["Pedro Sánchez", "presupuestos"],
        }

        result = build_shorts_metadata_context(chapter, None, _lookup_stub(roster))

        assert result["topics"] == ["Pedro Sánchez", "presupuestos"]
        assert "Pedro Sánchez" not in result["mentioned_display_names"]
        assert result["speaker_display_name"] != "Pedro Sánchez"

    def test_mapped_turn_speaker_slug_uses_catalogue_over_lookup(self):
        """Issue #511 slice 5, design D5: catalogue wins over participants_lookup."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        # The roster would resolve a different (uncurated) full name — the
        # catalogue must win, so participants_lookup's value never surfaces.
        roster = {"pedro-sanchez-perez-castejon": {"display_name": "Pedro Sánchez Pérez-Castejón"}}
        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, "pedro-sanchez-perez-castejon", _lookup_stub(roster))

        assert result["speaker_display_name"] == "Sánchez"

    def test_unmapped_turn_speaker_slug_falls_through_to_lookup_unchanged(self):
        """Design D5: catalogue miss falls through to today's participants_lookup behaviour."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {"ana-perez": {"display_name": "Ana Pérez"}}
        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, "ana-perez", _lookup_stub(roster))

        assert result["speaker_display_name"] == "Ana Pérez"

    def test_none_turn_speaker_slug_never_consults_catalogue(self):
        """Design D5: a None slug is byte-identical to today — no catalogue lookup either."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        lookup = _lookup_stub({})
        chapter = {"mentioned_participant_slugs": []}

        result = build_shorts_metadata_context(chapter, None, lookup)

        assert result["speaker_display_name"] == ""
        assert lookup.calls == []

    def test_mentioned_people_never_canonicalised(self):
        """Spec: only the resolved speaker is canonicalised, never mentioned people."""
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {"pedro-sanchez-perez-castejon": {"display_name": "Pedro Sánchez Pérez-Castejón"}}
        chapter = {"mentioned_participant_slugs": ["pedro-sanchez-perez-castejon"]}

        result = build_shorts_metadata_context(chapter, None, _lookup_stub(roster))

        assert result["mentioned_display_names"] == ["Pedro Sánchez Pérez-Castejón"]
        assert "Sánchez" not in result["mentioned_display_names"]


class TestShortsCrossSeamDisplayNameConsistency:
    """Issue #511 slice 5: extends slice 4's title/art-direction cross-seam
    proof (spec "Same slug, same name across seams") to the shorts seam. All
    three seams consult canonical_display_name for the same mapped slug, so
    they must render the identical curated name."""

    def test_shorts_speaker_matches_title_and_art_direction_for_mapped_slug(self, mocker):
        from congress_videos.modules.thumbnail_generation import resolved_photo_speaker_name
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        participant_slug = "pedro-sanchez-perez-castejon"
        key_speakers = ["Pedro Sánchez"]

        art_name = resolved_photo_speaker_name({"source": "photo"}, key_speakers, participant_slug=participant_slug)

        chapter = {"mentioned_participant_slugs": []}
        roster = {participant_slug: {"display_name": "Pedro Sánchez Pérez-Castejón"}}
        shorts_result = build_shorts_metadata_context(chapter, participant_slug, _lookup_stub(roster))

        assert art_name == "Sánchez"
        assert shorts_result["speaker_display_name"] == art_name


# ---------------------------------------------------------------------------
# _generate_metadata — empty-metadata byte-compatibility (issue #433, T5)
# ---------------------------------------------------------------------------


class TestGenerateMetadataByteCompatibility:
    def test_empty_metadata_prompt_byte_identical_to_template(self, mocker):
        """Spec: 'Empty metadata is byte-compatible with today's output' (AC4)."""
        from congress_videos.config.ai_prompts import SHORTS_METADATA_USER_PROMPT_TEMPLATE
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = _make_chapter_metadata(
            key_speakers=[],
            speakers=[],
            topics=[],
            mentioned_participant_slugs=None,
        )

        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.run").return_value.returncode = 0
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto de prueba"},
        )

        captured: dict = {}

        def fake_generate_json_completion(system_prompt, user_prompt, **kwargs):
            captured["user_prompt"] = user_prompt
            return {"data": None, "error": "not used"}

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            side_effect=fake_generate_json_completion,
        )

        pending_shorts = [{"id": 1, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        mock_db.get_turn_speaker_slug.assert_not_called()
        expected_prompt = SHORTS_METADATA_USER_PROMPT_TEMPLATE.format(
            transcript="Texto de prueba"[:2000],
            chapter_title="Debate presupuestos",
            primary_speaker="",
            secondary_speakers="",
            topics="Debate parlamentario",
            scoring_reasoning="Alta relevancia"[:500],
        )
        assert captured["user_prompt"] == expected_prompt
        assert "PERSONAS MENCIONADAS" not in captured["user_prompt"]

        metadata = ti.xcom_store["shorts_metadata"]
        assert metadata[0]["title"] == "Debate presupuestos #Shorts"


# ---------------------------------------------------------------------------
# _generate_metadata — turn speaker precedence integration (issue #433, T6)
# ---------------------------------------------------------------------------


class TestGenerateMetadataTurnSpeakerPrecedence:
    def test_resolved_slug_beats_key_speakers_in_prompt(self, mocker):
        """Spec: 'Resolved slug takes precedence over heuristic'; mentioned block once, no raw slug."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = _make_chapter_metadata(
            key_speakers=["Otro Diputado"],
            speakers=["Otro Diputado"],
            mentioned_participant_slugs=["luis-gomez"],
        )
        mock_db.get_turn_speaker_slug.return_value = {
            "turn_id": 42,
            "resolved_participant_slug": "ana-perez",
            "speaker_resolution_confidence": 0.97,
            "speaker_resolution_method": "llm",
        }

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.lookup_participant_by_slug",
            side_effect=lambda slug: {
                "ana-perez": {"display_name": "Ana Pérez"},
                "luis-gomez": {"display_name": "Luis Gómez"},
            }.get(slug),
        )
        mocker.patch("os.path.exists", return_value=True)
        mocker.patch("subprocess.run").return_value.returncode = 0
        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.transcribe_audio_file",
            return_value={"success": True, "text": "Texto de prueba"},
        )

        captured: dict = {}

        def fake_generate_json_completion(system_prompt, user_prompt, **kwargs):
            captured["user_prompt"] = user_prompt
            return {"data": {"title": "T", "description": "D"}}

        mocker.patch(
            "congress_videos.reap_shorts_uploader_dag.generate_json_completion",
            side_effect=fake_generate_json_completion,
        )

        pending_shorts = [{"id": 1, "chapter_id": 10, "turn_id": 42, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        mock_db.get_turn_speaker_slug.assert_called_once_with(42)
        user_prompt = captured["user_prompt"]
        assert "PONENTE PRINCIPAL: Ana Pérez" in user_prompt
        assert "PONENTE PRINCIPAL: Otro Diputado" not in user_prompt
        assert user_prompt.count("PERSONAS MENCIONADAS") == 1
        assert "Luis Gómez" in user_prompt
        assert "ana-perez" not in user_prompt
        assert "luis-gomez" not in user_prompt

    def test_accessor_exception_falls_back_to_heuristic_no_raise(self, mocker, caplog):
        """D5: accessor raises (DB error) → caught at call site, WARNING, today's output."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = _make_chapter_metadata(
            key_speakers=["Ana García"],
        )
        mock_db.get_turn_speaker_slug.side_effect = RuntimeError("connection reset")

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 9, "chapter_id": 10, "turn_id": 99, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})

        with caplog.at_level(logging.WARNING):
            _generate_metadata(ti)  # must not raise

        metadata = ti.xcom_store["shorts_metadata"]
        assert metadata[0]["title"] == "Ana García: Debate presupuestos #Shorts"
        warnings = [r.message for r in caplog.records if r.levelno == logging.WARNING]
        assert any("99" in w and "connection reset" in w for w in warnings)


# ---------------------------------------------------------------------------
# _generate_metadata — legacy short without turn_id never queries the
# turn-speaker accessor (issue #433, T6b)
# ---------------------------------------------------------------------------


class TestGenerateMetadataLegacyShortNoTurnId:
    def test_turn_id_key_absent_never_calls_accessor(self, mocker):
        """Spec: 'Legacy short has no turn_id' — key absent falls through to heuristic."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = _make_chapter_metadata(
            key_speakers=["Ana García"],
        )

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 1, "chapter_id": 10, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        mock_db.get_turn_speaker_slug.assert_not_called()
        metadata = ti.xcom_store["shorts_metadata"]
        assert metadata[0]["title"] == "Ana García: Debate presupuestos #Shorts"

    def test_turn_id_none_never_calls_accessor(self, mocker):
        """Spec: 'Legacy short has no turn_id' — explicit turn_id=None, same fall-through."""
        from congress_videos.reap_shorts_uploader_dag import _generate_metadata

        mock_db_cls = mocker.patch("congress_videos.reap_shorts_uploader_dag.CongressionalVideoDB")
        mock_db = mock_db_cls.return_value
        mock_db.get_chapter_metadata.return_value = _make_chapter_metadata(
            key_speakers=["Ana García"],
        )

        mocker.patch("os.path.exists", return_value=False)

        pending_shorts = [{"id": 2, "chapter_id": 10, "turn_id": None, "local_file_path": "/fake/clip.mp4"}]
        ti = _make_ti({"pending_shorts": pending_shorts})
        _generate_metadata(ti)

        mock_db.get_turn_speaker_slug.assert_not_called()


class TestSpeakerExclusionSurvivesCanonicalShortening:
    """Issue #511: shortening the speaker's name must not break mentioned-people dedup.

    Mentioned people always render their FULL display name. The speaker is
    excluded from that list both by slug identity and by case-folded
    display-name equality. Once the catalogue shortens the speaker to a bare
    surname, comparing mentioned people against only the shortened form stops
    matching, so a duplicate participant record for the same person would be
    listed as speaker AND as mentioned. The speaker must be excluded on either
    form.
    """

    def test_duplicate_record_of_speaker_excluded_despite_short_form(self):
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        full_name = "Sánchez Pérez-Castejón, Pedro"
        roster = {
            "pedro-sanchez-perez-castejon": {"display_name": full_name},
            # Same human, second participant row under a different slug.
            "pedro-sanchez-duplicate-row": {"display_name": full_name},
            "ana-perez": {"display_name": "Ana Pérez"},
        }
        chapter = {
            "mentioned_participant_slugs": [
                "pedro-sanchez-duplicate-row",
                "ana-perez",
            ]
        }

        result = build_shorts_metadata_context(chapter, "pedro-sanchez-perez-castejon", _lookup_stub(roster))

        # The catalogue still shortens what is rendered for the speaker.
        assert result["speaker_display_name"] == "Sánchez"
        # ...and the duplicate row of that same person is still excluded.
        assert result["mentioned_display_names"] == ["Ana Pérez"]

    def test_unmapped_speaker_dedup_unchanged(self):
        from congress_videos.reap_shorts_uploader_dag import build_shorts_metadata_context

        roster = {
            "ana-perez": {"display_name": "Ana Pérez"},
            "ana-perez-duplicate-row": {"display_name": "Ana Pérez"},
            "luis-gomez": {"display_name": "Luis Gómez"},
        }
        chapter = {"mentioned_participant_slugs": ["ana-perez-duplicate-row", "luis-gomez"]}

        result = build_shorts_metadata_context(chapter, "ana-perez", _lookup_stub(roster))

        assert result["speaker_display_name"] == "Ana Pérez"
        assert result["mentioned_display_names"] == ["Luis Gómez"]
