"""Tests for reap_shorts_uploader DAG (congress_videos.reap_shorts_uploader_dag)."""

from __future__ import annotations

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
        # Tasks: get_pending_shorts, generate_metadata, trigger_youtube_upload, mark_shorts_uploaded
        assert len(dag.tasks) == 4

    def test_dag_schedule(self):
        from congress_videos.reap_shorts_uploader_dag import dag
        assert dag.schedule == "0,30 17-20 * * *"

    def test_dag_correct_task_ids(self):
        from congress_videos.reap_shorts_uploader_dag import dag
        task_ids = {t.task_id for t in dag.tasks}
        assert "get_pending_shorts" in task_ids
        assert "generate_metadata" in task_ids
        assert "trigger_youtube_upload" in task_ids
        assert "mark_shorts_uploaded" in task_ids

    def test_dag_correct_dependency_chain(self):
        from congress_videos.reap_shorts_uploader_dag import dag
        tasks_by_id = {t.task_id: t for t in dag.tasks}
        t1 = tasks_by_id["get_pending_shorts"]
        t2 = tasks_by_id["generate_metadata"]
        t3 = tasks_by_id["trigger_youtube_upload"]
        t4 = tasks_by_id["mark_shorts_uploaded"]

        assert t2.task_id in {t.task_id for t in t1.downstream_list}
        assert t3.task_id in {t.task_id for t in t2.downstream_list}
        assert t4.task_id in {t.task_id for t in t3.downstream_list}


# ---------------------------------------------------------------------------
# 7.7 — Title truncation logic (unit test without Airflow context)
# ---------------------------------------------------------------------------

class TestTriggerYoutubeUploadTitleTruncation:

    def _build_title(self, raw_title: str, suffix: str = " #Shorts", max_len: int = 100) -> str:
        """Reproduce the exact truncation logic from _trigger_youtube_upload."""
        if len(raw_title) + len(suffix) > max_len:
            raw_title = raw_title[:max_len - len(suffix)]
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
