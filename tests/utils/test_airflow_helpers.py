"""Tests for utils.airflow_helpers module."""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from airflow.utils.json import XComDecoder, XComEncoder

from utils.airflow_helpers import (
    ensure_project_data_directory,
    utc_normalize_row,
    utc_normalize_rows,
    xcom_task,
)


def _xcom_round_trip(value):
    """Byte-identical to Airflow's real XCom push+pull serialization path."""
    return json.loads(json.dumps(value, cls=XComEncoder), cls=XComDecoder)


# ---------------------------------------------------------------------------
# xcom_task — no input_key (func called with no args)
# ---------------------------------------------------------------------------

class TestXcomTaskNoInputKey:

    def test_func_called_without_args(self, mock_task_instance):
        func = MagicMock(return_value="result_value")
        xcom_task(mock_task_instance, func, output_key="my_key")
        func.assert_called_once_with()

    def test_result_pushed_to_xcom_under_output_key(self, mock_task_instance):
        func = MagicMock(return_value="pushed_value")
        xcom_task(mock_task_instance, func, output_key="out")
        assert mock_task_instance.xcom_store["out"] == "pushed_value"

    def test_xcom_pull_not_called_when_no_input_key(self, mock_task_instance):
        func = MagicMock(return_value="x")
        xcom_task(mock_task_instance, func, output_key="out")
        mock_task_instance.xcom_pull.assert_not_called()

    def test_returns_none_when_branch_false(self, mock_task_instance):
        func = MagicMock(return_value="branch_id")
        result = xcom_task(mock_task_instance, func, output_key="out", branch=False)
        assert result is None

    def test_func_result_is_none_still_pushes(self, mock_task_instance):
        func = MagicMock(return_value=None)
        xcom_task(mock_task_instance, func, output_key="none_key")
        assert "none_key" in mock_task_instance.xcom_store
        assert mock_task_instance.xcom_store["none_key"] is None


# ---------------------------------------------------------------------------
# xcom_task — with input_key (pulls then calls func with value)
# ---------------------------------------------------------------------------

class TestXcomTaskWithInputKey:

    def test_xcom_pull_called_with_input_key(self, mock_task_instance):
        mock_task_instance.xcom_store["in_key"] = "input_data"
        func = MagicMock(return_value="processed")
        xcom_task(mock_task_instance, func, output_key="out", input_key="in_key")
        mock_task_instance.xcom_pull.assert_called_once_with(key="in_key")

    def test_func_receives_pulled_value(self, mock_task_instance):
        mock_task_instance.xcom_store["in_key"] = {"data": [1, 2, 3]}
        func = MagicMock(return_value="done")
        xcom_task(mock_task_instance, func, output_key="out", input_key="in_key")
        func.assert_called_once_with({"data": [1, 2, 3]})

    def test_result_pushed_with_transformed_value(self, mock_task_instance):
        mock_task_instance.xcom_store["raw"] = "hello"
        func = MagicMock(return_value="HELLO")
        xcom_task(mock_task_instance, func, output_key="transformed", input_key="raw")
        assert mock_task_instance.xcom_store["transformed"] == "HELLO"

    def test_pull_missing_key_returns_none_passed_to_func(self, mock_task_instance):
        # Key not in store → xcom_pull returns None
        func = MagicMock(return_value="handled_none")
        xcom_task(mock_task_instance, func, output_key="out", input_key="nonexistent")
        func.assert_called_once_with(None)


# ---------------------------------------------------------------------------
# xcom_task — branch=True returns the result
# ---------------------------------------------------------------------------

class TestXcomTaskBranchMode:

    def test_branch_true_returns_func_result(self, mock_task_instance):
        func = MagicMock(return_value="task_a")
        result = xcom_task(mock_task_instance, func, output_key="branch_out", branch=True)
        assert result == "task_a"

    def test_branch_true_also_pushes_to_xcom(self, mock_task_instance):
        func = MagicMock(return_value="task_b")
        xcom_task(mock_task_instance, func, output_key="branch_key", branch=True)
        assert mock_task_instance.xcom_store["branch_key"] == "task_b"

    def test_branch_true_with_input_key(self, mock_task_instance):
        mock_task_instance.xcom_store["check"] = "value"
        func = MagicMock(return_value="go_left")
        result = xcom_task(
            mock_task_instance, func, output_key="decision", input_key="check", branch=True
        )
        assert result == "go_left"
        func.assert_called_once_with("value")

    def test_branch_false_is_default(self, mock_task_instance):
        func = MagicMock(return_value="something")
        result = xcom_task(mock_task_instance, func, output_key="out")
        assert result is None


# ---------------------------------------------------------------------------
# ensure_project_data_directory
# ---------------------------------------------------------------------------

class TestEnsureProjectDataDirectory:

    def test_creates_directory_if_not_exists(self, tmp_path):
        base = str(tmp_path / "airflow_data")
        result = ensure_project_data_directory("my_project", base_data_path=base)
        assert os.path.isdir(result)

    def test_returns_full_project_path(self, tmp_path):
        base = str(tmp_path)
        result = ensure_project_data_directory("congreso_youtube", base_data_path=base)
        assert result == os.path.join(base, "congreso_youtube")

    def test_idempotent_when_directory_exists(self, tmp_path):
        base = str(tmp_path)
        # Create once
        ensure_project_data_directory("project", base_data_path=base)
        # Call again — must not raise
        result = ensure_project_data_directory("project", base_data_path=base)
        assert os.path.isdir(result)

    def test_uses_default_base_path(self, mocker):
        # Patch os.path.exists and os.makedirs to avoid filesystem side effects
        mocker.patch("os.path.exists", return_value=False)
        mocker.patch("os.makedirs")
        result = ensure_project_data_directory("test_project")
        assert result == "/opt/airflow/data/test_project"


# ---------------------------------------------------------------------------
# utc_normalize_row / utc_normalize_rows (issue #303)
# ---------------------------------------------------------------------------


class TestUtcNormalizeRows:

    def test_non_utc_offset_datetime_becomes_utc_offset(self):
        original = datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))
        row = {"chapter_id": 42, "youtube_video_id": "abc123", "youtube_upload_date": original}

        result = utc_normalize_row(row)

        normalized = result["youtube_upload_date"]
        assert normalized.utcoffset() == timedelta(0)
        assert normalized == original
        assert result["chapter_id"] == 42
        assert result["youtube_video_id"] == "abc123"

    def test_naive_datetime_gets_utc_attached_not_converted(self):
        naive = datetime(2026, 8, 20, 10, 0)
        row = {"youtube_upload_date": naive}

        result = utc_normalize_row(row)

        normalized = result["youtube_upload_date"]
        assert normalized.tzinfo == timezone.utc
        assert (normalized.year, normalized.month, normalized.day) == (2026, 8, 20)
        assert (normalized.hour, normalized.minute) == (10, 0)

    def test_non_datetime_values_pass_through(self):
        row = {
            "chapter_id": 7,
            "youtube_video_id": "xyz",
            "note": None,
            "published_on": date(2026, 8, 20),
        }

        result = utc_normalize_row(row)

        assert result["chapter_id"] == 7
        assert result["youtube_video_id"] == "xyz"
        assert result["note"] is None
        assert result["published_on"] == date(2026, 8, 20)

    def test_input_rows_are_not_mutated(self):
        original_dt = datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))
        row = {"youtube_upload_date": original_dt}

        result = utc_normalize_row(row)

        assert row["youtube_upload_date"] is original_dt
        assert row["youtube_upload_date"].utcoffset() == timedelta(hours=2)
        assert result is not row

    def test_non_dict_row_passes_through(self):
        assert utc_normalize_row("not-a-dict") == "not-a-dict"
        assert utc_normalize_row(None) is None

    def test_rows_list_normalized(self):
        rows = [
            {"youtube_upload_date": datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))},
            {"youtube_upload_date": None},
        ]

        result = utc_normalize_rows(rows)

        assert result[0]["youtube_upload_date"].utcoffset() == timedelta(0)
        assert result[1]["youtube_upload_date"] is None
        assert result is not rows


# ---------------------------------------------------------------------------
# Real-serializer round-trip regression (issue #303, bug-pinning + fix-proving)
# ---------------------------------------------------------------------------


class TestXComSerializerRoundTrip:

    def test_raw_non_utc_offset_row_breaks_xcom_round_trip(self):
        """Bug-pin: an UN-normalized non-UTC offset datetime always breaks the
        real XCom serializer round-trip, both before and after the fix — this
        test must keep failing the same way forever, it never turns green."""
        row = {
            "chapter_id": 42,
            "youtube_video_id": "abc123",
            "youtube_upload_date": datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2))),
        }

        with pytest.raises(ValueError):
            _xcom_round_trip([row])

    def test_normalized_row_survives_xcom_round_trip(self):
        original = datetime(2026, 8, 20, 10, 0, tzinfo=timezone(timedelta(hours=2)))
        row = {
            "chapter_id": 42,
            "youtube_video_id": "abc123",
            "youtube_upload_date": original,
        }

        result = _xcom_round_trip(utc_normalize_rows([row]))

        pushed_date = result[0]["youtube_upload_date"]
        assert isinstance(pushed_date, datetime)
        assert pushed_date.utcoffset() == timedelta(0)
        assert pushed_date == original

    def test_offset_zero_round_trips_before_and_after_normalization(self):
        row = {
            "chapter_id": 1,
            "youtube_upload_date": datetime(2026, 8, 20, 10, 0, tzinfo=timezone.utc),
        }

        raw_result = _xcom_round_trip([row])
        normalized_result = _xcom_round_trip(utc_normalize_rows([row]))

        assert raw_result[0]["youtube_upload_date"] == row["youtube_upload_date"]
        assert normalized_result[0]["youtube_upload_date"] == row["youtube_upload_date"]
