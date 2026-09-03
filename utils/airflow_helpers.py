# dags/repo/utils/airflow_helpers.py
import os
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timezone
from typing import Any, Optional

from airflow.models.taskinstance import TaskInstance


def xcom_task(ti: TaskInstance, func: Callable, output_key: str, input_key: str | None = None, branch: bool = False):
    """
    Generic wrapper to run a callable, push result to XCom, and optionally branch.

    :param ti: Airflow TaskInstance (injected automatically by PythonOperator)
    :param func: Callable to execute
    :param output_key: XCom key to push the result under
    :param input_key: Optional XCom key to pull input from
    :param branch: If True, return the result as a branch task_id
    """
    # Pull input from XCom if needed
    if input_key:
        value = ti.xcom_pull(key=input_key)
        result = func(value)
    else:
        result = func()

    # Push result to XCom
    ti.xcom_push(key=output_key, value=result)

    # If this is a branch task, return the branch decision
    if branch:
        return result


def ensure_project_data_directory(project_name: str, base_data_path: str = "/opt/airflow/data") -> str:
    """
    Check if a project-specific data directory exists and create it if not.

    :param project_name: Name of the project (e.g., 'congreso_youtube')
    :param base_data_path: Base path for data directories (default: '/opt/airflow/data')
    :return: Full path to the project data directory
    """
    project_data_path = os.path.join(base_data_path, project_name)

    if not os.path.exists(project_data_path):
        os.makedirs(project_data_path, exist_ok=True)
        print(f"Created project data directory: {project_data_path}")
    else:
        print(f"Project data directory already exists: {project_data_path}")

    return project_data_path


def _to_utc(value: Any) -> Any:
    """Return a UTC-offset datetime, or the value unchanged if not a datetime.

    Naive values get UTC ATTACHED (never .astimezone(), which would silently
    assume system-local time). date (non-datetime) and all other types pass
    through untouched.
    """
    if not isinstance(value, datetime):
        return value
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def utc_normalize_row(row: Any) -> Any:
    """Return a new dict with every datetime value normalized to UTC.

    psycopg2 returns TIMESTAMPTZ as a datetime whose tzinfo is an unnamed
    non-UTC fixed offset. Airflow 2.10.2 serializes that with an empty tz
    name and then raises ValueError on every xcom_pull (issues #163, #303).
    A UTC offset (0) takes the working "UTC" branch of
    airflow/serialization/serializers/timezone.py and round-trips cleanly.

    Pure: never mutates the input. Non-dict input passes through unchanged.
    Only the row's own values are inspected (flat rows); nested containers
    are intentionally not walked.
    """
    if not isinstance(row, dict):
        return row
    return {key: _to_utc(value) for key, value in row.items()}


def utc_normalize_rows(rows: Iterable[Any]) -> list:
    """Return a new list with utc_normalize_row applied to each row.

    Deliberately has no None guard: callers must supply an iterable. A
    silent [] on None would hide a data-layer failure as "nothing to do";
    the TypeError is louder and therefore safer.
    """
    return [utc_normalize_row(row) for row in rows]
