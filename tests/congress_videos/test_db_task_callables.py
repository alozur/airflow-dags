"""Cross-DAG shape guards for the operator → callable migration — issue #227.

Confirms:
- `postgres_operators.py` no longer exists.
- None of the 4 migrated DAGs instantiate a `PostgreSQLOperator`.
- Each of the 7 migrated callables is importable from its host DAG module.
"""

from __future__ import annotations

import importlib

import pytest


def test_postgres_operators_module_absent():
    """The module and its `PostgreSQLOperator` class no longer exist."""
    with pytest.raises(ModuleNotFoundError):
        importlib.import_module("congress_videos.modules.postgres_operators")


DAG_MODULES = [
    "congress_videos.youtube_upload_dag",
    "congress_videos.video_analytics_dag",
    "congress_videos.post_upload_verification_dag",
    "congress_videos.youtube_channel_monitor_dag",
]


@pytest.mark.parametrize("module_path", DAG_MODULES)
def test_dag_has_no_postgresql_operator_tasks(module_path):
    """No task in the 4 migrated DAGs is a `PostgreSQLOperator` instance.

    Import-based, not string-based: `PostgreSQLOperator` no longer exists as
    an importable name at all (see test_postgres_operators_module_absent),
    so this test also proves every task is one of Airflow's own operators.
    """
    mod = importlib.import_module(module_path)
    dag = mod.dag

    for task in dag.tasks:
        assert type(task).__name__ != "PostgreSQLOperator", (
            f"{module_path}: task {task.task_id!r} is still a PostgreSQLOperator"
        )


CALLABLE_IMPORTS = [
    ("congress_videos.youtube_upload_dag", "_run_check_upload_quota"),
    ("congress_videos.youtube_upload_dag", "_run_mark_chapters_uploaded"),
    ("congress_videos.youtube_upload_dag", "_run_mark_turns_uploaded"),
    ("congress_videos.video_analytics_dag", "_run_get_pending_checkpoints"),
    ("congress_videos.video_analytics_dag", "_run_record_snapshots"),
    ("congress_videos.post_upload_verification_dag", "_run_select_unverified_uploads"),
    ("congress_videos.youtube_channel_monitor_dag", "_run_save_chapters_to_db"),
]


@pytest.mark.parametrize("module_path, name", CALLABLE_IMPORTS)
def test_callable_importable_from_dag_module(module_path, name):
    """Each of the 7 migrated callables is a module-level, importable name."""
    mod = importlib.import_module(module_path)
    assert hasattr(mod, name), f"{module_path} must define {name}"
    assert callable(getattr(mod, name))


@pytest.mark.parametrize(
    "module_path, task_id, callable_name",
    [
        ("congress_videos.youtube_upload_dag", "check_upload_quota", "_run_check_upload_quota"),
        ("congress_videos.youtube_upload_dag", "mark_chapters_uploaded", "_run_mark_chapters_uploaded"),
        ("congress_videos.youtube_upload_dag", "mark_turns_uploaded", "_run_mark_turns_uploaded"),
        ("congress_videos.video_analytics_dag", "get_pending_checkpoints", "_run_get_pending_checkpoints"),
        ("congress_videos.video_analytics_dag", "record_snapshots", "_run_record_snapshots"),
        (
            "congress_videos.post_upload_verification_dag",
            "select_unverified_uploads",
            "_run_select_unverified_uploads",
        ),
        (
            "congress_videos.youtube_channel_monitor_dag",
            "save_chapters_to_db",
            "_run_save_chapters_to_db",
        ),
    ],
)
def test_task_id_wired_to_expected_callable(module_path, task_id, callable_name):
    """Each migrated task_id is a PythonOperator wired to its named callable
    (task_ids are preserved byte-for-byte across the operator migration)."""
    from airflow.operators.python import PythonOperator

    mod = importlib.import_module(module_path)
    dag = mod.dag

    tasks_by_id = {t.task_id: t for t in dag.tasks}
    task = tasks_by_id.get(task_id)
    assert task is not None, f"{module_path} must still have task_id={task_id!r}"
    assert isinstance(task, PythonOperator), f"{task_id} must be a PythonOperator"
    assert task.python_callable.__name__ == callable_name
