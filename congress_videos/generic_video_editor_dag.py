"""Generic video overlay editor DAG.

On-demand (``schedule=None``) DAG that burns static drawtext overlays into a
source video using ffmpeg, producing a new edited video file.  The output
path is returned via XCom ``return_value`` so the caller can pass it as
``video_file`` to ``generic_youtube_uploader``.

Trigger configuration::

    {
        "domain": "congreso",
        "source_path": "/opt/airflow/data/congress_videos/V1/C1/chapter_video.mp4",
        "overlays": [
            {
                "tipo": "extracto_sesion",
                "tiempo_inicio": 0.0,
                "tiempo_fin": 5.0,
                "titulo": "Extracto de sesión"
            }
        ]
    }

Alternatively, supply ``video_id`` + ``chapter_id`` instead of ``source_path``
to derive the canonical path automatically.  An optional ``output_path`` key
overrides the default ``<source_stem>_edited.mp4`` naming.

Task graph::

    validate_input → apply_overlays → editor_result
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import PythonOperator

from congress_videos.config.video_editor_config import get_domain_config
from congress_videos.modules.video_editor import (
    _default_output_path,
    _resolve_source_path,
    apply_overlays,
    validate_editor_input,
)

# ---------------------------------------------------------------------------
# Required conf keys for every DAG run.
# ---------------------------------------------------------------------------

_REQUIRED_CONF_KEYS = ("domain", "overlays")

# ---------------------------------------------------------------------------
# Public helper: validate_input
# ---------------------------------------------------------------------------


def validate_input(conf: dict) -> dict:
    """Validate *conf* and return it unchanged if valid.

    Delegates the detailed validation to :func:`validate_editor_input` which
    checks required keys, source XOR, overlay fields, domain existence, tipo
    membership, and font-file availability.

    Args:
        conf: DAG run configuration dict.

    Returns:
        The validated conf dict (unchanged) for downstream XCom hand-off.

    Raises:
        ValueError: When any required key is absent or overlays are empty.
        ConfigError: When ``conf["domain"]`` is not registered.
        FileNotFoundError: When the font file or resolved source is absent.
    """
    validate_editor_input(conf)
    return conf


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def _task_validate_input(**context: object) -> dict:
    """Airflow task wrapper for :func:`validate_input`.

    Reads ``dag_run.conf``, validates it, and returns the conf dict so
    PythonOperator pushes it to XCom ``return_value`` automatically.
    """
    conf: dict = context["dag_run"].conf or {}
    return validate_input(conf)


def _task_apply_overlays(ti: TaskInstance, **context: object) -> dict:
    """Apply drawtext overlays to the source video.

    Pulls the validated conf from the ``validate_input`` XCom, resolves the
    source path, determines the output path, loads the domain config, and
    delegates to :func:`apply_overlays`.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    domain_cfg = get_domain_config(conf["domain"])

    source_path = _resolve_source_path(conf)
    output_path = conf.get("output_path") or _default_output_path(source_path)

    return apply_overlays(
        source_path=source_path,
        output_path=output_path,
        overlays=conf["overlays"],
        domain_cfg=domain_cfg,
    )


def _task_editor_result(ti: TaskInstance, **context: object) -> dict:
    """Collect the editing result and push it as the terminal XCom.

    Returns a dict with ``success``, ``output_path``, and ``domain`` so the
    calling workflow can forward ``output_path`` to ``generic_youtube_uploader``
    as its ``video_file`` conf key.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    result: dict = ti.xcom_pull(task_ids="apply_overlays") or {}

    return {
        "success": True,
        "output_path": result["output_path"],
        "domain": conf["domain"],
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

_DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1, tzinfo=UTC),
}

with DAG(
    dag_id="generic_video_editor",
    default_args=_DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["video", "editor", "generic"],
    description="Burn drawtext overlays into a source video using ffmpeg.",
) as dag:
    t_validate = PythonOperator(
        task_id="validate_input",
        python_callable=_task_validate_input,
    )

    t_apply = PythonOperator(
        task_id="apply_overlays",
        python_callable=_task_apply_overlays,
    )

    t_result = PythonOperator(
        task_id="editor_result",
        python_callable=_task_editor_result,
    )

    t_validate >> t_apply >> t_result
