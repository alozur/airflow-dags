"""Generic Thumbnail Generator DAG.

On-demand, config-driven DAG that generates and scores 2 thumbnail options
for a YouTube video using the Pikzels API, selects the best option, generates
a title via OpenAI, and persists results to the ``video_thumbnails`` table.

Triggered exclusively via the Airflow trigger API. All domain-specific values
come from per-domain config (``thumbnail_config.py``) and per-run ``conf``.

Usage::

    airflow dags trigger generic_thumbnail_generator --conf '{
        "youtube_video_id": "abc123",
        "chapter_id": 7,
        "debate_summary": "El debate ...",
        "session": "Pleno 2026-06-10",
        "domain": "<domain_key>"
    }'

The participant ``slug`` is optional. When it is absent, the pipeline generates
a generic thumbnail without a participant support photo.

Pipeline overview::

    validate_input
      → resolve_participant_photo
        → art_direction
          → generate_thumbnail_option_a → download_option_a → score_option_a
            → check_score_threshold [BranchPythonOperator]
                ├─[score < threshold]→ art_direction_retry
                │   → generate_thumbnail_option_b → download_option_b → score_option_b ─┐
                └─[score >= threshold]──────────────────────────────────────────────────┤
                   → choose_best_option (trigger_rule=none_failed_min_one_success) ◄────┘
                     → generate_title → persist_results → thumbnail_result
"""

from __future__ import annotations

import os
import pathlib
import shutil
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.operators.python import BranchPythonOperator, PythonOperator

from congress_videos.config.paths import get_thumbnail_dir
from congress_videos.config.thumbnail_config import ConfigError, get_domain_config
from congress_videos.modules import pikzels_client as _pkz
from congress_videos.modules.thumbnail_generation import (
    art_direct,
    choose_best_option,
    fetch_recent_thumbnail_history,
    generate_title,
    persist_results,
    resolve_participant_photo,
    resolved_photo_speaker_name,
)
from congress_videos.modules.thumbnail_prompt import build_pikzels_prompt

# ---------------------------------------------------------------------------
# Required conf keys for every DAG run.
# ---------------------------------------------------------------------------

_REQUIRED_CONF_KEYS = (
    "youtube_video_id",
    "chapter_id",
    "debate_summary",
    "session",
    "domain",
)

# ---------------------------------------------------------------------------
# Public helper: validate_input
# Exposed at module level so tests can call it directly without Airflow.
# ---------------------------------------------------------------------------


def validate_input(conf: dict) -> dict:
    """Validate the per-run conf dict and load domain config.

    Args:
        conf: DAG run configuration dict.

    Returns:
        The validated conf dict (unchanged) for downstream use.

    Raises:
        ValueError: When any required key is absent or empty.
        ConfigError: When ``conf["domain"]`` is not registered.
    """
    for key in _REQUIRED_CONF_KEYS:
        if key not in conf:
            raise ValueError(f"Missing required conf key: '{key}'")
        if not str(conf[key]).strip():
            raise ValueError(f"Required conf key '{key}' must not be empty")

    # Eagerly validate the domain — raises ConfigError for unknown domains.
    get_domain_config(conf["domain"])

    return conf


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------


def _task_validate_input(**context: object) -> dict:
    """Airflow task wrapper for validate_input."""
    conf: dict = context["dag_run"].conf or {}
    return validate_input(conf)


def _task_resolve_photo(ti: TaskInstance, **context: object) -> dict:
    """Resolve participant photo to base64 support image."""
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    domain_cfg = get_domain_config(conf["domain"])
    return resolve_participant_photo(conf.get("slug"), domain_cfg)


def _task_fetch_recent_history(ti: TaskInstance, **context: object) -> dict:
    """Fetch the most recent chosen thumbnail briefs and titles (GLOBAL scope).

    Calls fetch_recent_thumbnail_history which never raises — on DB error
    or empty result it returns ([], []). Pushes via XCom return value.

    Returns:
        Dict with 'briefs' (list[str]) and 'titles' (list[str]).
    """
    briefs, titles = fetch_recent_thumbnail_history()
    return {"briefs": briefs, "titles": titles}


def _task_art_direction(ti: TaskInstance, **context: object) -> dict:
    """Generate the art-direction brief shared by both thumbnail options.

    Accepts optional ``previous_brief``/``previous_archetype`` from the
    triggering conf (issue #102, anti-convergence steering) so the action
    DAG can force a different visual approach on the FIRST attempt, not
    only via the internal art_direction_retry path.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    history: dict = ti.xcom_pull(task_ids="fetch_recent_history") or {}
    photo_data: dict = ti.xcom_pull(task_ids="resolve_participant_photo") or {}
    domain_cfg = get_domain_config(conf["domain"])
    return art_direct(
        conf["debate_summary"],
        domain_cfg,
        previous_brief=conf.get("previous_brief"),
        sibling_briefs=history.get("briefs") or None,
        srt_fragment=conf.get("srt_fragment"),
        resolved_speaker_name=resolved_photo_speaker_name(
            photo_data, conf.get("key_speakers")
        ),
        forbidden_archetype=conf.get("previous_archetype"),
    )


def _task_generate_thumbnail(label: str, ti: TaskInstance, **context: object) -> dict:
    """Generate one thumbnail option via Pikzels using the art-direction brief.

    option_a pulls from task_ids='art_direction' (primary brief).
    option_b pulls from task_ids='art_direction_retry' (retry brief with
    different-approach instruction).
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    photo_data: dict = ti.xcom_pull(task_ids="resolve_participant_photo") or {}
    # Each option uses the correct art-direction source:
    # option_a → primary art_direction; option_b → art_direction_retry brief.
    brief_source = "art_direction" if label == "option_a" else "art_direction_retry"
    art_brief: dict = ti.xcom_pull(task_ids=brief_source) or {}
    domain_cfg = get_domain_config(conf["domain"])

    styles = domain_cfg["styles"]
    style_cfg = next(s for s in styles if s["label"] == label)
    layout = style_cfg["layout"]

    support_b64 = photo_data.get("support_image_b64", "")
    data_url = f"data:image/jpeg;base64,{support_b64}" if support_b64 else None

    prompt = build_pikzels_prompt(art_brief, layout)

    result = _pkz.thumbnail_from_text(
        prompt,
        support_image_base64=data_url,
    )
    # Attach label, layout (as style), prompt, and archetype for downstream
    # tasks (archetype carried through download/score to persist_results,
    # migration 041's video_thumbnails.archetype column, issue #102).
    result["label"] = label
    result["style"] = layout
    result["prompt"] = prompt
    result["archetype"] = art_brief.get("archetype")
    return result


def _task_download_option(label: str, ti: TaskInstance, **context: object) -> dict:
    """Download the generated thumbnail for one option and return local path info."""
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    gen_result: dict = ti.xcom_pull(task_ids=f"generate_thumbnail_{label}") or {}

    output_url: str = gen_result.get("output") or gen_result.get("output_url", "")
    _output_path = conf.get("output_path")
    if _output_path:
        # Turn-type item: co-locate thumbnail beside video.mp4 in the canonical dir.
        thumb_dir = pathlib.Path(os.path.dirname(_output_path))
    else:
        # Chapter-type or standalone trigger: use the legacy thumbnail directory.
        thumb_dir = get_thumbnail_dir(conf["youtube_video_id"])
    local_path = thumb_dir / f"{label}.png"

    _pkz.download(output_url, str(local_path))

    return {
        "label": label,
        "local_path": str(local_path),
        "output_url": output_url,
        "style": gen_result.get("style", ""),
        "prompt": gen_result.get("prompt", ""),
        "archetype": gen_result.get("archetype"),
    }


def _task_score_option(label: str, ti: TaskInstance, **context: object) -> dict:
    """Score a downloaded thumbnail option and return a scored option dict."""
    download_info: dict = ti.xcom_pull(task_ids=f"download_{label}") or {}
    local_path: str = download_info.get("local_path", "")

    # Read the local file and encode as base64 data-URI for scoring.
    image_bytes = pathlib.Path(local_path).read_bytes()
    image_b64 = _pkz.to_base64_data_url(image_bytes, "image/png")

    score_result = _pkz.score_thumbnail(image_base64=image_b64)
    main_score: float = score_result.get("main_score", 0.0)

    return {
        **download_info,
        "main_score": main_score,
    }


def _task_choose_best(ti: TaskInstance, **context: object) -> dict:
    """Select the best option from the available scored options.

    On the fast path only option_a is present; option_b XCom is {} and filtered out.
    On the retry path both options are non-empty.
    """
    option_a: dict = ti.xcom_pull(task_ids="score_option_a") or {}
    option_b: dict = ti.xcom_pull(task_ids="score_option_b") or {}
    options = [x for x in [option_a, option_b] if x]
    return choose_best_option(options)


def _task_generate_title(ti: TaskInstance, **context: object) -> str:
    """Generate a YouTube title for the chosen thumbnail option.

    Accepts an optional ``previous_title`` from the triggering conf (issue
    #102, anti-convergence steering) — passed as generate_title's
    forbidden_title so a 24h checkpoint regeneration avoids repeating the
    exact prior headline.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    best: dict = ti.xcom_pull(task_ids="choose_best_option") or {}
    history: dict = ti.xcom_pull(task_ids="fetch_recent_history") or {}
    domain_cfg = get_domain_config(conf["domain"])
    return generate_title(
        conf["debate_summary"],
        best,
        domain_cfg,
        sibling_titles=history.get("titles") or None,
        key_speakers=conf.get("key_speakers") or None,
        forbidden_title=conf.get("previous_title"),
    )


def _task_persist_results(ti: TaskInstance, **context: object) -> None:
    """Persist available thumbnail options to the video_thumbnails table.

    On the fast path, option_b XCom is {} and filtered out → exactly 1 row.
    On the retry path, both options are non-empty → 2 rows.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    option_a: dict = ti.xcom_pull(task_ids="score_option_a") or {}
    option_b: dict = ti.xcom_pull(task_ids="score_option_b") or {}
    best: dict = ti.xcom_pull(task_ids="choose_best_option") or {}
    title: str = ti.xcom_pull(task_ids="generate_title") or ""

    options = [x for x in [option_a, option_b] if x]
    persist_results(
        chapter_id=int(conf["chapter_id"]),
        youtube_video_id=str(conf["youtube_video_id"]),
        title=title,
        options=options,
        best_label=best.get("label", ""),
    )


def _persist_canonical_thumbnail(best_local_path: str, canonical_dir: pathlib.Path) -> str:
    """Copy the chosen best option to thumbnail.png in the canonical directory.

    This reconciliation step ensures the final artifact is always named
    ``thumbnail.png`` beside ``video.mp4`` for turn-type items, regardless
    of which option label (option_a / option_b) was selected as the best.

    Args:
        best_local_path: Absolute path of the best-scored PNG (e.g. .../option_a.png).
        canonical_dir: Parent directory derived from conf['output_path'] (anchor folder).

    Returns:
        Absolute path of the resulting ``thumbnail.png`` file.
    """
    canonical_thumbnail = canonical_dir / "thumbnail.png"
    shutil.copy(best_local_path, str(canonical_thumbnail))
    return str(canonical_thumbnail)


def _task_thumbnail_result(ti: TaskInstance, **context: object) -> dict:
    """Expose the persisted generation result to the DAG that triggered this run.

    For turn-type items (canonical path): copies the best option to thumbnail.png
    in the canonical directory and returns that path as output_path.
    For chapter-type or standalone triggers (legacy path): returns best local_path
    unchanged.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    best: dict = ti.xcom_pull(task_ids="choose_best_option") or {}
    title: str = ti.xcom_pull(task_ids="generate_title") or ""

    best_local_path: str | None = best.get("local_path")
    _conf_output_path = conf.get("output_path")

    if _conf_output_path and best_local_path:
        # Turn-type canonical path: reconcile to thumbnail.png.
        canonical_dir = pathlib.Path(os.path.dirname(_conf_output_path))
        output_path = _persist_canonical_thumbnail(best_local_path, canonical_dir)
    else:
        # Legacy chapter path or standalone trigger: use best local_path as-is.
        output_path = best_local_path

    return {
        "chapter_id": int(conf["chapter_id"]),
        "success": True,
        "output_path": output_path,
        "title": title,
    }


def _task_check_score_threshold(ti: TaskInstance, **context: object) -> str:
    """BranchPythonOperator callable: compare option_a score against domain threshold.

    Returns:
        'art_direction_retry' when score < threshold (retry path).
        'choose_best_option' when score >= threshold (fast path).
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    score_result: dict = ti.xcom_pull(task_ids="score_option_a") or {}
    score: float = score_result.get("main_score", 0.0)
    domain_cfg = get_domain_config(conf["domain"])
    threshold: int = domain_cfg.get("score_retry_threshold", 60)
    return "art_direction_retry" if score < threshold else "choose_best_option"


def _task_art_direction_retry(ti: TaskInstance, **context: object) -> dict:
    """Generate a DIFFERENT art-direction brief for the retry option.

    Pulls the original brief from task_ids='art_direction' and passes it as
    previous_brief so art_direct injects the retry instruction.
    Also passes sibling_briefs from fetch_recent_history to steer diversity.
    """
    conf: dict = ti.xcom_pull(task_ids="validate_input") or {}
    previous_brief: dict = ti.xcom_pull(task_ids="art_direction") or {}
    history: dict = ti.xcom_pull(task_ids="fetch_recent_history") or {}
    photo_data: dict = ti.xcom_pull(task_ids="resolve_participant_photo") or {}
    domain_cfg = get_domain_config(conf["domain"])
    return art_direct(
        conf["debate_summary"],
        domain_cfg,
        previous_brief=previous_brief,
        sibling_briefs=history.get("briefs") or None,
        resolved_speaker_name=resolved_photo_speaker_name(
            photo_data, conf.get("key_speakers")
        ),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

_GENERATION_TIMEOUT = timedelta(minutes=10)

with DAG(
    dag_id="generic_thumbnail_generator",
    default_args=default_args,
    description=(
        "On-demand DAG: generates 1 or 2 thumbnail options via Pikzels with "
        "score-gated retry, picks the best, generates a title, and persists results."
    ),
    schedule=None,
    start_date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["thumbnail", "pikzels", "generic", "on-demand"],
    max_active_runs=3,
) as dag:

    t_validate = PythonOperator(
        task_id="validate_input",
        python_callable=_task_validate_input,
    )

    t_resolve_photo = PythonOperator(
        task_id="resolve_participant_photo",
        python_callable=_task_resolve_photo,
    )

    t_fetch_recent_history = PythonOperator(
        task_id="fetch_recent_history",
        python_callable=_task_fetch_recent_history,
    )

    t_art_direction = PythonOperator(
        task_id="art_direction",
        python_callable=_task_art_direction,
    )

    t_gen_a = PythonOperator(
        task_id="generate_thumbnail_option_a",
        python_callable=lambda ti, **ctx: _task_generate_thumbnail("option_a", ti=ti, **ctx),
        execution_timeout=_GENERATION_TIMEOUT,
    )

    t_dl_a = PythonOperator(
        task_id="download_option_a",
        python_callable=lambda ti, **ctx: _task_download_option("option_a", ti=ti, **ctx),
    )

    t_score_a = PythonOperator(
        task_id="score_option_a",
        python_callable=lambda ti, **ctx: _task_score_option("option_a", ti=ti, **ctx),
        execution_timeout=_GENERATION_TIMEOUT,
    )

    t_check_score = BranchPythonOperator(
        task_id="check_score_threshold",
        python_callable=_task_check_score_threshold,
    )

    t_art_direction_retry = PythonOperator(
        task_id="art_direction_retry",
        python_callable=_task_art_direction_retry,
    )

    t_gen_b = PythonOperator(
        task_id="generate_thumbnail_option_b",
        python_callable=lambda ti, **ctx: _task_generate_thumbnail("option_b", ti=ti, **ctx),
        execution_timeout=_GENERATION_TIMEOUT,
    )

    t_dl_b = PythonOperator(
        task_id="download_option_b",
        python_callable=lambda ti, **ctx: _task_download_option("option_b", ti=ti, **ctx),
    )

    t_score_b = PythonOperator(
        task_id="score_option_b",
        python_callable=lambda ti, **ctx: _task_score_option("option_b", ti=ti, **ctx),
        execution_timeout=_GENERATION_TIMEOUT,
    )

    t_choose = PythonOperator(
        task_id="choose_best_option",
        python_callable=_task_choose_best,
        trigger_rule="none_failed_min_one_success",
    )

    t_title = PythonOperator(
        task_id="generate_title",
        python_callable=_task_generate_title,
    )

    t_persist = PythonOperator(
        task_id="persist_results",
        python_callable=_task_persist_results,
    )

    t_result = PythonOperator(
        task_id="thumbnail_result",
        python_callable=_task_thumbnail_result,
    )

    # Task dependency graph:
    #   validate_input → resolve_participant_photo ──────────────────────┐
    #   validate_input → fetch_recent_history ──────────────────────────┤
    #                                                                    ↓
    #                                                              art_direction
    #     → generate_thumbnail_option_a → download_option_a → score_option_a
    #       → check_score_threshold [BranchPythonOperator]
    #           ├─[score < threshold]→ art_direction_retry (also fed by fetch_recent_history)
    #           │                        → generate_thumbnail_option_b → download_option_b → score_option_b ─┐
    #           └─[score >= threshold]──────────────────────────────────────────────────────────────────────┤
    #                                  → choose_best_option (trigger_rule=none_failed_min_one_success) ◄────┘
    #                                    → generate_title (also fed by fetch_recent_history)
    #                                      → persist_results → thumbnail_result
    t_validate >> t_resolve_photo
    t_validate >> t_fetch_recent_history
    [t_resolve_photo, t_fetch_recent_history] >> t_art_direction
    t_art_direction >> t_gen_a >> t_dl_a >> t_score_a >> t_check_score
    t_check_score >> t_art_direction_retry >> t_gen_b >> t_dl_b >> t_score_b >> t_choose
    t_check_score >> t_choose
    t_fetch_recent_history >> t_art_direction_retry
    t_fetch_recent_history >> t_title
    t_choose >> t_title >> t_persist >> t_result
