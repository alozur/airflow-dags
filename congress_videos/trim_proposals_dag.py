"""Trim Proposals DAG.

On-demand, config-driven DAG that detects silence and applause zones
**within** existing ``speaker_turns`` and persists non-destructive trim
candidates to the ``speaker_turn_trim_proposals`` table (migration 023).

Nothing is ever cut automatically. Proposals are advisory records only —
no audio or video file is modified. Auto-cut is a future milestone pending
ground-truth-by-ear calibration (issue #87, non-goal).

Triggered exclusively via the Airflow trigger API (``schedule=None``).

Usage::

    airflow dags trigger trim_proposals --conf '{"limit": 10}'
    airflow dags trigger trim_proposals --conf '{"turn_ids": [7, 8]}'

Pipeline::

    select_turns (speaker_turns table, LIMIT from conf or explicit IDs)
      → generate_proposals (per turn, graceful skip)
          _find_source_video → extract_audio_wav (turn-window WAV slice)
            → generate_trim_proposals(vad_fn=webrtc_vad_fn,
                                       yamnet_fn=docker_yamnet_fn)
              → _upsert_proposals (idempotent, ON CONFLICT DO UPDATE)
"""
from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.modules.trim_proposals import (
    _upsert_proposals,
    generate_trim_proposals,
)
from congress_videos.modules.trim_proposals_docker import docker_yamnet_fn
from congress_videos.modules.vad_helpers import _find_source_video, extract_audio_wav
from utils.postgres_helpers import PostgresConnection

logger = logging.getLogger(__name__)

DAG_ID = "trim_proposals"
DEFAULT_LIMIT = 10


# ---------------------------------------------------------------------------
# VAD backend — default webrtc segments injected as VadFn
# ---------------------------------------------------------------------------

def _webrtc_vad_fn(wav_path: str, offset: float) -> list[tuple[float, float]]:
    """Production VAD backend: webrtcvad-based voiced-segment detector.

    Lazy-imported here (not in trim_proposals.py) to keep the pure module
    free of subprocess/model dependencies. Returns a list of (start, end)
    tuples in absolute session seconds.

    Falls back to an empty list on any backend error so the DAG never blocks.
    """
    try:
        from congress_videos.modules.vad_helpers import detect_speech_bounds
        # detect_speech_bounds returns (start_secs, end_secs) for the turn window;
        # for per-turn proposal generation we need ALL voiced intervals, not just
        # the outer boundary. Use webrtcvad raw blocks when available.
        # Fallback: treat the returned bounds as a single voiced segment.
        bounds = detect_speech_bounds(wav_path, backend="webrtc")
        if bounds and bounds[0] is not None and bounds[1] is not None:
            return [(offset + bounds[0], offset + bounds[1])]
        return []
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "vad_fn failed for wav_path=%s offset=%s error=%s",
            wav_path, offset, exc,
        )
        return []


# ---------------------------------------------------------------------------
# Per-turn orchestration
# ---------------------------------------------------------------------------


def select_turns(
    limit: int = DEFAULT_LIMIT,
    turn_ids: list[int] | None = None,
) -> list[dict]:
    """Read speaker turns from the ``speaker_turns`` table.

    When ``turn_ids`` is given, only those turns are returned; otherwise
    the first ``limit`` turns ordered by turn_id are returned.
    """
    pg = PostgresConnection()
    table = pg.get_qualified_table("speaker_turns")
    cols = "turn_id, chapter_id, video_id, session_date, start_seconds, end_seconds"
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            if turn_ids:
                cur.execute(
                    f"SELECT {cols} FROM {table} WHERE turn_id = ANY(%s) "
                    f"ORDER BY turn_id",
                    (list(turn_ids),),
                )
            else:
                cur.execute(
                    f"SELECT {cols} FROM {table} ORDER BY turn_id LIMIT %s",
                    (limit,),
                )
            names = [d[0] for d in cur.description]
            return [dict(zip(names, row)) for row in cur.fetchall()]


def run_turn_proposals(
    turn: dict,
    cursor,
    *,
    vad_fn=_webrtc_vad_fn,
    yamnet_fn=docker_yamnet_fn,
) -> dict:
    """Generate and persist trim proposals for a single speaker turn.

    Locates the source video (skips the turn if absent — never fails the run),
    extracts the turn-window WAV, generates silence and applause proposals via
    the injected callables, and upserts them. Returns a status dict.

    Args:
        turn:      Speaker-turn dict with turn_id, video_id, session_date,
                   start_seconds, end_seconds.
        cursor:    DB cursor (transaction owned by caller).
        vad_fn:    Injectable VAD callable (default: webrtc backend).
        yamnet_fn: Injectable YAMNet callable (default: Docker wrapper).

    Returns:
        Dict with ``status``, ``turn_id``, and ``proposals`` count.
    """
    turn_id = turn["turn_id"]
    video_id = turn["video_id"]
    session_date = str(turn.get("session_date"))
    start_secs = float(turn["start_seconds"])
    end_secs = float(turn["end_seconds"])
    duration = max(0.0, end_secs - start_secs)

    video_path = _find_source_video(session_date, video_id)
    if not video_path:
        logger.warning(
            "turn %s: no source video for %s/%s — skipping",
            turn_id, session_date, video_id,
        )
        return {"status": "skipped_no_video", "turn_id": turn_id, "proposals": 0}

    wav_path = os.path.join(
        tempfile.gettempdir(), f"trim_proposals_{video_id}_{turn_id}.wav"
    )
    try:
        extract_audio_wav(
            video_path, wav_path,
            start_secs=start_secs,
            duration_secs=duration,
        )

        proposals = generate_trim_proposals(
            turn, wav_path, vad_fn, yamnet_fn
        )
        count = _upsert_proposals(cursor, proposals)
        logger.info(
            "turn %s: %d proposal(s) upserted", turn_id, count
        )
        return {"status": "ok", "turn_id": turn_id, "proposals": count}
    finally:
        if os.path.exists(wav_path):
            os.remove(wav_path)


# ---------------------------------------------------------------------------
# Airflow task callables
# ---------------------------------------------------------------------------


def _select_task(**context) -> list[dict]:
    dag_run = context.get("dag_run")
    conf = (dag_run.conf or {}) if dag_run else {}
    turns = select_turns(
        limit=conf.get("limit", DEFAULT_LIMIT),
        turn_ids=conf.get("turn_ids"),
    )
    logger.info("Selected %d turn(s) for trim-proposal generation", len(turns))
    context["ti"].xcom_push(key="turns", value=turns)
    return turns


def _process_task(**context) -> dict:
    turns = context["ti"].xcom_pull(key="turns", task_ids="select_turns") or []
    summary = {"processed": 0, "skipped": 0, "proposals": 0}
    pg = PostgresConnection()
    with pg.get_connection() as conn:
        with conn.cursor() as cur:
            for turn in turns:
                try:
                    result = run_turn_proposals(turn, cur)
                except Exception:  # noqa: BLE001 — one bad turn must not sink the run
                    logger.exception(
                        "turn %s failed — skipping", turn.get("turn_id")
                    )
                    summary["skipped"] += 1
                    continue
                if result["status"] == "ok":
                    summary["processed"] += 1
                    summary["proposals"] += result["proposals"]
                else:
                    summary["skipped"] += 1
        conn.commit()
    logger.info("Trim-proposal run summary: %s", summary)
    return summary


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    dag_id=DAG_ID,
    description=(
        "On-demand per-turn silence and applause trim proposals (issue #87) — "
        "advisory records only, nothing is auto-cut"
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    default_args=default_args,
    tags=["congress_videos", "trim-proposals", "on-demand"],
)

with dag:
    select_turns_task = PythonOperator(
        task_id="select_turns",
        python_callable=_select_task,
    )
    generate_proposals_task = PythonOperator(
        task_id="generate_proposals",
        python_callable=_process_task,
    )
    select_turns_task >> generate_proposals_task
