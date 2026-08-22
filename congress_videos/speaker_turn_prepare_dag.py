"""Speaker Turn Prepare DAG (issue #146).

Nightly preparation of up to N=2 unprepared TURN items. Generates all required
sidecars, validates video integrity with an ffmpeg decode check, then sets
prepared_at as the upload readiness gate.

Schedule: 0 2 * * * UTC (off-peak; 02:00 avoids monitor scrape, reap, and upload
windows). The upload DAG runs at 0 19 * * *.

Design constraints (non-negotiable):
- Strictly sequential: one PythonOperator, pool="nas_ffmpeg", pool_slots=1.
- No dynamic task mapping (.expand() is prohibited).
- prepared_at set ONLY after: all sidecars on disk AND ffmpeg decode check rc==0.
- Failures are self-healing: prepared_at stays NULL, retry next night.
"""

import logging
import os
import subprocess
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.operators.python import PythonOperator

from congress_videos.modules.database import CongressionalVideoDB
from utils.env_loader import load_env_if_local

load_env_if_local()

logger = logging.getLogger(__name__)

_THUMBNAIL_DAG_ID = "generic_thumbnail_generator"
_THUMBNAIL_RESULT_TASK_ID = "thumbnail_result"

# Poll cadence and hard deadline for the child thumbnail DAG. A bounded loop
# guarantees the single nas_ffmpeg pool slot is released even if the child hangs;
# leaving prepared_at unset lets the nightly loop self-heal on the next run.
_THUMBNAIL_POLL_INTERVAL_SECS = 15
_THUMBNAIL_POLL_TIMEOUT_SECS = 1800

# ---------------------------------------------------------------------------
# Internal helpers (testable pure functions)
# ---------------------------------------------------------------------------


def _trigger_thumbnail_for_turn(turn: dict) -> None:
    """Trigger generic_thumbnail_generator and poll until completion.

    Reuses the trigger+poll pattern from youtube_upload_dag.trigger_thumbnail_generation.
    Thumbnails are written to the canonical #133 path by the thumbnail DAG.

    Args:
        turn: Row dict from select_unprepared_turns (turn_id, output_path, …).

    Raises:
        RuntimeError: when the thumbnail DAG fails (non-zero exit); prepare will
            NOT set prepared_at for this turn.
    """
    turn_id = turn["turn_id"]
    output_path = turn.get("output_path") or ""

    # Build the child config mirroring the chapter-uploader pattern (Slice 4a).
    child_conf = {
        "youtube_video_id": str(turn_id),
        "chapter_id": turn.get("chapter_id"),
        "debate_summary": (
            (turn.get("chapter_title") or "")
            + ("\n" + (turn.get("description") or "") if turn.get("description") else "")
        ),
        "domain": "congreso",
        "session": (
            f"Sesión {turn['session_number']}"
            if turn.get("session_number") is not None
            else str(turn.get("session_date", ""))
        ),
        "slug": None,
        "key_speakers": [turn["resolved_name"]] if turn.get("resolved_name") else [],
        "output_path": output_path,
    }

    try:
        dag_run = trigger_dag_api(
            dag_id=_THUMBNAIL_DAG_ID,
            conf=child_conf,
            run_id=f"prepare_turn_{turn_id}_{int(datetime.utcnow().timestamp())}",
        )
    except Exception as exc:
        raise RuntimeError(
            f"_trigger_thumbnail_for_turn: could not trigger thumbnail DAG "
            f"for turn_id={turn_id}: {exc}"
        ) from exc

    logger.info(
        "_trigger_thumbnail_for_turn: triggered run %s for turn_id=%d",
        dag_run.run_id,
        turn_id,
    )

    deadline = time.monotonic() + _THUMBNAIL_POLL_TIMEOUT_SECS
    while True:
        time.sleep(_THUMBNAIL_POLL_INTERVAL_SECS)
        dag_run.refresh_from_db()
        if dag_run.state in ("success", "failed"):
            break
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"_trigger_thumbnail_for_turn: thumbnail DAG run {dag_run.run_id} "
                f"for turn_id={turn_id} did not complete within "
                f"{_THUMBNAIL_POLL_TIMEOUT_SECS}s; leaving prepared_at unset for retry"
            )

    if dag_run.state != "success":
        raise RuntimeError(
            f"_trigger_thumbnail_for_turn: thumbnail DAG run {dag_run.run_id} "
            f"failed for turn_id={turn_id}"
        )

    logger.info(
        "_trigger_thumbnail_for_turn: thumbnail ready for turn_id=%d (run=%s)",
        turn_id,
        dag_run.run_id,
    )


def _write_turn_sidecars(turn: dict) -> None:
    """Generate and write title.txt, description.txt, and subtitles.srt for a turn.

    Args:
        turn: Row dict from select_unprepared_turns.

    Raises:
        Exception: Any sidecar write failure propagates so prepared_at is NOT set.
    """
    from congress_videos.modules.youtube import youtube_ai
    from congress_videos.modules.youtube.youtube_upload import _write_orador_sidecars
    from congress_videos.srt_helpers import (
        _parse_srt_blocks,
        _serialize_srt_blocks,
        find_srt_for_chapter,
    )

    output_path = turn.get("output_path") or ""
    video_dir = os.path.dirname(output_path)

    # Generate YouTube metadata (title + description) for this turn.
    meta_results = youtube_ai.generate_youtube_metadata_for_selected_videos([turn])
    topic_metadata = (meta_results or {}).get("topic_metadata") or []
    if topic_metadata:
        entry = topic_metadata[0]
        title_data = entry.get("title") or {}
        desc_data = entry.get("description") or {}
        title = (
            title_data.get("title", "")
            if isinstance(title_data, dict)
            else str(title_data)
        )
        description = (
            desc_data.get("description", "")
            if isinstance(desc_data, dict)
            else str(desc_data)
        )
    else:
        title = turn.get("resolved_name") or ""
        description = ""

    # Write title.txt + description.txt via canonical helper.
    _write_orador_sidecars(output_path, title, description)

    # Write subtitles.srt from windowed SRT fragment.
    video_id = turn.get("video_id")
    chapter_id = turn.get("chapter_id")
    session_date = turn.get("session_date")

    srt_path = (
        find_srt_for_chapter(
            str(video_id),
            chapter_id,
            str(session_date) if session_date else None,
        )
        if video_id is not None
        else None
    )

    if srt_path is not None:
        blocks = _parse_srt_blocks(srt_path)
        window_start = float(turn.get("start_seconds", 0))
        window_end = float(turn.get("end_seconds", 99 * 3600))
        windowed = [
            b
            for b in blocks
            if b["start_secs"] >= window_start and b["end_secs"] <= window_end
        ]
    else:
        windowed = []

    srt_out = os.path.join(video_dir, "subtitles.srt")
    with open(srt_out, "w", encoding="utf-8") as f:
        f.write(_serialize_srt_blocks(windowed) if windowed else "")

    logger.info(
        "_write_turn_sidecars: sidecars written for turn_id=%d at %s",
        turn.get("turn_id"),
        video_dir,
    )


def _run_ffmpeg_decode_check(path: str) -> int:
    """Fully decode a video via ffmpeg to verify integrity.

    Uses ``ffmpeg -f null -`` (ffprobe has no null output muxer and always
    returns rc=1, which means prepared_at is never set — live-confirmed on
    prod 2026-08-22). A non-zero return code means the file is truncated or
    corrupt. Does not raise on failure; returns the process return code.
    """
    result = subprocess.run(
        ["ffmpeg", "-v", "error", "-i", str(path), "-f", "null", "-"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    return result.returncode


def _prepare_turns_callable() -> None:
    """Callable for the prepare_turns PythonOperator task.

    Selects up to N=2 unprepared turns and processes each sequentially:
    1. Trigger + poll generic_thumbnail_generator.
    2. Write title.txt, description.txt, subtitles.srt.
    3. Run ffmpeg decode integrity check on video.mp4.
    4. Set prepared_at (only when all prior steps succeeded).

    Any failure within a turn's steps leaves prepared_at NULL; the next
    nightly run retries all steps from scratch.
    """
    db = CongressionalVideoDB()
    turns = db.select_unprepared_turns(limit=2)

    if not turns:
        logger.info("_prepare_turns_callable: no unprepared turns; nothing to do")
        return

    logger.info("_prepare_turns_callable: %d turn(s) selected for preparation", len(turns))

    for turn in turns:
        turn_id = turn["turn_id"]
        output_path = turn.get("output_path") or ""

        logger.info(
            "_prepare_turns_callable: preparing turn_id=%d output_path=%s",
            turn_id,
            output_path,
        )

        try:
            # Step 1: Generate thumbnail (may take 5–10 min).
            _trigger_thumbnail_for_turn(turn)

            # Step 2: Write text sidecars.
            _write_turn_sidecars(turn)

            # Step 3: ffmpeg decode integrity check.
            rc = _run_ffmpeg_decode_check(output_path)
            if rc != 0:
                logger.warning(
                    "_prepare_turns_callable: ffmpeg decode check failed for turn_id=%d "
                    "(rc=%d) — prepared_at NOT set; will retry next night",
                    turn_id,
                    rc,
                )
                continue

            # Step 4: Atomic readiness flip — called LAST.
            db.mark_turn_prepared(turn_id)
            logger.info(
                "_prepare_turns_callable: turn_id=%d prepared successfully", turn_id
            )

        except Exception as exc:
            logger.warning(
                "_prepare_turns_callable: turn_id=%d preparation failed (%s) "
                "— prepared_at NOT set; will retry next night",
                turn_id,
                exc,
            )
            continue


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,  # No retries — failures self-heal on next nightly run
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "speaker_turn_prepare",
    default_args=default_args,
    description=(
        "Nightly PREPARE phase: generates sidecars and validates up to 2 speaker "
        "turn videos; sets prepared_at readiness gate before upload."
    ),
    schedule="0 2 * * *",
    start_date=datetime(2026, 8, 21),
    catchup=False,
    tags=["congress", "speaker-turns", "prepare"],
) as dag:
    prepare_turns = PythonOperator(
        task_id="prepare_turns",
        python_callable=_prepare_turns_callable,
        pool="nas_ffmpeg",
        pool_slots=1,
    )
