"""
Congress YouTube Chapter Uploader DAG

This DAG uploads individual chapters from congressional videos to YouTube:

1. Ensure data directory exists
2. Query the highest-priority uploadable chapter from uploadable_chapters view
   - Ordered by relevance_score DESC (highest relevance first)
   - Then by created_at DESC (most recent first)
   - Only chapters not yet uploaded to YouTube

The uploadable_chapters view filters chapters with:
- is_uploaded_to_youtube = FALSE
- relevance_score >= 2 (configurable in view)
- Joined with source video metadata

This allows uploading the most relevant and recent congressional debate chapters
as standalone YouTube videos.
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import XCom
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api

from congress_videos.modules.postgres_operators import PostgreSQLOperator
from congress_videos.modules.participants_db import lookup_participant_fuzzy
from congress_videos.srt_helpers import (
    _parse_srt_blocks,
    _srt_timestamp_to_seconds,
    find_srt_for_chapter,
)
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "development")
IS_DEVELOPMENT = POSTGRES_SCHEMA == "development"

# The scheduled 19:00 UTC run uploads only when at least one chapter is queued.
# This is a calendar-day cap for long-form chapter uploads; database quota data
# reports successful uploads_today, while limit=1 remains a selection safeguard.
DAILY_LONG_FORM_UPLOAD_LIMIT = 1
STALE_RUN_TOLERANCE_MINUTES = int(
    os.getenv("CHAPTER_UPLOADER_STALE_RUN_TOLERANCE_MINUTES", "30")
)
_THUMBNAIL_DAG_ID = "generic_thumbnail_generator"
_THUMBNAIL_RESULT_TASK_ID = "thumbnail_result"


def should_upload(**context):
    """Return True when queue_size is non-empty (queue_size > 0).

    Used as the python_callable for t1_skip (ShortCircuitOperator).
    Receives full Airflow context via **context (REQ-GATE-01).
    """
    data_interval_end = context.get("data_interval_end")
    if data_interval_end:
        now = datetime.now(timezone.utc)
        staleness = now - data_interval_end
        if staleness > timedelta(minutes=STALE_RUN_TOLERANCE_MINUTES):
            logging.info(
                "Skipping stale re-parse replay: data_interval_end=%s is %s "
                "behind now=%s (tolerance=%dm)",
                data_interval_end,
                staleness,
                now,
                STALE_RUN_TOLERANCE_MINUTES,
            )
            return False
    ti = context["ti"]
    upload_quota = ti.xcom_pull(key="upload_quota") or {}
    queue_size = upload_quota.get("queue_size", 0)
    uploads_today = upload_quota.get("uploads_today", 0)
    if uploads_today >= DAILY_LONG_FORM_UPLOAD_LIMIT:
        logging.info(
            "Skipping upload: %d long-form chapter upload(s) already recorded today "
            "(daily limit=%d)",
            uploads_today,
            DAILY_LONG_FORM_UPLOAD_LIMIT,
        )
        return False

    return queue_size > 0


# ---------------------------------------------------------------------------
# Thumbnail pipeline helpers
# ---------------------------------------------------------------------------


def _resolve_speaker_name(chapter: dict) -> str | None:
    """Extract the first speaker name from key_speakers or speakers.

    Args:
        chapter: Chapter dict from the uploadable_chapters view.

    Returns:
        Speaker name string, or None when no speakers are present.

    Raises:
        LookupError: Re-raised from downstream participant lookup callers.
        ValueError: Re-raised from downstream participant lookup callers.
    """
    key_speakers = chapter.get("key_speakers") or []
    speakers = chapter.get("speakers") or []

    # Prefer key_speakers; fall back to speakers
    if key_speakers:
        first = key_speakers[0]
        # key_speakers entries may be dicts {"name": "..."} or plain strings
        return first["name"] if isinstance(first, dict) else first
    if speakers:
        first = speakers[0]
        return first["name"] if isinstance(first, dict) else first
    return None


def _prepare_thumbnail_config(chapter: dict, db) -> dict:
    """Build the thumbnail-generation config dict for a single chapter or turn.

    Handles two row types:
    - Chapter rows (from uploadable_chapters view): use key_speakers list, resolved_participant_slug,
      and SRT window from start_time/end_time strings.
    - Turn rows (from uploadable_turns view, identified by presence of 'turn_id'): anchor
      key_speakers to [resolved_name], use resolved_name for slug lookup, and use start_seconds/
      end_seconds (float seconds) for the SRT window.

    Args:
        chapter: Chapter or turn row from the database view.
        db: CongressionalVideoDB instance (kept for task-call compatibility).

    Returns:
        Dict with keys: chapter_id, debate_summary, domain, session,
        slug (may be None on fallback), key_speakers, optionally srt_fragment.
    """
    is_turn = "turn_id" in chapter

    chapter_id = chapter.get("chapter_id")
    title = chapter.get("chapter_title", "")
    description = chapter.get("description", "")
    session_number = chapter.get("session_number")
    session_date = chapter.get("session_date")

    # debate_summary: title + description (D6)
    debate_summary = f"{title}\n{description}" if description else title

    # session label (D6)
    if session_number is not None:
        session = f"Sesión {session_number}"
    else:
        session = str(session_date) if session_date else None

    if is_turn:
        # Turn path: anchor key_speakers to the turn's own speaker (resolved_name).
        # Use resolved_name directly for the fuzzy slug lookup.
        resolved_name = chapter.get("resolved_name") or ""
        key_speakers = [resolved_name] if resolved_name else []
        slug = None
        if resolved_name:
            try:
                participant = lookup_participant_fuzzy(resolved_name)
                slug = participant.get("slug") if participant else None
            except Exception as exc:
                logging.warning(
                    "_prepare_thumbnail_config: turn speaker resolution failed for "
                    "turn_id=%s resolved_name=%r: %s — setting slug=None",
                    chapter.get("turn_id"),
                    resolved_name,
                    exc,
                )
                slug = None
    else:
        # Chapter path: preserve existing behaviour.
        # Prefer the authoritative slug written by speaker normalization (fuzzy or
        # institutional-role resolution). Fall back to a raw-speaker fuzzy match only
        # when the chapter was never resolved. Fuzzy matching stays confined to this
        # boundary; downstream thumbnail code receives only the stable slug.
        key_speakers = chapter.get("key_speakers") or []
        slug = chapter.get("resolved_participant_slug") or None
        if not slug:
            try:
                raw_speaker = _resolve_speaker_name(chapter)
                participant = lookup_participant_fuzzy(raw_speaker) if raw_speaker else None
                slug = participant.get("slug") if participant else None
            except Exception as exc:
                logging.warning(
                    "_prepare_thumbnail_config: speaker resolution failed for "
                    "chapter_id=%s: %s — setting slug=None",
                    chapter_id,
                    exc,
                )
                slug = None

    config = {
        "chapter_id": chapter_id,
        "debate_summary": debate_summary,
        "domain": "congreso",
        "session": session,
        "slug": slug,
        "key_speakers": key_speakers,
    }

    # Thread the canonical output_path for turn-type items (Slice 4a).
    # Chapter-type items never have output_path — key is absent by design.
    if is_turn:
        config["output_path"] = chapter.get("output_path")

    # Resolve SRT fragment for lapidary quote extraction (issue #57).
    video_id = chapter.get("video_id")
    srt_path = (
        find_srt_for_chapter(str(video_id), chapter_id, str(session_date) if session_date else None)
        if video_id is not None
        else None
    )
    if srt_path is not None:
        blocks = _parse_srt_blocks(srt_path)
        if is_turn:
            # Turn: use integer seconds directly from the turn row.
            window_start_secs = float(chapter.get("start_seconds", 0))
            window_end_secs = float(chapter.get("end_seconds", 99 * 3600))
        else:
            # Chapter: convert SRT-format timestamp strings to seconds.
            window_start_secs = _srt_timestamp_to_seconds(chapter.get("start_time", "00:00:00,000"))
            window_end_secs = _srt_timestamp_to_seconds(chapter.get("end_time", "99:59:59,999"))
        windowed = [
            b
            for b in blocks
            if b["start_secs"] >= window_start_secs and b["end_secs"] <= window_end_secs
        ]
        config["srt_fragment"] = " ".join(b["text"] for b in windowed)[:10_000]
        # Note (issue #146 Fix C): the turn subtitles.srt sidecar is now written
        # exclusively by the nightly speaker_turn_prepare DAG. The upload path no
        # longer writes it; srt_fragment above is still needed for the lapidary
        # thumbnail quote (chapter path) and is harmless for turns.

    return config


def _extract_metadata_title_description(
    youtube_metadata_results: dict | None,
) -> tuple[str, str]:
    """Extract (title, description) from youtube_metadata_results XCom payload.

    Handles both dict-wrapped values (``{"title": "..."}`` nested dict) and
    plain string values. Returns ``("", "")`` on any missing or empty input.

    Args:
        youtube_metadata_results: The XCom value pushed by _generate_youtube_metadata.
            Shape: ``{"topic_metadata": [{"title": {...}|str, "description": {...}|str}]}``.

    Returns:
        A tuple ``(title, description)`` where both are strings (may be empty).
    """
    topic = (youtube_metadata_results or {}).get("topic_metadata") or []
    if not topic:
        return "", ""
    entry = topic[0]
    t = entry.get("title") or {}
    d = entry.get("description") or {}
    title = t.get("title", "") if isinstance(t, dict) else str(t)
    desc = d.get("description", "") if isinstance(d, dict) else str(d)
    return title, desc


def _sanitize_row_for_xcom(row: dict) -> dict:
    """Return a NEW row dict with every datetime.datetime value replaced by
    its ISO-8601 string, so the row survives Airflow's XCom serializer.

    TIMESTAMPTZ columns (materialized_at, prepared_at) arrive from psycopg2 as
    datetime objects with a non-zero fixed-offset stdlib tzinfo; Airflow encodes
    those as an empty-named tz that crashes on xcom_pull (issue #163). Values of
    other types (date, Decimal, str, int, float, list, None) pass through
    unchanged. Not recursive: the row is flat.
    """
    return {
        k: (v.isoformat() if isinstance(v, datetime) else v)
        for k, v in row.items()
    }


def _run_get_uploadable_item(db) -> dict | None:
    """Select the next item from the turn queue only.

    Returns a dict with keys:
      - 'item': the row dict (turn)
      - 'item_type': 'turn'
    Returns None when the turn queue is empty.
    """
    turns = db.get_uploadable_turns(limit=1)
    if turns:
        return {"item": _sanitize_row_for_xcom(turns[0]), "item_type": "turn"}

    return None


def _thumbnail_failure(chapter_id: int | None) -> dict:
    return {
        "chapter_id": chapter_id,
        "success": False,
        "output_path": None,
        "title": None,
    }


def trigger_thumbnail_generation(ti, **context) -> str | None:
    """Run the generic thumbnail DAG and retain its result for upload configuration."""
    thumbnail_config = ti.xcom_pull(key="thumbnail_config") or {}
    chapter_id = thumbnail_config.get("chapter_id")
    required_values = ("chapter_id", "debate_summary", "session", "domain")
    if not all(thumbnail_config.get(key) for key in required_values):
        logging.info(
            "Thumbnail input incomplete for chapter_id=%s; uploading without custom thumbnail",
            chapter_id,
        )
        ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
        return None

    child_conf = {
        "youtube_video_id": str(chapter_id),
        **{key: thumbnail_config[key] for key in required_values},
        "slug": thumbnail_config.get("slug"),
        "key_speakers": thumbnail_config.get("key_speakers") or [],
    }
    if "srt_fragment" in thumbnail_config:
        child_conf["srt_fragment"] = thumbnail_config["srt_fragment"]
    if thumbnail_config.get("output_path"):
        child_conf["output_path"] = thumbnail_config["output_path"]
    try:
        dag_run = trigger_dag_api(
            dag_id=_THUMBNAIL_DAG_ID,
            conf=child_conf,
            run_id=f"chapter_thumbnail_{context['run_id']}",
        )
    except Exception as exc:
        logging.warning(
            "Could not trigger thumbnail DAG for chapter_id=%s: %s", chapter_id, exc
        )
        ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
        return None

    child_run_id = dag_run.run_id
    ti.xcom_push(key="thumbnail_dag_run_id", value=child_run_id)
    logging.info("Triggered thumbnail DAG run: %s", child_run_id)

    while True:
        time.sleep(10)
        dag_run.refresh_from_db()
        if dag_run.state not in ["success", "failed"]:
            continue

        if dag_run.state != "success":
            logging.warning("Thumbnail DAG run %s failed", child_run_id)
            ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
            return child_run_id

        result = XCom.get_one(
            dag_id=_THUMBNAIL_DAG_ID,
            task_id=_THUMBNAIL_RESULT_TASK_ID,
            key="return_value",
            run_id=child_run_id,
        )
        if not (
            isinstance(result, dict)
            and result.get("success") is True
            and result.get("chapter_id") == chapter_id
            and isinstance(result.get("output_path"), str)
            and result["output_path"]
            and isinstance(result.get("title"), str)
            and result["title"]
        ):
            logging.warning(
                "Thumbnail DAG run %s returned no valid result", child_run_id
            )
            ti.xcom_push(key="thumbnail_result", value=_thumbnail_failure(chapter_id))
            return child_run_id

        ti.xcom_push(key="thumbnail_result", value=result)
        return child_run_id


def _backfill_thumbnail_video_id(ti, db=None) -> None:
    """Back-fill the real YouTube video ID in video_thumbnails after upload.

    Reads thumbnail_result and upload_results XComs. Calls
    ``db.update_thumbnail_youtube_video_id`` only when the thumbnail
    generation succeeded (``success is True``).

    Args:
        ti: Airflow TaskInstance.
        db: CongressionalVideoDB instance (injected for testability; created
            internally when None).
    """
    thumbnail_result = ti.xcom_pull(key="thumbnail_result") or {}
    if not thumbnail_result.get("success"):
        logging.info(
            "_backfill_thumbnail_video_id: thumbnail_result.success is False — "
            "skipping back-fill"
        )
        return

    upload_results = ti.xcom_pull(key="upload_results") or {}
    upload_details = upload_results.get("upload_details", [])

    chapter_id = thumbnail_result.get("chapter_id")

    # Find the matching upload detail for this chapter
    youtube_video_id = None
    for detail in upload_details:
        if detail.get("chapter_id") == chapter_id:
            youtube_video_id = detail.get("youtube_video_id")
            break

    if youtube_video_id is None:
        logging.warning(
            "_backfill_thumbnail_video_id: no youtube_video_id found for "
            "chapter_id=%s in upload_details",
            chapter_id,
        )
        return

    if db is None:
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()

    db.update_thumbnail_youtube_video_id(
        chapter_id=chapter_id, youtube_video_id=youtube_video_id
    )
    logging.info(
        "_backfill_thumbnail_video_id: chapter_id=%s → %r",
        chapter_id,
        youtube_video_id,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with (
    DAG(
        "congress_youtube_chapter_uploader",
        default_args=default_args,
        description="Upload top congressional video chapters to YouTube based on relevance score",
        schedule="0 19 * * *",  # Run once daily at 19:00 UTC; gate enforces one long video per calendar day
        start_date=datetime(2025, 11, 14),
        catchup=False,
        tags=["congress", "youtube", "chapters"],
        params={
            "max_chapters": 1,
            "min_relevance_score": 2,
            "isTesting": False,
            "dry_run": False,  # Set to True to run the full pipeline without triggering the YouTube upload
        },
    ) as dag
):
    # Step 0: Ensure data directory exists
    t0 = PythonOperator(
        task_id="ensure_data_directory",
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory("congress_videos"),
            "data_directory_path",
        ),
    )

    # Step 1: Check daily upload quota
    # Queries DB for uploads today and pending queue size.
    # Returns {queue_size, uploads_today}.
    t1_quota = PostgreSQLOperator(
        task_id="check_upload_quota",
        operation="check_upload_quota",
        output_xcom_key="upload_quota",
    )

    # Step 1b: Short-circuit based on queue_size vs time-of-day threshold
    t1_skip = ShortCircuitOperator(
        task_id="skip_if_quota_reached",
        python_callable=should_upload,
    )

    # Step 2: Turn-only queue; None when empty (no chapter fallback) (limit=1 per run)
    def _run_get_uploadable_item_task(ti):
        """Select upload item from the turn queue only.

        Pushes 'uploadable_item' XCom key with dict:
          {'item': <row dict>, 'item_type': 'turn'}
        or {'item': None, 'item_type': None} when the turn queue is empty.
        """
        from congress_videos.modules.database import CongressionalVideoDB

        db = CongressionalVideoDB()
        result = _run_get_uploadable_item(db)
        if result is None:
            result = {"item": None, "item_type": None}
        ti.xcom_push(key="uploadable_item", value=result)
        logging.info(
            "Selected uploadable item: item_type=%s, item=%s",
            result.get("item_type"),
            result.get("item", {}).get("chapter_id") if result.get("item") else None,
        )

    def _generate_youtube_metadata(ti):
        from congress_videos.modules.youtube import youtube_ai

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        # Pass a single-item list matching the chapter-list contract expected by youtube_ai
        items = [item] if item else []
        return xcom_task(
            ti,
            lambda: youtube_ai.generate_youtube_metadata_for_selected_videos(items),
            "youtube_metadata_results",
        )

    def _run_prepare_thumbnail_config(ti):
        """Resolve speaker name and build thumbnail config struct for the item (turn or chapter)."""
        from congress_videos.modules.database import CongressionalVideoDB

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        db = CongressionalVideoDB()
        result = _prepare_thumbnail_config(item, db)
        ti.xcom_push(key="thumbnail_config", value=result)

    def _run_generate_thumbnail(ti):
        """Trigger the generic thumbnail DAG for both turns and chapters (issue #169).

        Turn items now go through the same thumbnail generation path as chapters:
        _prepare_thumbnail_config already resolves the turn's speaker slug and
        sets output_path, so trigger_thumbnail_generation writes thumbnail.png to
        the canonical #133 turn directory.
        """
        return trigger_thumbnail_generation(ti, run_id=ti.run_id)

    def _extract_chapter_videos(ti):
        """Extract chapter video via ffmpeg, or use the pre-materialized turn video path."""
        from congress_videos.modules import video_splitter

        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item = uploadable.get("item") or {}
        item_type = uploadable.get("item_type")

        if item_type == "turn":
            # Turn video already exists at output_path — no ffmpeg extraction needed.
            output_path = item.get("output_path")
            success = bool(output_path)
            result = {
                "total_chapters": 1,
                "successful_extractions": 1 if success else 0,
                "failed_extractions": 0 if success else 1,
                "results": [
                    {
                        "chapter_id": item.get("chapter_id"),
                        "turn_id": item.get("turn_id"),
                        "video_id": item.get("video_id"),
                        "success": success,
                        "output_path": output_path,
                        "file_size_mb": None,
                        "duration_seconds": None,
                        "error": None if success else "turn output_path missing",
                    }
                ],
            }
            ti.xcom_push(key="chapter_extraction_results", value=result)
            return result
        else:
            # Chapter: call video_splitter (ffmpeg-based extraction)
            chapters = [item] if item else []
            return xcom_task(
                ti,
                lambda: video_splitter.extract_chapters_from_video(
                    chapters,
                    ti.xcom_pull(key="data_directory_path"),
                ),
                "chapter_extraction_results",
            )

    def _prepare_upload_config(ti, **context):
        """Build upload config for the selected item (turn or chapter).

        Turn items: read pre-prepared sidecars via prepare_orador_upload_config.
          - Zero AI calls, zero ffmpeg calls, zero thumbnail triggers.
          - Validates sidecar presence; raises FileNotFoundError if any is missing.
        Chapter items: unchanged path via prepare_chapter_upload_config + AI metadata.
        """
        from congress_videos.modules.youtube import prepare_chapter_upload_config
        from congress_videos.modules.youtube.youtube_upload import prepare_orador_upload_config

        dry_run = context.get("params", {}).get("dry_run", False)
        is_testing = context.get("params", {}).get("isTesting", False)
        uploadable = ti.xcom_pull(key="uploadable_item") or {}
        item_type = uploadable.get("item_type")

        if item_type == "turn":
            # Turn path: read pre-written sidecars; build a minimal extraction_results
            # wrapper so the generic uploader receives the same envelope shape.
            extraction_results = ti.xcom_pull(key="chapter_extraction_results") or {}
            results = extraction_results.get("results") or []
            if not results or not results[0].get("success"):
                logging.warning(
                    "_prepare_upload_config: turn extraction result missing or failed — "
                    "skipping upload config"
                )
                ti.xcom_push(key="upload_config", value=None)
                return None

            output_path = results[0].get("output_path") or ""
            if not output_path:
                logging.warning(
                    "_prepare_upload_config: turn output_path missing — skipping"
                )
                ti.xcom_push(key="upload_config", value=None)
                return None

            # Issue #169: overwrite title.txt/description.txt from fresh 19:00 AI metadata
            # BEFORE prepare_orador_upload_config reads them from disk.
            # This ensures fresh 19:00 AI output wins over any stale prepare-side sidecars.
            from congress_videos.modules.youtube.youtube_upload import _write_orador_sidecars

            youtube_metadata_results = ti.xcom_pull(key="youtube_metadata_results")
            fresh_title, fresh_desc = _extract_metadata_title_description(youtube_metadata_results)
            _write_orador_sidecars(output_path, fresh_title, fresh_desc)
            logging.info(
                "_prepare_upload_config: turn path — overwrote sidecars from XCom metadata, "
                "title=%r",
                fresh_title[:60] if fresh_title else "",
            )

            turn_config = prepare_orador_upload_config(
                output_path=output_path,
                is_testing=is_testing,
            )
            # Add turn_id + chapter_id from extraction result for upload tracking
            turn_config["chapter_id"] = results[0].get("chapter_id")
            turn_config["turn_id"] = results[0].get("turn_id")
            turn_config["video_id"] = results[0].get("video_id")

            from congress_videos.config.youtube_channels import (
                DEFAULT_CHANNEL,
                resolve_token_path,
            )

            config = {
                "token_file": resolve_token_path(DEFAULT_CHANNEL, "upload"),
                "videos": [turn_config],
            }
            logging.info(
                "_prepare_upload_config: turn path — sidecars read from %s, title=%r",
                os.path.dirname(output_path),
                turn_config.get("title", "")[:60],
            )
            ti.xcom_push(key="upload_config", value=config)
            return config

        # Chapter path (unchanged).
        return xcom_task(
            ti,
            lambda: prepare_chapter_upload_config(
                ti.xcom_pull(key="chapter_extraction_results"),
                ti.xcom_pull(key="youtube_metadata_results"),
                thumbnail_result=ti.xcom_pull(key="thumbnail_result"),
                is_testing=is_testing,
                dry_run=dry_run,
            ),
            "upload_config",
        )

    def _run_backfill_thumbnail_video_id(ti):
        """Back-fill youtube_video_id in video_thumbnails after upload completes."""
        _backfill_thumbnail_video_id(ti)

    t1_item = PythonOperator(
        task_id="get_uploadable_item",
        python_callable=_run_get_uploadable_item_task,
    )

    # Step 2: Generate YouTube metadata for the selected item (turn or chapter)
    t2 = PythonOperator(
        task_id="generate_youtube_metadata",
        python_callable=_generate_youtube_metadata,
    )

    # Step 3 (new): Prepare thumbnail config — resolve speaker, build config struct
    t3_prepare = PythonOperator(
        task_id="prepare_thumbnail_config",
        python_callable=_run_prepare_thumbnail_config,
    )

    # Step 4 (new): Generate thumbnail via the generic thumbnail DAG
    t4_generate = PythonOperator(
        task_id="generate_thumbnail",
        python_callable=_run_generate_thumbnail,
    )

    # Step 5: Extract video — turn path reused directly; chapter extracted via ffmpeg
    t5 = PythonOperator(
        task_id="extract_chapter_videos",
        python_callable=_extract_chapter_videos,
    )

    # Step 6: Prepare upload configuration for generic YouTube uploader DAG
    t6 = PythonOperator(
        task_id="prepare_upload_config",
        python_callable=_prepare_upload_config,
    )

    # Step 7: Trigger generic YouTube uploader DAG and wait for completion
    def trigger_upload_with_config(ti, **context):
        """Trigger the generic YouTube uploader DAG with config from XCom."""
        import time
        from airflow.models import XCom

        if context.get("params", {}).get("dry_run", False):
            logging.info("dry_run=True — skipping YouTube upload")
            ti.xcom_push(key="upload_results", value={"upload_details": []})
            return None

        # Get config from XCom
        config = ti.xcom_pull(key="upload_config")

        if not config:
            logging.warning("No upload config found, skipping upload")
            ti.xcom_push(key="upload_results", value={"upload_details": []})
            return None

        # Trigger the DAG
        logging.info(
            f"Triggering generic_youtube_uploader with {len(config.get('videos', []))} videos"
        )
        dag_run = trigger_dag_api(
            dag_id="generic_youtube_uploader",
            conf=config,
            run_id=f"chapter_upload_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")

        # Wait for completion
        logging.info("Waiting for upload to complete...")
        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ["success", "failed"]:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

                # Pull upload results from the triggered DAG
                upload_results = XCom.get_many(
                    execution_date=dag_run.execution_date,
                    dag_ids=["generic_youtube_uploader"],
                    task_ids=["upload_videos"],
                    key="return_value",
                    limit=1,
                )

                if upload_results:
                    results_data = upload_results[0].value
                    logging.info(f"Retrieved upload results: {results_data}")
                    ti.xcom_push(key="upload_results", value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    # Create results based on config and DAG state
                    upload_details = []
                    for video_config in config.get("videos", []):
                        upload_details.append(
                            {
                                "chapter_id": video_config.get("chapter_id"),
                                "video_id": video_config.get("video_id"),
                                "video_file": video_config.get("video_file"),
                                "success": dag_run.state == "success",
                                "youtube_video_id": None,
                                "error": "Upload failed - no results available"
                                if dag_run.state == "failed"
                                else None,
                            }
                        )
                    ti.xcom_push(
                        key="upload_results", value={"upload_details": upload_details}
                    )

                return dag_run.run_id

    def _check_upload_failures(ti):
        """Raise after DB writes so failures are visible in the Airflow UI."""
        updates = ti.xcom_pull(key="chapter_upload_updates")
        if updates is None:
            raise Exception(
                "chapter_upload_updates XCom missing after mark_chapters_uploaded succeeded"
            )

        recorded = updates.get("recorded_failures", 0)
        failed = updates.get("failed_updates", 0)
        if recorded > 0 or failed > 0:
            bad = [
                d
                for d in updates.get("details", [])
                if d.get("status") in ("failure_recorded", "failed")
            ]
            raise Exception(
                f"Chapter upload failures: {recorded} recorded, {failed} "
                f"DB-update failures. Chapters: {[d.get('chapter_id') for d in bad]}"
            )

        logging.info("No chapter upload failures recorded")

    t7 = PythonOperator(
        task_id="trigger_youtube_upload",
        python_callable=trigger_upload_with_config,
    )

    # Step 8: Update database to mark chapters as uploaded
    t8_db = PostgreSQLOperator(
        task_id="mark_chapters_uploaded",
        operation="mark_chapters_uploaded",
        xcom_keys={"upload_results": "upload_results"},
        output_xcom_key="chapter_upload_updates",
    )

    # Step 8c: Mark turn videos as uploaded (runs in parallel with mark_chapters_uploaded)
    t8_turns = PostgreSQLOperator(
        task_id="mark_turns_uploaded",
        operation="mark_turns_uploaded",
        xcom_keys={"upload_results": "upload_results"},
        output_xcom_key="turn_upload_updates",
    )

    # Step 8b: Back-fill youtube_video_id in video_thumbnails
    t8_backfill = PythonOperator(
        task_id="backfill_thumbnail_video_id",
        python_callable=_run_backfill_thumbnail_video_id,
    )

    # Step 9: Alert when chapter upload failures were recorded (after DB write)
    t9 = PythonOperator(
        task_id="check_upload_failures",
        python_callable=_check_upload_failures,
    )

    # Task dependencies (14 tasks total)
    # t0 > t1_quota > t1_skip > t1_item > t2 > t3_prepare > t4_generate > t5 > t6 > t7 > [t8_db, t8_turns] > t8_backfill > t9
    (
        t0
        >> t1_quota
        >> t1_skip
        >> t1_item
        >> t2
        >> t3_prepare
        >> t4_generate
        >> t5
        >> t6
        >> t7
        >> [t8_db, t8_turns]
        >> t8_backfill
        >> t9
    )
