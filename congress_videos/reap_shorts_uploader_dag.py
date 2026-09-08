"""
Reap Shorts Uploader DAG

Uploads one downloaded Reap Short to YouTube per run, with AI-generated title and
description derived from audio transcription (Whisper) + chapter metadata (GPT-4o-mini).

Flow:
1. get_pending_shorts   — claim the highest-virality unuploaded clip
2. generate_metadata    — extract audio → Whisper transcript → GPT title+description
3. trigger_youtube_upload — upload via generic_youtube_uploader
4. mark_shorts_uploaded — persist youtube_video_id + is_uploaded=TRUE
"""

import logging
import os
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api
from airflow.operators.python import PythonOperator

from congress_videos.config.ai_prompts import (
    SHORTS_METADATA_MENTIONED_PEOPLE_INSTRUCTION,
    SHORTS_METADATA_SYSTEM_PROMPT,
    SHORTS_METADATA_USER_PROMPT_TEMPLATE,
)
from congress_videos.config.youtube_channels import DEFAULT_CHANNEL, resolve_token_path
from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.modules.participants_db import lookup_participant_by_slug
from utils.ai_helpers import generate_json_completion, truncate_text
from utils.env_loader import load_env_if_local
from utils.llm_config import LLM_DEFAULT
from utils.whisper_helpers import transcribe_audio_file

load_env_if_local()

POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA", "development")


def _resolve_speakers(ch: dict, preferred_primary: str = "") -> tuple[str, str]:
    """Return (primary_speaker, rest_speakers) filtering placeholders.

    Priority: key_speakers (placeholder-filtered) before speakers
    (placeholder-filtered). Deduplicates order-preserving. Returns ("", "")
    when the combined pool is empty after filtering.

    Args:
        preferred_primary: when non-empty (issue #433, design D4), promotes
            this name to the front of the pool ahead of key_speakers/speakers,
            de-duplicating so no name is lost. Used to render a turn's
            resolved speaker slug ahead of the chapter-level heuristic. The
            default keeps every existing caller's behaviour unchanged.
    """
    from congress_videos.modules.speaker_placeholders import is_placeholder

    key_speakers: list[str] = ch.get("key_speakers") or []
    speakers: list[str] = ch.get("speakers") or []

    seen: set[str] = set()
    pool: list[str] = []
    for name in list(key_speakers) + list(speakers):
        if not is_placeholder(name) and name not in seen:
            pool.append(name)
            seen.add(name)

    if preferred_primary:
        pool = [preferred_primary] + [n for n in pool if n.strip() != preferred_primary]

    if not pool:
        return ("", "")
    return (pool[0].strip(), ", ".join(pool[1:]))


def build_shorts_metadata_context(
    chapter: dict,
    turn_speaker_slug: str | None,
    participants_lookup,
) -> dict:
    """Resolve speaker, mentioned people and topics as three separate prompt inputs.

    Speaker precedence (issue #433, design D4; spec "Speaker identity
    precedence"): the turn's resolved_participant_slug, rendered via
    participants_lookup, wins over the chapter-level _resolve_speakers
    heuristic — that promotion happens at the call site, this function only
    resolves the display name. Mentioned people are resolved the same way,
    excluding the resolved speaker (by slug identity and by case-folded
    display-name equality) and dropping any slug that does not resolve.
    Topics pass through unmodified and are never merged into either people
    list (spec "A topic never renders as a person"). Every
    participants_lookup call is individually guarded — this function never
    raises.

    Args:
        chapter: row from CongressionalVideoDB.get_chapter_metadata.
        turn_speaker_slug: speaker_turn_videos.resolved_participant_slug for
            the short's turn, or None when unavailable.
        participants_lookup: callable slug -> row|None (injected, mirrors
            thumbnail_config.py's participants_lookup convention).

    Returns:
        {"speaker_display_name": str, "mentioned_display_names": list[str],
         "topics": list[str]}
    """
    speaker_display_name = ""
    if turn_speaker_slug:
        try:
            participant = participants_lookup(turn_speaker_slug)
        except Exception as exc:
            logging.warning(
                "build_shorts_metadata_context: speaker slug lookup failed for "
                "turn_speaker_slug=%r: %s — speaker_display_name stays empty",
                turn_speaker_slug,
                exc,
            )
            participant = None
        if participant and participant.get("display_name"):
            speaker_display_name = participant["display_name"]

    speaker_slug_key = (turn_speaker_slug or "").strip().lower()
    speaker_name_key = speaker_display_name.strip().lower()

    mentioned_display_names: list[str] = []
    seen_names: set[str] = set()
    for slug in chapter.get("mentioned_participant_slugs") or []:
        if slug and slug.strip().lower() == speaker_slug_key:
            continue
        try:
            participant = participants_lookup(slug)
        except Exception as exc:
            logging.info(
                "build_shorts_metadata_context: mentioned slug lookup failed for "
                "slug=%r: %s — dropped from mentioned people",
                slug,
                exc,
            )
            continue
        if not participant or not participant.get("display_name"):
            logging.info(
                "build_shorts_metadata_context: mentioned slug %r did not resolve — dropped from mentioned people",
                slug,
            )
            continue
        display_name = participant["display_name"]
        name_key = display_name.strip().lower()
        if name_key == speaker_name_key and speaker_name_key:
            continue
        if name_key in seen_names:
            continue
        seen_names.add(name_key)
        mentioned_display_names.append(display_name)

    return {
        "speaker_display_name": speaker_display_name,
        "mentioned_display_names": mentioned_display_names,
        "topics": chapter.get("topics") or [],
    }


_MONTHS = [
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


def _format_own_channel_footer(youtube_video_id: str | None) -> str:
    """Return an own-channel footer linking the full long-form video, or '' if ID is absent.

    Hard contract: if youtube_video_id is None or empty, no footer is appended.
    There is NO fallback to the source video URL.
    """
    if not youtube_video_id:
        return ""
    return f"\n\n📺 Vídeo completo:\nhttps://www.youtube.com/watch?v={youtube_video_id}"


def _format_session_line(session_number: int | None, session_date: date | None) -> str:
    """Return a Spanish attribution line for a congressional session, or '' if both args are falsy."""
    number_part = f"Sesión nº {session_number} del Congreso" if session_number else ""
    date_part = (
        f"{session_date.day} de {_MONTHS[session_date.month - 1]} de {session_date.year}" if session_date else ""
    )
    body = " - ".join(p for p in (number_part, date_part) if p)
    return f"\n\n🏛️ {body}" if body else ""


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "reap_shorts_uploader",
    default_args=default_args,
    description="Upload one Reap Short to YouTube per run with AI-generated title/description",
    # Maintain a three-hour buffer around the 19:00 UTC long upload: 15→19 and 19→22.
    schedule="0 8,10,13,15,22 * * *",
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=["congress", "youtube", "shorts", "reap"],
    params={
        "max_shorts_per_run": 1,
        "min_virality_score": 0.0,
    },
) as dag:

    def _get_pending_shorts(ti, **context):
        max_shorts = context["params"].get("max_shorts_per_run", 1)
        min_virality = context["params"].get("min_virality_score", 0.0)

        db = CongressionalVideoDB()
        shorts = db.get_pending_shorts(limit=max_shorts, min_virality_score=min_virality)

        if not shorts:
            logging.info("No pending shorts to upload")

        ti.xcom_push(key="pending_shorts", value=shorts)

    t1 = PythonOperator(
        task_id="get_pending_shorts",
        python_callable=_get_pending_shorts,
    )

    def _generate_metadata(ti, **context):
        import subprocess
        import tempfile

        pending_shorts = ti.xcom_pull(key="pending_shorts") or []

        if not pending_shorts:
            ti.xcom_push(key="shorts_metadata", value=[])
            return

        db = CongressionalVideoDB()
        metadata_list = []

        for short in pending_shorts:
            short_id = short.get("id")
            chapter_id = short.get("chapter_id")
            video_path = short.get("local_file_path")

            ch = db.get_chapter_metadata(chapter_id) if chapter_id else {}
            ch = ch or {}

            if chapter_id:
                # AC5/design D1 (option A): the analysis marker is operator-visible in
                # the task log. This is a single get_chapter_metadata read — no XCom
                # hop, no cached copy — so mentioned_participant_slugs and topics
                # always originate from the same row snapshot.
                logging.info(
                    "generate_metadata: chapter_id=%s content_analysis snapshot "
                    "updated_at=%s mentioned_participant_slugs=%s topics=%s",
                    chapter_id,
                    ch.get("updated_at"),
                    ch.get("mentioned_participant_slugs"),
                    ch.get("topics"),
                )

            turn_id = short.get("turn_id")
            turn_speaker_slug = None
            if turn_id:
                try:
                    turn_speaker_row = db.get_turn_speaker_slug(turn_id)
                except Exception as exc:
                    logging.warning(
                        "generate_metadata: turn speaker lookup failed for short_id=%s "
                        "turn_id=%s: %s — falls back to the chapter-level heuristic",
                        short_id,
                        turn_id,
                        exc,
                    )
                    turn_speaker_row = None
                if turn_speaker_row:
                    turn_speaker_slug = turn_speaker_row.get("resolved_participant_slug")

            metadata_context = build_shorts_metadata_context(ch, turn_speaker_slug, lookup_participant_by_slug)
            mentioned_display_names = metadata_context["mentioned_display_names"]

            chapter_title = ch.get("title") or f"Short clip {short_id}"
            primary_speaker, secondary_speakers = _resolve_speakers(
                ch, preferred_primary=metadata_context["speaker_display_name"]
            )
            topics = ", ".join(metadata_context["topics"]) or "Debate parlamentario"
            scoring_reasoning = ch.get("scoring_reasoning") or ""

            # Fallback metadata — used if Whisper or GPT fail
            title = truncate_text(
                f"{primary_speaker}: {chapter_title} #Shorts" if primary_speaker else f"{chapter_title} #Shorts",
                max_length=100,
            )
            description = "🏛️ Debate en el Congreso de los Diputados.\n\n#Congreso #España #Política #Shorts"

            transcript = None
            if video_path and os.path.exists(video_path):
                try:
                    with tempfile.NamedTemporaryFile(suffix=".wav", delete=True) as tmp:
                        ffmpeg_result = subprocess.run(
                            [
                                "ffmpeg",
                                "-i",
                                video_path,
                                "-vn",
                                "-acodec",
                                "pcm_s16le",
                                "-ar",
                                "16000",
                                "-ac",
                                "1",
                                tmp.name,
                                "-y",
                            ],
                            capture_output=True,
                            timeout=60,
                        )
                        if ffmpeg_result.returncode == 0:
                            whisper_result = transcribe_audio_file(
                                tmp.name,
                                language="es",
                                use_local_whisper=True,
                                model_size="tiny",
                                save_srt=False,
                            )
                            if whisper_result.get("success"):
                                transcript = whisper_result.get("text", "").strip()
                                logging.info(f"Transcribed short {short_id}: {len(transcript)} chars")
                        else:
                            logging.warning(
                                f"ffmpeg failed for short {short_id}: {ffmpeg_result.stderr.decode()[:200]}"
                            )
                except Exception as e:
                    logging.warning(f"Audio extraction failed for short {short_id}: {e}")
            else:
                logging.warning(f"Video file not found for short {short_id}: {video_path}")

            if transcript:
                user_prompt = SHORTS_METADATA_USER_PROMPT_TEMPLATE.format(
                    transcript=transcript[:2000],
                    chapter_title=chapter_title,
                    primary_speaker=primary_speaker,
                    secondary_speakers=secondary_speakers,
                    topics=topics,
                    scoring_reasoning=scoring_reasoning[:500],
                )
                if mentioned_display_names:
                    mentioned_list = "\n".join(f"- {name}" for name in mentioned_display_names)
                    user_prompt += SHORTS_METADATA_MENTIONED_PEOPLE_INSTRUCTION.format(mentioned_list=mentioned_list)
                ai_result = generate_json_completion(
                    system_prompt=SHORTS_METADATA_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    model=LLM_DEFAULT,
                )
                if ai_result.get("data"):
                    ai_title = ai_result["data"].get("title", "").strip()
                    ai_description = ai_result["data"].get("description", "").strip()
                    if ai_title:
                        title = truncate_text(ai_title, max_length=100)
                    if ai_description:
                        description = ai_description
                    logging.info(f"AI metadata for short {short_id}: title='{title}'")
                else:
                    logging.warning(f"GPT metadata generation failed for short {short_id}: {ai_result.get('error')}")

            description += _format_own_channel_footer(ch.get("youtube_video_id"))
            description += _format_session_line(ch.get("session_number"), ch.get("session_date"))

            metadata_list.append(
                {
                    "short_id": short_id,
                    "title": title,
                    "description": description,
                }
            )

        ti.xcom_push(key="shorts_metadata", value=metadata_list)

    t2 = PythonOperator(
        task_id="generate_metadata",
        python_callable=_generate_metadata,
    )

    def _trigger_youtube_upload(ti, **context):
        import time

        from airflow.models import XCom

        pending_shorts = ti.xcom_pull(key="pending_shorts") or []
        shorts_metadata = ti.xcom_pull(key="shorts_metadata") or []

        if not pending_shorts:
            logging.info("No pending shorts — skipping upload")
            ti.xcom_push(key="upload_results", value={"upload_details": []})
            return None

        videos = []
        for short, meta in zip(pending_shorts, shorts_metadata):
            short_id = short.get("id")

            video_config = {
                "short_id": short_id,
                "reap_clip_id": short.get("reap_clip_id"),
                "video_file": short.get("local_file_path"),
                "title": meta.get("title") or f"Short clip {short_id} #Shorts",
                "description": meta.get("description") or "#Shorts",
                "category_id": "25",
                "privacy_status": "public",
                "tags": ["shorts", "congress", "politics", "españa", "congreso"],
                "made_for_kids": False,
            }
            videos.append(video_config)

        config = {
            "token_file": resolve_token_path(DEFAULT_CHANNEL, "upload"),
            "videos": videos,
        }

        logging.info(f"Triggering generic_youtube_uploader with {len(videos)} shorts")
        dag_run = trigger_dag_api(
            dag_id="generic_youtube_uploader",
            conf=config,
            run_id=f"shorts_upload_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")
        logging.info("Waiting for upload to complete...")

        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ["success", "failed"]:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

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
                    file_to_meta = {v["video_file"]: v for v in videos}
                    enriched = []
                    for detail in results_data.get("upload_details", []):
                        meta = file_to_meta.get(detail.get("video_file"), {})
                        enriched.append(
                            {
                                **detail,
                                "reap_clip_id": meta.get("reap_clip_id"),
                                "short_id": meta.get("short_id"),
                            }
                        )
                    results_data = {**results_data, "upload_details": enriched}
                    ti.xcom_push(key="upload_results", value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    upload_details = []
                    for video_config in videos:
                        upload_details.append(
                            {
                                "short_id": video_config.get("short_id"),
                                "reap_clip_id": video_config.get("reap_clip_id"),
                                "video_file": video_config.get("video_file"),
                                "success": dag_run.state == "success",
                                "youtube_video_id": None,
                                "error": "Upload failed - no results available" if dag_run.state == "failed" else None,
                            }
                        )
                    ti.xcom_push(key="upload_results", value={"upload_details": upload_details})

                if dag_run.state == "failed":
                    raise Exception(f"Upload DAG failed: {dag_run.run_id}")

                return dag_run.run_id

    t3 = PythonOperator(
        task_id="trigger_youtube_upload",
        python_callable=_trigger_youtube_upload,
    )

    def _mark_shorts_uploaded(ti, **context):
        upload_results = ti.xcom_pull(key="upload_results") or {}
        upload_details = upload_results.get("upload_details", [])

        if not upload_details:
            logging.info("No upload results to process")
            return

        db = CongressionalVideoDB()
        successful = 0
        failed = 0

        for detail in upload_details:
            reap_clip_id = detail.get("reap_clip_id")
            youtube_video_id = detail.get("youtube_video_id")
            if detail.get("success") and reap_clip_id and youtube_video_id:
                db.mark_short_uploaded(reap_clip_id, youtube_video_id)
                successful += 1
            else:
                failed += 1
                if reap_clip_id:
                    db.record_short_upload_failure(reap_clip_id, detail.get("error"))
                else:
                    logging.warning(f"Skipping failure recording — detail without reap_clip_id: {detail}")

        logging.info(f"Upload summary: {successful} successful, {failed} failed")

    def _check_short_upload_failures(ti, **context):
        upload_results = ti.xcom_pull(key="upload_results")
        if upload_results is None:
            raise Exception("Upload results XCom missing — data integrity unknown")
        upload_details = upload_results.get("upload_details", [])
        if not upload_details:
            logging.info("No upload results to check")
            return
        failed = [
            d for d in upload_details if not (d.get("success") and d.get("reap_clip_id") and d.get("youtube_video_id"))
        ]
        if failed:
            raise Exception(f"{len(failed)} short(s) failed to upload (DB writes already committed)")
        logging.info("All shorts uploaded successfully")

    t4 = PythonOperator(
        task_id="mark_shorts_uploaded",
        python_callable=_mark_shorts_uploaded,
    )

    t5 = PythonOperator(
        task_id="check_short_upload_failures",
        python_callable=_check_short_upload_failures,
    )

    t1 >> t2 >> t3 >> t4 >> t5
