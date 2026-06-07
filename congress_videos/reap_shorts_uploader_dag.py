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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_api

from congress_videos.modules.database import CongressionalVideoDB
from congress_videos.config.paths import YOUTUBE_TOKEN_FILE
from congress_videos.config.ai_prompts import (
    SHORTS_METADATA_SYSTEM_PROMPT,
    SHORTS_METADATA_USER_PROMPT_TEMPLATE,
)
from utils.ai_helpers import generate_json_completion, truncate_text
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local
from utils.whisper_helpers import transcribe_audio_file

load_env_if_local()

POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'reap_shorts_uploader',
    default_args=default_args,
    description='Upload one Reap Short to YouTube per run with AI-generated title/description',
    schedule='0,30 17-20 * * *',
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'youtube', 'shorts', 'reap'],
    params={
        'max_shorts_per_run': 1,
        'min_virality_score': 0.0,
    }
) as dag:

    def _get_pending_shorts(ti, **context):
        max_shorts = context['params'].get('max_shorts_per_run', 1)
        min_virality = context['params'].get('min_virality_score', 0.0)

        db = CongressionalVideoDB()
        shorts = db.get_pending_shorts(limit=max_shorts, min_virality_score=min_virality)

        if not shorts:
            logging.info("No pending shorts to upload")

        ti.xcom_push(key='pending_shorts', value=shorts)

    t1 = PythonOperator(
        task_id='get_pending_shorts',
        python_callable=_get_pending_shorts,
    )

    def _generate_metadata(ti, **context):
        import subprocess
        import tempfile

        pending_shorts = ti.xcom_pull(key='pending_shorts') or []

        if not pending_shorts:
            ti.xcom_push(key='shorts_metadata', value=[])
            return

        db = CongressionalVideoDB()
        metadata_list = []

        for short in pending_shorts:
            short_id = short.get('id')
            chapter_id = short.get('chapter_id')
            video_path = short.get('local_file_path')

            ch = db.get_chapter_metadata(chapter_id) if chapter_id else {}

            chapter_title = ch.get('title') or f'Short clip {short_id}'
            key_speakers = ch.get('key_speakers') or ch.get('speakers') or []
            speakers = ', '.join(key_speakers) if key_speakers else 'Diputados del Congreso'
            topics = ', '.join(ch.get('topics') or []) or 'Debate parlamentario'
            scoring_reasoning = ch.get('scoring_reasoning') or ''

            # Fallback metadata — used if Whisper or GPT fail
            title = truncate_text(f"{chapter_title} #Shorts", max_length=100)
            description = '🏛️ Debate en el Congreso de los Diputados.\n\n#Congreso #España #Política #Shorts'

            transcript = None
            if video_path and os.path.exists(video_path):
                try:
                    with tempfile.NamedTemporaryFile(suffix='.wav', delete=True) as tmp:
                        ffmpeg_result = subprocess.run(
                            [
                                'ffmpeg', '-i', video_path,
                                '-vn', '-acodec', 'pcm_s16le', '-ar', '16000', '-ac', '1',
                                tmp.name, '-y',
                            ],
                            capture_output=True,
                            timeout=60,
                        )
                        if ffmpeg_result.returncode == 0:
                            whisper_result = transcribe_audio_file(
                                tmp.name, language='es',
                                use_local_whisper=True, model_size='tiny',
                                save_srt=False,
                            )
                            if whisper_result.get('success'):
                                transcript = whisper_result.get('text', '').strip()
                                logging.info(f"Transcribed short {short_id}: {len(transcript)} chars")
                        else:
                            logging.warning(
                                f"ffmpeg failed for short {short_id}: "
                                f"{ffmpeg_result.stderr.decode()[:200]}"
                            )
                except Exception as e:
                    logging.warning(f"Audio extraction failed for short {short_id}: {e}")
            else:
                logging.warning(f"Video file not found for short {short_id}: {video_path}")

            if transcript:
                user_prompt = SHORTS_METADATA_USER_PROMPT_TEMPLATE.format(
                    transcript=transcript[:2000],
                    chapter_title=chapter_title,
                    speakers=speakers,
                    topics=topics,
                    scoring_reasoning=scoring_reasoning[:500],
                )
                ai_result = generate_json_completion(
                    system_prompt=SHORTS_METADATA_SYSTEM_PROMPT,
                    user_prompt=user_prompt,
                    model='gpt-4o-mini',
                    max_tokens=400,
                )
                if ai_result.get('data'):
                    ai_title = ai_result['data'].get('title', '').strip()
                    ai_description = ai_result['data'].get('description', '').strip()
                    if ai_title:
                        title = truncate_text(ai_title, max_length=100)
                    if ai_description:
                        description = ai_description
                    logging.info(f"AI metadata for short {short_id}: title='{title}'")
                else:
                    logging.warning(
                        f"GPT metadata generation failed for short {short_id}: "
                        f"{ai_result.get('error')}"
                    )

            metadata_list.append({
                'short_id': short_id,
                'title': title,
                'description': description,
            })

        ti.xcom_push(key='shorts_metadata', value=metadata_list)

    t2 = PythonOperator(
        task_id='generate_metadata',
        python_callable=_generate_metadata,
    )

    def _trigger_youtube_upload(ti, **context):
        import time
        from airflow.models import DagRun, XCom

        pending_shorts = ti.xcom_pull(key='pending_shorts') or []
        shorts_metadata = ti.xcom_pull(key='shorts_metadata') or []

        if not pending_shorts:
            logging.info("No pending shorts — skipping upload")
            ti.xcom_push(key='upload_results', value={'upload_details': []})
            return None

        videos = []
        for short, meta in zip(pending_shorts, shorts_metadata):
            short_id = short.get('id')

            video_config = {
                'short_id': short_id,
                'reap_clip_id': short.get('reap_clip_id'),
                'video_file': short.get('local_file_path'),
                'title': meta.get('title') or f'Short clip {short_id} #Shorts',
                'description': meta.get('description') or '#Shorts',
                'category_id': '25',
                'privacy_status': 'public',
                'tags': ['shorts', 'congress', 'politics', 'españa', 'congreso'],
                'made_for_kids': False,
            }
            videos.append(video_config)

        config = {
            'token_file': YOUTUBE_TOKEN_FILE,
            'videos': videos,
        }

        logging.info(f"Triggering generic_youtube_uploader with {len(videos)} shorts")
        dag_run = trigger_dag_api(
            dag_id='generic_youtube_uploader',
            conf=config,
            run_id=f"shorts_upload_{context['run_id']}",
        )

        logging.info(f"Triggered DAG run: {dag_run.run_id}")
        logging.info("Waiting for upload to complete...")

        while True:
            time.sleep(10)
            dag_run.refresh_from_db()

            if dag_run.state in ['success', 'failed']:
                logging.info(f"Upload DAG completed with state: {dag_run.state}")

                upload_results = XCom.get_many(
                    execution_date=dag_run.execution_date,
                    dag_ids=['generic_youtube_uploader'],
                    task_ids=['upload_videos'],
                    key='return_value',
                    limit=1,
                )

                if upload_results:
                    results_data = upload_results[0].value
                    logging.info(f"Retrieved upload results: {results_data}")
                    file_to_meta = {v['video_file']: v for v in videos}
                    enriched = []
                    for detail in results_data.get('upload_details', []):
                        meta = file_to_meta.get(detail.get('video_file'), {})
                        enriched.append({**detail, 'reap_clip_id': meta.get('reap_clip_id'), 'short_id': meta.get('short_id')})
                    results_data = {**results_data, 'upload_details': enriched}
                    ti.xcom_push(key='upload_results', value=results_data)
                else:
                    logging.warning("No upload results found from triggered DAG")
                    upload_details = []
                    for video_config in videos:
                        upload_details.append({
                            'short_id': video_config.get('short_id'),
                            'reap_clip_id': video_config.get('reap_clip_id'),
                            'video_file': video_config.get('video_file'),
                            'success': dag_run.state == 'success',
                            'youtube_video_id': None,
                            'error': 'Upload failed - no results available' if dag_run.state == 'failed' else None,
                        })
                    ti.xcom_push(key='upload_results', value={'upload_details': upload_details})

                if dag_run.state == 'failed':
                    raise Exception(f"Upload DAG failed: {dag_run.run_id}")

                return dag_run.run_id

    t3 = PythonOperator(
        task_id='trigger_youtube_upload',
        python_callable=_trigger_youtube_upload,
    )

    def _mark_shorts_uploaded(ti, **context):
        upload_results = ti.xcom_pull(key='upload_results') or {}
        upload_details = upload_results.get('upload_details', [])

        if not upload_details:
            logging.info("No upload results to process")
            return

        db = CongressionalVideoDB()
        successful = 0
        failed = 0

        for detail in upload_details:
            reap_clip_id = detail.get('reap_clip_id')
            youtube_video_id = detail.get('youtube_video_id')
            if detail.get('success') and reap_clip_id and youtube_video_id:
                db.mark_short_uploaded(reap_clip_id, youtube_video_id)
                successful += 1
            else:
                failed += 1

        logging.info(f"Upload summary: {successful} successful, {failed} failed")

    t4 = PythonOperator(
        task_id='mark_shorts_uploaded',
        python_callable=_mark_shorts_uploaded,
    )

    t1 >> t2 >> t3 >> t4
