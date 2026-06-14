"""
YouTube Channel Monitor DAG for Congress Videos

This DAG monitors the official Congress YouTube channel for "Sesión Plenaria (original)"
videos and identifies finished streams that can be downloaded.

YouTube Channel: https://www.youtube.com/@CanalParlamento-Congreso_Es/streams

Workflow:
1. Fetch recent videos from the YouTube channel (streams tab)
2. Filter for videos titled "Sesión Plenaria (original)"
3. Check if streams are finished (not live)
4. Filter by target date parameter
5. Save video information to database
6. (Future) Download videos from YouTube in higher quality

This approach provides higher quality videos compared to downloading from the
Congress website directly.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

from congress_videos.config.constants import (
    YOUTUBE_CHANNEL_ID,
    TARGET_VIDEO_TITLE,
    VAD_ENABLED,
)
from congress_videos.modules import youtube as yt_channel
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from congress_videos.modules.vad_helpers import trim_chapter_silence_with_vad
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
IS_DEVELOPMENT = POSTGRES_SCHEMA == 'development'

today_str = datetime.now().strftime("%Y-%m-%d")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'congress_youtube_channel_monitor',
    default_args=default_args,
    description='Monitor YouTube channel for Congress plenary sessions and identify finished streams',
    schedule='0 * * * *',  # Run every hour on the hour
    start_date=datetime(2025, 10, 9),
    catchup=False,
    max_active_runs=1,  # Serialize runs so overlapping hourly runs don't race on the same video_id
    tags=['congress', 'youtube', 'monitor'],
    params={  # Default to today; lookback range covers yesterday too
        "target_date": today_str,
        "lookback_days": 1,  # Inclusive lookback window: target_date - lookback_days .. target_date
        "min_hours_since_end": 2,  # Skip videos whose live broadcast ended less than this many hours ago
        "max_videos": 20,  # Maximum number of videos to check
        "chunk_duration_minutes": 30,  # Duration of each audio chunk in minutes (default: 30 minutes)
        "isTesting": False,  # Set to True manually when testing
        "test_video_url": "https://www.youtube.com/watch?v=ZBU0bVpYXM4"  # Test video URL (used when isTesting=True)
    }
) as dag:

    # Step 0: Branch based on test mode
    def check_test_mode(**context):
        """Branch based on isTesting parameter."""
        is_testing = context["params"].get("isTesting", False)
        if is_testing:
            logging.info("Running in TEST MODE - using predefined test video")
            return 'create_test_video_data'
        else:
            logging.info("Running in PRODUCTION MODE - fetching from YouTube channel")
            return 'fetch_youtube_channel_videos'

    t0_branch = BranchPythonOperator(
        task_id='check_test_mode',
        python_callable=check_test_mode,
    )

    # Test mode: Create test video data
    t0_test = PythonOperator(
        task_id='create_test_video_data',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.create_test_video_data(
                test_video_url=context["params"].get("test_video_url", "https://www.youtube.com/watch?v=ZBU0bVpYXM4")
            ),
            'plenary_videos'
        ),
    )

    # Step 1: Fetch videos from YouTube channel (streams tab) - PRODUCTION MODE
    t1 = PythonOperator(
        task_id='fetch_youtube_channel_videos',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.fetch_youtube_channel_videos(
                channel_id=YOUTUBE_CHANNEL_ID,
                max_results=context["params"].get("max_videos", 10)
            ),
            'channel_videos'
        ),
    )

    # Step 2: Filter for "Sesión Plenaria (original)" videos
    t2 = PythonOperator(
        task_id='filter_plenary_sessions',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.filter_plenary_session_videos(
                ti.xcom_pull(key='channel_videos'),
                target_title=TARGET_VIDEO_TITLE,
                target_date=context["params"].get("target_date"),
                lookback_days=context["params"].get("lookback_days", 1)
            ),
            'plenary_videos'
        ),
    )

    # Step 2a: Check if any plenary sessions were found
    def check_plenary_found(ti):
        """Branch based on whether plenary sessions were found."""
        plenary_videos = ti.xcom_pull(key='plenary_videos')

        if plenary_videos and plenary_videos.get('total_matches', 0) > 0:
            logging.info(f"Found {plenary_videos['total_matches']} plenary session(s). Continuing to process.")
            return ['get_video_details', 'get_video_descriptions']
        else:
            logging.info("No plenary sessions found for target date. Ending DAG execution.")
            return 'no_plenary_sessions'

    t2a = BranchPythonOperator(
        task_id='check_if_plenary_found',
        python_callable=check_plenary_found,
    )

    # Step 2b: Idempotency filter - drop videos already fully processed in DB.
    # Runs BEFORE any download/transcription. PRODUCTION path only
    # (test path goes t0_test >> [t3a, t3b] and never reaches this task).
    t2b = PythonOperator(
        task_id='filter_unprocessed_videos',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.filter_unprocessed_videos(
                ti.xcom_pull(key='plenary_videos')
            ),
            'plenary_videos',          # overwrite same key
        ),
    )

    # Step 3a: Get video details (duration, timing, etc.)
    # Note: We already filtered for completed streams, no need to check status again
    # trigger_rule: Execute if any upstream task succeeds (test or production path)
    t3a = PythonOperator(
        task_id='get_video_details',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.get_video_details(
                ti.xcom_pull(key='plenary_videos'),
                min_hours_since_end=context["params"].get("min_hours_since_end", 2)
            ),
            'video_details'
        ),
        trigger_rule='none_failed_min_one_success'
    )

    # Step 3b: Get full video descriptions (runs in parallel with get_video_details)
    # trigger_rule: Execute if any upstream task succeeds (test or production path)
    t3b = PythonOperator(
        task_id='get_video_descriptions',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.get_video_descriptions(
                ti.xcom_pull(key='plenary_videos')
            ),
            'video_descriptions'
        ),
        trigger_rule='none_failed_min_one_success'
    )

    # Step 3c: Try to download existing SRT subtitles from YouTube (FIRST - fastest option!)
    t3c = PythonOperator(
        task_id='try_download_subtitles_from_youtube',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.try_download_subtitles_from_youtube(
                ti.xcom_pull(key='video_details'),
                target_date=context["params"].get("target_date")
            ),
            'youtube_subtitles'
        ),
    )

    # Step 3c_branch: Check if subtitles were downloaded successfully
    def check_subtitles_downloaded(ti):
        """Branch based on whether subtitles were downloaded from YouTube."""
        subtitle_results = ti.xcom_pull(key='youtube_subtitles')

        if subtitle_results and subtitle_results.get('total_downloaded', 0) > 0:
            logging.info("✅ Subtitles downloaded from YouTube! Skipping audio extraction and transcription.")
            # Subtitles available - go directly to split_srt_by_silence
            return 'split_srt_by_silence'
        else:
            logging.info("No subtitles available on YouTube. Will extract audio and transcribe.")
            # Need to extract audio and transcribe
            return 'extract_audio_from_youtube'

    t3c_branch = BranchPythonOperator(
        task_id='check_subtitles_available',
        python_callable=check_subtitles_downloaded,
    )

    # Step 3c2: Download video from YouTube (runs after subtitle check)
    # ENABLED: Video download needed for chapter extraction in youtube_upload_dag_v2
    t3c2 = PythonOperator(
        task_id='download_video_from_youtube',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.download_video_from_youtube(
                ti.xcom_pull(key='video_details'),
                target_date=context["params"].get("target_date")
            ),
            'downloaded_videos'
        ),
        trigger_rule='none_failed_min_one_success'  # Run regardless of which branch
    )

    # Step 3d: Extract audio from YouTube (only if subtitles not available)
    t3d = PythonOperator(
        task_id='extract_audio_from_youtube',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.extract_audio_from_youtube(
                ti.xcom_pull(key='video_details'),
                target_date=context["params"].get("target_date"),
                chunk_duration_minutes=context["params"].get("chunk_duration_minutes", 10)
            ),
            'extracted_audio'
        ),
    )

    # Step 3e: Transcribe audio using Whisper API
    t3e = PythonOperator(
        task_id='transcribe_audio_with_whisper',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.transcribe_audio_with_whisper(
                ti.xcom_pull(key='extracted_audio'),
                language="es",
                timeout=3600  # 1 hour timeout per chunk
            ),
            'transcriptions'
        ),
    )

    # Step 3f: Merge SRT files into single simplified file per video
    t3f = PythonOperator(
        task_id='merge_srt_files',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.merge_transcription_srt_files(
                ti.xcom_pull(key='transcriptions'),
                target_date=context["params"].get("target_date")
            ),
            'merged_srt_files'
        ),
    )

    # Step 4: Parse description links (Nota de prensa and Orden del día)
    t4 = PythonOperator(
        task_id='parse_description_links',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.parse_description_links(
                ti.xcom_pull(key='video_descriptions')
            ),
            'parsed_links'
        ),
        trigger_rule='none_failed_min_one_success'  # Run from either branch or t3b
    )

    # Step 5a: Scrape press release (Nota de prensa) - runs in parallel with agenda download
    t5a = PythonOperator(
        task_id='scrape_press_release',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.scrape_press_release(
                ti.xcom_pull(key='parsed_links')
            ),
            'press_releases'
        ),
    )

    # Step 5b: Download and read agenda PDF (Orden del día) - runs in parallel with press release scraping
    t5b = PythonOperator(
        task_id='download_and_read_agenda',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.download_and_read_agenda(
                ti.xcom_pull(key='parsed_links'),
                target_date=context["params"].get("target_date")
            ),
            'agendas'
        ),
    )

    # Step 5c: Extract session number based on target date position
    t5c = PythonOperator(
        task_id='extract_session_date',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.extract_session_date(
                ti.xcom_pull(key='agendas'),
                target_date=context["params"].get("target_date")
            ),
            'session_date'
        ),
    )

    # Step 5d: Extract the specific agenda section for the target date
    t5d = PythonOperator(
        task_id='extract_agenda_section',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.extract_agenda_section(
                ti.xcom_pull(key='agendas'),
                ti.xcom_pull(key='session_date')
            ),
            'agenda_section'
        ),
    )

    # Step 5e: Split SRT by silence gaps (TASK 1)
    # Split transcription into chunks at natural breaks (15+ second silences)
    # Ensures chunks are at least 20 minutes, merging smaller ones
    t5e = PythonOperator(
        task_id='split_srt_by_silence',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.split_srt_by_silence(
                ti.xcom_pull(key='merged_srt_files') or ti.xcom_pull(key='youtube_subtitles'),  # Try both sources
                target_date=context["params"].get("target_date"),
                min_silence_seconds=15,
                min_chunk_duration_minutes=10,
                max_chunk_duration_minutes=20,
                use_adaptive=True,  # #13: adaptive silence threshold active in production
            ),
            'silence_chunks'
        ),
        trigger_rule='none_failed_min_one_success'  # Run if either path succeeded
    )

    # Step 5f: Summarize silence chunks (TASK 2) — PARALLELIZED (#9)
    # Dynamic task mapping: each silence-chunk is summarized in its own mapped
    # task instance (.expand), then re-grouped into the chunk_summaries shape.
    # Chunks are path-only refs (#7), so each mapped XCom payload stays <1MB.

    # 5f-1: flatten silence_chunks (nested per-video) into a flat list of
    # chunk-refs that dynamic task mapping can expand over.
    t5f_flatten = PythonOperator(
        task_id='flatten_chunks_for_mapping',
        python_callable=lambda ti: yt_channel.flatten_chunks_for_mapping(
            ti.xcom_pull(key='silence_chunks')
        ),
    )

    # 5f-2: mapped summarization — one task instance per chunk-ref. Empty input
    # ⇒ zero mapped instances (Airflow supports empty expand).
    t5f_map = PythonOperator.partial(
        task_id='summarize_one_chunk',
        python_callable=yt_channel.summarize_one_chunk,
    ).expand(op_args=t5f_flatten.output.map(lambda ref: [ref]))

    # 5f-3: reduce mapped results back into the chunk_summaries shape consumed
    # by identify_interesting_chapters (identical to the serial output).
    t5f = PythonOperator(
        task_id='aggregate_chunk_summaries',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.regroup_summarized_chunks(
                ti.xcom_pull(task_ids='summarize_one_chunk')
            ),
            'chunk_summaries'
        ),
        trigger_rule='none_failed_min_one_success',  # tolerate per-chunk failures
    )

    # Step 6: Use AI to identify interesting chapters within each chunk
    # This task waits for chunk summaries (t5f) and chunked SRT data (t5e)
    t6 = PythonOperator(
        task_id='identify_interesting_chapters',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.identify_interesting_chapters(
                ti.xcom_pull(key='chunk_summaries'),  # Summaries from t5f
                ti.xcom_pull(key='silence_chunks'),   # SRT chunks from t5e
                target_date=context["params"].get("target_date")
            ),
            'identified_chapters'
        ),
        trigger_rule='none_failed_min_one_success'  # Run if either path succeeded
    )

    # Step 7: Merge interesting chapters from all chunks into final list
    t7 = PythonOperator(
        task_id='merge_interesting_chapters',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_channel.merge_interesting_chapters(
                ti.xcom_pull(key='identified_chapters'),
                target_date=context["params"].get("target_date")
            ),
            'interesting_chapters'
        ),
    )

    # Step 8: Score chapter relevance using AI (0-5 scale)
    # Evaluates each chapter based on speaker relevance, topic relevance, and public interest
    t8 = PythonOperator(
        task_id='score_chapter_relevance',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.score_chapters_relevance(
                ti.xcom_pull(key='interesting_chapters')
            ),
            'scored_chapters'
        ),
    )

    # Step 8b: VAD chapter-start adjustment (between scoring and DB save)
    # For every scored chapter, run VAD ONCE over the chapter's own audio span and
    # trim the silence on BOTH edges: raise its start_time to the first sustained
    # speech and lower its end_time to just after the last sustained speech, so
    # persisted chapters no longer open in the silent "dead start" nor end in
    # trailing applause/silence. Best-effort: any VAD failure / no-detection leaves
    # that edge unchanged; never blocks the DAG. Re-pushes the corrected
    # scored_chapters under the same XCom key so t9_db saves the VAD-trimmed spans.
    def _trim_chapter_silence(ti, **context):
        scored = ti.xcom_pull(key='scored_chapters')
        if not scored:
            logging.warning("No scored_chapters available — VAD passthrough, nothing to trim.")
            return scored
        if not VAD_ENABLED:
            logging.info("VAD disabled (VAD_ENABLED=False) — passthrough, chapters unchanged.")
            return scored
        return trim_chapter_silence_with_vad(
            scored,
            target_date=context["params"].get("target_date"),
        )

    t_trim = PythonOperator(
        task_id='trim_chapter_silence',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: _trim_chapter_silence(ti, **context),
            'scored_chapters'  # overwrite same key with VAD-trimmed spans
        ),
        # Wait for BOTH scoring (t8) and the video download (t3c2) to reach a
        # terminal state. all_done (not all_success) so a failed/slow download
        # never blocks persisting chapters — VAD is best-effort.
        trigger_rule='all_done',
    )

    # Step 9: Save scored chapters to database
    # Stores YouTube videos and their chapters with relevance scores in PostgreSQL
    t9_db = PostgreSQLOperator(
        task_id='save_chapters_to_db',
        operation='save_youtube_chapters',
        xcom_keys={
            'scored_chapters': 'scored_chapters',
            'session_date': 'session_date'  # Contains session_number and target_date
        },
        output_xcom_key='db_save_results'
    )

    # End task for when no plenary sessions found
    t_end = PythonOperator(
        task_id='no_plenary_sessions',
        python_callable=lambda: logging.info("No plenary sessions found. DAG execution stopped."),
    )

    # Task dependencies

    # Start: Branch based on test mode
    t0_branch >> [t0_test, t1]

    # Test mode path: create test video data -> get video details and descriptions in parallel
    t0_test >> [t3a, t3b]

    # Production mode path: fetch from channel
    t1 >> t2 >> t2b >> t2a

    # Branch: no plenary sessions found
    t2a >> t_end

    # Branch: plenary sessions found - process in parallel
    # get_video_details and get_video_descriptions run in parallel
    t2a >> [t3a, t3b]

    # After getting video details, download video and try downloading subtitles in parallel
    t3a >> [t3c, t3c2]

    # Branch based on subtitle availability
    t3c >> t3c_branch

    # If subtitles available: skip to split_srt_by_silence (t5e)
    # Still need to process the subtitles through chunking pipeline!
    t3c_branch >> t5e

    # If no subtitles: extract audio, transcribe, merge SRT
    t3c_branch >> t3d >> t3e >> t3f

    # After transcription path, also go to split_srt_by_silence
    t3f >> t5e

    # After getting descriptions, parse links from description
    t3b >> t4

    # After parsing links, scrape press release and download agenda in parallel
    t4 >> [t5a, t5b]

    # After downloading agenda, extract session date info and then agenda section
    # These run sequentially since agenda_section depends on session_date
    t5b >> t5c >> t5d

    # After splitting SRT by silence (TASK 1), summarize each chunk (TASK 2).
    # Both subtitle and transcription paths converge at t5e. Summarization is
    # parallelized via dynamic task mapping (#9):
    #   t5e -> flatten -> [summarize_one_chunk mapped] -> aggregate (t5f)
    t5e >> t5f_flatten >> t5f_map >> t5f

    # After chunk summaries and SRT chunks are ready, identify interesting chapters
    # We need both: t5f (chunk summaries) and t5e (SRT chunks)
    # t5e must complete before t5f, so we only need to wait for t5f
    t5f >> t6

    # After identifying chapters in all chunks, merge them into final list
    t6 >> t7

    # After merging chapters, score their relevance with AI
    t7 >> t8

    # After scoring, run VAD chapter silence-trim (both edges) before persisting.
    # t_trim ALSO waits on t3c2 (download_video_from_youtube): VAD reads the
    # downloaded mp4 from disk via _find_source_video, so it must NOT start while
    # the (multi-hour) video is still being written — otherwise ffmpeg hits
    # "moov atom not found" on the half-written file. When subtitles are available
    # the chunk/score pipeline races ahead of the parallel video download, so this
    # join is required. trigger_rule='all_done' keeps VAD best-effort: a download
    # failure must never block persisting the scored chapters.
    [t8, t3c2] >> t_trim

    # After trimming the silence, save the chapters to the database
    # Note: session_number and session_date come from t5c and t5d tasks
    # We need both the VAD-trimmed scoring (t_trim) and session info (t5c)
    [t_trim, t5c] >> t9_db
