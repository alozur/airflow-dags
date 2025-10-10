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
    YOUTUBE_CHANNEL_HANDLE,
    TARGET_VIDEO_TITLE
)
from congress_videos.modules import youtube_channel as yt_channel
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
IS_DEVELOPMENT = POSTGRES_SCHEMA == 'development'

# Calculate yesterday's date
yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


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
    schedule_interval='0 22 * * *',  # Run at 10:00 PM daily (after sessions typically end)
    start_date=datetime(2025, 10, 9),
    catchup=False,
    tags=['congress', 'youtube', 'monitor'],
    params={  # Default to yesterday
        "target_date": yesterday_str,
        "max_videos": 10,  # Maximum number of videos to check
        "isTesting": IS_DEVELOPMENT  # True in development, False in production
    }
) as dag:

    # Step 1: Fetch videos from YouTube channel (streams tab)
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
                target_date=context["params"].get("target_date")
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

    # Step 3a: Get video details (duration, timing, etc.)
    # Note: We already filtered for completed streams, no need to check status again
    t3a = PythonOperator(
        task_id='get_video_details',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.get_video_details(
                ti.xcom_pull(key='plenary_videos')
            ),
            'video_details'
        ),
    )

    # Step 3b: Get full video descriptions (runs in parallel with get_video_details)
    t3b = PythonOperator(
        task_id='get_video_descriptions',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.get_video_descriptions(
                ti.xcom_pull(key='plenary_videos')
            ),
            'video_descriptions'
        ),
    )

    # Step 3c: Download video from YouTube (FUTURE - currently commented)
    # t3c = PythonOperator(
    #     task_id='download_video',
    #     python_callable=lambda ti: xcom_task(
    #         ti,
    #         lambda: yt_channel.download_youtube_video(
    #             ti.xcom_pull(key='plenary_videos')
    #         ),
    #         'downloaded_videos'
    #     ),
    # )

    # End task for when no plenary sessions found
    t_end = PythonOperator(
        task_id='no_plenary_sessions',
        python_callable=lambda: logging.info("No plenary sessions found. DAG execution stopped."),
    )

    # Step 4: Save to database (FUTURE - currently commented)
    # t4_db = PostgreSQLOperator(
    #     task_id='save_youtube_videos_to_db',
    #     operation='save_youtube_source_videos',
    #     xcom_keys={
    #         'video_details': 'video_details',
    #         'video_descriptions': 'video_descriptions',
    #         'downloaded_videos': 'downloaded_videos'
    #     },
    #     output_xcom_key='db_youtube_videos'
    # )

    # Task dependencies
    t1 >> t2 >> t2a

    # Branch: no plenary sessions found
    t2a >> t_end

    # Branch: plenary sessions found - process in parallel
    # get_video_details and get_video_descriptions run in parallel
    t2a >> [t3a, t3b]  # >> t3c (when uncommented, add to list for parallel execution)
