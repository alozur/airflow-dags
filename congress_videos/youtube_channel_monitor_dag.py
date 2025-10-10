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

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

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

    # Step 3: Check stream status (finished vs live)
    t3 = PythonOperator(
        task_id='check_stream_status',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: yt_channel.check_stream_status(
                ti.xcom_pull(key='plenary_videos')
            ),
            'finished_streams'
        ),
    )

    # Step 4: Save to database (for future download)
    # t4_db = PostgreSQLOperator(
    #     task_id='save_youtube_videos_to_db',
    #     operation='save_youtube_source_videos',
    #     xcom_keys={'finished_streams': 'finished_streams'},
    #     output_xcom_key='db_youtube_videos'
    # )

    # Task dependencies
    t1 >> t2 >> t3  # >> t4_db
