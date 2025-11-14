"""
Congress YouTube Chapter Uploader DAG v2

This DAG uploads individual chapters from congressional videos to YouTube:

1. Ensure data directory exists
2. Query top 5 uploadable chapters from uploadable_chapters view
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
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
IS_DEVELOPMENT = POSTGRES_SCHEMA == 'development'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'congress_youtube_chapter_uploader_v2',
    default_args=default_args,
    description='Upload top congressional video chapters to YouTube based on relevance score',
    schedule_interval='0 12 * * *',  # Run at 12:00 PM daily
    start_date=datetime(2025, 11, 14),
    catchup=False,
    tags=['congress', 'youtube', 'chapters', 'v2'],
    params={
        "max_chapters": 5,  # Maximum number of chapters to upload per day
        "min_relevance_score": 2,  # Minimum relevance score (0-5)
        "isTesting": IS_DEVELOPMENT  # True in development, False in production
    }
) as dag:

    # Step 0: Ensure data directory exists
    t0 = PythonOperator(
        task_id='ensure_data_directory',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory('congress_videos'),
            'data_directory_path'
        ),
    )

    # Step 1: Get top uploadable chapters from database view
    # Selects from uploadable_chapters view, ordered by:
    # 1. relevance_score DESC (highest first)
    # 2. created_at DESC (most recent first)
    t1_db = PostgreSQLOperator(
        task_id='get_uploadable_chapters',
        operation='get_uploadable_chapters',
        output_xcom_key='uploadable_chapters'
    )

    # Task dependencies
    t0 >> t1_db
