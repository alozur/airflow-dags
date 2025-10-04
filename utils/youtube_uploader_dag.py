"""
Generic YouTube Uploader DAG - Reusable across all projects.

This DAG is triggered by other DAGs to upload videos to YouTube.
It accepts configuration via dag_run.conf parameter.

USAGE from another DAG:
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

    trigger_upload = TriggerDagRunOperator(
        task_id='trigger_youtube_upload',
        trigger_dag_id='generic_youtube_uploader',
        conf={
            'token_file': '/path/to/youtube_token.pickle',
            'videos': [
                {
                    'video_file': '/path/to/video.mp4',
                    'title': 'Video Title',
                    'description': 'Description',
                    'category_id': '25',
                    'privacy_status': 'private',
                    'tags': ['tag1', 'tag2'],
                    'made_for_kids': False,
                }
            ]
        },
        wait_for_completion=True,  # Wait for upload to complete
    )
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from utils.youtube_helpers import (
    validate_upload_config,
    upload_videos_from_config,
)

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# DAG definition
with DAG(
    dag_id='generic_youtube_uploader',
    default_args=default_args,
    description='Generic YouTube uploader - triggered by other DAGs with video upload config',
    schedule_interval=None,  # Triggered only, never scheduled
    start_date=days_ago(1),
    catchup=False,
    tags=['youtube', 'upload', 'generic', 'utils'],
    max_active_runs=3,  # Allow multiple uploads in parallel
) as dag:

    # Task 1: Validate configuration
    validate_config = PythonOperator(
        task_id='validate_config',
        python_callable=lambda **context: validate_upload_config(
            context['dag_run'].conf or {}
        ),
    )

    # Task 2: Upload videos
    upload_videos = PythonOperator(
        task_id='upload_videos',
        python_callable=lambda ti: upload_videos_from_config(
            ti.xcom_pull(task_ids='validate_config')
        ),
    )

    # Task dependencies
    validate_config >> upload_videos


# =============================================================================
# USAGE DOCUMENTATION
# =============================================================================
#
# This DAG is designed to be triggered by other DAGs. It does NOT run on a schedule.
#
# Example 1: Trigger from another DAG
# -----------------------------------------------------------------------------
# from airflow.operators.trigger_dagrun import TriggerDagRunOperator
#
# trigger_upload = TriggerDagRunOperator(
#     task_id='trigger_youtube_upload',
#     trigger_dag_id='generic_youtube_uploader',
#     conf={
#         'token_file': '/opt/airflow/dags/repo/congress_videos/youtube_token.pickle',
#         'videos': [
#             {
#                 'video_file': '/opt/airflow/data/congress_videos/video.mp4',
#                 'title': 'Congressional Session - Jan 15, 2025',
#                 'description': 'Full session recording...',
#                 'category_id': '25',  # News & Politics
#                 'privacy_status': 'public',
#                 'tags': ['congress', 'politics', 'session'],
#                 'made_for_kids': False,
#             }
#         ]
#     },
#     wait_for_completion=True,
# )
#
# Example 2: Multiple videos in one trigger
# -----------------------------------------------------------------------------
# conf={
#     'token_file': '/opt/airflow/dags/repo/my_project/youtube_token.pickle',
#     'videos': [
#         {...video1...},
#         {...video2...},
#         {...video3...},
#     ]
# }
#
# Configuration Fields:
# -----------------------------------------------------------------------------
# - token_file: (required) Path to youtube_token.pickle file
# - videos: (required) List of video objects, each containing:
#   - video_file: (required) Path to video file
#   - title: (required) Video title
#   - description: (optional) Video description
#   - category_id: (optional) YouTube category (default: 22)
#   - privacy_status: (optional) 'private', 'unlisted', 'public' (default: 'private')
#   - tags: (optional) List of tags
#   - made_for_kids: (optional) COPPA compliance (default: False)
#
# =============================================================================
