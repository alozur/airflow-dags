"""
Congress YouTube Uploader DAG

This DAG runs daily to:
1. Query the top 5 videos by AI interest score from the database
2. Generate YouTube metadata (title, description) for those videos
3. Download the selected videos
4. Upload them to YouTube

Runs independently from the congress_session_processor DAG.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from congress_videos.modules import utils as cv_utils
from congress_videos.modules import youtube_upload as yt_upload
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'congress_youtube_uploader',
    default_args=default_args,
    description='Daily YouTube upload: Select top Congress videos by AI score, generate metadata, download, and upload',
    schedule_interval='0 10 * * *',  # Run at 10:00 AM daily
    start_date=datetime(2025, 10, 3),
    catchup=False,
    params={
        "max_videos": 5,  # Maximum number of videos to upload per day
        "min_interest_score": 6,  # Minimum AI interest score to consider
        "isTesting": False  # When True, skips actual YouTube upload
    }
) as dag:

    t0 = PythonOperator(
        task_id='ensure_data_directory',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: ensure_project_data_directory('congress_videos'),
            'data_directory_path'
        ),
    )

    # Query top videos from database based on AI interest score
    t1_db = PostgreSQLOperator(
        task_id='get_top_videos_by_score',
        operation='get_top_videos_for_upload',
        output_xcom_key='top_videos'
    )

    # Generate YouTube metadata for selected videos
    t2 = PythonOperator(
        task_id='generate_youtube_metadata',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: cv_utils.generate_youtube_metadata_for_selected_videos(
                ti.xcom_pull(key='top_videos')
            ),
            'youtube_metadata_results'
        ),
    )

    # Save YouTube metadata to database
    t3_db = PostgreSQLOperator(
        task_id='save_youtube_metadata_to_db',
        operation='save_youtube_metadata',
        output_xcom_key='db_metadata_updates'
    )

    # Create session folders and download videos
    t4 = PythonOperator(
        task_id='download_selected_videos',
        python_callable=lambda ti: xcom_task(
            ti,
            lambda: cv_utils.download_videos_for_upload(
                ti.xcom_pull(key='top_videos'),
                ti.xcom_pull(key='data_directory_path')
            ),
            'download_results'
        ),
    )

    # Update download status in database
    t5_db = PostgreSQLOperator(
        task_id='update_download_status_in_db',
        operation='update_downloads',
        output_xcom_key='db_download_updates'
    )

    # Prepare upload configuration for generic YouTube uploader DAG
    t6_prep = PythonOperator(
        task_id='prepare_upload_config',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: yt_upload.prepare_youtube_upload_config(
                ti.xcom_pull(key='download_results'),
                ti.xcom_pull(key='youtube_metadata_results'),
                is_testing=context["params"].get("isTesting", False)
            ),
            'upload_config'
        ),
    )

    # Trigger the generic YouTube uploader DAG
    t6_trigger = TriggerDagRunOperator(
        task_id='trigger_youtube_upload',
        trigger_dag_id='generic_youtube_uploader',
        conf="{{ ti.xcom_pull(task_ids='prepare_upload_config', key='upload_config') }}",
        wait_for_completion=True,
        poke_interval=30,
        execution_date="{{ ds }}",
        reset_dag_run=True,
    )

    # Update YouTube upload status in database
    t7_db = PostgreSQLOperator(
        task_id='update_youtube_status_in_db',
        operation='update_youtube_status',
        output_xcom_key='db_youtube_updates'
    )

    # Task dependencies
    t0 >> t1_db >> t2 >> t3_db >> t4 >> t5_db >> t6_prep >> t6_trigger >> t7_db
