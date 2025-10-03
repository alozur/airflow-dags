# dags/congreso_youtube/youtube_upload_dag.py
"""
Daily YouTube Upload DAG

This DAG runs daily to:
1. Query the top 5 videos by AI interest score from the database
2. Generate YouTube metadata (title, description) for those videos
3. Download the selected videos
4. Upload them to YouTube

Runs independently from the congreso_plenary_checker DAG.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from congreso_youtube import congreso_utils as cu
from utils.airflow_helpers import xcom_task, ensure_project_data_directory
from congreso_youtube.postgres_operators import PostgreSQLOperator

# Load environment variables
from utils.env_loader import load_env_if_local
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
    'youtube_daily_upload',
    default_args=default_args,
    description='Daily YouTube upload: Select top 5 videos by AI score, generate metadata, download, and upload',
    schedule_interval='0 10 * * *',  # Run at 10:00 AM daily
    start_date=datetime(2025, 10, 4),
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
            lambda: ensure_project_data_directory('congreso_youtube'),
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
            lambda: cu.generate_youtube_metadata_for_selected_videos(
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
            lambda: cu.download_videos_for_upload(
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

    # Upload videos to YouTube
    t6 = PythonOperator(
        task_id='upload_to_youtube',
        python_callable=lambda ti, **context: xcom_task(
            ti,
            lambda: cu.upload_videos_to_youtube(
                ti.xcom_pull(key='download_results'),
                is_testing=context["params"].get("isTesting", False)
            ),
            'upload_results'
        ),
    )

    # Update YouTube upload status in database
    t7_db = PostgreSQLOperator(
        task_id='update_youtube_status_in_db',
        operation='update_youtube_status',
        output_xcom_key='db_youtube_updates'
    )

    # Task dependencies
    t0 >> t1_db >> t2 >> t3_db >> t4 >> t5_db >> t6 >> t7_db
