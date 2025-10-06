"""
Congressional Video Processing DAG.

This DAG checks for plenary sessions, extracts video data, organizes topics,
enriches metadata, evaluates videos with AI, and saves to database.
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from bs4 import BeautifulSoup

from congress_videos.modules import utils as cv_utils
from congress_videos.modules.postgres_operators import PostgreSQLOperator
from utils.airflow_helpers import ensure_project_data_directory, xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check if running in development environment
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'development')
IS_DEVELOPMENT = POSTGRES_SCHEMA == 'development'


yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def _enrich_and_limit_if_testing(organized_groups, is_testing):
    """Helper function to enrich metadata and limit groups if in testing mode."""
    enriched_groups = cv_utils.enrich_with_metadata(organized_groups)

    if is_testing:
        enriched_groups = cv_utils.limit_enriched_groups_for_testing(enriched_groups, max_topics=2)

    return enriched_groups


default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'email_on_failure': False,
  'email_on_retry': False,
  'retries': 1,
  'retry_delay': timedelta(minutes=5),
}

with DAG(
  'congress_session_processor',
  default_args=default_args,
  description='Process Spanish Congress plenary sessions: extract videos, evaluate with AI, save to database',
  schedule_interval='@daily',
  start_date=datetime(2025, 8, 28),
  catchup=False,
  params={   # Default to yesterday
      "target_date": yesterday_str,
      "isTesting": IS_DEVELOPMENT  # True in development (limits to 2 topics), False in production (all topics)
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

  t1 = PythonOperator(
      task_id='construct_url',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda: cv_utils.construct_url(
              target_date=context["params"].get("target_date")
          ),
          'constructed_url'
      ),
  )

  t2 = PythonOperator(
      task_id='get_soup',
      python_callable=lambda ti: xcom_task(
          ti, lambda url: str(cv_utils.get_soup(url)), 'soup_html', input_key='constructed_url'
      ),
  )

  t3 = BranchPythonOperator(
      task_id='check_plenary',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: "get_session_number" if cv_utils.has_plenary_session(BeautifulSoup(html, "html.parser")) else "no_plenary",
          'has_plenary',
          input_key='soup_html',
          branch=True
      ),
  )

  t4 = PythonOperator(
      task_id='get_session_number',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: cv_utils.get_session_number(BeautifulSoup(html, "html.parser")),
          'session_number',
          input_key='soup_html'
      ),
  )

  t5 = PythonOperator(
      task_id='construct_session_link',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda num: cv_utils.construct_session_link(
              num,
              target_date=context["params"].get("target_date")
          ),
          'session_link',
          input_key='session_number'
      ),
  )

  t6 = PythonOperator(
      task_id='get_soup_from_session_link',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda link: str(cv_utils.get_soup(link)),
          'session_soup_html',
          input_key='session_link'
      ),
  )

  t7 = PythonOperator(
      task_id='extract_video_data',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: cv_utils.extract_video_data(html),
          'video_data',
          input_key='session_soup_html'
      ),
  )

  t8 = PythonOperator(
      task_id='organize_video_groups',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda data: cv_utils.organize_video_groups(data),
          'organized_video_groups',
          input_key='video_data'
      ),
  )

  t9 = PythonOperator(
      task_id='enrich_with_metadata',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda groups: _enrich_and_limit_if_testing(
              groups,
              context["params"].get("isTesting", False)
          ),
          'enriched_video_groups',
          input_key='organized_video_groups'
      ),
  )

  # AI Evaluation task
  t11_ai = PythonOperator(
      task_id='evaluate_videos_with_ai',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda: cv_utils.evaluate_video_interest_with_ai(
              ti.xcom_pull(key='enriched_video_groups')
          ),
          'ai_evaluation_results'
      ),
  )

  # Database operations using custom PostgreSQLOperator
  t13_db = PostgreSQLOperator(
      task_id='create_session_in_db',
      operation='create_session',
      output_xcom_key='db_session_id'
  )

  t14_db = PostgreSQLOperator(
      task_id='save_topics_to_db',
      operation='save_topics',
      output_xcom_key='db_topic_ids'
  )

  t14_ai_db = PostgreSQLOperator(
      task_id='save_ai_evaluations_to_db',
      operation='save_ai_evaluations',
      output_xcom_key='db_ai_evaluation_updates'
  )

  # NOTE: The following tasks have been removed from this DAG:
  # - create_session_folder (t10)
  # - download_main_topic_videos (t11)
  # - generate_youtube_metadata (t12)
  # - update_download_status_in_db (t15_db)
  # - save_youtube_metadata_to_db (t16_db)
  # - upload_to_youtube (t14)
  #
  # These will be handled by a separate daily DAG that:
  # 1. Queries the top 5 videos by ai_interest_score
  # 2. Generates YouTube metadata for those videos
  # 3. Downloads the videos
  # 4. Uploads them to YouTube

  t13 = PythonOperator(
      task_id='no_plenary',
      python_callable=lambda: print("No plenary session today. DAG execution stopped."),
  )

  # Sequential flow until we get session_number
  t0 >> t1 >> t2 >> t3

  # Branch: no plenary session
  t3 >> t13

  # Branch: plenary session found - parallel execution opportunities
  t3 >> t4

  # Main data processing pipeline
  t4 >> t5 >> t6 >> t7 >> t8 >> t9

  # Database operations pipeline
  # Create session in DB after we have session number and link
  [t4, t5] >> t13_db

  # Save topics to DB after enrichment
  [t9, t13_db] >> t14_db

  # AI Evaluation - evaluate all main topics for YouTube upload interest
  [t9, t14_db] >> t11_ai

  # Save AI evaluations to DB - FINAL TASK
  t11_ai >> t14_ai_db