# dags/congreso_youtube/congreso_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from congreso_youtube import congreso_utils as cu
from bs4 import BeautifulSoup
from utils.airflow_helpers import xcom_task, ensure_project_data_directory  # 👈 import helpers
from congreso_youtube.postgres_operators import PostgreSQLOperator

# Load environment variables
from utils.env_loader import load_env_if_local
load_env_if_local()


yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

def _enrich_and_limit_if_testing(organized_groups, is_testing):
    """Helper function to enrich metadata and limit groups if in testing mode."""
    enriched_groups = cu.enrich_with_metadata(organized_groups)

    if is_testing:
        enriched_groups = cu.limit_enriched_groups_for_testing(enriched_groups, max_topics=2)

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
  'congreso_plenary_checker',
  default_args=default_args,
  description='Check Congreso plenary sessions',
  schedule_interval='@daily',
  start_date=datetime(2025, 8, 28),
  catchup=False,
  params={   # Default to yesterday
      "target_date": yesterday_str,
      "isTesting": False  # When True, limits to 2 main topics only
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

  t1 = PythonOperator(
      task_id='construct_url',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda: cu.construct_url(
              target_date=context["params"].get("target_date")
          ),
          'constructed_url'
      ),
  )

  t2 = PythonOperator(
      task_id='get_soup',
      python_callable=lambda ti: xcom_task(
          ti, lambda url: str(cu.get_soup(url)), 'soup_html', input_key='constructed_url'
      ),
  )

  t3 = BranchPythonOperator(
      task_id='check_plenary',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: "get_session_number" if cu.has_plenary_session(BeautifulSoup(html, "html.parser")) else "no_plenary",
          'has_plenary',
          input_key='soup_html',
          branch=True
      ),
  )

  t4 = PythonOperator(
      task_id='get_session_number',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: cu.get_session_number(BeautifulSoup(html, "html.parser")),
          'session_number',
          input_key='soup_html'
      ),
  )

  t5 = PythonOperator(
      task_id='construct_session_link',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda num: cu.construct_session_link(
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
          lambda link: str(cu.get_soup(link)),
          'session_soup_html',
          input_key='session_link'
      ),
  )

  t7 = PythonOperator(
      task_id='extract_video_data',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda html: cu.extract_video_data(html),
          'video_data',
          input_key='session_soup_html'
      ),
  )

  t8 = PythonOperator(
      task_id='organize_video_groups',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda data: cu.organize_video_groups(data),
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

  t10 = PythonOperator(
      task_id='create_session_folder',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda session_num: cu.create_session_folder(session_num),
          'session_folder_path',
          input_key='session_number'
      ),
  )

  # Downloads ONLY main topic videos (NOT individual interventions)
  # For a session with 5 topics and 20 interventions, only 5 videos are downloaded
  t11 = PythonOperator(
      task_id='download_main_topic_videos',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda: cu.download_main_topic_videos(
              ti.xcom_pull(key='enriched_video_groups'),
              ti.xcom_pull(key='session_folder_path')
          ),
          'download_results'
      ),
  )

  t11_ai = PythonOperator(
      task_id='evaluate_videos_with_ai',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda: cu.evaluate_video_interest_with_ai(
              ti.xcom_pull(key='enriched_video_groups')
          ),
          'ai_evaluation_results'
      ),
  )

  t12 = PythonOperator(
      task_id='generate_youtube_metadata',
      python_callable=lambda ti, **context: xcom_task(
          ti,
          lambda: cu.generate_youtube_metadata_from_enriched_groups(
              ti.xcom_pull(key='enriched_video_groups'),
              ti.xcom_pull(key='session_number'),
              context["params"].get("target_date")
          ),
          'youtube_metadata_results'
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

  t15_db = PostgreSQLOperator(
      task_id='update_download_status_in_db',
      operation='update_downloads',
      output_xcom_key='db_download_updates'
  )

  t16_db = PostgreSQLOperator(
      task_id='save_youtube_metadata_to_db',
      operation='save_youtube_metadata',
      output_xcom_key='db_metadata_updates'
  )

  # Future task for YouTube upload (placeholder for when you're ready)
  t14 = PythonOperator(
      task_id='upload_to_youtube',
      python_callable=lambda ti: xcom_task(
          ti,
          lambda: print("Ready for YouTube upload with metadata:",
                       ti.xcom_pull(key='youtube_metadata_results')),
          'upload_results'
      ),
  )

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

  # Main data processing pipeline (no downloads yet)
  t4 >> t5 >> t6 >> t7 >> t8 >> t9

  # Database operations pipeline (save all data without downloads)
  # Create session in DB after we have session number and link
  [t4, t5] >> t13_db

  # Save topics to DB after enrichment (no download data needed)
  [t9, t13_db] >> t14_db

  # AI Evaluation - runs in parallel with YouTube metadata generation
  [t9, t14_db] >> t11_ai

  # Save AI evaluations to DB
  t11_ai >> t14_ai_db

  # Generate YouTube metadata directly from enriched groups (without downloads)
  [t9, t14_db] >> t12

  # Save YouTube metadata to DB
  t12 >> t16_db

  # NOW do downloads near the end - create session folder and download
  # Wait for both AI evaluations and YouTube metadata to be saved
  [t14_ai_db, t16_db] >> t10  # Create session folder when we're ready to download
  [t10, t14_ai_db, t16_db] >> t11  # Download videos

  # Update download status after downloads complete
  t11 >> t15_db

  # YouTube upload depends on all operations being complete
  t15_db >> t14