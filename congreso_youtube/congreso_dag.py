# dags/congreso_youtube/congreso_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from congreso_youtube import congreso_utils as cu
from bs4 import BeautifulSoup
from utils.airflow_helpers import xcom_task  # 👈 import helper


yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

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
      "target_date": yesterday_str
  }
) as dag:

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
      task_id='no_plenary',
      python_callable=lambda: print("No plenary session today. DAG execution stopped."),
  )

  t1 >> t2 >> t3
  t3 >> t4 >> t5 >> t6 >> t7
  t3 >> t8