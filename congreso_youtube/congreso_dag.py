# dags/congreso_youtube/congreso_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from congreso_youtube import congreso_utils as cu
from bs4 import BeautifulSoup

def task_construct_url(**context):
    url = cu.construct_url(days_back=1)
    context['ti'].xcom_push(key='constructed_url', value=url)

def task_get_soup(**context):
    url = context['ti'].xcom_pull(key='constructed_url')
    soup = cu.get_soup(url)
    context['ti'].xcom_push(key='soup_html', value=str(soup))

def branch_check_plenary(**context):
    soup_html = context['ti'].xcom_pull(key='soup_html')
    result = cu.has_plenary_session(BeautifulSoup(soup_html, "html.parser"))
    context['ti'].xcom_push(key='has_plenary', value=result)

    if result:
        return "get_session_number"  # task_id to run if plenary exists
    else:
        return "no_plenary"  # task_id to run if no plenary

def task_get_session_number(**context):
    soup_html = context['ti'].xcom_pull(key='soup_html')
    session_number = cu.get_session_number(BeautifulSoup(soup_html, "html.parser"))
    context['ti'].xcom_push(key='session_number', value=session_number)

def task_construct_session_link(**context):
    session_number = context['ti'].xcom_pull(key='session_number')
    if session_number:
        link = cu.construct_session_link(session_number, days_back=1)
        context['ti'].xcom_push(key='session_link', value=link)

def task_no_plenary():
    print("No plenary session today. DAG execution stopped.")

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
) as dag:

  t1 = PythonOperator(
      task_id='construct_url',
      python_callable=task_construct_url,
  )

  t2 = PythonOperator(
      task_id='get_soup',
      python_callable=task_get_soup,
  )

  t3 = BranchPythonOperator(
      task_id='check_plenary',
      python_callable=branch_check_plenary,
  )

  t4 = PythonOperator(
      task_id='get_session_number',
      python_callable=task_get_session_number,
  )

  t5 = PythonOperator(
      task_id='construct_session_link',
      python_callable=task_construct_session_link,
  )

  t6 = PythonOperator(
      task_id='no_plenary',
      python_callable=task_no_plenary,
  )

  # DAG flow with branching
  t1 >> t2 >> t3
  t3 >> t4 >> t5
  t3 >> t6