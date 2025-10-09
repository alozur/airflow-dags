from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello_weekly():
  print("Hello from my weekly Airflow DAG!")

with DAG(
  dag_id="hello_weekly_dag",          # Unique name for the DAG
  start_date=datetime(2024, 1, 1),    # First possible run date
  schedule_interval="@weekly",        # Run once a week
  catchup=False                       # Don't backfill past runs
) as dag:

  hello_weekly_task = PythonOperator(
      task_id="say_hello_weekly_task",
      python_callable=say_hello_weekly
  )