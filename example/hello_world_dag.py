from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
  print("Hello from my first Airflow DAG!")

with DAG(
  dag_id="hello_world_dag",          # Name of the DAG
  start_date=datetime(2024, 1, 1),   # Start date
  schedule_interval="@daily",        # Run daily
  catchup=False                      # Don't run past dates
) as dag:

  hello_task = PythonOperator(
      task_id="say_hello_task",
      python_callable=say_hello
  )