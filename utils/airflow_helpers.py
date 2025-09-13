# dags/repo/utils/airflow_helpers.py
from typing import Callable, Optional
from airflow.models.taskinstance import TaskInstance
import os

def xcom_task(
  ti: TaskInstance,
  func: Callable,
  output_key: str,
  input_key: Optional[str] = None,
  branch: bool = False
):
  """
  Generic wrapper to run a callable, push result to XCom, and optionally branch.

  :param ti: Airflow TaskInstance (injected automatically by PythonOperator)
  :param func: Callable to execute
  :param output_key: XCom key to push the result under
  :param input_key: Optional XCom key to pull input from
  :param branch: If True, return the result as a branch task_id
  """
  # Pull input from XCom if needed
  if input_key:
      value = ti.xcom_pull(key=input_key)
      result = func(value)
  else:
      result = func()

  # Push result to XCom
  ti.xcom_push(key=output_key, value=result)

  # If this is a branch task, return the branch decision
  if branch:
      return result


def ensure_project_data_directory(project_name: str, base_data_path: str = "/opt/airflow/data") -> str:
  """
  Check if a project-specific data directory exists and create it if not.
  
  :param project_name: Name of the project (e.g., 'congreso_youtube')
  :param base_data_path: Base path for data directories (default: '/opt/airflow/data')
  :return: Full path to the project data directory
  """
  project_data_path = os.path.join(base_data_path, project_name)
  
  if not os.path.exists(project_data_path):
      os.makedirs(project_data_path, exist_ok=True)
      print(f"Created project data directory: {project_data_path}")
  else:
      print(f"Project data directory already exists: {project_data_path}")
  
  return project_data_path