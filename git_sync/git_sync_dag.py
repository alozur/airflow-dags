"""
Git Repository Synchronization DAG.

This DAG synchronizes configured GitHub repositories by pulling the latest changes
from specified branches. It can run on a schedule or be triggered manually with
custom repository and branch parameters.

Features:
- Daily scheduled sync of all configured repositories
- Manual trigger with custom repository and branch selection
- Environment-based branch selection (dev for development, main for production)
- Multiple repository support via environment variables
"""

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from git_sync.modules.git_operations import (
    get_repository_configs_from_env,
    sync_multiple_repositories,
    sync_repository
)
from utils.airflow_helpers import xcom_task
from utils.env_loader import load_env_if_local

# Load environment variables
load_env_if_local()

# Check environment to determine default branch
ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
DEFAULT_BRANCH = 'dev' if ENVIRONMENT == 'development' else 'main'


def sync_all_repos_task(ti, **context):
    """
    Sync all repositories configured in environment variables.

    Uses GIT_SYNC_REPOS environment variable to get the list of repositories to sync.
    """
    # Get repository configurations from environment
    repo_configs = get_repository_configs_from_env(environment=ENVIRONMENT)

    # Sync all repositories
    results = sync_multiple_repositories(repo_configs, base_path="/opt/airflow/dags")

    # Push results to XCom
    ti.xcom_push(key='sync_results', value=results)

    # Log summary
    successful = sum(1 for r in results if r['status'] == 'success')
    failed = sum(1 for r in results if r['status'] == 'failed')

    print(f"Sync Summary: {successful} successful, {failed} failed out of {len(results)} total")

    for result in results:
        if result['status'] == 'success':
            print(f"✓ {result['message']} - Commit: {result.get('commit', 'N/A')[:8]}")
        else:
            print(f"✗ {result['message']} - Error: {result.get('error', 'Unknown error')}")

    return results


def sync_single_repo_task(ti, **context):
    """
    Sync a single repository with parameters from manual trigger.

    Expects 'repository' and 'branch' parameters from the DAG run configuration.
    """
    params = context['params']
    repository = params.get('repository')
    branch = params.get('branch', DEFAULT_BRANCH)

    if not repository:
        raise ValueError("Repository parameter is required for manual sync")

    # Get GitHub credentials from environment
    github_user = os.getenv('GITHUB_USER')
    github_token = os.getenv('GITHUB_TOKEN')

    if not github_user or not github_token:
        raise ValueError("GITHUB_USER and GITHUB_TOKEN environment variables must be set")

    # Build repository configuration
    repo_config = {
        'user': github_user,
        'token': github_token,
        'repo': repository,
        'branch': branch
    }

    # Sync the repository
    result = sync_repository(repo_config, base_path="/opt/airflow/dags")

    # Push result to XCom
    ti.xcom_push(key='sync_result', value=result)

    # Log result
    if result['status'] == 'success':
        print(f"✓ {result['message']} - Commit: {result.get('commit', 'N/A')[:8]}")
    else:
        print(f"✗ {result['message']} - Error: {result.get('error', 'Unknown error')}")

    return result


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Main DAG for scheduled sync of all repositories
with DAG(
    'git_sync_all_repos',
    default_args=default_args,
    description='Synchronize all configured Git repositories from GitHub',
    schedule_interval='0 2 * * *',  # Run daily at 2:00 AM
    start_date=datetime(2025, 10, 19),
    catchup=False,
    tags=['git-sync', 'infrastructure'],
    params={
        'environment': ENVIRONMENT
    }
) as dag_all:

    sync_all = PythonOperator(
        task_id='sync_all_repositories',
        python_callable=sync_all_repos_task,
        provide_context=True,
    )

    sync_all


# Manual trigger DAG for syncing a single repository
with DAG(
    'git_sync_manual',
    default_args=default_args,
    description='Manually sync a specific Git repository with custom branch',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 10, 19),
    catchup=False,
    tags=['git-sync', 'infrastructure', 'manual'],
    params={
        'repository': 'alozur/airflow-dags',  # Default repository
        'branch': DEFAULT_BRANCH  # Default branch based on environment
    }
) as dag_manual:

    sync_single = PythonOperator(
        task_id='sync_single_repository',
        python_callable=sync_single_repo_task,
        provide_context=True,
    )

    sync_single
