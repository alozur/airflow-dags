"""
Git Sync DAG - On-demand repository synchronization

This DAG replaces the continuous git-sync container with an on-demand
pull mechanism. Trigger this DAG manually when you want to pull the
latest changes from the remote repository.

Usage:
  - Trigger manually from Airflow UI when you want to update DAGs
  - Or trigger via CLI: airflow dags trigger git_sync_dag
"""

import os
from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# Git configuration from environment variables
GITHUB_USER = os.getenv("GITHUB_USER", "")
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN", "")
GITHUB_REPO = os.getenv("GITHUB_REPO", "")
GIT_BRANCH = os.getenv("GIT_SYNC_BRANCH", "dev")

# DAGs are mounted at /opt/airflow/dags/repo
DAGS_REPO_PATH = "/opt/airflow/dags/repo"

with DAG(
    dag_id="git_sync_dag",
    description="Pull latest DAG changes from GitHub repository",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["utility", "git", "sync"],
) as dag:

    # Configure git with credentials for this pull
    configure_git = BashOperator(
        task_id="configure_git",
        bash_command=f"""
            cd {DAGS_REPO_PATH}
            git config --local credential.helper store
            git config --local user.email "airflow@localhost"
            git config --local user.name "Airflow Git Sync"
        """,
    )

    # Fetch and pull latest changes
    git_pull = BashOperator(
        task_id="git_pull",
        bash_command=f"""
            cd {DAGS_REPO_PATH}
            echo "Current branch: $(git branch --show-current)"
            echo "Fetching from remote..."
            git fetch origin {GIT_BRANCH}
            echo "Pulling latest changes..."
            git reset --hard origin/{GIT_BRANCH}
            echo "Latest commit:"
            git log -1 --oneline
            echo "Git sync completed successfully!"
        """,
    )

    # Show sync status
    show_status = BashOperator(
        task_id="show_status",
        bash_command=f"""
            cd {DAGS_REPO_PATH}
            echo "=== Git Sync Status ==="
            echo "Branch: $(git branch --show-current)"
            echo "Latest commit: $(git log -1 --oneline)"
            echo "Repository: {GITHUB_REPO}"
            echo "========================"
        """,
    )

    trigger_migrations = TriggerDagRunOperator(
        task_id="trigger_migrations",
        trigger_dag_id="run_migrations",
        wait_for_completion=False,
    )

    configure_git >> git_pull >> show_status >> trigger_migrations
