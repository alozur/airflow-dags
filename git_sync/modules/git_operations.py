"""Git operations module for repository synchronization."""

import os
import subprocess
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)


def construct_git_url(github_user: str, github_token: str, repo: str) -> str:
    """
    Construct authenticated GitHub URL.

    :param github_user: GitHub username
    :param github_token: GitHub personal access token
    :param repo: Repository name (e.g., 'username/repository')
    :return: Authenticated HTTPS Git URL
    """
    return f"https://{github_user}:{github_token}@github.com/{repo}.git"


def sync_repository(repo_config: Dict[str, str], base_path: str = "/opt/airflow/dags") -> Dict[str, str]:
    """
    Synchronize a single Git repository by pulling latest changes.

    :param repo_config: Dictionary with 'user', 'token', 'repo', and 'branch' keys
    :param base_path: Base directory path for git repositories
    :return: Dictionary with sync status and details
    """
    user = repo_config['user']
    token = repo_config['token']
    repo = repo_config['repo']
    branch = repo_config['branch']

    # Extract repo name from 'user/repo' format
    repo_name = repo.split('/')[-1]
    repo_path = os.path.join(base_path, repo_name)

    try:
        # Construct authenticated URL
        git_url = construct_git_url(user, token, repo)

        # Check if repository already exists
        if os.path.exists(repo_path):
            logger.info(f"Repository {repo_name} exists. Pulling latest changes...")

            # Change to repo directory and pull
            result = subprocess.run(
                ['git', '-C', repo_path, 'fetch', 'origin', branch],
                capture_output=True,
                text=True,
                check=True
            )

            # Reset to origin/branch to ensure clean sync
            result = subprocess.run(
                ['git', '-C', repo_path, 'reset', '--hard', f'origin/{branch}'],
                capture_output=True,
                text=True,
                check=True
            )

            # Get latest commit hash
            commit_result = subprocess.run(
                ['git', '-C', repo_path, 'rev-parse', 'HEAD'],
                capture_output=True,
                text=True,
                check=True
            )
            commit_hash = commit_result.stdout.strip()

            return {
                'status': 'success',
                'action': 'pulled',
                'repo': repo,
                'branch': branch,
                'path': repo_path,
                'commit': commit_hash,
                'message': f'Successfully synced {repo} on branch {branch}'
            }
        else:
            logger.info(f"Repository {repo_name} does not exist. Cloning...")

            # Create base path if it doesn't exist
            os.makedirs(base_path, exist_ok=True)

            # Clone the repository
            result = subprocess.run(
                ['git', 'clone', '-b', branch, git_url, repo_path],
                capture_output=True,
                text=True,
                check=True
            )

            # Get latest commit hash
            commit_result = subprocess.run(
                ['git', '-C', repo_path, 'rev-parse', 'HEAD'],
                capture_output=True,
                text=True,
                check=True
            )
            commit_hash = commit_result.stdout.strip()

            return {
                'status': 'success',
                'action': 'cloned',
                'repo': repo,
                'branch': branch,
                'path': repo_path,
                'commit': commit_hash,
                'message': f'Successfully cloned {repo} on branch {branch}'
            }

    except subprocess.CalledProcessError as e:
        error_msg = f"Git operation failed for {repo}: {e.stderr}"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'repo': repo,
            'branch': branch,
            'error': error_msg,
            'message': f'Failed to sync {repo} on branch {branch}'
        }
    except Exception as e:
        error_msg = f"Unexpected error syncing {repo}: {str(e)}"
        logger.error(error_msg)
        return {
            'status': 'failed',
            'repo': repo,
            'branch': branch,
            'error': error_msg,
            'message': f'Failed to sync {repo} on branch {branch}'
        }


def sync_multiple_repositories(repositories: List[Dict[str, str]], base_path: str = "/opt/airflow/dags") -> List[Dict[str, str]]:
    """
    Synchronize multiple Git repositories.

    :param repositories: List of repository configuration dictionaries
    :param base_path: Base directory path for git repositories
    :return: List of sync results for each repository
    """
    results = []

    for repo_config in repositories:
        result = sync_repository(repo_config, base_path)
        results.append(result)

    return results


def get_repository_configs_from_env(environment: str = 'development') -> List[Dict[str, str]]:
    """
    Build repository configurations from environment variables.

    Expected environment variables:
    - GITHUB_USER: GitHub username
    - GITHUB_TOKEN: GitHub personal access token
    - GIT_SYNC_REPOS: Comma-separated list of repositories (e.g., "user/repo1,user/repo2")
    - GIT_SYNC_BRANCH: Branch to sync (defaults based on environment parameter)

    :param environment: 'development' or 'production' to determine default branch
    :return: List of repository configurations
    """
    github_user = os.getenv('GITHUB_USER')
    github_token = os.getenv('GITHUB_TOKEN')
    repos_str = os.getenv('GIT_SYNC_REPOS', '')

    # Determine branch based on environment variable or parameter
    if os.getenv('GIT_SYNC_BRANCH'):
        branch = os.getenv('GIT_SYNC_BRANCH')
    else:
        branch = 'dev' if environment == 'development' else 'main'

    if not github_user or not github_token:
        raise ValueError("GITHUB_USER and GITHUB_TOKEN environment variables must be set")

    if not repos_str:
        raise ValueError("GIT_SYNC_REPOS environment variable must be set with comma-separated repository list")

    # Parse repositories
    repos = [repo.strip() for repo in repos_str.split(',') if repo.strip()]

    # Build configurations
    configs = []
    for repo in repos:
        configs.append({
            'user': github_user,
            'token': github_token,
            'repo': repo,
            'branch': branch
        })

    return configs
