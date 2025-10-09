# utils/env_loader.py
"""
Centralized environment variable loading for all projects.
"""
import os

def load_env_if_local():
    """Load .env file only if not running in Docker environment"""
    if not os.getenv('AIRFLOW__CORE__DAGS_FOLDER'):  # Common Docker env var
        try:
            from dotenv import load_dotenv
            load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
        except ImportError:
            pass  # dotenv not available, use environment variables