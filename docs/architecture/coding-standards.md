# Coding Standards

**⚠️ MANDATORY FOR AI AGENTS ⚠️**

These standards directly control AI developer behavior and are critical for preventing bad code generation.

## Core Standards

- **Languages & Runtimes:** Python 3.11+, Apache Airflow 2.7.x
- **Database:** PostgreSQL 16
- **Style & Linting:** Ruff (modern, fast Python linter and formatter)
- **Formatting:** PEP 8 compliance with 88-character line length (Black compatible)
- **Execution Environment:** Conda environment named `airflow`
- **Deployment:** Docker Compose with LocalExecutor

## Naming Conventions

### DAG Files
| Element | Convention | Example |
|---------|------------|---------|
| **DAG File** | `<project>_dag.py` | `congreso_youtube_dag.py`, `etl_pipeline_dag.py` |
| **DAG ID** | `<project>_<description>` | `congreso_youtube`, `data_pipeline_daily` |
| **Task IDs** | `<action>_<subject>` | `extract_metadata`, `transform_data`, `load_postgres` |
| **Branch Task IDs** | `branch_<condition>` | `branch_data_quality`, `branch_weekday_check` |

### Python Code
| Element | Convention | Example |
|---------|------------|---------|
| **Functions** | `snake_case` | `process_video_data()`, `get_session_info()` |
| **Classes** | `PascalCase` | `VideoProcessor`, `CongressionalDatabase` |
| **Constants** | `UPPER_SNAKE_CASE` | `MAX_RETRIES`, `DEFAULT_TIMEOUT` |
| **Private Methods** | `_snake_case` | `_validate_input()`, `_connect_db()` |
| **Utility Modules** | `snake_case.py` | `airflow_helpers.py`, `postgres_helpers.py` |

### Database Objects
| Element | Convention | Example |
|---------|------------|---------|
| **Schemas** | `<env>_<project>` | `dev_congreso`, `prod_congreso` |
| **Tables** | `snake_case` | `video_metadata`, `session_topics` |
| **Columns** | `snake_case` | `video_id`, `created_at`, `is_processed` |

## Critical Rules

### Environment & Execution
- **Conda Environment MANDATORY:** All Python/Airflow commands must use `conda run -n airflow`
- **Schema Separation Required:** Development uses `dev_*` schemas, production uses `prod_*` schemas
- **No Hardcoded Credentials:** Always use Airflow Connections UI or environment variables
- **Test Before Deploy:** DAG must pass syntax validation before deployment

### DAG Development
- **Idempotency Required:** All tasks must be safely re-runnable
- **Atomic Tasks:** Each task should do one thing well
- **Proper Dependencies:** Use `>>` or `set_upstream()`/`set_downstream()` clearly
- **XCom Usage:** Use `xcom_task` wrapper from `utils/airflow_helpers.py`
- **Error Handling:** Implement retry logic and appropriate timeouts
- **Documentation:** Every DAG must have clear docstring explaining purpose
- **Operator Selection:** Use appropriate operators (PostgresOperator for DB, PythonOperator for logic)

### PostgreSQL Integration
- **Use PostgresOperator:** Never use raw psycopg2 connections in tasks
- **Parameterized Queries:** Use parameter placeholders to prevent SQL injection
- **Connection Management:** Define connections in Airflow UI, reference by `conn_id`
- **Schema Qualification:** Always specify schema in queries: `dev_congreso.video_metadata`
- **Transaction Handling:** Use explicit transactions for multi-statement operations

### Code Quality
- **Linting Required:** Code must pass `ruff check` before commit
- **Import Organization:**
  1. Standard library
  2. Third-party packages (airflow, requests, etc.)
  3. Local modules (utils, project modules)
- **Type Hints Encouraged:** Use for function parameters and returns where it aids clarity
- **Docstrings Required:** Google-style docstrings for all functions and classes
- **Error Messages:** Provide context-rich error messages for debugging

### XCom Best Practices
- **Use Utility Wrapper:** Always use `xcom_task` decorator from `airflow_helpers.py`
- **Size Limits:** Keep XCom data small (<1MB); use external storage for large data
- **Consistent Keys:** Use descriptive, consistent key names across DAG
- **Serializable Data:** Only pass JSON-serializable Python objects

### Retry & Timeout Strategy
- **Default Retries:** 2 retries for most tasks
- **Retry Delay:** 5 minutes default, exponential backoff for API calls
- **Execution Timeout:** Set appropriate timeout for each task (default 1 hour)
- **Database Tasks:** 3 retries with 2-minute delay for connection issues

## Code Organization Patterns

### DAG File Structure
```python
"""
Brief description of DAG purpose.

This DAG processes... and loads data to...
Schedule: @daily
Owner: team_name
"""

# Standard library imports
from datetime import datetime, timedelta

# Third-party imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Local imports
from utils.airflow_helpers import xcom_task
from congreso_youtube.congreso_utils import process_data

# Constants
DEFAULT_ARGS = {...}
DAG_ID = 'project_name'

# Task functions
@xcom_task
def extract_data(**context):
    """Extract data from source."""
    pass

# DAG definition
with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    schedule='@daily',
    catchup=False,
) as dag:

    task1 = PythonOperator(...)
    task2 = PostgresOperator(...)

    task1 >> task2
```

### Utility Module Pattern
```python
"""
Module description.

This module provides utilities for...
"""

from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

def utility_function(param: str) -> Dict[str, Any]:
    """
    Function description.

    Args:
        param: Description of parameter

    Returns:
        Dictionary containing results

    Raises:
        ValueError: When validation fails
    """
    try:
        # Implementation
        pass
    except Exception as e:
        logger.error(f"Error in utility_function: {e}")
        raise
```

## Testing Requirements

### Pre-Commit Validation
```bash
# Syntax validation
conda run -n airflow python <dag_file>.py

# Linting
conda run -n airflow ruff check <file>.py

# Format check
conda run -n airflow ruff format --check <file>.py
```

### DAG Testing
```bash
# List DAGs (verify DAG loads)
conda run -n airflow airflow dags list

# Test entire DAG
conda run -n airflow airflow dags test <dag_id> <execution_date>

# Test individual task
conda run -n airflow airflow tasks test <dag_id> <task_id> <execution_date>
```

### Unit Testing (Recommended)
- Use `pytest` for unit tests
- Mock external dependencies (APIs, databases)
- Test task functions independently
- Validate XCom data structures
- Check error handling paths

## Ruff Configuration

Create `.ruff.toml` in project root:
```toml
line-length = 88
target-version = "py311"

[lint]
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "N",   # pep8-naming
    "UP",  # pyupgrade
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "SIM", # flake8-simplify
]
ignore = []

[lint.per-file-ignores]
"__init__.py" = ["F401"]  # Allow unused imports

[format]
quote-style = "double"
indent-style = "space"
```

## Version Control Best Practices

- **Branch Strategy:** Use `dev` branch for development, `main` for production
- **Commit Messages:** Clear, descriptive messages: `Add: video processing DAG`, `Fix: XCom key error in transform task`
- **Pre-Commit Checks:** Validate syntax and linting before pushing
- **DAG Versioning:** Include version in DAG documentation when making breaking changes

## Common Anti-Patterns to Avoid

❌ **DON'T:**
- Hardcode database credentials in DAG files
- Use large data in XCom (>1MB)
- Create circular task dependencies
- Ignore error handling
- Skip DAG testing before deployment
- Mix development and production schemas
- Use `import *` statements
- Leave print statements for logging

✅ **DO:**
- Use Airflow Connections for credentials
- Store large data in S3/Database and pass references via XCom
- Visualize dependencies with `airflow dags show`
- Implement retry logic with appropriate delays
- Test with `airflow dags test` command
- Use separate schemas: `dev_project` vs `prod_project`
- Import specific functions/classes
- Use `logging` module with appropriate levels

## Logging Standards

```python
import logging

logger = logging.getLogger(__name__)

# Log levels and usage
logger.debug("Detailed diagnostic info")      # Development only
logger.info("General informational messages") # Task progress
logger.warning("Warning messages")            # Recoverable issues
logger.error("Error messages")                # Task failures
logger.critical("Critical issues")            # System failures

# Good logging example
logger.info(f"Processing video_id={video_id}, session={session_number}")
logger.error(f"Failed to fetch URL {url}: {str(e)}", exc_info=True)
```

## Documentation Requirements

### DAG Documentation
Every DAG must include:
- Purpose and business context
- Schedule and time zone
- Owner/team responsible
- Dependencies (external systems, APIs)
- Data sources and destinations
- Known limitations or quirks

### Function Documentation
Use Google-style docstrings:
```python
def process_video_metadata(video_id: str, session_num: int) -> dict:
    """
    Process video metadata from congressional session.

    Args:
        video_id: YouTube video identifier
        session_num: Congressional session number

    Returns:
        Dictionary with processed metadata including title, date, topics

    Raises:
        ValueError: If video_id format is invalid
        requests.RequestException: If API call fails
    """
```

---

**Last Updated:** 2025-10-02
**Airflow Version:** 2.7.x
**Python Version:** 3.11+
**PostgreSQL Version:** 16
