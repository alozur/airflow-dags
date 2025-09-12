# Apache Airflow Expert Agent Configuration

## Project Context
This repository contains Apache Airflow DAGs and related utilities. Each folder represents an independent project with its own DAG implementation.

## Expertise Areas
- Apache Airflow DAG development and best practices
- Task orchestration and dependency management
- XCom usage for inter-task communication
- Branching and conditional logic in DAGs
- Airflow operators (PythonOperator, BranchPythonOperator, etc.)
- Error handling and retry mechanisms
- Scheduling and time-based operations

## Project Structure
Each folder in this repository is a separate Airflow project:
- `congreso_youtube/`: DAG for processing congressional session data from YouTube
- `utils/`: Shared utility functions and helpers used across all projects
  - Contains reusable methods that benefit multiple DAGs
  - Example: `airflow_helpers.py` with XCom management utilities
- Additional project folders contain their own independent DAG implementations

## Shared Utilities (utils folder)
The `utils/` folder contains common functionality shared across all DAG projects:
- **airflow_helpers.py**: Contains the `xcom_task` wrapper for simplified XCom operations
- When creating new utility functions that could benefit multiple projects, add them to this folder
- All projects can import from utils to avoid code duplication

## Development Guidelines
1. **DAG Best Practices**:
   - Use descriptive task IDs
   - Implement proper error handling
   - Utilize XCom for data passing between tasks
   - Follow idempotency principles
   - Document DAG purpose and task dependencies

2. **Code Organization**:
   - Keep DAG files modular and maintainable
   - Use utility functions in separate modules
   - Follow Python PEP 8 standards
   - Implement proper logging

3. **Testing Approach**:
   - Test DAG loading without errors
   - Validate task dependencies
   - Check for circular dependencies
   - Test individual task functions separately

## Common Patterns
- Use `xcom_task` utility from `airflow_helpers.py` for simplified XCom management
- Implement branching logic for conditional workflows
- Use `target_date` parameter for dynamic URL construction
- Handle API responses and data processing within tasks

## Key Commands
- Test DAG: `airflow dags test <dag_id> <execution_date>`
- Trigger DAG: `airflow dags trigger <dag_id>`
- List DAGs: `airflow dags list`
- Validate DAGs: `python <dag_file.py>`