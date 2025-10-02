# Tech Stack

## Overview

This document outlines the complete technology stack used in the Airflow DAGs project, including versions, deployment strategy, and rationale for each technology choice.

## Technology Stack Table

| Category | Technology | Version | Purpose | Rationale |
|----------|------------|---------|---------|-----------|
| **Workflow Orchestration** | Apache Airflow | 2.7.x (Docker: 2.10.2) | DAG scheduling and execution | Industry standard for workflow orchestration |
| **Database** | PostgreSQL | 16 | Data warehouse and Airflow metadata | Robust, open-source, excellent JSON support |
| **Language** | Python | 3.11+ | DAG development and task logic | Airflow's native language, rich ecosystem |
| **Environment Manager** | Conda | Latest | Python environment isolation | Reliable environment management |
| **Linter & Formatter** | Ruff | Latest | Code quality and formatting | Fast, modern Python linter (replaces flake8, black) |
| **Container Platform** | Docker | Latest | Airflow deployment | Consistent deployment environment |
| **Container Orchestration** | Docker Compose | Latest | Multi-service orchestration | Simple, effective for small-medium deployments |
| **Version Control** | Git | Latest | Source code management | Industry standard |
| **Repository Hosting** | GitHub | N/A | Code hosting and collaboration | Free, integrated CI/CD |
| **DAG Sync** | git-sync | v3.6.5 | Automated DAG deployment | Kubernetes git-sync for Docker |
| **HTTP Client** | requests | >=2.31.0 | API calls and web scraping | Standard Python HTTP library |
| **HTML Parser** | BeautifulSoup4 | >=4.12.2 | HTML parsing and scraping | Easy-to-use web scraping |
| **XML/HTML Parser** | lxml | >=4.9.3 | Fast HTML parsing | Performance boost for BeautifulSoup |
| **HTTP Library** | urllib3 | >=2.0.4 | Low-level HTTP operations | Handles SSL and connection pooling |
| **AI Integration** | OpenAI API | Latest | AI-powered metadata enrichment | GPT models for text processing |

## Core Infrastructure

### Apache Airflow
- **Version:** 2.7.x (development), 2.10.2 (Docker image - backward compatible)
- **Executor:** LocalExecutor
- **Database Backend:** PostgreSQL (shared instance)
- **Deployment:** Docker Compose
- **Web UI Port:** 8081 (mapped from container 8080)
- **Time Zone:** Europe/Madrid

**Key Configuration:**
```yaml
AIRFLOW__CORE__EXECUTOR: LocalExecutor
AIRFLOW__CORE__PARALLELISM: 4
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: 2
AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1
AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL: 30
AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL: 30
```

### PostgreSQL
- **Version:** 16
- **Purpose:**
  - Airflow metadata database
  - Application data storage
  - Project-specific data warehousing
- **Schema Strategy:**
  - `airflow_db` - Airflow metadata
  - `dev_<project>` - Development schemas
  - `prod_<project>` - Production schemas
- **Connection:** External Docker network `postgres_infra_network`

## Development Environment

### Local Development Setup

#### Conda Environment
- **Environment Name:** `airflow`
- **Python Version:** 3.11+
- **Purpose:** Isolated development environment for testing DAGs locally
- **Activation:** `conda activate airflow`

**Required Packages:**
```bash
conda create -n airflow python=3.11
conda activate airflow
pip install apache-airflow==2.7.3
pip install apache-airflow-providers-postgres
pip install requests beautifulsoup4 lxml urllib3
pip install openai  # For AI-enhanced projects
pip install ruff    # Linting and formatting
pip install pytest  # Testing (optional)
```

#### Ruff (Linting & Formatting)
- **Purpose:** Code quality enforcement and auto-formatting
- **Speed:** 10-100x faster than traditional tools (flake8, black, isort)
- **Features:**
  - Linting (replaces flake8)
  - Formatting (replaces black)
  - Import sorting (replaces isort)

**Installation:**
```bash
conda run -n airflow pip install ruff
```

**Usage:**
```bash
# Check code
ruff check .

# Fix auto-fixable issues
ruff check --fix .

# Format code
ruff format .
```

**Configuration:** See `.ruff.toml` in project root (see coding-standards.md)

### Development Workflow Commands

#### Airflow Commands (via Conda)
```bash
# Validate DAG syntax
conda run -n airflow python congreso_youtube/congreso_dag.py

# List all DAGs
conda run -n airflow airflow dags list

# Test specific DAG
conda run -n airflow airflow dags test congreso_youtube 2024-01-01

# Test individual task
conda run -n airflow airflow tasks test congreso_youtube extract_metadata 2024-01-01

# Trigger DAG manually
conda run -n airflow airflow dags trigger congreso_youtube
```

#### Code Quality Commands
```bash
# Run linter
conda run -n airflow ruff check .

# Auto-fix issues
conda run -n airflow ruff check --fix .

# Format code
conda run -n airflow ruff format .
```

## Docker Deployment

### Docker Compose Architecture

**Services:**
1. **airflow-webserver** - Web UI and API
2. **airflow-scheduler** - Task scheduling engine
3. **airflow-init** - One-time database initialization
4. **git-sync** - Automated DAG synchronization from GitHub
5. **init-perms** - Permission setup for volumes

**Volumes:**
- `airflow_logs` - Task execution logs
- `airflow_plugins` - Custom Airflow plugins
- `airflow_dags` - DAG files (managed by git-sync)
- `git_dags` - Git repository clone
- `airflow_data` - Persistent data storage

**Networks:**
- `postgres_infra_network` - External network for PostgreSQL connection
- `airflow_network` - Internal network for Airflow services

### Git-Sync Integration

**Purpose:** Automatically sync DAGs from GitHub repository to Airflow

**Configuration:**
```yaml
GIT_SYNC_REPO: GitHub repository URL with token
GIT_SYNC_BRANCH: dev
GIT_SYNC_WAIT: 15 seconds between syncs
GIT_SYNC_ROOT: /git
GIT_SYNC_DEST: repo
```

**Behavior:**
- Pulls latest code from `dev` branch every 15 seconds
- Airflow scheduler detects new/updated DAGs automatically
- No manual deployment needed for DAG changes

### Resource Limits

**Memory Management:**
```yaml
mem_limit: 1.5g        # Maximum memory per container
mem_reservation: 1g    # Guaranteed memory allocation
```

## Python Dependencies

### Global Dependencies (Docker)
Installed in all Airflow containers via `_PIP_ADDITIONAL_REQUIREMENTS`:
- `apache-airflow-providers-postgres` - PostgreSQL integration
- `openai` - OpenAI API client
- `beautifulsoup4` - HTML parsing
- `requests` - HTTP client
- `urllib3` - HTTP library

### Project-Specific Dependencies
Each project folder has its own `requirements.txt`:

**Example: `congreso_youtube/requirements.txt`**
```txt
requests>=2.31.0
beautifulsoup4>=4.12.2
lxml>=4.9.3
urllib3>=2.0.4
```

## Testing Strategy

### Manual Testing (Current)
```bash
# Syntax validation
conda run -n airflow python <dag_file>.py

# DAG structure test
conda run -n airflow airflow dags test <dag_id> <execution_date>

# Individual task test
conda run -n airflow airflow tasks test <dag_id> <task_id> <execution_date>
```

### Automated Testing (Recommended)

#### Unit Testing with pytest
```bash
# Install pytest
conda run -n airflow pip install pytest pytest-mock

# Run tests
conda run -n airflow pytest tests/
```

**Test Structure:**
```python
import pytest
from congreso_youtube.congreso_utils import process_video_data

def test_process_video_data():
    result = process_video_data(video_id="test123", session=15)
    assert result["video_id"] == "test123"
    assert result["session"] == 15
```

#### Integration Testing
```bash
# Test DAG loading
conda run -n airflow python -m pytest tests/dags/test_dag_validation.py
```

**Example Test:**
```python
import pytest
from airflow.models import DagBag

def test_no_import_errors():
    dag_bag = DagBag(include_examples=False)
    assert len(dag_bag.import_errors) == 0

def test_dag_has_tags():
    dag_bag = DagBag(include_examples=False)
    dag = dag_bag.get_dag("congreso_youtube")
    assert dag.tags is not None
```

#### CI/CD Testing (Future)
Planned GitHub Actions workflow:
```yaml
name: DAG Validation
on: [push, pull_request]
jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: pip install apache-airflow ruff pytest
      - run: ruff check .
      - run: pytest tests/
      - run: python -m py_compile congreso_youtube/*.py
```

## Database Schema Management

### Schema Separation Strategy

#### Development Schema
- **Naming:** `dev_<project>`
- **Example:** `dev_congreso`
- **Purpose:** Testing and development
- **Permissions:** Full read/write access
- **Data:** Test/sample data

#### Production Schema
- **Naming:** `prod_<project>`
- **Example:** `prod_congreso`
- **Purpose:** Live production data
- **Permissions:** Read-only for most users
- **Data:** Real production data

### Connection Management

**Airflow Connections UI:**
1. Navigate to Admin → Connections
2. Create connection:
   - **Conn Id:** `postgres_dev` / `postgres_prod`
   - **Conn Type:** Postgres
   - **Host:** postgres_shared
   - **Schema:** `dev_<project>` / `prod_<project>`
   - **Login:** database username
   - **Password:** database password
   - **Port:** 5432

**Using in DAGs:**
```python
from airflow.providers.postgres.operators.postgres import PostgresOperator

task = PostgresOperator(
    task_id='load_data',
    postgres_conn_id='postgres_dev',  # or 'postgres_prod'
    sql='INSERT INTO dev_congreso.videos ...',
)
```

## Environment Variables

### Required Variables (`.env` file)

```bash
# Airflow Configuration
AIRFLOW__CORE__FERNET_KEY=<generated_key>

# GitHub Integration
GITHUB_USER=<username>
GITHUB_TOKEN=<personal_access_token>
GITHUB_REPO=airflow-dags

# OpenAI Integration
OPENAI_API_KEY=<api_key>

# Database (if not using Airflow Connections)
POSTGRES_HOST=postgres_shared
POSTGRES_PORT=5432
POSTGRES_USER=<username>
POSTGRES_PASSWORD=<password>
POSTGRES_DB_DEV=dev_congreso
POSTGRES_DB_PROD=prod_congreso
```

### Generating Fernet Key
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Monitoring & Logging

### Airflow Built-in Monitoring
- **Web UI:** http://localhost:8081
- **DAG Status:** Real-time task status
- **Task Logs:** Accessible via UI
- **Gantt Chart:** Task execution timeline
- **Calendar View:** Historical DAG runs

### Log Storage
- **Location:** `airflow_logs` Docker volume
- **Format:** Per-DAG, per-task, per-execution date
- **Retention:** Configure in `airflow.cfg`

### Recommended Enhancements (Future)
- **Prometheus + Grafana:** Metrics and dashboards
- **Sentry:** Error tracking and alerting
- **ELK Stack:** Centralized log aggregation

## Performance Considerations

### Airflow Configuration Tuning
```yaml
PARALLELISM: 4              # Max tasks across all DAGs
MAX_ACTIVE_TASKS_PER_DAG: 2 # Max concurrent tasks per DAG
MAX_ACTIVE_RUNS_PER_DAG: 1  # Max concurrent DAG runs
```

### PostgreSQL Optimization
- **Connection Pooling:** Enable in Airflow config
- **Indexes:** Add indexes on frequently queried columns
- **Partitioning:** For large tables (future consideration)

### DAG Performance Best Practices
- Keep tasks lightweight (< 5 minutes ideal)
- Use XCom sparingly (< 1MB per message)
- Offload heavy processing to external systems
- Use appropriate retry strategies
- Set reasonable timeouts

## Security

### Secrets Management
- **Airflow Connections:** Store credentials in Airflow UI
- **Environment Variables:** Use `.env` (gitignored)
- **Secret Backend (Future):** AWS Secrets Manager, HashiCorp Vault

### Network Security
- Airflow services on isolated Docker network
- PostgreSQL on separate external network
- Web UI exposed on localhost only (port 8081)

### Access Control
- Airflow RBAC (Role-Based Access Control)
- PostgreSQL schema-level permissions
- Git repository access via token (not password)

## Deployment Strategy

### Development to Production Path
1. **Local Development:**
   - Develop DAG in `dev` branch
   - Test with `conda run -n airflow airflow dags test`
   - Lint with `ruff check`

2. **Push to Dev:**
   - Commit and push to `dev` branch
   - git-sync pulls changes to Docker
   - Test in Docker environment with dev schema

3. **Production Deployment:**
   - Merge `dev` → `main` via pull request
   - Update git-sync to pull from `main`
   - Switch connections to `prod` schema
   - Monitor execution closely

### Rollback Strategy
- Git revert problematic commits
- git-sync automatically pulls reverted code
- Airflow scheduler detects changes within 30 seconds

## Version Matrix

| Component | Development | Docker/Production |
|-----------|-------------|-------------------|
| Python | 3.11+ | 3.11+ |
| Airflow | 2.7.x | 2.10.2 (compatible) |
| PostgreSQL | 16 | 16 |
| Docker | Latest | Latest |
| Docker Compose | Latest | 3.8 spec |
| git-sync | N/A | v3.6.5 |

## Future Enhancements

### Potential Additions
- **Celery Executor:** For horizontal scaling
- **Redis:** As message broker for Celery
- **Kubernetes:** For cloud-native deployment
- **Prometheus/Grafana:** Advanced monitoring
- **Airflow Sensors:** For event-driven workflows
- **Data Quality Tools:** Great Expectations integration
- **CI/CD Pipeline:** Automated testing and deployment

### Technology Evaluation Criteria
When considering new technologies:
1. **Airflow Compatibility:** Must work with Airflow 2.7+
2. **Docker Support:** Easy containerization
3. **Documentation:** Good docs and community support
4. **Performance:** Minimal overhead
5. **Maintenance:** Low maintenance burden

---

**Last Updated:** 2025-10-02
**Airflow Version:** 2.7.x (2.10.2 in Docker)
**Python Version:** 3.11+
**PostgreSQL Version:** 16
**Deployment:** Docker Compose with LocalExecutor
