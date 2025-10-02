# Source Tree

This document describes the directory structure and organization of the Airflow DAGs repository.

## Directory Structure

```plaintext
airflow-dags/
├── .claude/                        # Claude Code AI agent configurations
│   ├── agents/
│   │   └── dev.md                 # Alvaro - Airflow Expert agent definition
│   └── settings.local.json        # Claude Code local settings
├── .github/                        # GitHub Actions CI/CD workflows
│   └── workflows/
│       └── (future: DAG validation, testing)
├── .vscode/                        # VSCode workspace settings
├── docs/                           # Project documentation
│   └── architecture/
│       ├── coding-standards.md    # Coding standards and best practices
│       ├── source-tree.md         # This file - directory structure
│       └── tech-stack.md          # Technology stack and tools
├── utils/                          # Shared utilities across all DAG projects
│   ├── __init__.py
│   ├── airflow_helpers.py         # XCom utilities and Airflow decorators
│   ├── postgres_helpers.py        # PostgreSQL utility functions
│   └── env_loader.py              # Environment variable management
├── congreso_youtube/               # Congressional YouTube video processing project
│   ├── __init__.py
│   ├── congreso_dag.py            # Main DAG definition
│   ├── congreso_utils.py          # Project-specific utility functions
│   ├── congress_database.py       # Database schema and operations
│   ├── postgres_operators.py      # Custom PostgreSQL operators
│   ├── congressional_videos_schema.sql  # Database schema DDL
│   └── requirements.txt           # Project-specific Python dependencies
├── example/                        # Example DAGs for reference
│   ├── hello_world_dag.py         # Simple example DAG
│   └── hello_weekly_dag.py        # Scheduled weekly DAG example
├── [future_projects]/              # Future project folders (TBD)
│   ├── __init__.py
│   ├── <project>_dag.py
│   ├── <project>_utils.py
│   └── requirements.txt
├── .env                            # Environment variables (gitignored)
├── .gitignore                      # Git ignore patterns
├── CLAUDE.md                       # AI agent project instructions
├── docker-compose.yml              # Docker Compose configuration for Airflow
└── README.md                       # Project overview and setup instructions

```

## Directory Descriptions

### `.claude/`
**Purpose:** AI agent configurations for Claude Code

- **agents/dev.md:** Complete definition of "Alvaro", the Airflow Expert agent
  - Agent persona and capabilities
  - Airflow best practices
  - Command reference
  - Quick start guides

### `.github/`
**Purpose:** CI/CD workflows and GitHub Actions

- Future location for:
  - DAG syntax validation
  - Automated testing
  - Linting checks (ruff)
  - Deployment pipelines

### `docs/architecture/`
**Purpose:** Project documentation and architectural decisions

- **coding-standards.md:** Mandatory coding standards for development
- **source-tree.md:** This file - directory structure guide
- **tech-stack.md:** Technology stack and environment setup

### `utils/`
**Purpose:** Shared utility functions used across multiple DAG projects

**Key Files:**
- **airflow_helpers.py:** Airflow-specific utilities
  - `xcom_task` decorator: Simplified XCom operations
  - Task management helpers
- **postgres_helpers.py:** PostgreSQL utilities
  - Connection helpers
  - Query builders
  - Transaction management
- **env_loader.py:** Environment configuration
  - Load variables from `.env`
  - Configuration validation

**Usage Pattern:**
```python
from utils.airflow_helpers import xcom_task
from utils.postgres_helpers import get_connection
```

### Project Folders (e.g., `congreso_youtube/`)
**Purpose:** Independent Airflow projects, each with its own DAG

**Standard Structure:**
- `__init__.py` - Python package initialization
- `<project>_dag.py` - Main DAG definition file
- `<project>_utils.py` - Project-specific utility functions
- `requirements.txt` - Python dependencies specific to this project
- Additional modules as needed (database, operators, processors, etc.)

**Current Projects:**

#### `congreso_youtube/`
Processes congressional session videos from YouTube:
- Extracts video metadata
- Identifies topics and speakers
- Stores structured data in PostgreSQL
- Generates AI-enriched metadata

**Key Components:**
- `congreso_dag.py` - Main orchestration DAG
- `congress_database.py` - PostgreSQL schema and table management
- `postgres_operators.py` - Custom operators for database operations
- `congressional_videos_schema.sql` - Database schema DDL

#### `example/`
Reference DAGs demonstrating Airflow patterns:
- `hello_world_dag.py` - Basic DAG structure
- `hello_weekly_dag.py` - Scheduled DAG with weekly runs

### Root Configuration Files

#### `docker-compose.yml`
**Purpose:** Docker Compose configuration for Airflow deployment

**Services:**
- `airflow-webserver` - Airflow UI (port 8081)
- `airflow-scheduler` - Task scheduling service
- `airflow-init` - Database initialization
- `git-sync` - Automated DAG synchronization from GitHub
- `postgres_shared` - PostgreSQL database (external network)

**Key Settings:**
- Executor: LocalExecutor
- Airflow version: 2.10.2 (compatible with 2.7.x)
- Time zone: Europe/Madrid
- DAG sync from `dev` branch every 15 seconds

#### `.env`
**Purpose:** Environment variables (gitignored for security)

**Typical Contents:**
- `AIRFLOW__CORE__FERNET_KEY` - Encryption key
- `OPENAI_API_KEY` - OpenAI API credentials
- `GITHUB_USER` - GitHub username for git-sync
- `GITHUB_TOKEN` - GitHub personal access token
- `GITHUB_REPO` - Repository name
- Database connection strings (dev/prod)

#### `CLAUDE.md`
**Purpose:** AI agent instructions for Claude Code

Contains:
- Project context
- Expertise areas required
- Development guidelines
- Common patterns
- Key commands

## Project Organization Principles

### 1. **Folder Per Project**
Each independent DAG project gets its own folder with:
- Self-contained code
- Own requirements.txt
- Clear boundaries

### 2. **Shared Utilities**
Common functionality lives in `utils/`:
- Reusable across all projects
- Well-documented
- Tested independently

### 3. **Documentation First**
`docs/` contains all architectural documentation:
- Standards are mandatory
- Updated with project evolution
- Version controlled

### 4. **Environment Separation**
Development and production environments are separated:
- PostgreSQL schemas: `dev_*` vs `prod_*`
- Environment-specific configuration
- Clear deployment path

## Adding a New Project

To add a new DAG project:

1. **Create Project Folder:**
   ```bash
   mkdir <project_name>
   cd <project_name>
   ```

2. **Create Standard Files:**
   ```bash
   touch __init__.py
   touch <project_name>_dag.py
   touch <project_name>_utils.py
   touch requirements.txt
   ```

3. **Follow Naming Conventions:**
   - DAG file: `<project>_dag.py`
   - DAG ID: `<project>_<description>`
   - Utilities: `<project>_utils.py`

4. **Document in This File:**
   - Add project description
   - List key components
   - Note any special requirements

5. **Update Requirements:**
   - Project-specific deps in `<project>/requirements.txt`
   - Global deps in `docker-compose.yml` `_PIP_ADDITIONAL_REQUIREMENTS`

## File Naming Patterns

| File Type | Pattern | Example |
|-----------|---------|---------|
| DAG File | `<project>_dag.py` | `congreso_youtube_dag.py` |
| Utils | `<project>_utils.py` | `congreso_utils.py` |
| Database | `<project>_database.py` | `congress_database.py` |
| Operators | `<project>_operators.py` | `postgres_operators.py` |
| Schema SQL | `<project>_schema.sql` | `congressional_videos_schema.sql` |
| Tests | `test_<module>.py` | `test_congreso_utils.py` |

## Import Conventions

### Importing Shared Utilities
```python
# Standard imports
from utils.airflow_helpers import xcom_task
from utils.postgres_helpers import execute_query
from utils.env_loader import load_env_var
```

### Importing Project Modules
```python
# Within project folder
from congreso_youtube.congreso_utils import process_data
from congreso_youtube.congress_database import CongressDatabase
```

### Importing Airflow
```python
# Airflow core
from airflow import DAG
from airflow.operators.python import PythonOperator

# Airflow providers
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
```

## Git Management

### Branches
- `main` - Production-ready code
- `dev` - Development branch (used by git-sync in Docker)
- `feature/*` - Feature branches for new development

### Ignored Files
See `.gitignore` for complete list:
- `.env` - Environment variables
- `__pycache__/` - Python cache
- `*.pyc` - Compiled Python
- `logs/` - Log files
- `.vscode/` - IDE settings (some)

## Testing Structure (Future)

Planned test organization:
```plaintext
tests/
├── unit/
│   ├── test_airflow_helpers.py
│   ├── test_postgres_helpers.py
│   └── projects/
│       └── test_congreso_utils.py
├── integration/
│   └── test_congreso_dag_integration.py
└── dags/
    └── test_dag_validation.py
```

---

**Last Updated:** 2025-10-02
**Repository:** airflow-dags
**Primary Branch:** dev
