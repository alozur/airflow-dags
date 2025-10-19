# Git Sync DAG

This project contains Airflow DAGs for synchronizing GitHub repositories automatically. It provides both scheduled and manual synchronization capabilities.

## Overview

The Git Sync DAG system provides two DAGs:

1. **git_sync_all_repos** - Scheduled DAG that syncs all configured repositories daily
2. **git_sync_manual** - Manual trigger DAG for syncing a specific repository with custom parameters

## Features

- Automatic daily synchronization of multiple repositories
- Environment-aware branch selection (dev/main)
- Manual trigger with custom repository and branch selection
- Comprehensive error handling and logging
- Support for multiple repositories via environment configuration
- Authenticated GitHub access using personal access tokens

## Configuration

### Environment Variables

Add these variables to your `.env` file or Airflow environment:

```bash
# Required
GITHUB_USER=alozur                                    # Your GitHub username
GITHUB_TOKEN=github_pat_11AIZYRSQ0...                # Your GitHub Personal Access Token
GIT_SYNC_REPOS=alozur/airflow-dags,alozur/repo2      # Comma-separated list of repositories

# Optional
GIT_SYNC_BRANCH=dev                                   # Override branch (default: auto-detected)
ENVIRONMENT=development                               # Environment: development or production
```

### Repository List Format

The `GIT_SYNC_REPOS` variable should be a comma-separated list of repositories in `username/repository` format:

```bash
GIT_SYNC_REPOS=alozur/airflow-dags,alozur/congress-videos,alozur/tech-finance
```

### Branch Selection

The DAG automatically selects branches based on the environment:

- **Development environment** (`ENVIRONMENT=development`): Uses `dev` branch
- **Production environment** (`ENVIRONMENT=production`): Uses `main` branch
- **Override**: Set `GIT_SYNC_BRANCH` to force a specific branch

## DAG Details

### git_sync_all_repos

**Schedule**: Daily at 2:00 AM
**Purpose**: Synchronize all repositories configured in `GIT_SYNC_REPOS`

**Behavior**:
- Reads repository list from environment variables
- Syncs each repository sequentially
- Clones repositories that don't exist locally
- Pulls latest changes for existing repositories
- Reports summary of successful and failed syncs

**Usage**:
```bash
# Trigger manually
airflow dags trigger git_sync_all_repos

# Test the DAG
airflow dags test git_sync_all_repos 2025-10-19
```

### git_sync_manual

**Schedule**: Manual trigger only
**Purpose**: Sync a specific repository with custom branch

**Parameters**:
- `repository` (required): Repository in `username/repository` format
- `branch` (optional): Branch to sync (defaults to environment-based branch)

**Usage**:
```bash
# Trigger with default parameters
airflow dags trigger git_sync_manual

# Trigger with custom repository and branch
airflow dags trigger git_sync_manual \
  --conf '{"repository": "alozur/airflow-dags", "branch": "main"}'

# Test the DAG
airflow dags test git_sync_manual 2025-10-19
```

## File Structure

```
git_sync/
├── __init__.py                 # Package initialization
├── git_sync_dag.py            # Main DAG definitions
├── README.md                  # This file
└── modules/
    ├── __init__.py            # Module initialization
    └── git_operations.py      # Git operation functions
```

## How It Works

### Scheduled Sync (git_sync_all_repos)

1. Reads `GIT_SYNC_REPOS` environment variable
2. Parses comma-separated repository list
3. For each repository:
   - Checks if it exists locally in `/opt/airflow/dags` directory
   - If exists: Fetches and hard resets to latest commit
   - If not exists: Clones the repository
4. Reports results via XCom and logs

### Manual Sync (git_sync_manual)

1. Accepts `repository` and `branch` parameters
2. Validates GitHub credentials from environment
3. Syncs the specified repository:
   - Clones if it doesn't exist
   - Pulls latest changes if it exists
4. Reports result via XCom and logs

## Git Operations

The DAG performs the following Git operations:

**For existing repositories**:
```bash
git fetch origin <branch>
git reset --hard origin/<branch>
```

**For new repositories**:
```bash
git clone -b <branch> <authenticated-url> /opt/airflow/dags/<repo-name>
```

## Error Handling

- **Missing credentials**: DAG fails with clear error message
- **Invalid repository**: Git errors are caught and logged
- **Network issues**: Retries up to 2 times with 5-minute delay
- **Failed syncs**: Reported in summary without stopping other syncs

## Monitoring

### XCom Output

Both DAGs push results to XCom:

**Successful sync**:
```json
{
  "status": "success",
  "action": "pulled",
  "repo": "alozur/airflow-dags",
  "branch": "dev",
  "path": "/opt/airflow/dags/airflow-dags",
  "commit": "abc123def456...",
  "message": "Successfully synced alozur/airflow-dags on branch dev"
}
```

**Failed sync**:
```json
{
  "status": "failed",
  "repo": "alozur/invalid-repo",
  "branch": "dev",
  "error": "Git operation failed...",
  "message": "Failed to sync alozur/invalid-repo on branch dev"
}
```

### Logs

Check Airflow logs for detailed output:
```
Sync Summary: 3 successful, 0 failed out of 3 total
✓ Successfully synced alozur/airflow-dags on branch dev - Commit: abc123de
✓ Successfully synced alozur/congress-videos on branch dev - Commit: def456ab
✓ Successfully synced alozur/tech-finance on branch dev - Commit: 789012cd
```

## Integration with Docker Compose

This DAG replicates the functionality of the `git-sync` container from your docker-compose.yml, but with more flexibility:

**Advantages over git-sync container**:
- Sync multiple repositories (not just one)
- Schedule flexibility (not just fixed interval)
- Manual trigger capability with custom parameters
- Better error reporting and monitoring
- Environment-aware branch selection

## Security Considerations

1. **Token Security**: Store `GITHUB_TOKEN` securely in Airflow Variables or Secrets
2. **Token Permissions**: Use fine-grained tokens with read-only repository access
3. **Log Sanitization**: Tokens are not logged in plain text
4. **Token Rotation**: Regularly rotate GitHub personal access tokens

## Testing

Test the DAG syntax and imports:

```bash
# Activate conda environment
conda activate airflow

# Test DAG file
conda run -n airflow python git_sync/git_sync_dag.py

# List DAGs
airflow dags list | grep git_sync

# Test the scheduled DAG
airflow dags test git_sync_all_repos 2025-10-19

# Test the manual DAG with parameters
airflow dags test git_sync_manual 2025-10-19
```

## Troubleshooting

### DAG not showing in Airflow UI

- Check that the DAG file is in the correct dags directory
- Verify no Python syntax errors: `python git_sync/git_sync_dag.py`
- Check Airflow scheduler logs for import errors

### Authentication failures

- Verify `GITHUB_USER` and `GITHUB_TOKEN` are set correctly
- Ensure token has proper repository access permissions
- Check token hasn't expired

### Repository not syncing

- Check repository name format is `username/repository`
- Verify branch exists in the repository
- Check network connectivity to GitHub
- Review task logs for specific error messages

## Future Enhancements

Potential improvements:
- [ ] Support for multiple branches per repository
- [ ] Slack/email notifications on sync failures
- [ ] Metrics and sync duration tracking
- [ ] Selective file sync (not full repository)
- [ ] Integration with Airflow sensors for change detection
