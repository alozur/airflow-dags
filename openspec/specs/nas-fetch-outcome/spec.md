# NAS Fetch Outcome Specification

## Purpose

`nas_fetch` MUST report a run's true outcome to Airflow. Today `_run_fetch_videos` always returns a summary dict and never raises, so a run where every requested video fails still shows `success` in the Airflow UI — masking failures that need an operator's attention.

## Requirements

### Requirement: All-failed run fails the task

When every requested `video_id` fails to fetch, the `fetch_videos` task MUST fail (raise, not return a silent summary).

#### Scenario: Single requested video fails

- GIVEN a `nas_fetch` run triggered with `{"video_id": "abc123"}`
- WHEN the fetch for `abc123` raises (rsync failure, verification mismatch, or no remote directory found)
- THEN the `fetch_videos` task MUST fail
- AND the failure MUST report `video_id=abc123` and the underlying error

#### Scenario: All videos in a batch fail

- GIVEN a `nas_fetch` run triggered with `{"video_ids": ["abc123", "def456"]}`
- WHEN both `abc123` and `def456` fail to fetch
- THEN the `fetch_videos` task MUST fail
- AND the failure MUST list every failed `video_id` with its error

### Requirement: Partial failure stays visible

When some requested videos succeed and others fail, the run outcome MUST make the failures visible to an operator — a silent per-video catch that returns overall `success` is insufficient.

#### Scenario: Mixed batch with one failure

- GIVEN a `nas_fetch` run triggered with `{"video_ids": ["abc123", "def456"]}`
- WHEN `abc123` fetches successfully and `def456` fails
- THEN the task outcome MUST surface `def456`'s failure (task-level failure, or an equivalently visible non-success signal distinct from a fully successful run)
- AND `abc123`'s successful restore MUST NOT be reverted or discarded because of `def456`'s failure

### Requirement: Empty request is a no-op success

A run requesting zero videos MUST NOT be treated as a failure.

#### Scenario: No video_id or video_ids provided

- GIVEN a `nas_fetch` run triggered with an empty or missing `conf`
- WHEN the DAG validates the request
- THEN the task MUST fail with a clear "conf must include 'video_id' or a non-empty 'video_ids' list" error (existing validation), which is a distinct, expected failure mode from the all-failed-fetch case above

#### Scenario: Every video already fetched (fully successful, nothing to restore)

- GIVEN a `nas_fetch` run triggered with `{"video_ids": ["abc123"]}`
- WHEN `abc123` fetches and verifies successfully (a genuine restore, not a failure)
- THEN the task MUST succeed
- AND the summary MUST record `abc123` under restored results
