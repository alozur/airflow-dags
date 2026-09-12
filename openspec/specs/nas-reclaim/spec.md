# NAS Reclaim Specification

## Purpose

A new scheduled `nas_reclaim` DAG MUST reclaim local disk space for videos whose material is already safely on the NAS and no longer needed locally, running frequently enough (every 4 hours) that ephemeral fetched material does not sit on the VPS's constrained disk for the 14-day `nas_archive` window.

## Requirements

### Requirement: Three-gate deletion

`nas_reclaim` MUST delete a video's local ephemeral material only when ALL of the following hold: the NAS copy is verified present, the DB-completeness query reports nothing pending for that video, and the local material is older than `NAS_RECLAIM_GRACE_HOURS` (default 12) since its last fetch or refresh.

#### Scenario: All three gates pass — reclaim happy path

- GIVEN `video_id=abc123` has a verified NAS copy (marker present and confirmed, or `verify_synced` passes)
- AND the DB-completeness query reports nothing pending for `abc123`
- AND `abc123`'s local material's newest mtime is older than `NAS_RECLAIM_GRACE_HOURS` ago
- WHEN `nas_reclaim` runs
- THEN `abc123`'s local ephemeral material MUST be deleted
- AND the `.nas_archived.json` marker MUST be written or kept so later consumers know the NAS has it

#### Scenario: Blocked by pending DB work

- GIVEN `video_id=abc123` has a verified NAS copy and passed its grace window
- AND the DB-completeness query reports pending work for `abc123` (e.g. an unuploaded chapter or turn)
- WHEN `nas_reclaim` runs
- THEN `abc123`'s local material MUST NOT be deleted

#### Scenario: Blocked by grace window

- GIVEN `video_id=abc123` has a verified NAS copy and nothing pending in the DB
- AND `abc123`'s local material was fetched or refreshed less than `NAS_RECLAIM_GRACE_HOURS` ago
- WHEN `nas_reclaim` runs
- THEN `abc123`'s local material MUST NOT be deleted this run

#### Scenario: Blocked by unverified NAS copy

- GIVEN `video_id=abc123` has nothing pending in the DB and passed its grace window
- AND the NAS copy for `abc123` cannot be verified as present (no marker and no confirmed match, or verification fails)
- WHEN `nas_reclaim` runs
- THEN `abc123`'s local material MUST NOT be deleted

### Requirement: Protected and non-media content is never touched

`nas_reclaim` MUST NOT delete `MIRROR_ONLY_DIRS` content (e.g. `thumbnails/`) or non-media sidecar files that the archive keeps, regardless of gate outcomes.

#### Scenario: Thumbnails untouched

- GIVEN `video_id=abc123` clears all three reclaim gates
- WHEN `nas_reclaim` deletes `abc123`'s local ephemeral material
- THEN files under `thumbnails/` (or any other `MIRROR_ONLY_DIRS` entry) MUST NOT be deleted

#### Scenario: Sidecar files preserved

- GIVEN `video_id=abc123` clears all three reclaim gates and has derived-artifact sidecar files (SRT/JSON/PNG) alongside its media
- WHEN `nas_reclaim` deletes `abc123`'s local ephemeral material
- THEN only media files eligible for pruning under the existing `prune_local` rules MUST be removed
- AND non-media sidecar files MUST be preserved

### Requirement: In-flight fetch is never reclaimed

A video currently being fetched (holding a fetch lock or in-progress marker) MUST NOT be reclaimed, even if it otherwise appears to clear the three gates.

#### Scenario: Reclaim runs during an active fetch

- GIVEN `video_id=abc123` is currently being fetched by a consumer DAG (fetch lock/marker held)
- AND `abc123` would otherwise clear all three reclaim gates based on stale prior state
- WHEN `nas_reclaim` runs concurrently
- THEN `abc123` MUST be skipped this run (no deletion) because the in-progress fetch lock/marker is detected

### Requirement: Bounded run size

Each `nas_reclaim` run MUST cap the number of videos it reclaims, consistent with `nas_archive`'s batched, rate-limited pruning approach.

#### Scenario: More eligible videos than the run cap

- GIVEN more videos clear all three reclaim gates than the run's cap allows
- WHEN `nas_reclaim` runs
- THEN only up to the cap MUST be reclaimed this run
- AND the remaining eligible videos MUST remain candidates for a subsequent run

### Requirement: Schedule and environment contract

`nas_reclaim` MUST run on a schedule more frequent than `nas_archive` (every 4 hours), and `NAS_RECLAIM_GRACE_HOURS` MUST be part of the deploy environment contract.

#### Scenario: Scheduled cadence

- GIVEN the `nas_reclaim` DAG is deployed and enabled
- WHEN Airflow evaluates its schedule
- THEN the DAG MUST run every 4 hours

#### Scenario: Grace hours configurable via environment

- GIVEN `NAS_RECLAIM_GRACE_HOURS` is set in the deploy environment (`deploy/vps-dev/compose.yml`)
- WHEN `nas_reclaim` reads its settings
- THEN it MUST use the configured value instead of the default of 12 hours
- AND `deploy/vps-dev/test_contract.py` MUST assert the env var is wired with its default

#### Scenario: Default grace hours when unset

- GIVEN `NAS_RECLAIM_GRACE_HOURS` is not set in the environment
- WHEN `nas_reclaim` reads its settings
- THEN it MUST default to 12 hours
