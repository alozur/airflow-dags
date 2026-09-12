# NAS Auto-Fetch Specification

## Purpose

Consumer DAGs (`speaker_turns`, `trim_proposals`, `speaker_turn_videos`, `reap_clip_preparer`, `speaker_turn_prepare`) MUST restore a missing source video from the NAS inline and continue processing, instead of returning `skipped_archived` or failing to find the file. The fetch MUST reuse `nas_fetch`'s existing primitives, be idempotent, and be safe when two consumer runs need the same `video_id` concurrently.

## Requirements

### Requirement: Inline fetch on missing local source

When a consumer DAG's source video is missing locally and the NAS (archive root or legacy root) has it, the DAG MUST fetch it inline, verify it, and proceed with normal processing instead of skipping.

#### Scenario: NAS-aware consumer finds source archived

- GIVEN `speaker_turns` (or `trim_proposals`, or `speaker_turn_videos`) needs `video_id=abc123` and its local material is missing
- AND `abc123` is present on the NAS (marker or discoverable under archive/legacy root)
- WHEN the DAG runs
- THEN the DAG MUST fetch `abc123` inline via the shared fetch primitive
- AND, once verified, the DAG MUST proceed with its normal processing (no `skipped_archived` result)

#### Scenario: Previously NAS-unaware consumer finds source archived

- GIVEN `reap_clip_preparer` (or `speaker_turn_prepare`) needs a source video that is missing locally
- AND the video is present on the NAS
- WHEN the DAG runs
- THEN the DAG MUST fetch it inline via the same shared primitive used by the NAS-aware consumers
- AND proceed with normal processing after verification

### Requirement: NAS-missing preserves existing behavior

When the source video is missing both locally and on the NAS, the DAG's pre-existing behavior (skip or fail, per that DAG's current contract) MUST be preserved unchanged.

#### Scenario: Video absent everywhere (NAS-aware consumer)

- GIVEN `speaker_turns` needs `video_id=zzz999` and it is missing locally
- AND `zzz999` is not on the NAS (no marker, no match under archive or legacy root)
- WHEN the DAG runs
- THEN the DAG MUST return its existing missing-source outcome (`skipped_no_video`, per design D6a), unchanged by this feature

#### Scenario: Video absent everywhere (previously unaware consumer)

- GIVEN `reap_clip_preparer` needs a source video missing locally and not found on the NAS
- WHEN the DAG runs
- THEN the DAG MUST fall back to its pre-existing behavior for a missing source (skip or fail, per current contract), unchanged in outcome by this feature

### Requirement: Idempotent, concurrency-safe fetch

The inline fetch MUST be idempotent and safe when multiple consumer runs need the same `video_id` concurrently: a concurrent run MUST either wait for the in-progress fetch or reuse its completed result, and MUST NOT produce a duplicate, partial, or corrupted local tree.

#### Scenario: Two consumers need the same video concurrently

- GIVEN `speaker_turns` and `trim_proposals` both need `video_id=abc123`, which is missing locally and present on the NAS
- AND both DAG runs start their fetch for `abc123` at nearly the same time
- WHEN both inline fetches execute
- THEN exactly one fetch MUST perform the rsync pull and verification for `abc123`
- AND the other MUST either wait for that fetch to complete or detect the already-fetched, verified local tree and reuse it
- AND neither run MUST observe a partial or corrupted local tree for `abc123`

#### Scenario: Fetch already completed before a second consumer starts

- GIVEN `abc123` was already fetched and verified locally by an earlier consumer run in the same window
- WHEN a later consumer run needs `abc123`
- THEN the DAG MUST detect the local material is already present and MUST NOT re-fetch unnecessarily
- AND MUST proceed directly with processing

### Requirement: Retention refresh on auto-fetch

A video restored via inline auto-fetch MUST have its local retention refreshed the same way an explicit `nas_fetch` run refreshes it, so the video does not immediately re-qualify for `nas_archive` pruning.

#### Scenario: Auto-fetched video gets a fresh retention window

- GIVEN a consumer DAG inline-fetches `video_id=abc123` from the NAS
- WHEN the fetch completes and verification passes
- THEN the fetched media files' local mtime MUST be refreshed to the fetch time (same mechanism as `nas_fetch.refresh_retention`)
