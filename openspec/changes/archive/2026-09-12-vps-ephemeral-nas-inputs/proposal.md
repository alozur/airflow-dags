# Proposal: VPS ephemeral NAS inputs (auto-fetch then reclaim)

## Intent

**Problem**: the VPS has a 99 GB shared disk (~28 GB free) while the NAS holds ~75 GiB / 31 videos of backlog (~2.4 GiB each). When source material is archived and pruned, the 3 NAS-aware consumer DAGs return `skipped_archived` and the 2 unaware ones fail to find the file, so archived videos are never processed without a manual `nas_fetch` trigger.

**Why now**: the 2026-09-11 cutover made the VPS the Airflow producer; the backlog cannot be processed and cannot fit on disk at once.

**Success**: consumers pull what they need on demand, and space is reclaimed hours later — not after 14 days.

## Scope

### In Scope
- Fix `nas_fetch` reporting SUCCESS when every requested video fails (`_run_fetch_videos` in `congress_videos/nas_fetch_dag.py`).
- Shared inline-fetch helper over existing `nas_fetch` primitives, idempotent and concurrency-safe (per-video lock/marker).
- Wire all 5 consumers: `speaker_turns`, `trim_proposals`, `speaker_turn_videos`, `reap_clip_preparer`, `speaker_turn_prepare`.
- New scheduled `nas_reclaim` DAG (every 4 h) reusing `nas_archive`'s completeness query, `verify_synced`, `prune_local`.
- New `NAS_RECLAIM_GRACE_HOURS` (default 12) in `ArchiveSettings`, `deploy/vps-dev/compose.yml`, `deploy/vps-dev/test_contract.py`.

### Out of Scope
- `thumbnail_republish abandon=True` on NAS-only trees; NFS/SSHFS mounts; Ansible changes (env already rendered); disk alarms.

## Capabilities

### New Capabilities
- `nas-fetch-outcome`: fetch run status truthfully reflects per-video failures.
- `nas-auto-fetch`: consumer DAGs restore missing source material inline instead of skipping.
- `nas-reclaim`: scheduled, gated deletion of local copies already verified on the NAS.

### Modified Capabilities
- None.

## Approach

Exploration approach 2. NAS is source of truth; VPS is a working copy. Consumers call the fetch primitive inline; `nas_reclaim` deletes local material only when three gates pass: NAS-verified presence (archive or legacy root), DB completeness (nothing pending), and `NAS_RECLAIM_GRACE_HOURS` elapsed. The grace window replaces `NAS_ARCHIVE_MIN_AGE_DAYS` (14 d) for fetched material, which would defeat the goal.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/nas_fetch_dag.py` | Modified | Fail run when all fetches fail |
| `congress_videos/modules/nas_fetch.py` | Modified | Inline-fetch helper + lock |
| 5 consumer DAGs + `modules/vad_helpers.py` | Modified | Replace skip / add hook |
| `congress_videos/nas_reclaim_dag.py` | New | Scheduled reclaim, every 4 h |
| `congress_videos/modules/nas_archive.py` | Modified | `NAS_RECLAIM_GRACE_HOURS` |
| `deploy/vps-dev/{compose.yml,test_contract.py}` | Modified | Env contract |

## PR Slices (400-line budget each)

1. Fetch-outcome fix + shared inline-fetch helper.
2. Wire the 3 NAS-aware DAGs.
3. Wire the 2 unaware DAGs (`reap_clip_preparer`, `speaker_turn_prepare`).
4. `nas_reclaim` DAG + env/compose.

Feature branches PR into `dev`; `main` takes release PRs. Strict TDD, `uv run pytest`.

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Reclaim deletes material a run just fetched | Med | Grace window + completeness gate + lock marker |
| Completeness query gap inherited | Med | Slice 1 fixes fetch outcome; reclaim gate covered by tests |
| Multi-GB rsync inside a previously skip-only task | High | Reuse `_RSYNC_TIMEOUT_SECS`; review task timeout/retries in design |
| Tailnet outage now blocks 5 DAGs | Med | Explicit failure, retries; NAS unreachable = fail, never silent prune |
| Re-fetch churn for multi-consumer videos | Med | Accepted; grace window absorbs most |

## Rollback Plan

Per slice: revert the slice's commits and redeploy. `nas_reclaim` is disabled by pausing the DAG (no data loss — NAS copies are authoritative). Inline fetch degrades to the previous `skipped_archived` behaviour when reverted. No schema or NAS-side migration.

## Dependencies

- NAS reachable over the tailnet; `NAS_ARCHIVE_*` / `NAS_FETCH_LEGACY_ROOT` already rendered by `homeserver-config`.

## Success Criteria

- [ ] An archived video processes end to end with no manual `nas_fetch` trigger.
- [ ] `nas_fetch` run fails when every requested video fails.
- [ ] `nas_reclaim` frees local material within ~12–16 h of last use, never while pending work exists.
- [ ] Concurrent consumer runs on the same `video_id` fetch safely, without duplicate or partial trees.
- [ ] `uv run pytest` green per slice; each slice under 400 changed lines.
