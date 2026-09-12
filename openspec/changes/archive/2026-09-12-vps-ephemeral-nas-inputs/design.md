# Design: VPS ephemeral NAS inputs (auto-fetch then reclaim)

Change: `vps-ephemeral-nas-inputs` · Approach 2 (proposal) · Grounded against `vps-dev-foundation`.

## Technical Approach

NAS is the source of truth; VPS local disk is a working copy with a short lease. One shared, Airflow-free helper `nas_fetch.ensure_local_video()` becomes the single fetch path: `nas_fetch_dag.fetch_one_video` delegates to it and the 5 consumer DAGs call it inline where they today skip. A new `nas_reclaim` DAG re-prunes local material every 4 h behind four independent gates. No new subprocess/SSH primitive is introduced — every remote call reuses `nas_fetch`/`nas_archive` command builders with an injected runner.

## Architecture Decisions

### D1 — Shared helper lives in `modules/nas_fetch.py`, not in a DAG module

| Option | Tradeoff | Decision |
|---|---|---|
| Consumers import `nas_fetch_dag.fetch_one_video` | Zero new code, but DAG-to-DAG import re-parses a module that builds a `DAG` object at import time | Rejected |
| New `modules/nas_autofetch.py` | Clean, but splits fetch logic across two modules and duplicates the marker/discovery branch | Rejected |
| **Move orchestration into `modules/nas_fetch.py`; `fetch_one_video` delegates** | One tested fetch path, no Airflow import in the shared helper, file grows ~450→~620 lines (under the 800 cap) | **Chosen** |

`AirflowException` cannot be raised from a pure module, so the module gains `NasFetchError(RuntimeError)`; `nas_fetch_dag` adds it to the per-video catch tuple in `_run_fetch_videos`.

`ensure_local_video` returns a status dict rather than raising for every non-success outcome, so the delegating `fetch_one_video` MUST re-raise — see **D6a**. Without that conversion a NAS-absent video would land in `summary["restored"]`, which is the exact false-success bug this change exists to fix.

### D2 — Concurrency: per-video lock file with atomic `O_CREAT|O_EXCL`, non-blocking

| Option | Tradeoff | Decision |
|---|---|---|
| In-flight marker (write-then-check) | Not atomic; two processes can both observe "absent" | Rejected |
| `flock` on a fd | Atomic, but silently degrades on some network filesystems and cannot carry owner metadata for stale detection | Rejected |
| **`os.open(O_CREAT\|O_EXCL\|O_WRONLY)` at `{channel_slug}/{video_id}/.nas_fetch.lock`, JSON payload `{token, pid, acquired_at}`** | Atomic on the local ext4 volume; Airflow LocalExecutor tasks are separate processes on one host, so a filesystem lock is sufficient | **Chosen** |

- **Location**: the channel dir survives `prune_local` (it only unlinks `*.mp4` there), unlike `downloads/{date}/{video_id}` which is `rmtree`d. Same directory as `.nas_archived.json`.
- **Contention**: never wait. A holder blocks for up to `RSYNC_TIMEOUT_SECS` (3600); blocking would pin a LocalExecutor slot for an hour and duplicate a 2.4 GiB transfer. A contending caller returns `status: "in_progress"` and retries on its next scheduled run.
- **Stale locks**: a lock older than `RSYNC_TIMEOUT_SECS + 600` is broken and re-acquired. Release unlinks **only** when the on-disk `token` matches the holder's `uuid4`, so breaking a stale lock can never make one holder delete another's lock.

### D3 — Reclaim cannot delete a tree a concurrent run just fetched

Four independent barriers; each alone would be enough for the common case.

1. **Lock** — reclaim acquires the same per-video lock non-blockingly and skips the candidate when busy, so no prune overlaps an in-flight fetch.
2. **Grace window** — `refresh_retention()` sets every fetched media file's mtime to fetch time *inside the lock*, before release. Reclaim requires newest-mtime age ≥ `NAS_RECLAIM_GRACE_HOURS` (12), so a just-released tree is ineligible for 12 h.
3. **DB completeness** — a video being processed has rows pending in `uploadable_chapters`/`uploadable_turns`, so it is not "complete" and is never a candidate. **Selection-time only** — not re-checked inside the lock.
4. **NAS verification** — verified immediately before deletion, so the worst possible outcome is re-fetch churn, never data loss.

Gates 1, 2, and 4 are re-evaluated **inside** the lock immediately before `prune_local`. Gate 3 (DB completeness) is applied at selection time only, mirroring D4's accepted-risk reasoning: reclaim only deletes NAS-verified bytes, and a consumer that needs the video re-fetches it. The selection pass is advisory for gate 3; gates 1, 2, and 4 are definitive barriers inside the lock.

### D4 — `verify_synced`/`verify_fetched` must fail closed on a non-zero exit (hardening)

Both today inspect only `stdout` (`nas_archive.py:350-353`, `nas_fetch.py:269-272`). A dry-run rsync that *errors* (remote dir absent, tailnet down) returns empty stdout, `all()` over zero lines is `True`, and the function reports "verified". Harmless while the only caller is `nas_archive` (which already rsynced successfully), **dangerous** as an unattended 4-hourly delete gate. Both gain `if getattr(result, "returncode", 0) != 0: return False`.

**Does the DB completeness query need hardening? No.** Its documented gap (a video vacuously "complete" because filtered turns were never materialized) is recoverable here: reclaim only deletes NAS-verified bytes, and a consumer that needs the video re-fetches it. Hardening the query would require a schema flag that does not exist. The *verify* primitive above is the gate that actually needed fixing.

### D5 — Reclaim verifies against `NAS_ARCHIVE_ROOT` only; legacy root stays pull-only

The deletion gate must answer "does the remote already hold every local byte", which is the **push**-direction dry run (`nas_archive.verify_synced`) — not `verify_fetched`, which answers the opposite question. Running a push-direction dry run against `NAS_FETCH_LEGACY_ROOT` would break the pull-only invariant and need a new root-parameterised command builder. Consequence: material fetched from the legacy root is not reclaimable until the next daily `nas_archive` run mirrors it into the archive root (≤24 h latency). Accepted: bounded, self-healing, and it keeps the legacy tree read-only.

Reclaim never pushes to the NAS; `nas_archive` keeps sole ownership of that direction.

### D6 — Partial-failure semantics (one rule for `nas_fetch` and every inline fetch)

- ≥1 item requested, **every** item failed → **raise** (`nas_fetch` run FAILS; consumer task FAILS). A tailnet outage lands here, which is the intended "fail loudly, never silently skip" behaviour.
- Some succeeded → task SUCCEEDS, failures logged at ERROR and returned in the summary's `failed` list.

Rejected alternative: classifying rsync/ssh exit codes (255, 12, …) to distinguish "NAS down" from "video missing" — brittle across rsync 3.1.2/3.2.x and ssh versions, and untestable without a live NAS.

### D6a — The two callers classify `ensure_local_video`'s statuses differently

`ensure_local_video` is non-raising for "nothing was fetched" outcomes because the inline path needs to keep degrading to a skip. The direct-trigger path must not inherit that leniency: an operator named an explicit `video_id`, and `_run_fetch_videos` classifies a video purely by whether `fetch_one_video` raised. **`summary["restored"]` must only ever contain videos whose material is local when the task ends.** `fetch_one_video` therefore converts every non-`fetched` status into an exception before returning:

| `ensure_local_video` result | Direct trigger (`fetch_one_video`) | Inline consumer |
|---|---|---|
| `{"status": "fetched"}` | return the summary dict | re-run the locator, then process |
| `{"status": "unavailable", "reason": "not_on_nas"}` | **raise `FileNotFoundError`** with today's exact message shape, so the existing catch tuple and per-video isolation in `_run_fetch_videos` are unchanged | today's `skipped_no_video` — **not** a failure |
| `{"status": "unavailable", "reason": "disabled"}` | **raise `NasFetchError`** (unreachable in practice — `check_enabled` short-circuits first — but never a silent success) | `skipped_no_video` |
| `{"status": "in_progress"}` | **raise `NasFetchError`** — a concurrent holder means this run restored nothing | `fetch_in_progress`, deferred to the next run — **not** a failure |
| raises `NasFetchError` (rsync/verify) | propagates; caught per video by `_run_fetch_videos` | `fetch_failed`, subject to the D6 all-failed rule |

Consequence, and the observable behaviour a RED test pins: triggering `nas_fetch` for a single `video_id` that exists on neither root FAILS the run (all requested failed), and triggering it for that id plus one restorable id SUCCEEDS with the absent id present in `failed` and absent from `restored`. The inline path's non-raising `unavailable` semantics are scoped to consumers only.

### D7 — Timeout and retry policy for consumers that now rsync multi-GB

| Knob | Value | Rationale |
|---|---|---|
| rsync/ssh subprocess timeout | `RSYNC_TIMEOUT_SECS = 3600` (moved to `modules/nas_fetch.py`, imported by the DAG) | Existing ceiling, single source of truth |
| `NAS_AUTOFETCH_MAX_PER_RUN` | 1 (module constant) | Bounds the added wall time of any task run to ≤1 h; further items report `in_progress`-style deferral and are picked up next run |
| `execution_timeout` on wired tasks | 6 h (`speaker_turns`, `trim_proposals`, `speaker_turn_videos`), 2 h (`reap_clip_preparer`, `speaker_turn_prepare`) | These tasks have **no** ceiling today; a hung ffmpeg or rsync holds the LocalExecutor slot forever. Generous ceilings are strictly safer than none and sit far above observed runtimes plus one 1 h fetch |
| `retries` | unchanged (1) | A retry re-runs the whole task and re-attempts the fetch; rsync `--partial` makes that idempotent but expensive. No increase |

## Data Flow

```
consumer task ──locator miss──> nas_fetch.ensure_local_video(video_id)
                                  │ lock busy ──> {"status": "in_progress"} (defer to next run)
                                  │ not on NAS ─> {"status": "unavailable"} (skipped_no_video)
                                  └─ lock held ─> marker | SSH discovery
                                                   → ensure_local_dir → rsync pull → verify_fetched
                                                   → refresh_retention → remove_marker
                                                   → {"status": "fetched"}  ──> re-run locator, process

nas_reclaim (every 4 h) ── complete_video_ids(DB) ── video_paths ── age ≥ grace ── lock free
      └─ per video, INSIDE lock: re-check gates → verify_synced(root) → prune_local → write_marker
```

## Interfaces / Contracts

```python
# congress_videos/modules/nas_fetch.py
class NasFetchError(RuntimeError): ...
class FetchLockBusy(RuntimeError): ...

@contextmanager
def fetch_lock(project_dir, channel_slug, video_id, *, now=None, stale_after=RSYNC_TIMEOUT_SECS + 600):
    """Atomic per-video lock at {channel_slug}/{video_id}/.nas_fetch.lock.
    Raises FetchLockBusy when held and not stale. Releases only on token match."""

def ensure_local_video(project_dir, channel_slug, video_id, settings, *, runner=None, now=None) -> dict:
    """Idempotent, concurrency-safe inline restore.
    Returns {"status": "fetched"|"in_progress"|"unavailable", "restored": [...], "source": ..., "reason": ...}.
    Raises NasFetchError on rsync/verify failure. Never touches the NAS copy.
    Non-`fetched` statuses are NOT self-classifying: callers map them per D6a —
    fetch_one_video re-raises them, consumers degrade to a skip/defer."""

# congress_videos/modules/nas_reclaim.py
def select_reclaim_candidates(settings, project_dir, channel_slug, complete_video_ids, *, now, batch) -> list[dict]
def reclaim_one_video(settings, project_dir, channel_slug, video_id, *, runner, now) -> dict
```

`ArchiveSettings` gains `reclaim_grace_hours: int = 12`, parsed from `NAS_RECLAIM_GRACE_HOURS`, validated `>= 0` like `min_age_days`.

Consumer status strings: `skipped_archived` is replaced by `fetched` (proceed), `fetch_failed`, `fetch_in_progress`. `speaker_turn_videos`' summary keeps its shape and gains the matching counters.

## File Changes and PR Slicing

Delivery units 1 and 4 exceed the 400-line budget and are pre-split. Estimates are authored additions + deletions.

| Slice | File | Action | ~Lines |
|---|---|---|---|
| **1a** (~145) | `congress_videos/modules/nas_archive.py` | Modify — `verify_synced` returncode gate | 5 |
| | `congress_videos/modules/nas_fetch.py` | Modify — `verify_fetched` returncode gate, `RSYNC_TIMEOUT_SECS` | 10 |
| | `congress_videos/nas_fetch_dag.py` | Modify — raise when all requested videos failed; import the constant | 20 |
| | `tests/congress_videos/test_nas_archive.py`, `test_nas_fetch.py` | Modify — RED: errored dry run is not "verified" | 45 |
| | `tests/congress_videos/test_nas_fetch_dag.py` | Modify — RED: all-failed raises, partial returns | 65 |
| **1b** (~375) | `congress_videos/modules/nas_fetch.py` | Modify — `NasFetchError`, `FetchLockBusy`, `fetch_lock`, `ensure_local_video` | 165 |
| | `congress_videos/nas_fetch_dag.py` | Modify — `fetch_one_video` delegates and applies the D6a status→exception mapping; catch tuple | 30 |
| | `tests/congress_videos/test_nas_fetch.py` | Modify — lock atomicity/stale/token, helper branches | 140 |
| | `tests/congress_videos/test_nas_fetch_dag.py` | Modify — **RED (D6a)**: `unavailable`/`in_progress` raise out of `fetch_one_video` and never enter `summary["restored"]`; single absent id fails the run; absent + restorable id succeeds with the absent id only in `failed` | 40 |
| **2** (~230) | `speaker_turns_dag.py`, `trim_proposals_dag.py`, `speaker_turn_videos_dag.py` | Modify — replace the `skipped_archived` branch, add `execution_timeout` | 80 |
| | `tests/.../test_speaker_turns*.py`, `test_trim_proposals*.py`, `test_speaker_turn_videos*.py` | Modify — RED per DAG: fetched / in_progress / unavailable / failed | 150 |
| **3** (~185) | `reap_clip_preparer_dag.py` | Modify — missing `output_path` → `ensure_local_video`, re-probe | 35 |
| | `speaker_turn_prepare_dag.py` | Modify — same hook before the decode check | 30 |
| | `tests/congress_videos/test_reap_clip_preparer*.py`, `test_speaker_turn_prepare*.py` | Modify — RED, both hooks | 120 |
| **4a** (~205) | `congress_videos/modules/nas_completeness.py` | Create — `complete_video_ids()` moved verbatim from `nas_archive_dag` | 65 |
| | `congress_videos/nas_archive_dag.py` | Modify — import the moved query | 55 |
| | `congress_videos/modules/nas_archive.py` | Modify — `reclaim_grace_hours` in `ArchiveSettings` | 15 |
| | `deploy/vps-dev/compose.yml` | Modify — `NAS_RECLAIM_GRACE_HOURS: ${NAS_RECLAIM_GRACE_HOURS:-12}` | 3 |
| | `deploy/vps-dev/test_contract.py` | Modify — one `assertEqual` in `test_nas_archive_mount_...` | 3 |
| | `deploy/vps-dev/README.md` | Modify — document the knob | 14 |
| | `tests/congress_videos/test_nas_archive.py` | Modify — settings parse/validate | 50 |
| **4b** (~320) | `congress_videos/modules/nas_reclaim.py` | Create — selection + `reclaim_one_video` | 145 |
| | `tests/congress_videos/test_nas_reclaim.py` | Create — gates, lock, `MIRROR_ONLY_DIRS`, unverified-skip | 175 |
| **4c** (~270) | `congress_videos/nas_reclaim_dag.py` | Create — `0 */4 * * *`, `max_active_runs=1`, paused on creation | 150 |
| | `tests/congress_videos/test_nas_reclaim_dag.py` | Create — DAG callables, batch cap, all-failed | 120 |

`NAS_RECLAIM_BATCH` (default 3) stays an operational `os.getenv` knob in the DAG, matching `NAS_ARCHIVE_BATCH` — deliberately **not** part of the compose env contract.

**`congress_videos/modules/vad_helpers.py` — no change** (proposal.md listed it as Modified). `_find_source_video` and `_find_source_video_any_date` stay pure locators returning `str | None`; the fetch hook belongs in the DAG callables, which already call the locator and branch on `None`, and only they can decide between skip, defer, and fail. Pushing the hook into the locators would give two pure functions an SSH/rsync side effect and an `ArchiveSettings` dependency they cannot obtain. The two DAGs wired in slice 3 do not use `vad_helpers` at all — they locate material through `turn["output_path"]` from the DB.

## Testing Strategy

| Layer | What | How |
|---|---|---|
| Unit (pure) | lock atomicity, stale break, token-scoped release, `ensure_local_video` branches, reclaim gates, verify hardening | `tmp_path` + injected runner returning `SimpleNamespace(stdout, stderr, returncode)`; never a real SSH/rsync |
| Unit (DAG callables) | consumer hook branches, all-failed raise, batch caps | `importlib` DAG-module import + `monkeypatch.setattr` on `_subprocess_runner`, exactly as `test_nas_fetch_dag.py` does today |
| Concurrency | two sequential `fetch_lock` acquisitions; reclaim skips a locked video; reclaim skips a tree whose mtime is inside the grace window | Deterministic, single-process, injected `now` |
| E2E | DAG import errors | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) |

Strict TDD (`strict_tdd: true`): RED test before each production change, `uv run pytest` green per slice.

## Threat Matrix

Applicable boundary: subprocess/shell (rsync + ssh) and unattended filesystem deletion.

| Boundary | Applicability | Design response | Planned RED test |
|---|---|---|---|
| Remote shell argument injection (`video_id`, `channel_slug`, `source_root`) | Applicable | Unchanged: `validate_video_id` + `_validate_channel_slug` + `shlex.quote` already gate every interpolation; `ensure_local_video` calls them before any command is built | Malformed `video_id`/`channel_slug` raises `ValueError` before any runner call |
| Unattended deletion outside the project tree | Applicable | `prune_local`'s `_validate_prunable` + `_PROTECTED_TOP_LEVEL_NAMES` (already covers `MIRROR_ONLY_DIRS`, `assets`, `youtube_tokens`); reclaim reuses it unchanged and never builds paths itself | Reclaim refuses `thumbnails/`, `downloads/` root, and `downloads/{date}` |
| Delete gate false positive on a failed probe | Applicable | D4 returncode hardening | Errored dry run ⇒ `verify_synced` is `False` ⇒ no prune |
| Lock forged/stolen by another process | Applicable | uuid4 token compared before unlink; stale break only past `RSYNC_TIMEOUT_SECS + 600` | Foreign token ⇒ release is a no-op |
| Documentation-like paths | N/A — no file-type classification or execution of repository content |
| Git repository selection / commit state / push state / PR commands | N/A — no VCS or PR automation in this change |

## Migration / Rollout

No schema or NAS-side migration. `nas_reclaim` ships `is_paused_upon_creation=True`; enable it only after `nas_fetch`'s outcome fix is deployed. Rollback is per slice (revert + redeploy); pausing `nas_reclaim` stops all deletion with no data loss, and reverting the consumer wiring restores the previous skip behaviour.

## Open Questions

- None blocking. Accepted with evidence: ≤24 h reclaim latency for legacy-root material (D5), and re-fetch churn for videos consumed by several DAGs more than `NAS_RECLAIM_GRACE_HOURS` apart.
