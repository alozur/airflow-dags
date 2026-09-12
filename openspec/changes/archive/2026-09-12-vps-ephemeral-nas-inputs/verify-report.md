```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:7f5d423f9de5271080224b47c56fe10ff79414deda49cf46e3426e5d6cc9424f
verdict: pass_with_warnings
blockers: 0
critical_findings: 0
requirements: 12/12
scenarios: 23/23
test_command: uv run pytest -n auto -q -p no:cacheprovider --no-cov
test_exit_code: 0
test_output_hash: sha256:2fe0449b541738f8694eceee89cba02d5ff7634711b759b875f831bc29b82654
build_command: uv run ruff check . && uv run ruff format --check .
build_exit_code: 0
build_output_hash: sha256:ac5aa9298809c9a55d08ace765a88b34f14c2e7ab8c37e2def346504355e9002
```

## Verification Report

**Change**: vps-ephemeral-nas-inputs
**Version**: N/A (three delta spec domains: nas-fetch-outcome, nas-auto-fetch, nas-reclaim)
**Mode**: Strict TDD

Merged into `dev` as 11 squash PRs (#626–#636). This worktree's HEAD (`3ce1da5`) equals `origin/dev` tip; pre-change base is `01f246b`. All commands below ran on this exact tree.

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total | 70 |
| Tasks complete | 65 |
| Tasks incomplete | 5 (Phase 8, slice-independent verification — see below) |

Phase 8 disposition (not counted as CRITICAL, per explicit scope: these require the live VPS/NAS):
- **8.1** (`deploy/vps-dev/test_contract.py` full run) — independently executed by this verify pass: `25 passed`. Functionally satisfied; tasks.md checkbox not yet flipped (hygiene gap, SUGGESTION).
- **8.2** (DAG import check + e2e gate) — independently executed by this verify pass: all 8 touched DAG modules import cleanly; `bash scripts/test-airflow-e2e.sh` reported `unavailable` (Docker daemon unreachable on this host, exit 4 — not a failure). Functionally satisfied modulo the Docker-dependent e2e leg; tasks.md checkbox not yet flipped (SUGGESTION).
- **8.3 / 8.4 / 8.5** — `pending-live`, cannot run from this environment. Exact operator commands:
  - 8.3: trigger `nas_fetch` via Airflow UI/CLI with `{"video_id": "<known-nas-only-id>"}`; confirm the run succeeds and that id appears under `restored`.
  - 8.4: temporarily set `NAS_RECLAIM_GRACE_HOURS` to a short value in `deploy/vps-dev/compose.yml`, redeploy, wait out the shortened window, then trigger `nas_reclaim`; confirm the 8.3 video's local material is pruned and `.nas_archived.json` is kept.
  - 8.5: revert the `NAS_RECLAIM_GRACE_HOURS` override and redeploy.

### Build & Tests Execution

**Build** (lint/format): PASSED
```text
$ uv run ruff check .
All checks passed!
$ uv run ruff format --check .
346 files already formatted
```

**Tests**: PASSED
```text
$ uv run pytest -n auto -q -p no:cacheprovider --no-cov
5723 passed, 36 skipped in 33.40s
(skips: pre-existing Postgres-dependent live tests, environmental, unrelated to this change)

$ uv run pytest tests/congress_videos/ -q -k "nas_ or reclaim or speaker_turn or trim_proposals or reap_clip" -p no:cacheprovider --no-cov
831 passed, 7 skipped, 3977 deselected in 25.07s

$ uv run pytest deploy/vps-dev/test_contract.py -q --no-cov
25 passed in 2.46s

$ uv run python -c "import congress_videos.nas_reclaim_dag, congress_videos.nas_fetch_dag, congress_videos.nas_archive_dag, congress_videos.speaker_turns_dag, congress_videos.trim_proposals_dag, congress_videos.speaker_turn_videos_dag, congress_videos.reap_clip_preparer_dag, congress_videos.speaker_turn_prepare_dag"
OK: all 8 DAG modules imported cleanly (only pre-existing unrelated Airflow RemovedInAirflow3Warning noise)

$ uv run pytest tests/congress_videos/test_dag_id_registration.py -q --no-cov   # DagBag-based check, covers 3/8 touched DAGs directly
4 passed in 4.63s

$ bash scripts/dag-paths-changed.sh   # gate against default origin/dev base
exit 1 (skip) — expected: HEAD IS origin/dev in this worktree (change already merged), so there is
no diff to gate on against that base.
$ E2E_DIFF_BASE=01f246b bash scripts/dag-paths-changed.sh   # gate against true pre-change base
exit 0 (run) — confirms this change does touch congress_videos/** and the e2e leg is applicable.

$ bash scripts/test-airflow-e2e.sh
[test-airflow-e2e] Docker daemon is not reachable (docker info failed); skipping e2e (unavailable).
exit 4 — unavailable, not failed, per openspec/config.yaml's documented unavailable_exit_code contract.
```

**Coverage** (new/changed modules, targeted run — 237 passed):
| Module | Line Cover | Branch | Missing |
|---|---|---|---|
| `congress_videos/modules/nas_fetch.py` | 95.31% | 60 br / 3 partial | 385, 517-518, 524, 527-528, 574-575, 596, 602 |
| `congress_videos/modules/nas_archive.py` | 94.79% | 54 br / 4 partial | 229->227, 410, 416, 461-463 |
| `congress_videos/modules/nas_reclaim.py` | 94.85% | 26 br / 3 partial | 74->70, 83, 169 |
| `congress_videos/modules/nas_completeness.py` | 100.00% | — | — |
| `congress_videos/nas_fetch_dag.py` | 97.12% | 22 br / 1 partial | 113, 173 |
| `congress_videos/nas_archive_dag.py` | 83.20% | 62 br / 4 partial | 101-110, 145-146, 195-198, 207, 235->234, 237-241, 275 |
| `congress_videos/nas_reclaim_dag.py` | 98.63% | 6 br / 0 partial | 118 |

All above the 80% threshold; no coverage gate configured in `openspec/config.yaml` (`coverage_threshold` unset → informational only).

### Spec Compliance Matrix

**Domain: nas-fetch-outcome**

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| All-failed run fails the task | Single requested video fails | `test_nas_fetch_dag.py::TestRunFetchVideosD6aIntegration::test_single_absent_video_id_fails_the_run` | ✅ COMPLIANT |
| All-failed run fails the task | All videos in a batch fail | `test_nas_fetch_dag.py::TestRunFetchVideos::test_all_videos_in_batch_failing_raises_and_lists_every_id` | ✅ COMPLIANT |
| Partial failure stays visible | Mixed batch with one failure | `test_nas_fetch_dag.py::TestRunFetchVideos::test_mixed_batch_surfaces_failure_without_discarding_success` + `::test_one_failure_does_not_block_the_other_video` | ✅ COMPLIANT |
| Empty request is a no-op success | No video_id/video_ids provided | `test_nas_fetch_dag.py::TestRunFetchVideos::test_raises_when_conf_has_no_video_ids` + `::test_empty_conf_is_distinct_failure_from_all_failed_fetch` | ✅ COMPLIANT |
| Empty request is a no-op success | Every video already fetched | `test_nas_fetch_dag.py::TestRunFetchVideosD6aIntegration::test_absent_plus_restorable_succeeds_with_absent_only_in_failed` + `TestFetchOneVideo::test_happy_path_fetches_verifies_refreshes_and_removes_marker` | ✅ COMPLIANT |

**Domain: nas-auto-fetch**

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Inline fetch on missing local source | NAS-aware consumer finds source archived | `test_speaker_turns_dag.py::test_missing_source_video_fetched_from_nas_proceeds` (+ equivalent in `test_trim_proposals_dag.py`, `test_speaker_turn_videos_dag.py`) | ✅ COMPLIANT |
| Inline fetch on missing local source | Previously NAS-unaware consumer finds source archived | `test_reap_clip_preparer_dag.py::test_missing_source_calls_nas_fetch_before_probe` + `test_speaker_turn_prepare_dag.py::test_missing_source_calls_nas_fetch_before_decode_check` | ✅ COMPLIANT |
| NAS-missing preserves existing behavior | Video absent everywhere (NAS-aware) | `test_speaker_turns_dag.py::test_missing_source_video_degrades_per_fetch_outcome` (parametrized `unavailable/not_on_nas` → `skipped_no_video`) | ✅ COMPLIANT — see Deviation D-1 below (spec scenario text says `skipped_archived`; design D6a + code use `skipped_no_video`) |
| NAS-missing preserves existing behavior | Video absent everywhere (previously unaware) | `test_reap_clip_preparer_dag.py::test_nas_fetch_failure_preserves_block_contract_without_crashing` + `test_speaker_turn_prepare_dag.py::test_nas_fetch_failure_preserves_swallow_contract_without_raising` | ✅ COMPLIANT |
| Idempotent, concurrency-safe fetch | Two consumers need the same video concurrently | `test_nas_fetch.py::TestFetchLock::test_contended_lock_raises_busy_without_blocking` + `TestEnsureLocalVideo::test_in_progress_when_another_holder_already_has_the_lock` | ✅ COMPLIANT |
| Idempotent, concurrency-safe fetch | Fetch already completed before a second consumer starts | `test_nas_fetch.py::TestEnsureLocalVideo::test_fetched_via_marker_refreshes_retention_and_removes_marker` | ✅ COMPLIANT |
| Retention refresh on auto-fetch | Auto-fetched video gets a fresh retention window | `test_nas_fetch.py::TestEnsureLocalVideo::test_fetched_via_marker_refreshes_retention_and_removes_marker` (calls `refresh_retention`) + `TestRefreshRetention::test_bumps_media_file_mtime_under_a_directory` | ✅ COMPLIANT |

**Domain: nas-reclaim**

| Requirement | Scenario | Test | Result |
|---|---|---|---|
| Three-gate deletion | All three gates pass — happy path | `test_nas_reclaim.py::TestSelectReclaimCandidates::test_all_gates_pass_selects_the_video` + `TestReclaimOneVideo::test_all_gates_pass_deletes_media_and_writes_a_marker` | ✅ COMPLIANT |
| Three-gate deletion | Blocked by pending DB work | `test_nas_reclaim.py::TestSelectReclaimCandidates::test_excluded_when_video_id_not_in_complete_video_ids` | ✅ COMPLIANT (selection-time only — see Deviation D-2) |
| Three-gate deletion | Blocked by grace window | `test_nas_reclaim.py::TestSelectReclaimCandidates::test_excluded_within_the_grace_window` + `TestReclaimOneVideo::test_blocked_by_grace_window_inside_the_lock` | ✅ COMPLIANT |
| Three-gate deletion | Blocked by unverified NAS copy | `test_nas_reclaim.py::TestReclaimOneVideo::test_blocked_when_nas_verification_fails` | ✅ COMPLIANT |
| Protected/non-media content never touched | Thumbnails untouched | `test_nas_reclaim.py::TestReclaimOneVideo::test_thumbnails_and_sidecar_files_are_never_touched` | ✅ COMPLIANT |
| Protected/non-media content never touched | Sidecar files preserved | `test_nas_reclaim.py::TestReclaimOneVideo::test_thumbnails_and_sidecar_files_are_never_touched` (same test, combined assertion) | ✅ COMPLIANT |
| In-flight fetch is never reclaimed | Reclaim runs during an active fetch | `test_nas_reclaim.py::TestSelectReclaimCandidates::test_excluded_while_the_fetch_lock_is_held` + `TestReclaimOneVideo::test_skipped_when_the_fetch_lock_is_already_held` | ✅ COMPLIANT |
| Bounded run size | More eligible videos than the run cap | `test_nas_reclaim.py::TestSelectReclaimCandidates::test_batch_cap_limits_candidates_per_run` + `test_nas_reclaim_dag.py::TestRunSelectCandidates::test_wires_db_pool_into_select_reclaim_candidates` | ✅ COMPLIANT |
| Schedule and environment contract | Scheduled cadence | `test_nas_reclaim_dag.py::TestDagLoads::test_schedule_is_every_four_hours` | ✅ COMPLIANT |
| Schedule and environment contract | Grace hours configurable via environment | `test_nas_archive.py::test_reclaim_grace_hours_parses_from_env` + `deploy/vps-dev/test_contract.py::test_nas_archive_mount_...` (`NAS_RECLAIM_GRACE_HOURS` assertion, line 148) | ✅ COMPLIANT |
| Schedule and environment contract | Default grace hours when unset | `test_nas_archive.py::test_reclaim_grace_hours_defaults_to_12` | ✅ COMPLIANT |

**Compliance summary**: 23/23 scenarios compliant (2 carry documented deviations noted below — both are non-blocking, evidence-backed tradeoffs, not gaps).

### Correctness (Static Evidence)
| Requirement | Status | Notes |
|---|---|---|
| `verify_synced`/`verify_fetched` fail closed on non-zero returncode (D4) | ✅ Implemented | `nas_archive.py`/`nas_fetch.py`, tested |
| `fetch_lock` atomic `O_CREAT\|O_EXCL`, stale-break at `RSYNC_TIMEOUT_SECS+600`, token-scoped release (D2) | ✅ Implemented | tested incl. path-injection defence-in-depth |
| `ensure_local_video` single fetch path, D6a status→exception mapping (D1/D6a) | ✅ Implemented | tested via real-path integration tests, not fakes |
| 5 consumer DAGs wired to inline fetch, `execution_timeout` per D7 | ✅ Implemented | all 5 DAGs verified in source + tests |
| `nas_reclaim` four-gate deletion (D3) | ✅ Implemented | gate 4 (NAS verify) always re-checked in-lock; gate 3 (DB completeness) is selection-time only — see D-2 |
| `NAS_RECLAIM_GRACE_HOURS` env contract | ✅ Implemented | compose.yml + test_contract.py + ArchiveSettings |
| `nas_reclaim` DAG schedule/pause/batch cap | ✅ Implemented | `0 */4 * * *`, `max_active_runs=1`, `is_paused_upon_creation=True` |
| Path-injection hardening on `video_id`/`channel_slug` in the new lock/marker code paths | ✅ Implemented | remediated post-independent-review (HIGH finding), now tested |
| Malformed `video_id` no longer crashes the `speaker_turn_videos` batch | ✅ Implemented | remediated post-independent-review (HIGH finding), now tested |
| Per-candidate isolation in `nas_archive_dag._run_archive_videos` | ✅ Implemented | added by verifier direction during slice 1a, tested |

### Coherence (Design)
| Decision | Followed? | Notes |
|---|---|---|
| D1 (shared helper in `modules/nas_fetch.py`, `NasFetchError`) | ✅ Yes | |
| D2 (lock file design) | ✅ Yes | |
| D3 (reclaim's four gates re-evaluated inside the lock) | ⚠️ Partially | Gate 3 (DB completeness) is NOT re-queried inside the lock — selection-time only. Documented, evidence-backed rationale (mirrors D4's own "recoverable, self-healing" reasoning: reclaim only deletes NAS-verified bytes; a consumer that needs the video re-fetches it). Design.md's own D3 prose ("Gates 1–4 are re-evaluated INSIDE the lock") is now inaccurate and should be corrected at archive time. |
| D4 (returncode hardening) | ✅ Yes | |
| D5 (reclaim verifies against archive root only) | ✅ Yes | |
| D6 (partial-failure semantics) | ✅ Yes, with a documented scope reading for inline consumers (whole-batch, not narrower "items that needed a fetch") — reasonable per D6's own intent and the Interfaces contract not carrying the extra signal needed for the narrower reading | |
| D6a (status→exception mapping table) | ✅ Yes | |
| D7 (timeouts) | ✅ Yes | |
| File Changes/PR Slicing table | ⚠️ Partially | Several slices exceeded their per-PR budget and were split into stacked commits (documented in apply-progress, no `size:exception` available); final trees are byte-identical to the pre-split single commits, zero behaviour change |

### Issues Found

**CRITICAL**: None

**WARNING**:
1. **D-1 (spec/design/code text mismatch)**: `specs/nas-auto-fetch/spec.md`'s "Video absent everywhere (NAS-aware consumer)" scenario text and `tasks.md` task 3.1 both say the DAG "stays `skipped_archived`" for a NAS-aware consumer when the video is missing everywhere. This contradicts design.md's own D6a table and the actual shipped code, both of which use `skipped_no_video` for that case (`skipped_archived` is retired entirely — it meant "found via `is_archived_elsewhere`", the opposite of "absent"). The implementation follows the design/code contract correctly and is behaviourally unchanged from pre-existing behaviour, satisfying the requirement's actual prose ("preserved unchanged"). The spec's scenario text and task 3.1's wording should be corrected at archive time so future readers don't see a contradictory literal string.
2. **D-2 (design.md D3 prose inaccuracy)**: design.md states "Gates 1–4 are re-evaluated INSIDE the lock immediately before `prune_local`" but the shipped `reclaim_one_video` only re-checks gates 1 (lock), 2 (grace), and 4 (NAS verify) inside the lock — gate 3 (DB completeness) is selection-time only, per `modules/nas_reclaim.py`'s own module docstring and task 6.9's documented deviation. The tradeoff is evidence-backed and mirrors D4's own accepted-risk reasoning (worst case is re-fetch churn, not data loss, since gate 4 always re-verifies NAS presence immediately before deletion), but design.md's D3 prose should be corrected to state this explicitly rather than claiming all four gates are re-evaluated.
3. **Review-budget overages, resolved via commit splits, not PR count**: slices 1b, 2 (superseded), and 4c each initially exceeded the ≤390/≤400 per-commit budget; each was resolved by a coordinator-directed, verified-byte-identical commit split rather than a `size:exception`. This is process-compliant per apply-progress's own evidence but means several "PRs" in the original 7-PR plan are now stacked multi-commit branches — worth confirming the actual GitHub PR structure matches intent before archive (the 11 merged PRs listed in this task's inputs suggest it does).
4. **Phase 8 task-checkbox hygiene**: 8.1 and 8.2 are functionally satisfied by this verify run's own command evidence (`deploy/vps-dev/test_contract.py` full suite passes; all 8 DAG modules import cleanly) but remain unchecked in `tasks.md`. Low-risk, cosmetic.

**SUGGESTION**:
1. No dedicated DagBag-based import-error test covers all 8 touched DAG modules in one place — `test_dag_id_registration.py` only covers the 3 turn-train files (for a different, pre-existing regression reason). The plain `python -c "import ..."` check plus the existing per-DAG `TestDagLoads` classes in each DAG's own test file already give equivalent coverage; a single consolidated `DagBag(dag_folder=...)` smoke test would be a nice-to-have for future changes but is not a gap in this change's spec compliance.

### Verdict

**PASS WITH WARNINGS**

All 12 requirements / 23 scenarios across the three delta specs are behaviorally compliant, backed by passing tests executed in this session (full suite: 5723 passed, 36 pre-existing/environmental skips; targeted nas_*/consumer suite: 831 passed; deploy contract: 25 passed; ruff clean; all 8 touched DAGs import cleanly; e2e Docker leg correctly reports `unavailable` on this host). Two independent-reviewer-found HIGH regressions (path-injection in the lock/marker code, and an unvalidated `ValueError` crashing the `speaker_turn_videos` batch) were caught and remediated with dedicated RED→GREEN tests, confirmed present in this tree. No CRITICAL findings. Warnings are documentation/design-text corrections owed at archive time (spec/design wording vs. the actually-shipped and tested contract) plus two cosmetic task-checkbox gaps — none block archive, but D-1 and D-2 should be folded into the specs/design during `sdd-archive` so the merged main specs don't carry a stale contradiction.
