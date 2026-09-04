# Archive Report: srt-artifacts-people-topics

**Change**: srt-artifacts-people-topics (issues #431, #432)
**Archived**: 2026-09-04
**Archived to**: `openspec/changes/archive/2026-09-04-srt-artifacts-people-topics/`
**Mode**: openspec, repo-local (worktree `/home/alozur/src/github.com/alozur/airflow-dags-wt-431`)
**Archive branch**: `chore/sdd-archive-srt-artifacts-people-topics` (based on `origin/dev`, cherry-picked `a7caf12` -> `ead63de`)

## Final State (authoritative — see Final-State Authority hierarchy)

This report describes the state of the change AT CLOSE, per the orchestrator's explicit
final-state facts, which outrank the intermediate `apply-progress.md` and `verify-report.md`
snapshots read below.

### Delivery — five PRs merged to `dev`, all with `--merge`

| PR | Branch | Merge commit | Scope | Notes |
|----|--------|---------------|-------|-------|
| #447 | `feat/431-a-short-srt-sidecar` | `838d353` | paths helper + `write_short_srt_sidecar` | `size:exception`, 474 lines |
| #448 | `feat/431-b-reap-sensor-srt-hook` | `7e3037c` | `get_chapter_srt_context` + `ReapJobSensor.poke` hook + `docs/PIPELINE.md` shorts note | Closes #431 |
| #449 | `feat/432-a-mentioned-people-migration` | `0b693d4` | migration 045 + `production_schema.sql` mirror + drift test + `resolve_mentioned_people` | `size:exception`, 597 lines |
| #453 | `feat/432-b-topics-upload-hook` | `c763363` | `extract_topics` + `_analyze_chapter_content` + `update_chapter_content_analysis` + dev schema mirror + `docs/ARCHITECTURE.md` + `TestChapterAndShortSidecarsCoexist` remediation test (`ce9c14c`) | `size:exception`, 681 lines; Closes #432 |
| #457 | `docs/431-432-deferred-docs` | `ffb818f` | tasks 1.27 and 3.29 deferred docs edits | `docs/ARCHITECTURE.md` NAS layout + `docs/PIPELINE.md` upload flow |

As a result, tasks **1.27** and **3.29** — unchecked at verification time per the second
`verify-report.md` (`docs/ARCHITECTURE.md` NAS layout, `docs/PIPELINE.md` turn/upload flow) — are
now **DONE**, delivered in PR #457. Task **2.4** (dev schema mirror, `youtube_chapters_schema.sql`)
was completed in the PR3/#453 batch, matching its `[x]` state already recorded in `tasks.md` at
verify time.

`tasks.md` in this archive has been updated to mark 1.27 and 3.29 `[x]` with a provenance note
(PR #457, commit `ffb818f`) before the archive move, per the Task Completion Gate's exceptional
reconciliation clause — the reconciliation reason is this launch's explicit final-state fact
(five PRs merged, including #457) that supersedes the `verify-report.md` snapshot's "unchecked,
deferred to release PR" claim.

### Verification — PASS WITH WARNINGS (second report)

Per `verify-report.md` (this change's second verification, after remediating the single CRITICAL
finding of the first report):

- 21/21 requirements, 31/31 scenarios COMPLIANT (29 direct runtime tests, 2 static evidence:
  cache-key distinctness by construction, one DDL comment not pytest-checkable).
- `uv run pytest -n auto -q`: 4496 passed, 0 failed, 27 skipped (pre-existing live-Postgres skips,
  unrelated to this change).
- `uv run ruff check . && uv run ruff format --check .`: clean, byte-identical output hash to the
  first report.
- Both touched DAG modules (`reap_processor_dag.py`, `youtube_upload_dag.py`) import cleanly.
- E2E smoke test (`scripts/test-airflow-e2e.sh`): reported **unavailable** — Docker daemon not
  reachable in the verification environment. Per this change's launch-prompt final-state facts,
  this is **still to be confirmed on the NAS** via `dags list-import-errors` after `git_sync_dag`
  and the migration rollout — it is a Phase 4 delivery-verification step, not a resolved item.
- 0 CRITICAL findings. 4 WARNING items (see below), 0 SUGGESTION items.

### Task Completion Gate

All implementation tasks (Phase 1: 1.1-1.32, Phase 2: 2.1-2.26, Phase 3: 3.1-3.33) are `[x]`,
including 1.27 and 3.29 after this archive's reconciliation. **Phase 4 (4.1-4.6) remains
unchecked** — these are explicitly delivery/orchestrator-owned tasks (conventional-commit
bookkeeping, PR-body wording, migration rollout on live Postgres, filing the F2 follow-up issue,
and the release PR), not implementation tasks. `verify-report.md` itself classifies them this way
("explicitly orchestrator/delivery-phase actions... not apply-phase actions") and recommends
`sdd-archive` as the next step despite them being open. The orchestrator's launch-prompt
final-state facts confirm these remain pending after this archive and are tracked separately
(Phase 4 is orchestrator-owned). Archiving proceeds with Phase 4 explicitly recorded as pending
below — this is not a stale-checkbox reconciliation of implementation work, it is the documented
scope boundary between `sdd-apply`/`sdd-verify` (implementation) and post-archive delivery
(release, migration rollout, follow-up issue).

## Specs Synced

No `openspec/specs/` main-spec directory existed in this repository before this archive (confirmed
by filesystem check and by the proposal's own statement: "Modified Capabilities: None (no
`openspec/specs/` source-of-truth files exist yet)"). All three delta specs are full ADDED-only
specs, not deltas against an existing main spec. Each was mechanically copied (`cp` to a temp file
in the target directory, `diff -r` readback, then `mv` into place — never Read/Write) into a new
`openspec/specs/{domain}/spec.md`:

| Domain | Action | Requirements | Readback diff |
|--------|--------|---------------|----------------|
| `short-video-srt-artifacts` | Created | 6 requirements / 11 scenarios | empty (exit 0) |
| `chapter-mentioned-people` | Created | 8 requirements / 11 scenarios | empty (exit 0) |
| `chapter-topic-extraction` | Created | 7 requirements / 9 scenarios | empty (exit 0) |

### Verbatim `diff -r` output — spec sync (Step 2)

```
diff status for short-video-srt-artifacts: 0
OK: short-video-srt-artifacts -> openspec/specs/short-video-srt-artifacts/spec.md
diff status for chapter-mentioned-people: 0
OK: chapter-mentioned-people -> openspec/specs/chapter-mentioned-people/spec.md
diff status for chapter-topic-extraction: 0
OK: chapter-topic-extraction -> openspec/specs/chapter-topic-extraction/spec.md
```

Independent readback (source change-folder spec vs. new main spec, after the copy):

```
=== readback diff short-video-srt-artifacts ===
exit=0
=== readback diff chapter-mentioned-people ===
exit=0
=== readback diff chapter-topic-extraction ===
exit=0
```

## Archive Move (Step 3)

`git mv openspec/changes/srt-artifacts-people-topics openspec/changes/archive/2026-09-04-srt-artifacts-people-topics`
succeeded. Snapshot-vs-destination readback (mandatory, excludes this additive `archive-report.md`
which did not exist in the source):

```
git mv succeeded
=== final readback diff ===
diff exit=0
```

Empty diff confirms byte-identical archive of all 9 pre-existing artifacts (`proposal.md`,
`exploration.md`, `design.md`, `specs/*/spec.md` x3, `tasks.md`, `apply-progress.md`,
`verify-report.md`).

## Archive Contents

- `proposal.md` — present
- `exploration.md` — present
- `design.md` — present
- `specs/short-video-srt-artifacts/spec.md` — present
- `specs/chapter-mentioned-people/spec.md` — present
- `specs/chapter-topic-extraction/spec.md` — present
- `tasks.md` — present, 88/91 tasks complete pre-archive → 90/91 after this archive's 1.27/3.29
  reconciliation (91 = 85 implementation tasks + 6 Phase-4 delivery tasks; the remaining 1 unchecked
  line group is Phase 4, explicitly pending per above)
- `apply-progress.md` — present
- `verify-report.md` — present (second/final report, PASS WITH WARNINGS)
- `archive-report.md` — this file, additive

## Source of Truth Updated

- `openspec/specs/short-video-srt-artifacts/spec.md`
- `openspec/specs/chapter-mentioned-people/spec.md`
- `openspec/specs/chapter-topic-extraction/spec.md`

## SDD Ledger

This change ran through `gentle-ai sdd-attempt` with three resets (two budget overruns, one
rebase mid-objective) and one remediation attempt (the CRITICAL finding from the first
`verify-report.md`, closed by commit `ce9c14c`'s `TestChapterAndShortSidecarsCoexist`). Final
ledger state: `complete`.

## Still Pending After This Archive (Phase 4 — orchestrator-owned, not blocking archive)

1. **Release PR `dev` -> `main`** — not yet opened. Carries the migration-045 rollout confirmation
   and this change's cumulative diff.
2. **Migration 045 rollout** — `video_chapters.mentioned_participant_slugs TEXT[]` must be applied
   on both NAS stacks via the `run_migrations` DAG after `git_sync_dag`, confirmed against the
   live schema (`information_schema` is blind to the `airflow` role on this repo's Postgres setup;
   use `pg_attribute`).
3. **Import-error check on both NAS stacks** — `dags list-import-errors` after the git sync, in
   place of the environment's `unavailable` local e2e smoke test.
4. **F2 follow-up issue** — pre-existing defect in `reap_clip_preparer_dag.py`: `pretrim_start_secs`
   / `pretrim_end_secs` carry absolute source-video seconds when `pretrim_used_srt=TRUE` but
   chapter-relative seconds when `FALSE`, while ffmpeg always applies them chapter-relative. This
   is contained (not silent) by this change's D3 validity guard (WARNING + fallback to full
   chapter span), but the issue itself could not be filed during this cycle because
   `.github/ISSUE_TEMPLATE/feature.yml` is not committed to any branch — confirmed present only as
   an untracked file (`?? .github/ISSUE_TEMPLATE/`) in the working tree's `git status` at session
   start, not on `origin/dev` or `origin/main`.

None of the above blocks this archive: they are release/rollout/follow-up-tracking actions
outside `sdd-apply`/`sdd-verify` scope, and the second `verify-report.md` explicitly recommends
`sdd-archive` as the next step with these same items open.

## Contradictions Recorded

None. All facts in this report either come directly from `verify-report.md`/`tasks.md` at the time
they were written (attributed as snapshots above) or from the orchestrator's launch-prompt
final-state facts, which corroborate rather than contradict the snapshots — they describe work
completed after the snapshots were persisted (PR #457 shipping after the second verify report).

## Artifacts Read (traceability)

All read via mechanical filesystem access in the openspec worktree
`/home/alozur/src/github.com/alozur/airflow-dags-wt-431` (openspec mode — no Engram observation
IDs apply):

- `openspec/changes/srt-artifacts-people-topics/proposal.md`
- `openspec/changes/srt-artifacts-people-topics/specs/short-video-srt-artifacts/spec.md`
- `openspec/changes/srt-artifacts-people-topics/specs/chapter-mentioned-people/spec.md`
- `openspec/changes/srt-artifacts-people-topics/specs/chapter-topic-extraction/spec.md`
- `openspec/changes/srt-artifacts-people-topics/design.md`
- `openspec/changes/srt-artifacts-people-topics/tasks.md`
- `openspec/changes/srt-artifacts-people-topics/verify-report.md`

Also mirrored to Engram: `sdd/srt-artifacts-people-topics/archive-report` (topic_key, project
`airflow-dags`, type `architecture`), per the hybrid persistence contract in
`skills/_shared/sdd-phase-common.md` Section C.

## SDD Cycle Complete

The change has been fully explored, proposed, spec'd, designed, task-planned, implemented (5 PRs
merged to `dev`), verified (PASS WITH WARNINGS, 0 CRITICAL), and archived. Release to `main`,
migration rollout, NAS import-error confirmation, and the F2 follow-up issue remain as
orchestrator-owned Phase 4 delivery steps, tracked above.
