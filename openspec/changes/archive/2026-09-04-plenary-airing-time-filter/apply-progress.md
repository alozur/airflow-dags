# Apply Progress: Filter plenary sessions by airing time, not publish time

**Change**: `plenary-airing-time-filter` (issue #426)
**Mode**: Strict TDD
**Status**: All 19 tasks (6 phases) complete (19/19). `tasks.md` fully `[x]`.

## Delivery: re-sliced into 3 stacked branches

The original single-PR plan (568 authored lines) and its two-PR fallback (PR 2 alone ≈524 lines)
both exceeded the 400-line review budget. Per the orchestrator's `auto-chain` delivery decision,
the work was rebased onto `origin/dev` (`0b693d4`) and re-sliced into three stacked branches in the
same worktree (`chain_strategy: stacked-to-main`), each branched from the previous:

| Branch | Commits (in order) | Measured vs parent (excl. `openspec/`) | Targeted tests |
|---|---|---|---|
| `fix/426-a-airing-helpers` | `5a4167a` chore(sdd): plan · `44ef6ac` refactor(youtube): lift the batched videos.list call into a shared helper · `afbf6b9` feat(youtube): add airing-timestamp helpers for plenary matching | vs `origin/dev`: **182 lines** (179+/3−) | 61 passed |
| `fix/426-b-airing-predicate` | + `802bbd4` fix(youtube-monitor): match plenary sessions on airing time, not publishedAt | vs slice A: **301 lines** (268+/33−) | 65 passed |
| `fix/426-c-airing-scenario-tests` | + `5232cb7` test(youtube-monitor): cover airing-time edge cases and observability · + this bookkeeping commit | vs slice B: **191 lines** (191+/0−) | 71 passed |

Full suite (run once, on slice C, cumulative): `uv run pytest -n auto -q` → **4495 passed, 27
skipped** (Postgres-dependent live tests, unavailable in this environment — expected), **90.18%**
coverage. `uv run ruff check` / `ruff format --check` clean on all three slices.
`PYTHONPATH=. uv run python congress_videos/youtube_channel_monitor_dag.py` confirmed the DAG still
imports after slice B's one-line comment change.

No branch was pushed and no PR was opened by `sdd-apply` — that delivery action belongs to the
orchestrator. The worktree is left checked out on `fix/426-c-airing-scenario-tests`.

## What moved where (mapping to the original single-PR plan)

- **Slice A** = the original Phase 1 (shared fetch helper) **plus** the pure, still-unwired
  `_airing_timestamp`/`_airing_date` helpers from the original Phase 2, now with **direct** unit
  tests (`TestAiringTimestampHelpers`, 10 tests) instead of being exercised only through the public
  seam — this is an explicit, coordinator-requested deviation from design's "private helpers are
  exercised through it, no direct tests" statement, scoped to make this helpers-only slice
  independently reviewable and testable before any predicate wiring exists.
- **Slice B** = the original Phase 2's wiring step + all of Phase 3 (re-mock the 9 existing tests +
  cutover) + the docstring/DAG doc tasks from Phase 5, plus 4 of the original Phase 4 tests
  (regression, both boundaries, exactly-one-call).
- **Slice C** = the remaining 6 tests from the original Phase 4 (missing liveStreamingDetails,
  actualStartTime fallback, unparseable timestamp, id absent, zero-survivor WARNING, key
  preservation), plus this SDD bookkeeping commit.

## TDD Cycle Evidence (unchanged findings, now mapped to slices)

| Task | Slice | RED | GREEN | Note |
|---|---|---|---|---|
| 1.1–1.2 (helper lift) | A | ✅ Approval test written first, confirmed PASS pre-lift (pure refactor — approval-test pattern, not a real RED) | ✅ 21/21 `TestFilterFinishedStreams` green post-lift | — |
| 2.1–2.2 (pure helpers) | A | N/A — new pure functions, no prior behavior to contradict | ✅ 10/10 `TestAiringTimestampHelpers` green, directly testing precedence, empty/missing/non-string inputs, both timestamp formats' UTC conversion, and the unparseable → `ValueError` case | Direct-test deviation noted above |
| 2.3 (wiring) + 3.1–3.3 (re-mock + cutover) | B | ✅ Confirmed by the transient regression when the predicate was wired without re-mocking (5 tests hit a real network `HttpError` — no false pass) | ✅ All 9 `TestFilterPlenarySessionVideos` green after re-mock; 65/65 targeted file green | — |
| 4.1 (regression + boundaries) + 4.4 (exactly-one-call) | B | RED already covered by 3.2's regression (same window-comparison code path) | ✅ Passed on first run | — |
| 4.2–4.3 (zero-survivor WARNING) | C | ✅ Re-demonstrated by execution on this slice: the warning branch was temporarily neutralized (`if False and not matching_videos:`), the test failed exactly as expected (`assert 0 == 1`), then restored | ✅ Passed once restored; 71/71 targeted file green | Hard RED-before-GREEN gate satisfied per-slice |
| 4.5 (remaining precedence/enrichment tests) | C | Implementation pre-existed from slice B's cohesive predicate edit; tests lock the behavior in | ✅ Passed on first run | — |
| 5.1–5.4 (docs, lint, full suite) | B (docs) / C (final full-suite verification) | N/A | ✅ ruff clean all 3 slices; full suite 4495 passed / 27 skipped on slice C | — |

## Deviations from Design

1. **Extracted `_select_airing_window_matches`** (slice B) as an additional private helper, not
   named in design's Interfaces section — needed to keep the predicate under C901 10; matches
   design's generic "branching lives in the helpers" intent.
2. **Direct unit tests for `_airing_timestamp`/`_airing_date`** (slice A, `TestAiringTimestampHelpers`)
   contradict design's "Private helpers are exercised through it — no direct tests" statement. This
   was an explicit, coordinator-requested change to make the helpers-only slice independently
   reviewable before the predicate wiring exists in a later slice; not a silent deviation.
3. **Review Workload Guard fallback triggered twice**: the original single-PR estimate (~330) and
   the first two-PR fallback (PR 2 ≈524 lines) both undershot reality. The final 3-slice split
   (182 / 301 / 191 lines) is the first plan that lands every slice under the 400-line budget with
   no tests, docs, comments, or blank lines dropped to shrink any of them.

## Issues Found

None — all spec scenarios pass, `get_video_details` and `filter_finished_streams` are unmodified
and their existing tests pass unmodified (`TestGetVideoDetails`, `TestFilterFinishedStreamsExport`
untouched; all 21 `TestFilterFinishedStreams` tests green including the parity test).

## Remaining Tasks

None — all 19 tasks are `[x]`.

## Status

19/19 tasks complete across 3 stacked branches, each independently green and under the 400-line
budget. Worktree left on `fix/426-c-airing-scenario-tests`. Ready for the orchestrator to open the
three PRs (or continue to `sdd-verify`).
