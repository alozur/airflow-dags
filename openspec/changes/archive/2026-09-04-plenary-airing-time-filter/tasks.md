# Tasks: Filter plenary sessions by airing time, not publish time

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~330 (+92/−30 source, +188/−18 tests, +1/−1 DAG) |
| 400-line budget risk | Medium-High |
| Chained PRs recommended | No (fallback slice pre-planned) |
| Suggested split | Single PR; fallback PR 1/PR 2 if >400 |
| Delivery strategy | auto-chain |
| Chain strategy | pending |

Decision needed before apply: No
Chained PRs recommended: No
Chain strategy: pending
400-line budget risk: Medium-High

**Superseded by measurement.** The original single-PR plan measured at 568 authored lines
(`git diff --stat origin/dev...HEAD -- . ':!openspec'`), exceeding both the ~330 forecast and the
400-line budget. The two-PR fallback named above was itself re-sliced into three stacked branches
after PR 2 alone still measured ~524 lines. See "Delivery: 3-slice stacked result" at the end of
this file for the final branch/commit/line-count record.

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Helper lift + parity test | PR 1 | `pytest -k FilterFinishedStreams` | N/A — mocked `build()` | Revert lift |
| 2 | Predicate + enrichment + docs | PR 2 | `pytest -k FilterPlenarySessionVideos` | N/A — mocked `build()` | Revert; gate returns to `published_at` |

## Phase 1: Shared fetch helper

- [x] 1.1 RED — `TestFilterFinishedStreams`: add design test #11 (one call, exact `part`/`id`). FAIL.
- [x] 1.2 GREEN — add `_fetch_video_items_by_id(youtube, ids, part)`; replace the line-251 call in `filter_finished_streams` (no chunking/short-circuit). Run: GREEN, else revert and keep duplication.

## Phase 2: Airing-time key resolution helpers

- [x] 2.1 RED — `TestFilterPlenarySessionVideos`: design tests #4–#7 (end-time ok, start-time fallback, missing details, unparseable timestamp, id absent). FAIL.
- [x] 2.2 GREEN — add `_airing_timestamp(item)` (end→start→`None`) and `_airing_date(iso)` via `astimezone(UTC).date()`; extend the `datetime` import with `date`.
- [x] 2.3 Wire both into the predicate inside `except (ValueError, TypeError, AttributeError)`; emit fallback/no-airing-time WARNINGs per design's log table. Run: GREEN.

## Phase 3: Re-mock the 9 existing tests and cut over

- [x] 3.1 Add `YOUTUBE_API_KEY` + no-call `build` mock (`AssertionError` side_effect) to the 4 no-title-match tests; still GREEN.
- [x] 3.2 RED — re-mock the other 5 tests: `build` → `_service({...})` keyed by airing timestamps, not `published_at`. FAIL.
- [x] 3.3 GREEN — cut over: build client after title-match, one `_fetch_video_items_by_id` call, window `[target_date - lookback_days, target_date]` on UTC airing date. Raise `ValueError` (message matches `fetch_youtube_channel_videos`) when the key is missing with matches present. Run: all 9 GREEN.

## Phase 4: Boundary, regression, observability

- [x] 4.1 RED — design tests #1–#3 (`5RNELQ2W6co` regression, lower boundary, `target_date + 1 day` upper boundary). FAIL pre-3.3, PASS after.
- [x] 4.2 RED — design test #9 (zero-survivor WARNING via `caplog`, both ids named, `"missing"` for the unresolved one). FAIL.
- [x] 4.3 GREEN — implement the zero-survivor WARNING per design's log table. Run: 4.1–4.2 GREEN.
- [x] 4.4 RED — design tests #8/#10 (exactly-one-call `id="A,B,C"`; existing keys survive). FAIL.
- [x] 4.5 GREEN — enrichment only adds `actual_end_time`/`actual_start_time`, never mutates. Extend `_item` with `actual_start: str | None = None`. Run: GREEN.

## Phase 5: Docs, lint, full verification

- [x] 5.1 Update `filter_plenary_session_videos`'s docstring (drop "published date" language), same commit as the predicate.
- [x] 5.2 Update `youtube_channel_monitor_dag.py:158`'s `lookback_days` comment to name "airing".
- [x] 5.3 `uv run ruff check` + `uv run ruff format --check` on touched files: clean (no reliance on the file's existing `C901` ignore).
- [x] 5.4 `uv run pytest -n auto`: all green, incl. unmodified `TestGetVideoDetails`/`TestFilterFinishedStreams`.

## Phase 6: Delivery guard

- [x] 6.1 Confirm every tasks.md checkbox is `[x]` before opening the PR.
- [x] 6.2 `git diff --stat origin/dev...HEAD` vs the ~330-line forecast; if >400, split into fallback PR 1 (Phase 1) / PR 2 (Phases 2–5) — never drop tests or docs to shrink.

**Result**: the two-PR fallback (PR 1 = Phase 1, PR 2 = Phases 2–5) still measured PR 2 at ~524
lines on its own — over budget. Per the orchestrator's `auto-chain` delivery decision, re-sliced
into three stacked branches, each independently green and under 400 lines. See "Delivery: 3-slice
stacked result" below for the final record.

## Delivery: 3-slice stacked result

Rebased onto `origin/dev` at `0b693d4` (no conflicts; sibling PRs had no overlap). Three stacked
branches in the same worktree, each branched from the previous, `chain_strategy: stacked-to-main`.
No branch was pushed by `sdd-apply`; the orchestrator opens the PRs.

| Branch | Commits | Measured (`git diff --stat <parent>...HEAD -- . ':!openspec'`) | Targeted tests | Full suite |
|---|---|---|---|---|
| `fix/426-a-airing-helpers` | `5a4167a` chore(sdd) plan, `44ef6ac` refactor(youtube) lift, `afbf6b9` feat(youtube) airing-timestamp helpers | vs `origin/dev`: 179+/3− = 182 lines | 61 passed | not re-run per-branch (see slice C) |
| `fix/426-b-airing-predicate` | + `802bbd4` fix(youtube-monitor) predicate cutover | vs `fix/426-a-airing-helpers`: 268+/33− = 301 lines | 65 passed | not re-run per-branch (see slice C) |
| `fix/426-c-airing-scenario-tests` | + `5232cb7` test(youtube-monitor) edge cases, + this bookkeeping commit | vs `fix/426-b-airing-predicate`: 191+/0− = 191 lines | 71 passed | `uv run pytest -n auto -q`: 4495 passed, 27 skipped (Postgres-dependent, expected), 90.18% coverage |

All three slices: `uv run ruff check` and `uv run ruff format --check` clean. `PYTHONPATH=. uv run
python congress_videos/youtube_channel_monitor_dag.py` confirmed the DAG still imports after the
slice B comment change. No tests, docs, comments, or blank lines were dropped to shrink any slice.
