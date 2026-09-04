# Archive Report: Filter plenary sessions by airing time, not publish time

**Change**: plenary-airing-time-filter  
**Issue**: GitHub issue #426  
**Change archived**: 2026-09-04  
**Archived to**: `openspec/changes/archive/2026-09-04-plenary-airing-time-filter/`  

## SDD Cycle Summary

The SDD cycle for issue #426 is complete. The change has been fully planned, implemented (as three stacked PRs), verified, and archived.

### Artifact Store Mode

**Mode**: openspec (repo-local)

### Final State (at close)

**Specification**: Delivered to dev as **three stacked PRs**, all merged on **2026-09-04**:

| PR | Title | Status | Commits | Measured diff |
|-----|-------|--------|---------|---|
| #450 | `fix/426-a-airing-helpers` | MERGED to dev | 3 commits | 182 lines (+179/−3, excl. openspec/) |
| #451 | `fix/426-b-airing-predicate` | MERGED to dev | +1 commit | 301 lines (+268/−33, vs. slice A) |
| #452 | `fix/426-c-airing-scenario-tests` | MERGED to dev | +4 commits | 209 lines (+209/−0, vs. slice B; 191 planned + the post-verify API-key test) |

**Dev head after merge**: `871a79a`

**Total authored lines delivered**: 692 lines (182 + 301 + 209; the pre-split single-branch measurement was 568 before the post-verify test)

### Verification Outcome

**Verdict**: PASS WITH WARNINGS

Per `verify-report.md`:

| Metric | Result |
|--------|--------|
| Requirements met | 7/7 ✅ |
| Scenarios compliant | 10/10 ✅ |
| Test suite status | 4495 passed, 27 skipped (pre-existing Postgres unavailability) |
| Coverage (full suite) | 90.18% |
| Ruff check | Clean (no errors) |
| Ruff format | Clean (297 files already formatted) |
| DAG import check | Passed (`youtube_channel_monitor_dag.py` still imports) |
| Docker E2E | Unavailable (no Docker daemon in sandbox) |

**Warnings documented in verify-report**:

1. **Untested error branch** (lines 189–191): Missing `YOUTUBE_API_KEY` with title matches present raises `ValueError`. This branch was introduced by this change but has no direct test in `TestFilterPlenarySessionVideos`. Not a spec scenario (does not affect 10/10 compliance), but a code path introduced.  
   **Closure**: After verification, `test_missing_api_key_with_title_matches_raises` was added in slice C (commit message: "test(youtube-monitor): prove a missing YOUTUBE_API_KEY fails loudly once titles match"). The test is now in the final merged code on dev.

2. **Design deviation documented**: Direct unit tests for private helpers `_airing_timestamp`/`_airing_date` (`TestAiringTimestampHelpers`, 10 tests) deviate from the design's stated "no direct tests for private helpers" seam discipline. This was an explicit, coordinator-requested deviation for slice-A reviewability (documented in apply-progress). Both helpers are also exercised indirectly through the public seam by the 19 `TestFilterPlenarySessionVideos` tests.  
   **Status**: Accepted deviation, recorded in apply-progress deviation #1.

### Task Completion

**Persisted tasks artifact status**: All 19 implementation tasks are marked complete (`[x]`).

Per `tasks.md`:

| Phase | Task count | Status |
|-------|-----------|--------|
| Phase 1: Shared fetch helper | 2 | ✅ All [x] |
| Phase 2: Airing-time key resolution helpers | 3 | ✅ All [x] |
| Phase 3: Re-mock the 9 existing tests and cut over | 3 | ✅ All [x] |
| Phase 4: Boundary, regression, observability | 5 | ✅ All [x] |
| Phase 5: Docs, lint, full verification | 4 | ✅ All [x] |
| Phase 6: Delivery guard | 2 | ✅ All [x] |
| **Total** | **19** | **✅ All complete** |

### Specs Synced

The delta spec from `openspec/changes/plenary-airing-time-filter/specs/plenary-session-matching/spec.md` has been copied to `openspec/specs/plenary-session-matching/spec.md` (a new spec, not a delta to an existing one).

**Spec**: `plenary-session-matching`  
**Capability**: Which channel videos count as "the plenary" for a target date — title match, airing-time window (replacing publish-time), missing-data handling, and match observability.

| Requirement | Status |
|---|---|
| Title match gates the API call | ✅ Implemented |
| One batched airing-time lookup via shared helper | ✅ Implemented |
| Airing key precedence, never publish time | ✅ Implemented |
| Airing-time window replaces publish-time window | ✅ Implemented |
| Zero survivors are observable; existing keys are preserved | ✅ Implemented |
| Downstream guards stay unmodified | ✅ Implemented |
| Tests mock the API and assert call behavior | ✅ Implemented |

All 10 scenarios from the spec are compliant with the implementation:

1. ✅ Zero title matches skip the API
2. ✅ One call for all matched ids
3. ✅ filter_finished_streams call shape is unchanged
4. ✅ Precedence table decides the key and the WARNING
5. ✅ Regression — 5RNELQ2W6co matches
6. ✅ Outside the window does not match; boundaries do/don't
7. ✅ Title matches, zero survivors, one WARNING
8. ✅ Existing keys survive enrichment
9. ✅ guard_enabled=False remains a passthrough
10. ✅ Test asserts call shape and no-match skip

### Changes Made

**Affected files**:

1. `congress_videos/modules/youtube/youtube_channel.py`
   - Added `_fetch_video_items_by_id(youtube, ids, part)` shared helper
   - Added `_airing_timestamp(item)` helper (precedence: `actualEndTime` → `actualStartTime` with WARNING → WARNING + exclude)
   - Added `_airing_date(iso_timestamp)` helper (UTC calendar date via `.astimezone(UTC).date()`)
   - Rewrote `filter_plenary_session_videos` predicate to use airing time (via `_select_airing_window_matches`)
   - Updated docstring: "airing time" instead of "published date"
   - Replaced lines 250–252 in `filter_finished_streams` with the shared `_fetch_video_items_by_id` call

2. `congress_videos/youtube_channel_monitor_dag.py`
   - Updated `lookback_days` parameter comment to name "airing"

3. `tests/congress_videos/modules/youtube/test_youtube_channel.py`
   - Re-mocked all 9 existing `TestFilterPlenarySessionVideos` tests with `YOUTUBE_API_KEY` env var and `build()` mock
   - Added 10 new tests:
     - `TestAiringTimestampHelpers` (direct tests of `_airing_timestamp` / `_airing_date`)
     - `TestFilterPlenarySessionVideos`: regression (`5RNELQ2W6co`), boundaries (lower, upper), missing data, unparseable timestamps, id absent, exactly-one-call assertion, zero-survivor WARNING, existing keys preservation
   - Added new parity test in `TestFilterFinishedStreams` (`test_single_batched_call_with_expected_part_and_ids`) asserting the lift did not change the call shape

**Downstream confirmations**:

- `TestGetVideoDetails` tests: unmodified and passing
- `TestFilterFinishedStreams` tests (21 total, incl. new parity test): unmodified behavior confirmed, all passing

### Review Workload

**Single-PR forecast**: ~330 lines  
**Measured implementation**: 568 lines  
**Budget**: 400 lines per PR (default review workload guard)  

**Outcome**: Forecast exceeded budget; sliced into 3 stacked PRs per orchestrator's `auto-chain` delivery decision:

- Slice A (`fix/426-a-airing-helpers`): 182 lines ✅ Under budget
- Slice B (`fix/426-b-airing-predicate`): 301 lines ✅ Under budget
- Slice C (`fix/426-c-airing-scenario-tests`): 209 lines ✅ Under budget

Each slice is independently Green, independently under budget, and has autonomous scope (A: pure lift + parity test; B: predicate cutover + docs; C: edge cases + observability).

### Architecture & Design Adherence

Per `design.md`, the following design decisions were implemented:

| Decision | Choice | Evidence |
|---|---|---|
| Client construction | `os.getenv("YOUTUBE_API_KEY")` + `build(...)` **after** the title pass | Confirmed by source order; four `AssertionError`-mocked no-call tests prove the key is never required for zero-title-match runs |
| Missing key with title matches | `ValueError`, same message as `fetch_youtube_channel_videos` | Added test (PR #452, commit "test(youtube-monitor): prove a missing YOUTUBE_API_KEY fails loudly once titles match") |
| Airing key | `actualEndTime` → `actualStartTime` (WARNING) → WARNING + exclude | Verified in precedence table tests; never uses `publishedAt` as fallback |
| Non-string / unparseable timestamp | Treated as **missing**: WARNING + exclude | Exception discipline via `except (ValueError, TypeError, AttributeError)` |
| Timezone | `.astimezone(UTC).date()` | Confirmed in implementation; handles non-`Z` offsets correctly |
| Per-video enrichment | Add `actual_end_time` / `actual_start_time` (may be `None`); every existing key untouched | Verified in test "existing keys survive enrichment" |
| Shared fetch | Pure lift of lines 250–252 into `_fetch_video_items_by_id` | Confirmed via `git diff` — only 2-line replacement in `filter_finished_streams`, no other changes to that function |

### Docs Drift Check

Confirmed via `rg -n "publishedAt|published date|published_at"` that no stale prose describes the plenary-matching filter as publish-date-based. The only references are in unrelated functions (`fetch_youtube_channel_videos`, `get_video_details`) that legitimately use `published_at`/`publishedAt`.

**Status**: No WARNING.

### Rollback Plan

Reverting the three stacked PRs (revert #452, #451, #450 in order) returns:
- `filter_plenary_session_videos` to its original `publishedAt`-based predicate
- `filter_finished_streams` to its original inlined `videos.list` call
- Test mocks to the original state (9 tests re-mocked, new tests removed)

No schema, migration, DAG topology, or XCom contract changes were introduced, so the revert is clean with no data orphaning.

### Confidence & Open Questions

**None**. All success criteria from the proposal are met:

- ✅ `5RNELQ2W6co` shape (published 08-27, ended 09-03, target 09-03, lookback 1) matches.
- ✅ Airing time outside the window does not match.
- ✅ Missing `liveStreamingDetails` excludes with a WARNING naming the id.
- ✅ Zero survivors after title matches emits a WARNING.
- ✅ `uv run pytest` green; `get_video_details` and `filter_finished_streams` unchanged.

### Next Steps

**Post-archive delivery steps** (NOT yet done; the orchestrator does these next):

1. Release dev → main (release PR; this repo does not tag releases)
2. Deploy to NAS (trigger `git_sync_dag` on both schedulers: the dev stack tracks `dev`, the prod stack tracks `main`)
3. Real monitor run (verify the airing-time filter works in production)

These steps are post-archive; the SDD cycle itself is closed.

### Archive Contents

The following artifacts have been archived:

- `proposal.md` ✅
- `specs/plenary-session-matching/spec.md` ✅
- `design.md` ✅
- `tasks.md` ✅
- `verify-report.md` ✅
- `exploration.md` ✅
- `apply-progress.md` ✅
- `archive-report.md` ← this file

**Archived to**: `openspec/changes/archive/2026-09-04-plenary-airing-time-filter/`

### SDD Cycle Close

The SDD cycle for issue #426 (`plenary-airing-time-filter`) is **COMPLETE**.

- ✅ Proposal phase complete
- ✅ Spec phase complete
- ✅ Design phase complete
- ✅ Tasks phase complete
- ✅ Apply phase complete (3 slices, all merged to dev)
- ✅ Verify phase complete (PASS WITH WARNINGS, all requirements/scenarios met)
- ✅ Archive phase complete (specs synced, change archived, audit trail closed)

---

*Generated by sdd-archive phase*  
*Archive date: 2026-09-04*  
*Final-state authority: Explicit facts in launch prompt + persisted tasks artifact + verify-report*
