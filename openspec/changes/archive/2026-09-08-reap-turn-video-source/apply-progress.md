# Apply Progress: reap-turn-video-source

## Batch 1 (PR1, `feat/467-a-turn-id-migration`)

**Status**: Phase 1 complete. Ready for `sdd-verify` on this work unit / next PR (Phase 2)
in a follow-up `sdd-apply` batch.

### What landed

- `congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql` (new): adds
  nullable `video_shorts.turn_id INTEGER REFERENCES speaker_turn_videos(turn_id)
  ON DELETE SET NULL`, `idx_video_shorts_turn_id` index, and a column comment.
  DOWN block fully commented per the 046 convention (the migration runner executes
  the whole file in one transaction with no automatic rollback).
- `congress_videos/sql/production_schema.sql`: appended the `turn_id` column to
  the `video_shorts` table block (after `last_upload_error`) and the matching
  `CREATE INDEX idx_video_shorts_turn_id` statement to the INDEXES section.
- `tests/congress_videos/sql/test_production_schema.py`:
  - Added `"turn_id"` to `VIDEO_SHORTS_COLUMNS` (20 → 21) and updated the class
    docstring column count.
  - Added `TestVideoShortsTableSnapshot.test_turn_id_fk_is_production_qualified`
    asserting `REFERENCES PRODUCTION.SPEAKER_TURN_VIDEOS(TURN_ID) ON DELETE SET NULL`.
  - Added `TestVideoShortsIndexCompleteness` (new class, mirrors
    `TestVideoChaptersIndexCompleteness`) asserting the `idx_video_shorts_turn_id`
    `CREATE INDEX` statement is present in the snapshot.

### TDD evidence

- RED: `uv run pytest tests/congress_videos/sql/test_production_schema.py -o addopts=`
  → 3 failed (`test_column_present_in_block[turn_id]`,
  `test_turn_id_fk_is_production_qualified`,
  `TestVideoShortsIndexCompleteness::test_index_statement_present`), 215 passed.
- GREEN: same scoped command → 218 passed, 0 failed.
- Full suite: `uv run pytest -n auto` → 4583 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB, as expected in this environment), 0 failed.
  `--cov-fail-under=80` is enforced by `pyproject.toml` addopts and the run passed.
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 301 files already formatted.

### Changed lines

`git diff --cached --stat` (3 files, PR1 scope only):

```
congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql | 28 ++++++++++++++++++
congress_videos/sql/production_schema.sql                       |  8 +++++-
tests/congress_videos/sql/test_production_schema.py             | 31 ++++++++++++++++++++--
3 files changed, 64 insertions(+), 3 deletions(-)
```

Total: 64 additions + 3 deletions = 67 changed lines (budget: 400).

### Deviations from design

None. Migration text, column placement, index name, and test additions match
`design.md` §"Interfaces / Contracts" → "2. Migration `047_add_video_shorts_turn_id.sql`"
verbatim, including the FK's `ON DELETE SET NULL` semantics and the `chapter_rank`-style
comment conventions used elsewhere in the file.

### Manual DOWN-block verification

Confirmed every line of the `-- DOWN` section in `047_add_video_shorts_turn_id.sql`
starts with `--` (checked with `bat -A`), matching the 044/046 convention that the
migration runner (`utils/migrations_dag.py`) executes the whole file in one
transaction with no rollback support.

### Not in scope for this batch

Phases 2–5 (`get_turn_videos_for_shorts`, preparer rewrite, processor/sidecar wiring,
Tier-1 partition, and post-merge ops) are untouched — this batch is PR1 only, per the
orchestrator's work-unit scope.

## Batch 2 (PR2, `feat/467-b-turn-selection`, base `feat/467-a-turn-id-migration` @ `a2ab9fd`)

**Status**: Phase 2 complete. Ready for `sdd-verify` on this work unit / next PR
(Phase 3) in a follow-up `sdd-apply` batch.

**Commit**: `32412e5` — `feat(reap): source turn-video candidate selection from speaker turns`

### What landed

- `congress_videos/modules/database.py`:
  - Deleted `get_chapters_for_shorts` (chapter-only Reap candidate selection,
    the `is_uploaded_to_youtube = TRUE` publish gate, `min_relevance_score`
    threshold, and the interval-arithmetic duration check).
  - Added `get_turn_videos_for_shorts(max_turns: int | None = None) -> list[dict]`
    per design §1: unfiltered `group_spans` CTE (`MIN`/`MAX(start/end_seconds)`
    + summed `procedural_seconds`, no `WHERE` inside the CTE — issue #151 trap),
    `DISTINCT ON (stv.output_path)` representative row, dedup via
    `NOT EXISTS (... vs.turn_id = stv.turn_id)` (turn-keyed, not chapter-keyed),
    `NOT COALESCE(st.is_procedural, FALSE)` exclusion, `group_duration_seconds
    >= 120` floor with no upper ceiling, `LIMIT %s` appended only when
    `max_turns is not None`, and ordering by `COALESCE(interest_score, 1) DESC,
    relevance_score DESC, session_date DESC, turn_id ASC`. No `prepared_at`,
    relevance threshold, or parent-upload-date gate — confirmed absent from the
    emitted SQL by dedicated tests.
  - Extended `insert_video_short` with a trailing optional `turn_id: int | None
    = None` keyword parameter (backward compatible for existing positional/
    keyword callers) while inserting it as the SQL column right after
    `chapter_id`: `(chapter_id, turn_id, reap_project_id, reap_status,
    pretrim_start_secs, pretrim_end_secs, pretrim_used_srt, staged_clip_path,
    scoring_reasoning)` — 9 placeholders, `params[1]` is `turn_id`.
- `tests/congress_videos/modules/test_reap_db_methods.py`:
  - Replaced `TestGetChaptersForShorts` (4 tests) with `TestGetTurnVideosForShorts`
    (14 tests) covering: empty/non-empty results, `LIMIT` presence/absence tied
    to `max_turns`, `group_spans` CTE presence, the CTE being unfiltered by
    `is_procedural` (issue #151 regression guard), `DISTINCT ON` usage, dedup
    keying on `turn_id` (and NOT `chapter_id`), procedural exclusion, the 120s
    floor, absence of any upper ceiling (`<=` anywhere in the query), absence
    of `prepared_at`/`youtube_upload_date`/`is_uploaded_to_youtube` gates, and
    the exact editorial ordering keys.
  - Extended `TestInsertVideoShort` with 5 new tests: 9-placeholder count,
    column-list ordering (`chapter_id, turn_id, reap_project_id`), `turn_id`
    defaulting to `None`, `turn_id` being passed through when given, and a
    regression test that the pre-existing positional-call shape (no `turn_id`)
    still returns the inserted id.
- `tests/congress_videos/modules/test_database_surface.py`: moved
  `get_chapters_for_shorts` from `LIVE_METHOD_NAMES` to `DEAD_METHOD_NAMES`
  (with an issue #467 comment) and added `get_turn_videos_for_shorts` to
  `LIVE_METHOD_NAMES`.
- `openspec/changes/reap-turn-video-source/tasks.md`: marked tasks 2.1–2.6 `[x]`.

### TDD evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 2.1 `get_turn_videos_for_shorts` SQL-text tests | 14 new tests fail with `AttributeError: get_turn_videos_for_shorts` | method implemented per design §1; all 14 pass | ruff clean |
| 2.2 `insert_video_short(turn_id=)` | 3 new tests fail with `TypeError: unexpected keyword argument 'turn_id'` | signature + column list extended; all pass | ruff clean |
| 2.3 surface guard swap | `test_dead_method_absent[get_chapters_for_shorts]` and `test_live_method_present[get_turn_videos_for_shorts]` fail | swap applied; both pass | ruff clean |

- RED: `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/modules/test_database_surface.py -o addopts=`
  → 19 failed, 84 passed (exact failures: 14 `TestGetTurnVideosForShorts` tests,
  3 `TestInsertVideoShort` turn_id tests, 2 surface-guard parametrized cases).
- GREEN: same scoped command → 103 passed, 0 failed.
- Full suite: `uv run pytest -n auto` → 4599 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB), 0 failed. `--cov-fail-under=80` enforced by
  `pyproject.toml` addopts and the run passed.
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 301 files already formatted.
- DagBag import check: `uv run python -c "from airflow.models import DagBag; ..."`
  → `16 {}` (16 DAGs, zero import errors — `reap_clip_preparer_dag.py` still calls
  the now-removed `get_chapters_for_shorts` inside task-function bodies only,
  which DagBag import never executes; see "Deviations" below).

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/modules/test_database_surface.py -o addopts=` → 103 passed |
| Runtime harness command/scenario and exact result | N/A — MagicMock cursor, no live DB (per tasks.md work-unit table); DagBag import check run instead as the closest applicable runtime boundary → `16 {}` |
| Rollback boundary | Revert commit `32412e5`; the unit is inert without a caller until PR3 rewrites the preparer, so reverting removes no other work |

### Changed lines

`git diff --stat a2ab9fd..32412e5` (4 files, PR2 scope):

```
congress_videos/modules/database.py                | 108 ++++++++----
openspec/changes/reap-turn-video-source/tasks.md    |  12 +-
tests/congress_videos/modules/test_database_surface.py |   5 +-
tests/congress_videos/modules/test_reap_db_methods.py  | 186 +++++++++++++++++++--
4 files changed, 255 insertions(+), 56 deletions(-)
```

Total: 255 additions + 56 deletions = 311 changed lines (budget: 400).

### Deviations from design

None. `get_turn_videos_for_shorts` SQL, `insert_video_short` column order/
placeholder count, and the surface guard swap match `design.md` §1, §3, and §8
verbatim.

One scope clarification, not a deviation: `congress_videos/reap_clip_preparer_dag.py`
still calls `db.get_chapters_for_shorts(...)` inside `_query_chapters` and
`_extract_and_pretrim_clip` (task functions, not module-level code), and its test
file still mocks that method name via plain `mocker.patch(...)` (no `autospec`).
Design explicitly defers the preparer rewrite to PR3 ("Phase 3: Preparer rewrite")
and does not prescribe a stub — the DAG module still imports cleanly (confirmed:
DagBag reports 0 import errors) because Python does not validate attribute
existence until the attribute is actually accessed at task-execution time, which
DagBag import never triggers. This means the `congress_reap_clip_preparer` DAG
would raise `AttributeError` if actually **run** between PR2 landing and PR3
merging — acceptable per the design's explicit "order is load-bearing" rollout
note, since PRs 1–3 are meant to merge as a stack before any deploy, not
individually deployed.

### Issues Found

None.

### Remaining Tasks

- [ ] Phase 3: Preparer rewrite (`reap_clip_preparer_dag.py` — `output_path` staging,
  leading pre-trim, dead-code removal, `max_chapters`→`max_turns`)
- [ ] Phase 4a: `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` +
  processor/sidecar wiring
- [ ] Phase 4b: `pending_shorts_candidate_sql` partition + parent-gate drop
- [ ] Phase 5: Orchestrator-run ops (migration 047 on NAS dev+prod, git_sync,
  validation query, manual trigger)

### Workload / PR Boundary

- Mode: stacked PR slice (auto-chain, `stacked-to-main`)
- Current work unit: PR2 — `get_turn_videos_for_shorts` + `insert_video_short(turn_id=)`
  + surface swap, branch `feat/467-b-turn-selection`, base `feat/467-a-turn-id-migration`
- Boundary: starts from PR1's `a2ab9fd` (migration 047 + schema snapshot), ends at
  commit `32412e5` — turn-video candidate selection is implemented and unit-inert
  until PR3 wires a caller
- Estimated review budget impact: 311 changed lines, within the 400-line budget

### Not in scope for this batch

Phases 3–5 (preparer rewrite, processor/sidecar wiring, Tier-1 partition, and
post-merge ops) are untouched — this batch is PR2 only, per the orchestrator's
work-unit scope. The orchestrator settles the native attempt ledger; this batch
does not call `sdd-attempt settle`.

## Batch 3 (PR3, `feat/467-c-preparer-rewrite`, base PR2 @ `6d4691c`)

**Status**: Phase 3 complete (tasks 3.1–3.7). Ready for `sdd-verify`.

**Commit**: `88c4d80` — `feat(reap): stage materialized turn output directly with leading pre-trim`

### What landed

- `congress_videos/reap_clip_preparer_dag.py`: rewrote `_query_chapters`→`_query_turns`
  and `_extract_and_pretrim_clip`→`_stage_and_pretrim_clip` per design §6.
  `_query_turns` calls `get_turn_videos_for_shorts(max_turns=...)` and logs the
  zero-eligible WARNING. `_stage_and_pretrim_clip` ffprobes `output_path`
  (authoritative), skips <120s, stages `output_path` unmodified under
  threshold, or pre-trims a leading `[0, target_secs]` window to
  `turn_{turn_id}_reap.mp4` over threshold, re-probes the staged file for the
  unchanged-shape safety gate, and passes `turn_id` through to
  `insert_video_short`. Task ids/count/schedule/chain kept unchanged (design
  explicit). Params: `max_chapters`→`max_turns`, `min_relevance_score` dropped,
  threshold/target `600→900`. Deleted `_find_source_video`, `_interval_to_srt`,
  the `split_video_chapter`/`DOWNLOADS_DIR`/`find_srt_for_chapter`/
  `select_pretrim_window` imports; kept `_ffmpeg_extract_window` unchanged; added
  `_probe_duration_secs` helper. Module docstring rewritten for the turn-based
  flow (issue #422 docs policy).
- `tests/congress_videos/test_reap_clip_preparer_dag.py`: rebuilt surgically —
  `TestCongressReapClipPreparerDAGLoads` and `TestFfmpegExtractWindow` kept
  byte-identical (plus one added param-rename test and a 2-line command-injection
  assertion) to minimize diff noise; `TestQueryChapters`/`TestExtractAndPretrimClip`
  (chapter-based, ~500 lines testing now-deleted functions) replaced with
  `TestQueryTurns` (5 tests) and `TestStageAndPretrimClip` (6 tests) covering the
  threat-matrix and Testing-Strategy-mandated scenarios: command injection (list,
  no `shell=True`), destructive-fs-op (`staged_clip_path != output_path` on
  pre-trim), zero-eligible WARNING text, no-pretrim/over-threshold staging paths
  and offsets, <120s skip, ffprobe-failure block, safety-gate block, and
  partial-success (good clip inserted, then `AirflowException`).

### TDD evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 3.1–3.3 preparer behavior | New test file written first against the not-yet-rewritten module; old test suite already failed on import of removed `get_chapters_for_shorts`/`_find_source_video` symbols, confirming the old contract was gone | Rewrote `reap_clip_preparer_dag.py`; all 21 new/kept tests pass | ruff clean |

- Scoped: `uv run pytest tests/congress_videos/test_reap_clip_preparer_dag.py -o addopts=`
  → 21 passed.
- Full suite: `uv run pytest -n auto` → 4594 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB), 0 failed. `--cov-fail-under=80` enforced and passed.
- `uv run ruff check .` → All checks passed. `uv run ruff format --check .` → 301
  files formatted.
- DagBag: `uv run python -c "from airflow.models import DagBag; ..."` → `16 {}`.
- `bash scripts/test-airflow-e2e.sh` → reported `unavailable` (Docker daemon not
  reachable in this environment) — not a failure per repo policy; run manually
  before merge.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/test_reap_clip_preparer_dag.py -o addopts=` → 21 passed |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → `unavailable` (Docker daemon unreachable); DagBag import check run as fallback → `16 {}` |
| Rollback boundary | Revert this commit; pauses `congress_reap_clip_preparer` back to the PR2 state (module still imports, task bodies would `AttributeError` on run until PR3 lands — same as documented in Batch 2) |

### Changed lines

`git diff --numstat 6d4691c..HEAD` (code files only):

```
congress_videos/reap_clip_preparer_dag.py               |  92 +180
tests/congress_videos/test_reap_clip_preparer_dag.py     | 117 +389
```

Total (code+tests): 209 additions + 569 deletions = **778 changed lines**
(ledger cap: 400). Plus `tasks.md`/`apply-progress.md` doc deltas.

### Deviations from design

None functionally — `_query_turns`/`_stage_and_pretrim_clip`, params, deletions,
and the staged-path/offset shape match design §6 verbatim, including the
"safety gate (unchanged shape)" re-probe of the staged file before insert.

**Budget deviation (flagged, not silently absorbed)**: this slice is **778**
changed lines against the ledger's 400-line cap, not the ~280 forecast in
`tasks.md`. Root cause: the old chapter-based test suite for this DAG
(`TestQueryChapters` + `TestExtractAndPretrimClip`, ~495 lines) tests functions
this rewrite deletes outright (`_find_source_video`, SRT-window pre-trim,
codec-cache-sharing across two probes, chapter-cut safety gate) — none of it
carries over to the turn-based flow, so removing it is mandatory dead-code
cleanup (task 3.5 in spirit, same "delete tests for deleted code" logic as PR2's
`get_chapters_for_shorts` surface-guard swap), not discretionary trimming. That
deletion alone is ~495 lines, which exceeds the 400 cap **before any new test
is added**. The production file's 272-line diff is the minimum needed to swap
chapter-lookup+cut for ffprobe-first+stage (kept `_ffmpeg_extract_window`
untouched to avoid inflating it further). Per `sdd-apply`'s explicit guidance
("implement it honestly... report the final authored line count... do not
iterate trying to reach the number"), this was not force-fit smaller by cutting
tests or comments. Recommend the orchestrator apply `size:exception` to this
slice; the alternative (splitting PR3 further) would still require deleting the
same ~495 obsolete test lines in whichever sub-slice ships the rewrite, so
splitting does not by itself bring any single slice under 400.

### Issues Found

None.

### Remaining Tasks

- [ ] Phase 4a: `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` +
  processor/sidecar wiring
- [ ] Phase 4b: `pending_shorts_candidate_sql` partition + parent-gate drop
- [ ] Phase 5: Orchestrator-run ops (migration 047 on NAS dev+prod, git_sync,
  validation query, manual trigger)

### Workload / PR Boundary

- Mode: stacked PR slice (auto-chain, `stacked-to-main`) — **budget exceeded,
  size:exception recommended** (see "Deviations from design" above)
- Current work unit: PR3 — preparer rewrite, branch `feat/467-c-preparer-rewrite`,
  base `feat/467-b-turn-selection` @ `6d4691c`
- Boundary: starts from PR2's `6d4691c`, ends at this batch's commit — the
  preparer now stages `output_path` directly and pre-trims file-relative
- Estimated review budget impact: 778 changed lines (code+tests), over the
  400-line budget — flagged for orchestrator decision, not silently absorbed

### Not in scope for this batch

Phases 4a–5 (processor/sidecar wiring, Tier-1 partition, and post-merge ops)
are untouched — this batch is PR3 only, per the orchestrator's work-unit scope.
The orchestrator settles the native attempt ledger; this batch does not call
`sdd-attempt settle`.

## Batch 4a (PR4a, `feat/467-d-processor-sidecar`, base PR3 @ `b339a12`)

**Status**: Phase 4a complete (tasks 4a.1–4a.7). Ready for `sdd-verify`.

### What landed

- `congress_videos/modules/database.py`:
  - `insert_video_short_clip`: added trailing `turn_id: int | None = None`
    (backward compatible), inserted right after `chapter_id` in the column
    list — `(chapter_id, turn_id, reap_project_id, reap_clip_id, ...)`,
    8 placeholders.
  - `claim_pending_clip`: wrapped the atomic `UPDATE ... RETURNING *` in a
    `claimed` CTE (per design §4) — ordering, `FOR UPDATE SKIP LOCKED`, and
    the two priority subqueries (`session_date`, `relevance_score`) carried
    forward byte-identical. The outer `SELECT` `LEFT JOIN`s
    `speaker_turn_videos` on `stv.turn_id = c.turn_id` and `LEFT JOIN
    LATERAL`s a sibling aggregate (`MIN`/`MAX(start/end_seconds)` over rows
    sharing `output_path`) to surface `group_start_seconds`/
    `group_end_seconds` alongside the claimed row. Legacy rows
    (`turn_id IS NULL`) yield `NULL` spans through the `LEFT JOIN`, exactly
    the sidecar's chapter-fallback signal.
- `congress_videos/reap_processor_dag.py`:
  - `ReapJobSensor.poke` reads `turn_id = claimed_clip.get("turn_id")` and
    forwards it to **both** `db.insert_video_short_clip(..., turn_id=turn_id)`
    and `_write_short_sidecar_best_effort(..., turn_id=turn_id)` — the
    design-flagged risk #3 fix: without this, downloaded clips would carry
    `turn_id IS NULL` and collapse into the per-chapter Tier-1 partition
    (PR4b), since that partition is computed over downloaded rows, not the
    pending parent.
  - `_write_short_sidecar_best_effort` gained a `turn_id=None` parameter,
    forwarded to `write_short_srt_sidecar`.
- `congress_videos/srt_helpers.py`:
  - `write_short_srt_sidecar` gained a trailing `turn_id: int | None = None`
    parameter. When `turn_id is not None`, the function now ALWAYS falls back
    to the full chapter span — it never reaches the existing
    pretrim-offset/window-validity branch — regardless of whether
    `pretrim_start_secs`/`pretrim_end_secs` are present. When `turn_id is
    None` (default), behaviour is byte-identical to before this change.
  - Module docstring extended to document the turn-sourced approximation
    (issue #422 docs-drift policy: the change reads differently now, so the
    docstring must too).

### Deviation from design.md §7 (flagged, not silently absorbed)

design.md §7 sketches window math for a turn-sourced clip using two
`turn_group_start_secs`/`turn_group_end_secs` parameters: with both pretrim
offsets present, derive `origin = chapter_start + turn_base` and use
`[origin + pretrim_start, origin + pretrim_end]`; with no pretrim, fall back
to the **group span** (`[origin, chapter_start + turn_end]`) — narrower than
the full chapter span.

The orchestrator's launch prompt explicitly named
`specs/short-video-srt-artifacts/spec.md`'s "Short SRT window derivation with
fallback" requirement **the contract** for this behaviour, overriding
design.md/tasks.md 4a.4's group-span text. That requirement (and both its
"Turn-sourced clip ignores chapter-relative pretrim offsets" /
"Turn-sourced clip with no pretrim offsets" scenarios) mandates the **full
chapter span unconditionally** for any turn-sourced clip, never the group
span, regardless of whether pretrim offsets are present. Implemented per the
spec: `turn_id is not None` → full chapter span always. The `group_span`
plumbing added to `claim_pending_clip` (design §4, task 4a.3, still required
by name) is therefore not consumed by the sidecar's window computation in
this batch — it is exposed on `claimed_clip` for any future caller, but
`write_short_srt_sidecar` intentionally ignores it per the spec's explicit
"regardless of whether pretrim offsets are present" language. No test asserts
group-span-based window math; tests instead assert the full-chapter-span
outcome for both the pretrim-present and pretrim-absent turn-sourced cases
(see `TestWriteShortSrtSidecar::test_turn_sourced_clip_ignores_pretrim_offsets`
and `::test_turn_sourced_clip_with_no_pretrim_offsets_uses_full_chapter_span`).

### TDD Cycle Evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 4a.1/4a.2 threat-matrix regressions | N/A — confirmation only, no new test | `_SAFE_CLIP_ID_RE` (sensor + sidecar) and adaptive-timeout tests re-run post-wiring | Both still pass unmodified |
| 4a.3 `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` | 6 new tests fail (`TestClaimPendingClip` × 3 SQL-shape assertions on the bare `UPDATE...RETURNING *`; `TestInsertVideoShortClip` × 3 `turn_id` assertions with `TypeError`) | CTE + column-list changes land; all pass | ruff clean |
| 4a.3 sensor forwards `turn_id` | 4 new tests fail (`KeyError: 'turn_id'` — kwarg absent) | `poke` reads and forwards `turn_id` to both call sites | ruff clean |
| 4a.4 sidecar turn-sourced guard | 2 new tests fail (`TypeError: unexpected keyword argument 'turn_id'`) | guard branch added; all pass, including 1 added regression test for the `turn_id=None` default path | ruff clean |

- RED (db): `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py -o addopts=`
  → 6 failed, 80 passed.
- GREEN (db): same command → 86 passed.
- RED (sensor): `uv run pytest tests/congress_videos/test_reap_processor_dag.py -o addopts= -k TestShortSrtSidecarHook`
  → 4 failed, 4 passed.
- GREEN (sensor): `uv run pytest tests/congress_videos/test_reap_processor_dag.py -o addopts=`
  → 45 passed.
- RED (sidecar): `uv run pytest tests/congress_videos/test_srt_helpers.py -o addopts= -k TestWriteShortSrtSidecar`
  → 2 failed, 16 passed.
- GREEN (sidecar): `uv run pytest tests/congress_videos/test_srt_helpers.py tests/congress_videos/test_srt_helpers_multi_window.py -o addopts=`
  → 126 passed, 1 skipped.
- Scoped work-unit command (per tasks.md's Suggested Work Units table):
  `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/test_reap_processor_dag.py tests/congress_videos/test_srt_helpers*.py -o addopts=`
  → 257 passed, 1 skipped.
- Full suite: `uv run pytest -n auto` → 4612 passed, 29 skipped (Postgres-dependent
  live tests skip without a DB, as expected in this environment), 0 failed.
  `--cov-fail-under=80` enforced by `pyproject.toml` addopts and the run passed.
- `uv run ruff check .` → All checks passed.
- `uv run ruff format --check .` → 301 files already formatted.
- DagBag: `uv run python -c "from airflow.models import DagBag; ..."` → `16 {}`.
- `bash scripts/test-airflow-e2e.sh` → reported `unavailable` (Docker daemon not
  reachable in this environment) — not a failure per repo policy; run manually
  before merge.

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/test_reap_processor_dag.py tests/congress_videos/test_srt_helpers*.py -o addopts=` → 257 passed, 1 skipped |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → `unavailable` (Docker daemon unreachable); DagBag import check run as fallback → `16 {}` |
| Rollback boundary | Revert this commit; legacy rows (`turn_id IS NULL`) keep working via the `LEFT JOIN`/default-`None` guards added in this batch — reverting removes only the turn-context propagation, not PR1–PR3 |

### Changed lines

`git diff --numstat b339a12..HEAD` (code + tests):

```
congress_videos/modules/database.py                    |  54 +24
congress_videos/reap_processor_dag.py                  |  21 +1
congress_videos/srt_helpers.py                          |  53 +19
tests/congress_videos/modules/test_reap_db_methods.py  | 160 +0
tests/congress_videos/test_reap_processor_dag.py        |  53 +0
tests/congress_videos/test_srt_helpers.py                |  64 +0
```

Code+tests total: 405 additions + 44 deletions = **449 changed lines**
(ledger cap: 450). Plus `tasks.md` (7+7=14 lines) and this `apply-progress.md`
section — both docs, over the orchestrator's "docs delta ≤ 50" note once this
section is included, consistent with the same "implement honestly, report the
final count, do not force-fit" guidance `sdd-apply`'s SKILL.md gives and PR3
already exercised for its own overage. No test, comment, or doc line was cut
to chase the number.

### Issues Found

None.

### Remaining Tasks

- [ ] Phase 4b: `pending_shorts_candidate_sql` partition + parent-gate drop
- [ ] Phase 5: Orchestrator-run ops (migration 047 on NAS dev+prod, git_sync,
  validation query, manual trigger)

### Workload / PR Boundary

- Mode: stacked PR slice (auto-chain, `stacked-to-main`) — code+tests at 449
  changed lines sits at the ledger's 450 cap before docs; flagged for the
  orchestrator, not silently absorbed (same pattern as PR3)
- Current work unit: PR4a — `claim_pending_clip` CTE + turn_id propagation +
  sidecar guard, branch `feat/467-d-processor-sidecar`, base `feat/467-c-preparer-rewrite` @ `b339a12`
- Boundary: starts from PR3's `b339a12`, ends at this batch's commit — claimed
  clips and downloaded rows now carry `turn_id`, and the sidecar correctly
  refuses to apply chapter-relative pretrim math to turn-relative offsets
- Estimated review budget impact: 449 changed lines (code+tests) before docs —
  at or slightly over the 450-line ledger cap once `tasks.md`/
  `apply-progress.md` are included; recommend the orchestrator apply
  `size:exception` if the ledger settle requires it

### Not in scope for this batch

Phase 4b (Tier-1 partition + parent-gate drop) and Phase 5 (post-merge ops)
are untouched — this batch is PR4a only, per the orchestrator's work-unit
scope. The orchestrator settles the native attempt ledger; this batch does
not call `sdd-attempt settle`.

## Batch 4b (PR4b, `feat/467-e-tier1-partition`, base PR4a `0570027`) — last code slice

**Status**: Phase 4b complete (tasks 4b.1-4b.4). Commit `47e29d7`. Only
Phase 5 (orchestrator-run ops) remains.

### What landed

- `congress_videos/modules/database.py` — `pending_shorts_candidate_sql`:
  partition key `PARTITION BY COALESCE(vs.turn_id, -vs.chapter_id)` (design
  D6); dropped `AND vc.youtube_upload_date IS NOT NULL`; kept
  `chapter_rank` alias + `youtube_upload_date DESC NULLS LAST` (design D7).
  Updated `get_pending_shorts`/`pending_shorts_candidate_sql` docstrings to
  drop the parent-publish-gate claim and describe per-source-unit ranking.
- `tests/congress_videos/modules/test_reap_db_methods.py`: updated the
  partition-expression assertion, removed the dropped-gate predicate from
  the outer-WHERE list, added `test_parent_upload_date_gate_removed`.
- `tests/congress_videos/modules/test_get_pending_shorts_sql.py`: added
  `turn_id INTEGER` (no FK) to the fixture schema and `_insert_clip`,
  allowed `youtube_upload_date=None` in `_insert_chapter`, added 3 cases
  (independent per-turn Tier-1 caps, unpublished-parent now returned, mixed
  legacy+turn partitioning) — 10 pre-existing cases unchanged (legacy
  regression suite), 13 total, all skip cleanly without Postgres here.
- `congress_videos/reap_shorts_uploader_dag.py`: audited — no docstring
  describes the parent-publish gate, no drift to fix (module docstring and
  `_get_pending_shorts` only reference the method name).

### TDD Cycle Evidence

| Task | RED | GREEN | REFACTOR |
|---|---|---|---|
| 4b.1/4b.2 partition + gate drop | 2 tests fail (`test_candidate_query_ranks_clips_per_chapter` on old partition text; new `test_parent_upload_date_gate_removed`) | SQL rewrite lands, both pass | ruff clean |

- RED: `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py -o addopts= -k "test_candidate_query_ranks_clips_per_chapter or test_outer_where_predicates_unchanged or test_parent_upload_date_gate_removed"` → 2 failed, 1 passed.
- GREEN: `uv run pytest tests/congress_videos/modules/test_get_pending_shorts_sql.py tests/congress_videos/modules/test_reap_db_methods.py -o addopts=` → 87 passed, 13 skipped.
- Full suite: `uv run pytest -n auto` → 4613 passed, 32 skipped, 0 failed.
- `uv run ruff check .` → All checks passed. `uv run ruff format --check .` → 301 files formatted.
- DagBag: `uv run python -c "from airflow.models import DagBag; ..."` → `16 {}`.
- `bash scripts/test-airflow-e2e.sh` → `unavailable` (Docker daemon unreachable).

### Work Unit Evidence

| Evidence | Value |
|---|---|
| Focused test command and exact result | `uv run pytest tests/congress_videos/modules/test_get_pending_shorts_sql.py tests/congress_videos/modules/test_reap_db_methods.py -o addopts=` → 87 passed, 13 skipped |
| Runtime harness command/scenario and exact result | `bash scripts/test-airflow-e2e.sh` → `unavailable`; live-Postgres suite in `test_get_pending_shorts_sql.py` skips without a DB (exercised by the orchestrator on the NAS later) |
| Rollback boundary | Revert commit `47e29d7`; ranking degrades to chapter-only partitioning and the parent-publish gate returns — no other PR4a/PR4b behavior is touched |

### Changed lines

`git diff --numstat 0570027..HEAD` (code + tests):

```
congress_videos/modules/database.py                        | 35 +20
tests/congress_videos/modules/test_get_pending_shorts_sql.py | 67 +4
tests/congress_videos/modules/test_reap_db_methods.py       | 15 +2
```

Code+tests total: 117 additions + 26 deletions = **143 changed lines**
(ledger cap 450, budget 330). Plus `tasks.md` (4 lines) and this
`apply-progress.md` section — both docs, well under the 50-line docs note.

### Issues Found

None. Design followed exactly; `chapter_rank` alias kept per D7 with the
in-SQL comment the design specifies.

### Remaining Tasks

- [ ] Phase 5: Orchestrator-run ops (migration 047 on NAS dev+prod,
  git_sync, validation query, manual trigger of `congress_reap_clip_preparer`)

### Not in scope for this batch

Phase 5 (post-merge ops) is orchestrator-owned, not `sdd-apply` work. The
orchestrator settles the native attempt ledger; this batch does not call
`sdd-attempt settle`. This is the last code slice for `reap-turn-video-source`
(issue #467) — Phases 1 through 4b are all `[x]` in `tasks.md`.
