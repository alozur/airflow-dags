# Design: Source Reap shorts from diarized speaker-turn videos

## Technical Approach

Replace the chapter-sourced Reap queue with a turn-sourced one. Selection mirrors
`uploadable_turns` (`production_schema.sql:548-623`) — unfiltered `group_spans` CTE,
`DISTINCT ON (output_path)` representative row — minus the long-form publish gates.
`video_shorts` gains a nullable `turn_id`; every consumer reaches turns via
`LEFT JOIN`/`COALESCE`, so legacy `turn_id IS NULL` rows keep per-chapter behaviour.

**Correction to the launch brief**: `_REAP_CLIP_PROMPT` (`reap_processor_dag.py:36-44`)
is a **static module constant**. No chapter title, description, or session context is
interpolated into it; the only interpolation is `name=f"chapter_{chapter_id}"`
(line 283). The selection therefore does **not** need to carry chapter prose. It
carries `scoring_reasoning` (consumed by `insert_video_short`), the ordering keys, and
the group span. The Reap job name stays `chapter_{chapter_id}` — renaming it is out of
scope.

## Architecture Decisions

| # | Decision | Options | Choice + rationale |
|---|---|---|---|
| D1 | Dedup / FK key | `turn_id` FK; `turn_output_path TEXT` | **`turn_id`** — `speaker_turn_videos` has `UNIQUE (turn_id)` (025), giving referential integrity and `ON DELETE SET NULL`. Text duplicates derivable data. |
| D2 | Duration floor formula | raw span; span − procedural | **span − `procedural_seconds` ≥ 120** — the materialized file already has procedural spans excised (`keep_intervals`, #143), so the raw span overstates the real file. Same formula as `uploadable_turns`, different literal. |
| D3 | Pre-trim window | leading; centred; SRT-selected | **Leading `[0, pre_trim_target_secs]`, file-relative** — a turn is one continuous intervention whose opening carries the announcement and thesis; a centred window discards exactly that. Deterministic, no SRT lookup, and matches today's SRT-less fallback (`0.0 → target`). Affects 4/46 groups. |
| D4 | Staging strategy | copy; symlink; reference `output_path` | **Reference `output_path` directly** when no pre-trim is needed. Verified: nothing in the repo deletes, moves, or rewrites `staged_clip_path` (only `reap_processor_dag.py:236` reads it) and no `shutil.move`/`rmtree` touches turn outputs. Copying doubles I/O on a NAS already under contention; symlinks add a failure mode across bind mounts for zero gain. Pre-trimmed clips get a **new derived file** (below), never overwriting the turn video. |
| D5 | Turn-relative SRT sidecar | rebase `pretrim_*` to chapter time at insert; pass the group span to the sidecar | **Pass the group span.** Rebasing looks free but breaks the *no-pre-trim* case: NULL offsets make `write_short_srt_sidecar` fall back to the **full chapter span**, which is correct for a chapter clip and wrong for a turn. Keeping `pretrim_*` file-relative also keeps them truthful about what ffmpeg cut. |
| D6 | Tier-1 partition key | `COALESCE(vs.turn_id, -vs.chapter_id)`; two-column `COALESCE` | **`COALESCE(vs.turn_id, -vs.chapter_id)`** — `turn_id > 0` and `-chapter_id < 0` occupy disjoint domains, so no cross-table collision is possible. |
| D7 | `chapter_rank` alias | rename to `source_rank`; keep | **Keep `chapter_rank`.** `test_get_pending_shorts_sql.py` asserts it in 6 places and **skips silently without Postgres**, so a rename would go undetected by `uv run pytest`. Tradeoff accepted: the name is now mildly misleading; a SQL comment states it ranks within the *source unit* (turn group, or chapter for legacy rows). |

## Data Flow

```
speaker_turn_videos.output_path ──┐
speaker_turns (span, procedural) ─┼─→ get_turn_videos_for_shorts()
video_chapters / ysv (context) ───┘         │
                                            ▼
              preparer: ffprobe(output_path) → [pre-trim if > threshold]
                                            │
                          insert_video_short(chapter_id, turn_id, staged_clip_path)
                                            ▼
        processor: claim_pending_clip() ──→ Reap ──→ insert_video_short_clip(turn_id)
                                            │                    │
                                            └── group span ──→ write_short_srt_sidecar
                                                                 │
                     uploader: pending_shorts_candidate_sql (PARTITION BY turn_id)
```

## Interfaces / Contracts

### 1. `get_turn_videos_for_shorts` — `modules/database.py` (replaces `get_chapters_for_shorts`)

```python
def get_turn_videos_for_shorts(self, max_turns: int | None = None) -> list[dict]:
    """Materialized turn videos eligible for Reap clip generation.

    Explicit NON-gates (issue #467): stv.prepared_at (a long-form slot gate,
    #146), any relevance threshold (Reap scores virality itself), and any
    parent-published flag on video_chapters or speaker_turn_videos.
    No upper duration bound — the preparer pre-trims above the Reap ceiling.
    """
```

```sql
WITH group_spans AS (
    -- DELIBERATELY UNFILTERED over every sibling row of an output_path
    -- (issue #151 trap): gating before the aggregate collapses the span to
    -- one turn's window. is_procedural is read only to sum excised seconds.
    SELECT stv.output_path,
           MIN(st.start_seconds) AS group_start_seconds,
           MAX(st.end_seconds)   AS group_end_seconds,
           SUM(CASE WHEN st.is_procedural THEN st.end_seconds - st.start_seconds ELSE 0 END)
               AS procedural_seconds
    FROM {stv_table} stv
    JOIN {st_table} st ON stv.turn_id = st.turn_id
    GROUP BY stv.output_path
)
SELECT * FROM (
    SELECT DISTINCT ON (stv.output_path)
        stv.turn_id, stv.output_path, stv.turn_type, stv.keep_intervals,
        st.chapter_id, st.resolved_name, st.interest_score,
        gs.group_start_seconds, gs.group_end_seconds, gs.procedural_seconds,
        (gs.group_end_seconds - gs.group_start_seconds - gs.procedural_seconds)
            AS group_duration_seconds,
        vc.video_id, vc.relevance_score, vc.scoring_reasoning,
        ysv.session_number, ysv.session_date
    FROM {stv_table} stv
    JOIN {st_table} st  ON stv.turn_id = st.turn_id
    JOIN {vc_table} vc  ON st.chapter_id = vc.chapter_id
    JOIN {ysv_table} ysv ON vc.video_id = ysv.video_id
    JOIN group_spans gs ON gs.output_path = stv.output_path
    WHERE stv.output_path IS NOT NULL
      AND NOT COALESCE(st.is_procedural, FALSE)   -- issue #143
      AND NOT EXISTS (                            -- dedup on the turn, not the chapter
          SELECT 1 FROM {shorts_table} vs WHERE vs.turn_id = stv.turn_id
      )
    ORDER BY stv.output_path, stv.turn_id          -- deterministic representative
) dedup
WHERE dedup.group_duration_seconds >= 120          -- REAP_MIN_CLIP_SECONDS
ORDER BY COALESCE(dedup.interest_score, 1) DESC,
         dedup.relevance_score DESC,
         dedup.session_date DESC,
         dedup.turn_id ASC                          -- total-order backstop
```

`LIMIT %s` is appended **only** when `max_turns is not None`, mirroring today's
`limit`/`max_chapters=0 → no LIMIT` semantics. `params = [max_turns]` or `[]`.
The `NOT EXISTS` sits inside the inner query (before `DISTINCT ON`): the
representative row is picked by `turn_id ASC`, so testing the representative is
sufficient — a group is either wholly consumed or wholly free.

### 2. Migration `047_add_video_shorts_turn_id.sql`

```sql
-- Migration 047: reference the source speaker turn from video_shorts (issue #467)
-- Depends on: 025_create_speaker_turn_videos.sql, 004_create_video_shorts.sql
-- Additive and nullable: legacy rows keep turn_id = NULL and every existing
-- INSERT/UPDATE keeps working. No backfill. No view is recreated —
-- uploadable_turns does not touch video_shorts, so the 044 lockstep guard in
-- tests/congress_videos/sql/test_production_schema.py stays valid untouched.
-- Runner does `SET search_path TO {schema}, public` — names are UNQUALIFIED.

-- UP
ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS turn_id INTEGER
        REFERENCES speaker_turn_videos(turn_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_video_shorts_turn_id ON video_shorts(turn_id);

COMMENT ON COLUMN video_shorts.turn_id IS
    'Representative speaker_turn_videos.turn_id this short was cut from (issue #467); NULL = legacy chapter-sourced row';

-- DOWN
-- Manual psql only -- the runner executes the WHOLE file in ONE transaction and
-- has no rollback, so this block MUST stay commented (046 convention).
-- DROP INDEX IF EXISTS idx_video_shorts_turn_id;
-- ALTER TABLE video_shorts DROP COLUMN IF EXISTS turn_id;
```

Snapshot edits:
- `production_schema.sql:127` — add after `chapter_id`:
  `turn_id INTEGER REFERENCES production.speaker_turn_videos(turn_id) ON DELETE SET NULL, -- migration 047 (issue #467)`
- `production_schema.sql:391` — append
  `CREATE INDEX idx_video_shorts_turn_id ON production.video_shorts(turn_id);`
- `test_production_schema.py:441` — add `"turn_id"` to `VIDEO_SHORTS_COLUMNS` (20 → 21;
  update the class docstring count), add a `test_turn_id_fk_is_production_qualified`
  asserting `REFERENCES PRODUCTION.SPEAKER_TURN_VIDEOS(TURN_ID) ON DELETE SET NULL` in
  the `video_shorts` block, and add a `TestVideoShortsIndexCompleteness` case for
  `idx_video_shorts_turn_id` (pattern: `TestVideoChaptersIndexCompleteness`,
  line 518-538). `FK_QUALIFICATIONS` is for `TABLE_COLUMNS` tables only —
  `video_shorts` has its own class, so the assertion goes there, not in the tuple.

### 3. Insert signatures

```python
def insert_video_short(self, chapter_id: int, reap_project_id=None, reap_status="pending",
                       pretrim_start_secs=None, pretrim_end_secs=None, pretrim_used_srt=False,
                       staged_clip_path=None, scoring_reasoning=None,
                       turn_id: int | None = None) -> int:
```
Column list becomes `(chapter_id, turn_id, reap_project_id, ...)` with `VALUES (%s, %s, ...)`
(9 placeholders). `turn_id` is keyword-only-by-position-at-the-end so no existing call breaks.

```python
def insert_video_short_clip(self, chapter_id: int, reap_project_id: str, reap_clip_id: str,
                            reap_virality_score: float, reap_clip_url: str, local_file_path: str,
                            reap_status="downloaded", turn_id: int | None = None) -> int:
```
`ReapJobSensor.poke` already pulls `claimed_clip` (line 155) for the pre-trim offsets;
it passes `turn_id=claimed_clip.get("turn_id")`. Without this, downloaded clips would
carry `turn_id IS NULL` and collapse back into the per-chapter Tier-1 partition — the
partition is computed on the **downloaded clip rows**, not the pending parent.

### 4. `claim_pending_clip` — CTE wrapper for turn context

`RETURNING *` cannot project joined columns, so wrap the atomic claim:

```sql
WITH claimed AS (
    UPDATE {shorts_table} SET reap_status = 'processing', updated_at = CURRENT_TIMESTAMP
    WHERE id = (
        SELECT vs.id FROM {shorts_table} vs
        WHERE vs.reap_status = 'pending'
        ORDER BY ( ...unchanged session_date subquery... ) DESC NULLS LAST,
                 ( ...unchanged relevance_score subquery... ) DESC NULLS LAST
        LIMIT 1 FOR UPDATE SKIP LOCKED
    )
    RETURNING *
)
SELECT c.*, gs.group_start_seconds, gs.group_end_seconds
FROM claimed c
LEFT JOIN {stv_table} stv ON stv.turn_id = c.turn_id
LEFT JOIN LATERAL (
    SELECT MIN(st.start_seconds) AS group_start_seconds,
           MAX(st.end_seconds)   AS group_end_seconds
    FROM {stv_table} sib
    JOIN {st_table} st ON st.turn_id = sib.turn_id
    WHERE sib.output_path = stv.output_path
) gs ON TRUE
```

Ordering, `FOR UPDATE SKIP LOCKED`, and the two ordering subqueries are **carried
forward verbatim**. Legacy rows (`turn_id IS NULL`) yield `NULL` spans through the
`LEFT JOIN`, which is exactly the sidecar's chapter fallback signal.

### 5. `pending_shorts_candidate_sql`

```sql
ROW_NUMBER() OVER (
    -- Rank within the SOURCE UNIT: the turn group for turn-sourced rows
    -- (issue #467), the chapter for legacy turn_id IS NULL rows. turn_id > 0
    -- and -chapter_id < 0 are disjoint, so the two domains cannot collide.
    -- Alias kept as chapter_rank for consumer compatibility.
    PARTITION BY COALESCE(vs.turn_id, -vs.chapter_id)
    ORDER BY vs.reap_virality_score DESC NULLS LAST, vs.id ASC
) AS chapter_rank
```

The ranking CTE stays **unfiltered by upload state** — no `is_uploaded`,
`local_file_path`, or virality predicate moves into it (#262;
`test_uploaded_top_clips_consume_chapter_tier1_slots`). Delete only
`AND vc.youtube_upload_date IS NOT NULL` from the outer `WHERE`. The
`ORDER BY ... vc.youtube_upload_date DESC NULLS LAST` is already NULL-safe and stays.

`test_get_pending_shorts_sql.py` changes: add `turn_id INTEGER` to `_SCHEMA_SQL`'s
`video_shorts` (no FK — the fixture has no `speaker_turn_videos`), add `turn_id=None`
to `_insert_clip`, allow `youtube_upload_date=None` in `_insert_chapter`. The 8 existing
tests never set `turn_id`, so they become the legacy-fallback regression suite unchanged.
Add three: two turn groups inside one chapter get independent Tier-1 caps; a chapter with
`youtube_upload_date IS NULL` is now returned; a mixed legacy+turn chapter partitions
both ways in one query.

### 6. Preparer rewrite — `reap_clip_preparer_dag.py`

Deletions: `_find_source_video` (local, no other caller — the other DAGs have their own
`_find_source_video_any_date`), the `split_video_chapter` import (the function itself
stays: `video_splitter.py:368` calls it and `test_video_splitter.py` covers it), the
`_interval_to_srt` helper, `DOWNLOADS_DIR` and `find_srt_for_chapter`/
`select_pretrim_window` imports. `_ffmpeg_extract_window` is **kept** (pre-trim still
needs it) with its tests.

`params`: `max_chapters` → `max_turns` (0 = no limit), `min_relevance_score` **removed**
(no longer a gate), `pre_trim_threshold_secs`/`pre_trim_target_secs` `600 → 900` to match
the Reap ceiling. Task ids, count (4), schedule, and chain are unchanged.

```python
def _query_turns(ti, **context):
    turns = CongressionalVideoDB().get_turn_videos_for_shorts(
        max_turns=context["params"]["max_turns"] or None)
    if not turns:
        logging.warning(
            "No eligible turn videos for Reap: 0 groups passed "
            "(output_path present, non-procedural, span >= 120s, not already queued) "
            "— skipping this run")
    return bool(turns)

def _stage_and_pretrim_clip(ti, **context):
    # per turn: ffprobe(output_path) -> actual_secs (authoritative; the SQL floor
    # is an estimate over start/end_seconds).
    #   actual_secs < 120                -> logging.warning, skip
    #   actual_secs <= threshold_secs    -> staged = output_path, pretrim_* = None
    #   actual_secs >  threshold_secs    -> leading window [0, target_secs]:
    #       reencode = reencode_for_codec(get_cached_codec(output_path, codec_cache))
    #       _ffmpeg_extract_window(output_path, staged, 0.0, target_secs, reencode)
    #       staged = PROJECT_DATA_DIR/{video_id}/{chapter_id}/turn_{turn_id}_reap.mp4
    #       pretrim_start_secs, pretrim_end_secs = 0.0, float(target_secs)
    #       pretrim_used_srt = False
    # safety gate (unchanged shape): actual > target + _FRAME_TOLERANCE_SECS -> block
    db.insert_video_short(chapter_id=..., turn_id=..., reap_status="pending",
                          staged_clip_path=staged, ...)
```

The codec is probed from `output_path` itself — unlike today, that **is** the real
source, so the "reuse the raw source's decision" comment at lines 156-160/213-217 is
deleted with the code it explains.

### 7. `write_short_srt_sidecar` guard — `srt_helpers.py:494`

Two new optional params, defaulting to `None` so the chapter path is byte-identical:

```python
    turn_group_start_secs: float | None = None,
    turn_group_end_secs: float | None = None,
```

```python
# Turn-sourced short (issue #467): staged_clip_path is the materialized turn
# video, whose t=0 is the CHAPTER-RELATIVE group start. pretrim_* are
# file-relative offsets into that file, so the chapter-SRT window is
# chapter_start + group_start + pretrim_*. With no pre-trim the window is the
# whole group span -- NOT the chapter span, which is the legacy fallback.
turn_base = _coerce_pretrim_offset(turn_group_start_secs)
turn_end = _coerce_pretrim_offset(turn_group_end_secs)
if turn_base is not None and turn_end is not None:
    origin = chapter_start_secs + turn_base
    if pretrim_start is None or pretrim_end is None:
        window_start, window_end = origin, chapter_start_secs + turn_end
    else:
        window_start, window_end = origin + pretrim_start, origin + pretrim_end
else:
    ...existing chapter branch, unchanged...
```

The existing `window_valid` overlap check against the chapter span and its fallback run
afterwards for both branches; for a turn the fallback target becomes the **group span**,
not the chapter span. `_write_short_sidecar_best_effort` forwards the two spans from
`claimed_clip`. Known approximation (already the documented contract): when a group
carries excised procedural spans (`keep_intervals`, #143), file time drifts from
group-relative time by the excised seconds. Not inverted here — the sidecar window is an
approximation by contract, and Reap exposes no per-clip timing anyway.

### 8. Surface guard

`tests/congress_videos/modules/test_database_surface.py`: move `get_chapters_for_shorts`
from `LIVE_METHOD_NAMES` to `DEAD_METHOD_NAMES` (a free deletion guard) and add
`get_turn_videos_for_shorts` to `LIVE_METHOD_NAMES`.

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql` | Create | Column + index + comment, DOWN commented |
| `congress_videos/sql/production_schema.sql` | Modify | `video_shorts.turn_id` + index line |
| `congress_videos/modules/database.py` | Modify | `-get_chapters_for_shorts`, `+get_turn_videos_for_shorts`, `insert_video_short(turn_id=)`, `insert_video_short_clip(turn_id=)`, `claim_pending_clip` CTE, `pending_shorts_candidate_sql` partition + gate drop |
| `congress_videos/reap_clip_preparer_dag.py` | Modify | Turn-sourced staging, leading pre-trim, zero-eligible WARNING, dead-code removal |
| `congress_videos/reap_processor_dag.py` | Modify | Forward `turn_id` and the group span from `claimed_clip` |
| `congress_videos/srt_helpers.py` | Modify | Turn-relative sidecar window guard |
| `tests/congress_videos/sql/test_production_schema.py` | Modify | Column, FK, index assertions |
| `tests/congress_videos/modules/test_reap_db_methods.py` | Modify | Selection SQL, dedup key, insert params |
| `tests/congress_videos/modules/test_get_pending_shorts_sql.py` | Modify | `turn_id` fixture column + 3 new cases |
| `tests/congress_videos/modules/test_database_surface.py` | Modify | Method surface swap |
| `tests/congress_videos/test_reap_clip_preparer_dag.py` | Modify | Rewrite the extraction suite |
| `tests/congress_videos/test_reap_processor_dag.py`, `tests/congress_videos/test_srt_helpers*.py` | Modify | `turn_id` / span propagation |

## Testing Strategy

`strict_tdd: true` — write the failing test before each behaviour.

| Layer | What | How |
|---|---|---|
| Unit (SQL text) | Gates present/absent, ordering keys, `NOT EXISTS vs.turn_id`, `LIMIT` only when `max_turns`, partition expression, dropped `youtube_upload_date` gate | `MagicMock` cursor, assert on `mock_cursor.execute.call_args` (`test_reap_db_methods.py` fixture) |
| Unit (DAG) | Zero-eligible WARNING text, no-pre-trim references `output_path`, >threshold writes `turn_{id}_reap.mp4` with offsets `(0, 900)`, <120s skip, safety-gate block, `turn_id` reaches `insert_video_short` | `mocker.patch` + `_make_ti()` (no live DB, no ffmpeg) |
| Unit (sidecar) | Turn branch window math; no-pre-trim turn uses the group span; `None` spans reproduce today's chapter behaviour exactly | `tmp_path` SRT fixtures |
| Unit (schema) | Snapshot column/FK/index drift | static SQL-text assertions |
| Integration (live PG, skips without it) | Per-turn Tier-1 cap, legacy chapter fallback, unfiltered ranking universe (#262), NULL `youtube_upload_date` now returned | `test_get_pending_shorts_sql.py` disposable schema |
| E2E | DagBag import errors == 0 | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) |

## Threat Matrix

Applicable — the preparer builds and runs `ffmpeg`/`ffprobe` subprocesses.

| Row | Status | Expected behaviour | RED test |
|---|---|---|---|
| Command injection via path | **Applicable** | `output_path` comes from the DB, never a shell. `subprocess.run` is called with a **list**, never `shell=True`; `build_ffmpeg_cut_cmd` is reused unchanged. | Assert the ffmpeg call receives a list and no `shell=True` |
| Path traversal in derived paths | **Applicable** | The pre-trim destination is composed from integer `video_id`/`chapter_id`/`turn_id`, never from external text. `clip_id` traversal is already guarded by `_SAFE_CLIP_ID_RE` in both the sensor and `write_short_srt_sidecar`. | Existing `_SAFE_CLIP_ID_RE` tests stay green |
| Subprocess timeout / hang | **Applicable** | `compute_ffmpeg_timeout(duration)` unchanged; ffprobe keeps `timeout=30`. | Assert the adaptive timeout is passed (existing test) |
| Destructive filesystem op | **Applicable** | The turn video is **read-only** to this pipeline. No `unlink`/`move`/`rmtree` is introduced; pre-trim always writes a **new** path. | Assert `staged_clip_path != output_path` whenever a pre-trim ran |
| Routing / VCS-PR automation / executable classification | **N/A** | No routing, git, or executable-file handling in this change. | — |

## Migration / Rollout

1. Merge the chain in order (below). 2. Deploy via `git_sync`. 3. Apply `047` on the
NAS through `migrations_dag` in **both** `development` and `production` schemas (repo
policy — the migration is not applied by deploy). 4. Confirm
`airflow dags list-import-errors` is empty on both stacks.

**Work units** (auto-chain; each ≤400 changed lines incl. tests, each green under
`uv run pytest` with the 80% coverage gate, each DagBag-importable):

| PR | Scope | Est. |
|---|---|---|
| 1 | Migration 047 + snapshot + schema tests | ~150 |
| 2 | `get_turn_videos_for_shorts` + `insert_video_short(turn_id=)` + surface swap + tests | ~280 |
| 3 | Preparer rewrite + dead-code removal + DAG tests | ~280 |
| 4a | `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` + processor/sidecar wiring + tests | ~200 |
| 4b | `pending_shorts_candidate_sql` partition + parent-gate drop + tests | ~260 |

Order is load-bearing: 1 and 2 are inert without a caller, so the DAG rewrite (3) can
never reference a column that does not exist yet; 4b lands last because it is the only
unit that changes already-published behaviour. PR 1 targets `feat/467-reap-turn-source`;
each later PR targets its immediate predecessor.

**Pre-merge validation** (read-only, prod schema — expects ~28 rows):

```sql
WITH group_spans AS (
    SELECT stv.output_path, MIN(st.start_seconds) AS s, MAX(st.end_seconds) AS e,
           SUM(CASE WHEN st.is_procedural THEN st.end_seconds - st.start_seconds ELSE 0 END) AS p
    FROM production.speaker_turn_videos stv
    JOIN production.speaker_turns st ON stv.turn_id = st.turn_id
    GROUP BY stv.output_path)
SELECT count(*) FROM (
    SELECT DISTINCT ON (stv.output_path) gs.e - gs.s - gs.p AS dur
    FROM production.speaker_turn_videos stv
    JOIN production.speaker_turns st ON stv.turn_id = st.turn_id
    JOIN group_spans gs ON gs.output_path = stv.output_path
    WHERE stv.output_path IS NOT NULL AND NOT COALESCE(st.is_procedural, FALSE)
    ORDER BY stv.output_path, stv.turn_id) d
WHERE d.dur >= 120;
```

**Rollback**: revert-only, in reverse merge order (4b → 4a → 3 → 2 → 1). `047` stays
applied — `turn_id` is additive, nullable, and ignored by reverted code (repo convention
keeps DOWN commented). Deploy-free stop-gap: pause `congress_reap_clip_preparer`.

**Observability**: the zero-eligible WARNING names the count and every gate; the preparer
already logs per-clip decisions and `clips_queued`. No metric infrastructure exists in
this repo and none is added (out of scope).

## Open Questions

None. Every product decision was confirmed in the pre-proposal handoff; the remaining
technical formulas (D2–D7) are settled above.
