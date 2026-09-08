# Exploration: reap-turn-video-source (issue #467)

Date: 2026-09-07. Worktree branch `feat/467-reap-turn-source` from `origin/dev` @ 59c4fea.

## Orchestrator-confirmed product decisions (pre-proposal handoff)

The user authorised the recommended option for every open decision ("sigue lo recomendado", user AFK). These are **confirmed**, not open:

| Decision | Confirmed choice |
| --- | --- |
| Parent-published gate (clip generation, `get_chapters_for_shorts` `vc.is_uploaded_to_youtube = TRUE`) | **Dropped.** Reap generation is governed by diarization/materialization throughput only. |
| Parent-published gate (short publication, `pending_shorts_candidate_sql` `vc.youtube_upload_date IS NOT NULL`) | **Dropped.** Accepted tradeoff: a short may publish before its long-form parent is live; `_format_own_channel_footer` is already null-safe. |
| `video_shorts` turn reference / dedup key | **`turn_id INTEGER REFERENCES speaker_turn_videos(turn_id) ON DELETE SET NULL`**, selection picks a deterministic representative `turn_id` per `output_path` (`DISTINCT ON (output_path) ORDER BY output_path, turn_id`). Dedup guard becomes `NOT EXISTS (... WHERE vs.turn_id = candidate.turn_id)`. `chapter_id` stays populated. |
| Tier-1 universe in `get_pending_shorts` | **Per `turn_id`** (`PARTITION BY COALESCE(vs.turn_id, ...)`: legacy rows with `turn_id IS NULL` keep partitioning by `chapter_id`). Ranking CTE stays unfiltered by upload state (#262 discipline). |
| `get_chapters_for_shorts` | **Deleted**, not kept as fallback. |
| `prepared_at IS NOT NULL` gate from `uploadable_turns` | **Explicit non-gate** for Reap eligibility. |
| Relevance threshold on generation | **None** (Reap scores virality itself). Ordering: `COALESCE(interest_score,1) DESC, relevance_score DESC, session_date DESC, turn_id ASC`. |
| Research lane (`sdd-research`) | **Unselected** (internal refactor, no external evidence needed). |

Production evidence 2026-09-07 (`production` schema on the NAS): 46 `output_path` groups; 28 within 120–900 s; 4 over 900 s; 14 under 120 s; 11 uploaded; 0 procedural. `speaker_turn_videos` has **no** `chapter_id` column — chapters are reached through `speaker_turns.chapter_id`. `speaker_turns` has `start_seconds/end_seconds/interest_score/is_procedural` (no `group_*` columns; group span = MIN/MAX per `output_path`). `video_shorts` today: downloaded/not-uploaded 623, downloaded/uploaded 591, done 156, failed 10, invalid 5, processing 3. Last applied migration in both schemas: 046.

## Current State

**Three-DAG Reap chain** (`congress_videos/reap_clip_preparer_dag.py`, `reap_processor_dag.py`, `reap_shorts_uploader_dag.py`):

1. **Preparer** (`congress_reap_clip_preparer`, 05:15 UTC daily): `_query_chapters` (ShortCircuit) calls `db.get_chapters_for_shorts(...)` (`congress_videos/modules/database.py:507-554`) — selects `video_chapters` rows where `is_uploaded_to_youtube = TRUE AND relevance_score >= min_relevance_score (default 3) AND duration BETWEEN 120–900s AND NOT EXISTS (video_shorts row for chapter_id)`. `_extract_and_pretrim_clip` (`reap_clip_preparer_dag.py:133-315`) re-runs the same query, then for each chapter: `_find_source_video(video_id)` scans `DOWNLOADS_DIR` (line 59-70), `split_video_chapter(...)` re-cuts the chapter from the raw source (line 168-174), pre-trims via `find_srt_for_chapter`/`select_pretrim_window`/`_ffmpeg_extract_window` when duration > `pre_trim_threshold_secs` (default 600s), validates with `ffprobe`, then `db.insert_video_short(chapter_id=..., reap_status="pending", staged_clip_path=..., ...)` (`database.py:556-615`).
2. **Processor** (`congress_reap_processor`, 14:30 & 17:30 UTC): `claim_pending_clip()` (`database.py:716-763`) atomically claims one `pending` `video_shorts` row via `SELECT ... FOR UPDATE SKIP LOCKED`, ordered by `session_date DESC NULLS LAST, relevance_score DESC NULLS LAST` (both subqueries join back through `chapter_id`). Uploads to Reap, creates a job, and `ReapJobSensor.poke` (`reap_processor_dag.py:107-204`) downloads each resulting clip via `db.insert_video_short_clip(chapter_id=..., reap_clip_id=..., ...)` (`database.py:617-670`), status `downloaded`.
3. **Uploader** (`reap_shorts_uploader`, 5x/day): `get_pending_shorts()` (`database.py:803-896`) — two queries: an upload-history query (`vs.is_uploaded = TRUE ... ORDER BY updated_at DESC LIMIT 50`) and a candidate query built by `pending_shorts_candidate_sql()` (`database.py:67-105`) — a `ranked` CTE ranks `downloaded, non-abandoned` clips `PARTITION BY vs.chapter_id ORDER BY reap_virality_score DESC`, tiers them (`chapter_rank <= SHORTS_TIER1_PER_CHAPTER_LIMIT=3` → Tier 1, else Tier 2), then the outer query filters `is_uploaded=FALSE, is_upload_abandoned=FALSE, local_file_path IS NOT NULL, reap_status='downloaded', virality >= min, vc.youtube_upload_date IS NOT NULL` and orders by `tier ASC, vc.youtube_upload_date DESC NULLS LAST, virality DESC`. `filter_shorts_by_source_cooldown()` (pure function, `database.py:25-64`) then excludes candidates whose source video had fewer than `SHORTS_SOURCE_VIDEO_COOLDOWN=5` other-video uploads since its last upload. `mark_short_uploaded(reap_clip_id, youtube_video_id)` (`database.py:898-919`) sets `is_uploaded=TRUE`; `record_short_upload_failure` (`database.py:921+`) increments `upload_attempts`/`is_upload_abandoned` at threshold 3.

**`video_shorts` schema** (`congress_videos/sql/production_schema.sql:123-162`): `chapter_id INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE`, `pretrim_start_secs/end_secs/used_srt`, `reap_project_id`, `reap_clip_id UNIQUE`, `reap_status`, `reap_virality_score`, `reap_clip_url`, `local_file_path`, `youtube_video_id`, `is_uploaded`, `staged_clip_path`, `scoring_reasoning`, `upload_attempts`, `is_upload_abandoned`, `last_upload_error`. No turn reference exists today. Dedup guard lives in `get_chapters_for_shorts`'s `NOT EXISTS (... WHERE vs.chapter_id = vc.chapter_id)`.

**`speaker_turn_videos`** (`production_schema.sql:292-346`): one row per `turn_id` (`UNIQUE (turn_id)`), `output_path TEXT NOT NULL` shared across grouped turns (issue #129), `is_uploaded_to_youtube`, `prepared_at`, `is_upload_abandoned`, `keep_intervals` (procedural excision plan, #143). **`speaker_turns`** (`production_schema.sql:244-268`): `start_seconds/end_seconds NUMERIC`, `interest_score`, `is_procedural BOOLEAN NOT NULL DEFAULT FALSE`, `UNIQUE (chapter_id, start_seconds)`.

**`uploadable_turns` view** (`production_schema.sql:529-625`, mirrored by migration `044_deterministic_turn_publish_order.sql`) is the exact template for the new selection SQL: a `group_spans` CTE computes `MIN(start_seconds)/MAX(end_seconds)/SUM(procedural_seconds)` **unfiltered**, over ALL rows per `output_path` (the #151 trap — filtering before the aggregate would truncate the span); then `DISTINCT ON (stv.output_path) ... ORDER BY stv.output_path, stv.turn_id` picks one deterministic representative row per file; outer `WHERE dedup.group_end_seconds - dedup.group_start_seconds - dedup.procedural_seconds >= 300` enforces the duration floor; gates: `stv.is_uploaded_to_youtube = FALSE`, `stv.prepared_at IS NOT NULL`, `NOT stv.is_upload_abandoned`, `vc.is_uploaded_to_youtube = FALSE`, `vc.relevance_score >= 2`, `COALESCE(interest_score,1) >= 1`, `NOT COALESCE(is_procedural, FALSE)`. Order: `COALESCE(interest_score,1) DESC, relevance_score DESC, session_date DESC, materialized_at ASC (FIFO), turn_id ASC` (backstop).

**`youtube_upload_dag.py`** (docstring lines 1-31) confirms: exactly ONE turn/day publishes via `uploadable_turns LIMIT 1`; `mark_chapter_uploaded` fires **as a side effect** of that single daily turn upload — this is the literal mechanism behind the issue's "chapters flagged at ~1/day" claim. `video_chapters.is_uploaded_to_youtube` is NOT an independent long-form-chapter-upload signal anymore; it is downstream of the turn cadence.

**Pre-trim path today**: `_find_source_video` (raw downloaded source scan), `split_video_chapter` (re-cut chapter from raw source), then conditional `_ffmpeg_extract_window` pre-trim using `find_srt_for_chapter`/`select_pretrim_window` when `duration > pre_trim_threshold_secs` (default 600s). All three become unnecessary for turn videos: `output_path` is already materialized; pre-trim only applies above the Reap ceiling (900s, only 4/46 groups today).

**Tests**: `tests/congress_videos/modules/test_reap_db_methods.py` mocks `psycopg2.connect` with a `MagicMock` cursor/connection (fixture `db`; pattern: patch `psycopg2.connect`, assert on `mock_cursor.execute.call_args`). `tests/congress_videos/modules/test_get_pending_shorts_sql.py` covers `pending_shorts_candidate_sql()`. `tests/congress_videos/test_reap_clip_preparer_dag.py`, `test_reap_processor_dag.py`, `test_reap_uploader_dag.py` use an in-memory XCom `MagicMock` TaskInstance double (`_make_ti()`), test DAG structure and task callables in isolation, no live Postgres. Live-Postgres tests are gated separately and are out of scope for this SQL-text/mock-cursor test style.

**Migrations**: numbered sequentially (`047` is next after `046_add_speaker_resolution_evidence.sql`), UP-only executable (DOWN block **must stay commented** `--`, `utils/migrations_dag.py` runs the whole file in one transaction), idempotent (`ADD COLUMN IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`). `production_schema.sql` is a **static-text snapshot** (no live DB), verified by `tests/congress_videos/sql/test_production_schema.py`: `TABLE_COLUMNS` dict lists every column per table (must add `turn_id`), `_normalize_view_sql()` diffs the snapshot's view text against the latest view migration file verbatim. **Any new index/column on `video_shorts` MUST be added to `TABLE_COLUMNS` and, if applicable, `FK_QUALIFICATIONS`**, and a companion index statement in the INDEXES section (pattern: `idx_speaker_turn_videos_uploadable`, `037_upload_path_indexes.sql`).

## Affected Areas

- `congress_videos/modules/database.py:507-554` — `get_chapters_for_shorts` replaced by a new turn-sourced selection method (model: `select_unprepared_turns` at `database.py:1259-1318`, which already joins `speaker_turn_videos + speaker_turns + video_chapters + youtube_source_videos` with the `DISTINCT ON (output_path)` dedup pattern, minus the `uploadable_turns` duration-floor/procedural logic which must be folded in).
- `congress_videos/modules/database.py:556-615` (`insert_video_short`) — needs a new `turn_id` parameter.
- `congress_videos/modules/database.py:716-763` (`claim_pending_clip`) — ordering subqueries currently join only through `chapter_id`; keep or extend with turn context (e.g. `interest_score`).
- `congress_videos/modules/database.py:67-105` (`pending_shorts_candidate_sql`) and `803-896` (`get_pending_shorts`) — per-chapter `PARTITION BY vs.chapter_id` tiering and the `vc.youtube_upload_date IS NOT NULL` parent gate.
- `congress_videos/reap_clip_preparer_dag.py:59-331` — `_find_source_video`, `split_video_chapter` call, and `_extract_and_pretrim_clip` all need rewriting to consume `output_path` directly; `pre_trim_threshold_secs`/`pre_trim_target_secs` DAG params (currently 600/600) should move toward 900 to match the Reap ceiling.
- `congress_videos/sql/production_schema.sql:123-162` (`video_shorts` table + `386-391` indexes) — new turn reference column + index.
- `congress_videos/sql/migrations/` — new `047_*.sql`.
- `tests/congress_videos/sql/test_production_schema.py:81,203-227` (`TABLE_COLUMNS["video_shorts"]`) — add the new column.
- `tests/congress_videos/modules/test_reap_db_methods.py` — new/updated test classes for the new selection method, dedup key, and zero-eligible warning.
- `tests/congress_videos/test_reap_clip_preparer_dag.py` — pre-trim/extraction rewrite tests.
- `congress_videos/config/paths.py:238-276` (`get_chapter_short_file_path/srt_path`) — unaffected (still keyed by `chapter_id`, which stays populated for context).

## Approaches considered

1. **`turn_id` FK to `speaker_turn_videos(turn_id)`, representative-row dedup** (CONFIRMED) — selection mirrors `uploadable_turns`'s `DISTINCT ON (output_path) ORDER BY output_path, turn_id`; `video_shorts.turn_id INTEGER REFERENCES speaker_turn_videos(turn_id) ON DELETE SET NULL`; dedup guard `NOT EXISTS (... WHERE vs.turn_id = candidate.turn_id)`. Pros: referential integrity (`turn_id` is `UNIQUE`), no duplicated text, deterministic. Cons: sibling `speaker_turns` rows of a grouped clip are invisible from `video_shorts` (acceptable — they share `output_path` and are never independently Reap-eligible).
2. **`turn_output_path TEXT` (no FK)** — rejected: no referential integrity, duplicates derivable data, larger index, no delete semantics.

## The crux decision: parent-published gate (CONFIRMED: drop both)

- **Gate A** (clip generation, `get_chapters_for_shorts:531`): `vc.is_uploaded_to_youtube = TRUE`.
- **Gate B** (short publication, `pending_shorts_candidate_sql:99`): `vc.youtube_upload_date IS NOT NULL`.

Keeping them (re-based to `speaker_turn_videos.is_uploaded_to_youtube`) would cap Reap at 11/46 = the same ~1/day cadence the issue exists to fix. Dropping both makes generation and publication independent of the long-form cadence. YouTube-side risk (a short referencing an unpublished long) is already mitigated: `reap_shorts_uploader_dag.py:78-86` (`_format_own_channel_footer`) returns `""` when `youtube_video_id` is falsy; the `vc.youtube_upload_date DESC NULLS LAST` ordering is NULL-safe. Design must document this as an accepted tradeoff.

## Zero-eligible warning

`_query_chapters` (`reap_clip_preparer_dag.py:120-131`) only returns `bool(chapters)`; a `False` yields an Airflow skip with no distinguishing log. Add an explicit `logging.warning(...)` when the eligible-turn count is zero, citing the count. No metric infrastructure exists in this repo; a queue-depth metric is out of scope.

## Migration 047 sketch

- `ALTER TABLE video_shorts ADD COLUMN IF NOT EXISTS turn_id INTEGER REFERENCES speaker_turn_videos(turn_id) ON DELETE SET NULL;`
- `CREATE INDEX IF NOT EXISTS idx_video_shorts_turn_id ON video_shorts(turn_id);`
- DOWN block commented out per repo convention.
- `production_schema.sql`: add `turn_id` to the `video_shorts` `CREATE TABLE` block, add the index line.
- `tests/congress_videos/sql/test_production_schema.py`: add `"turn_id"` to the `video_shorts` columns, add an index assertion, add an FK qualification assertion.
- No change to `speaker_turn_videos` (all needed columns exist). `prepared_at IS NOT NULL` is a long-form-slot gate (issue #146), NOT a Reap gate — the new selection only needs `output_path IS NOT NULL`.

## Risks

- **SRT pre-trim windowing**: `find_srt_for_chapter`/`select_pretrim_window` operate on chapter-relative timestamps; a turn video's span (`MIN(start_seconds)`/`MAX(end_seconds)`) is also chapter-relative, but the turn video file itself starts at 0 (and may have procedural excisions via `keep_intervals`). Pre-trim on a turn file must therefore operate on file-relative time (ffprobe duration), not chapter-relative SRT windows; design must settle this explicitly (simplest: pre-trim = take the first `pre_trim_target_secs` of the file, or a centred window, without SRT).
- **`prepared_at` gate confusion**: copying it in would silently shrink eligibility to whatever the long-form prepare queue processed. Must be an explicit non-gate.
- **Tier-1 partition change** (`chapter_id` → `turn_id`) repeats the #262 bug shape — the ranking CTE must remain unfiltered by `is_uploaded`/`local_file_path`/virality (`test_rank_universe_includes_uploaded_clips`).
- **Backward compatibility**: legacy `video_shorts` rows have `turn_id = NULL` — `get_pending_shorts`/`claim_pending_clip` must use `LEFT JOIN`/`COALESCE`, never an inner join on `turn_id`.
- **Dropping the parent-published gate is a behavior change** — confirmed by the orchestrator on the user's standing instruction; the proposal must state it explicitly.

## Review Workload Forecast (400-line budget, auto-chain)

| Work unit | Scope | Est. changed lines |
|---|---|---|
| 1 | Migration 047 + schema snapshot + `test_production_schema.py` updates | ~120–180 |
| 2 | New turn-selection method in `database.py` (replaces `get_chapters_for_shorts`) + `insert_video_short(turn_id=...)` + unit tests | ~200–280 |
| 3 | Preparer DAG rewrite (drop `_find_source_video`/`split_video_chapter`, consume `output_path`, pre-trim threshold to 900s, zero-eligible warning) + DAG tests | ~200–280 |
| 4a | `claim_pending_clip` extension (turn-aware ordering) + tests | ~80–140 |
| 4b | `get_pending_shorts`/`pending_shorts_candidate_sql` rebase (partition by `turn_id`, drop parent-published gate) + tests | ~220–300 |

Decision needed before apply: No (decisions confirmed above). Chained PRs recommended: Yes. 400-line budget risk: Medium.

## Ready for Proposal

Yes.
