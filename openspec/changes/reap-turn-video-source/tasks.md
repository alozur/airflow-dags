# Tasks: Source Reap shorts from diarized speaker-turn videos

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | PR1 ~150, PR2 ~280, PR3 ~280, PR4a ~200, PR4b ~260 (~1170 total) |
| 400-line budget risk | High |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 → PR 2 → PR 3 → PR 4a → PR 4b |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: High

### Suggested Work Units

| Unit | Goal | PR | Focused test | Runtime harness | Rollback boundary |
|---|---|---|---|---|---|
| 1 | Migration 047 + snapshot + schema tests | PR1 | `uv run pytest tests/congress_videos/sql/test_production_schema.py -o addopts=` | N/A — migration applied separately via `migrations_dag`, not by deploy | Revert PR; `047` additive/nullable |
| 2 | `get_turn_videos_for_shorts` + `insert_video_short(turn_id=)` + surface swap | PR2 | `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/modules/test_database_surface.py -o addopts=` | N/A — MagicMock cursor, no live DB | Revert PR; unit inert without a caller |
| 3 | Preparer rewrite: `output_path` staging, leading pre-trim, dead-code removal | PR3 | `uv run pytest tests/congress_videos/test_reap_clip_preparer_dag.py -o addopts=` | `bash scripts/test-airflow-e2e.sh` | Revert PR; pause `congress_reap_clip_preparer` |
| 4a | `claim_pending_clip` CTE + `insert_video_short_clip(turn_id=)` + processor/sidecar wiring | PR4a | `uv run pytest tests/congress_videos/modules/test_reap_db_methods.py tests/congress_videos/test_reap_processor_dag.py tests/congress_videos/test_srt_helpers*.py -o addopts=` | `bash scripts/test-airflow-e2e.sh` | Revert PR; legacy rows keep working via `COALESCE` |
| 4b | `pending_shorts_candidate_sql` partition + parent-gate drop | PR4b | `uv run pytest tests/congress_videos/modules/test_get_pending_shorts_sql.py -o addopts=` | Live-PG suite skips without Postgres | Revert PR; ranking degrades to chapter-only |

Order is load-bearing (design.md "Migration / Rollout"). PR1 base `feat/467-reap-turn-source`; each later PR bases on its predecessor.

## Phase 1: Migration + Schema (PR1, `feat/467-a-turn-id-migration`)

- [x] 1.1 RED: add `turn_id` to `VIDEO_SHORTS_COLUMNS` (20→21), `test_turn_id_fk_is_production_qualified`, `TestVideoShortsIndexCompleteness` case for `idx_video_shorts_turn_id` in `tests/congress_videos/sql/test_production_schema.py`
- [x] 1.2 GREEN: create `congress_videos/sql/migrations/047_add_video_shorts_turn_id.sql` (column, index, comment; DOWN commented)
- [x] 1.3 GREEN: edit `congress_videos/sql/production_schema.sql:127,391`
- [x] 1.4 REFACTOR: `uv run pytest`; `uv run ruff check .`; `uv run ruff format --check .`
- [x] 1.5 Commit: `feat(db): add nullable turn_id fk to video_shorts (migration 047)`

## Phase 2: Turn selection + insert (PR2, `feat/467-b-turn-selection`, base PR1)

- [ ] 2.1 RED: `get_turn_videos_for_shorts` SQL-text tests (group_spans, DISTINCT ON, dedup on turn_id, floor 120, no ceiling, LIMIT-only-if-max_turns, ordering) in `tests/congress_videos/modules/test_reap_db_methods.py`
- [ ] 2.2 RED: `insert_video_short(turn_id=)` 9-placeholder column-list test
- [ ] 2.3 RED: swap `get_chapters_for_shorts`→`DEAD_METHOD_NAMES`, add `get_turn_videos_for_shorts`→`LIVE_METHOD_NAMES` in `test_database_surface.py`
- [ ] 2.4 GREEN: `congress_videos/modules/database.py` — delete `get_chapters_for_shorts` (507-554), add `get_turn_videos_for_shorts` (design §1), extend `insert_video_short` (design §3)
- [ ] 2.5 REFACTOR: `uv run pytest`; ruff check/format
- [ ] 2.6 Commit: `feat(reap): source turn-video candidate selection from speaker turns`

## Phase 3: Preparer rewrite (PR3, `feat/467-c-preparer-rewrite`, base PR2)

- [ ] 3.1 RED (threat matrix — command injection): assert ffmpeg subprocess call is a list, never `shell=True`
- [ ] 3.2 RED (threat matrix — destructive fs op): assert `staged_clip_path != output_path` whenever pre-trim ran
- [ ] 3.3 RED: zero-eligible WARNING with count; no-pretrim stages `output_path` unmodified; over-threshold writes `turn_{id}_reap.mp4` offsets `(0.0, 900.0)`; <120s actual-duration skip; `turn_id` reaches `insert_video_short` — in `test_reap_clip_preparer_dag.py`
- [ ] 3.4 GREEN: rewrite `congress_videos/reap_clip_preparer_dag.py` — `_query_turns`/`_stage_and_pretrim_clip` (design §6); `max_chapters→max_turns`; drop `min_relevance_score`; threshold `600→900`
- [ ] 3.5 GREEN: delete `_find_source_video`, `split_video_chapter` import, `_interval_to_srt`, `DOWNLOADS_DIR`, unused SRT-window imports; keep `_ffmpeg_extract_window`
- [ ] 3.6 REFACTOR: `uv run pytest`; ruff check/format; `bash scripts/test-airflow-e2e.sh`
- [ ] 3.7 Commit: `feat(reap): stage materialized turn output directly with leading pre-trim`

## Phase 4a: Processor + sidecar wiring (PR4a, `feat/467-d-processor-sidecar`, base PR3)

- [ ] 4a.1 RED (threat matrix — path traversal, existing coverage): confirm `_SAFE_CLIP_ID_RE` tests in sensor + `write_short_srt_sidecar` stay green after wiring
- [ ] 4a.2 RED (threat matrix — subprocess timeout): confirm adaptive `compute_ffmpeg_timeout`/ffprobe `timeout=30` assertion stays green
- [ ] 4a.3 RED: `claim_pending_clip` CTE returns group span via LEFT JOIN LATERAL (design §4); `insert_video_short_clip(turn_id=)`; `ReapJobSensor.poke` forwards `turn_id=claimed_clip.get("turn_id")`
- [ ] 4a.4 RED: `write_short_srt_sidecar` turn branch — window math with both spans set, no-pretrim falls back to group span (not chapter span), `None` spans byte-identical to today — in `test_srt_helpers*.py`
- [ ] 4a.5 GREEN: implement `claim_pending_clip` CTE + `insert_video_short_clip` in `database.py`; forward turn_id/span in `reap_processor_dag.py`; extend `write_short_srt_sidecar` (`srt_helpers.py:494`, design §7)
- [ ] 4a.6 REFACTOR: `uv run pytest`; ruff check/format; `bash scripts/test-airflow-e2e.sh`
- [ ] 4a.7 Commit: `feat(reap): propagate turn context through claim and srt sidecar`

## Phase 4b: Tier-1 partition + gate drop (PR4b, `feat/467-e-tier1-partition`, base PR4a)

- [ ] 4b.1 RED: `turn_id` fixture column, `youtube_upload_date=None` allowed, 3 new cases (independent per-turn caps, NULL-upload-date returned, mixed legacy+turn) in `test_get_pending_shorts_sql.py`
- [ ] 4b.2 GREEN: `pending_shorts_candidate_sql` — `PARTITION BY COALESCE(vs.turn_id, -vs.chapter_id)` (design §5); drop `AND vc.youtube_upload_date IS NOT NULL`; keep `NULLS LAST`
- [ ] 4b.3 REFACTOR: full `uv run pytest`; ruff check/format; `bash scripts/test-airflow-e2e.sh`
- [ ] 4b.4 Commit: `feat(reap): partition tier-1 ranking by turn_id with legacy fallback`

## Phase 5: Orchestrator-run ops (post-merge, not sdd-apply)

- [ ] 5.1 Apply migration `047` on NAS `development` schema via `migrations_dag`
- [ ] 5.2 Apply migration `047` on NAS `production` schema via `migrations_dag`
- [ ] 5.3 `git_sync` both stacks; confirm `airflow dags list-import-errors` empty on both
- [ ] 5.4 Run design.md's read-only prod validation query (~28 rows expected)
- [ ] 5.5 Manually trigger `congress_reap_clip_preparer` on prod; confirm staged turn-sourced clips
