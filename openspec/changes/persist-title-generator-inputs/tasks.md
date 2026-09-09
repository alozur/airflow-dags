# Tasks: Persist Title Generator Input Payloads (issue #549)

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~935 total (305 / 360 / 270 per slice) |
| 400-line budget risk | Low per slice (each slice under 400; whole change would be High as one PR) |
| Chained PRs recommended | Yes |
| Suggested split | Slice 1 (migration + write methods) → Slice 2 (turn path) → Slice 3 (shorts path) |
| Delivery strategy | auto-chain |
| Chain strategy | feature-branch-chain |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: feature-branch-chain
400-line budget risk: Low

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Migration 051 + schema snapshot mirror + `database.py` write methods, base = `feat/549-generator-input-payloads` | PR 1 | `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database.py` | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/sql/**`) | Revert PR 1; columns stay additive/nullable, no caller references them yet |
| 2 | Turn path: `build_turn_title_payload`, `_task_thumbnail_result` payload build, `trigger_thumbnail_generation` hook, base = PR 1 branch | PR 2 | `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py tests/congress_videos/modules/test_generic_thumbnail_dag.py tests/congress_videos/test_youtube_upload_dag.py` | `bash scripts/test-airflow-e2e.sh` | Revert PR 2 only; turn hook is additive and try/except-isolated, PR 1 columns remain unused but valid |
| 3 | Shorts path: `build_shorts_title_payload` + `_generate_metadata` hook, base = PR 2 branch | PR 3 | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py` | `bash scripts/test-airflow-e2e.sh` | Revert PR 3 only; independent of PR 2's turn hook, both depend only on PR 1 |

## Traceability: Spec Scenario → Task

| Scenario | Requirement | Task ID(s) |
|---|---|---|
| 1.1 Title generated for a turn | Req 1 | 2.3, 2.4, 2.5 |
| 2.1 Shorts LLM generation | Req 2 | 3.2, 3.4 |
| 2.2 Fallback branch persists nothing | Req 2 | 3.5 |
| 3.1 Grouped-turn write touches only its own siblings | Req 3 | 1.7 |
| 3.2a Re-run overwrites, rowcount >= 1 | Req 3 | 1.8 |
| 3.2b Non-matching key is loud (`no_row`, logged) | Req 3 | 2.7 |
| 4.1 Turn payload round-trip | Req 4 | 2.9 |
| 4.1 Shorts payload round-trip | Req 4 | 3.6 |
| 5.1 Forced DB failure during turn write | Req 5 | 2.6 |
| 5.2 Forced DB failure during shorts write | Req 5 | 3.5b |
| 6.1 Schema-only, credential-free (turn) | Req 6 | 2.2 |
| 6.1 Schema-only, credential-free (shorts) | Req 6 | 3.3 |
| 7.1 Empty-title guard still raises | Req 7 | 2.8 |
| 7.2 #512 verification seam still runs (turn) | Req 7 | 2.10 |
| 7.2 #512 verification seam still runs (shorts) | Req 7 | 3.7 |

Note: the design's original count of "12 scenarios" splits 3.2 into two behaviorally distinct GIVEN/WHEN/THEN
blocks (re-run overwrite vs. non-matching key); both are listed here as 3.2a/3.2b against the single heading.

## Slice 1 — Migration + write methods (base: `feat/549-generator-input-payloads`)

- [ ] 1.1 Create `congress_videos/sql/migrations/051_persist_title_generation_input.sql` with two
  `ALTER TABLE ... ADD COLUMN IF NOT EXISTS title_generation_input JSONB` statements
  (`speaker_turn_videos`, `video_shorts`), and write the `DOWN` block as SQL comments only — the
  migration runner executes the whole file transactionally, so the DOWN statements MUST stay
  commented out, never active SQL.
- [ ] 1.2 Mirror both `ADD COLUMN` lines from task 1.1 into `congress_videos/sql/production_schema.sql`,
  in the existing `speaker_turn_videos` and `video_shorts` table blocks. [C1-a/b]
- [ ] 1.3 Add `"title_generation_input"` to the `TABLE_COLUMNS` tuple entries for BOTH
  `speaker_turn_videos` and `video_shorts` in `tests/congress_videos/sql/test_production_schema.py`.
  This is NOT covered by any existing test — task 1.1 alone makes no test fail, so this edit is what
  makes the drift check meaningful. [C1-c]
- [ ] 1.4 Add `record_title_generation_input_turn(self, output_path: str, *, payload: dict) -> int` to
  `congress_videos/modules/database.py` (after `record_copy_verification_short`): unguarded
  `UPDATE ... SET title_generation_input = %s::jsonb WHERE output_path = %s`
  (no `IS DISTINCT FROM` guard, per Req 3/D3), bind via `json.dumps(payload, ensure_ascii=False)`,
  raise `ValueError` on falsy `output_path` or non-dict/empty `payload`, return `cur.rowcount`, log
  `(%d rows)`.
- [ ] 1.5 Add `record_title_generation_input_short(self, short_id: int, *, payload: dict) -> int` to
  `congress_videos/modules/database.py`: same shape, keyed by `video_shorts.id`.
- [ ] 1.6 Unit test in `tests/congress_videos/modules/test_database.py`: mocked cursor asserts the
  `UPDATE` SQL text, the `%s::jsonb` bind, the `json.dumps` argument, `rowcount` passthrough, and
  `ValueError` on falsy key / non-dict payload for both methods.
- [ ] 1.7 Test (Scenario 3.1) in `tests/congress_videos/modules/test_database.py`: N sibling
  `speaker_turn_videos` rows sharing one `output_path` — one `record_title_generation_input_turn` call
  updates all N rows with the identical payload; a row under a different `output_path` is untouched.
- [ ] 1.8 Test (Scenario 3.2a) in `tests/congress_videos/modules/test_database.py`: calling
  `record_title_generation_input_turn` twice with the same `output_path` overwrites the payload and
  returns `rowcount >= 1` both times, without raising.
- [ ] 1.9 Run `uv run pytest tests/congress_videos/sql/test_production_schema.py tests/congress_videos/modules/test_database.py`
  and confirm green before opening the slice 1 PR.

## Slice 2 — Turn path (base: slice 1 branch)

- [ ] 2.1 Add `build_turn_title_payload(summary, best, title, *, sibling_titles=None, key_speakers=None,
  forbidden_title=None, participant_slug=None) -> dict` to
  `congress_videos/modules/thumbnail_generation.py` (next to `generate_title`). Build the dict from
  explicit literal keys only (`generator="turn_title"`, `schema_version=1`, plus the six inputs and
  `title`); reduce `best` to `{"label", "style", "prompt"}` only, and normalize `key_speakers` entries
  (`str` kept, `dict` reduced to `entry["name"]`, anything else dropped). Never spread `{**best}` or
  `dict(conf)`.
- [ ] 2.2 Test (Scenario 6.1, turn half) in `tests/congress_videos/modules/test_thumbnail_generation.py`:
  `set(json.loads(json.dumps(payload)))` equals the declared turn schema keys; recursive scan over
  serialized values finds no `http`, no `/`-rooted path, and no `token`/`key`/`secret` substring. Also
  assert `best`'s `local_path` / asset URL is dropped and `key_speakers` dict entries reduce to names
  only.
- [ ] 2.3 In `congress_videos/generic_thumbnail_generator_dag.py`, extend `_task_thumbnail_result` to
  pull `fetch_recent_history` from XCom and call `build_turn_title_payload` with the same values
  `generate_title` consumed, attaching the result under `title_generation_input` in the returned dict
  alongside the four existing legacy keys (`success`, `chapter_id`, `output_path`, `title`). Do not
  change `_task_generate_title`'s `str` return type.
- [ ] 2.4 Contract test in `tests/congress_videos/modules/test_generic_thumbnail_dag.py`:
  `_task_thumbnail_result` still returns the 4 legacy keys plus `title_generation_input`, and
  `_task_generate_title` still returns a bare `str`.
- [ ] 2.5 In `congress_videos/youtube_upload_dag.py`'s `trigger_thumbnail_generation`, immediately before
  `ti.xcom_push(key="thumbnail_result", value=result)`, read
  `payload = result.get("title_generation_input")` (never added to the strict `:808-816` validation
  conjunction) and `key = thumbnail_config.get("output_path")` (never `result["output_path"]`, per
  D3). Add an optional `db=None` parameter to `trigger_thumbnail_generation`, matching
  `_prepare_thumbnail_config(item, db)`.
- [ ] 2.6 Wire the call in task 2.5 with the explicit `try/except` convention from
  `congress_videos/modules/upload_marking.py` (~lines 60-108) — NOT the bare
  `record_copy_verification_*` call-site shape. On success set
  `provenance = {"status": "written" if rows else "no_row", "rows": rows, "error": None}`; on
  exception catch it, set `provenance = {"status": "failed", "rows": 0, "error": str(exc)}`, log at
  ERROR, and never re-raise or block publication. Push `title_provenance` to XCom. [C2]
- [ ] 2.7 Test (Scenario 3.2b) in `tests/congress_videos/test_youtube_upload_dag.py`: with an injected
  fake `db` whose `record_title_generation_input_turn` returns `0`, assert `title_provenance ==
  {"status": "no_row", "rows": 0, "error": None}` and a WARNING is logged (`caplog`), never treated as
  success. [C4]
- [ ] 2.8 Test (Scenario 5.1) in `tests/congress_videos/test_youtube_upload_dag.py`: with an injected
  fake `db` whose `record_title_generation_input_turn` raises, assert `title_provenance["status"] ==
  "failed"` with the error string, the exception does not propagate, and
  `trigger_thumbnail_generation` still completes.
- [ ] 2.9 Test (Scenario 7.1) in `tests/congress_videos/test_youtube_upload_dag.py`: `_prepare_upload_config`
  still raises `ValueError` when `thumbnail_result["title"]` is missing or blank, unaffected by the new
  hook.
- [ ] 2.10 Test (Scenario 4.1, turn half) in `tests/congress_videos/modules/test_thumbnail_generation.py`:
  round-trip a built turn payload — `generate_title(p["summary"], p["best"],
  sibling_titles=p["sibling_titles"], key_speakers=p["key_speakers"],
  forbidden_title=p["forbidden_title"], participant_slug=p["participant_slug"])` — succeeds using only
  stored fields, no live-worktree read, no `fetch_recent_history` re-call.
- [ ] 2.11 Test (Scenario 7.2, turn half) in `tests/congress_videos/test_youtube_upload_dag.py`: assert
  `record_copy_verification_turn` is still invoked, unchanged, on the same path as before the new hook.
- [ ] 2.12 Also add the key-selection assertion described in design D3: injected fake `db` captures the
  key argument passed to `record_title_generation_input_turn` and asserts it equals
  `thumbnail_config["output_path"]`, explicitly different from the child DAG's `thumbnail.png`
  `output_path` in `result`.
- [ ] 2.13 Run `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py tests/congress_videos/modules/test_generic_thumbnail_dag.py tests/congress_videos/test_youtube_upload_dag.py`
  and confirm green before opening the slice 2 PR against the slice 1 branch.

## Slice 3 — Shorts path (base: slice 2 branch)

- [ ] 3.1 Add `build_shorts_title_payload(transcript, *, chapter_title, primary_speaker,
  secondary_speakers, topics, scoring_reasoning, mentioned_display_names, title) -> dict` at module
  level in `congress_videos/reap_shorts_uploader_dag.py`, beside `build_shorts_metadata_context`.
  The `transcript` parameter MUST receive the FULL, unsliced Whisper transcript; the function performs
  its own `transcript[:2000]` slice and computes `transcript_truncated = len(transcript) > 2000` and
  `transcript_full_length = len(transcript)` internally. Do not pass an already-sliced value in. [C3]
- [ ] 3.2 In `congress_videos/reap_shorts_uploader_dag.py`'s `_generate_metadata` per-short loop, after
  `title` is finalized (the LLM branch, `truncate_text(ai_title, 100)`) and before the
  `metadata_list.append` block, call `build_shorts_title_payload` with the full in-scope `transcript`
  variable and the finalized `title`. [Req 2, C3]
- [ ] 3.3 Test (Scenario 6.1, shorts half) in `tests/congress_videos/test_reap_uploader_dag.py`:
  `set(json.loads(json.dumps(payload)))` equals the declared shorts schema keys; recursive scan finds
  no `http`, `/`-rooted path, or `token`/`key`/`secret` substring.
- [ ] 3.4 Test (Scenario 2.1) in `tests/congress_videos/test_reap_uploader_dag.py`: given a non-empty
  transcript longer than 2000 chars, assert the payload's `transcript` equals exactly the first 2000
  chars, `transcript_truncated is True`, `transcript_full_length` equals the full length, and the
  stored `title` equals the accepted LLM title. Also cover the 1999/2001-char boundary cases.
- [ ] 3.5 Wire the call from task 3.2 with the same `upload_marking.py`-style `try/except` used in task
  2.6 (never the bare `reap_shorts_uploader_dag.py:571` shape), keyed by `short_id` (already in scope),
  recording the outcome as `"title_provenance"` inside the appended metadata dict so it rides the
  existing `shorts_metadata` XCom. One short's failure MUST NOT abort the loop over remaining
  shorts. [C2, Req 5]
- [ ] 3.5b Test (Scenario 5.2) in `tests/congress_videos/test_reap_uploader_dag.py`: with an injected
  fake `db` whose `record_title_generation_input_short` raises for short #1, assert short #1's
  `title_provenance["status"] == "failed"` and metadata assembly for short #2 in the same loop still
  completes.
- [ ] 3.6 Test (Scenario 2.2) in `tests/congress_videos/test_reap_uploader_dag.py`: given an empty
  transcript that skips the LLM branch, assert no `record_title_generation_input_short` call occurs
  and the metadata's `title_provenance` reflects "skipped" / the column stays untouched (NULL).
- [ ] 3.7 Test (Scenario 4.1, shorts half) in `tests/congress_videos/test_reap_uploader_dag.py`:
  round-trip a built shorts payload by re-rendering `SHORTS_METADATA_USER_PROMPT_TEMPLATE.format(...)`
  from the stored fields verbatim (no re-applying `[:2000]`/`[:500]` slicing on already-sliced stored
  values) and assert the render succeeds using only stored fields.
- [ ] 3.8 Test (Scenario 7.2, shorts half) in `tests/congress_videos/test_reap_uploader_dag.py`: assert
  `record_copy_verification_short` is still invoked, unchanged, on the same path as before the new
  hook.
- [ ] 3.9 Run `uv run pytest tests/congress_videos/test_reap_uploader_dag.py` and confirm green before
  opening the slice 3 PR against the slice 2 branch.
- [ ] 3.10 After slice 3 is green, run `bash scripts/test-airflow-e2e.sh` once across the full chain
  (touches `congress_videos/**`) to confirm `airflow dags list-import-errors` stays empty for all three
  modified DAGs.
