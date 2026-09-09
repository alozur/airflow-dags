# Exploration: persist-title-generator-inputs (issue #549)

## Current State

**`generate_title` signature** (`congress_videos/modules/thumbnail_generation.py:684-791`):
`generate_title(summary, best, sibling_titles=None, key_speakers=None, forbidden_title=None, participant_slug=None) -> str`.
`_build_title_prompt` (`thumbnail_generation.py:593-638`) interpolates `best["style"]`/`best["prompt"]`
(lines 612-613), `sibling_titles` (620-623), `key_speakers` + `participant_slug` (624-629). Exactly
matches the issue's required payload fields: summary, best, sibling_titles, key_speakers,
forbidden_title, participant_slug.

**Call sites — only ONE live call site:**

- `generic_thumbnail_generator_dag.py:257-275` (`_task_generate_title`), reading `conf` (from
  `validate_input`), `best` (from `choose_best_option`), `history` (from `fetch_recent_history`).
- This DAG is triggered exclusively from `youtube_upload_dag.py:753-822`
  (`trigger_thumbnail_generation`), itself invoked only from `_run_generate_thumbnail`
  (`youtube_upload_dag.py:1001-1009`), inside a DAG whose sole item source is
  `_run_get_uploadable_item` -> `db.get_uploadable_turns(limit=1)` (`youtube_upload_dag.py:520-538`,
  docstring: "Select the next item from the turn queue only"). **No chapter fallback remains
  reachable** — the `is_turn=False` branch inside `_prepare_thumbnail_config`
  (`youtube_upload_dag.py:368-489`, branch at 403-413) is DEAD CODE since #171, confirming the
  issue's claim.

**Turn upload path**: `youtube_upload_dag.py` processes exactly one uploadable item (a turn, possibly
representing a grouped-turns publish) per DAG run. `trigger_thumbnail_generation` triggers the generic
thumbnail DAG once per run, which calls `generate_title` once and returns
`{success, chapter_id, output_path, title}` via XCom (`youtube_upload_dag.py:802-819`).
`_prepare_upload_config` (turn branch, 1070-1128) reads `thumbnail_result["title"]`, raises
`ValueError` if missing/blank (issue #245 guard), then calls `prepare_orador_upload_config`, setting
`turn_config["turn_id" | "chapter_id" | "video_id"]` from `results[0]`. **turn_id, chapter_id,
video_id and output_path are ALL available at this point.**

**Persistence of the resulting title today**: `persist_results()` (`thumbnail_generation.py:887-976`)
upserts `video_thumbnails` keyed by `(chapter_id, label)` via `ON CONFLICT (chapter_id, label) DO
UPDATE`. Since #171, distinct DAG runs for distinct turns that share a `chapter_id` overwrite each
other's `openai_title`/`style`/`prompt` rows — this IS the exact collision the issue describes. The
actual published title is audited via sidecar files (`title.txt`), NOT
`video_thumbnails.openai_title`: there is currently no DB write that durably and correctly records
the turn's published title. `mark_turns_uploaded` / `mark_turns_uploaded_by_output_path`
(`database.py:1096-1171`) write `youtube_video_id`/`is_uploaded_to_youtube` to `speaker_turn_videos`
but never the title text.

**`speaker_turn_videos` schema** (migration 025): `UNIQUE (turn_id)` — ONE ROW PER TURN, even for
grouped uploads ("Grouped short-turn plans insert one row per constituent turn_id, each referencing
the same output_path"). This is the collision-free key: grouped turns already get separate rows.

**Existing #512 precedent for exactly this shape** — migration 050
(`050_final_copy_verification_audit.sql`) added `copy_verification_verdict`,
`copy_verification_findings JSONB`, `copy_original_title`, `copy_corrected_title` and friends to BOTH
`speaker_turn_videos` and `video_shorts`. Write functions:

- `record_copy_verification_turn` (`database.py:1173-1256`) — guarded
  `UPDATE speaker_turn_videos SET ... WHERE output_path = %s AND copy_content_version IS DISTINCT
  FROM %s`. Keys by `output_path` (not `turn_id`) deliberately, mirroring
  `mark_turns_uploaded_by_output_path` (#129): it intentionally applies the SAME payload to every row
  sharing a grouped video's `output_path`, which is correct because it is one published video.
  Idempotent re-run (same `content_version`) = 0 rows affected = success, not error.
- `record_copy_verification_short` (`database.py:1258-1332`) — identical shape, keyed by
  `video_shorts.id` (each short is its own row, no grouping).
- Both are called from failure-isolated call sites (`youtube_upload_dag.py:1254`,
  `reap_shorts_uploader_dag.py:571`) that never raise past a wrapping try/except — matching
  `upload_marking.py`'s pattern (`database.py:148-303`). This IS the established "persistence failure
  does not block publication" convention the issue asks for.

**jsonb precedent**: migration 043 (`043_persist_art_direction_brief.sql`) added
`video_thumbnails.art_direction_brief JSONB`, written via `_brief_json()`
(`thumbnail_generation.py:880-884`), binding `NULL` for missing/non-dict values, never `"{}"`.

**Migration convention**: highest existing migration is `050_final_copy_verification_audit.sql`; the
DOWN block is ALWAYS commented out (the migration runner executes the whole file transactionally — an
uncommented DOWN silently reverts). The new migration must be numbered **051**.

**Shorts generator** (`reap_shorts_uploader_dag.py::_generate_metadata`, lines 326-487): per pending
short, builds `user_prompt` from `transcript[:2000]` (truncated Whisper output, generated on the fly
from the clip via ffmpeg+Whisper, lines 396-436, never persisted), `chapter_title`,
`primary_speaker`, `secondary_speakers`, `topics`, `scoring_reasoning[:500]`, plus conditional
`mentioned_display_names`. Calls `generate_json_completion` (line 450). The result is appended to the
`shorts_metadata` XCom (lines 469-485) alongside `chapter` and `turn_speaker_row` snapshots (already
carried for `verify_final_copy`, issues #512/#546). `_verify_final_copy` (t2b, lines 494+) runs after
and calls `db.record_copy_verification_short(short_id=..., ...)` at line 571 — proving the
`video_shorts` row (by `id`) already exists and is writable at that stage.

**`video_shorts` schema** (migration 004): `id SERIAL PRIMARY KEY`, one row per Reap clip, no
grouping concern.

**`benchmarks/title_eval/` does NOT exist on this branch** (built in the separate uncommitted
worktree `wt-510`; confirmed absent via glob on this worktree, which is off `origin/main` f6ff2e4).

## Affected Areas

- `congress_videos/sql/migrations/051_*.sql` (new) — jsonb input-payload column(s) on
  `speaker_turn_videos` and `video_shorts`, DOWN commented out.
- `congress_videos/modules/database.py` — new write method(s) mirroring
  `record_copy_verification_turn`/`_short` (guarded UPDATE; try/except at the call site, never inside
  the DB method).
- `congress_videos/youtube_upload_dag.py` — hook point for the turn payload write.
- `congress_videos/modules/thumbnail_generation.py` — `generate_title`/`_task_generate_title`
  currently return only the title string; the assembled payload does not cross the XCom boundary.
- `congress_videos/reap_shorts_uploader_dag.py::_generate_metadata` — must retain the assembled
  shorts-generator input so a later task can persist it.
- `congress_videos/modules/upload_marking.py` — candidate write call inside `mark_turn_uploads`.

## Approaches

### (A) JSONB column(s) on the existing per-entity tables

Add `title_generation_input JSONB` to `speaker_turn_videos` (guarded UPDATE keyed by `output_path`,
mirroring `record_copy_verification_turn`) and to `video_shorts` (guarded UPDATE keyed by `id`,
mirroring `record_copy_verification_short`).

- **Pros**: reuses the exact pattern #512 already established (same migration shape, same write
  function shape, same failure-isolation call-site shape). The grouped-turn collision is solved for
  free: `speaker_turn_videos` is already one row per `turn_id` even when grouped, and the existing
  `output_path`-keyed UPDATE correctly replicates identical data across a group's sibling rows (one
  published video, one payload). Shorts already have their own row by `id`. Smallest migration; no
  new join to read a payload back.
- **Cons**: two more wide jsonb columns on tables that already carry the #512 audit columns; two write
  call sites rather than one.
- **Effort**: Low.

### (B) New dedicated audit table

`title_generation_inputs (id, entity_type, turn_id/output_path/short_id, generator, payload JSONB,
created_at)`, one row per generation.

- **Pros**: single write path for both generators; naturally supports multiple generation attempts per
  entity (reroll history), which the guarded UPDATE in (A) discards.
- **Cons**: a new join to read back; new natural-key design that re-derives the very collision
  question the issue wants avoided (keyed by `output_path`, several turn_ids sit behind one row); no
  existing precedent for a cross-entity polymorphic audit table in this schema; more code for scope
  the issue does not ask for (the payload of the ONE published title, not full history).
- **Effort**: Medium.

## Recommendation

**(A) JSONB columns on `speaker_turn_videos` and `video_shorts`.** It is the direct continuation of
the pattern issue #512 already validated in this codebase (migration 050 +
`record_copy_verification_turn`/`_short`) under the identical constraint. It satisfies the
"must not collide" acceptance criterion with zero new modeling, inheriting collision safety from
migration 025's `UNIQUE (turn_id)`. Reject (B) unless multi-attempt generation history becomes an
explicit requirement — it is out of scope per the issue.

## Risks / Open Questions for propose and design

1. **Where the turn-path write happens.** `record_copy_verification_turn` fires near actual publish
   (`youtube_upload_dag.py:1254`), but the title-generation payload is assembled much earlier, inside
   the CHILD thumbnail DAG (`_task_generate_title`), which has no DB handle to `speaker_turn_videos`
   and whose XCom returns only the title string. Two candidate designs:
   (i) the child DAG persists into its own `video_thumbnails` table AND the parent separately threads
   the payload back to write on `speaker_turn_videos` — two writes;
   (ii) `generate_title`/`_task_generate_title` returns `{title, generation_input}` and the parent
   persists it once after publish success — one write, but changes the child-DAG XCom contract and
   the `youtube_upload_dag.py:802-819` validation block.
2. **"Same transaction that stores the generated title"** has no literal referent today: no DB write
   durably records the turn's published title text (sidecar only). Design must decide whether this
   change also starts persisting the title text or persists the payload alone.
3. **Shorts transcript scope**: full Whisper transcript vs the `transcript[:2000]` slice actually fed
   to the prompt (`reap_shorts_uploader_dag.py:440`). The acceptance criterion says "the Whisper
   transcript text it prompted on".
4. **No credentials in the payload**: confirm no `photo_url`/token-shaped fields leak through
   `key_speakers` dicts before serialising to jsonb.
5. **Failure isolation** must follow the `upload_marking.py` / `record_copy_verification_*`
   convention exactly (try/except at the call site, never a bare method-level raise).

## Ready for Proposal

Yes. Both live call sites, their exact call chains, the collision-free per-turn/per-short row shapes,
and a directly reusable precedent (#512 migration 050) are confirmed by reading the actual code. The
open write-hook question is resolved explicitly in propose/design rather than assumed here.
