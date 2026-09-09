# Design: Persist Title Generator Input Payloads (issue #549)

## Technical Approach

Continue the pattern migration 050 / `record_copy_verification_turn` established for #512: one
nullable `JSONB` column per live-generator entity table, written by a guarded `UPDATE` from a
failure-isolated call site. Two **pure payload builders** assemble an allowlisted dict; the DAG
seams only transport and persist it. Neither `generate_title` nor `_task_generate_title` changes
signature, so no existing consumer contract moves.

## Architecture Decisions

### D1 — Where the turn payload is assembled

| Option | Tradeoff | Decision |
|---|---|---|
| Widen `_task_generate_title` to return a dict | Breaks BOTH consumers (`_task_persist_results:291`, `_task_thumbnail_result:332`) which pull `task_ids="generate_title"` as `str \| None`; an in-flight run resumed across deploy crashes | Rejected |
| New `build_title_payload` DAG task | Extra task + dependency edges for a pure function; no new information | Rejected |
| **`_task_thumbnail_result` builds the payload itself** | Needs one extra `xcom_pull("fetch_recent_history")` | **Chosen** |

**Rationale (overrides the proposer's "update both consumers" mitigation, keeps its
"`generate_title` unchanged" half).** `_task_thumbnail_result` already pulls `validate_input`,
`choose_best_option` and `generate_title` (lines 330-332) and runs strictly downstream
(`t_title >> t_persist >> t_result`, line 523). The payload is a **pure function of XCom values that
are immutable once written**, so rebuilding it there is byte-exact with what `_task_generate_title`
fed the prompt. Risk 1 is dissolved, not mitigated: zero consumers change.
*Limitation*: a manual clear+rerun of `fetch_recent_history` between the two tasks would desync the
`sibling_titles` snapshot. Accepted; not reachable in normal scheduling.

### D2 — Optional payload key in the strict result validation

`trigger_thumbnail_generation`'s conjunction (`youtube_upload_dag.py:808-816`) validates exactly
`success`, `chapter_id`, `output_path`, `title`. **`title_generation_input` is NOT added to that
conjunction.** It is read *after* the conjunction passes, via `result.get("title_generation_input")`.
A missing or non-dict payload logs a WARNING and records `status="skipped"` — it can never degrade a
valid title into `_thumbnail_failure` (line 818). Risk 2 resolved.

### D3 — The write key, and making a wrong key loud

The child returns the reconciled `thumbnail.png` path as `output_path`
(`generic_thumbnail_generator_dag.py:340`). The write key is **`thumbnail_config["output_path"]`**
(the turn `video.mp4`, set at `youtube_upload_dag.py:427`), read from the local variable already in
scope at line 755 — the child result's `output_path` is never used as a key.

Unlike `record_copy_verification_turn`, the `WHERE` clause carries **no idempotency guard**
(no `IS DISTINCT FROM`), so `rowcount == 0` is unambiguous: *no row matched the key*. The DB method
still never raises on 0 (returns the count); the **call site** logs a WARNING and records
`status="no_row"`. Risk 3 resolved without a raise. Re-running overwrites unconditionally, which is
correct: the last generation is the published one.

### D4 — Replay fidelity is attempt-1 exact only

`_choose_reprompt_instruction` / the forbidden-title reroll (`thumbnail_generation.py:656-681`,
770-789) mutate the prompt on attempts 2-3 and are **not** recorded. A stored payload replays the
*first* prompt exactly; it does not reproduce a rerolled title. Out of scope per the proposal —
**#510 must not assume attempt-level replay.** Risk 4 stated, accepted.

### D5 — Shorts write condition

Persist only when the LLM branch ran (`transcript` truthy, line 438) **and** returned a non-empty
`ai_title`. NULL then unambiguously means "no LLM title was generated", and the stored `title` is
always the LLM output actually accepted (`truncate_text(ai_title, 100)`, line 459) — never the
fallback headline.

## Data Flow

```
CHILD thumbnail DAG                          PARENT youtube_upload_dag
  validate_input ─┐
  fetch_recent_history ─┼─→ generate_title (str, UNCHANGED)
  choose_best_option ─┘        │
                               ↓
                     thumbnail_result ── XCom ──→ trigger_thumbnail_generation
                     + title_generation_input        │ strict validation (payload EXCLUDED)
                                                     ↓
                                        db.record_title_generation_input_turn(
                                            thumbnail_config["output_path"])
                                                     │ try/except → title_provenance XCom

reap_shorts_uploader_dag._generate_metadata
  transcript[:2000] → prompt → ai_title
        └─→ build_shorts_title_payload → db.record_title_generation_input_short(short_id)
                                              │ try/except → metadata["title_provenance"]
```

## Interfaces / Contracts

### Migration `051_persist_title_generation_input.sql`

```sql
-- Migration 051: serialized title-generator input payload (issue #549).
-- Nullable/additive: rows generated before this change legitimately carry NULL.
ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS title_generation_input JSONB;

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS title_generation_input JSONB;

-- DOWN (manual only; migration runner executes the whole file transactionally):
-- ALTER TABLE video_shorts        DROP COLUMN IF EXISTS title_generation_input;
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS title_generation_input;
```

No index: the column is write-only for this change and read by ad-hoc `#510` corpus extraction over
a table already indexed on `output_path`/`turn_id` (`production_schema.sql:451-453`). Mirror both
`ADD COLUMN` lines into `congress_videos/sql/production_schema.sql` in the **same slice** — the
lockstep drift test `tests/congress_videos/sql/test_production_schema.py` fails otherwise.

### Payload schema — turn (`generator="turn_title"`, `schema_version=1`)

| Key | Type | Null? | Source |
|---|---|---|---|
| `generator` | `str` | no | literal `"turn_title"` |
| `schema_version` | `int` | no | literal `1` |
| `summary` | `str` | no | `conf["debate_summary"]` |
| `best` | `{label: str, style: str, prompt: str}` | no | reduced from `choose_best_option` |
| `sibling_titles` | `list[str] \| None` | yes | `history.get("titles") or None` |
| `key_speakers` | `list[str] \| None` | yes | names only (see D6) |
| `forbidden_title` | `str \| None` | yes | `conf.get("previous_title")` |
| `participant_slug` | `str \| None` | yes | `conf.get("slug")` |
| `title` | `str` | no | the `generate_title` return value |

### Payload schema — shorts (`generator="shorts_metadata"`, `schema_version=1`)

| Key | Type | Null? | Source |
|---|---|---|---|
| `generator` / `schema_version` | `str` / `int` | no | literals |
| `transcript` | `str` | no | exactly `transcript[:2000]` (line 440) |
| `transcript_truncated` | `bool` | no | `len(transcript) > 2000` |
| `transcript_full_length` | `int` | no | `len(transcript)` |
| `chapter_title` | `str` | no | template arg |
| `primary_speaker` | `str` | no | template arg |
| `secondary_speakers` | `str` | no | template arg |
| `topics` | `str` | no | template arg |
| `scoring_reasoning` | `str` | no | exactly `scoring_reasoning[:500]` (line 445) |
| `mentioned_display_names` | `list[str] \| None` | yes | conditional block (447-449) |
| `title` | `str` | no | `truncate_text(ai_title, 100)` |

`schema_version` is the evolution hinge: #510 branches on it, and any field addition/removal
increments it.

### Pure builders (allowlist, D6)

```python
# congress_videos/modules/thumbnail_generation.py  (next to generate_title)
def build_turn_title_payload(
    summary: str, best: dict, title: str, *,
    sibling_titles: list[str] | None = None, key_speakers: list | None = None,
    forbidden_title: str | None = None, participant_slug: str | None = None,
) -> dict: ...

# congress_videos/reap_shorts_uploader_dag.py  (module level, beside build_shorts_metadata_context)
def build_shorts_title_payload(
    transcript: str, *, chapter_title: str, primary_speaker: str, secondary_speakers: str,
    topics: str, scoring_reasoning: str, mentioned_display_names: list[str] | None, title: str,
) -> dict: ...
```

### D6 — Credential exclusion: allowlist, enforced in the builders

Both builders construct the dict from **explicit literal keys only** — never `{**best}`,
never `dict(conf)`. Two reducers do the narrowing:

- `best` → `{"label": best.get("label", ""), "style": best.get("style", ""), "prompt":
  best.get("prompt", "")}`. Drops `local_path` and every Pikzels asset URL. Sufficient because
  `_build_title_prompt` reads only `style`/`prompt` (lines 612-613); `label` is provenance.
- `key_speakers` → names only. The live turn path already yields `list[str]`
  (`_turn_speaker_fields`, `youtube_upload_dag.py:318-360`), but `generate_title` also accepts dicts
  with a `name` key, so entries are normalized: `str` kept, `dict` reduced to `entry["name"]`,
  anything else dropped. No `photo_url`, no slug dict ever reaches jsonb.

A test asserts `set(json.loads(serialized)) == DECLARED_KEYS` for both schemas, and that no value
recursively contains `http`, `/`-rooted paths, or a `token`/`key`/`secret` substring.

### Write methods (`database.py`, after `record_copy_verification_short`)

```python
def record_title_generation_input_turn(self, output_path: str, *, payload: dict) -> int:
    """Guarded UPDATE keyed by output_path; writes the same payload to every
    sibling speaker_turn_videos row of one grouped video (one video, one
    generation). Returns cur.rowcount. 0 means NO ROW MATCHED — the caller
    MUST log it loudly (no IS DISTINCT FROM guard exists here).
    Raises ValueError if output_path is falsy or payload is not a non-empty dict.
    """
    # UPDATE {stv_table} SET title_generation_input = %s::jsonb WHERE output_path = %s
    #   with json.dumps(payload, ensure_ascii=False)   ← _brief_json convention (tg.py:880-884)

def record_title_generation_input_short(self, short_id: int, *, payload: dict) -> int:
    """Same shape keyed by video_shorts.id. Raises ValueError on falsy short_id
    or non-dict payload. Returns cur.rowcount; 0 means no row matched."""
```

Both use `self.pg_conn.get_qualified_table(...)` and the
`with self.pg_conn.get_connection() as conn, conn.cursor() as cur:` form, and log
`(%d rows)` exactly as lines 1249-1255 / 1326-1332 do.

### Call-site wiring (failure isolation — `upload_marking.py:60-108` convention)

**Turns** — inside `trigger_thumbnail_generation`, immediately before
`ti.xcom_push(key="thumbnail_result", value=result)` (line 821):

```python
payload = result.get("title_generation_input")          # D2: optional, never validated above
key = thumbnail_config.get("output_path")               # D3: NEVER result["output_path"]
provenance = {"status": "skipped", "rows": 0, "error": None}
if isinstance(payload, dict) and payload and key:
    try:
        rows = (db or CongressionalVideoDB()).record_title_generation_input_turn(key, payload=payload)
        provenance = {"status": "written" if rows else "no_row", "rows": rows, "error": None}
        if not rows:
            logging.warning("title provenance: 0 rows for output_path=%r — key mismatch", key)
    except Exception as exc:                             # never blocks publication
        provenance = {"status": "failed", "rows": 0, "error": str(exc)}
        logging.error("title provenance write failed for output_path=%r: %s", key, exc)
ti.xcom_push(key="title_provenance", value=provenance)
```

`trigger_thumbnail_generation` gains an optional `db=None` parameter for test injection, matching how
`_prepare_thumbnail_config(item, db)` already receives one (line 998).

**Shorts** — inside `_generate_metadata`'s per-short loop, after `title` is finalized (line 459) and
before the `metadata_list.append` block; the same try/except, keyed by `short_id`, with the result
recorded as `"title_provenance"` inside the appended metadata dict so it rides the existing
`shorts_metadata` XCom. `db` is already in scope (line 336). One short's failure never aborts the
loop.

**Not silent, not blocking**: every outcome (`written` / `no_row` / `skipped` / `failed`) is both
logged at WARNING-or-above when abnormal *and* recorded in an XCom the operator can read.

## Read-back / replay path (#510 consumer contract)

Read-only; **no new read method is added by this change** — the eval harness owns its `SELECT`:

```sql
SELECT turn_id, output_path, title_generation_input
  FROM speaker_turn_videos
 WHERE title_generation_input IS NOT NULL;     -- and the video_shorts analogue by id
```

Turn replay maps the payload straight onto the unchanged `generate_title` signature — the field
names were chosen to be its parameter names:

```python
p = row["title_generation_input"]                       # psycopg2 returns jsonb as dict
replayed = generate_title(
    p["summary"], p["best"],
    sibling_titles=p["sibling_titles"], key_speakers=p["key_speakers"],
    forbidden_title=p["forbidden_title"], participant_slug=p["participant_slug"],
)   # compare against p["title"]
```

This touches **no worktree file** and never calls `fetch_recent_history`: the as-of sibling window is
frozen inside the payload. Shorts replay re-renders
`SHORTS_METADATA_USER_PROMPT_TEMPLATE.format(...)` from the stored fields **verbatim** — the stored
`transcript` and `scoring_reasoning` are already sliced, so replay must not re-apply `[:2000]` /
`[:500]` semantics as if the values were full-length.

## File Changes

| File | Action | Slice |
|---|---|---|
| `congress_videos/sql/migrations/051_persist_title_generation_input.sql` | Create | 1 |
| `congress_videos/sql/production_schema.sql` | Modify (snapshot lockstep) | 1 |
| `congress_videos/modules/database.py` | Modify (2 write methods) | 1 |
| `congress_videos/modules/thumbnail_generation.py` | Modify (`build_turn_title_payload` + reducers) | 2 |
| `congress_videos/generic_thumbnail_generator_dag.py` | Modify (`_task_thumbnail_result` only) | 2 |
| `congress_videos/youtube_upload_dag.py` | Modify (`trigger_thumbnail_generation` hook) | 2 |
| `congress_videos/reap_shorts_uploader_dag.py` | Modify (builder + hook) | 3 |
| `tests/congress_videos/**` | Create/Modify | 1-3 |

## Testing Strategy

| Layer | What | How |
|---|---|---|
| Unit | Both builders: exact key set, `best` reduction, dict→name `key_speakers` normalization, `None` vs `[]` for siblings, shorts truncation flags at 1999/2000/2001 chars | Pure calls, no DB |
| Unit | Credential exclusion | `set(json.loads(dumps(payload)))` equals `DECLARED_KEYS`; recursive scan finds no URL/path/token substring |
| Unit | Write methods | Mock cursor: SQL text, `%s::jsonb` bind, `json.dumps` arg, `rowcount` passthrough, `ValueError` on falsy key / non-dict payload |
| Contract | `_task_generate_title` still returns `str`; `_task_persist_results` still receives `str \| None`; `_task_thumbnail_result` returns the 4 legacy keys **plus** `title_generation_input` | XCom stubs |
| Contract | Strict validation (`:808-816`) still passes with the payload **absent**, and still rejects a genuinely malformed result | Fake `XCom.get_one` |
| Contract | Write key is `thumbnail_config["output_path"]`, asserted **different** from the child's `thumbnail.png` `output_path` | Injected fake db captures the key |
| Isolation | DB raising → publication continues, `title_provenance.status == "failed"`; `rowcount==0` → `"no_row"` + WARNING | `caplog` + injected db |
| Isolation | Shorts: short #1 raising does not stop short #2 | Loop over 2 pending shorts |
| Schema | Migration 051 / `production_schema.sql` drift | Existing `test_production_schema.py` |
| E2E | DagBag import of all three DAGs | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) |

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary. The change adds two nullable columns and two DB writes; the existing
ffmpeg/Whisper subprocess in `_generate_metadata` is read-only input and is not modified.

## Migration / Rollout

Migration 051 must be applied by the migrations DAG in **both** dev and prod schemas before the
writing code is deployed. Additive and nullable, so an early deploy of the code against a pre-051
schema fails only inside the isolated try/except (`status="failed"`) and never blocks publication.
No backfill; existing rows keep `NULL`. Rollback = revert the code commits; leave the columns.

## Delivery: 3-slice feature-branch chain (`auto-chain`, 400-line budget)

| # | Slice | Prod | Tests | Total | Independently verifiable by |
|---|---|---|---|---|---|
| 1 | Migration 051 + schema snapshot + both `database.py` write methods | ~140 | ~165 | **~305** | `uv run pytest` unit + drift test; column exists, methods callable |
| 2 | Turn path: `build_turn_title_payload`, `_task_thumbnail_result`, parent hook | ~150 | ~210 | **~360** | Contract + isolation tests; a published turn writes non-NULL on every sibling row |
| 3 | Shorts path: `build_shorts_title_payload` + `_generate_metadata` hook | ~90 | ~180 | **~270** | Shorts payload/isolation tests; a published short writes non-NULL |

Every slice is under 400. Slice 1 targets the feature branch; slice 2 targets slice 1's branch;
slice 3 targets slice 2's branch. Rollback of any slice leaves the previous ones coherent: slices 2
and 3 are independent of each other and both depend only on slice 1.

## Open Questions

- [ ] None blocking. `gh issue view 549` reconciliation is carried by `sdd-spec` (proposal risk 5).

## Post-validation corrections (orchestrator gate, binding on `sdd-tasks` and `sdd-apply`)

A fresh-context phase-contract validator checked this design against the worktree and returned
`PASS_WITH_FINDINGS` (~40 line-number citations verified exact). Four corrections are binding; they
override any contradicting statement earlier in this document.

### C1 — The schema drift test is NOT an automatic safety net (was: HIGH)

This document claimed that mirroring the `ADD COLUMN` lines into
`congress_videos/sql/production_schema.sql` is enforced because "the lockstep drift test
`tests/congress_videos/sql/test_production_schema.py` fails otherwise". That is **false**.
`TABLE_COLUMNS` in that file is a static, manually transcribed column tuple (see the
`speaker_turn_videos` block around line 184 and the `video_shorts` block around line 495); it is not
derived from the migrations. Adding migration 051 without touching the test makes **no existing test
fail**.

Therefore slice 1 MUST explicitly do all three, as separate checklist items:

1. Add both `ADD COLUMN` statements to `congress_videos/sql/production_schema.sql`.
2. Add `"title_generation_input"` to the `TABLE_COLUMNS` entry for `speaker_turn_videos` **and** for
   `video_shorts` in `tests/congress_videos/sql/test_production_schema.py`.
3. Do not describe any of this as covered by an existing safety net.

### C2 — Failure isolation follows `upload_marking.py`, NOT the `record_copy_verification_*` call sites (was: MEDIUM)

This document justified the failure-isolated call site by pointing at the #512 precedent. The write
*methods* are a valid precedent; their **call sites are not**. The real calls at
`youtube_upload_dag.py:1254` and `reap_shorts_uploader_dag.py:571` are **bare calls with no
`try/except`** — a DB exception there propagates uncaught. Copying that call-site shape would violate
acceptance criterion "persistence failure does not block or crash publication".

Both new call sites MUST use the explicit `try/except` convention from
`congress_videos/modules/upload_marking.py` (~lines 60-108): catch the exception, record a
`status="failed"` outcome with the error, log it, and continue. Never re-raise, and never swallow it
silently — the acceptance criterion demands both halves.

### C3 — The shorts payload builder receives the FULL transcript (was: LOW)

`build_shorts_title_payload` MUST be passed the complete, unsliced Whisper transcript and perform the
`[:2000]` truncation itself. Only then can its three fields be simultaneously correct: `transcript`
(the exact 2000-char slice the prompt received), `transcript_truncated` (`len(full) > 2000`) and
`transcript_full_length` (`len(full)`). Passing an already-sliced value would pin
`transcript_truncated` to `False` and cap `transcript_full_length` at 2000. The full `transcript`
variable is in scope throughout the per-short loop in `_generate_metadata`, past the `title`
assignment, so this is available at the hook point.

### C4 — Zero rows is a loud `no_row` outcome, not success

The spec (Requirement 3) has been reconciled to this design's unguarded-`UPDATE` decision (D3). Since
the `WHERE` clause carries no `IS DISTINCT FROM` predicate, `rowcount == 0` means exactly one thing:
the key matched no row. The call site MUST record and log it as a distinct `no_row` outcome, never as
success. A re-run with the same key overwrites and returns `rowcount >= 1`, which is intended.

### Accepted without change

The 935-line total across the three slices exceeds the proposal's ~600-700 forecast. Every individual
slice remains under the 400-line review budget (305 / 360 / 270), which is the binding constraint, so
the decomposition stands. If slice 2's contract tests grow past 400 during apply, split the parent
hook from the child-DAG change into a fourth slice.
