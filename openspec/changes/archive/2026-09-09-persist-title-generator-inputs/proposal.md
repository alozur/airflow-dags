# Proposal: Persist Title Generator Input Payloads (issue #549)

## Intent

Published titles are not replayable. Of 814 published videos in the #510 corpus, only 52 can be
replayed, and all 52 sit on the chapter path that #171 made unreachable. **Zero items on either live
path (turns, shorts) are replayable**, because three prompt inputs are destroyed after use:

| Input | Why it is lost | Evidence |
|---|---|---|
| `best["style"]` / `best["prompt"]` | Interpolated into the prompt text; stored only in `video_thumbnails` keyed by `chapter_id`, and since #171 sibling turns of one chapter overwrite that row | `thumbnail_generation.py:612-618`, `persist_results` 887-976 |
| `sibling_titles` | Rolling `ORDER BY chapter_id DESC LIMIT 5` window; the as-of set is never recorded | `_task_generate_title` reads `fetch_recent_history` (`generic_thumbnail_generator_dag.py:267`) |
| Shorts Whisper transcript | Transcribed on the fly from the clip, never persisted; the clip is later reaped | `reap_shorts_uploader_dag.py:396-440` |

This blocks #510: a title-eval baseline needs `output = generator(input)`, and today neither side of
that equation is durably recorded for a live path. This change records the input, and the output
next to it, at generation time.

## Scope

### In Scope

- Migration **051**: `title_generation_input JSONB` on `speaker_turn_videos` and on `video_shorts`
  (nullable, additive, DOWN block commented out per repo convention — the runner executes the whole
  file in one transaction).
- `database.py`: two write methods mirroring `record_copy_verification_turn` / `_short`
  (`database.py:1173-1332`) — guarded `UPDATE`, keyed by `output_path` for turns and by `id` for
  shorts, returning `rowcount`.
- Turn path: expose the assembled payload from the title-generation seam so it crosses the child
  thumbnail DAG's XCom boundary back to `youtube_upload_dag.py`, which persists it once.
- Shorts path: assemble and persist the payload in `_generate_metadata`.
- The generated title text is stored **inside the same jsonb write**, so input and output land
  together in one statement.
- Unit tests for payload assembly, the guarded writes, the XCom contract, credential exclusion, and
  failure isolation.

### Out of Scope

- Retroactive recovery of historical inputs (explicitly excluded by the issue).
- The dead chapter path (`_prepare_thumbnail_config` `is_turn=False` branch,
  `youtube_upload_dag.py:403-413`, unreachable since #171).
- Multi-attempt generation history / reroll audit (approach B, a polymorphic audit table, is
  rejected).
- Any change to the sidecar `title.txt` contract, to `video_thumbnails`, or to the #512 audit
  columns.
- Building the eval harness itself (that is #510).

## Capabilities

### New Capabilities

- `title-generation-provenance`: for every live title generator, the serialized generator input and
  the resulting title are persisted on the entity's own row at generation time, collision-free
  across grouped uploads and free of credentials.

### Modified Capabilities

- None.

## Approach

**Approach (A): JSONB column on the existing per-entity tables.** It is the direct continuation of
the pattern #512 validated in migration 050 under the identical constraint, and it inherits
collision safety from migration 025's `UNIQUE (turn_id)` on `speaker_turn_videos` at zero modelling
cost.

### Payload schema — turn path (`speaker_turn_videos.title_generation_input`)

| Field | Type | Source |
|---|---|---|
| `generator` | `str` — `"turn_title"` | constant |
| `schema_version` | `int` — `1` | constant |
| `summary` | `str` | `conf["debate_summary"]` |
| `best` | `object {label, style, prompt}` | reduced from `choose_best_option`; `local_path`, image URLs and scoring fields are dropped |
| `sibling_titles` | `list[str] \| null` | `history.get("titles") or None` |
| `key_speakers` | `list[str] \| null` | `conf.get("key_speakers") or None` |
| `forbidden_title` | `str \| null` | `conf.get("previous_title")` |
| `participant_slug` | `str \| null` | `conf.get("slug")` |
| `title` | `str` | the title `generate_title` returned |

`best` is reduced to exactly the three fields the prompt reads (`thumbnail_generation.py:612-613`)
plus `label` for provenance. The full option dict carries `local_path` and Pikzels asset URLs that
replay does not need.

### Payload schema — shorts path (`video_shorts.title_generation_input`)

| Field | Type | Source |
|---|---|---|
| `generator` | `str` — `"shorts_metadata"` | constant |
| `schema_version` | `int` — `1` | constant |
| `transcript` | `str` | **exactly `transcript[:2000]`**, the slice fed to the template (`reap_shorts_uploader_dag.py:440`) |
| `transcript_truncated` | `bool` | `len(transcript) > 2000` |
| `transcript_full_length` | `int` | `len(transcript)` |
| `chapter_title` | `str` | template arg |
| `primary_speaker` | `str` | template arg |
| `secondary_speakers` | `str` | template arg |
| `topics` | `str` | template arg |
| `scoring_reasoning` | `str` | **exactly `scoring_reasoning[:500]`**, as prompted |
| `mentioned_display_names` | `list[str] \| null` | conditional block (line 447-449) |
| `title` | `str` | the accepted title after `truncate_text(..., 100)` |

The unbounded full transcript is **not** persisted. The LLM branch runs only when `transcript` is
truthy (line 438); on the fallback branch no generation happens, so nothing is written and a NULL
column unambiguously means "no LLM generation occurred".

### Write hooks

- **Turns** — the payload is assembled at the title-generation seam in the child thumbnail DAG,
  returned through `_task_generate_title` → `_task_thumbnail_result` → the parent's
  `thumbnail_result` XCom, and persisted by `youtube_upload_dag.py` **as soon as the thumbnail
  result comes back**, keyed by `thumbnail_config["output_path"]` (the turn's `video.mp4` path, set
  at `youtube_upload_dag.py:427`). It is **not** gated behind upload success: the
  `speaker_turn_videos` row already exists (written by the prepare DAG), and the acceptance
  criterion says *generated*, not *published*.
  Note: the child's returned `output_path` is the reconciled `thumbnail.png`
  (`generic_thumbnail_generator_dag.py:340`) and must **not** be used as the key.
- **Shorts** — persisted inside `_generate_metadata`, keyed by `short_id`. The `video_shorts` row is
  provably present and writable at this stage: `_verify_final_copy` already calls
  `record_copy_verification_short(short_id=...)` immediately downstream (line 571).

### Failure isolation

Both writes follow the `upload_marking.py` convention (`database.py`-adjacent module, lines 66-284):
`try/except Exception` **at the call site**, logging and recording the failure instead of raising.
The DB methods themselves raise only on a falsy key (mirroring
`record_copy_verification_turn`'s `ValueError` guard, line 1216-1217), which the call-site wrapper
absorbs. A persistence failure can never block or crash publication.

### Safeguards explicitly untouched

- The #245 empty-title guard (`youtube_upload_dag.py:1094-1100`) keeps raising on a missing/blank
  title; this change adds no new source of publishable title text.
- The #512 final-copy verification seam (`youtube_upload_dag.py:1246-1273`) and its columns are
  untouched; the new column is written earlier and independently.
- Speaker attribution (`_turn_speaker_fields`, `canonical_display_name` substitution) is read-only
  input to the payload and is not modified.

## Acceptance criteria coverage

| Criterion | How it is satisfied |
|---|---|
| Both live generators persist their serialized input | Turn path via the thumbnail-DAG XCom hook; shorts path in `_generate_metadata`. The dead chapter path is out of scope. |
| Persisted whenever a title is **generated** | The turn write fires on the thumbnail result, before and independent of upload success. The shorts write fires in the metadata task. |
| Covers the three unrecoverable inputs | `best.style` / `best.prompt`, the as-of `sibling_titles` list, and the exact prompted transcript slice are all fields of the schemas above. |
| Grouped turn uploads must not collide | `speaker_turn_videos` is `UNIQUE (turn_id)` — one row per constituent turn even when grouped (migration 025). The `output_path`-keyed UPDATE writes the same payload to every sibling row of one published video, which is correct: one video, one generation. `video_shorts` is one row per `id`, no grouping. No row is ever overwritten by a different turn's payload, which is exactly the `video_thumbnails` failure mode this replaces. |
| Same transaction that stores the generated title | Input and generated title are written by one statement into one jsonb value, so an eval case can never observe an input without its output. |
| No credentials in the payload | The live turn path's `key_speakers` is a `list[str]` of display names (`_turn_speaker_fields`, `youtube_upload_dag.py:326-360`) — no dicts, no `photo_url`. `best` is reduced to `{label, style, prompt}`, dropping `local_path` and Pikzels asset URLs. The shorts payload contains only prompt text fields. A test asserts the serialized payload contains no key outside the declared schema. |
| Retroactive recovery out of scope | Stated in Out of Scope; existing rows keep `NULL`. |

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/sql/migrations/051_*.sql` | New | Two nullable JSONB columns; DOWN commented out |
| `congress_videos/modules/database.py` | Modified | Two guarded-UPDATE write methods |
| `congress_videos/modules/thumbnail_generation.py` | Modified | Pure payload-builder helper next to `generate_title` |
| `congress_videos/generic_thumbnail_generator_dag.py` | Modified | `_task_generate_title` / `_task_thumbnail_result` carry the payload |
| `congress_videos/youtube_upload_dag.py` | Modified | XCom result validation + failure-isolated turn write |
| `congress_videos/reap_shorts_uploader_dag.py` | Modified | Payload assembly + failure-isolated shorts write |
| `tests/` | New/Modified | Payload, write, contract, isolation and credential tests |

## Delivery forecast

| Bucket | Estimate |
|---|---|
| Production | ~250 changed lines |
| Tests (`strict_tdd: true`) | ~350-450 changed lines |
| **Total** | **~600-700 changed lines** |

`400-line budget risk: High` → **chained PRs are required**. Strategy `auto-chain` /
`feature-branch-chain`, budget 400 lines per PR. Suggested slices, each independently verifiable:

1. Migration 051 + both `database.py` write methods + their tests.
2. Turn path: payload builder, XCom contract change, parent write hook + tests.
3. Shorts path: payload assembly, write hook + tests.

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| `_task_generate_title` currently returns a bare `str` and has **two** consumers (`_task_persist_results` line 291, `_task_thumbnail_result` line 332); widening it to a dict can break both | Med | Keep `generate_title`'s own signature/return unchanged and add a pure builder; update both consumers in the same slice with tests asserting `persist_results` still receives a `str \| None` |
| `trigger_thumbnail_generation`'s strict result validation (`youtube_upload_dag.py:808-816`) rejects a malformed dict and silently degrades to `_thumbnail_failure` | Med | Treat the new key as optional in that validation — a missing payload must never turn a good title into a thumbnail failure |
| Replay fidelity is attempt-1 exact only: the re-prompt and reroll instructions (`_choose_reprompt_instruction`, lines 656-681) are derived from the invalid candidate and are not recorded | Med | Accept — multi-attempt history is out of scope; document the limitation in the spec so #510 does not assume it |
| Two more wide JSONB columns on tables already carrying #512 audit columns | Low | Payloads are bounded (transcript capped at 2000 chars, `scoring_reasoning` at 500) |
| Issue #549's body was read from the Engram record (#2687), not from `gh` — no shell in this executor | Low | Re-read `gh issue view 549` during `sdd-spec` and reconcile the criteria table |

## Rollback Plan

Revert the code commits. Migration 051 is additive and nullable, so leaving the columns in place is
harmless and no consumer reads them; per repo convention the DOWN block is commented out, so an
actual schema rollback would be a separately authored migration. No data migration, no backfill, no
change to any published artifact.

## Dependencies

- Migration runner applies `051` in both dev and prod schemas before the code that writes to it.
- Unblocks #510 (title-eval corpus); does not depend on it.

## Success Criteria

- [ ] `uv run pytest` passes; new tests cover both payload schemas, both writes, and failure isolation.
- [ ] A newly published turn has non-NULL `title_generation_input` on every sibling
      `speaker_turn_videos` row of its `output_path`, containing `best.style`, `best.prompt`,
      the as-of `sibling_titles` and the generated title.
- [ ] A newly published short has non-NULL `title_generation_input` containing the exact prompted
      transcript slice, its truncation flags, and the accepted title.
- [ ] A forced DB failure at either write site logs and continues; publication still succeeds.
- [ ] Serialized payloads contain no key outside the declared schema (no URLs, paths, or tokens).
