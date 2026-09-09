# Proposal: Verify Final Copy Before Publication

Issue #512. Stacked on `canonical-politician-display-names` (#511).

## Intent

**Problem**: title, description and thumbnail text are LLM-generated with no independent check before publication. Wrong politician names, wrong party attributions, spelling/grammar errors and unsupported claims reach YouTube. Nothing records what was actually published — turn/short title and description exist only as XCom plus `title.txt`/`description.txt` sidecars, so errors cannot be audited after the fact.

**Success**: an independent verifier runs at both publish seams, corrects title/description within trusted evidence, flags thumbnail text, and leaves an auditable DB record.

## Scope

### In Scope
- Shared module `congress_videos/modules/final_copy_verification.py` (convention: `upload_marking.py`, `mentioned_people_resolution.py`), consumed by both DAGs.
- Verifier prompt pair in `congress_videos/config/ai_prompts.py`; call via `cached_json_completion` → `generate_json_completion`, `model=LLM_DEFAULT`, no `temperature`/`max_tokens`/`max_completion_tokens` (#375).
- Evidence gathering reuses `database.get_chapter_metadata` (:1866), `get_turn_speaker_slug` (:1893), `lookup_participant_by_slug`; party from free-text `congress_participants.party`/`parliamentary_group` (no parties table exists).
- Verifier task at both seams; bounded correction of **title and description only**; migration **050** audit columns; tests.

### Out of Scope
- **Thumbnail-text auto-correction** — architecturally impossible here: the text is baked into the PNG by `_task_generate_thumbnail` before the winning option is chosen. Verified and flagged only. **Follow-up issue: automatic thumbnail regeneration on rejected thumbnail text.**
- **Chapter path** `prepare_chapter_upload_config` (`modules/youtube/youtube_upload.py:140-297`) — dead in production since #171 per the DAG docstring. No verification, schema or tests for it.
- New alerting channel; any new fail-loud path (see asymmetry).

## Capabilities

### New Capabilities
- `final-copy-verification`: independent pre-publication verification, bounded correction and audit persistence of published copy.

### Modified Capabilities
- None.

## Approach

| Seam | Between | Verifier input |
|---|---|---|
| Long-form turn | `t6 _prepare_upload_config` (`youtube_upload_dag.py:930-1029`; title L969, description L984, sidecars L986) and `t7 trigger_upload_with_config` (L1071-1139) | title, description, thumbnail text (`video_thumbnails.art_direction_brief->>'text'`, `chapter_id` + `is_chosen`), evidence |
| Shorts | `t2 _generate_metadata` (`reap_shorts_uploader_dag.py:237-392`) and `t3 _trigger_youtube_upload` (L394-494) | title, description, evidence (no thumbnail step exists) |

**Verdict schema** (single JSON): `verdict` ∈ `pass|correctable|reject`; `findings[]` = `{field, category, severity, detail, suggestion}` with `field` ∈ `title|description|thumbnail_text` and `category` ∈ `person_name|party_name|spelling|grammar|language|unsupported_claim`; `corrected` = `{title?, description?}`.

**Bounded correction**: at most **one** correction round, then a mandatory recheck of the corrected text. Corrections must be derivable from the supplied evidence — no new identities, parties or claims. Verifier unavailable, inconclusive, malformed, or a correction unsupported by evidence ⇒ publish the existing value unchanged and write no correction.

### Hard-rejection asymmetry — deliberate and load-bearing

A `reject` verdict blocks publication **only on the turn TITLE**, reusing the fail-loud path that already exists (`raise ValueError`, `youtube_upload_dag.py:964-976`, #245). For **description, thumbnail text and the entire shorts path there is no existing fail-loud path and this change MUST NOT invent one**: a `reject` is persisted and surfaced observably, and publication proceeds via the existing safe fallback (e.g. the hardcoded Spanish shorts description, `reap_shorts_uploader_dag.py:304`). Rationale: a verification feature must not silently convert soft-failure paths into new publication blockers. This is a documented limitation, not an oversight.

**Observability**: verdict pushed to XCom and appended to the existing `_check_upload_failures` accumulator (`youtube_upload_dag.py:1141-1189`, #320 D6 / #332) on reject or persistence failure. No new channel.

**Migration 050** (precedent: 048, multiple `ALTER TABLE` in one file) adds to `speaker_turn_videos` and `video_shorts`: `copy_verification_verdict`, `copy_original_title`, `copy_original_description`, `copy_corrected_title`, `copy_corrected_description`, `copy_verification_findings JSONB`, `copy_verified_at`. Writes are bounded, idempotent, keyed to the verified row, and skipped when the verdict is inconclusive. `production_schema.sql` updated in lockstep.

**#511 consumption**: every display-name rendering for evidence and corrections (`lookup_participant_by_slug` call sites in `youtube_upload_dag.py` and `reap_shorts_uploader_dag.py`, and the new module) MUST use #511's canonical catalogue, not raw `congress_participants.display_name`.

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/modules/final_copy_verification.py` | New | Verifier, verdict parsing, bounded correction, persistence |
| `congress_videos/config/ai_prompts.py` | Modified | Verifier system prompt + user template (Spanish, neutral register) |
| `congress_videos/youtube_upload_dag.py` | Modified | Verifier task at long-form seam; accumulator finding |
| `congress_videos/reap_shorts_uploader_dag.py` | Modified | Verifier task at shorts seam |
| `congress_videos/sql/migrations/050_*.sql`, `sql/production_schema.sql` | New/Modified | Audit columns + lockstep snapshot |
| `congress_videos/modules/database.py` | Modified | Bounded idempotent audit writes |

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Verifier hallucinates a "correction" | Med | Corrections constrained to supplied evidence; unsupported ⇒ discarded, original published |
| Duplicate surnames (three distinct "Rodríguez" in prod) misresolved | Med | Evidence carries slug + full display name + party; name checks are slug-anchored |
| Extra LLM call delays publication | Low | One call per publication; `cached_json_completion` gives Airflow-retry idempotency (`retries=1`) |
| Turn-title `reject` blocks an otherwise fine publication | Med | Reuses existing #245 raise; retry-safe; findings visible in the task log |
| Migration 050 not applied before code lands | Low | Migration ships in slice 1, ahead of every consumer |

## Rollback Plan

Revert the stacked PRs in reverse order. The verifier is additive: with the task removed, both seams behave exactly as today. Migration 050 `DOWN` drops the added columns (uncommented `DOWN` block — the whole file runs in one transaction).

## Dependencies

- #511 `canonical-politician-display-names` must land first (display-name catalogue).
- Migration 050 applied in both schemas before the wiring slices reach production.

## Success Criteria

- [ ] Verifier is a distinct LLM call from the title, thumbnail-text and description generators, receiving the three fields as separate named inputs plus trusted evidence.
- [ ] It checks politician names, party names, spelling, grammar and language use, returning `pass|correctable|reject` with field-level findings.
- [ ] At most one correction round, followed by a recheck; corrections never invent identities, parties or claims.
- [ ] Turn-title `reject` blocks publication; every other field/path persists the verdict and publishes the existing fallback.
- [ ] Verdict, original value, corrected value and findings are persisted idempotently; nothing is written when inconclusive or unsupported.
- [ ] Verifier unavailable/inconclusive preserves the existing safe fallback and is observable via the accumulator.
- [ ] Tests cover: consistent copy, politician-name correction, party correction, spelling/grammar correction, speaker/person mismatch, unsupported claims, malformed verifier output, verifier failure, thumbnail-text flag-without-correction, and audit-write idempotency.
- [ ] `uv run pytest`, `ruff check` and `ruff format --check` pass on every slice.

## Delivery Slices (stacked to main, ≤400 changed lines each)

1. **Migration 050 + schema snapshot + drift test** — no consumers yet.
2. **`final_copy_verification` module + prompts + verdict parsing/correction contract + unit tests** — pure module, not yet wired.
3. **Long-form turn seam wiring + audit persistence + accumulator finding + DAG tests.**
4. **Shorts seam wiring + audit persistence + DAG tests.**
