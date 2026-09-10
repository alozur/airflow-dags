# Design: Regenerate the thumbnail when the verifier flags its text

Issue #545 · base `origin/main` @ caefba2 · long-form `youtube_upload_dag.py` only (shorts out of scope).

## Technical Approach

`t6b _verify_final_copy` gains one branch, placed **after** the locked title-reject raise: when
`verdict.findings` contains `field == "thumbnail_text"`, claim a paid attempt, trigger
`generic_thumbnail_generator` with the triggering turn's own `output_path`, poll under a hard bound, then
re-push `upload_config` before `t7`. The branch performs **at most one attempt per `t6b` execution** — never a
loop — and is wrapped in a single `try/except Exception` that logs and continues, so it has no path to raise.

## Architecture Decisions

### D1 — Migration 052 columns live on `speaker_turn_videos`

| Option | Tradeoff | Decision |
|---|---|---|
| `video_thumbnails` | Natural home for a brief, but `(chapter_id, label)` UNIQUE + `ON CONFLICT DO UPDATE` means the child DAG's own `persist_results` **clobbers the counter it is meant to bound**; also chapter-scoped, so siblings share it | Rejected |
| New table | Correct scoping, but a whole table for 7 audit columns with no query pattern beyond one key | Rejected (YAGNI) |
| `speaker_turn_videos` | `output_path`-scoped, already hosts `copy_verification_*` (#512) and `thumbnail_republish_*` (#331) | **Chosen** |

```sql
ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS thumbnail_regen_attempts    INTEGER     DEFAULT 0,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_exhausted   BOOLEAN     DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_at          TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_outcome     TEXT,
    ADD COLUMN IF NOT EXISTS last_thumbnail_regen_error  TEXT,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_prior_brief JSONB,
    ADD COLUMN IF NOT EXISTS thumbnail_regen_brief       JSONB;
```

Additive, all nullable/defaulted; `video_shorts` deliberately untouched (unlike 050/051). The DOWN block is
**commented out**, per 050/051: `migrations_dag` runs the whole file in one transaction, so a live DOWN
silently reverts the migration.

### D2 — Poll bound: `_THUMBNAIL_REGEN_MAX_POLLS = 100` × 10 s = **1000 s**

Measured (75 runs): p50 214 s, p95 888 s, max 3989 s. 1000 s clears p95 with ~13 % headroom and refuses the
analytics DAG's 1800 s, whose post-publication context tolerates waiting that a pre-publication upload task
does not. Expected hit rate: **under 5 %** of regenerations — we only hold p50/p95/max, so the exact tail
fraction between 888 s and 3989 s is not resolvable; treat 5 % as the upper bound, not a point estimate.
Worst case added latency per run is one bound (1000 s), because a run makes at most one attempt.

### D3 — `THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS = 2` — a spend ceiling, not a loop guard

#331 uses 3 because its retries call a **free** YouTube `thumbnails.set()`. Each attempt here costs 1–2
Pikzels images + 1 OpenAI title call with **no throttle anywhere in the codebase**, so this counter is the
only ceiling. 2 caps a video at ≤4 images + 2 LLM calls. A third re-roll of the same art-direction inputs has
low marginal probability of fixing a systematic defect (e.g. a wrong person name in the evidence) while
raising fleet spend 50 % at an unmeasurable base rate. Shape borrowed from #331; the number is not.

Claim-before-act (`_apply_one_action` shape): one atomic conditional UPDATE charges the attempt **before** any
paid call, so a crash after spending cannot re-spend for free.

```sql
UPDATE speaker_turn_videos
   SET thumbnail_regen_attempts = COALESCE(thumbnail_regen_attempts,0)+1,
       thumbnail_regen_exhausted = (COALESCE(thumbnail_regen_attempts,0)+1 >= 2),
       thumbnail_regen_at = NOW(),
       thumbnail_regen_prior_brief = COALESCE(thumbnail_regen_prior_brief, %s::jsonb)  -- write-once
 WHERE output_path = %s
   AND NOT COALESCE(thumbnail_regen_exhausted, FALSE)
   AND COALESCE(thumbnail_regen_attempts,0) < 2
RETURNING thumbnail_regen_attempts, thumbnail_regen_exhausted;
```

`None`/rowcount 0 = not claimed (exhausted, or no row — chapter items have no `speaker_turn_videos` row) →
publish as-is. `COALESCE` keeps the *first* brief as the true original across attempts.

### D4 — The timeout branch is the design, not an error path

Trigger failure, child failure, invalid result, timeout, exhausted counter and unclaimable key **all converge
on one behaviour**: record the outcome best-effort, leave `upload_config` untouched, publish with the existing
thumbnail, return `None`. Three structural guarantees:

1. The branch sits **after** the title-reject `raise`, so it can never suppress the locked asymmetry.
2. It is enclosed in one `try/except Exception: logging.exception(...)` that has no `raise`.
3. The outcome write uses the `_write_title_provenance` failure-isolation shape (catch, log, return status) —
   a DB outage cannot become a publication outage.

Rejected: an Airflow `execution_timeout` on `t6b` as a backstop — a task timeout **fails** `t6b`, skips `t7`,
and converts a thumbnail-text finding into a publication block. The in-code bound is the only guarantee.

Operator signal: `_copy_verification_problems` gains one line when a `thumbnail_text` finding was raised and
regeneration did not land. It feeds `_check_upload_failures`, which raises **after** `mark_chapters_uploaded`
— informational, non-blocking, exactly as #512 established.

### D5 — How the regenerated thumbnail reaches `t7`

Same in-memory XCom rewrite `t5b _apply_intro_overlay` (#558) and `t6b`'s own correction branch already use:
mutate the pulled dict, re-push under the same key. Concretely:

```python
video["thumbnail_file"] = regen["output_path"]   # only when os.path.exists(...)
mutated = True
...
if mutated:                       # ONE push covering correction + swap
    ti.xcom_push(key="upload_config", value=config)
```

Both mutations must share a single push; today's push lives inside `if verdict.correction_applied:` and must
be hoisted, or a regeneration without a correction would be silently dropped.

**Load-bearing subtlety**: for turn items the child DAG's `_persist_canonical_thumbnail` overwrites
`<video_dir>/thumbnail.png` — the exact path `prepare_orador_upload_config` already put in `thumbnail_file`.
So `t7` picks up the new **bytes** with no XCom change at all; the swap is defence-in-depth for turns and the
real mechanism only for the legacy chapter path. A returned path that does not exist on disk is recorded as
`invalid_result` and never swapped in.

### D6 — Sibling isolation: files are authoritative, the DB row is not

| Layer | Guarantee |
|---|---|
| File | `child_conf["output_path"] = video["video_file"]` (the triggering turn's own `video.mp4`), so the child writes **only** `<that turn's dir>/thumbnail.png`. Sibling turn B has a different `video_dir` and a different `thumbnail.png`; nothing B publishes changes. Grouped siblings sharing one `output_path` are the same physical video by construction (#129). |
| DB | The child's `persist_results` **will** upsert the shared `(chapter_id, label)` row. We cannot stop it without editing the child DAG (out of scope), so we snapshot the prior brief into `thumbnail_regen_prior_brief` keyed by the turn's `output_path` **before** triggering, and treat `video_thumbnails` as audit-only, never authoritative for a sibling's file. |

**Named residual hazard** (not fixed here): a sibling verified *after* A's regeneration reads
`get_chosen_thumbnail(chapter_id)` and therefore verifies A's regenerated text while its own
`thumbnail.png` may still carry the old text. That mismatch is pre-existing — the row was never per-turn — and
#545 only makes it diagnosable. Recommend a follow-up issue.

## Data Flow

```
t6 prepare_upload_config ──→ t6b _verify_final_copy ──→ t7 trigger_youtube_upload
                                   │
                      thumbnail_text finding?
                                   │ yes
              claim attempt (052, output_path-scoped, snapshot prior brief)
                                   │ claimed
              trigger generic_thumbnail_generator (conf.output_path = turn's video.mp4)
                                   │
                      poll ≤100 × 10 s  ──timeout/fail──→ record outcome, publish as-is
                                   │ success
              child overwrites <video_dir>/thumbnail.png ; swap thumbnail_file ; one xcom_push
```

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/sql/migrations/052_thumbnail_text_regeneration.sql` | Create | D1 columns; DOWN commented out |
| `congress_videos/modules/database.py` | Modify | `claim_thumbnail_text_regeneration`, `record_thumbnail_text_regeneration_outcome` |
| `congress_videos/youtube_upload_dag.py` | Modify | Regen constants + `_regenerate_flagged_thumbnail` helper; `t6b` branch; hoisted single `xcom_push`; `_copy_verification_problems` line |
| `tests/congress_videos/...` | Modify | RED-first unit tests per slice (`strict_tdd: true`) |
| `CONTEXT.md` / `docs/adr/` | Modify | Record the bound, the threshold and the audit-only DB semantics |

## Interfaces / Contracts

```python
_THUMBNAIL_REGEN_POLL_INTERVAL_SECONDS = 10
_THUMBNAIL_REGEN_MAX_POLLS = int(os.getenv("UPLOAD_THUMBNAIL_REGEN_MAX_POLLS", "100"))  # 1000s
THUMBNAIL_TEXT_REGEN_MAX_ATTEMPTS = 2

def claim_thumbnail_text_regeneration(output_path: str, *, prior_brief: dict | None) -> dict | None: ...
def record_thumbnail_text_regeneration_outcome(
    output_path: str, *, outcome: str, error: str | None = None, regenerated_brief: dict | None = None
) -> int: ...
# outcome ∈ {applied, timeout, trigger_failed, child_failed, invalid_result, not_claimed}
```

## Testing Strategy

| Layer | What to Test | Approach |
|---|---|---|
| Unit (DB) | Claim is atomic and idempotent at the ceiling; second claim past threshold returns `None`; `prior_brief` is write-once; unknown `output_path` returns `None` | pytest against the live-Postgres fixture already used by the `copy_*` tests |
| Unit (helper) | Timeout returns after exactly `_THUMBNAIL_REGEN_MAX_POLLS` iterations; trigger exception, child `failed`, malformed XCom each return a non-success dict; never raises | Fake `dag_run` with scripted `refresh_from_db` states, monkeypatched `trigger_dag_api` / `XCom.get_one` / `time.sleep` |
| Unit (`t6b`) | Finding → one claim, one trigger; **no** finding → zero claims; every failure mode still returns `None` and leaves `upload_config` unmutated; success swaps `thumbnail_file` with exactly one `xcom_push`; title-reject still raises before any claim | Existing `test_youtube_upload_dag.py` fake-`ti`/fake-`db` conventions |
| E2E | DAG import stays clean | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) |

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or process-integration
boundary. The child DAG is triggered through the existing in-cluster `trigger_dag_api` helper, not a spawned
process, and no argument reaches a shell.

## Migration / Rollout

Migration 052 **must be applied before slice 3 runs in any environment** (051 is current; production is
confirmed at 051). Slices 1–2 are inert without it. Rollback: revert in reverse order; 052 is additive so its
columns may simply stay unused, and reverting slice 3 alone restores today's flag-and-publish behaviour with
no schema rollback.

**Slicing** (`auto-chain`, feature-branch-chain, 400-line budget; tracker PR draft on
`feat/545-thumbnail-text-regeneration`, PR1 → tracker, PR2 → PR1, PR3 → PR2):

| # | Work unit | Est. lines | Rollback boundary |
|---|---|---|---|
| 1 | Migration 052 + two `database.py` accessors + tests | ~250 | Drop the accessors; columns stay unused |
| 2 | Constants + bounded trigger/poll helper + tests (no wiring) | ~300 | Delete the helper; nothing calls it |
| 3 | `t6b` branch, single `xcom_push`, operator-signal line, docs + tests | ~330 | Revert to flag-and-publish |

`Decision needed before apply: No` · `Chained PRs recommended: Yes` · `400-line budget risk: Medium`

## Open Questions

- [ ] None blocking. Deferred to follow-ups: (a) the unbounded `while True` in `trigger_thumbnail_generation`
      (pre-existing, out of scope per the proposal); (b) the shared-`(chapter_id, label)` brief-vs-file
      mismatch named in D6.
- [ ] Base rate is **unmeasurable today** (1 verified row, 0 findings; the #512 verifier reached production
      2026-09-09). Every bound above is chosen to be safe at any frequency, not tuned to a rare event. Revisit
      D2 and D3 once a real base rate exists.
