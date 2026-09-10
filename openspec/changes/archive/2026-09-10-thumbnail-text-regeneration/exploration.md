# Exploration: Regenerate the thumbnail when the verifier flags its text

Issue: #545 — `feat(thumbnail): regenerate the thumbnail when the verifier flags its text`
Date: 2026-09-10
Phase: explore (read-only; grounded against `feat/545-thumbnail-text-regeneration` off `main` @ caefba2)

> **Unlike #558 and #544, this issue's technical claims check out.** Both of those issues carried materially
> wrong or overstated statements that only surfaced on inspection. #545's core claims — thumbnail text is
> baked into the Pikzels prompt before option selection, the verifier flags but never corrects it, and the
> non-blocking asymmetry must be preserved — are all **confirmed accurate against the code**. What the issue
> omits is more consequential than anything it gets wrong; see "Findings not in the issue" below.

## Current State

### The verifier

`congress_videos/modules/final_copy_verification.py::verify_final_copy` accepts `thumbnail_text`, renders it
into the LLM prompt, and includes it in `compute_content_version`. The system prompt (`ai_prompts.py`) is
explicit:

> "El texto de miniatura se verifica pero NUNCA se corrige: no lo incluyas en 'corrected'."

A `CopyFinding(field="thumbnail_text", ...)` can appear in `verdict.findings` regardless of the overall
verdict (`pass` / `correctable` / `reject`). `verify_final_copy` never raises; `run_correction_round` runs at
most 2 LLM calls.

It is wired **only** in `congress_videos/youtube_upload_dag.py`, task `t6b` (`_verify_final_copy`, ~lines
1341-1471), which runs **between** `t6 prepare_upload_config` and `t7 trigger_youtube_upload` — i.e. it fires
**before** the video is ever published, for both turn and chapter items.

### What actually happens to a thumbnail-text finding today

It is persisted verbatim in `speaker_turn_videos.copy_verification_findings` (JSONB, via
`record_copy_verification_turn`). It becomes operator-visible only through `_copy_verification_problems()`
when its `category == "unsupported_claim"` (a generic, non-field-specific check) or when the overall verdict
is `reject`/`inconclusive`.

**A thumbnail_text finding of category `person_name` / `spelling` / `grammar` under an otherwise
`correctable` or `pass` verdict produces zero operator-visible signal beyond the raw JSONB row.** The
issue's "the flag leads nowhere automatic" framing is accurate, not overstated.

### The non-blocking asymmetry (#512) — quoted, confirmed

```python
if verdict.verdict == "reject" and any(f.field == "title" for f in verdict.findings):
    raise ValueError(...)
```

**Only a title reject blocks publication.** Description and thumbnail-text findings merely feed
`_copy_verification_problems`, which raises a batched `Exception` in `_check_upload_failures` **after**
`mark_chapters_uploaded` has already run — informational, not blocking. This is the invariant #545 must not
break.

### Where the thumbnail text is baked in — CONFIRMED

`congress_videos/modules/thumbnail_prompt.py:115` does `text_upper = art_brief["text"].upper()`, passed as
`text=text_upper` into the Pikzels prompt inside `generate_thumbnail_option_a` / `_b`, which run **before**
`choose_best_option`.

So regeneration must re-run the whole `art_direction -> generate -> score -> choose_best` pipeline. It
cannot patch a field.

## Findings NOT in the issue

### A. A migration IS required (next number: 052)

`copy_thumbnail_text` is a **single** column — the verified text, not an original/regenerated pair. There is
no attempt counter. And `video_thumbnails` is keyed `(chapter_id, label)` UNIQUE and upserted destructively
(`ON CONFLICT ... DO UPDATE`), so a regeneration would **clobber the prior brief** unless it is snapshotted
first — exactly as `video_analytics_actions_dag.py::_apply_one_action` already does, capturing
`prior = {"archetype":..., "title":..., "local_path":...}` into `action_detail` before triggering.

Migration 052 must cover: prior-brief snapshot, regenerated-brief retention, and the bounded attempt counter.

### B. ONE path, not two — shorts are out of scope

`congress_videos/reap_shorts_uploader_dag.py`'s `t2b` calls
`verify_final_copy(title=..., description=..., evidence=...)` — **no `thumbnail_text` argument at all** — and
`video_shorts` (migration 050) has no `copy_thumbnail_text` column. Shorts never verify thumbnail text, so
there is nothing there for #545 to hook into. Adding shorts support would be new scope beyond what #545 asks.

### C. The right precedent is `video_analytics_actions_dag.py`, NOT the #331 healer

There are **two** existing call sites that trigger `generic_thumbnail_generator`, and they are not equally
good models:

| Call site | Context | Poll |
| --- | --- | --- |
| `youtube_upload_dag.py::trigger_thumbnail_generation` (~794-882) | pre-publication | **`while True` — UNBOUNDED. A real existing risk.** |
| `video_analytics_actions_dag.py::_apply_one_action` + `_poll_thumbnail_dag_run` | post-publication | bounded: `_THUMBNAIL_MAX_POLLS = 180` x 10s, claim-before-act, prior-brief steering via `previous_brief` |

#545 must copy the **bounded** shape from the analytics-actions DAG. The unbounded t4 loop must not be the
template.

`thumbnail_republish_attempts` / `thumbnail_republish_abandoned` (THRESHOLD=3, from the #331 healer) is the
right SHAPE for a bounded counter — output_path-scoped, claim-before-act — but it was built for a different
failure mode (post-upload `thumbnails.set()` failure). Take the counter shape from #331 and the trigger
mechanics from the analytics DAG.

### D. `video_thumbnails` rows are SHARED across sibling turns — a real design fork

`video_thumbnails.chapter_id` is shared across a chapter's own upload and **every sibling turn of that
chapter** (`uploadable_turns` view: `st.chapter_id = vc.chapter_id`, confirmed at `production_schema.sql:610,632`).

So `get_chosen_thumbnail(chapter_id)` can return a brief regenerated by a *different* turn's verification
pass. Any #545 design must decide **explicitly** whether regeneration scopes to the shared chapter-level DB
row or only to the triggering turn's own canonical `thumbnail.png` file.

This is a design fork, not a detail: get it wrong and one turn's verification silently rewrites a sibling's
brief.

### E. Cost, and the absence of any throttle

Fast path: 1 Pikzels image + 1 OpenAI title call. The score-below-threshold retry path (already common):
2 Pikzels images + 1 OpenAI title call. **No existing quota or throttle guards this** — the bounded attempt
counter would be the only spend ceiling, which makes it a cost control rather than merely a loop guard.

See `evidence-regeneration-cost.md` for the measured production timings that bound the latency question.

## Affected Areas

- `congress_videos/youtube_upload_dag.py` — `t6b _verify_final_copy` (~1341-1471), `trigger_thumbnail_generation`
  (~794-882), `_copy_verification_problems` (~703-750), `_thumbnail_brief_text` (~688-700)
- `congress_videos/modules/database.py` — `record_copy_verification_turn`, `get_chosen_thumbnail`, and the
  `select_turns_needing_thumbnail_republish` / `record_turn_thumbnail_republish_failure` bounded-retry precedent
- `congress_videos/sql/migrations/` — new migration **052**
- `congress_videos/video_analytics_actions_dag.py` — read-only reference for the trigger/poll/snapshot shape
- NOT affected: `reap_shorts_uploader_dag.py`, `video_shorts` — shorts are out of scope
- No change needed to the LLM contract in `final_copy_verification.py` itself

## Approaches

### 1. Pre-publication regeneration inside t6b (RECOMMENDED)

On a thumbnail-text finding: claim-before-act via a new bounded counter, trigger
`generic_thumbnail_generator` with a **bounded** poll copied from `video_analytics_actions_dag.py`, swap
`upload_config["videos"][0]["thumbnail_file"]` before t7 fires, and snapshot the prior brief for audit.

- **Pros**: the video is not live yet, so no post-hoc YouTube `thumbnails.set()` call is needed; reuses
  proven trigger/poll/claim code; preserves #512's asymmetry trivially, since it never raises.
- **Cons**: adds real latency to the critical upload path; must resolve the shared-`chapter_id` ambiguity.
- **Effort**: Medium.

### 2. Post-publication healer DAG mirroring `video_analytics_actions_dag.py`

Record the finding at t6b as today, and let a new healer-style DAG (like #331) pick it up later and
republish via `thumbnails.set()`.

- **Pros**: does not slow the upload DAG; mirrors a battle-tested async pattern.
- **Cons**: the video is briefly live with wrong thumbnail text; needs its own polling DAG plus new schema;
  more moving parts.
- **Effort**: Medium-High.

## Recommendation

**Approach 1**, scoped to `youtube_upload_dag.py` only (long-form, turn + chapter — not shorts), reusing the
analytics-actions DAG's claim/poll/snapshot mechanics and a new output_path-scoped bounded-attempt column
pair modelled on `thumbnail_republish_attempts` / `abandoned`.

The shared-`chapter_id` clobbering risk must be resolved explicitly in design. Recommendation: scope the
regeneration's *effect* to the triggering turn's own canonical `thumbnail.png`, treating the
`video_thumbnails` row write as best-effort/audit-only rather than authoritative for siblings.

## Risks

1. **Shared `(chapter_id, label)` row** can race across sibling turns processed in parallel or in sequence —
   needs an explicit scoping decision.
2. **The existing unbounded poll** in `trigger_thumbnail_generation` must not be the template. (It is also a
   pre-existing risk in its own right, worth a separate follow-up issue.)
3. **No Pikzels/OpenAI cost throttle** beyond the new attempt counter.
4. **Added latency on the pre-publication path** — quantified in `evidence-regeneration-cost.md`.

## Ready for Proposal

Yes. The three big open questions are answered: a migration IS required, only the long-form path needs
wiring, and there IS an existing bounded-retry pattern to reuse. The one item `sdd-propose` must decide
explicitly rather than infer is the shared-`chapter_id` scoping question.
