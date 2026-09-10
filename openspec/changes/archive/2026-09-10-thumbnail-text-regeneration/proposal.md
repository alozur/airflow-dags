# Proposal: Regenerate the thumbnail when the verifier flags its text

Issue #545 · base `origin/main` @ caefba2 · long-form upload path only.

## Intent

**Problem.** The verifier checks thumbnail text but is contractually forbidden from correcting it ("NUNCA se
corrige"). A `thumbnail_text` finding under a `pass`/`correctable` verdict produces **zero** operator signal
beyond a raw JSONB row, and the video publishes with the flagged text. The text is baked into the Pikzels
prompt before option selection (`thumbnail_prompt.py:115`), so it cannot be patched — only regenerated.

**Why now.** The #512 verifier reached production 2026-09-09; it detects the defect and drops it. Base rate
is **unmeasurable** (1 verified row, 0 findings), so the design must be safe at *any* frequency, not tuned
to an assumed-rare event.

## Scope

### In Scope
- Regeneration inside `t6b _verify_final_copy` (`youtube_upload_dag.py`), before `t7` publishes.
- Migration **052**: prior-brief snapshot, regenerated-brief retention, bounded attempt counter.
- Bounded poll + claim-before-act, modelled on `video_analytics_actions_dag.py`.

### Out of Scope
- **Shorts** — `reap_shorts_uploader_dag.py` never passes `thumbnail_text`; `video_shorts` has no column.
- Fixing the pre-existing unbounded `while True` in `trigger_thumbnail_generation` (separate follow-up).
- Any change to the verifier's LLM contract.

## Capabilities

### New Capabilities
- `thumbnail-text-regeneration`: bounded, non-blocking regeneration triggered by a thumbnail-text finding.

### Modified Capabilities
- `final-copy-verification`: "Thumbnail Text Flagged Without Correction" — the finding now *also* drives a
  bounded downstream regeneration; the LLM still never corrects, and "Hard-Rejection Asymmetry" is unchanged.

## Approach

On a `thumbnail_text` finding: claim an attempt (counter-guarded, `output_path`-scoped, shape from #331
`thumbnail_republish_attempts`), snapshot the prior brief, trigger `generic_thumbnail_generator`, poll with a
**bounded** budget, then swap `upload_config["videos"][0]["thumbnail_file"]` before `t7`.

**Poll bound: ~1000s recommended** (900–1200s acceptable; design may refine). Measured over 75 production
runs: p50 214s, p95 888s, **max 3989s**. The max exceeds the analytics DAG's 1800s bound, so the timeout
branch is **load-bearing and regularly exercised**, not an edge case. On timeout, exhausted attempts, or any
failure, the task **publishes with the existing thumbnail** and records the outcome. That makes #512's
non-blocking asymmetry structural, not aspirational: no path delays or prevents publication.

**Shared-`chapter_id` fork — DECIDED.** `video_thumbnails` is keyed `(chapter_id, label)` and shared with
every sibling turn. Regeneration's *effect* scopes to the triggering turn's own canonical `thumbnail.png`;
the `video_thumbnails` write is **best-effort/audit-only, not authoritative for siblings**.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/youtube_upload_dag.py` | Modified | `t6b` regeneration branch; bounded trigger/poll helper |
| `congress_videos/modules/database.py` | Modified | Attempt claim/abandon, brief snapshot + retention |
| `congress_videos/sql/migrations/052_*.sql` | New | Snapshot, retained brief, attempt counter |
| `congress_videos/video_analytics_actions_dag.py` | Read-only | Reference shape only |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Added latency on the pre-publication path | High | Bounded poll ~1000s; timeout publishes as-is |
| Sibling-turn brief clobbering | Med | Effect scoped to the turn's own file; DB write audit-only |
| Unthrottled Pikzels/OpenAI spend | Med | Attempt counter is the spend ceiling, not just a loop guard |
| Regeneration reintroduces bad text | Low | Counter caps retries; publication proceeds regardless |
| Base rate unknown | Certain | Design safe at any frequency; no rare-event assumption |

## Rollback Plan

Revert the slices in reverse order; migration 052 is additive so its columns can remain unused. Reverting the
`t6b` wiring alone restores today's flag-and-publish behaviour with no schema rollback required.

## Dependencies

- `generic_thumbnail_generator` DAG availability (absence is a normal, non-blocking timeout).
- Migration 052 applied before the wiring slice runs in any environment.

## Delivery Sketch (`auto-chain`, feature-branch-chain, 400-line budget)

1. Migration 052 + `database.py` accessors + tests.
2. Bounded trigger/poll/claim helper + tests (no `t6b` wiring yet).
3. `t6b` wiring, thumbnail swap, audit recording, docs + tests.

Forecast: each slice is expected to land under 400 changed lines.

## Success Criteria

- [ ] A thumbnail-text finding triggers at most a bounded number of regeneration attempts.
- [ ] The flow is idempotent and cannot loop; re-running `t6b` does not consume extra attempts.
- [ ] Both the original and the regenerated brief are retained for audit.
- [ ] Publication behaviour is byte-for-byte unchanged when regeneration is unavailable, times out, or fails.
- [ ] `uv run pytest` passes; no thumbnail-text path can raise into `t7`.
