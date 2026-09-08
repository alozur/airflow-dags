# Proposal: Freshness bucket as the leading publish-order key for `uploadable_turns`

## Intent

**Problem.** `uploadable_turns` ranks purely on editorial quality (`interest_score`, `relevance_score`, `session_date`) and drains at 1 upload/day. Live prod evidence in issue #513: a 2026-09-04 turn sat behind three 2026-06-10 turns, so a clip about a session from days ago waits at least three days while three-month-old material publishes first. Congressional content is news-shaped — its value decays, and a stale-first queue publishes it after it stopped mattering.

**Success.** Any turn from a session in the last 14 days outranks every older turn, while ordering *within* each bucket keeps today's editorial and FIFO semantics byte-for-byte.

## Scope

### In Scope
- New migration `congress_videos/sql/migrations/049_*.sql`: `DROP VIEW` + `CREATE VIEW uploadable_turns`, carrying migration 044's body verbatim with one new **leading** `ORDER BY` key and an inline `-- issue #513` comment.
- Sync the snapshot `congress_videos/sql/production_schema.sql:537-633` (view body, `COMMENT ON VIEW`, cumulative lineage header gains a `049` line).
- Update `tests/congress_videos/sql/test_production_schema.py`: repoint `MIGRATION_PATH` to 049, retarget `TestSnapshotLockstepWithLatestMigration`, add `TestUploadableTurns049FreshnessBucket`.

### Out of Scope
- Application code. `get_uploadable_turns()` is `SELECT * FROM uploadable_turns LIMIT %s` with no external `ORDER BY` — confirmed, untouched.
- `uploadable_chapters`, shorts selection, DAG schedules, quota rules.
- Any eligibility, filter, join, or column change.
- Making 14 days configurable, or a live-Postgres ordering test (neither exists today for this view).

## Capabilities

### New Capabilities
- `turn-publish-order`: the deterministic publish ranking of `uploadable_turns` — freshness bucket, editorial keys, FIFO tie-break, total-order backstop.

### Modified Capabilities
- None.

## Approach

Add exactly one leading key; change nothing else:

```sql
ORDER BY (dedup.session_date >= CURRENT_DATE - INTERVAL '14 days') DESC,  -- freshness bucket (#513)
         COALESCE(dedup.interest_score, 1) DESC,
         dedup.relevance_score DESC,
         dedup.session_date DESC,
         dedup.materialized_at ASC,
         dedup.turn_id ASC;
```

**ELIGIBILITY-NEUTRAL**, stated as migration 044 states it: the body is carried forward verbatim — the unfiltered `group_spans` CTE (issue #151 trap), the `DISTINCT ON (stv.output_path)` dedup, every inner `WHERE` gate, and the 300s published-duration floor. **No row enters or leaves the view; only the sequence changes.**

`CURRENT_DATE` is safe here: `uploadable_turns` is a plain view re-executed per query (no materialization, no expression index), `session_date` is a plain `DATE`, and `CURRENT_DATE` in views is already shipped precedent in this repo.

**Expected rollout effect.** On the day 049 applies, backlog rows at the 14-day edge reshuffle at once: recent turns jump ahead of the aged queue. This is the intended outcome, not a defect — no row's eligibility changes, and the aged queue resumes draining in its existing order once the fresh bucket empties.

## Rejected Alternatives

| Alternative | Why rejected |
|---|---|
| Continuous recency decay folded into one blended score | Changes editorial semantics beyond the issue's scope; much harder to keep deterministic and exact-string testable |
| Materialized freshness flag / cached column + refresh job | No performance problem to solve (`LIMIT 1`, once per day); adds staleness and a refresh job |

## Test Strategy (Strict TDD — RED first)

File: `tests/congress_videos/sql/test_production_schema.py`. Write each assertion and watch it fail before writing SQL. Exact-string assertions on normalized text, never loose regex — this repo's established anti-drift discipline for this view.

1. **Leading key** — normalized migration/snapshot `ORDER BY` starts with the exact string `ORDER BY (DEDUP.SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC, COALESCE(DEDUP.INTEREST_SCORE, 1) DESC`.
2. **Suffix preserved** — the existing `_TIEBREAK_SUFFIX` `endswith` assertion still passes unmodified (a leading key must not disturb the tail).
3. **Intent comment** — the `-- ... #513` comment survives inside the `ORDER BY` clause of both the migration and the snapshot (mirrors the #328 FIFO-comment test).
4. **Transcription guard** — normalized 049 body equals the normalized 044 body with the new key spliced in immediately after `ORDER BY `, proving nothing else drifted.
5. **Lockstep** — `TestSnapshotLockstepWithLatestMigration` compares the snapshot against 049.

## Affected Areas

| Area | Impact | Description |
|---|---|---|
| `congress_videos/sql/migrations/049_*.sql` | New | Recreates the view with the leading freshness bucket |
| `congress_videos/sql/production_schema.sql` | Modified | Snapshot + lineage sync (lockstep-enforced) |
| `tests/congress_videos/sql/test_production_schema.py` | Modified | Repoint + new assertion class |
| `congress_videos/modules/database.py` | None | No external `ORDER BY`; confirmed untouched |

## Risks

| Risk | Likelihood | Mitigation |
|---|---|---|
| Whitespace/format deviation trips the exact-string tests | High | Intentional; author the SQL to match the asserted string precisely |
| Snapshot not synced in the same PR | Med | Lockstep test fails loudly; both files ship together |
| 14-day cliff: a 15-day turn ranks below a 13-day one regardless of quality | High (by design) | Accepted per issue #513; within-bucket order is unchanged and the boundary is documented in the migration header |
| One-time backlog reshuffle at rollout | High (expected) | Documented above as desired behavior; eligibility-neutral, self-resolving |

## Rollback Plan

Manual `psql` only. The migration ships a fully commented-out `-- DOWN` block (the runner executes the whole file text in one transaction; an uncommented DOWN silently reverts on the same run) that recreates the view with migration 044's five-key `ORDER BY`. Rollback is order-only: no data loss, no eligibility change.

## Dependencies

- Migration 044 (current view body) and 048 (highest applied migration; 049 is the next free number).

## Size estimate vs. 400-line review budget

**Low risk — single PR.** New migration ≈ 170 lines (mostly the mandatory commented DOWN block and header rationale), snapshot edit ≈ 15 lines changed, tests ≈ 60 lines added. Estimated total ≈ 250 changed lines, comfortably inside the 400-line budget.

## Success Criteria

- [ ] Migration 049 exists; its `ORDER BY` is 044's five keys with exactly one new leading freshness-bucket key.
- [ ] `production_schema.sql` matches migration 049 under `_normalize_view_sql`; lockstep test passes.
- [ ] New `TestUploadableTurns049FreshnessBucket` assertions pass; the pre-existing 044 suffix assertion still passes.
- [ ] `uv run pytest` green; no `congress_videos/modules/**` or DAG file touched.
- [ ] Transcription guard proves the 049 body is 044 plus the leading key and nothing else.
