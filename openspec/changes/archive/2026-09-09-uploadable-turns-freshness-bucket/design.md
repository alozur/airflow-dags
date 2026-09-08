# Design: Freshness bucket as the leading publish-order key for `uploadable_turns`

Issue #513. Change `uploadable-turns-freshness-bucket`.

## Technical Approach

One new SQL migration recreates the `uploadable_turns` view with migration 044's body
**carried forward verbatim** plus exactly one new leading `ORDER BY` key, the snapshot in
`production_schema.sql` is synced in the same commit, and the static SQL-text test suite is
repointed and extended. No application code, no DAG, no other view.

The migration is produced by **copy-then-patch, never re-transcription**: `sdd-apply` copies
`044_deterministic_turn_publish_order.sql` byte-for-byte to `049_freshness_bucket_turn_publish_order.sql`
and applies exactly the three hunks below. This is normative — the transcription-guard test
(`test_migration_049_body_is_044_plus_leading_freshness_key`) compares normalized bodies and any
hand-retyped character that survives normalization is a hard CI failure.

## Architecture Decisions

### D1 — Copy-then-patch construction of migration 049

| Option | Tradeoff | Decision |
|---|---|---|
| Copy 044, apply 3 hunks | Body verbatim by construction; matches what the guard asserts | **Chosen** |
| Re-type the view body | Any stray whitespace/word survives normalization → CI failure | Rejected |

### D2 — `DROP VIEW IF EXISTS` + `CREATE VIEW`, unqualified names

`CREATE OR REPLACE VIEW` cannot change a view's column list in Postgres and is
`uploadable_chapters`' convention (038) only. Every prior `uploadable_turns` migration
(025/028/029/030/032/040/044) uses DROP+CREATE. The runner issues
`SET search_path TO {schema}, public`, so names stay **unqualified** in the migration and
`production.`-qualified in the snapshot; `_normalize_view_sql` strips the qualification so both
compare equal.

### D3 — `-- DOWN` block entirely commented out

The migration runner executes the whole file text in one transaction. An uncommented DOWN
silently reverts the migration on the same run (Engram `migration-down-block-silent-revert`;
stated in 044's and 048's own headers). Rollback is manual `psql` only.

### D4 — `_TIEBREAK_SUFFIX` must lose its `"ORDER BY "` prefix (correction to the exploration)

The exploration asserted the existing 044 `endswith` check "still holds unmodified". **It does
not.** `_TIEBREAK_SUFFIX` literally starts with `"ORDER BY COALESCE(DEDUP.INTEREST_SCORE, 1) DESC"`.
After a leading key is prepended, the normalized text reads
`... DESC, COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, ...` — the characters immediately before
`COALESCE` are `DESC, `, not `ORDER BY `, so `endswith` fails.

| Option | Tradeoff | Decision |
|---|---|---|
| Delete the `"ORDER BY "` line from `_TIEBREAK_SUFFIX`; keep the `endswith` | 1-line edit; still proves the five-key tail is byte-identical; positional anchoring moves to the 049 class's full-clause `endswith`, so no coverage is lost | **Chosen** |
| Rewrite the 044 assertion to `in` / regex | Weakens the repo's exact-string anti-drift discipline | Rejected |
| Delete `TestUploadableTurns044PublishOrder` | Destroys the #328 regression guard | Rejected |

The same applies to `test_tiebreak_carries_fifo_intent_comment`, whose slice marker
`"ORDER BY COALESCE(dedup.interest_score"` no longer exists in the snapshot. Only the marker
literal changes; its `FIFO` and `#328` assertions stay untouched.

### D5 — `CURRENT_DATE` in a plain view carries no immutability risk

`uploadable_turns` is a plain (non-materialized) view, re-planned and re-executed per query, so a
STABLE function is safe: no materialization staleness, no expression index to violate (a view has
none). `session_date` is a plain `DATE`, so the comparison is `DATE` vs `DATE` and
timezone-boundary-safe. **In-repo precedent:** `production_schema.sql:472` ships
`CURRENT_DATE - cs.session_date AS days_old` inside a plain view definition, from migrations
007/010/011/021/036/038.

### D6 — Test determinism

Every assertion reads static SQL file text. No DB connection, no `psycopg2`, no wall clock.
`CURRENT_DATE` is asserted **as literal text**, never evaluated — the suite gives the same verdict
on any date.

## Data Flow

    speaker_turn_videos ─┐
    speaker_turns ───────┼─→ group_spans CTE ─→ DISTINCT ON (output_path) dedup
    video_chapters ──────┤                              │
    youtube_source_videos┘                              ↓
                                         WHERE published_duration >= 300
                                                        ↓
                            ORDER BY  freshness bucket (NEW, #513)
                                      interest / relevance / session_date  (044)
                                      materialized_at ASC, turn_id ASC     (044 #328)
                                                        ↓
                              get_uploadable_turns(): SELECT * ... LIMIT 1

Only the last stage's key list changes. No row enters or leaves the view.

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql` | Create | Copy of 044 + hunks H1/H2/H3 |
| `congress_videos/sql/production_schema.sql` | Modify | Header line 537, lineage list after 552, ORDER BY at 618, `COMMENT ON VIEW` at 633 |
| `tests/congress_videos/sql/test_production_schema.py` | Modify | Repoint fixtures, adjust two 044 literals, add `TestUploadableTurns049FreshnessBucket` |
| `congress_videos/modules/database.py` | None | `get_uploadable_turns()` has no external `ORDER BY` |

## Interfaces / Contracts

### Migration 049 — construction recipe (normative)

`cp congress_videos/sql/migrations/044_deterministic_turn_publish_order.sql \
   congress_videos/sql/migrations/049_freshness_bucket_turn_publish_order.sql`

then apply exactly these three hunks. **Everything between `-- UP` and the outer `ORDER BY`, and
everything after it, stays byte-identical to 044.**

**H1 — replace 044 lines 1-39 (the whole header, up to but excluding the blank line before `-- UP`):**

```sql
-- Migration 049: freshness bucket as the leading publish-order key for
-- uploadable_turns (issue #513)
-- Created: 2026-09-08
-- Depends on: 044_deterministic_turn_publish_order.sql
--
-- uploadable_turns is the only view in this repo whose INTERNAL ORDER BY governs
-- runtime behaviour: get_uploadable_turns() runs `SELECT * FROM uploadable_turns
-- LIMIT %s` with no external ordering, and the single row it returns wins the one
-- daily long-form slot (DAILY_LONG_FORM_UPLOAD_LIMIT = 1).
--
-- Congressional content is news-shaped: its value decays. Ranking purely on the 044
-- editorial keys parked a 2026-09-04 turn behind three 2026-06-10 turns in the live
-- prod queue, so a clip about a session from days ago waited at least three days
-- while three-month-old material published first (measured 2026-09-08, issue #513).
--
-- Prepends ONE key and nothing else:
--   (dedup.session_date >= CURRENT_DATE - INTERVAL '14 days') DESC
-- Boolean DESC puts TRUE first in Postgres, so every turn from a session in the last
-- 14 days outranks every older turn. Within each bucket the 044 order is unchanged.
--
-- The 14-day cliff is deliberate: a 15-day turn ranks below a 13-day one regardless
-- of quality. That is the point — a hard bucket stays deterministic and exactly
-- testable, which a blended recency decay would not.
--
-- CURRENT_DATE is safe here: uploadable_turns is a PLAIN view, re-planned and
-- re-executed on every query, so a STABLE function carries no materialization or
-- expression-index immutability risk. session_date is a plain DATE column, so the
-- comparison is DATE vs DATE. Precedent: this repo already ships CURRENT_DATE inside
-- plain view definitions (migrations 007/010/011/021/036/038).
--
-- One-time effect on apply: backlog rows at the 14-day edge reshuffle at once and
-- recent turns jump ahead of the aged queue. Intended, bounded and self-resolving —
-- the aged queue resumes draining in its existing order once the fresh bucket empties.
--
-- ELIGIBILITY-NEUTRAL. The view body is carried forward from 044 VERBATIM — the
-- unfiltered group_spans CTE (issue #151 trap), the DISTINCT ON (stv.output_path)
-- dedup (028), every inner WHERE gate (030 prepared_at, 141 abandon, chapter
-- upload gate, relevance >= 2, interest >= 1, 143 procedural exclusion) and the
-- 300s published-duration floor (234/143) are unchanged. No row enters or leaves
-- the view; only the sequence changes.
--
-- Convention: DROP VIEW + CREATE VIEW, as every prior uploadable_turns migration
-- (025/028/029/030/032/040/044). CREATE OR REPLACE is uploadable_chapters'
-- convention (038) and cannot change a view's column list in Postgres.
--
-- Idempotent: DROP VIEW IF EXISTS + CREATE VIEW are safe to re-run.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.
```

**H2 — replace 044 line 106 (the single line `ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: ...`) with these seven lines.** Lines 107-119 (relevance, session_date, the FIFO comment block, `materialized_at ASC`, the backstop comment block, `turn_id ASC;`) are **not touched**:

```sql
-- FRESHNESS BUCKET (issue #513). Congressional content is news-shaped and its value
-- decays: ranking purely on the editorial keys below parked a 2026-09-04 turn behind
-- three 2026-06-10 turns. TRUE sorts before FALSE under DESC, so any turn from a
-- session in the last 14 days outranks every older turn. The 14-day cliff is
-- deliberate, and within each bucket the 044 keys below are byte-for-byte unchanged.
ORDER BY (dedup.session_date >= CURRENT_DATE - INTERVAL '14 days') DESC,  -- freshness bucket (issue #513)
         COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
```

The inline `-- freshness bucket (issue #513)` tag **must** sit on the `ORDER BY` line itself: the
intent-comment test slices the block from `ORDER BY (dedup.session_date >=` to the end, so a tag
placed only in the comment block above would fall outside the slice.

**H3 — replace 044 lines 121-127 and the final DOWN `ORDER BY` (lines 166-167).** The commented
body lines 128-165 stay byte-identical; only the header prose and the last two lines change:

```sql
-- DOWN
-- Manual psql only -- the runner has no automatic rollback, and it executes the
-- WHOLE file text in one transaction, so this block MUST stay commented out.
-- Restores the migration 044 view body (five-key ORDER BY, no freshness bucket).
-- Order-only rollback: no data is lost and no row's eligibility changes -- the queue
-- simply returns to stale-first editorial ranking.
--
```

and the DOWN block's trailing `ORDER BY` becomes:

```sql
-- ORDER BY COALESCE(dedup.interest_score, 1) DESC,
--          dedup.relevance_score DESC, dedup.session_date DESC,
--          dedup.materialized_at ASC, dedup.turn_id ASC;
```

**Repo-wide migration lint (`tests/utils/test_migrations_dag.py::TestMigrationIdempotency`, parametrized over every `*.sql`):** the file text — comments included — must contain no `INSERT INTO`, no bare `CREATE TABLE`, no bare `CREATE INDEX`, no bare `DROP TABLE`. The text above satisfies all four.

### `production_schema.sql` — exact edits

1. **Line 537**: `-- View: uploadable_turns (migration 044)` → `-- View: uploadable_turns (migration 049)`.
   The `-- View: uploadable_turns` prefix is `_view_block()`'s index marker and must survive.
2. **After line 552**, append to the cumulative lineage list:

```sql
--   049 freshness bucket prepended to the outer ORDER BY
--       ((session_date >= CURRENT_DATE - INTERVAL '14 days') DESC) — congressional
--       content decays, so any turn from a session in the last 14 days outranks every
--       older turn; within-bucket order is unchanged from 044 (issue #513)
```

3. **Line 618**: apply hunk **H2 verbatim** (same text — the `dedup` alias is not schema-qualified,
   so the migration and snapshot lines are character-identical). Lines 619-631 unchanged.
4. **Line 633** `COMMENT ON VIEW`: keep the literal prefix
   `COMMENT ON VIEW production.uploadable_turns IS '` (the `_view_block()` terminator) and extend
   the existing string with an order clause:

```sql
COMMENT ON VIEW production.uploadable_turns IS 'Speaker turn videos eligible for YouTube upload — prepared_at IS NOT NULL (issue #146), NOT is_upload_abandoned (issue #141), NOT is_procedural (issue #143), and published clip duration (span minus excised procedural seconds) >= 300s (issue #234/#143). Publish order: freshness bucket (session_date within 14 days, issue #513), then interest_score, relevance_score, session_date, materialized_at FIFO, turn_id backstop (issue #328)';
```

No test asserts this string; `_normalize_view_sql` splits on `;` and never selects this segment.
The added text must not contain the token `CREATE VIEW`.

### Normalized form the assertions target

`_normalize_view_sql` strips `--` comments, strips `production.`, collapses whitespace and
uppercases — including inside string literals, so `'14 days'` becomes `'14 DAYS'`:

```
ORDER BY (DEDUP.SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC, COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, DEDUP.RELEVANCE_SCORE DESC, DEDUP.SESSION_DATE DESC, DEDUP.MATERIALIZED_AT ASC, DEDUP.TURN_ID ASC
```

## Testing Strategy

All in `tests/congress_videos/sql/test_production_schema.py`. Static SQL text only.

### Fixture changes

| Symbol | Change |
|---|---|
| `MIGRATION_PATH` (line 18) | Repoint `044_deterministic_turn_publish_order.sql` → `049_freshness_bucket_turn_publish_order.sql` |
| `MIGRATION_044_PATH` | **New** constant pointing at the 044 file, same shape as `MIGRATION_040_PATH` |
| Module docstring (line 5) | "currently 044" → "currently 049" |

### Assertions repointed to 049

- `TestSnapshotLockstepWithLatestMigration.test_normalized_view_matches_migration_044` → rename to
  `test_normalized_view_matches_migration_049`; docstring and failure message say 049. It reads
  `MIGRATION_PATH`, so no other edit is needed.

### Assertions that must stay pointed at 044

- `TestUploadableTurns044PublishOrder.test_migration_044_body_is_040_plus_tiebreak` — its
  `MIGRATION_PATH` read (line 631) becomes `MIGRATION_044_PATH`. The 040-vs-044 splice literal and
  the assertion are otherwise **unchanged**: this is the #328 regression guard and must keep
  comparing 040 against 044, not 049.
- `test_outer_order_by_is_editorial_keys_then_fifo_tiebreak` — keeps its `endswith` shape and keeps
  reading the snapshot. Exactly one literal changes (per D4): delete the
  `"ORDER BY COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, "` line's `"ORDER BY "` prefix so
  `_TIEBREAK_SUFFIX` becomes:

```python
    _TIEBREAK_SUFFIX = (
        "COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, "
        "DEDUP.RELEVANCE_SCORE DESC, "
        "DEDUP.SESSION_DATE DESC, "
        "DEDUP.MATERIALIZED_AT ASC, "
        "DEDUP.TURN_ID ASC"
    )
```

- `test_tiebreak_carries_fifo_intent_comment` — only `marker` changes, to
  `"ORDER BY (dedup.session_date >="`. The `FIFO` and `#328` assertions stay verbatim.

### New class

```python
class TestUploadableTurns049FreshnessBucket:
    """049: a freshness bucket is prepended to the outer ORDER BY so a turn from a
    session in the last 14 days outranks every older turn (issue #513)."""

    _LEADING_KEY = "(DEDUP.SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC"
    _OUTER_ORDER_BY_044 = "ORDER BY COALESCE(DEDUP.INTEREST_SCORE, 1) DESC"
    _FULL_ORDER_BY = (
        "ORDER BY (DEDUP.SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC, "
        "COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, "
        "DEDUP.RELEVANCE_SCORE DESC, "
        "DEDUP.SESSION_DATE DESC, "
        "DEDUP.MATERIALIZED_AT ASC, "
        "DEDUP.TURN_ID ASC"
    )

    def test_outer_order_by_leads_with_freshness_bucket(self):
        """The freshness key is FIRST and the five 044 keys follow in their exact
        text and direction. The whole ORDER BY is the last thing in the normalized
        view, so one suffix check covers position, text and direction at once."""
        normalized = _normalize_view_sql(SCHEMA_PATH.read_text(encoding="utf-8"))
        assert normalized.endswith(self._FULL_ORDER_BY), (
            "outer ORDER BY must lead with the freshness bucket, followed by the "
            "five unchanged 044 keys"
        )

    def test_freshness_key_carries_issue_513_intent_comment(self):
        """Scoped to the ORDER BY clause itself, not the whole block: the lineage
        header already names 049, so a block-wide scan would stay green with the
        inline comment deleted — exactly the edit this test exists to catch."""
        block = TestProductionQualification._view_block()
        marker = "ORDER BY (dedup.session_date >="
        assert marker in block, "outer ORDER BY not found in the uploadable_turns block"
        order_by_clause = block[block.index(marker) :].upper()

        assert "FRESHNESS" in order_by_clause, (
            "the leading key must carry an inline comment naming its freshness intent, "
            "inside the ORDER BY clause itself — not only in the lineage header"
        )
        assert "#513" in order_by_clause, "the inline freshness comment must cite issue #513"

    def test_migration_049_body_is_044_plus_leading_freshness_key(self):
        """The transcription guard: migration 049's body must be migration 044's body
        with ONLY the freshness key prepended to the outer ORDER BY. Comment-,
        whitespace- and qualification-immune — this is what makes eligibility
        preservation a mechanically enforced fact, not a reviewer's hope."""
        migration_049 = _normalize_view_sql(MIGRATION_PATH.read_text(encoding="utf-8"))
        migration_044 = _normalize_view_sql(MIGRATION_044_PATH.read_text(encoding="utf-8"))

        assert migration_044.count(self._OUTER_ORDER_BY_044) == 1, (
            "the outer ORDER BY anchor must be unique in migration 044 for this splice "
            "to be exact — the inner ORDER BY is ORDER BY STV.OUTPUT_PATH, STV.TURN_ID"
        )
        expected = migration_044.replace(
            self._OUTER_ORDER_BY_044,
            f"ORDER BY {self._LEADING_KEY}, COALESCE(DEDUP.INTEREST_SCORE, 1) DESC",
            1,
        )
        assert migration_049 == expected, (
            "migration 049's view body must equal migration 044's body with exactly "
            "the freshness-bucket key prepended to the outer ORDER BY — nothing else"
        )
```

The expected value is derived from a **different file** (044) plus a hand-written literal, so the
guard is not tautological.

### Strict TDD — RED-first order

| # | Step | Expected state |
|---|---|---|
| 1 | **RED** Repoint `MIGRATION_PATH` → 049; add `MIGRATION_044_PATH`; repoint `test_migration_044_body_is_040_plus_tiebreak` to it; rename the lockstep test to `..._049` | `test_normalized_view_matches_migration_049` fails: `FileNotFoundError` on the missing 049 file. **This is the first RED.** The 040-vs-044 guard stays green. |
| 2 | **RED** Add `TestUploadableTurns049FreshnessBucket.test_migration_049_body_is_044_plus_leading_freshness_key` | Fails: `FileNotFoundError` |
| 3 | **GREEN** Create `049_freshness_bucket_turn_publish_order.sql` (copy 044 + H1/H2/H3) | Step-2 guard passes. Lockstep still RED — the snapshot is still 044's order. |
| 4 | **RED** Add `test_outer_order_by_leads_with_freshness_bucket` and `test_freshness_key_carries_issue_513_intent_comment` | Both fail against the un-synced snapshot |
| 5 | **GREEN** Apply the four `production_schema.sql` edits | Steps 2/4 and lockstep green. `TestUploadableTurns044PublishOrder` now fails on both `_TIEBREAK_SUFFIX` and `marker` — **expected and predicted by D4**, not a surprise. |
| 6 | **GREEN** Apply the two D4 literal edits (drop `"ORDER BY "` from `_TIEBREAK_SUFFIX`; change `marker`) | `uv run pytest` fully green |

Verification command each step: `uv run pytest tests/congress_videos/sql/test_production_schema.py`;
full `uv run pytest` plus `uv run pytest tests/utils/test_migrations_dag.py` before the PR.

| Layer | What | Approach |
|---|---|---|
| Unit (static SQL text) | Leading key, inline #513 comment, 044→049 transcription, snapshot/migration lockstep, 040→044 regression | Exact-string assertions on `_normalize_view_sql` output |
| Repo lint | 049 obeys the migration-file idempotency rules | `tests/utils/test_migrations_dag.py` picks the new file up automatically via `glob("*.sql")` |
| Integration / E2E | None added | No live-Postgres ordering test exists for this view today (044 set that precedent); the Docker e2e smoke test does not touch `congress_videos/sql/**` behaviour |

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary. The change is declarative SQL plus test text.

## Migration / Rollout

Migration 049 is applied by the existing `migrations_dag` (dev then prod). Idempotent
(`DROP VIEW IF EXISTS` + `CREATE VIEW`). No data migration, no backfill, no feature flag.
Rollback is manual `psql` from the commented `-- DOWN` block and is order-only.

## Review Workload Forecast

| Artifact | Changed lines (est.) |
|---|---|
| `049_freshness_bucket_turn_publish_order.sql` | ~175 (new; ~50 header, ~80 UP, ~45 commented DOWN) |
| `production_schema.sql` | ~17 (+14 / -3) |
| `test_production_schema.py` | ~70 (+65 / -5) |
| **Total** | **~262** |

`400-line budget risk: Low` — single PR, no slicing needed.

## Open Questions

- None.
