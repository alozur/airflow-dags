# Exploration: uploadable-turns-freshness-bucket (issue #513)

Worktree: `airflow-dags-wt-513`, branch `feat/513-uploadable-turns-freshness-bucket`, based on `origin/dev == origin/main @ 8868432`.

## Current State

`uploadable_turns` is created by migration 044 (`congress_videos/sql/migrations/044_deterministic_turn_publish_order.sql`) and mirrored verbatim (modulo comments/qualification/whitespace) in `congress_videos/sql/production_schema.sql:537-633`. Exact current outer `ORDER BY` (migration 044, lines 106-119):

```sql
ORDER BY COALESCE(dedup.interest_score, 1) DESC,  -- PRIMARY: interest score (NULL → INTEREST_NEUTRAL=1)
         dedup.relevance_score DESC,
         dedup.session_date DESC,
         -- FIFO tie-break (issue #328) ...
         dedup.materialized_at ASC,
         -- Total-order backstop ...
         dedup.turn_id ASC;
```

The full view body (the `group_spans` CTE, the `DISTINCT ON` dedup, all `WHERE` gates, the 300s floor) is unchanged since migration 040 except this tie-break; migration 044's own header states "ELIGIBILITY-NEUTRAL... No row enters or leaves the view; only the sequence changes." The same discipline applies to issue #513: the new migration must be the whole 044 body with only the new leading `ORDER BY` key added, nothing else.

`get_uploadable_turns()` (`congress_videos/modules/database.py:1077-1095`) confirmed: `SELECT * FROM {uploadable_turns_view} LIMIT %s` — no external `ORDER BY`. The issue's "no application code changes" claim holds.

## Migration numbering (prior memory was stale)

The highest migration is **048** (`048_manual_uploads_do_not_consume_scheduled_quota.sql`, issue #500), not 047 as prior memory said (047 exists too: `047_add_video_shorts_turn_id.sql`, issue #467, adds `video_shorts.turn_id`, does not touch the view). **The new migration must be 049.** Sequence 004-048 has no gaps.

## Migration file conventions (verified against 044 and 048)

Header: `-- Migration N: <summary> (issue #NNN)`, `-- Created: <date>`, `-- Depends on: <file>`, rationale comments, then `-- UP`, then the SQL, then `-- DOWN` with the ENTIRE rollback body commented out (`--` prefix on every line). Both 044 and 048 state explicitly why: "the runner has no automatic rollback, and it executes the WHOLE file text in one transaction, so this block MUST stay commented out" (044) / "DOWN (manual only; migration runner executes the whole file transactionally)" (048). An uncommented DOWN silently reverts the migration on the same run — documented in Engram memory `migration-down-block-silent-revert`.

View migrations use `DROP VIEW IF EXISTS` + `CREATE VIEW` (not `CREATE OR REPLACE`, which cannot change a view's column list and is `uploadable_chapters`'s convention only). The runner sets `search_path TO {schema}, public`, so view/table names inside the file stay UNQUALIFIED.

## production_schema.sql snapshot + drift lockstep

`congress_videos/sql/production_schema.sql:537-633` carries a cumulative-lineage header comment listing every migration that touched the view (028...044) — the new migration entry (049) must be appended to that list, and the view body + `COMMENT ON VIEW` replaced with the migration 049 body (schema-qualified with `production.`).

`tests/congress_videos/sql/test_production_schema.py`:

- `SCHEMA_PATH` = production_schema.sql, `MIGRATION_PATH` = currently `044_deterministic_turn_publish_order.sql` (must be repointed to 049), `MIGRATION_040_PATH` = the 040 file (kept as a further-back anchor).
- `_normalize_view_sql(text, view_name="UPLOADABLE_TURNS")` (lines 43-58): strips `--` comments (which also kills the whole commented-out DOWN block), strips `production.` qualification, collapses whitespace, uppercases, rewrites `CREATE OR REPLACE VIEW` → `CREATE VIEW`, and returns the first `;`-delimited segment containing `CREATE VIEW {TARGET}`.
- `TestSnapshotLockstepWithLatestMigration.test_normalized_view_matches_migration_044` (lines 568-580): asserts `_normalize_view_sql(snapshot) == _normalize_view_sql(migration_044_file)`. Must become `..._matches_migration_049` reading the new `MIGRATION_PATH`.
- `TestUploadableTurns044PublishOrder` (lines 583-637) is the exact template for a new `TestUploadableTurns049FreshnessBucket` class:
  - `_TIEBREAK_SUFFIX` exact string (lines 587-593): `"ORDER BY COALESCE(DEDUP.INTEREST_SCORE, 1) DESC, DEDUP.RELEVANCE_SCORE DESC, DEDUP.SESSION_DATE DESC, DEDUP.MATERIALIZED_AT ASC, DEDUP.TURN_ID ASC"`, and `test_outer_order_by_is_editorial_keys_then_fifo_tiebreak` asserts `normalized.endswith(self._TIEBREAK_SUFFIX)`. Since #513 adds a NEW LEADING key, this `endswith` assertion still holds unmodified (the suffix is untouched), but a new companion assertion is needed for the new leading `(SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC` prefix, most naturally checked as the exact string `"ORDER BY (DEDUP.SESSION_DATE >= CURRENT_DATE - INTERVAL '14 DAYS') DESC, COALESCE(DEDUP.INTEREST_SCORE, 1) DESC"`.
  - `test_tiebreak_carries_fifo_intent_comment` (606-624) and `test_migration_044_body_is_040_plus_tiebreak` (626-637) are the transcription-guard pattern: for 049 the analogous test should assert `migration_049_normalized == migration_044_normalized` with the new leading key spliced in right after `ORDER BY ` (mirroring how 044's guard spliced its tie-break onto 040's normalized text), keeping the same comment/whitespace/qualification-immune diff discipline.
  - `TestProductionQualification._view_block()` (lines 383-389) extracts the view text between `-- View: uploadable_turns` and `COMMENT ON VIEW production.uploadable_turns` — reused by both qualification tests and the FIFO-comment test. The new freshness-bucket `ORDER BY` key should also carry an inline `-- issue #513` comment naming its intent, following the pattern the FIFO key uses for #328, so an analogous "comment survives inside the ORDER BY clause, not just the lineage header" test can be added.
- No live-Postgres test exists specifically for `uploadable_turns` ordering. Live-DB tests elsewhere (`tests/congress_videos/test_speaker_turns_chapter_order_live.py`, `tests/congress_videos/sql/test_migration_029.py`) are gated by `os.getenv("TEST_DATABASE_URL", ...)` and skip cleanly when `psycopg2`/a live DB is unavailable — the same pattern a new live-DB test would follow IF one were added (not required by the issue scope, which is snapshot/static-text only, matching `TestUploadableTurns044PublishOrder`'s own style).

## CURRENT_DATE-in-view risk

`uploadable_turns` is a plain (non-materialized) VIEW — Postgres re-plans and re-executes it on every query, so `CURRENT_DATE` (STABLE, not IMMUTABLE) is safe: no index/materialization staleness risk. Precedent for `CURRENT_DATE` inside view definitions already exists in this repo: `uploadable_chapters`/`priority_queue`-style views compute `CURRENT_DATE - DATE(vc.created_at) AS days_since_created` and `CURRENT_DATE - cs.session_date AS days_old` (`production_schema.sql:472`; migrations `007`, `010`, `011`, `021`, `036`, `038`). Those are SELECT-list expressions, not `ORDER BY` predicates, but the underlying STABLE-function-in-a-plain-view pattern is identical and already shipped to production without issue.

Timezone note: `CURRENT_DATE` resolves in the session's `TimeZone` GUC, not UTC explicitly — `session_date` is a plain `DATE` column (no offset), so the comparison is same-type `DATE` vs `DATE` and is timezone-boundary-safe as long as the Postgres session timezone is stable (it is; no per-connection `SET TIME ZONE` override was found in `database.py`). No functional/expression index exists on `uploadable_turns` (it is a view, not a materialized view or table), so there is no index-immutability requirement to violate.

## Affected files

- `congress_videos/sql/migrations/049_*.sql` (new) — `DROP VIEW` + `CREATE VIEW`, the full 044 body plus the new leading `(session_date >= CURRENT_DATE - INTERVAL '14 days') DESC` key with an inline `-- issue #513` comment; DOWN block commented out, restoring the 044 `ORDER BY`.
- `congress_videos/sql/production_schema.sql:537-633` — cumulative lineage header gets a `049` line; view body and `COMMENT ON VIEW` updated to match.
- `tests/congress_videos/sql/test_production_schema.py` — `MIGRATION_PATH` repointed to 049; `TestSnapshotLockstepWithLatestMigration` docstring/test renamed to reference 049; new `TestUploadableTurns049FreshnessBucket` class (sibling of `TestUploadableTurns044PublishOrder`) with leading-key assertion, inline-comment assertion, and a 044-vs-049 transcription guard.
- No changes needed: `congress_videos/modules/database.py` (`get_uploadable_turns` has no external `ORDER BY`), no DAG files, no other views (`uploadable_chapters` is untouched/unaffected per issue scope).

## Approaches

1. **Exactly as issue #513 specifies — new leading boolean-bucket key `(session_date >= CURRENT_DATE - INTERVAL '14 days') DESC`** (RECOMMENDED). Pros: matches the issue's own decision, one-line diff to the `ORDER BY`, preserves every existing key and direction, cheap to test with the same string-assertion style as 044. Cons: a hard 14-day cliff — a turn 15 days old sits fully behind a turn 13 days old regardless of quality; and the day the migration ships some backlog rows jump the bucket at once (bounded, self-resolving in one drain cycle). Effort: Low.
2. **Continuous recency decay blended into a single score** (e.g. `interest_score - days_old * k`) instead of a hard bucket. Pros: no cliff effect. Cons: changes editorial semantics beyond what the issue asked for, much harder to keep ELIGIBILITY-NEUTRAL / deterministic-order style tests, not requested. Effort: Medium-High. Not recommended — out of issue scope.
3. **Materialized view / cron-refreshed freshness flag column** instead of computing `CURRENT_DATE` live. Pros: none identified — the view already reads live database state on every query (no caching layer exists). Cons: adds a refresh job, a staleness window, and unnecessary complexity for a same-query boolean comparison. Effort: Medium. Not recommended — no evidence of a performance problem; `uploadable_turns` is queried with `LIMIT 1` once per day.

## Recommendation

Approach 1, exactly as specified in the issue. The migration number is 049 (not implied by the issue text; confirmed by reading the migrations directory). Test additions should mirror `TestUploadableTurns044PublishOrder`'s three-assertion shape (leading-key position/text, inline issue comment, and a byte-level 044-vs-049 transcription guard) rather than looser regex matching, since that is this repo's established anti-drift discipline for this specific view.

## Risks

- `_TIEBREAK_SUFFIX`-style exact-string tests are strict: any whitespace/formatting deviation in the new migration's `ORDER BY` will fail CI immediately — this is intentional but must be respected precisely when authoring the SQL text.
- The lockstep test (`TestSnapshotLockstepWithLatestMigration`) fails loudly if `production_schema.sql` is not updated in the same PR as the migration — both files must ship together (the same discipline as every prior view migration in this repo).
- Bucket-boundary jump on rollout: rows at the 14-day edge move in ranking the moment migration 049 is applied. This is expected and desired per the issue, and is not a data-eligibility change (no row enters or leaves the view).
- No live-Postgres test currently exercises `uploadable_turns` ordering end to end; the static snapshot/migration text tests are the only automated coverage for this view's `ORDER BY` (consistent with migration 044's own test strategy — not a new gap introduced by this change).

## Ready for Proposal

Yes. The scope is narrow (one new migration, one snapshot sync, one test-file update), there are no application code changes and no open product decisions — the issue's own "Decision" section is prescriptive enough to proceed straight to `sdd-propose`.
