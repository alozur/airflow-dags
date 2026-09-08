# Turn Publish Order Specification

## Purpose

Defines the deterministic ranking that `uploadable_turns` (a Postgres view) applies to eligible turns so that `get_uploadable_turns()` can select the next turn(s) to upload with `SELECT * FROM uploadable_turns LIMIT %s` and no external `ORDER BY`. The ranking is a strict total order: a freshness bucket, three editorial keys, an FIFO tie-break, and a total-order backstop.

## Requirements

### Requirement: Freshness bucket leads the publish order

The `uploadable_turns` view MUST rank every turn from a session dated within the last 14 days (`session_date >= CURRENT_DATE - INTERVAL '14 days'`) ahead of every turn from an older session, regardless of `interest_score` or `relevance_score`.

#### Scenario: A fresh low-score turn outranks stale high-score turns

- GIVEN a turn from session `2026-09-04` with `relevance_score = 5`, `interest_score = 7`
- AND three turns from session `2026-06-10` with `relevance_score = 2`, `interest_score` between 8 and 9
- WHEN `uploadable_turns` is queried
- THEN the `2026-09-04` turn ranks ahead of all three `2026-06-10` turns

#### Scenario: A turn exactly 14 days old is still in the fresh bucket

- GIVEN a turn whose `session_date` equals `CURRENT_DATE - INTERVAL '14 days'`
- WHEN `uploadable_turns` is queried
- THEN that turn is placed in the fresh bucket, ahead of every turn with an older `session_date`

#### Scenario: A turn 15 days old ranks below the fresh bucket regardless of quality

- GIVEN a turn whose `session_date` is `CURRENT_DATE - INTERVAL '15 days'` with the highest `interest_score` and `relevance_score` in the dataset
- AND a turn whose `session_date` is within the last 14 days with the lowest `interest_score` and `relevance_score` in the dataset
- WHEN `uploadable_turns` is queried
- THEN the 15-day-old turn ranks below the fresh turn

This 14-day cliff is an accepted tradeoff: a turn one day past the boundary ranks below the entire fresh bucket even if its editorial scores are higher. It is not mitigated by this requirement — see the Modified Requirements section for the unchanged within-bucket editorial order.

### Requirement: Editorial order applies independently within each bucket

Within the fresh bucket and independently within the stale bucket, `uploadable_turns` MUST order turns by `COALESCE(interest_score, 1) DESC`, then `relevance_score DESC`, then `session_date DESC` — unchanged from the pre-#513 order.

#### Scenario: Editorial order holds within the fresh bucket

- GIVEN two turns both with `session_date` inside the fresh 14-day window
- AND turn A has a higher `interest_score` than turn B
- WHEN `uploadable_turns` is queried
- THEN turn A ranks ahead of turn B

#### Scenario: Editorial order holds within the stale bucket

- GIVEN two turns both with `session_date` older than 14 days
- AND turn A has a higher `interest_score` than turn B
- WHEN `uploadable_turns` is queried
- THEN turn A ranks ahead of turn B, and both still rank behind every fresh-bucket turn

#### Scenario: NULL interest_score is treated as neutral (1)

- GIVEN a turn with `interest_score = NULL` and a turn with `interest_score = 1` in the same freshness bucket
- WHEN `uploadable_turns` is queried and all other editorial keys are equal
- THEN the two turns rank identically (both use the neutral value 1 for ordering)

### Requirement: FIFO tie-break and total-order backstop are unchanged

After the freshness bucket and the three editorial keys, `uploadable_turns` MUST break remaining ties by `materialized_at ASC` (FIFO — issue #328), then by `turn_id ASC` (total-order backstop). Neither key changes position, direction, or column relative to migration 044.

#### Scenario: Two turns tie on freshness bucket and all editorial keys

- GIVEN two turns in the same freshness bucket with identical `interest_score`, `relevance_score`, and `session_date`
- AND turn A was materialized before turn B
- WHEN `uploadable_turns` is queried
- THEN turn A ranks ahead of turn B

#### Scenario: Full tie falls back to turn_id

- GIVEN two turns identical on freshness bucket, all editorial keys, and `materialized_at`
- WHEN `uploadable_turns` is queried
- THEN the turn with the lower `turn_id` ranks first

### Requirement: Eligibility is unaffected by the freshness bucket

Adding the freshness bucket key MUST NOT change which rows `uploadable_turns` returns. The set of eligible turns (dedup, `WHERE` gates, the 300s published-duration floor) MUST remain identical to migration 044's output — only the row order changes.

#### Scenario: Row set is identical before and after the freshness bucket ships

- GIVEN a fixed set of turns eligible under migration 044's `uploadable_turns`
- WHEN the same dataset is queried against the view carrying the freshness-bucket key
- THEN the returned row set is identical; only the relative order differs

#### Scenario: A turn ineligible under migration 044 stays ineligible

- GIVEN a turn excluded by an existing `WHERE` gate (e.g. below the 300s published-duration floor) or excluded by the `DISTINCT ON (output_path)` dedup
- WHEN the view carrying the freshness-bucket key is queried
- THEN that turn is still absent from the result set
