# Delta for generic-thumbnail-generation

## ADDED Requirements

---

### Requirement: Score-Gated Single Generation — Fast Path

When the first thumbnail's Pikzels score meets or exceeds the configured
threshold, the DAG MUST stop at 1 Pikzels generation call and persist exactly
1 row in `video_thumbnails`. No retry tasks MAY execute.

#### Scenario: Score meets threshold — 1 call, 1 row, no retry

- GIVEN `score_retry_threshold` is 60 (default)
- AND the first thumbnail (`option_a`) receives a Pikzels score of 60 or above
- WHEN `check_score_threshold` evaluates the score
- THEN exactly 1 `pikzels_client.thumbnail_from_text` call is made for the run
- AND exactly 1 row is written to `video_thumbnails` for the run's `chapter_id`
- AND the retry-path tasks (`art_direction_retry`, `generate_thumbnail_option_b`,
  `download_option_b`, `score_option_b`) are all skipped (marked skipped, not failed)

#### Scenario: Score strictly above threshold — fast path reached

- GIVEN `score_retry_threshold` is 60
- AND `option_a` scores 75
- WHEN `check_score_threshold` evaluates the score
- THEN `choose_best_option` and downstream tasks run via the fast path
- AND the persisted row has `is_chosen = TRUE`

---

### Requirement: Score-Gated Retry — Conditional Second Generation

When the first thumbnail's score is strictly below the configured threshold,
the DAG MUST generate a second thumbnail using a DIFFERENT art-direction brief,
score it, and persist both options. The row with the higher score MUST have
`is_chosen = TRUE`.

#### Scenario: Score below threshold — 2 calls, 2 rows, winner chosen

- GIVEN `score_retry_threshold` is 60
- AND `option_a` scores 45
- WHEN `check_score_threshold` evaluates the score
- THEN the retry path executes (`art_direction_retry` → `generate_thumbnail_option_b`
  → `download_option_b` → `score_option_b`)
- AND exactly 2 calls to `pikzels_client.thumbnail_from_text` are made for the run
- AND exactly 2 rows are written to `video_thumbnails` for the run's `chapter_id`
- AND the row with the higher `pikzels_score` has `is_chosen = TRUE`

#### Scenario: Retry option scores lower — first option still wins

- GIVEN `option_a` scores 40 and `option_b` scores 30
- WHEN `choose_best_option` evaluates both options
- THEN `option_a` is marked `is_chosen = TRUE`
- AND `option_b` is persisted with `is_chosen = FALSE`

---

### Requirement: Retry Art-Direction Brief Is Verifiably Different

`art_direction_retry` MUST receive the original brief produced by `art_direction`
as `previous_brief`. The prompt sent to OpenAI MUST include an explicit instruction
to use a different visual approach from the previous brief. This MUST be
traceable: the XCom pulled by `art_direction_retry` MUST come from `task_ids="art_direction"`.

#### Scenario: Retry brief receives original brief and different-approach instruction

- GIVEN a retry is triggered (score below threshold)
- WHEN `art_direction_retry` runs
- THEN `previous_brief` passed to `art_direct` equals the dict returned by
  `task_ids="art_direction"` (not None, not empty)
- AND the prompt sent to OpenAI contains an instruction to change the visual approach

#### Scenario: Retry does not use its own XCom output as its own input

- GIVEN `art_direction_retry` executes
- WHEN the XCom pull is inspected
- THEN `task_ids` resolves to `"art_direction"` (the primary task), not `"art_direction_retry"`

---

### Requirement: `score_retry_threshold` Is Domain-Configurable, Default 60

`thumbnail_config.py` MUST expose `score_retry_threshold` as a per-domain
configurable integer. When not explicitly set for a domain, it MUST default to
60. The DAG MUST read this value from domain config at run time, not hardcode it.

#### Scenario: Default threshold is 60 when not overridden

- GIVEN a domain config entry that does NOT set `score_retry_threshold`
- WHEN `get_domain_config` is called for that domain
- THEN the returned config dict contains `score_retry_threshold = 60`

#### Scenario: Domain override is respected

- GIVEN a domain config entry sets `score_retry_threshold = 75`
- WHEN `check_score_threshold` reads the config
- THEN a score of 70 triggers the retry path
- AND a score of 75 takes the fast path

---

### Requirement: Fast Path Reaches `persist_results` Without Failure

All tasks that join the fast and retry paths (`choose_best_option`, `generate_title`,
`persist_results`) MUST use `trigger_rule="none_failed_min_one_success"`. The fast
path (retry tasks skipped) MUST reach `persist_results` successfully.

#### Scenario: Fast path reaches persist_results — skipped tasks do not block

- GIVEN `option_a` scores above threshold (retry tasks are skipped)
- WHEN `choose_best_option` is evaluated for scheduling
- THEN `choose_best_option` runs (trigger_rule allows skipped upstreams)
- AND `persist_results` executes and writes the row

#### Scenario: Retry path reaches persist_results — all tasks succeed

- GIVEN `option_a` scores below threshold and `option_b` is generated successfully
- WHEN the retry path completes
- THEN `choose_best_option`, `generate_title`, and `persist_results` all run
- AND exactly 2 rows exist in `video_thumbnails` after persist

---

### Requirement: Empty or Missing Retry Option Is Filtered Before Comparison

When `option_b` is absent (fast path) or its XCom resolves to an empty dict,
`choose_best_option` and `persist_results` MUST operate on the filtered list
`[x for x in [option_a, option_b] if x]`. An absent `option_b` MUST NOT be
treated as a zero-score competitor.

#### Scenario: Absent option_b is not scored as 0

- GIVEN the fast path ran so `option_b` XCom is empty or None
- WHEN `choose_best_option` builds the candidate list
- THEN the candidate list contains exactly 1 item (`option_a`)
- AND no comparison against a zero-score placeholder occurs
- AND `option_a` is chosen with `is_chosen = TRUE`

#### Scenario: persist_results writes only present options

- GIVEN the fast path ran and `option_b` is absent
- WHEN `persist_results` executes
- THEN exactly 1 row is written (not 2 rows with one being empty/zeroed)

---

## MODIFIED Requirements

---

### Requirement: 1-or-2 Thumbnail Options Generated Conditionally

(Previously: "Exactly 2 Thumbnail Options Generated and Downloaded Immediately" — changed to support 1-option fast path.)

For each run the DAG MUST always generate `option_a` by calling
`pikzels_client.thumbnail_from_text` and downloading the result immediately. A
second option (`option_b`) MUST only be generated when the `option_a` score is
strictly below `score_retry_threshold`. Pikzels-issued URLs MUST NOT be relied
upon after the generation step.

Local storage path pattern:
`/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png`

where `{label}` is `"option_a"` or `"option_b"`.

#### Scenario: Fast path — only option_a generated

- GIVEN `option_a` scores at or above threshold
- WHEN the generation tasks execute
- THEN exactly 1 call to `pikzels_client.thumbnail_from_text` is made

#### Scenario: Retry path — option_a and option_b both generated

- GIVEN `option_a` scores below threshold
- WHEN the retry tasks execute
- THEN exactly 2 calls to `pikzels_client.thumbnail_from_text` are made
- AND both options are downloaded to disk

#### Scenario: Local path is created if it does not exist

- GIVEN the directory
  `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/` does not
  exist at run time
- WHEN the download is attempted
- THEN the directory is created before writing the file
- AND the file is written successfully

#### Scenario: Pikzels generation failure surfaces as task failure

- GIVEN `pikzels_client.thumbnail_from_text` raises a non-retryable error for
  option_a
- WHEN the generation task executes
- THEN that task is marked failed
- AND the Airflow retry policy governs re-attempt behaviour

---

### Requirement: Task Graph Shape

(Previously: fixed two-branch parallel graph — changed to sequential single-start plus conditional retry branch.)

The DAG MUST implement the following conditional task graph:

```
validate_input
  → resolve_participant_photo
    → art_direction
      → generate_thumbnail_option_a → download_option_a → score_option_a
        → check_score_threshold (BranchPythonOperator)
            [fast path]  → choose_best_option
            [retry path] → art_direction_retry
                            → generate_thumbnail_option_b
                              → download_option_b
                                → score_option_b
                                  → choose_best_option
        → choose_best_option (trigger_rule=none_failed_min_one_success)
          → generate_title
            → persist_results (trigger_rule=none_failed_min_one_success)
              → thumbnail_result
```

`generate_thumbnail_option_a` MUST run after `art_direction` (sequential, not parallel).
`check_score_threshold` MUST be a `BranchPythonOperator` that returns either
the fast-path task ID or the retry-path task ID.

#### Scenario: Task graph contains check_score_threshold as branch operator

- GIVEN the DAG `generic_thumbnail_generator` is loaded
- WHEN task types are inspected
- THEN `check_score_threshold` is a `BranchPythonOperator`

#### Scenario: Fast-path graph — retry tasks skipped

- GIVEN score >= threshold
- WHEN task dependencies are evaluated
- THEN `art_direction_retry`, `generate_thumbnail_option_b`, `download_option_b`,
  `score_option_b` are all in skipped state
- AND `choose_best_option` runs via `none_failed_min_one_success`

#### Scenario: Retry-path graph — all tasks run

- GIVEN score < threshold
- WHEN task dependencies are evaluated
- THEN all tasks in the retry path execute in order
- AND `choose_best_option` receives both scores before running

---

### Requirement: Results Persisted — 1 or 2 Rows Depending on Path

(Previously: "Results Persisted in `video_thumbnails` Table" — changed from always-2-rows to 1-or-2 based on path taken.)

Both options that were generated MUST be persisted as separate rows. When only
`option_a` is generated (fast path), exactly 1 row MUST be written. The table
schema is unchanged. `(chapter_id, label)` remains unique.

The `pikzels_report.json` file MUST NOT be created or updated by this DAG.

#### Scenario: Fast path — exactly 1 row persisted

- GIVEN a DAG run completes on the fast path (score >= threshold)
- WHEN `video_thumbnails` is queried for the run's `chapter_id`
- THEN exactly 1 row exists with `label = "option_a"` and `is_chosen = TRUE`
- AND no `option_b` row exists for that `chapter_id`

#### Scenario: Retry path — exactly 2 rows persisted, winner flagged

- GIVEN a DAG run completes on the retry path (score < threshold)
- WHEN `video_thumbnails` is queried for the run's `chapter_id`
- THEN exactly 2 rows exist with labels `"option_a"` and `"option_b"`
- AND exactly 1 row has `is_chosen = TRUE` (the higher-scored option)
- AND the chosen row has a non-null `openai_title`

#### Scenario: `local_path` stored for all generated options

- GIVEN a run completes (fast or retry)
- WHEN the rows in `video_thumbnails` for the run are read
- THEN every `local_path` value matches the pattern
  `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png`
- AND the files exist on the filesystem

#### Scenario: Re-run for same chapter_id replaces prior rows

- GIVEN `video_thumbnails` already contains rows for `chapter_id = 7`
- WHEN the DAG is re-triggered for the same `chapter_id`
- THEN prior rows are deleted+reinserted or updated via upsert
- AND after the run only the rows generated in this run remain for `chapter_id = 7`

#### Scenario: `pikzels_report.json` is not written

- GIVEN a successful DAG run completes
- WHEN the working directory and data directory are checked
- THEN no `pikzels_report.json` file is created or modified by this DAG run
