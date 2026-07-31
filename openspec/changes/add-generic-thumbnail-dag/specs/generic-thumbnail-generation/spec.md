# Delta for generic-thumbnail-generation

## ADDED Requirements

---

### Requirement: DAG `generic_thumbnail_generator` Is On-Demand, Config-Driven, and Free of Domain Hardcoding

A new DAG `generic_thumbnail_generator` MUST exist with `schedule=None`.
It MUST be invokable exclusively via `trigger_dag_api` (or the Airflow trigger
API). It MUST NOT contain any Congreso-specific hardcoded value (domain names,
endpoints, participant sources, visual styles, persona lists). All domain-specific
values MUST come from per-domain config files and/or DAG run `conf`.

#### Scenario: DAG loads cleanly with no import errors

- GIVEN the DAG file `congress_videos/generic_thumbnail_generator_dag.py` is
  imported by the Airflow scheduler
- WHEN `airflow dags list-import-errors` is evaluated
- THEN no import error is reported for `generic_thumbnail_generator`

#### Scenario: DAG has schedule None

- GIVEN the DAG `generic_thumbnail_generator` is loaded
- WHEN its `schedule` attribute is inspected
- THEN it equals `None`

#### Scenario: DAG contains no Congreso-specific string literals

- GIVEN the DAG source file is read
- WHEN it is searched for Congreso-specific strings (e.g. `"congreso"`,
  `"diputado"`, `"CONGRESO"`) outside of import paths
- THEN no such literals are found in task logic, task IDs, or default config
  keys

---

### Requirement: Input Contract — Per-Video Run Conf

Each DAG run MUST accept a `conf` dict containing exactly these keys for the
target video:

| Key | Type | Description |
|-----|------|-------------|
| `youtube_video_id` | `str` | YouTube video identifier |
| `chapter_id` | `int` | FK to `video_chapters.chapter_id` |
| `debate_summary` | `str` | Text summary of the debate, used for Pikzels prompt and OpenAI title prompt |
| `session` | `str` | Session label (e.g. `"Pleno 2026-06-10"`) |
| `domain` | `str` | Domain key used to look up per-domain config |
| `normalized_name` | `str` | Speaker/participant normalized name; used for photo resolution |

The DAG MUST fail the run with a clear error at `validate_input` if any of the
above keys is absent or empty.

#### Scenario: Valid conf triggers a run

- GIVEN a `conf` dict containing all six required keys with non-empty values
- WHEN the DAG run is triggered
- THEN `validate_input` completes successfully
- AND downstream tasks are scheduled

#### Scenario: Missing required conf key fails fast

- GIVEN a `conf` dict that omits `chapter_id`
- WHEN the DAG run is triggered
- THEN `validate_input` raises a `ValueError` (or equivalent Airflow failure)
- AND no downstream task executes

#### Scenario: Empty string value for a required key fails fast

- GIVEN a `conf` dict where `debate_summary` is an empty string `""`
- WHEN `validate_input` executes
- THEN the task raises a `ValueError`
- AND no downstream task executes

---

### Requirement: Per-Domain Config Controls Visual Styles, Personas, and Participants Source

A per-domain config (YAML or equivalent) MUST be loaded for the `domain` key
supplied in `conf`. It MUST define at minimum:

- `visual_styles`: list of visual style descriptors passed to Pikzels
- `personas`: list of speaker/persona descriptors
- `participants_source`: the lookup mechanism for photo resolution (e.g.
  `"congress_participants"`)
- `party_logo_path` (optional): absolute path to a fallback party-logo image;
  may be absent or `null`

The DAG MUST raise a clear `ConfigError` (or equivalent) at startup if the
requested `domain` key does not exist in any config file.

#### Scenario: Known domain config is loaded successfully

- GIVEN a per-domain config file defines domain `"congreso_es"`
- WHEN the DAG run uses `conf["domain"] = "congreso_es"`
- THEN the config is loaded and its `visual_styles` list is non-empty

#### Scenario: Unknown domain raises config error

- GIVEN no config file defines domain `"unknown_domain"`
- WHEN the DAG run uses `conf["domain"] = "unknown_domain"`
- THEN a `ConfigError` or `KeyError` is raised at config-load time
- AND no Pikzels or OpenAI calls are made

---

### Requirement: Participant Photo Resolution — DB Lookup then HTTP Download

`resolve_participant_photo` MUST query `congress_participants` for the row
whose `normalized_name` matches `conf["normalized_name"]`. If a row is found
and `photo_url` is non-null, it MUST perform an HTTP GET to download the actual
image bytes (not pass the URL string directly to Pikzels). The raw bytes are the
support image passed to `pikzels_client.thumbnail_from_text`.

The lookup MUST use the existing `participants_db` module; no raw SQL in the DAG
task body.

#### Scenario: Photo resolved and bytes downloaded

- GIVEN `congress_participants` contains a row with
  `normalized_name = "garcia_lopez_maria"` and a non-null `photo_url`
- WHEN `resolve_participant_photo` executes
- THEN an HTTP GET is issued to `photo_url`
- AND the returned bytes are pushed to XCom as `photo_bytes` (base64-encoded or
  equivalent serializable form)

#### Scenario: `photo_url` is NULL — fallback to party logo

- GIVEN `congress_participants` contains a row for the `normalized_name`
  but `photo_url` IS NULL
- AND the domain config specifies a non-null `party_logo_path`
- WHEN `resolve_participant_photo` executes
- THEN the file at `party_logo_path` is read as bytes
- AND those bytes are used as the support image (no HTTP call is made)
- AND the task completes successfully (no exception raised)

#### Scenario: `photo_url` is NULL and no party logo configured — fail fast

- GIVEN `congress_participants` contains a row for the `normalized_name`
  but `photo_url` IS NULL
- AND the domain config does NOT specify a `party_logo_path` (absent or null)
- WHEN `resolve_participant_photo` executes
- THEN the task raises a `ValueError` with a message indicating no photo source
  is available
- AND downstream tasks do not execute

#### Scenario: `normalized_name` not found in DB

- GIVEN `congress_participants` has no row matching `conf["normalized_name"]`
- WHEN `resolve_participant_photo` executes
- THEN the task raises a `LookupError` (or equivalent)
- AND the DAG run fails at this task with no Pikzels calls made

---

### Requirement: Exactly 2 Thumbnail Options Generated and Downloaded Immediately

For each run the DAG MUST generate exactly 2 thumbnail options by calling
`pikzels_client.thumbnail_from_text` twice, once per option, using distinct
visual style / persona parameters drawn from the domain config. Immediately after
each Pikzels call (before any other task), the thumbnail image MUST be downloaded
via `pikzels_client.download` and saved locally. Pikzels-issued URLs MUST NOT be
relied upon after the generation step.

Local storage path pattern:
`/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png`

where `{label}` identifies the option (e.g. `"option_a"`, `"option_b"`).

#### Scenario: Two options are generated and saved locally

- GIVEN a valid conf and a resolved photo support image
- WHEN the thumbnail generation tasks execute
- THEN exactly 2 calls to `pikzels_client.thumbnail_from_text` are made
- AND each result is immediately downloaded to the path
  `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png`
- AND both files exist on disk after the generation step

#### Scenario: Local path is created if it does not exist

- GIVEN the directory
  `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/` does not
  exist at run time
- WHEN the download is attempted
- THEN the directory is created before writing the file
- AND the file is written successfully

#### Scenario: Pikzels generation failure surfaces as task failure

- GIVEN `pikzels_client.thumbnail_from_text` raises a non-retryable error for
  one option
- WHEN the generation task executes
- THEN that task is marked failed
- AND the Airflow retry policy governs re-attempt behaviour

---

### Requirement: Pikzels Client — Trimmed Port with Retry/Backoff

A new module `congress_videos/modules/pikzels_client.py` MUST expose:
`thumbnail_from_text`, `score_thumbnail`, `download`, `to_base64_data_url`.

The following Pikzels endpoints MUST NOT be present in this module:
`thumbnail_from_image`, `edit_thumbnail`, `generate_titles`, any pikzonality
method.

All Pikzels HTTP calls MUST implement retry with exponential backoff on
retryable errors (5xx, network timeout). Non-retryable 4xx errors MUST raise
immediately without retry.

`PIKZELS_API_KEY` MUST be read from the environment; the module MUST raise
`EnvironmentError` at import time (or first call) if the variable is absent or
empty.

#### Scenario: Module exposes only the required surface

- GIVEN `pikzels_client` is imported
- WHEN its public symbols are inspected
- THEN `thumbnail_from_text`, `score_thumbnail`, `download`, and
  `to_base64_data_url` are present
- AND `thumbnail_from_image`, `edit_thumbnail`, `generate_titles` are NOT
  present

#### Scenario: Missing `PIKZELS_API_KEY` raises at startup

- GIVEN the environment variable `PIKZELS_API_KEY` is unset
- WHEN `pikzels_client` is imported or its first method is called
- THEN an `EnvironmentError` (or `RuntimeError`) is raised with a message
  naming the missing variable

#### Scenario: Retryable 503 is retried with backoff

- GIVEN `pikzels_client.thumbnail_from_text` is called
- AND the Pikzels API returns HTTP 503 on the first two attempts
- AND HTTP 200 on the third
- WHEN the call is made
- THEN the client retries at least twice before succeeding
- AND the delay between attempts is non-zero (exponential backoff)

#### Scenario: Non-retryable 400 raises immediately

- GIVEN `pikzels_client.thumbnail_from_text` is called with invalid parameters
- AND the Pikzels API returns HTTP 400
- WHEN the call is made
- THEN the client raises an exception immediately without retrying

---

### Requirement: Each Thumbnail Option Is Scored via `score_thumbnail`

After local download, each option MUST be scored by calling
`pikzels_client.score_thumbnail` using the locally-downloaded image (not the
expired Pikzels URL). The numeric score MUST be stored per option and passed
downstream for best-option selection.

#### Scenario: Both options receive a score

- GIVEN 2 thumbnail files are downloaded locally
- WHEN `score_thumbnail` is called for each
- THEN each option has a numeric score associated with it
- AND both scores are available in XCom for the `choose_best_option` task

---

### Requirement: Title Generated by OpenAI Only, Matching the Thumbnail

After the best option is chosen (`choose_best_option`), `generate_title` MUST
produce exactly one title for the chosen thumbnail via `utils/ai_helpers.py`
(the shared wrapper — no direct OpenAI SDK calls in the DAG or Pikzels client).
The prompt MUST include:

- The `debate_summary` from conf
- The chosen thumbnail's visual context (style, persona descriptor)
- Explicit instructions to produce a title in dramatic Spanish political tone
- Explicit instructions to output no emoji, no stray channel symbols, and no
  hashtags

The returned title MUST conform to all of the following constraints:

| Constraint | Value |
|------------|-------|
| Maximum length | 100 characters |
| Language | Spanish |
| Tone | Dramatic political (e.g. headlines, not neutral descriptions) |
| Forbidden characters | Emojis (any Unicode emoji), `#`, `@`, `|`, `~`, `^` |
| Forbidden content | Channel names, hashtags, promotional copy |

The DAG task MUST validate the returned title against these constraints and
retry (up to one re-prompt) if the title violates them. If the second attempt
also violates the constraints, the task MUST log a warning and truncate/strip to
the nearest valid form rather than failing the run.

#### Scenario: Valid title is accepted on first attempt

- GIVEN the OpenAI response returns `"El Congreso vota el futuro de las pensiones"`
  (42 chars, no emojis, no symbols)
- WHEN `generate_title` validates the response
- THEN the title is accepted without re-prompting

#### Scenario: Title exceeding 100 characters triggers re-prompt

- GIVEN the OpenAI response returns a title of 120 characters
- WHEN `generate_title` validates the response
- THEN a second OpenAI call is made with an instruction to shorten the title
- AND if the second response is within 100 characters, it is accepted

#### Scenario: Title containing emojis triggers re-prompt

- GIVEN the OpenAI response returns `"El Congreso debate 🔥 el futuro"`
- WHEN `generate_title` validates the response
- THEN a second call is made explicitly forbidding emojis
- AND the accepted title contains no emoji characters

#### Scenario: Both attempts return invalid title — strip and warn

- GIVEN both OpenAI attempts return a title containing emojis or exceeding
  100 characters
- WHEN `generate_title` processes the second response
- THEN the emojis are stripped and the string is truncated to 100 characters
- AND a WARNING is logged indicating the fallback was applied
- AND the task succeeds (no exception raised)

#### Scenario: `ai_helpers` wrapper is used — no direct OpenAI SDK calls

- GIVEN the DAG source and all DAG-level module files are inspected
- WHEN they are searched for direct `openai.` or `OpenAI()` instantiation
  outside of `utils/ai_helpers.py`
- THEN no such direct calls are found

---

### Requirement: Best Option Selected by Highest Pikzels Score; Tie-Break is First Option

`choose_best_option` MUST select the option with the highest `score_thumbnail`
value. When both scores are equal, option A (the first generated) MUST be
selected. The selected option's `label` MUST be flagged as chosen in the
persistence payload.

#### Scenario: Option B scores higher — option B chosen

- GIVEN option A has score 72 and option B has score 85
- WHEN `choose_best_option` executes
- THEN option B is marked as chosen

#### Scenario: Equal scores — option A wins

- GIVEN option A has score 78 and option B has score 78
- WHEN `choose_best_option` executes
- THEN option A is marked as chosen

---

### Requirement: Results Persisted in `video_thumbnails` Table

A new table `video_thumbnails` MUST be created via migration `017_*.sql`. Both
options MUST be persisted as separate rows. The table schema MUST include at
minimum:

| Column | Type | Notes |
|--------|------|-------|
| `thumbnail_id` | `SERIAL PRIMARY KEY` | |
| `chapter_id` | `INT NOT NULL` | FK → `video_chapters(chapter_id)` |
| `youtube_video_id` | `TEXT NOT NULL` | Denormalized for query convenience |
| `label` | `TEXT NOT NULL` | e.g. `"option_a"`, `"option_b"` |
| `local_path` | `TEXT NOT NULL` | Absolute path to the downloaded PNG |
| `pikzels_score` | `NUMERIC` | Score from `score_thumbnail` |
| `openai_title` | `TEXT` | Generated title (on chosen option; null on non-chosen) |
| `is_chosen` | `BOOLEAN NOT NULL DEFAULT FALSE` | True for the best-option row |
| `created_at` | `TIMESTAMPTZ NOT NULL DEFAULT NOW()` | |

`(chapter_id, label)` MUST be unique (unique constraint or unique index).

The `pikzels_report.json` file MUST NOT be created or updated by this DAG.
Existing legacy report files are unaffected.

#### Scenario: Both options are persisted after a successful run

- GIVEN a DAG run completes successfully with `youtube_video_id = "abc123"` and
  `chapter_id = 7`
- WHEN `video_thumbnails` is queried for `chapter_id = 7`
- THEN exactly 2 rows exist with labels `"option_a"` and `"option_b"`
- AND exactly one row has `is_chosen = TRUE`
- AND the chosen row has a non-null `openai_title`

#### Scenario: `local_path` stored for both options

- GIVEN a run completes successfully
- WHEN the two rows in `video_thumbnails` for the run are read
- THEN both `local_path` values match the pattern
  `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png`
- AND both files exist on the filesystem

#### Scenario: Re-run for same chapter_id replaces prior rows

- GIVEN `video_thumbnails` already contains rows for `chapter_id = 7`
- WHEN the DAG is re-triggered for the same `chapter_id`
- THEN the prior rows are either deleted+reinserted or updated via upsert
- AND after the run exactly 2 rows remain for `chapter_id = 7`

#### Scenario: `pikzels_report.json` is not written

- GIVEN a successful DAG run completes
- WHEN the working directory and data directory are checked
- THEN no `pikzels_report.json` file is created or modified by this DAG run

---

### Requirement: `PIKZELS_API_KEY` Wired Into All Runtime Surfaces

`PIKZELS_API_KEY` MUST be present as a named variable in:

- `.env` (with a placeholder value)
- All `docker-compose*.yml` env blocks that mount Airflow worker env vars
- `conftest.py` `_TEST_ENV` dict (as a non-empty stub string, e.g. `"test-key"`)

The DAG MUST NOT proceed past `validate_input` if this variable is absent from
the runtime environment.

#### Scenario: `PIKZELS_API_KEY` absent at DAG execution time

- GIVEN the environment variable `PIKZELS_API_KEY` is not set
- WHEN `validate_input` or the Pikzels client initializes
- THEN an `EnvironmentError` is raised
- AND no Pikzels API call is attempted

---

### Requirement: Task Graph Shape

The DAG MUST implement the following task graph with no additional top-level
branches:

```
validate_input
  → resolve_participant_photo
    → generate_thumbnail_option_a  → download_option_a  → score_option_a
    → generate_thumbnail_option_b  → download_option_b  → score_option_b
      → choose_best_option
        → generate_title
          → persist_results
```

`generate_thumbnail_option_a` and `generate_thumbnail_option_b` MUST run in
parallel after `resolve_participant_photo`.

`choose_best_option` MUST run after both score tasks complete. `generate_title`
MUST run after `choose_best_option` so it can use the chosen thumbnail's visual
context.

#### Scenario: Task graph reflects the required shape

- GIVEN the DAG `generic_thumbnail_generator` is loaded
- WHEN task dependencies are inspected
- THEN `resolve_participant_photo` has `validate_input` as its sole upstream
- AND `generate_thumbnail_option_a` and `generate_thumbnail_option_b` both have
  `resolve_participant_photo` as their sole upstream
- AND `choose_best_option` has both score tasks as upstreams
- AND `generate_title` has `choose_best_option` as its sole upstream
- AND `persist_results` has `generate_title` as its sole upstream

---

### Requirement: Test Coverage at 80% Minimum

All new modules (`pikzels_client.py`, DAG file, config loader, persistence
layer) MUST be covered by tests in `tests/congress_videos/modules/`. Coverage
MUST be measured by `pytest --cov` and MUST NOT fall below 80% for the new
code paths introduced by this change.

#### Scenario: Coverage gate passes

- GIVEN the test suite in `tests/congress_videos/modules/` is executed with
  `uv run pytest --cov=congress_videos/modules/pikzels_client --cov-fail-under=80`
- WHEN all tests complete
- THEN the coverage report shows ≥ 80% for the measured modules
- AND no test failures are reported

---

## MODIFIED Requirements

_(None — this change is purely additive. No existing DAG, table, or module
contract is altered.)_
