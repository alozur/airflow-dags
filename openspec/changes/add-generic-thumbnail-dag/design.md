# Design: Generic Thumbnail + Title DAG (Pikzels + OpenAI)

## Technical Approach

A triggered, `schedule=None` DAG `generic_thumbnail_generator` mirroring `utils/youtube_uploader_dag.py`: `dag_run.conf` carries per-video input, per-domain style/persona/prompt config lives in a code module keyed by domain. Tasks are `PythonOperator`s wired with `xcom_task` (`utils/airflow_helpers.py`). Pikzels v2 client is ported/trimmed into `congress_videos/modules/pikzels_client.py`. Titles come EXCLUSIVELY from OpenAI via `utils/ai_helpers.generate_json_completion`. Results persist to a new `video_thumbnails` child table (migration `017`), FK to `video_chapters(chapter_id)`.

## Architecture Decisions

| Decision | Choice | Alternatives (rejected) | Rationale |
|---|---|---|---|
| Persistence | New `video_thumbnails` 1:N child table | Extend `video_chapters` w/ wide nullable cols | 1 video → 2 options + reruns; keeps chapters clean; per-option score/label/path |
| Titles | OpenAI only (`generate_json_completion`, gpt-4o-mini) | Pikzels `generate_titles` | Proposal mandate; consistent tone control; Pikzels title endpoint trimmed |
| Config surface | Python module `thumbnail_config.py`, dict keyed by domain | YAML file | No new dep/loader (YAGNI); matches `config/ai_prompts.py` + `paths.py` precedent |
| Support image | Layered: politician photo (from `photo_url`) as the single Pikzels `support_image`; if photo missing/undownloadable, fall back to the domain `party_logo`; if neither, fail-fast | Photo-only fail-fast; logo always | Pikzels accepts ONE support image per call, so photo and logo are mutually exclusive per generation. Real face is preferred; logo keeps the run alive when a photo is absent; failing only when truly nothing is available avoids brittle runs while still surfacing a genuine gap |
| Best option | Highest Pikzels `main_score`; tie-break = first option | Composite subscore weighting | Simple, deterministic, matches proposal |
| Download timing | Co-located: download immediately after each `generate_thumbnail` | Batch download at end | Pikzels URLs expire 24h; minimize window |

## Data Flow

    dag_run.conf ─→ validate_input ─→ resolve_participant_photo (lookup_participant_fuzzy → photo_url → download bytes → base64)
                                              │
              ┌───────────────────────────────┴──────────────────────────────┐
        generate_options (loop over 2 styles):                                 │
          thumbnail_from_text → download local PNG → score_thumbnail           │
                                              │                                │
                            choose_best_option (max main_score, tie=first)     │
                                              │                                │
                          generate_title (OpenAI: summary + best prompt/style) │
                                              │                                │
                                     persist_results (video_thumbnails) ←──────┘

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/generic_thumbnail_generator_dag.py` | Create | Triggered DAG, `schedule=None`, `max_active_runs=3` |
| `congress_videos/modules/pikzels_client.py` | Create | Ported/trimmed client (see Interfaces) |
| `congress_videos/modules/thumbnail_generation.py` | Create | Domain logic: resolve photo, generate/score/choose, title, persist |
| `congress_videos/config/thumbnail_config.py` | Create | Per-domain styles/personas, participants source, prompt template, party-logo map |
| `congress_videos/config/ai_prompts.py` | Modify | Add `THUMBNAIL_TITLE_SYSTEM_PROMPT` + `_USER_PROMPT_TEMPLATE` |
| `congress_videos/config/paths.py` | Modify | Add `get_thumbnail_dir(youtube_video_id)` |
| `congress_videos/sql/migrations/017_create_video_thumbnails.sql` | Create | Table + indexes (unqualified names — runner sets `search_path`) |
| `.env` / `docker-compose*.yml` / `conftest.py` (`_TEST_ENV`) | Modify | Add `PIKZELS_API_KEY` (test stub `pkz_test-...`) |
| `tests/congress_videos/modules/test_pikzels_client.py`, `test_thumbnail_generation.py`, `test_generic_thumbnail_dag.py` | Create | Client, logic, DAG-import tests |

## Interfaces / Contracts

**`pikzels_client.py`** — KEEP verbatim: `PikzelsClient.__init__`, `_request` (retry/backoff), `thumbnail_from_text`, `score_thumbnail`, `to_base64_data_url`, `download`, `PikzelsError`, `_drop_none`, `_check_xor`, `_check_model_support`. TRIM: `thumbnail_from_image`, `edit_thumbnail`, `generate_titles`, all `*_pikzonality*`, `create_persona/style`.

**`thumbnail_generation.py`**
```python
def resolve_participant_photo(name: str, cfg: dict) -> dict          # {support_image_b64, source: "photo"|"party_logo"}; photo_url first, party_logo fallback, raises only if neither available
def generate_and_score_options(prompt: str, photo_b64: str, cfg: dict) -> list[dict]  # [{label, output_url, local_path, main_score, style}]
def choose_best_option(options: list[dict]) -> dict                  # max main_score, tie=first
def generate_title(summary: str, best: dict, cfg: dict) -> str       # OpenAI only, cleaned
def persist_results(chapter_id: int, youtube_video_id: str, title: str, options: list[dict], best_label: str) -> None
```

**`dag_run.conf`**: `{domain, youtube_video_id, chapter_id, participant_name, summary}`.

**`017_create_video_thumbnails.sql`** (unqualified; `IF NOT EXISTS`):
```sql
CREATE TABLE IF NOT EXISTS video_thumbnails (
    thumbnail_id      SERIAL PRIMARY KEY,
    chapter_id        INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    youtube_video_id  VARCHAR(50),
    label             TEXT NOT NULL,          -- option identifier / style label
    style             TEXT,
    prompt            TEXT,
    main_score        NUMERIC(6,3),
    local_path        TEXT NOT NULL,
    output_url        TEXT,                   -- expires 24h; kept for audit
    openai_title      TEXT,                   -- OpenAI-generated (best/chosen option only; null on others)
    is_chosen         BOOLEAN DEFAULT FALSE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_video_thumbnails_chapter ON video_thumbnails(chapter_id);
CREATE INDEX IF NOT EXISTS idx_video_thumbnails_chosen ON video_thumbnails(chapter_id, is_chosen);
```

## OpenAI Title Spec

Model `gpt-4o-mini` (ai_helpers default), `max_tokens≈120`. System prompt: dramatic Spanish political-YouTube tone. User template: `{summary}` + best option's `{prompt}`/`{style}` (visual context so title matches image). Constraints: max 90 chars; NO emojis, NO channel symbols/hashtags/stray characters (prior user feedback); no surrounding quotes. `generate_json_completion` returns `{"title": "..."}`; strip whitespace/quotes before persist.

## Per-Domain Config Surface (YAGNI-bounded)

Configurable (issue-listed only): `styles` (2 style/persona entries), `participants_lookup` (fn ref to `lookup_participant_fuzzy`), `title_prompt` template refs, `party_logo_map`. Fixed in code: task graph, Pikzels client + retry, DB schema, photo-download + local-store convention. Congreso lives as `THUMBNAIL_CONFIG["congreso"]` so it runs identically; a new domain = a new dict entry, zero Congreso hardcoding.

## Retry / Timeout

Pikzels ~60s/image: `PikzelsClient(timeout=300, max_retries=5)` (client backoff). DAG `default_args`: `retries=2, retry_delay=5min`. Generate/score tasks `execution_timeout=timedelta(minutes=10)`.

## Testing Strategy

| Layer | What | Approach (strict TDD, 80%) |
|---|---|---|
| Unit | `pikzels_client` request/retry, xor/model guards | `mock_requests`; assert trimmed methods absent |
| Unit | resolve photo (found/missing→raise), download bytes | `mock_requests` + monkeypatch `lookup_participant_fuzzy` |
| Unit | choose_best (max, tie=first), title cleaning (no emoji/quotes) | pure fns + `mocker.patch` on `ai_helpers.generate_json_completion` |
| Unit | persist_results SQL params, is_chosen flag | `mock_psycopg2_connection` |
| Import | DAG parses, no import errors | import DAG module; `airflow dags list-import-errors` (docker e2e touches `congress_videos/**`) |

Note: `_TEST_ENV` must add `PIKZELS_API_KEY = "pkz_test-not-real"` (must start `pkz_`). ai_helpers patched at function level (mock_openai_client patches `openai.OpenAI`, but ai_helpers uses module-level `openai.chat` — patch `utils.ai_helpers.generate_json_completion` directly).

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or process-integration boundary. Only outbound HTTPS (Pikzels/OpenAI) and parameterised Postgres writes.

## Migration / Rollout

Additive. Apply `017` via existing `run_migrations` DAG (idempotent `IF NOT EXISTS`). Rollback: remove new files + drop `video_thumbnails`; `video_chapters` untouched. Legacy Pillow render and `pikzels_report.json` remain.

## Open Questions

- None blocking. Politician photo is the primary Pikzels `support_image`; the domain
  `party_logo` is the fallback when `photo_url` is absent/undownloadable. The run fails only
  when neither a photo nor a party logo is available.
