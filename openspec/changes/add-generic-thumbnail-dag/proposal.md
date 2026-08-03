# Proposal: Generic Thumbnail + Title DAG (Pikzels + OpenAI)

## Intent

Thumbnail generation today is Congreso-specific and ad hoc: Pikzels output is captured in a
manual `pikzels_report.json`, results are not queryable, and Pikzels `output` URLs expire after
24h so nothing is durably stored. We need a GENERIC, on-demand DAG (mirroring
`generic_youtube_uploader`) that any domain can drive by config — resolving the politician
photo automatically from `congress_participants`, producing scored thumbnail options plus a
matching title, and persisting structured results in Postgres. Congreso must keep working
identically; only per-domain config changes for a new domain.

## Scope

### In Scope
- New triggered DAG `generic_thumbnail_generator` (`schedule=None`, conf/config-driven), invokable via `trigger_dag_api`.
- Per-domain config (YAML or equivalent): visual styles, personas/speakers, participants source.
- Input contract (per video): `youtube_video_id`, `chapter_id`, debate summary, session.
- Auto photo resolution: `normalized_name` → `congress_participants.photo_url`, then DOWNLOAD the real image bytes for Pikzels; optional party-logo support image.
- Port Pikzels v2 client → `congress_videos/modules/pikzels_client.py`: KEEP `thumbnail_from_text`, `score_thumbnail`, `download`, `to_base64_data_url`, retry/backoff; TRIM `thumbnail_from_image`, `edit_thumbnail`, `generate_titles` (titles come from OpenAI, not Pikzels), all pikzonality methods.
- Title generation via OpenAI ONLY, using the shared `utils/ai_helpers.py` wrapper (not direct SDK). Pikzels' own title endpoint is deliberately not used.
- Persistence: new dedicated `video_thumbnails` table (+ migration `017`) storing both options, scores, the OpenAI-generated title, local path.
- Local thumbnail download to `/opt/airflow/data/congress_videos/thumbnails/{youtube_video_id}/{label}.png` (co-located with generation, before 24h expiry).
- Wire `PIKZELS_API_KEY` into `.env`, docker-compose env block, and `conftest.py` `_TEST_ENV`.
- Tests + fixtures under `tests/congress_videos/modules/`; strict TDD; 80% coverage.

### Out of Scope
- Post-generation editing (`edit_thumbnail` + scorer-suggestion loop) — later issue.
- Removing/altering the legacy Pillow render in `modules/thumbnail_generator.py` (stays as fallback).
- Pikzonality / persona-discovery API surface.
- Changing the Pikzels API contract itself (same API; only input config differs).

## Genericity Boundary

| Per-domain config | Fixed (code) |
|-------------------|--------------|
| Visual styles, personas/speakers list | Task graph + DAG shape |
| Participants source / lookup key | Pikzels client + retry logic |
| Prompt templates, title constraints | DB schema + persistence layer |
| Party-logo asset mapping | Photo download + local-store convention |

Acceptance: a new domain generates thumbnails with zero Congreso hardcoding; Congreso becomes just one config.

## Capabilities

### New Capabilities
- `generic-thumbnail-generation`: on-demand, config-driven DAG that resolves a participant photo, generates + scores 2 Pikzels thumbnail options, produces a matching OpenAI title, chooses the best option, and persists results with a locally-downloaded image.

### Modified Capabilities
- None

## Approach

Approach A — triggered-per-video, conf-driven (`schedule=None`). Task graph:
`validate_input` → `resolve_participant_photo` (lookup + download bytes) → per option
(`generate_thumbnail` + `download` local) → `score_thumbnail` → `generate_title`
(OpenAI via `ai_helpers`, prompted to match the thumbnail) → `choose_best_option` → `persist_results` (DB).
Mirror `generic_youtube_uploader` for the on-demand + per-domain config/trigger pattern.

**Persistence decision — new `video_thumbnails` table (not extend `video_chapters`).** Rationale:
one video can yield multiple options and reruns; a 1:N child table keeps `video_chapters` clean,
avoids wide nullable columns, and records per-option score/label/local-path/title-source natively.
FK to `video_chapters(chapter_id)`.

**Title decision.** Titles come EXCLUSIVELY from OpenAI (`ai_helpers.generate_chat_completion/json`),
prompted with the video summary plus the chosen thumbnail's prompt/visual context so the title
matches the image. Pikzels' `generate_titles` is deliberately NOT used. Exact OpenAI constraints
deferred to design (see Residual Decisions).

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/generic_thumbnail_generator_dag.py` | New | Triggered, config-driven DAG |
| `congress_videos/modules/pikzels_client.py` | New | Ported/trimmed Pikzels v2 client |
| `congress_videos/modules/participants_db.py` | Reused | `lookup_participant_fuzzy` for photo_url |
| `congress_videos/config/` | New/Modified | Per-domain thumbnail config (styles/personas) |
| `migrations/017_*.sql` | New | `video_thumbnails` table |
| `.env` / `docker-compose*.yml` / `conftest.py` | Modified | Add `PIKZELS_API_KEY` |
| `tests/congress_videos/modules/*` | New | Client, resolver, persistence, DAG-import tests |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Pikzels URLs expire in 24h | High | Download co-located with generation; persist local path |
| `PIKZELS_API_KEY` absent everywhere | High | Add to `.env`, compose, conftest stub before code |
| Photo missing / null `photo_url` | Med | Validate + fail-fast or skip-with-log per config; design to specify |
| Over-generalizing config surface (YAGNI) | Med | Constrain config to issue-listed fields only |
| Pikzels/OpenAI cost or rate limits | Med | Reuse client retry/backoff; 2 options cap |

## Rollback Plan

Additive and isolated. Revert by removing the new DAG, `pikzels_client.py`, domain config, and
tests. Drop `video_thumbnails` via a down-migration (or leave table unused — no other consumer).
`video_chapters` is untouched, so no destructive schema change. Legacy Pillow render and
`pikzels_report.json` path remain intact until deliberately retired.

## Dependencies

- #19 participants DB (satisfied): `congress_participants` with `photo_url`.
- Reachable Pikzels + OpenAI APIs from the Airflow runtime; `PIKZELS_API_KEY` provisioned.

## Residual Decisions (for spec/design)

- (a) Exact OpenAI title constraints: model, max length, tone, and stripping stray
  channel symbols/emojis (per prior user feedback).
- (b) "Best option" selection rule: by Pikzels score alone vs. score + tie-break/other signal.

## Success Criteria

- [ ] A new domain generates thumbnails with no Congreso-specific hardcoding (config only).
- [ ] DAG receives video + politician info via param/config and runs on-demand.
- [ ] Per video: 2 thumbnail options + scores + chosen title persisted in `video_thumbnails`.
- [ ] Politician photo bytes fetched automatically from `congress_participants`.
- [ ] Thumbnails downloaded locally before Pikzels URL expiry.
- [ ] Congreso de España generates thumbnails exactly as before (same Pikzels API).
- [ ] Tests pass; DAG import-error check clean; 80% coverage held.
