# Tasks: Canonical Politician Display Names

## Review Workload Forecast

| Field | Value |
|-------|-------|
| Estimated changed lines | ~1220 total (350/170/150/160/180/90/120) |
| 400-line budget risk | Medium (slice 1 tightest at ~350) |
| Chained PRs recommended | Yes |
| Suggested split | PR 1 → PR 2 → PR 3 → PR 4 → PR 5 → PR 6 → PR 7 |
| Delivery strategy | auto-chain |
| Chain strategy | stacked-to-main |

Decision needed before apply: No
Chained PRs recommended: Yes
Chain strategy: stacked-to-main
400-line budget risk: Medium

### Suggested Work Units

| Unit | Goal | Likely PR | Focused test command | Runtime harness | Rollback boundary |
|------|------|-----------|----------------------|-----------------|-------------------|
| 1 | Loader + dataclasses + 1-entry catalogue | PR 1 | `uv run pytest tests/congress_videos/test_politician_display_names.py` | `bash scripts/test-airflow-e2e.sh` (auto, touches `congress_videos/**`) | Revert deletes module+catalog+tests; no consumer wired |
| 2 | Populate 11-entry roster | PR 2 | `uv run pytest tests/congress_videos/test_politician_display_names.py -k bundled` | same (auto) | Revert restores 1-entry catalogue; pure data |
| 3 | Title wiring | PR 3 | `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py -k title` | same (auto) | Revert restores `_build_title_prompt`/`generate_title` signatures only |
| 4 | Art-direction wiring + cross-seam test | PR 4 | `uv run pytest tests/congress_videos/modules/test_thumbnail_generation.py -k "photo or art_direct"` | same (auto) | Revert restores `resolved_photo_speaker_name` only |
| 5 | Shorts wiring | PR 5 | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k ShortsMetadataContext` | same (auto) | Revert restores `build_shorts_metadata_context` only |
| 6 | Prompt taxonomy removal | PR 6 | `uv run pytest tests/congress_videos/test_reap_uploader_dag.py -k PromptTemplates` | same (auto) | Revert restores taxonomy prose; independent of wiring |
| 7 | Governance doc | PR 7 | `uv run pytest tests/docs/test_canonical_display_names_doc.py` | N/A — docs-only, outside e2e path filter | Revert removes the doc file only |

## Phase 1: Catalogue Module + Loader (PR 1, ~350 lines)

- [ ] 1.1 RED `tests/congress_videos/test_politician_display_names.py` (clone `tests/congress_videos/test_institutional_role_resolver.py` (read-only)): 1-entry catalogue loads; malformed shapes, duplicate slug, colliding normalized `display_name` (resolvable set only), `display_name` not a subsequence of `full_name`, bad `reference_url`, unparsable `reviewed_on`, blank `selection_note` → `CatalogValidationError`; `ambiguous: true` skips collision, never resolves; accented slug resolves; unmapped/`None`/blank slug → `None`; missing/corrupt catalog never raises, logs once.
- [ ] 1.2 GREEN `congress_videos/modules/politician_display_names.py`: import `CatalogValidationError` from `institutional_role_resolver.py` (read-only, D3); frozen `DisplayName`/`DisplayNameCatalog`, `DisplayNameCatalogLoader`, private `_normalize_display_name`, lazy-singleton `canonical_display_name(slug)`.
- [ ] 1.3 GREEN `congress_videos/catalogs/politician_display_names.v1.json`: `catalog_version: 1`, one entry (`pedro-sanchez-perez-castejon` → `Sánchez`).
- [ ] 1.4 Run `uv run ruff check` on the new module; if `max-complexity=10` fires on the loader, split fixture-driven tests into their own PR (design contingency).

## Phase 2: Populate Roster (PR 2, ~170 lines)

- [ ] 2.1 RED: extend `test_politician_display_names.py` — exactly 11 entries, no collisions, `isabel-rodriguez-garcia`→`Isabel Rodríguez` and `javier-rodriguez-palacios`→`Javier Rodríguez` distinct, `pedro-sanchez-perez-castejon`→`Sánchez`, each `display_name` a subsequence of its `full_name`.
- [ ] 2.2 GREEN: add the remaining 10 entries (Tellado Filgueira, Gamarra Ruiz-Clavijo, Corujo Berriel, Muñoz Abrines, Rodríguez Palacios, Micó Micó, Hernández Quero, Guinart Moreno, Abascal Conde, Rodríguez García) to `politician_display_names.v1.json` with real `provenance`.

## Phase 3: Title Wiring (PR 3, ~150 lines)

- [ ] 3.1 RED: extend `tests/congress_videos/modules/test_thumbnail_generation.py` near `TestGenerateTitlePromptInjection` — mapped `participant_slug` replaces the first `_real_speakers` entry in `THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION`; unmapped/`None` byte-identical; `pedro-sanchez-perez-castejon` → `Sánchez`.
- [ ] 3.2 GREEN: add `participant_slug: str | None = None` to `_build_title_prompt`/`generate_title` in `congress_videos/modules/thumbnail_generation.py`; substitute `canonical_display_name(participant_slug)` for `real[0]` when it resolves.
- [ ] 3.3 GREEN: pass `participant_slug=conf.get("slug")` at `_task_generate_title` in `congress_videos/generic_thumbnail_generator_dag.py`.

## Phase 4: Art-Direction Wiring + Cross-Seam Consistency (PR 4, ~160 lines)

- [ ] 4.1 RED: extend tests near `TestResolvedPhotoSpeakerName`/`TestArtDirectResolvedPhotoInstruction` — mapped slug wins over `_real_speakers(...)[0]`; unmapped byte-identical; one test asserting title and art-direction render the identical name for one mapped slug.
- [ ] 4.2 GREEN: add `participant_slug: str | None = None` to `resolved_photo_speaker_name`; call `canonical_display_name`.
- [ ] 4.3 GREEN: pass `conf.get("slug")` at `_task_art_direction` and `_task_art_direction_retry` in `generic_thumbnail_generator_dag.py`.

## Phase 5: Shorts Wiring (PR 5, ~180 lines)

- [ ] 5.1 RED: extend `TestBuildShortsMetadataContext` in `tests/congress_videos/test_reap_uploader_dag.py` — mapped `turn_speaker_slug` yields the catalogued name over `participants_lookup`; unmapped/`None` falls through unchanged; mentioned people never canonicalised; `pedro-sanchez-perez-castejon` → `Sánchez`.
- [ ] 5.2 GREEN: in `build_shorts_metadata_context` (`congress_videos/reap_shorts_uploader_dag.py` L104-117), try `canonical_display_name(turn_speaker_slug)` before `participants_lookup`.

## Phase 6: Prompt Taxonomy Removal (PR 6, ~90 lines)

- [ ] 6.1 RED: flip `test_system_prompt_contains_siempre_and_taxonomy_rule` to assert no `"Nivel 1"`–`"Nivel 4"`, presence of `"EXACTAMENTE como aparece"` + `"cargo o rol"`; assert `"taxonomía de 4 niveles"` absent from `SHORTS_METADATA_USER_PROMPT_TEMPLATE`.
- [ ] 6.2 GREEN: replace `SHORTS_METADATA_SYSTEM_PROMPT` (lines 13-20) and `SHORTS_METADATA_USER_PROMPT_TEMPLATE` (line 46) in `congress_videos/config/ai_prompts.py` with the D6 wording.

## Phase 7: Governance Documentation (PR 7, ~120 lines)

- [ ] 7.1 RED `tests/docs/test_canonical_display_names_doc.py`: `docs/CANONICAL_DISPLAY_NAMES.md` exists and mentions ownership, the "≥2 appearances" selection criterion, quarterly-plus-reshuffle cadence, add/edit procedure.
- [ ] 7.2 GREEN: write `docs/CANONICAL_DISPLAY_NAMES.md` — ownership, mechanical selection criterion (against the 11-entry roster), review cadence, step-by-step add/edit procedure (schema, provenance, `uv run pytest tests/congress_videos/test_politician_display_names.py`).
