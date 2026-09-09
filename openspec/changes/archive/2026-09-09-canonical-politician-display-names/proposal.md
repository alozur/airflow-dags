# Proposal: Canonical Politician Display Names (Issue #511)

## Problem Statement

Three generators name politicians inconsistently:

1. **Long-form titles** inject raw `key_speakers` names
2. **Art direction** injects the first real speaker name  
3. **Shorts metadata** hardcodes a 4-level Spanish naming taxonomy in free LLM prose ("Nivel 1 solo apellido: Sánchez, Feijóo…") — an uncurated, unversioned, LLM-guessed approximation of an editorial decision

This inconsistency produces different names in different channels for the same politician and makes the shorts taxonomy impossible to maintain at scale.

## Approach

Introduce a deep module `congress_videos/modules/politician_display_names.py` with a versioned catalogue `congress_videos/catalogs/politician_display_names.v1.json`, cloning the proven `institutional_role_resolver.py` + `institutional_roles.v1.json` pattern:

- **CatalogLoader** → frozen dataclasses
- Strict `CatalogValidationError` 
- Load-time collision detection
- Accent/case-insensitive normalization
- Never-raising resolve
- Lazy module-singleton loading (as in `speaker_normalization._get_role_catalog`)

Interface: `canonical_display_name(slug: str | None) -> str | None`

Identity resolution remains untouched. No DB migration.

## Consumers (Three Call Sites)

1. `thumbnail_generation.generate_title`/`_build_title_prompt` ← `conf["slug"]` at `generic_thumbnail_generator_dag._task_generate_title`
2. `thumbnail_generation.art_direct`/`resolved_photo_speaker_name` ← same `conf["slug"]` at `_task_art_direction`
3. `reap_shorts_uploader_dag.build_shorts_metadata_context` ← `turn_speaker_slug` from `db.get_turn_speaker_slug(turn_id)`

Plus `congress_videos/config/ai_prompts.py` (delete the prose taxonomy) and a new `docs/CANONICAL_DISPLAY_NAMES.md`.

## Verified Pre-Proposal Details

- **No new DB plumbing needed.** The shorts path has been turn-sourced since #467 and `_generate_metadata` already reads `db.get_turn_speaker_slug(turn_id)` → `build_shorts_metadata_context(ch, turn_speaker_slug, lookup_participant_by_slug)`, which already performs the slug→display_name lookup. That function is the exact seam for the catalogue.

- **Existing wiring ready.** `youtube_upload_dag.trigger_thumbnail_generation` already puts `"slug"` in `child_conf`, so both long-form call sites already have the slug in `conf` — wiring is a parameter pass, not new plumbing.

- **Ambiguity enforcement required.** `congress_participants` has no surname column and `display_name` stores raw "Apellidos, Nombre", so a live duplicate-surname check is not reliably derivable. Enforcement must happen at catalogue-authoring time.

- **Fallback contract:** `canonical_display_name` never raises; returns None for absent/unresolved/uncatalogued/ambiguous slugs → consumers keep existing full-name behaviour.

- **Rollback is data-only:** emptying the catalogue's `entries` array restores today's behaviour with no code revert.

- **No existing spec:** No existing spec in `openspec/specs/` governs display-name rendering → one New Capability `canonical-display-names`, zero Modified Capabilities.

## Slice Plan

Stacked-to-main, ≤400 lines each:

1. **Catalogue module+JSON+tests** (~350 lines)
2. **Populate 11-entry roster** (~170 lines)
3. **Long-form title wiring** (~150 lines)
4. **Art-direction wiring** (~160 lines)
5. **Shorts wiring + taxonomy removal** (~180 lines)
6. **Prompt copy cleanup** (~90 lines)
7. **Governance documentation** (~120 lines)

Slices 2–4 are no-ops until a slug is in the catalogue.

## Rollback

Data-only: empty the catalogue's `entries` array to restore today's behaviour with no code revert.

## Risks

- Load-time `CatalogValidationError` on catalogue corruption — mitigated by lazy-singleton + never-raising resolve + strict CI test on the bundled catalogue
- Ambiguity collision detection requires manual catalogue authoring discipline — mitigated by strict load-time validation
- Omission of a needed politician requires catalogue update + re-deploy — acceptable per quarterly-plus-reshuffle governance cadence

## Success Criteria

- All three generators render one consistent name for one resolved slug
- Catalogue is versioned, validated at load time, and documented
- Fallback to existing names is transparent when slug is absent/unmapped/ambiguous
- Shorts taxonomy prose is fully removed
