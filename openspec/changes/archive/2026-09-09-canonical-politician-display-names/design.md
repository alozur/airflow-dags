# Design: Canonical Politician Display Names (Issue #511)

## Overview

A curated slug→public-name catalogue, cloned from the `institutional_role_resolver` pattern, makes one resolved slug render one name everywhere.

## Architecture

### New Module: `congress_videos/modules/politician_display_names.py`

- **Classes**: `DisplayName`, `DisplayNameCatalog`, `DisplayNameCatalogLoader`
- **Functions**: `canonical_display_name(slug: str | None) -> str | None` (public interface)
- **Internals**: `_normalize_display_name(name: str) -> str` (private), `_get_catalog()` (lazy singleton)
- **Imports**: `CatalogValidationError` from `institutional_role_resolver` (shared definition at L15, stdlib-only module, no cycle)

### Catalogue File: `congress_videos/catalogs/politician_display_names.v1.json`

```json
{
  "catalog_version": 1,
  "entries": [
    {
      "participant_slug": "pedro-sanchez-perez-castejon",
      "display_name": "Sánchez",
      "full_name": "Pedro Sánchez Pérez-Castejón",
      "ambiguous": false,
      "selection_note": "PM: appears 2315 times",
      "provenance": {
        "publisher": "alozur",
        "reference_url": "https://www.congreso.es/",
        "evidence_note": "congressional_participants.display_name matches",
        "reviewed_on": "2026-09-05"
      }
    }
  ]
}
```

### Validation Schema

All catalogue entries MUST have:
- `participant_slug`: unique string (checked at load time)
- `display_name`: string, normalized form must not collide with other resolvable entries
- `full_name`: string, normalized `display_name` must be a token subsequence of normalized `full_name`
- `ambiguous`: boolean (false → resolve; true → never return, even if mapped)
- `selection_note`: string (min 10 chars)
- `provenance.publisher`: string
- `provenance.reference_url`: valid HTTPS/HTTP URL
- `provenance.evidence_note`: string
- `provenance.reviewed_on`: ISO date (YYYY-MM-DD)

### Key Design Decisions

**D1: Schema Design**
- `catalog_version` + `entries[]` with all fields above
- `full_name` is a LOAD-TIME invariant, not a second return value
- Normalized `display_name` must be a token subsequence of normalized `full_name`
- Mechanically enforces "never invent a shortened form"
- Example: Pedro Sánchez → `Sánchez` (valid subsequence)

**D2: Interface**
- Single caller function: `canonical_display_name(slug: str | None) -> str | None`
- Lazy module singleton (mirrors `speaker_normalization._get_role_catalog`)
- Never raises
- `DisplayName*` prefixes avoid colliding with the role module's generic `Catalog`/`CatalogLoader` names
- Public for tests only; normalizer stays private

**D3: CatalogValidationError Import**
- IMPORT from `institutional_role_resolver` (single repo definition)
- Rejected redefining locally (two same-named exceptions in one package → `except` silently misses one)
- Deferred extracting a shared `catalog_errors.py` until a third catalogue exists
- The 4-line normalizer is NOT shared — reimplemented privately because person-name normalization must be free to diverge

**D4: Collision Detection**
- Duplicate `participant_slug` checked across ALL entries
- Colliding normalized `display_name` checked over RESOLVABLE set only (`ambiguous == false`)
- Every structural defect is a hard `CatalogValidationError`

**D5: Wiring Pattern**
- `generate_title`/`_build_title_prompt` gain `participant_slug: str | None = None`
- `resolved_photo_speaker_name` gains `participant_slug: str | None = None`
- DAG seams pass `conf.get("slug")`
- `build_shorts_metadata_context` needs NO signature change — canonical lookup goes before `participants_lookup`
- Mentioned people are deliberately NOT canonicalised

**D6: Prompt Copy**
- Replace taxonomy prose ("Nivel 1-4…") with exact Spanish instruction
- New wording: "EXACTAMENTE como aparece en PONENTE PRINCIPAL… identifica al político por su cargo o rol y no inventes ningún nombre propio"
- Verified by absence of "Nivel 1-4" and presence of exact D6 strings
- Both `SHORTS_METADATA_SYSTEM_PROMPT` and `SHORTS_METADATA_USER_PROMPT_TEMPLATE` must change (line 46 also references taxonomy)

## Consumers

### 1. Title Generation
- **Call site**: `generic_thumbnail_generator_dag._task_generate_title` (L272)
- **Function**: `generate_title(..., participant_slug: str | None = None)` → `_build_title_prompt(..., participant_slug)`
- **Behavior**: If `canonical_display_name(participant_slug)` resolves, substitute for first `_real_speakers` entry; else use existing full name

### 2. Art Direction
- **Call sites**: `_task_art_direction` (L154) and `_task_art_direction_retry` (retry handler)
- **Function**: `resolved_photo_speaker_name(..., participant_slug: str | None = None)`
- **Behavior**: Same logic as title generation

### 3. Shorts Metadata
- **Call site**: `reap_shorts_uploader_dag.build_shorts_metadata_context` (L104-117)
- **Logic**: Try `canonical_display_name(turn_speaker_slug)` before existing `participants_lookup`
- **Mentioned people**: Never canonicalised (deliberate scope boundary)

## Testing Strategy

### Unit Tests
- Catalogue loading: well-formed, malformed schemas, missing fields, bad URLs, unparsable dates
- Collision detection: duplicate slugs, colliding surnames, ambiguous flag behavior
- Resolution: mapped slugs, unmapped slugs, None/blank input, accented input
- Never-raising guarantee: missing file, corrupt JSON, broken catalogue logs exactly once, returns None

### Integration Tests
- Title generation with mapped/unmapped slug
- Art direction consistency (same name as title for one slug)
- Shorts metadata fallback behavior
- Prompt copy verification: no "Nivel 1-4", presence of D6 wording

### Cross-Seam Consistency
- One mapped slug MUST render identically in title and art-direction prompts

## Rollback

Empty the `entries[]` array in `politician_display_names.v1.json`. All consumers fall back to existing behavior transparently.

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|-----------|
| Catalogue corruption breaks DAG import | Low | High | Lazy singleton + never-raising resolve + strict CI test |
| Ambiguous surname collision missed | Low | Medium | Strict load-time validation + docstring discipline |
| Missing politician requires update | High | Low | Quarterly-plus-reshuffle governance cadence |
| Untranslated identifiers confuse users | Low | Low | Name all internal fields in English; document in Spanish |

## Future Extensions

- Multi-language display names (future: extend schema to `display_names: { es: "...", en: "..." }`)
- Temporal validity (future: add `valid_from`/`valid_until` dates for reshuffle tracking)
- External catalogue sources (future: replace bundled JSON with API fetch + cache)
