# Proposal: Canonical Politician Display Names

Issue: #511

## Intent

Published titles and thumbnail text name politicians inconsistently. Three generators each invent their own naming: long-form titles inject raw `key_speakers` strings, art direction injects the first real speaker name, and `SHORTS_METADATA_SYSTEM_PROMPT` hardcodes a 4-level Spanish naming taxonomy in free LLM prose ("Nivel 1 solo apellido: Sánchez, Feijóo…"). That taxonomy is an uncurated, unversioned, LLM-guessed approximation of an editorial decision. The result is that the same person renders as a full legal name, a surname, or a role depending on which path published the video.

Success: after identity resolution yields a participant slug, every consumer renders the same curated public name for that person, or falls back to today's behaviour.

Note: the issue's "two title generators" framing is stale — `youtube_ai.generate_youtube_title` no longer exists.

## Scope

### In Scope

- New deep module `congress_videos/modules/politician_display_names.py` and versioned catalogue `congress_videos/catalogs/politician_display_names.v1.json`.
- Wiring at three consumers: long-form title, long-form art direction, shorts metadata.
- Replacing the prose naming taxonomy in `SHORTS_METADATA_SYSTEM_PROMPT`.
- Catalogue ownership/review documentation.

### Out of Scope

- Identity resolution (slug production) — untouched.
- Any DB migration. Next free number is 050; this change needs none.
- New `get_chapter_metadata` columns — see Approach, the slug already flows.
- Backfilling or rewriting already-published titles.
- Dead prompts `THUMBNAIL_TEXT_SYSTEM_PROMPT` / `THUMBNAIL_TEXT_USER_PROMPT_TEMPLATE` (no callers).

## Capabilities

### New Capabilities
- `canonical-display-names`: curated slug→public-name catalogue, its validation and ambiguity rules, and the rendering contract for title/thumbnail/shorts consumers.

### Modified Capabilities
- None. No existing spec in `openspec/specs/` governs display-name rendering.

## Approach

Clone the proven `institutional_role_resolver.py` + `institutional_roles.v1.json` pattern: versioned JSON, `CatalogLoader` → frozen dataclasses, strict `CatalogValidationError`, load-time collision detection, accent/case-insensitive normalization, a never-raising resolve, lazy module-singleton loading (as in `speaker_normalization._get_role_catalog`).

Interface (deep, one function for callers):

```
canonical_display_name(slug: str | None) -> str | None
```

Catalogue sketch:

```json
{
  "catalog_version": 1,
  "entries": [
    {
      "participant_slug": "pedro-sanchez-perez-castejon",
      "display_name": "Sánchez",
      "ambiguous": false,
      "selection_note": "Party leader, nationally unmistakable surname",
      "provenance": {"publisher": "...", "reference_url": "https://...",
                     "evidence_note": "...", "reviewed_on": "2026-09-09"}
    }
  ]
}
```

**Ambiguity rule** — enforced at authoring time, not derived live. `congress_participants` has no surname column and `display_name` stores raw "Apellidos, Nombre", so a live duplicate-surname check is not reliably derivable. At load: duplicate `participant_slug`, or two entries whose normalized `display_name` collides, raise `CatalogValidationError`. The author must then disambiguate (e.g. `Yolanda Díaz`) or set `"ambiguous": true`.

**Fallback contract**: `canonical_display_name` never raises and returns `None` when the slug is absent, unresolved, missing from the catalogue, or marked ambiguous. Every consumer then keeps its existing full-name behaviour. Never guess a shortened form.

**Verified plumbing correction** (checked in this worktree, narrows the handoff's assumption): no new DB read is needed.

| # | Consumer | Slug source | Change |
|---|---|---|---|
| 1 | `thumbnail_generation.generate_title` / `_build_title_prompt` | `conf["slug"]`, already threaded by `youtube_upload_dag.trigger_thumbnail_generation` → `generic_thumbnail_generator_dag._task_generate_title` | new `participant_slug` arg; canonical name replaces the injected `THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION` speaker entry |
| 2 | `thumbnail_generation.art_direct` / `resolved_photo_speaker_name` | same `conf["slug"]` at `_task_art_direction` | canonical name feeds `ART_DIRECTION_RESOLVED_PHOTO_INSTRUCTION` |
| 3 | `reap_shorts_uploader_dag.build_shorts_metadata_context` | `turn_speaker_slug`, already read via `db.get_turn_speaker_slug(turn_id)` in `_generate_metadata` | catalogue applied where the slug→`display_name` lookup already happens; prose taxonomy deleted from the system prompt |

The chapter-level `resolved_participant_slug` is indeed absent from `get_chapter_metadata`, but reap shorts have been turn-sourced since #467, so the turn slug is the authoritative one and no SELECT change is required.

**Initial selection criterion** (documented, reviewable): candidates ranked by resolved appearance count across `video_chapters.resolved_participant_slug` and `speaker_turn_videos.resolved_participant_slug`, then admitted only when an editor judges the short form nationally unmistakable. Review cadence: quarterly, plus after any general election or cabinet reshuffle.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/modules/politician_display_names.py` | New | Loader, dataclasses, `canonical_display_name` |
| `congress_videos/catalogs/politician_display_names.v1.json` | New | Curated initial entries |
| `congress_videos/modules/thumbnail_generation.py` | Modified | `_build_title_prompt`, `generate_title`, `art_direct`, `resolved_photo_speaker_name` |
| `congress_videos/generic_thumbnail_generator_dag.py` | Modified | Pass `conf["slug"]` at two task seams |
| `congress_videos/reap_shorts_uploader_dag.py` | Modified | `build_shorts_metadata_context` |
| `congress_videos/config/ai_prompts.py` | Modified | Replace the 4-level taxonomy in `SHORTS_METADATA_SYSTEM_PROMPT` |
| `docs/CANONICAL_DISPLAY_NAMES.md` | New | Ownership, criterion, cadence, how to add a mapping |
| `tests/congress_videos/**` | New/Modified | See test plan |

## Test Plan

Strict TDD, `uv run pytest`.

| Case | Where |
|------|-------|
| Mapped slug → short name; unmapped slug → `None` | `tests/congress_videos/test_politician_display_names.py` (clone `test_institutional_role_resolver.py`) |
| Duplicate slug and colliding short form → `CatalogValidationError` | same |
| `ambiguous: true` → `None` | same |
| Accented names normalize correctly | same |
| Malformed/missing catalogue → `CatalogValidationError`; resolve never raises | same |
| Bundled catalogue loads and every entry validates | same |
| Title prompt uses canonical name; falls back on `None` slug | `tests/congress_videos/modules/test_thumbnail_generation.py` (`TestGenerateTitle*`) |
| Art-direction speaker name canonicalised; unchanged when unmapped | same |
| Shorts speaker name canonicalised; taxonomy prose absent from prompt | new `tests/congress_videos/test_reap_shorts_uploader_dag.py` (none exists) |
| Title and thumbnail render the same name for one slug | consistency test across both seams |

## Slice Boundary (stacked-to-main, ≤400 changed lines each)

1. **Catalogue module + JSON + resolver tests** (~350) — standalone, no consumer touched.
2. **Long-form title wiring + tests** (~150).
3. **Long-form art-direction wiring + tests** (~150).
4. **Shorts wiring + prompt taxonomy removal + new test file** (~250).
5. **Documentation** (~120).

Each slice ships behaviour that is safe alone: slices 2–4 are no-ops until a slug is present in the catalogue.

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| A curated short form is wrong or becomes stale | Med | Provenance + `reviewed_on` per entry; documented review cadence |
| Removing the prose taxonomy degrades shorts titles for unmapped speakers | Med | Keep an explicit neutral instruction for the unmapped case; assert it in tests |
| A colliding surname reaches production | Low | Load-time `CatalogValidationError` fails the DAG import loudly, plus a test over the bundled catalogue |
| Prompt drift breaks golden expectations | Low | Prompt assertions live beside each wiring slice |

## Rollback Plan

Per slice: revert the slice commit. Full rollback: empty the catalogue's `entries` array — `canonical_display_name` then returns `None` for every slug and all three consumers resume today's full-name behaviour with no code revert. No migration, no persisted state, so nothing to unwind.

## Dependencies

- None external. Identity resolution and the participants roster already exist.

## Success Criteria

- [ ] Versioned catalogue maps participant slugs to curated public names, with a documented selection criterion.
- [ ] Pedro Sánchez renders as `Sánchez` in title and thumbnail text when his slug is resolved.
- [ ] Catalogue is consulted only after identity resolution produced a valid slug.
- [ ] Colliding short forms are rejected at load time.
- [ ] Missing, ambiguous, or stale mappings fall back to existing full-name behaviour.
- [ ] `SHORTS_METADATA_SYSTEM_PROMPT` no longer carries a hardcoded naming taxonomy.
- [ ] Tests cover mapped, unmapped, duplicate-surname, accented, and title/thumbnail consistency cases.
- [ ] `uv run pytest` passes; DAG import errors remain zero.
