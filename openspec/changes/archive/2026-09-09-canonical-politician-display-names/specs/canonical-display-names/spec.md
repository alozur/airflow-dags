# Canonical Display Names Specification

## Purpose

Define a curated, versioned catalogue that maps a resolved participant slug to a preferred public display name, and the contract by which title, art-direction, and shorts-metadata generation consult it after identity resolution — never before, and never as a guess.

## Requirements

### Requirement: Versioned Catalogue Schema

The system MUST load politician display names from a versioned JSON catalogue (`congress_videos/catalogs/politician_display_names.v1.json`) whose entries carry `participant_slug`, `display_name`, an `ambiguous` flag, a `selection_note`, and a `provenance` block (`publisher`, `reference_url`, `evidence_note`, `reviewed_on`).

#### Scenario: Well-formed catalogue loads
- GIVEN the bundled catalogue file with `catalog_version: 1` and valid entries
- WHEN the catalogue is loaded
- THEN loading succeeds and every entry is available for resolution

#### Scenario: Malformed catalogue fails loudly
- GIVEN a catalogue file with missing/invalid required fields, a bad `reference_url` scheme, or an unparsable `reviewed_on` date
- WHEN the catalogue is loaded
- THEN loading raises `CatalogValidationError` and no entry from that file resolves

### Requirement: Load-Time Ambiguity Enforcement

The system MUST reject, at load time, a duplicate `participant_slug` or two entries whose normalized `display_name` collides, by raising `CatalogValidationError`. Ambiguity MUST NOT be derived from a live database query.

#### Scenario: Duplicate slug rejected
- GIVEN two catalogue entries sharing one `participant_slug`
- WHEN the catalogue is loaded
- THEN loading raises `CatalogValidationError`

#### Scenario: Colliding surname rejected
- GIVEN two distinct-slug entries whose normalized `display_name` is the same surname
- WHEN the catalogue is loaded
- THEN loading raises `CatalogValidationError`

#### Scenario: Author marks an entry ambiguous instead of colliding
- GIVEN an entry authored with `"ambiguous": true` and a unique normalized `display_name`
- WHEN the catalogue is loaded
- THEN loading succeeds and that entry never resolves to a display name

### Requirement: Never-Raising Resolution

The system MUST expose `canonical_display_name(slug: str | None) -> str | None` that never raises. It MUST return `None` when the slug is `None`, unmapped, marked `ambiguous`, or the catalogue failed to load; it MUST return the curated `display_name` only for a valid, unambiguous, mapped slug.

#### Scenario: Mapped slug resolves
- GIVEN a catalogue entry for slug `pedro-sanchez-perez-castejon` with `display_name: "Sánchez"`
- WHEN `canonical_display_name("pedro-sanchez-perez-castejon")` is called
- THEN it returns `"Sánchez"`

#### Scenario: Unmapped slug falls back
- GIVEN a slug absent from the catalogue
- WHEN `canonical_display_name` is called with that slug
- THEN it returns `None`

#### Scenario: Missing slug falls back
- GIVEN `slug` is `None` or empty
- WHEN `canonical_display_name` is called
- THEN it returns `None`

#### Scenario: Accented input normalizes
- GIVEN a mapped slug whose stored form uses accented characters
- WHEN `canonical_display_name` is called with that exact slug
- THEN it returns the curated name unchanged, without normalization errors

### Requirement: Resolution Only After Identity Resolution

Every consumer (long-form title, long-form art direction, shorts metadata) MUST call `canonical_display_name` only with a slug already produced by identity resolution (`conf["slug"]` for long-form; `speaker_turn_videos.resolved_participant_slug` for shorts). No consumer MAY guess, truncate, or shorten a name outside the catalogue.

#### Scenario: Title generation consults the catalogue
- GIVEN a resolved `participant_slug` mapped in the catalogue
- WHEN the title prompt is built
- THEN the speaker instruction names the person using the catalogued display name

#### Scenario: Art direction consults the catalogue
- GIVEN a resolved `participant_slug` mapped in the catalogue and a genuinely resolved participant photo
- WHEN the art-direction prompt is built
- THEN the resolved-photo instruction names the person using the catalogued display name

#### Scenario: Shorts metadata consults the catalogue
- GIVEN a turn's `resolved_participant_slug` mapped in the catalogue
- WHEN shorts metadata context is built
- THEN `speaker_display_name` is the catalogued display name

### Requirement: Safe Fallback to Existing Behavior

When the slug is missing, unresolved, unmapped, or ambiguous, every consumer MUST fall back to its existing full-name behavior unchanged.

#### Scenario: Title falls back on unmapped slug
- GIVEN a resolved slug absent from the catalogue
- WHEN the title prompt is built
- THEN the speaker instruction uses the existing full real-speaker name, not a guessed short form

#### Scenario: Shorts falls back on missing slug
- GIVEN no turn speaker slug is available
- WHEN shorts metadata context is built
- THEN `speaker_display_name` behaves exactly as before this change

### Requirement: Neutral Instruction Replaces Prose Taxonomy

`SHORTS_METADATA_SYSTEM_PROMPT` MUST NOT contain the hardcoded 4-level Spanish naming taxonomy. It MUST instead carry an explicit, neutral instruction covering the case where the speaker is absent from the catalogue.

#### Scenario: Taxonomy prose is absent
- GIVEN the current `SHORTS_METADATA_SYSTEM_PROMPT`
- WHEN its text is inspected
- THEN it contains no "Nivel 1/2/3/4" naming-level prose

#### Scenario: Unmapped-speaker instruction remains actionable
- GIVEN a speaker absent from the catalogue
- WHEN the shorts system prompt is used
- THEN it still instructs the model how to name or refer to that speaker without inventing a shortened form

### Requirement: Cross-Consumer Name Consistency

For one resolved slug present in the catalogue, title generation and art-direction generation MUST render the identical display name.

#### Scenario: Same slug, same name across seams
- GIVEN one mapped `participant_slug`
- WHEN both the title prompt and the art-direction prompt are built for that slug
- THEN both use the exact same catalogued display name

### Requirement: Catalogue Governance Documentation

The system MUST document catalogue ownership, the initial important-politician selection criterion, the review cadence, and the procedure to add or change a mapping.

#### Scenario: Documentation covers required topics
- GIVEN `docs/CANONICAL_DISPLAY_NAMES.md`
- WHEN it is read
- THEN it states ownership, the ranking-by-appearance-count selection criterion, the quarterly-plus-reshuffle review cadence, and the steps to add or edit an entry
