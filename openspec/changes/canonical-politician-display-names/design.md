# Design: Canonical Politician Display Names

## Technical Approach

One deep module, `congress_videos/modules/politician_display_names.py`, hides catalogue loading, validation, collision detection, normalization and fallback behind a single caller function. The three consumers already hold a resolved slug, so wiring is a name substitution at each render seam — no DB read, no SELECT, no migration.

## Architecture Decisions

### D1 — Catalogue schema

**Choice**: `catalog_version` + `entries[]` with `participant_slug`, `display_name` (preferred short form), optional `full_name` (natural-order safe fallback / derivation anchor), `ambiguous`, `selection_note`, `provenance{publisher, reference_url, evidence_note, reviewed_on}`.

```json
{
  "catalog_version": 1,
  "entries": [
    {
      "participant_slug": "pedro-sanchez-perez-castejon",
      "display_name": "Sánchez",
      "full_name": "Pedro Sánchez",
      "ambiguous": false,
      "selection_note": "Party leader; surname nationally unmistakable.",
      "provenance": {
        "publisher": "Congress of Deputies",
        "reference_url": "https://www.congreso.es/busqueda-de-diputados",
        "evidence_note": "Slug cross-checked against the active-deputies register.",
        "reviewed_on": "2026-09-09"
      }
    }
  ]
}
```

**Alternatives**: `display_name` only (no `full_name`); a `style` parameter returning short/long variants.
**Rationale**: `full_name` earns its place as a *load-time* invariant, not a second return value — the normalized `display_name` must be a token subsequence of the normalized `full_name`, which mechanically enforces "never invent a shortened form". A `style` parameter would widen the interface for no caller. Provenance mirrors `institutional_roles.v1.json` verbatim so one review habit covers both catalogues.

### D2 — Module interface

**Choice**: one caller function plus a loader/catalogue pair for tests.

```python
def canonical_display_name(slug: str | None) -> str | None: ...

@dataclass(frozen=True)
class DisplayName: participant_slug: str; display_name: str; full_name: str | None; ambiguous: bool

@dataclass(frozen=True)
class DisplayNameCatalog:
    version: int
    entries: tuple[DisplayName, ...]
    def canonical_name(self, slug: str | None) -> str | None: ...

class DisplayNameCatalogLoader:
    def __init__(self, path: Path | str) -> None: ...
    def load(self) -> DisplayNameCatalog: ...   # raises CatalogValidationError
```

`canonical_display_name` is the only thing a consumer learns: lazy module singleton (mirrors `speaker_normalization._get_role_catalog`), never raises, returns `None` for a `None`/blank/unmapped/ambiguous slug or a catalogue that failed to load. The normalizer stays private — unlike role labels, catalogue `display_name` is human copy, never a normalized key.

**Alternatives**: exporting `Catalog`/`CatalogLoader` under the template's generic names.
**Rationale**: `DisplayName*` prefixes prevent two same-named `Catalog`/`CatalogLoader` classes colliding at import sites in `congress_videos/modules/`.

**Correction to the proposal's risk table**: a lazy singleton plus a never-raising resolve means `CatalogValidationError` does **not** fail DAG import. `canonical_display_name` catches it, logs ERROR once (cached load attempt, no per-call log spam), and returns `None`. The loud gate is the bundled-catalogue test in CI. Eager module-level loading was rejected: a typo in cosmetic naming data must never take the uploader DAG down.

### D3 — `CatalogValidationError`: import, do not redefine

| Option | Tradeoff | Decision |
|---|---|---|
| Import from `institutional_role_resolver` | Couples to a sibling domain module | **Chosen** |
| Redefine locally | Two same-named exceptions in one package; `except CatalogValidationError` silently misses one | Rejected |
| Extract to shared `catalog_errors.py` | Cleanest naming, but a two-module refactor plus a compat re-export for one existing test | Deferred |

**Rationale**: the name is already catalogue-generic and repo-unique (single definition, `institutional_role_resolver.py:15`), and the source module imports only stdlib, so there is no cycle and no import cost. Sharing the *type* has a correctness payoff (one `except` covers both catalogues); the 4-line normalizer is *not* shared — it is reimplemented as private `_normalize_display_name` because `normalize_role_label` names the wrong domain and person-name normalization must be free to diverge (e.g. nobiliary particles) without touching role resolution. Extract the shared module when a third catalogue arrives — two adapters make a real seam.

### D4 — Collision rule at load time

Hard-fail (`CatalogValidationError`) on: non-dict document; `catalog_version != 1`; `entries` not a list; any entry missing/blank `participant_slug` or `display_name`; non-bool `ambiguous`; blank `selection_note`; provenance failing the same checks as `_provenance_error` (all four fields present and non-blank, `reference_url` scheme `http`/`https` with a netloc, `reviewed_on` an ISO date); `display_name` not a token subsequence of `full_name` when `full_name` is present.

Collision:
- **Duplicate `participant_slug`** — checked across **all** entries, ambiguous included: two rows for one person is always an authoring error.
- **Colliding `display_name`** — two entries whose `_normalize_display_name` output is equal, checked over the **resolvable set only** (`ambiguous == false`): an ambiguous entry never renders, so it cannot collide in output.

Unlike the role catalogue's per-assignment soft `diagnostic`, every defect here is hard. That catalogue tolerates partial validity because assignments carry time intervals; this one is a pure editorial lookup where a defect means an editor typo that must be visible in CI, and the `None` fallback already makes an *absent* mapping safe.

### D5 — The three wiring points

| # | Seam | Signature change | Passes |
|---|---|---|---|
| 1 | `thumbnail_generation.generate_title` → `_build_title_prompt` | both gain `participant_slug: str \| None = None` | when `canonical_display_name(slug)` returns a name, it replaces the first entry of `_real_speakers(key_speakers)` in `THUMBNAIL_TITLE_SPEAKERS_INSTRUCTION`'s `speaker_list`; otherwise the list is byte-identical to today |
| 1' | `generic_thumbnail_generator_dag._task_generate_title` | — | `participant_slug=conf.get("slug")` (`validate_input` returns conf unchanged, so `youtube_upload_dag.trigger_thumbnail_generation`'s `child_conf["slug"]` reaches it) |
| 2 | `thumbnail_generation.resolved_photo_speaker_name` | gains `participant_slug: str \| None = None` | canonical name wins over `_real_speakers(...)[0]`; the `source == "photo"` gate is unchanged, so an unmapped slug is byte-identical |
| 2' | `generic_thumbnail_generator_dag._task_art_direction` | — | `resolved_photo_speaker_name(photo_data, conf.get("key_speakers"), conf.get("slug"))` |
| 3 | `reap_shorts_uploader_dag.build_shorts_metadata_context` | none | at `L104-117`, `canonical_display_name(turn_speaker_slug)` is tried before `participants_lookup`; `speaker_display_name` falls through to today's `participant["display_name"]` on `None`. Mentioned people are **not** canonicalised — a bare surname is only safe for the subject the thumbnail is about |

Consistency (spec "Same slug, same name across seams") holds by construction: seams 1 and 2 read the same `conf["slug"]` through the same function.

### D6 — Replacement wording for the shorts taxonomy

`SHORTS_METADATA_SYSTEM_PROMPT` lines 13–20 are replaced by:

```python
"SIEMPRE incluye al político principal en el título escribiendo su nombre EXACTAMENTE como aparece "
"en PONENTE PRINCIPAL, sin acortarlo, sin abreviarlo y sin sustituirlo por un apodo: ese campo ya "
"llega con la forma pública correcta. "
"Si PONENTE PRINCIPAL está vacío o es desconocido, identifica al político por su cargo o rol y no "
"inventes ningún nombre propio."
```

**Also required, and missed by the proposal**: `SHORTS_METADATA_USER_PROMPT_TEMPLATE` line 46 reads `usando la taxonomía de 4 niveles del sistema` — a dangling reference once the system prompt changes. Replace with:

```
- OBLIGATORIO: incluye al político principal escribiendo "{primary_speaker}" tal cual, sin acortarlo
```

Line 47 (`Si "{primary_speaker}" está vacío o es desconocido, usa el cargo/rol...`) already covers the unmapped case and stays.

Assertable in `TestPromptTemplates`: no `"Nivel 1"`/`"Nivel 2"`/`"Nivel 3"`/`"Nivel 4"`; no proper politician name (matching the convention stated at `ai_prompts.py:246-248`); `"EXACTAMENTE como aparece"` and `"cargo o rol"` both present; `"taxonomía de 4 niveles"` absent from the user template.

## Data Flow

```
identity resolution ──► participant_slug
                            │
        ┌───────────────────┼────────────────────┐
        │                   │                    │
  conf["slug"]        conf["slug"]     turn_speaker_slug
   (title)          (art direction)        (shorts)
        │                   │                    │
        └────────► canonical_display_name(slug) ─┘
                            │
              str ─────────────────────── None
               │                            │
      curated short form           existing full-name behaviour
```

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/modules/politician_display_names.py` | Create | Loader, frozen dataclasses, `canonical_display_name` |
| `congress_videos/catalogs/politician_display_names.v1.json` | Create | Curated entries |
| `congress_videos/modules/thumbnail_generation.py` | Modify | `_build_title_prompt`, `generate_title`, `resolved_photo_speaker_name` |
| `congress_videos/generic_thumbnail_generator_dag.py` | Modify | `_task_generate_title`, `_task_art_direction` |
| `congress_videos/reap_shorts_uploader_dag.py` | Modify | `build_shorts_metadata_context` |
| `congress_videos/config/ai_prompts.py` | Modify | System prompt + user template taxonomy removal |
| `docs/CANONICAL_DISPLAY_NAMES.md` | Create | Ownership, criterion, cadence, edit procedure |
| `tests/congress_videos/test_politician_display_names.py` | Create | Clone `test_institutional_role_resolver.py` structure |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Modify | Title + art-direction wiring, cross-seam consistency |
| `tests/congress_videos/test_reap_uploader_dag.py` | Modify | Extend `TestBuildShortsMetadataContext` and `TestPromptTemplates` |

**Correction**: the proposal plans a new `test_reap_shorts_uploader_dag.py`. That file must **not** be created — `reap_shorts_uploader_dag` is already tested by `tests/congress_videos/test_reap_uploader_dag.py`, which already holds `TestBuildShortsMetadataContext` (L957) and `TestPromptTemplates` (L436). A second file would split one module's coverage across two.

## Testing Strategy

Strict TDD, `uv run pytest`, one seam per cycle.

| Layer | What | Approach |
|---|---|---|
| Unit — catalogue | Mapped/unmapped/`None`/blank slug; `ambiguous` → `None`; accented slug; duplicate slug; colliding short form; `display_name` not derived from `full_name`; bad `reference_url`; unparsable `reviewed_on`; version ≠ 1 | `tmp_path` fixture catalogues + `pytest.raises(CatalogValidationError, match=...)`, mirroring `test_institutional_role_resolver.py` |
| Unit — resilience | Missing/corrupt bundled catalogue → `canonical_display_name` returns `None` and never raises; error logged once | monkeypatch the catalogue path, reset the singleton |
| Unit — bundled data | Every bundled entry validates; no collisions; `reviewed_on` parses | load the real `politician_display_names.v1.json` |
| Unit — wiring | Title/art-direction prompts carry the canonical name; unmapped slug leaves the prompt byte-identical to today | assert on built prompt strings |
| Unit — consistency | One mapped slug → identical name in the title and art-direction prompts | single test crossing both seams |
| Unit — shorts | `speaker_display_name` canonicalised; unmapped/`None` slug byte-identical; mentioned people never canonicalised | extend `TestBuildShortsMetadataContext` |
| Unit — prompts | Taxonomy strings absent, replacement strings present, in both prompt constants | extend `TestPromptTemplates` |
| E2E | DAG import errors stay zero | `bash scripts/test-airflow-e2e.sh` (touches `congress_videos/**`) |

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification, or process-integration boundary. The change adds one local read-only UTF-8 JSON read and text substitution inside existing prompt builders.

## Migration / Rollout

No migration. No persisted state. Rollback per slice is a commit revert; full rollback without a code revert is emptying `entries` — every slug then resolves to `None` and all three consumers resume today's behaviour.

## Slice Boundary (stacked-to-main, ≤400 changed lines each)

| # | Slice | Est. | Independently safe because |
|---|---|---|---|
| 1 | Module + loader + fixture-driven tests + 1-entry bundled catalogue | ~350 | No consumer imports it yet |
| 2 | Populate catalogue to the initial roster + bundled-catalogue invariant test | ~170 | Pure data; a reviewer with editorial context reviews it alone |
| 3 | Long-form title wiring + tests | ~150 | No-op until a slug is catalogued |
| 4 | Long-form art-direction wiring + tests + cross-seam consistency test | ~160 | Same; depends on slice 3 only for the shared consistency test |
| 5 | Shorts wiring + extend `test_reap_uploader_dag.py` | ~180 | Falls through to `participants_lookup` on `None` |
| 6 | Prompt taxonomy removal (system + user template) + prompt assertions | ~90 | Copy-only; independently revertible |
| 7 | `docs/CANONICAL_DISPLAY_NAMES.md` | ~120 | Docs only |

Splitting the proposal's slice 1 into code (1) and data (2) keeps both under budget and separates loader review from editorial roster review. Slice 6 is split out of the shorts slice so the behaviour change for *unmapped* speakers can be reverted without reverting the wiring.

Decision needed before apply: No
Chained PRs recommended: Yes
400-line budget risk: Medium

## Open Questions

- [ ] Initial roster size for slice 2 is uncapped by the proposal. Recommendation: cap at 10–15 entries so slice 2 stays under budget and every entry gets real editorial scrutiny.
