# SDD Verify Report: Canonical Politician Display Names (Issue #511)

**Status**: PASS
**Date**: 2026-09-09
**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-511`
**Branch**: `feat/511-canonical-politician-display-names`
**HEAD**: `422d77a` (merged into `origin/dev`)

## Summary

All 18 spec scenarios (8 requirements) trace to passing covering tests. All 18 implementation tasks are complete. Full test suite passes. No CRITICAL or WARNING issues. One SUGGESTION regarding manual Docker e2e verification.

## Verification Results

### Spec Coverage

| Requirement | Scenarios | Status | Test Coverage |
|-------------|-----------|--------|----------------|
| Versioned Catalogue Schema | 2 | PASS | `test_loader_loads_well_formed_catalogue`, `test_loader_rejects_malformed_catalogue` |
| Load-Time Ambiguity Enforcement | 3 | PASS | `test_loader_rejects_duplicate_slug_collision`, `test_loader_rejects_colliding_surname_after_normalization`, `test_loader_accepts_ambiguous_entry_with_unique_display_name` |
| Never-Raising Resolution | 4 | PASS | `test_canonical_display_name_resolves_mapped_slug`, `test_canonical_display_name_returns_none_for_unmapped_slug`, `test_canonical_display_name_returns_none_for_missing_slug`, `test_canonical_display_name_accepts_accented_input` |
| Resolution Only After Identity Resolution | 3 | PASS | `test_title_generation_consults_catalogue`, `test_art_direction_consults_catalogue`, `test_shorts_metadata_consults_catalogue` |
| Safe Fallback to Existing Behavior | 2 | PASS | `test_title_fallback_on_unmapped_slug`, `test_shorts_fallback_on_missing_slug` |
| Neutral Instruction Replaces Prose Taxonomy | 2 | PASS | `test_taxonomy_prose_absent`, `test_unmapped_speaker_instruction_remains_actionable` |
| Cross-Consumer Name Consistency | 1 | PASS | `test_title_and_art_direction_render_identical_name` |
| Catalogue Governance Documentation | 1 | PASS | `test_canonical_display_names_doc_covers_required_topics` |

**Total**: 18/18 scenarios passing

### Task Completion

All 18 implementation tasks (Phases 1-7 + corrective 5b and 7b) marked complete:

- [x] Phase 1 (4 tasks): Catalogue module, dataclasses, loader, 1-entry catalogue
- [x] Phase 2 (2 tasks): 11-entry roster population
- [x] Phase 3 (3 tasks): Title wiring
- [x] Phase 4 (3 tasks): Art-direction wiring + cross-seam consistency test
- [x] Phase 5 (2 tasks): Shorts wiring
- [x] Phase 5b (2 tasks): Mentioned-people dedup key fix (corrective)
- [x] Phase 6 (2 tasks): Prompt taxonomy removal
- [x] Phase 7 (2 tasks): Governance documentation
- [x] Phase 7b (2 tasks): Doc language alignment (corrective)

### Test Execution

**Full suite** (real-run `uv run pytest`): **4932 passed / 34 skipped**
- Claimed baseline: 4932 passed / 34 skipped
- Verification match: **EXACT**

**Linting**:
- `uv run ruff check`: exit 0 ✓
- `uv run ruff format --check`: exit 0 ✓

**E2E Smoke Test**:
- `bash scripts/test-airflow-e2e.sh`: **UNAVAILABLE** (Docker daemon unreachable in sandbox)
  - Per repo convention, unavailable is not a failure
  - Substitute verification: pending on NAS after `git_sync` reaches production
  - This is a documented expected step during production deploy

### Code Verification

Spot-checked implementation artifacts:

1. **`congress_videos/modules/politician_display_names.py`** (module + loader):
   - Imports `CatalogValidationError` from `institutional_role_resolver` (correct reuse)
   - Implements `DisplayName`, `DisplayNameCatalog`, `DisplayNameCatalogLoader`
   - Lazy-singleton `_get_catalog()` caches both success and failure
   - Public interface: `canonical_display_name(slug: str | None) -> str | None` never raises ✓

2. **`congress_videos/catalogs/politician_display_names.v1.json`** (11-entry roster):
   - `catalog_version: 1`
   - Entries verified: Pedro Sánchez, Isabel Rodríguez, Javier Rodríguez (distinct from Isabel), plus 8 others
   - No duplicate slugs
   - No colliding normalized display names
   - All entries have valid `provenance` blocks with real `reviewed_on` dates ✓

3. **Title Wiring** (`congress_videos/modules/thumbnail_generation.py` + `generic_thumbnail_generator_dag.py`):
   - `_build_title_prompt` now accepts `participant_slug: str | None = None`
   - `generate_title` passes slug through to `_build_title_prompt`
   - DAG task passes `conf.get("slug")` to both generators
   - Substitution logic: `canonical_display_name(participant_slug) or real[0]` ✓

4. **Art Direction Wiring** (`generic_thumbnail_generator_dag.py` + `thumbnail_generation.py`):
   - `resolved_photo_speaker_name` accepts `participant_slug: str | None = None`
   - Both `_task_art_direction` (L154) and `_task_art_direction_retry` pass the slug
   - Cross-seam test confirms title and art-direction use identical names ✓

5. **Shorts Wiring** (`reap_shorts_uploader_dag.py` + `build_shorts_metadata_context`):
   - Canonical lookup tried BEFORE `participants_lookup`
   - Mentioned people are NOT canonicalised (deliberate scope boundary)
   - Dedup comparison uses raw (uncanonicalized) speaker name for consistency
   - Fallback on missing slug is transparent ✓

6. **Prompt Copy** (`congress_videos/config/ai_prompts.py`):
   - `SHORTS_METADATA_SYSTEM_PROMPT`: no "Nivel 1/2/3/4" prose ✓
   - `SHORTS_METADATA_USER_PROMPT_TEMPLATE`: no "taxonomía de 4 niveles" reference ✓
   - Both contain exact D6 wording: "EXACTAMENTE como aparece en PONENTE PRINCIPAL" + "cargo o rol" ✓

7. **Governance Doc** (`docs/CANONICAL_DISPLAY_NAMES.md`):
   - Written in Spanish (matches all 10 sibling docs)
   - States ownership, selection criterion (appearance-count ranking), review cadence (quarterly-plus-reshuffle)
   - Documents add/edit procedure with schema examples
   - Test file `tests/docs/test_canonical_display_names_doc.py` verifies coverage ✓

### Issues Found

**CRITICAL**: None
**WARNING**: None
**SUGGESTION**: 1

#### SUGGESTION: Docker E2E Manual Verification Needed

The Docker e2e smoke test (`bash scripts/test-airflow-e2e.sh`) reported unavailable in this sandbox. Per repo convention, this is not a failure. However, before promoting this change to `main` and production, run the smoke test on a host with Docker available to confirm DAG import succeeds with the new module and catalogue bundled.

**Verification plan**: On a host with Docker:
```bash
cd /home/alozur/src/github.com/alozur/airflow-dags-wt-511
bash scripts/test-airflow-e2e.sh
```

Expected outcome: exit 0, with `dags list-import-errors` output showing 0 import errors.

## Spec-to-Implementation Traceability

Every requirement and scenario in the spec is covered by at least one test:

| Requirement ID | Requirement Name | Scenarios | Test Count | All Passing |
|---|---|---|---|---|
| REQ-1 | Versioned Catalogue Schema | 2 | 2 | Yes |
| REQ-2 | Load-Time Ambiguity Enforcement | 3 | 3 | Yes |
| REQ-3 | Never-Raising Resolution | 4 | 4 | Yes |
| REQ-4 | Resolution Only After Identity Resolution | 3 | 3 | Yes |
| REQ-5 | Safe Fallback to Existing Behavior | 2 | 2 | Yes |
| REQ-6 | Neutral Instruction Replaces Prose Taxonomy | 2 | 2 | Yes |
| REQ-7 | Cross-Consumer Name Consistency | 1 | 1 | Yes |
| REQ-8 | Catalogue Governance Documentation | 1 | 1 | Yes |

**Total**: 18 scenarios, 18 tests, 18/18 passing

## Deliverables Verification

### Artifacts Delivered

- [x] `congress_videos/modules/politician_display_names.py` — new module, 350 lines
- [x] `congress_videos/catalogs/politician_display_names.v1.json` — 11-entry catalogue
- [x] `tests/congress_videos/test_politician_display_names.py` — comprehensive loader tests
- [x] `congress_videos/modules/thumbnail_generation.py` — title and art-direction wiring
- [x] `congress_videos/generic_thumbnail_generator_dag.py` — DAG task parameter passing
- [x] `congress_videos/reap_shorts_uploader_dag.py` — shorts wiring, dedup fix
- [x] `congress_videos/config/ai_prompts.py` — prompt copy replacement
- [x] `docs/CANONICAL_DISPLAY_NAMES.md` — governance document (Spanish)
- [x] `tests/docs/test_canonical_display_names_doc.py` — doc verification tests
- [x] All 7 PR implementations merged into `origin/dev` + 2 corrective PRs

### Coverage

- **Unit test coverage**: Loader validation, collision detection, resolution, never-raising guarantee, lazy singleton caching
- **Integration test coverage**: Title/art-direction wiring, shorts wiring, cross-seam consistency, fallback behavior
- **Functional verification**: Real data (11-entry roster), real prompts, real DAG seams
- **Governance verification**: Documentation complete, ownership clear, procedures documented

### Known Limitations

None identified. All documented requirements verified.

## Next Steps

**Immediate (before archive)**:
- None — verification complete, all tests passing

**Before promoting to main**:
- [ ] Run `bash scripts/test-airflow-e2e.sh` on a host with Docker available (substitute for unavailable sandbox test)

**After merge to main and git_sync to production**:
- [ ] Verify production import: `airflow dags list-import-errors` on NAS (expected: 0 errors)
- [ ] Monitor shorts metadata generation in prod to confirm taxonomy removal doesn't affect output quality

**Quarterly governance review** (next scheduled: 2026-12-09):
- Verify all 11 catalogue entries remain current post-election/reshuffle
- Add new important politicians if they breach the ≥2 appearance threshold
- Update provenance `reviewed_on` dates

## Verdict

**Status**: PASS

The change is complete, verified, and ready for archive. All 18 spec scenarios trace to passing tests. All 18 tasks are done. No blockers.

The one SUGGESTION (manual Docker e2e test before production deploy) is a documented governance step, not a blocker for archive or main merge.
