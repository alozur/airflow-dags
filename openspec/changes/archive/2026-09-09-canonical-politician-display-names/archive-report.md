# Archive Report: Canonical Politician Display Names (Issue #511)

**Archive Date**: 2026-09-09
**Change Name**: `canonical-politician-display-names`
**GitHub Issue**: #511
**Worktree**: `/home/alozur/src/github.com/alozur/airflow-dags-wt-511`
**Branch**: `feat/511-canonical-politician-display-names`
**HEAD at Archive**: `422d77a` (merged into `origin/dev`)

## Final State Summary

The change `canonical-politician-display-names` (issue #511) is complete and archived. All 7 implementation phases plus 2 corrective work units have been merged into `origin/dev`. The change introduces a curated, versioned catalogue that maps resolved politician slugs to consistent public display names, used by title generation, art direction, and shorts metadata systems.

**Status**: COMPLETE
**All Tasks**: 18/18 Done
**Verification**: PASS (0 CRITICAL, 0 WARNING, 1 SUGGESTION for manual e2e on Docker host)
**Spec Coverage**: 18/18 scenarios passing
**Test Suite**: 4932 passed / 34 skipped

## Artifact Traceability

All artifacts were persisted to Engram during the SDD phase cycle. Observation IDs recorded below for complete traceability:

| Artifact | Engram Observation ID | Type | Created | Topic Key |
|----------|----------------------|------|---------|-----------|
| Proposal | #2626 | architecture | 2026-09-09 03:49:48 | `sdd/canonical-politician-display-names/proposal` |
| Specification | #2627 | architecture | 2026-09-09 03:52:22 | `sdd/canonical-politician-display-names/spec` |
| Design | #2630 | architecture | 2026-09-09 03:55:41 | `sdd/canonical-politician-display-names/design` |
| Tasks | #2634 | architecture | 2026-09-09 04:01:03 | `sdd/canonical-politician-display-names/tasks` |
| Apply Progress | #2637 | architecture | 2026-09-09 04:07:55 | `sdd/canonical-politician-display-names/apply-progress` |
| Verify Report | #2643 | architecture | 2026-09-09 05:06:16 | `sdd/canonical-politician-display-names/verify-report` |

## Implementation Summary

### Delivered Capabilities

**New Capability**: Canonical Display Names Catalogue
- **Module**: `congress_videos/modules/politician_display_names.py` (loader + interface)
- **Catalogue**: `congress_videos/catalogs/politician_display_names.v1.json` (11-entry roster)
- **Consumers**: Title generation, art direction, shorts metadata
- **Fallback**: Never-raising interface; transparent fallback to existing names when slug absent/unmapped/ambiguous

### Implementation Phases

All 7 primary phases plus 2 corrective units complete:

1. **Phase 1**: Catalogue module + loader + dataclasses + 1-entry test catalogue (PR #521, ~350 lines)
2. **Phase 2**: Populate 11-entry roster with politicians (PR #524, ~170 lines)
3. **Phase 3**: Wire title generation to consult catalogue (PR #527, ~150 lines)
4. **Phase 4**: Wire art direction to consult catalogue + cross-seam consistency test (PR #530, ~160 lines)
5. **Phase 5**: Wire shorts metadata to consult catalogue (PR #532, ~180 lines)
6. **Phase 5b** (Corrective): Fix mentioned-people dedup to use raw (pre-canonical) speaker name (inline with PR #532)
7. **Phase 6**: Remove hardcoded 4-level Spanish naming taxonomy from prompts (PR #535, ~90 lines)
8. **Phase 7**: Write governance documentation in Spanish (PR #536, ~120 lines)
9. **Phase 7b** (Corrective): Translate governance documentation to match sibling docs' language (inline with PR #536)

### Key Architectural Decisions

1. **CatalogValidationError reuse**: Imported from `institutional_role_resolver.py` to avoid duplicate exception definitions
2. **Lazy-singleton pattern**: `_get_catalog()` caches both success and failure; never-raising interface ensures DAG import never fails on catalogue corruption
3. **Load-time ambiguity detection**: Collision checking happens at load time (no live DB queries); duplicate slugs and colliding normalized surnames rejected
4. **Fallback-transparent design**: Consumers continue to use existing full names when catalogue unavailable/unmapped; zero behavior change in failure paths
5. **Cross-seam consistency**: Title and art-direction both call same `canonical_display_name()` function, guaranteeing identical display for one slug
6. **Dedup preservation**: Shorts metadata dedup comparison uses raw (uncanonicalized) speaker name to maintain correctness when catalogue shortens the name

### Verified Artifacts

- **New module**: `congress_videos/modules/politician_display_names.py` (loader, dataclasses, interface)
- **Catalogue data**: `congress_videos/catalogs/politician_display_names.v1.json` (11 entries, no collisions, real provenance)
- **Title wiring**: `congress_videos/modules/thumbnail_generation.py` + `generic_thumbnail_generator_dag.py`
- **Art-direction wiring**: Same files + cross-seam test
- **Shorts wiring**: `congress_videos/reap_shorts_uploader_dag.py` (canonical lookup + dedup fix)
- **Prompt copy**: `congress_videos/config/ai_prompts.py` (taxonomy fully removed, D6 wording verbatim)
- **Governance doc**: `docs/CANONICAL_DISPLAY_NAMES.md` (Spanish, complete ownership/cadence/procedure documentation)
- **Tests**: All covered by `tests/congress_videos/test_politician_display_names.py`, related thumbnail tests, reap uploader tests, doc tests

## Specification Verification

All 8 requirements with 18 scenarios verified passing:

| Requirement | Scenarios | Status |
|-------------|-----------|--------|
| Versioned Catalogue Schema | 2 | PASS |
| Load-Time Ambiguity Enforcement | 3 | PASS |
| Never-Raising Resolution | 4 | PASS |
| Resolution Only After Identity Resolution | 3 | PASS |
| Safe Fallback to Existing Behavior | 2 | PASS |
| Neutral Instruction Replaces Prose Taxonomy | 2 | PASS |
| Cross-Consumer Name Consistency | 1 | PASS |
| Catalogue Governance Documentation | 1 | PASS |

**Total**: 18/18 scenarios passing

## Quality Metrics

### Test Execution

- **Full pytest run**: 4932 passed / 34 skipped (baseline-exact match)
- **Ruff check**: Exit 0 (no style violations)
- **Ruff format**: Exit 0 (already formatted)
- **E2E smoke test**: UNAVAILABLE (Docker unreachable in sandbox; documented substitute: `airflow dags list-import-errors` on NAS after git_sync)

### Code Quality

- **Reused code**: `CatalogValidationError` from `institutional_role_resolver` (shared definition, no duplication)
- **Lazy loading**: Singleton pattern matches `speaker_normalization._get_role_catalog` (proven pattern in repo)
- **Never-raising guarantee**: Tested with 2 failure modes (missing file, bad catalogue_version) + caplog assertions proving log-once behavior
- **Collision detection**: Load-time enforcement with test cases for duplicate slug, colliding surname, ambiguous-flag skip
- **Cross-seam consistency**: Dedicated test asserting title and art-direction use identical names for one mapped slug

### Deliverable Quality

- **Line count**: Total ~1220 lines across 7 slices, well-distributed, 2 slices carried `size:exception` label (#521 slice 1)
- **Test coverage**: Unit tests for loader validation, integration tests for consumer wiring, functional tests with real catalogue data
- **Documentation**: Governance doc covers ownership, selection criterion, quarterly-plus-reshuffle cadence, add/edit procedure

## Final Verification

Per the sdd-verify PASS report (Engram #2643):
- All 18 spec scenarios trace to passing tests ✓
- All 18 implementation tasks marked complete ✓
- Full suite matches baseline (4932 passed / 34 skipped) ✓
- Linting clean (ruff check + format) ✓
- E2E smoke test substitute documented ✓
- No CRITICAL or WARNING issues ✓
- One SUGGESTION: run Docker e2e on host with Docker before promoting to main ✓

## Known Limitations & Future Work

### No Blockers

The change is production-ready. One governance step (manual Docker e2e test on host) is documented and expected before production deploy.

### Quarterly Review Cadence

Next review: 2026-12-09 (after potential election/reshuffle)
- Verify all 11 entries remain current
- Add new important politicians meeting ≥2 appearance threshold
- Update provenance `reviewed_on` dates

### Future Extensions

1. Multi-language display names (schema: `display_names: { es: "...", en: "..." }`)
2. Temporal validity tracking (`valid_from` / `valid_until` for reshuffles)
3. External catalogue sources (API fetch + cache instead of bundled JSON)

## Archive Contents

Archived to: `openspec/changes/archive/2026-09-09-canonical-politician-display-names/`

Contains:
- [x] `proposal.md` — problem statement, approach, slice plan
- [x] `design.md` — architecture, key decisions, testing strategy
- [x] `tasks.md` — 7 phases + 2 corrective work units, all marked done
- [x] `verify-report.md` — PASS status, 18/18 scenarios, full test results
- [x] `specs/canonical-display-names/spec.md` — 8 requirements with 18 scenarios
- [x] `archive-report.md` — this document

## Final State Authority

This archive report records the state of the change AT CLOSE OF ARCHIVE, not at earlier intermediate points. The change is **COMPLETE**:

- ✓ All 7 implementation phases complete and merged into `origin/dev`
- ✓ 2 corrective work units (dedup-key fix, doc language alignment) complete
- ✓ Verification PASS with all 18 spec scenarios passing
- ✓ All 18 implementation tasks marked done in tasks.md
- ✓ No CRITICAL or WARNING issues
- ✓ No outstanding blockers

The next phase is merge to `main` (orchestrator handles this). Production deployment follow-up: verify with `airflow dags list-import-errors` on NAS after git_sync reaches production.

## Engram Integration

Archive report persisted to Engram with topic key `sdd/canonical-politician-display-names/archive-report` for cross-session reference and traceability audit trail.
