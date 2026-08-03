# Proposal: congreso.es Official-Photo Fallback for Participants Sync

## Intent

`congress_participants_sync` fills only 26/350 `photo_url` values (~7%). All 26 come
from prior Wikidata runs; the SPARQL fuzzy-match misses the majority of current deputies
(incomplete Wikidata coverage, temporal-qualifier gaps, missing `P18` images). The
enrichment task runs correctly — the source is simply insufficient. Add a deterministic
congreso.es official-photo fallback (Option C) so still-null rows are filled from the
official portal, targeting ~100% coverage for active deputies.

## Scope

### In Scope
- New DAG task `t4` (`fill_congreso_photo_fallback`) wired `t3 >> t4`.
- One bulk `searchDiputados` POST per run → `dict[normalized_name → codParlamentario]`.
- Runtime photo-URL construction: `.../{codParlamentario}_{LEGISLATURE_ID}.jpg` (leg=15).
- New enrichment function in `participants_enrichment.py` (keep file < 800 lines).
- Graceful 404 / WAF / missing-key handling (log + skip, never fail the run).
- New constants: `CONGRESO_SEARCH_DIPUTADOS_URL`, `CONGRESO_PHOTO_URL_TEMPLATE`.
- Tests + `sample_search_diputados.json` fixture; update DAG task-count assertion (3→4).

### Out of Scope
- Changing the Wikidata/Commons SPARQL path or its 0.90 fuzzy threshold.
- Changing `opendataExport` ingestion of biography/dates.
- Any new DB column or migration for `codParlamentario`.
- Re-scraping the dead directory index.

## Capabilities

### New Capabilities
- None

### Modified Capabilities
- `congress-participants-enrichment`: adds a second, deterministic fallback pass that
  fills `photo_url` for rows still NULL after Wikidata, sourced from the congreso.es
  official photo endpoint.

## Approach

Option C — keep `opendataExport` for biography/dates and Wikidata as the primary photo
source. Add a fallback pass: fetch `codParlamentario` at runtime via one bulk
`searchDiputados` POST, join to records by the existing `normalize_member_name`,
construct the deterministic photo URL, and write via the existing `update_photo_url`
(`WHERE photo_url IS NULL`) guard — so Commons URLs survive and congreso.es fills only
the remainder. No DB priority logic; the null-guard already enforces ordering.

## Affected Areas

| Area | Impact | Description |
|------|--------|-------------|
| `congress_videos/config/constants.py` | Modified | Add search + photo-URL-template constants |
| `congress_videos/modules/participants_enrichment.py` | Modified | Add `fill_congreso_photo_fallback()` |
| `congress_videos/congress_participants_sync_dag.py` | Modified | Add `t4`, wire `t3 >> t4` |
| `tests/.../test_participants_enrichment.py` | Modified | Fallback fetch/join/update tests |
| `tests/.../test_congress_participants_sync_dag.py` | Modified | Task count 3→4; `t4` wiring |
| `tests/fixtures/sample_search_diputados.json` | New | `searchDiputados` response fixture |

## Risks

| Risk | Likelihood | Mitigation |
|------|------------|------------|
| Live `searchDiputados` schema unverified (needs Spain egress) | High | Design must specify defensive key handling; fixture from known portlet shape; verify against live endpoint in Airflow |
| Name-join mismatch between endpoints | Med | Apply same `normalize_member_name` on both sides |
| Photo URL 404 / WAF block | Med | Tolerate 404 (log + skip); reuse browser UA; never fail the run |
| `LEGISLATURE_ID=15` hardcoded | Low | Acceptable for current legislature scope |

## Rollback Plan

Fallback is additive and isolated in `t4`. Revert by removing the `t4` task/wiring, the
`fill_congreso_photo_fallback()` function, and the two constants. No DB migration exists,
so no schema rollback is needed; already-written congreso.es URLs remain valid photos.

## Dependencies

- Live access to congreso.es `searchDiputados` portlet from the Airflow runtime (Spain egress).

## Success Criteria

- [ ] After one sync run, `photo_url` coverage approaches ~100% for active deputies whose
      official photo exists at the deterministic URL.
- [ ] `t4` fills only rows NULL after Wikidata; existing Commons URLs are never overwritten.
- [ ] Missing `codParlamentario`, 404s, and WAF blocks log-and-skip without failing the run.
- [ ] New tests + fixture pass; DAG import-error check stays clean.
