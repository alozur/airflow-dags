```yaml
schema: gentle-ai.verify-result/v1
evidence_revision: sha256:48a98c041d6920444709ae41d1e053891d096462582ad73dc8f18fe5b2f9d50e
verdict: pass_with_warnings
blockers: 0
critical_findings: 0
requirements: 8/8
scenarios: 12/12
test_command: uv run pytest -q -p no:cacheprovider -o addopts= -n auto
test_exit_code: 0
test_output_hash: sha256:8d41562a72f90577f8ecbcd5a94c3dfcb3badf3c31a6c63ff1abda5713d21edb
build_command: uv run ruff check . && uv run ruff format --check .
build_exit_code: 0
build_output_hash: sha256:44493f1df6f317eec3f75a9bab2e21c7c89e60622379fed0329e02718b316dd6
```

## Verification Report

**Change**: verify-final-copy-before-publication (issue #512)
**Version**: N/A (single spec revision, no version markers)
**Mode**: Standard

### Note on spec count mismatch

The launch instructions stated "8 requirements, 13 scenarios." The actual
retrieved spec (`specs/final-copy-verification/spec.md`) contains **8
requirements and 12 scenarios** (`rg -c '^### Requirement:'` → 8,
`rg -c '^#### Scenario:'` → 12). Per the skill's counting rule, the
envelope above uses the actual retrieved count (12), not the injected
figure. This is a documentation/count discrepancy in the launch context,
not a code defect.

### Completeness
| Metric | Value |
|--------|-------|
| Tasks total (phases 1-4, sdd-apply scope) | 32 |
| Tasks complete | 32 |
| Tasks incomplete | 0 |
| Phase 5 (post-merge deployment verification) | 0/6 — correctly OPEN, out of sdd-apply scope |

### Build & Tests Execution

**Build (CI-blocking static gates)**: PASSED
```text
$ uv run ruff check .
All checks passed!
$ uv run ruff format --check .
313 files already formatted
```

**Tests**: 5085 passed / 0 failed / 34 skipped
```text
$ uv run pytest -q -p no:cacheprovider -o addopts= -n auto
5085 passed, 34 skipped in 26.49s
```

**Coverage**: not computed with `-o addopts=` (default addopts enforce an
80% global gate, disabled for the quick full-suite run per repo convention);
not a compliance gap — this matches project convention (`CLAUDE.md`).

**E2E smoke test**: `bash scripts/test-airflow-e2e.sh` reported
`[test-airflow-e2e] Docker daemon is not reachable ... skipping e2e
(unavailable)` — Docker socket permission denied in this environment, not a
failure (matches repo convention). Substituted a local
`DagBag(dag_folder="congress_videos", safe_mode=True, include_examples=False)`
check: zero import errors, `congress_youtube_chapter_uploader` has 15 tasks
with `verify_final_copy` wired strictly between `prepare_upload_config` and
`trigger_youtube_upload`; `reap_shorts_uploader` has 6 tasks with
`verify_final_copy` wired strictly between `generate_metadata` and
`trigger_youtube_upload`. Run the real e2e script before merge, per
`tasks.md` 5.2 and `CLAUDE.md`.

### Spec Compliance Matrix

| # | Requirement | Scenario | Test | Result |
|---|---|---|---|---|
| 1 | Independent Verification Call | Verifier runs after generation, before publication | `test_youtube_upload_dag.py::test_verify_final_copy_between_prepare_upload_config_and_trigger_upload` + DagBag task-graph check (both DAGs) + `verify_final_copy()` signature (title/description/thumbnail_text as separate kwargs, `evidence` dict) | ✅ COMPLIANT |
| 2 | Verdict and Findings Schema | Consistent copy passes with no findings | `test_final_copy_verification.py::TestConsistentCopyPasses::test_pass_verdict_with_no_findings` | ✅ COMPLIANT |
| 3 | Bounded Correction Constrained to Evidence | Politician-name correction is applied and rechecked | `TestBoundedCorrection::test_evidence_backed_correction_applied_and_rechecked` | ✅ COMPLIANT |
| 3 | Bounded Correction Constrained to Evidence | Unsupported claim correction is discarded | `TestBoundedCorrection::test_unsupported_claim_correction_is_discarded_original_published` | ✅ COMPLIANT |
| 4 | Party Mismatch Detection Avoids False Positives | True positive — contradicting party | `TestFindingCategoriesPassThrough::test_party_true_positive_is_a_finding` | ✅ COMPLIANT |
| 4 | Party Mismatch Detection Avoids False Positives | False-positive avoidance — same-party variant | `TestFindingCategoriesPassThrough::test_party_same_party_variant_is_not_a_finding` + `TestPinnedPromptContent::test_system_prompt_states_the_party_variant_rule` | ✅ COMPLIANT (see note below) |
| 5 | Thumbnail Text Flagged Without Correction | Thumbnail text finding is flagged only | `TestThumbnailTextNeverCorrected::test_thumbnail_finding_recorded_corrected_cannot_carry_it` | ✅ COMPLIANT |
| 6 | Hard-Rejection Asymmetry | Turn-title reject blocks publication | `test_youtube_upload_dag.py::TestVerifyFinalCopy::test_title_reject_raises_value_error` | ✅ COMPLIANT |
| 6 | Hard-Rejection Asymmetry | Shorts description reject does not block publication | `test_reap_uploader_dag.py::TestVerifyFinalCopyShorts::test_description_reject_persists_and_publishes_fallback_without_raising` (+ `test_title_reject_also_does_not_raise`, stronger than the letter of the scenario) | ✅ COMPLIANT |
| 7 | Fallback on Unavailable or Inconclusive Verification | Verifier failure preserves existing behavior | `test_youtube_upload_dag.py::TestVerifyFinalCopy::test_inconclusive_verdict_publishes_unchanged_and_writes_nothing` + `test_reap_uploader_dag.py::TestVerifyFinalCopyShorts::test_inconclusive_verdict_publishes_unchanged_and_writes_nothing` + module-level `TestDefensiveParsingReturnsInconclusive` (6 malformed-input cases) | ✅ COMPLIANT |
| 8 | Audit Persistence | Correction persists with both original and corrected values | `TestVerifyFinalCopy::test_correction_patches_config_and_rewrites_sidecars` + `test_database_turns.py::TestRecordCopyVerificationTurn::test_params_include_all_values_in_order` | ✅ COMPLIANT |
| 8 | Audit Persistence | Idempotent re-run does not duplicate the audit write | `TestRecordCopyVerificationTurn::test_second_call_same_content_version_returns_zero_rowcount` + `TestRecordCopyVerificationShort::test_idempotent_rerun_returns_zero_rowcount` | ✅ COMPLIANT |

**Compliance summary**: 12/12 scenarios compliant.

**Note on the party-variant scenario (row 6)**: the false-positive-avoidance
rule ("PSE-EE (PSOE)" is not a contradiction of "PSOE") is inherently an LLM
judgment call — the module never implements party matching in code, only
passes the verifier's `party_name` findings through unchanged (confirmed by
reading `final_copy_verification.py`: no party-comparison logic exists
anywhere in the module). The unit test therefore only proves (a) the parser
passes a `party_name` finding through when the stubbed LLM returns one, and
(b) a stubbed "pass" response yields no findings — it cannot exercise the
actual semantic judgment. The semantic rule itself is locked by
`TestPinnedPromptContent::test_system_prompt_states_the_party_variant_rule`,
which asserts the exact "PSE-EE (PSOE)"/"PSC-PSOE" guidance text is present
in `FINAL_COPY_VERIFICATION_SYSTEM_PROMPT` — a regression-drift guard, not a
behavioral proof. This is an honest and appropriate test design given the
LLM-owned nature of the judgment, not a gap to fix.

### Correctness (Static Evidence — claims verified by direct code reading)

| # | Claim | Status | Notes |
|---|---|---|---|
| 2 | Verifier reads published values, not upstream XComs | ✅ Confirmed | `_verify_final_copy` (long-form) reads `upload_config["videos"][0]["title"/"description"]`; test `test_verifies_upload_config_values_not_thumbnail_result_xcom` proves stale `thumbnail_result`/`youtube_metadata_results` XComs are ignored. On accepted correction, `_write_orador_sidecars(output_path, verdict.title, verdict.description)` rewrites sidecars (confirmed at `youtube_upload_dag.py:1239`, test `test_correction_patches_config_and_rewrites_sidecars`). Shorts reads `shorts_metadata` entries finalized by `_generate_metadata` (post channel-footer/session-line append). |
| 3 | Persistence gates | ✅ Confirmed | `if not verdict.ok: return` before any write (no DB write on inconclusive, both seams); `corrected_title`/`corrected_description` passed as `None` when `verdict.correction_applied` is `False` (both `database.py` writers); stale-copy guard recomputes `compute_content_version` from the about-to-publish values immediately before the write and skips (logs warning, `persisted=False`) on mismatch — both seams. |
| 4 | Idempotency (rowcount==0 as success) | ✅ Confirmed symmetric | `record_copy_verification_turn` and `record_copy_verification_short` both use `UPDATE ... WHERE {key} = %s AND copy_content_version IS DISTINCT FROM %s`, return `cur.rowcount`, and callers/tests treat `0` as success on both. |
| 5 | Long-form audit keys on `output_path`, not `turn_id` | ✅ Confirmed | `record_copy_verification_turn(output_path, ...)`, `WHERE output_path = %s`, no `turn_id` in the predicate; test `test_grouped_output_path_has_no_subquery` confirms no `SELECT`/subquery narrows the match. |
| 6 | Hard-rejection asymmetry | ✅ Confirmed | Long-form: `if verdict.verdict == "reject" and any(f.field == "title" for f in verdict.findings): raise ValueError(...)` — the ONLY raise in the entire change. Description/thumbnail-only rejects fall through to the persist+XCom path, never raising. Shorts: `_verify_final_copy` never raises on any verdict (only `logging.warning` on reject); confirmed `_check_short_upload_failures` (the shorts accumulator) does not reference `shorts_copy_verification` or `copy_verification` anywhere. |
| 7 | Bounded correction | ✅ Confirmed | `run_correction_round` is straight-line code, zero loop constructs (`for`/`while` absent from the function); `MAX_CORRECTION_ROUNDS = 1` module constant; parametrized test `test_call_round_invoked_at_most_twice_on_every_branch` asserts `len(calls) <= 2` across 5 branches; `test_recheck_failure_publishes_original_not_corrected` and `test_original_never_blocked_by_a_failed_recheck_of_the_correction` confirm a failed recheck publishes the original and never retroactively rejects it. |
| 8 | Nothing degrades to `pass` | ✅ Confirmed | `_parse_round_response` returns `_ParsedRound()` (i.e. `ok=False`) for: non-dict/error-set response, non-dict `data`, unknown `verdict` token, non-list `findings`, non-dict element in `findings`, `corrected` with a key outside `{title, description}`, and `verdict=="correctable"` with no usable `corrected`. `run_correction_round` converts every `not round0.ok` into `CopyVerdict()` (default `ok=False`) — never `pass`. Parametrized test `TestDefensiveParsingReturnsInconclusive` covers 6 malformed-input shapes, all asserting `result.ok is False`. |
| 9 | Party rule / false-positive avoidance | ✅ Confirmed (prompt-owned, see matrix note above) | No party-comparison logic in code; rule lives entirely in `FINAL_COPY_VERIFICATION_SYSTEM_PROMPT`, pinned by test. |
| 10 | `canonical_display_name` consumed for short_name, raw `display_name` also present | ✅ Confirmed by code reading, ⚠️ not covered by a dedicated unit test | Both `_copy_verification_evidence` functions (`youtube_upload_dag.py:631`, `reap_shorts_uploader_dag.py:227`) set `"display_name": participant.get("display_name")  # raw` and `"short_name": canonical_display_name(slug)  # canonical (#511)` for both the speaker and every mentioned person. No test in either DAG's test file asserts this mapping directly (see WARNING below). |
| 11 | Migration 050 shape | ✅ Confirmed | DOWN block: every non-blank line after `-- DOWN` starts with `--` (verified by reading the file and by `test_every_line_after_down_marker_is_a_comment`); `speaker_turn_videos.copy_verified_at` is `TIMESTAMPTZ`, `video_shorts.copy_verified_at` is `TIMESTAMP` (per-table convention, not unified); `copy_thumbnail_text` only on `speaker_turn_videos`; all 17 columns use `ADD COLUMN IF NOT EXISTS`; `production_schema.sql` lockstep confirmed by `rg` (both tables carry the identical column set with the identical types). |
| 12 | Duplicated `_copy_verification_evidence` divergence | ✅ Confirmed: NOT diverged | Read both functions in full: identical key structure, identical `mencionados` tri-valued handling, identical canonical/raw split. The shorts version differs only in its declared signature (`chapter, turn_speaker_row` dicts instead of `db, chapter_id, turn_id` — it takes pre-fetched data instead of querying) and the absence of the `thumbnail_text`-adjacent chapter fields that don't apply to shorts — both are documented, intentional, and match the apply-progress note. No behavioral drift found. |

### Coherence (Design)

| Decision | Followed? | Notes |
|---|---|---|
| D1 — Verdict schema, one dataclass, `ok: bool` gate | ✅ Yes | `CopyVerdict`/`CopyFinding` frozen dataclasses match the design's interfaces exactly. |
| D2 — Bounded correction, no loop, ≤2 calls | ✅ Yes | Confirmed by code reading and the parametrized bound test. |
| D3 — Content versioning / idempotent UPDATE / stale-copy guard | ✅ Yes | `compute_content_version` matches the documented canonical-JSON sha256 recipe exactly (`sort_keys=True, ensure_ascii=False`); guarded UPDATE and recompute-before-write both present at both seams. |
| D4 — Migration 050 shape | ✅ Yes | See Correctness table above. |
| D5 — Evidence assembly | ✅ Yes | Both `_copy_verification_evidence` functions match the documented key table exactly, including the tri-valued `"mencionados"` Spanish key. |
| D6 — Prompt constants | ✅ Yes | Both prompt constants present in `ai_prompts.py`; pinned-content tests guard exact required phrases against drift. |
| D7 — Observability | ✅ Yes | `_copy_verification_problems` (long-form) reports exactly the documented four conditions; shorts DAG has no accumulator (locked decision, unchanged), confirmed by absence of any `shorts_copy_verification` reference in `_check_short_upload_failures`. |
| D8 — Slice boundary / budget | ⚠️ Partially — see WARNING | 3 of 4 slices (2a, 3, 4) exceeded the 400-line budget and were reported/accepted as `size:exception`, requiring 3 separate maintainer ledger resets across the apply phase (per `apply-progress` obs #2638). All reported honestly rather than artificially split; no functional defect. |

### Issues Found

**CRITICAL**: None.

**WARNING**:
1. No dedicated unit test exercises `_copy_verification_evidence` in either DAG file to directly assert the canonical/raw split (`short_name = canonical_display_name(slug)`, `display_name` = raw roster value) for the speaker or for mentioned people. The behavior is correct by code reading (confirmed above), but nothing would catch a future accidental swap of the two fields or a dropped canonicalization call — the existing DAG-level tests all inject a pre-built `CopyVerdict`/mock and never exercise this helper's internals.
2. The launch context's spec-count claim ("13 scenarios") does not match the retrieved spec (12 scenarios, verified by direct grep). The envelope above uses the correct count of 12; this is a documentation drift in the phase handoff, not a code issue.
3. `scripts/test-airflow-e2e.sh` is unavailable in this environment (Docker socket permission denied) — reported as `unavailable`, not a failure, consistent with `CLAUDE.md`'s documented convention. A `DagBag(safe_mode=True)` substitute confirms clean imports and correct task ordering on both DAGs, but the authoritative e2e script (`airflow dags list-import-errors` inside the real container stack) has not run for this change and should run manually before merge, per `tasks.md` 5.2.
4. Three of the four apply slices (2a: 743 lines, slice 3: 921 lines, slice 4: 562 lines per the ledger) exceeded the 400-line review-workload budget and were accepted as `size:exception`, each requiring a separate maintainer `sdd-attempt reset` before the next slice could proceed. This did not block correctness but is worth surfacing as a repeated pattern across this change's apply phase.

**SUGGESTION**:
1. Add a small, focused unit test (or parametrized pair, one per DAG file) asserting `_copy_verification_evidence(...)["speaker"]["short_name"] == canonical_display_name(slug)` while `["speaker"]["display_name"]` remains the raw roster value — closing WARNING 1 cheaply, and mirroring the existing `TestShortsCrossSeamDisplayNameConsistency` pattern already used elsewhere in this codebase for the same canonical/raw guarantee.
2. Since the two `_copy_verification_evidence` functions are deliberately duplicated (not shared) across DAG files, consider a lightweight cross-file parity test asserting the two functions return identically-shaped evidence dicts for equivalent inputs — this would catch any future silent divergence between the two copies without requiring a cross-module import.

### Verdict

**PASS WITH WARNINGS** — all 8 requirements and all 12 spec scenarios trace
to a passing test; the full test suite (5085 passed, 34 skipped) and both
CI-blocking `ruff` gates pass cleanly; the migration, both DAG seams, the
verifier module, and both audit writers were read directly and match the
design and the locked hard-rejection asymmetry exactly. The warnings above
are test-coverage gaps and process notes, not functional defects — none
block correctness of the shipped behavior.
