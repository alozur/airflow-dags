# Design: C901 Backlog Slice 4 (issue #272)

`next_recommended: sdd-tasks`

Base: worktree `/home/alozur/src/github.com/alozur/airflow-dags-wt-272-s4` @ `origin/main 1aa5681`. All line
numbers are 1-based against that tree and were read directly from source in this phase.

## Technical Approach

Unchanged from slices 1-3: lift statement blocks **byte-for-byte** into module-level private helpers **in the
same module**, placed **immediately above** the outer function, each carrying a
`Lifted verbatim out of <outer> (issue #272)` docstring. No new module, no signature change on any public
name, no behaviour change. Every lift is proven by `ast.dump(include_attributes=False)` equality after a
**closed set of declared normalizations**, and every new helper gets RED-first quirk tests. Entry prune +
`EXPECTED_C901_FILE_COUNT` decrement live in the **same commit** as the lift they pay for.

Redesign of a lift is out of bounds (slice-2 rev-1 precedent: a dispatch table replaced a lift, broke AST
equality and shipped a live `KeyError`).

### Complexity model used below

Ruff's mccabe counts `if` (+1 per test), each **elif** clause (+1), `for`/`while` (+1), each `except`
handler (+1), nested `def`/`class` (+1), plus 1 for the function. A bare `else` adds **0**; ternaries,
comprehension `if`s and `and`/`or` add **0**. This model was validated in-phase against five measured
baselines (12, 12, 14, 14, 19) and reproduces all five exactly. It is still a **prediction**: `sdd-apply`
MUST re-measure every before/after number with
`uvx ruff check --isolated --select C901 --config 'lint.mccabe.max-complexity=1' <file>` and treat the
measurement, not this table, as authoritative.

## Per-lift contract

Cx fires at **≥ 11**. "Norm" refers to the normalization catalogue below.

| # | PR | Outer → Cx | File | Helper(s) → Cx | Call-site replacement | Norm |
|---|---|---|---|---|---|---|
| 1 | 1 | `is_procedural_turn` 12→**6** | `congress_videos/modules/speaker_turns.py` | `_collect_pattern_spans(patterns, normalized, spans, matched_names) -> None` → **4** | two calls, one per pattern tuple | (d) |
| 2 | 1 | `extract_announcement` 12→**4** | same | `_first_named_announcement(sorted_blocks) -> tuple[str \| None, bool] \| None` → **4**; `_first_phrase_announcement(sorted_blocks) -> tuple[str \| None, bool] \| None` → **6** | `best_named = _first_named_announcement(sorted_blocks)` / `best_phrase = _first_phrase_announcement(sorted_blocks)` | (c), (f) |
| 3 | 2 | `normalize_chapter_speakers` 14→**6** | `congress_videos/modules/speaker_normalization.py` | `_apply_institutional_role_corrections(cursor, chapter_id, speakers, key_speakers, timeline, session_date, result) -> None` → **5**; `_apply_roster_resolution_step(cursor, chapter_id, dirty_names, result) -> None` → **5** | one call inside the kept `if session_date is not None:` guard; one call inside the kept `if dirty_names:` guard | none |
| 4 | 3 | `_resolve_speaker_inner` 14→**6** | `congress_videos/modules/speaker_resolution.py` | `_build_resolution_user_prompt(turn, participants, all_blocks, chapter_span, chapter_start_seconds, region_end, intro_text, turn_text, combined_text) -> str \| None` → **5**; `_validate_completion_response(turn, response, valid_slugs, region_blocks) -> dict \| None` → **6** | `user_prompt = _build_...(...)` + `if user_prompt is None: return None`; `return _validate_completion_response(...)` | (c), (e) |
| 5 | 4 | `derive_candidate_intervals` 12→**7** | `benchmarks/pyannote_diarization/candidate_intervals.py` | `_merge_active_intervals(active_intervals) -> list[list[float]]` → **3**; `_intervals_to_gaps(merged_intervals, duration, min_gap_seconds) -> list[dict[str, object]]` → **4** | `merged_intervals = _merge_active_intervals(active_intervals)`; `return _intervals_to_gaps(merged_intervals, duration, min_gap_seconds)` | (c) |
| 6 | 5 | `_prepare_turns_callable` 19→**11** | `congress_videos/speaker_turn_prepare_dag.py` | `_resolve_qa_winner(turn, participants, turn_id, primary, primary_name, promote_signal, mentions) -> tuple[dict, str \| None, str \| None, str \| None]` → **6**; `_persist_turn_resolution(db, turn, turn_id, output_path, winner, winner_name, winner_verdict, promote_signal) -> bool` → **4** | `winner, winner_name, winner_verdict, wide_slug = _resolve_qa_winner(...)`; `promoted = _persist_turn_resolution(...)` | (c) |
| 7 | 6 | `_prepare_turns_callable` 11→**9** | same | `_prepare_turn_artifacts(db, turn, turn_id, output_path) -> None` → **3** | `_prepare_turn_artifacts(db, turn, turn_id, output_path)` replaces the whole `try/except` at 438-470 | (a) ×2 |
| 8 | 1 | *(free prune)* | `congress_videos/reap_clip_preparer_dag.py` | none | none | none |

### Exact source ranges lifted

| # | Base statements moved |
|---|---|
| 1 | `speaker_turns.py` 337-341 **and** 347-351 (two byte-identical `for` statements) |
| 2 | 418 + 421-429 → helper 1; 419 + 434-443 → helper 2 |
| 3 | `speaker_normalization.py` 247-279 (the `for` body of the Step-0 guard); 287-339 (the body of `if dirty_names:`) |
| 4 | `speaker_resolution.py` 385-428; 442-502 (tail lift, includes the function's own `return {...}`) |
| 5 | `candidate_intervals.py` 63-68; 70-90 (tail lift, includes `return gaps`) |
| 6 | `speaker_turn_prepare_dag.py` 328-364; 366-402 |
| 7 | `speaker_turn_prepare_dag.py` 438-470 (whole `try/except`) |

### Verified free-variable analysis (the helper signatures above)

- **#1** reads only the loop's own names plus `normalized`; appends into the caller's `spans` /
  `matched_names` lists **in place**, so it returns nothing.
- **#2** helper 1's locals `text`, `m`, `name` die inside; only `best_named` escapes. Helper 2's `text` dies
  inside; only `best_phrase` escapes. `sorted_blocks`, `window_blocks`, `lo` stay in the caller.
- **#3** `result` is the same `NormalizationResult` object, mutated in place — never copied — so the caller's
  `dirty_names` comprehension at 283-285 still reads `result.corrections` written by Step 0. `cursor` is the
  same live cursor object inside the same `with db_conn.cursor()` block. `participants`, `resolution`,
  `match`, `raw`, `dirty` all die inside their helper.
- **#4** the two guards named as risks stay **in the caller, untouched**: `chapter_id or 0` (line 318) and
  `group_start_seconds is not None` (342-343) are both outside every lifted range. Helper 1's
  `turn_type`, `chapter_text`, `wide_context_active`, `prompt_text_for_gate`, `roster_lines`,
  `participant_roster` all die inside; only `user_prompt` escapes. Helper 2 needs `valid_slugs` and
  `region_blocks`, both computed before the lift boundary.
- **#6** `wide`/`wide_name` die inside `_resolve_qa_winner`; `wide_slug` escapes because the audit log at
  404-418 (kept in the caller) reads it. `promoted` escapes `_persist_turn_resolution` for the same log. The
  outer `except Exception` at 431 still catches anything raised inside either helper — exception semantics
  are unchanged by a call boundary.
- **#7** `trim_start`, `trim_end`, `rc` all die inside.

### Why no helper needs a further split

Every helper predicted above is ≤ 6. The tightest outer is `_prepare_turns_callable` at 9 (2 points of
headroom). If apply's live measurement puts any outer at 10 or 11, the contingency is in "Open risks".

## Normalization catalogue (closed set)

Only these six rewrites may be applied when proving equality. Anything else is a redesign and stops apply.

| Id | Rule | Where used |
|---|---|---|
| (a) | loop-exit `Continue()` → `Return(None)` | #7 (lines 457 and 470) |
| (b) | trailing `append(EXPR)` / trailing assignment → `Return(EXPR)` | **unused this slice** |
| (c) | appended trailing `Return(<names>)` | #2, #4 helper 1, #5 helper 1, #6 both |
| (d) | **parameter-alias substitution**: rename the helper's parameter `Name` back to the global `Name` it replaced before comparing | #1 only (`patterns` → `PROCEDURAL_PATTERNS`, then → `PROCEDURAL_FILLER_PATTERNS`); the helper body must match **both** base sites |
| (e) | **abort-sentinel re-check**: the call site gains `if <lhs> is None: return None` | #4 helper 1 only. Admissible because every `return None` in the lifted block is an abort and the helper's terminal value (`str.format(...)`) can never legitimately be `None`. Not an AST rewrite of the block — it is a declared *replacement statement*, checked by proof step 4. |
| (f) | **inert initialization relocation**: a `= None` initializer may move across statements that neither read nor write its target | #2 only (`best_phrase` initializer crosses the named-announcement loop). The proof script must assert the no-read/no-write property mechanically, not by inspection. |

## AST-equality proof procedure

Scratch-only script `ast_check_s4.py` under the session scratchpad, **never versioned** (slice-1/2/3
precedent). Invoked as `uv run python <scratch>/ast_check_s4.py` from the worktree.

1. Read the base file with `git show <base-ref>:<path>`; `ast.parse`; address each block by statement-index
   path (nested paths such as `7.4:12:-1`, the slice-2/3 addressing already in use).
2. Apply the declared normalizations **to the base slice only** (never to shipped code), counting and
   printing each rewrite by name. (d) is applied to the **helper** side instead, and is the only rewrite that
   touches shipped code in the comparison.
3. Strip all docstrings from both sides, then compare
   `ast.dump(ast.Module(body=block, type_ignores=[]), include_attributes=False)`. The `def` line, the
   docstring and a `(c)`-appended `return` are scaffolding, excluded from the compared block.
4. Compare the **outer function** base vs shipped modulo declared replacements: the N statements the lift
   removed must be replaced by exactly the call-site statements in the per-lift table and nothing else.
   Recorded gotcha: `if parsed is None: continue` is 4 AST statements, not 5.
5. For (f), additionally assert that the relocated initializer's target appears in neither the `Load` nor
   the `Store` name set of any statement it crosses.
6. Output one line per block: `OK <name> (verbatim)` / `OK <name> (normalized: continue->return x2)` /
   `MISMATCH <name>` plus a unified dump diff; non-zero exit on any mismatch.
7. The captured `OK` lines are pasted verbatim into apply-progress and into every PR body. **That output,
   not the script, is the audit artifact.**

## RED-first quirk tests

One test class per helper, importing the private name **inside the test body**, written and observed RED
before the helper exists. Each class must pin the listed quirk — a class that only re-tests the outer
function's happy path does not satisfy this contract.

| Helper | Quirk pinned | Test file |
|---|---|---|
| `_collect_pattern_spans` | `matched_names` de-duplicates by pattern name while `spans` keeps one entry **per match**; in-place mutation of both caller lists; returns `None` | `tests/congress_videos/modules/test_speaker_turns_procedural.py` |
| `_first_named_announcement` | returns the **first** named match in input order and stops; a later named block never overwrites it; `None` when no named match | `tests/congress_videos/modules/test_speaker_turns.py` |
| `_first_phrase_announcement` | `_RE_SU_SENORIA` is tested before `_RE_GRACIAS_SENORIA` **within the same block** (no elif); a gracias-only block still yields `(None, True)`; `None` when neither matches | same |
| `_apply_institutional_role_corrections` | `resolved is None` and `role_name == raw` both skip **without** writing a cache row; `participant_normalized_name` is `None` when `is_participant` is False; the slug is written only when currently `None` **and** `is_participant`; `result` mutated in place | `tests/congress_videos/modules/test_speaker_normalization.py` |
| `_apply_roster_resolution_step` | roster fetch failure degrades to `participants=[]` and still calls the resolver; only the first `MAX_MENTIONS_PER_CALL` mentions are sent but the loop iterates **all** `dirty_names`, so over-cap mentions get `no_match` rows; slug = first accepted match in input order, never overwritten | same |
| `_build_resolution_user_prompt` | wide template only when `QA_WIDE_CONTEXT_ENABLED and turn_type=='qa' and chapter_span is not None`; an unparseable span on a qa turn warns and falls back to the narrow template; returns `None` **only** for the announcement pre-gate, and the gate reads `chapter_text` when wide is active, `combined_text` otherwise | `tests/congress_videos/modules/test_speaker_resolution.py` |
| `_validate_completion_response` | truthy `error` **or** empty `data` → `None`; slug not in `valid_slugs` → `None`; a **string** confidence is coerced by `float()`; confidence exactly at `SPEAKER_RESOLUTION_MIN_CONFIDENCE` passes (`>=`, not `>`); `evidence` defaults to `""` and still runs the anchored evidence gate | same |
| `_merge_active_intervals` | sorts internally (unsorted input is fine); touching intervals (`start <= last_end`) merge; a fully contained interval does **not** shrink the merged end | `tests/benchmarks/test_pyannote_diarization_candidate_intervals.py` |
| `_intervals_to_gaps` | `>=` on `min_gap_seconds` (exact equality **is** emitted); the tail gap uses `duration - cursor`; 6-decimal rounding; `min_gap_seconds=0.0` emits zero-length gaps (0-valid) | same |
| `_resolve_qa_winner` | wide re-pass only when `promote_signal and QA_WIDE_CONTEXT_ENABLED`; a raising wide call falls back to primary and never propagates; a wide candidate rejected by crosscheck leaves `winner_verdict is None` so the **primary** is crosschecked afterwards; `wide_slug` is reported even when the wide candidate loses; `turn` is never mutated | `tests/congress_videos/test_speaker_turn_prepare_dag.py` |
| `_persist_turn_resolution` | verdict `"reject"` withholds **both** the DB write and the in-memory `resolved_name` patch; promotion is sticky on `promote_signal` and never re-evaluated against the winner; `promoted` stays `False` when `winner_name` is falsy even if `promote_signal` is True | same |
| `_prepare_turn_artifacts` | `rc != 0` returns early **without** `mark_turn_prepared`; any exception is swallowed (returns `None`, never raises); `mark_turn_prepared` is called last | same |

`derive_candidate_intervals` additionally gets **characterization tests first** (PR4 commit 1), green on
untouched source, derived only from literals the existing CLI tests already prove:

| Literal (CLI-proven) | Behaviour pinned |
|---|---|
| `duration=20.0`, turns `[(-2,4),(3,6),(10,12)]`, `min_gap=3.0` → `[(6,10),(12,20)]` | negative clamp, overlap merge, interior gap, tail gap, `NO_DIARIZED_SPEECH` label |
| `duration=10.0`, turns `[(4,6)]`, `min_gap=5` → `[]` | both leading (4s) and tail (4s) gaps rejected under threshold |
| `duration=10.0`, turns `[(4,2)]` | `SummaryValidationError("...end_seconds must not precede start_seconds")` |

## Delivery

Strategy `auto-chain` / `stacked-to-main`: six stacked PRs to `dev`, PR1 targeting `dev` and each later PR
targeting the previous PR's branch, then one release PR `dev -> main`. **Never merge a parent with
`--delete-branch`** (it orphans children irrecoverably — #436→#437 precedent).

| PR | Commits | Est. lines (add+del) | `pyproject.toml` edit | Counter |
|---|---|---|---|---|
| 1 | (a) free prune + `is_procedural_turn`; (b) `extract_announcement` | **~220** | delete line 132 (`reap_clip_preparer_dag.py`); line 122 → `["F841", "SIM102", "SIM108"]` | 13 → 12 → 11 |
| 2 | one | **~280** | delete line 119 | 11 → 10 |
| 3 | one | **~330 ⚠** | delete line 121 | 10 → 9 |
| 4 | (a) characterization tests only; (b) lift + prune | **~160** | delete line 114 | 9 → 8 |
| 5 | one | **~245** | none | 8 (unchanged) |
| 6 | one | **~145** | line 134 → `["UP022"]` | 8 → 7 |

**⚠ PR3 is the budget risk (~330 of 400, ~70 lines of headroom).** `sdd-apply` MUST run
`git diff --shortstat` at that boundary before opening the PR. Pre-approved contingency if it exceeds 400:
split into PR3a (`_build_resolution_user_prompt` + its tests, **no** pyproject change, function still at 11
and still masked) and PR3b (`_validate_completion_response` + its tests + the prune + the counter). No
`size:exception` is expected anywhere in this change.

Commit shape follows `work-unit-commits`: **one commit per function**, carrying helper(s) + call site +
quirk tests + the `pyproject.toml` entry + the counter decrement together, so a single `git revert` returns
that file to a suppressed-but-working state with the counter still in lockstep. Two documented exceptions:
PR4's characterization commit is separate **on purpose** (it is only evidence if a reviewer can check it out
and see it green against untouched source), and PR5 carries no pyproject/counter change because the function
is still at 11 there.

`EXPECTED_C901_FILE_COUNT` ends at **7**, matching the 7 surviving entries: both benchmark `server.py`,
`vad_helpers.py`, `download.py`, `youtube_channel.py`, `reap_shorts_uploader_dag.py`,
`utils/youtube_downloader.py`. Both edited code lists stay sorted, as
`test_every_code_list_is_sorted_and_deduped` requires.

## Verification contract (per tip, not per branch)

1. **Hidden-regression check, run fresh immediately before each prune**, against the **actual PR base**:
   `uvx ruff check --select C901 --config 'lint.per-file-ignores = {}' --output-format concise <file>`.
   A `per-file-ignores` entry suppresses the **whole file**, so pruning it un-suppresses every offender in
   it — including any that `dev` added after this branch left `origin/main 1aa5681`. This is the exact
   slice-3 defect. Because this worktree is based on `main` and the PRs target `dev`, apply MUST re-run this
   check after rebasing onto `origin/dev`, not only on the local base.
2. **`uv run ruff check .` clean at EVERY PR tip**, verified in a disposable worktree
   (`git worktree add --detach <scratch>/tip-N <sha>`; remove it afterwards) — it is the one required CI
   check, and a red tip is an undeliverable PR that propagates red to every descendant.
3. Focused pytest per tip with `-o addopts=` (bypasses `--cov-fail-under=80` on partial selections); full
   `uv run pytest` with the coverage gate at the final tip only. Baseline ~4270 tests / ~89.9%.
4. `bash scripts/test-airflow-e2e.sh` at the final tip (PR5/PR6 touch a DagBag-parsed module). On
   `unavailable`, a NAS `airflow dags list-import-errors` after `git_sync_dag` is owed before merge to `main`.
5. Zero edits to pre-existing assertions: `git diff` on existing test files must show additions only.

## Tradeoffs

| Question | Options | Decision |
|---|---|---|
| Lift in place vs. a new `_helpers` module | new module groups the helpers and keeps files shorter | **In place, immediately above the outer.** Four of the six files are walked by the DagBag; a new helper module under `congress_videos/` whose text contains "airflow" and "dag" breaks the scheduler in safe mode (recorded gotcha). It would also add an import edge and a `__init__` decision this slice has no mandate for, and it breaks the reviewer's "the block did not move modules" assurance. |
| 2 helpers vs. 1 per function | one bigger helper per function is fewer moving parts | **2 (or 3) where the arithmetic demands it.** One helper spanning both blocks of `normalize_chapter_speakers` would itself be at 11 — it would move the violation, not remove it. For `_resolve_speaker_inner` a single helper would have to invent an intermediate contract (the base has no single value joining prompt-building and response-validation), which is a redesign. The helper count is a **consequence** of pure-lift plus ≤10, never a target. |
| `_collect_pattern_spans` shared vs. duplicated | two helpers, each with its constant inlined, give perfect byte equality with no (d) | **One shared helper.** The two base blocks are byte-identical; two near-clone helpers would ship the duplication the lift is supposed to relocate, and a reviewer would rightly flag it. The cost is normalization (d), which is mechanical and *stronger* evidence: the helper must match **both** sites, not one. |
| `_build_resolution_user_prompt` 9-parameter signature | derive `combined_text` inside the helper (7 params) | **Pass all 9.** Recomputing `combined_text = f"{intro_text}\n{turn_text}"` inside the helper is re-expression: it duplicates a computation the base performs once and breaks the AST-equality property for that statement. A wide but honest signature is the price of a pure lift. |
| PR5/PR6 boundary | one PR for all of `_prepare_turns_callable` | **Two.** One PR would be ~390 lines with no headroom against 400, and it would put the AI-attribution block (issues #282/#321/#322/#342) and the VAD/decode block in the same revert boundary. Splitting means PR5 lands the function at **11 — still over the limit, still masked, no token drop** — and PR6 takes it to 9 and drops the token. Accepted cost: PR5 moves the counter by zero, which its body must state explicitly so it is not mistaken for a stalled slice. |
| PR ordering | riskiest first, to fail fast | **Cheapest and purest first** (pure functions → module orchestration → DAG callable). If the stack has to be truncated mid-review, the counter has already moved and the truncated remainder is the part that was going to need the most attention anyway. It also keeps the DagBag-parsed module out of every tip until PR5. |
| PR4 characterization tests | fold them into the lift commit | **Separate commit.** A characterization test only proves what it claims if it is demonstrably green *before* the source changes; folded in, a reviewer cannot distinguish "pins existing behaviour" from "written to match the new code". |
| AST proof tooling | version a `scripts/` tool with its own tests | **Scratch-only.** One-shot verification evidence, not product code: versioning it spends ~90 lines of review budget, adds a maintenance/test burden, and pulls throwaway tooling into the DagBag walk's tree. The captured `OK` lines give the reviewer the same proof at no cost. |

## Rollback

Per-PR `git revert` of the merge. Because helper(s), call site, tests, entry and counter move together in one
commit, reverting any single PR leaves `TestC901BaselineCoverage` green. **One ordering constraint**: PR6
prunes the `speaker_turn_prepare_dag.py` token that PR5's lift half-pays for, so **PR5 may only be reverted
together with PR6** (or PR6 first) — reverting PR5 alone returns the function to 19 in a file whose token
PR6 already dropped, turning the required check red. Both PR bodies must state this pairing. Every other PR
reverts independently. No migration, no schema, no deployed-state change; full undo is `revert to dev` plus a
`git_sync_dag` trigger on each NAS scheduler.

## Threat Matrix

N/A — no routing, shell command, subprocess, VCS/PR automation, executable-file classification, or
process-integration boundary is introduced or modified. `_prepare_turn_artifacts` relocates code that calls
`trim_turn_silence_with_vad` and `_run_ffmpeg_decode_check`, but the subprocess construction, argument
handling and timeout all stay byte-identical inside their existing helpers, which this slice does not touch.
The uncommitted proof script shells out only to `git show` on a fixed revision under the operator's control.

## Migration / Rollout

No migration. After merge to `dev`, trigger `git_sync_dag` on **each** NAS scheduler (dev and prod are
separate stacks) and confirm `airflow dags list-import-errors` is empty.

## Open risks

- **Predicted vs. measured complexity.** Every Cx above is hand-derived from a model validated on five
  baselines, not measured (this phase had no shell). If apply measures an outer at 10 or 11, the contingency
  is one additional in-place helper from the same function using the same catalogue — never a redesign, and
  never an early token drop.
- **`dev` drift.** New offenders merged to `dev` since `1aa5681` in any of the six files would make a prune
  red at its tip. Mitigated by re-running the hidden-regression check against `origin/dev` after rebase.
- **PR3 headroom** (~70 lines) — measured contingency defined in Delivery.
- **`_prepare_turns_callable` behavioural surface** — 56+ existing tests run through the public entry point;
  no assertion in them may be edited, so any drift shows as a failure, not as an edited expectation.
