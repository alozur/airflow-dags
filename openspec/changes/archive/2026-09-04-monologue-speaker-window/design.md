# Design: Monologue Speaker Window (issue #430)

## Technical Approach

Exploration Approach 2, as confirmed in the proposal: a new pure module
`congress_videos/modules/monologue_speaker_window.py` owns a two-step resolution for non-qa
turns. Step 1 sees ONLY the pre-anchor announcement window; Step 2 sees ONLY Step 1's small JSON
plus the roster. `resolve_speaker` and `tests/congress_videos/modules/test_speaker_resolution.py`
are not touched (spec: *Non-Regression of the Existing Resolver*). Gates and helpers are reused by
IMPORT, never re-implemented: `has_announcement_phrase`, `_evidence_supported_in_blocks`,
`SPEAKER_RESOLUTION_MIN_CONFIDENCE`, `find_srt_for_chapter`, `_parse_srt_blocks`,
`chapter_roster_mentions`/`crosscheck_slug` (Gate B, at the caller), `mark_turn_resolved` (Gate A).

**Scheduler-safety convention (not a hard constraint).** The new module lives under the parsed
folder. `announcement_patterns.py` documents keeping its text free of the two parser trigger words,
and the new module follows that convention for cheapness — but it is a convention, not a rule:
seven sibling modules under `congress_videos/modules/` (`database.py`, `materialization.py`,
`speaker_turns.py`, `upload_marking.py`, `video_analytics.py`, `youtube/download.py`,
`youtube/__init__.py`) already carry both words and import cleanly. The real hazard is a module
that breaks on import — relative imports or import-time side effects — which this module avoids:
absolute imports only, no I/O at import time. Docstrings say "the prepare pipeline" / "the prepare
loop".

## Architecture Decisions

| # | Decision | Alternatives rejected | Rationale |
|---|---|---|---|
| D1 | New module, `resolve_speaker` untouched | refactor its narrow branch; dispatch inside it | Keeps the #322 byte-identical-prompt guards green; makes "at most 120 s reaches the model" provable by a pure function |
| D2 | Public return is `dict \| None` (`None` = unresolved), with a 4th key `audit` | always return a dataclass | The caller's `if narrow is not None` and Gate B stay byte-identical (Gate A only gains an optional kwarg). `participant_slug=None, confidence=0.0` is the INTERNAL `AnnouncedIdentity`/`FloorHolder` unresolved sentinel, which the public wrapper maps to `None` — the spec now states this binding directly |
| D3 | One `completion_fn` parameter shared by both steps | one per step | Matches `resolve_speaker`/`resolve_chapter_speakers` idiom; a single test double captures both payloads, so exclusion proofs assert over ALL captured prompts |
| D4 | Import the private helpers `_parse_srt_blocks`, `_evidence_supported_in_blocks` | add public aliases in `srt_helpers`/`speaker_resolution` | `speaker_resolution.py` already imports these privates; adding aliases would MODIFY a file the spec freezes. Importing does not modify it |
| D5 | Duplicate the 5-line `canonical_dir` probe from `speaker_resolution._resolve_speaker_inner` | extract a shared helper | Extraction edits the frozen file. Accepted duplication; unification belongs to the follow-up issue |
| D6 | Evidence column receives the audit JSON when present, else the raw quote | two columns; JSONB | Migration 046 stays one nullable TEXT column as specified; the JSON already embeds the verbatim quote |
| D7 | Persist evidence for BOTH resolvers via one call site (`evidence=winner.get("audit") or winner.get("evidence") or None`) | branch per resolver | One line, no branching in an already C901-exempt function; qa rows gain the same audit trail at zero risk (column is nullable) |
| D8 | The qa-promotion wide re-pass keeps calling `resolve_speaker` | route it too | Spec *Routing by Turn Type*; #342 stickiness semantics are unchanged |

**Rejected safety-net**: "fall back to `resolve_speaker` when the monologue resolver is
unresolved". It would re-admit post-turn transcript into the prompt — the exact defect #430 closes.

## Data Flow

    turn (turn_type != 'qa')
        │
        ▼
    _load_turn_blocks ──► find_srt_for_chapter ──► _parse_srt_blocks
        │
        ▼  anchor = group_start_seconds ?? start_seconds ; window_start = max(0, anchor-120)
    select_preceding_window(blocks, anchor)      # window_start <= b.start < anchor
        │  window_blocks ──► window_text
        ▼
    has_announcement_phrase(window_text) ── False ──► None  (0 LLM calls)
        │ True
        ▼
    identify_floor_holder(window_blocks, completion_fn)      # STEP 1: window text ONLY
        │  FloorHolder{announced_name_or_role, evidence, found}
        ├── found False ──────────────────────────────────► None  (Step 2 never called)
        ├── _evidence_supported_in_blocks(evidence, window_blocks) False ──► None
        ▼
    resolve_announced_identity(fh, participants, completion_fn)  # STEP 2: name+evidence+roster
        │  AnnouncedIdentity{full_name, participant_slug, confidence}
        ├── slug not in roster, or confidence < 0.80 ─────► None
        ▼
    {participant_slug, confidence, evidence, audit}
        │
        ▼  caller: Gate B crosscheck_slug ──► mark_turn_resolved(..., evidence=audit)

## File Changes

| File | Action | Description |
|------|--------|-------------|
| `congress_videos/modules/monologue_speaker_window.py` | Create | Window selection, Step 1, Step 2, audit builder, never-raise orchestrator |
| `congress_videos/config/ai_prompts.py` | Modify | 2 system prompts + 2 user templates, placed directly after `SPEAKER_RESOLUTION_WIDE_USER_TEMPLATE` |
| `congress_videos/sql/migrations/046_add_speaker_resolution_evidence.sql` | Create | `speaker_turn_videos.speaker_resolution_evidence TEXT` |
| `congress_videos/sql/production_schema.sql` | Modify | Snapshot the new column in the `speaker_turn_videos` block |
| `congress_videos/modules/database.py` | Modify | `mark_turn_resolved(..., evidence: str \| None = None)` |
| `congress_videos/speaker_turn_prepare_dag.py` | Modify | `turn_type` routing + evidence kwarg + docstring |
| `tests/congress_videos/modules/test_monologue_speaker_window.py` | Create | Unit suite (RED first) |
| `tests/congress_videos/sql/test_production_schema.py` | Modify | +1 entry in `TABLE_COLUMNS["speaker_turn_videos"]` |
| `tests/congress_videos/modules/test_database_speaker_resolution.py` | Modify | +3 SQL-shape tests |
| `tests/congress_videos/modules/test_mark_turn_resolved_live.py` | Modify | +1 column in `_SCHEMA_SQL`, +1 live test |
| `tests/congress_videos/test_speaker_turn_prepare_dag.py` | Modify | Patch-target rewiring + routing tests (see *Caller-suite impact*) |
| `docs/PIPELINE.md` | Modify | One paragraph on the two-step monologue resolution |

Paths verified in this worktree: migrations live under `congress_videos/sql/migrations/` (NOT
`sql/migrations/`) and the schema test is `tests/congress_videos/sql/test_production_schema.py`.

## Interfaces / Contracts

```python
# congress_videos/modules/monologue_speaker_window.py
MONOLOGUE_WINDOW_SECS: int = 120
MONOLOGUE_RESOLUTION_METHOD: str = "monologue_window_v1"

@dataclass(frozen=True)
class FloorHolder:                      # Step-1 result
    announced_name_or_role: str = ""
    evidence: str = ""
    found: bool = False

@dataclass(frozen=True)
class AnnouncedIdentity:                # Step-2 result (unresolved sentinel = defaults)
    full_name: str = ""
    participant_slug: str | None = None
    confidence: float = 0.0

def turn_anchor_seconds(turn: dict) -> float: ...
def select_preceding_window(blocks: list[dict], anchor_seconds: float,
                            window_seconds: int = MONOLOGUE_WINDOW_SECS) -> list[dict]: ...
def identify_floor_holder(window_blocks: list[dict],
                          completion_fn: Callable | None = None) -> FloorHolder: ...
def resolve_announced_identity(floor_holder: FloorHolder, participants: list[dict],
                               completion_fn: Callable | None = None) -> AnnouncedIdentity: ...
def build_resolution_audit(floor_holder: FloorHolder, identity: AnnouncedIdentity,
                           window_start_seconds: float, anchor_seconds: float) -> str: ...
def resolve_monologue_speaker(turn: dict, participants: list[dict],
                              completion_fn: Callable | None = None) -> dict | None: ...
# private: _load_turn_blocks(turn), _resolve_monologue_inner(turn, participants, completion_fn)
```

Every function stays under 50 lines and under C901 = 10 — the new module gets NO
`per-file-ignores` entry (that list is remove-only). `line-length = 120`.

- **Injection**: identical idiom to `resolve_speaker` — module-level `from utils.llm_config import
  LLM_CHEAP`; inside each step, `if completion_fn is None: from utils.llm_cache import
  cached_json_completion`; the call is `completion_fn(SYSTEM_PROMPT, user_prompt, model=LLM_CHEAP)`.
  `find_srt_for_chapter` / `_parse_srt_blocks` are imported at module level so tests patch
  `congress_videos.modules.monologue_speaker_window.<name>`.
- **Audit JSON** (`json.dumps(..., ensure_ascii=False, sort_keys=True)`), exactly seven keys:
  `announced_name_or_role`, `evidence`, `step1_found`, `step2_confidence`,
  `window_start_seconds`, `anchor_seconds`, `method` (`"monologue_window_v1"`).
- **Never-raise**: `resolve_monologue_speaker` wraps `_resolve_monologue_inner` in
  `try/except Exception` → `logger.warning("resolve_monologue_speaker: unexpected exception for
  turn_id=%s — returning None (%s: %s)", ...)` → `None`.
- **Error responses are NOT exceptions.** A completion result whose `error` is set, or whose `data`
  is missing/not a dict, is an ordinary degraded outcome on the same contract
  `cached_json_completion` returns — it never reaches the `except` arm. Step 1 returns the
  `FloorHolder()` sentinel and Step 2 returns the `AnnouncedIdentity()` sentinel, each logging
  `logger.warning("...: step N completion error for turn_id=%s: %s — returning None", ...)`; the
  orchestrator then returns `None` (and a Step-1 sentinel means Step 2 is never called, exactly as
  `found=false` does). This is why the steps return sentinels rather than raising: the caller reads
  one unresolved shape whatever went wrong.
- Additional assertable WARNING/INFO lines:
  `"...: no announcement phrase in window text for turn_id=%s — skipping both LLM calls"` (INFO),
  `"...: step 1 found no announcement for turn_id=%s — skipping step 2"` (INFO),
  `"...: step-1 evidence not locatable in the window for turn_id=%s — returning None"` (WARNING),
  `"...: step 2 returned slug %r not in roster for turn_id=%s — returning None"` (WARNING),
  `"...: confidence %.2f < %.2f for turn_id=%s — returning None"` (INFO).

### Prompts (`config/ai_prompts.py`, English, next to the existing speaker-resolution block)

```python
# Monologue floor-holder identification (issue #430, STEP 1). Sees ONLY the
# pre-anchor announcement window (max MONOLOGUE_WINDOW_SECS seconds). It must
# never receive the turn's own transcript, and it has NO roster: it extracts an
# announced name or role, nothing more.
# Output JSON: {"announced_name_or_role": str|null, "evidence": str, "found": bool}
MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT = (
    "You are a floor-holder extraction assistant for the Spanish Congress of Deputies. "
    "You receive ONLY the transcript immediately BEFORE a speaking turn begins — the "
    "presiding officer's handover. Your single job is to extract who is being given the "
    "floor NEXT.\n\n"
    "Typical Spanish handover patterns:\n"
    "- 'Tiene la palabra el señor <apellido>' / 'la señora <apellido>'\n"
    "- 'Tiene la palabra su señoría'\n"
    "- 'Por el Grupo Parlamentario <grupo>, tiene la palabra ...'\n"
    "- Role handovers: 'tiene la palabra el ministro de <cartera>', "
    "'la señora vicepresidenta primera', 'el señor presidente del Gobierno'\n"
    "- Courtesy openings that precede the real handover: 'Gracias, señoría', "
    "'Gracias, señora presidenta, tiene la palabra el señor <apellido>'\n\n"
    "Rules:\n"
    "- Respond with ONLY valid JSON and nothing else.\n"
    "- Return the FLOOR HOLDER: the person who is about to speak.\n"
    "- NEVER return someone merely ADDRESSED ('Señor <apellido>, le contesta el "
    "ministro X' announces X, not <apellido>), merely THANKED, or merely MENTIONED "
    "in the debate content.\n"
    "- announced_name_or_role is copied as announced: a surname, a full name, or a "
    "role phrase ('el ministro de Hacienda'). Do not expand, translate, or guess it.\n"
    "- evidence MUST be a verbatim quote copied from the text above — never "
    "paraphrase, summarize, or invent it.\n"
    "- Set found to false (and announced_name_or_role to null) whenever the text "
    "carries no handover, or you are unsure. An honest false is always preferred "
    "over a guess.\n\n"
    'JSON schema: {"announced_name_or_role": "<as announced, or null>", '
    '"evidence": "<verbatim quote from the text above>", "found": <true|false>}'
)

MONOLOGUE_FLOOR_HOLDER_USER_TEMPLATE = (
    "ANNOUNCEMENT WINDOW (transcript ending exactly where the turn starts):\n"
    "{window_text}\n\n"
    "Who is being given the floor next? Return ONLY valid JSON:\n"
    '{{"announced_name_or_role": "<as announced, or null>", '
    '"evidence": "<verbatim quote from the window above>", "found": <true|false>}}'
)

# Monologue identity resolution (issue #430, STEP 2). Receives ONLY step 1's
# small JSON plus the roster — NEVER any transcript beyond the evidence quote.
# Output JSON: {"full_name": str|null, "participant_slug": str|null, "confidence": float}
MONOLOGUE_IDENTITY_RESOLUTION_SYSTEM_PROMPT = (
    "You are a name-normalization assistant for the Spanish Congress of Deputies. "
    "You receive a name or role phrase announced by the presiding officer, the verbatim "
    "quote it came from, and the roster of known participants. Decide which participant "
    "was announced.\n\n"
    "Rules:\n"
    "- Respond with ONLY valid JSON and nothing else.\n"
    "- full_name is the person's full name: expand a bare surname, an abbreviation, or "
    "an institutional role ('el ministro de Hacienda', 'la vicepresidenta primera') to "
    "the full name of whoever held that office in this legislature, when you know it.\n"
    "- participant_slug MUST be copied EXACTLY from the roster, or null.\n"
    "- Choose a slug ONLY when the roster match is unambiguous. Two plausible "
    "participants, a surname shared by several deputies, or a role you cannot pin to a "
    "roster entry all mean participant_slug null.\n"
    "- confidence is a float 0.0-1.0 for the slug choice.\n"
    "- Never invent a slug that is absent from the roster; an honest null is always "
    "preferred over a guess.\n\n"
    'JSON schema: {"full_name": "<full name or null>", '
    '"participant_slug": "<slug from the roster or null>", "confidence": <0.0-1.0>}'
)

MONOLOGUE_IDENTITY_RESOLUTION_USER_TEMPLATE = (
    "ANNOUNCED NAME OR ROLE:\n{announced_name_or_role}\n\n"
    "ANNOUNCEMENT QUOTE:\n{evidence}\n\n"
    "KNOWN PARTICIPANTS (slug | display_name | party — one per line):\n"
    "{participant_roster}\n\n"
    "Which participant was announced? Return ONLY valid JSON:\n"
    '{{"full_name": "<full name or null>", '
    '"participant_slug": "<slug from the roster above or null>", '
    '"confidence": <float 0.0-1.0>}}'
)
```

Interpolated fields: Step 1 → `window_text` ONLY. Step 2 → `announced_name_or_role`, `evidence`,
`participant_roster` (same `slug | display_name | party` serialization as `resolve_speaker`).

### Migration 046 (`congress_videos/sql/migrations/046_add_speaker_resolution_evidence.sql`)

```sql
-- Migration 046: persist the accepted speaker-resolution evidence (issue #430)
-- Created: 2026-09-04
-- Depends on: 034_add_speaker_resolution.sql
--
-- Adds ONE nullable column to speaker_turn_videos:
--   speaker_resolution_evidence TEXT -- the monologue two-step audit JSON
--   (method "monologue_window_v1") or, for the qa path, the verbatim
--   announcement quote the model reported. NULL = resolved before this
--   migration, or never resolved. No backfill.
--
-- NO view is recreated: uploadable_turns does not select this column, so the
-- 044 snapshot/lockstep guard in tests/congress_videos/sql/test_production_schema.py
-- stays valid untouched. The column is additive and nullable, so every existing
-- INSERT and UPDATE keeps working unchanged.
--
-- Idempotent: ADD COLUMN IF NOT EXISTS.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS speaker_resolution_evidence TEXT;

-- DOWN
-- Manual psql only -- the runner has no automatic rollback and executes the WHOLE
-- file text in ONE transaction, so this block MUST stay commented out (044 convention):
-- a live DOWN would revert its own UP and still register as applied.
--
-- ALTER TABLE speaker_turn_videos DROP COLUMN IF EXISTS speaker_resolution_evidence;
```

`production_schema.sql` — inside `CREATE TABLE IF NOT EXISTS production.speaker_turn_videos`,
directly after `speaker_resolution_method TEXT,`:

```sql
    -- Added by migration 046 (two-step monologue resolution audit, issue #430)
    speaker_resolution_evidence   TEXT,
```

and extend the block's folded-migration header comment with `+ 046 (resolution evidence, issue
#430)`. Column-tuple test: append `"speaker_resolution_evidence",` after
`"speaker_resolution_method",` in `TABLE_COLUMNS["speaker_turn_videos"]` (membership-checked, but
keep DDL order).

### `mark_turn_resolved`

```python
def mark_turn_resolved(self, output_path, slug, confidence, method,
                       representative_turn_id, evidence: str | None = None) -> None:
    ...
    evidence_set = ",\n                        speaker_resolution_evidence = %s" if evidence is not None else ""
    set_params = (slug, confidence, method) + ((evidence,) if evidence is not None else ())
    cur.execute(f"""
            UPDATE {stv_table}
            SET resolved_participant_slug = %s,
                speaker_resolution_confidence = %s,
                speaker_resolution_method = %s{evidence_set}
            WHERE turn_id IN ( ... UNCHANGED, BYTE-IDENTICAL ... )
            """, (*set_params, output_path, representative_turn_id))
```

The Gate A `WHERE turn_id IN (SELECT ... speaker_label ...)` subselect and the existing
`logger.info` call stay byte-identical; only the SET list and the parameter tuple grow, and only
when `evidence is not None`. Placeholder order is preserved (evidence sits after `method`, before
`output_path`).

### Caller routing (`speaker_turn_prepare_dag.py`, current line 305-306)

```python
            if not already_resolved:
                # issue #430: monologue (non-qa) turns resolve from the pre-turn
                # announcement window only — the turn's own transcript never
                # reaches a model. qa turns keep the combined/wide resolver
                # (#322), and so does the qa-promotion re-pass below (#342).
                if (turn.get("turn_type") or "monologue") != "qa":
                    narrow = resolve_monologue_speaker(turn, participants)
                else:
                    narrow = resolve_speaker(turn, participants)
```

and, at the single existing write site (line 372):

```python
                        db.mark_turn_resolved(
                            output_path, winner["participant_slug"], winner["confidence"],
                            "ai_srt_context", turn_id,
                            evidence=winner.get("audit") or winner.get("evidence") or None,
                        )
```

The wide qa re-resolve (`resolve_speaker({**turn, "turn_type": "qa"}, participants)`), the
promotion stickiness, Gate B, the idempotency skip and the audit INFO line are untouched.

**Caller-suite impact (must not be discovered at apply time).** `_make_turn()` in
`tests/congress_videos/test_speaker_turn_prepare_dag.py` sets no `turn_type`, so after routing
those turns take the NEW resolver: the 23 `patch("...speaker_turn_prepare_dag.resolve_speaker")`
sites and 10 `mark_turn_resolved.assert_called_once_with(...)` assertions (plus 2
`assert_not_called`) in that file must be rewired in PR C. `TestQaPromotionReresolution._run` needs two mocks (narrow → monologue, wide →
`resolve_speaker`) instead of one `side_effect` list. The frozen suite is the MODULE suite
`tests/congress_videos/modules/test_speaker_resolution.py` (0 edits) — the caller suite is not
frozen and cannot be.

## Testing Strategy

RED first (`strict_tdd: true`), `uv run pytest`. Seams: the four public module functions, the DB
method, and `_prepare_turns_callable`. Mocking idiom copied from `test_speaker_resolution.py`:
patch `congress_videos.modules.monologue_speaker_window.find_srt_for_chapter` /
`._parse_srt_blocks`, inject `completion_fn` capturing `(system, user, **kw)` into a list.

| Spec requirement | Tests |
|---|---|
| Preceding Window Selection | 6 boundary cases: block at `anchor-120` in; at `anchor-120-0.001` out; at `anchor` out; overlapping (`start=anchor-1`, `end>anchor`) in; `anchor<120` clamps `window_start` to 0; `group_start_seconds` overrides `start_seconds` (+ `group_start_seconds == 0.0` honoured, `is not None`) |
| Announcement Pre-Gate | window without a phrase → result `None`, `completion_fn` call count == 0 |
| Step-1 Payload Scope | blocks before `window_start` and at/after `anchor` carry unique sentinel strings; assert they appear in NO captured user prompt |
| Step-1 Floor-Holder | Split honestly in two. (a) **Prompt-contract tests** (the only thing a unit test can prove about model behaviour): `MONOLOGUE_FLOOR_HOLDER_SYSTEM_PROMPT` contains the addressee rule, the `found=false`-when-unsure rule, and the verbatim-evidence rule; the user template interpolates `window_text` and nothing else. (b) **Pass-through tests at the `identify_floor_holder` seam** with an injected `completion_fn`: the `García` / `López`-vs-`X` / `Ruiz` strings are MOCK ECHOES asserting that whatever the model returns is parsed into `FloorHolder` unchanged — they do not demonstrate that a real model distinguishes an addressee. Note: `"Señor López, le contesta el ministro X"` matches none of the three patterns in `announcement_patterns.py:26-42`, so it never clears the pre-gate; it is reachable only at the `identify_floor_holder` seam, never through `resolve_monologue_speaker`. Plus `found=false` → Step 2 never called (call count == 1), and evidence not locatable in window blocks → `None` |
| Step-1/Step-2 error response (non-exception) | `completion_fn` returns `{"error": "boom", "data": None}`, then `{"error": None, "data": None}`, then `{"error": None, "data": "not-a-dict"}`: each yields `None`, one WARNING per step, no exception, and a Step-1 error means `completion_fn` was called exactly once |
| Step-2 Payload Scope | window text (minus the evidence quote) absent from the Step-2 prompt; roster present |
| Step-2 Roster-Backed | roster slug at 0.80 accepted; 0.79 rejected; slug outside roster rejected; non-numeric confidence rejected |
| Result Shape and Audit | resolved dict has `participant_slug`/`confidence`/`evidence`; `json.loads(result["audit"])` has exactly the seven keys and `method == "monologue_window_v1"` |
| Never-Raise | `completion_fn` raising on Step 1, and on Step 2 → `None` + one WARNING (`caplog`) |
| Evidence Persistence | migration file exists, contains `ADD COLUMN IF NOT EXISTS`, DOWN block fully commented; snapshot column-tuple test; `mark_turn_resolved` SQL contains `SPEAKER_RESOLUTION_EVIDENCE` with `evidence=` and does NOT without it; 5-positional-arg call leaves the SQL byte-identical; live-PG round-trip (opt-in) |
| Routing by Turn Type | monologue turn → `resolve_monologue_speaker` called, `resolve_speaker` not called; `turn_type='qa'` → `resolve_speaker` called, monologue resolver not called; promotion wide re-pass still calls `resolve_speaker` with `turn_type='qa'` |
| Non-Regression | `uv run pytest tests/congress_videos/modules/test_speaker_resolution.py` green with a zero-line diff on that file |

**Opt-in live-LLM test.** Because no mocked test can prove the addressee/floor-holder distinction,
add ONE opt-in test that sends the three window texts to the real model through
`cached_json_completion` and asserts the floor holder, marked
`@pytest.mark.live_llm` + `@pytest.mark.skipif(not (os.getenv("OPENAI_API_KEY") and
os.getenv("MONOLOGUE_LIVE_LLM_TESTS") == "1"), reason=...)`. It is skipped in CI and in every
default run, so it never gates a PR; it is the honest way to check the prompt when it is edited.
`live_llm` MUST be registered in `[tool.pytest.ini_options].markers` (the suite runs
`--strict-markers`), and a single-file run needs `-o addopts=` because the default
`--cov-fail-under=80` fails any partial run.

E2E: `bash scripts/test-airflow-e2e.sh` (triggered by `congress_videos/**`) — proves the new module
does not break the parsed folder. If Docker is unavailable, run `dags list-import-errors` on the
NAS after `git_sync`.

## Threat Matrix

`N/A` — no routing of shell commands, no subprocess, no VCS/PR automation, no executable-file
classification, no process integration. The only external boundary is SRT path probing, delegated
unchanged to `find_srt_for_chapter`, which already rejects a `video_id` outside `[A-Za-z0-9_-]+`.

| Boundary | Applicability |
|---|---|
| Documentation-like paths | N/A: no file-type classification or execution |
| Git repository selection | N/A: no VCS invocation |
| Commit state | N/A: no VCS invocation |
| Push state | N/A: no VCS invocation |
| PR commands | N/A: no PR automation |

## Migration / Rollout

Additive and nullable; no backfill (out of scope). Order: apply 046 to dev, then to prod BEFORE
PR C merges to main, so no deployed code writes a column that does not exist. Renumber to the next
free integer if PR #449 (045) has not landed at apply time. The prepare loop is idempotent: turns
already resolved at >= 0.80 are skipped, so nothing re-resolves retroactively.

## Delivery Slices (auto-chain, 400-line budget)

| PR | Content | Forecast (add+del) | Rollback |
|---|---|---|---|
| A1 | Module skeleton (constants, `turn_anchor_seconds`, `select_preceding_window`) + 4 prompts + window/prompt-contract tests | ~280 | Delete the module + prompt constants; nothing imports them |
| A2a | `FloorHolder`/`AnnouncedIdentity` dataclasses + `identify_floor_holder` + `resolve_announced_identity` + their tests (seam pass-through, roster/confidence gates, per-step error responses, per-step raises) | ~250 | Revert to A1's inert module |
| A2b | `build_resolution_audit` + `_load_turn_blocks` + `_resolve_monologue_inner` + `resolve_monologue_speaker` + their tests (pre-gate no-call, payload exclusion, evidence locatability, audit keys, never-raise end to end) | ~230 | Revert to A2a; the two steps stay usable and tested |
| B | Migration 046 + snapshot + column-tuple test + `mark_turn_resolved(evidence=)` + DB tests | ~120 | Revert code; leave the nullable column (or run the commented DOWN manually) |
| C | Routing + evidence kwarg + caller-suite rewiring + routing tests + docs + follow-up issue | ~210 | Revert this commit: monologue returns to `resolve_speaker`, module goes inert |

A was forecast as one PR in the proposal but lands at ~670 lines, so one honest slicing pass splits
it along the pure-window / LLM-steps seam. A2 was then re-estimated at ~480 (the per-step error and
raise cases roughly double its test body), which is over budget, so it is pre-split at the
steps / orchestrator seam into A2a and A2b — each is independently reviewable and each ships its
own tests. Order and dependencies are unchanged (A1 → A2a → A2b → B → C; C needs A2b and B).
`sdd-tasks` owns the final guard lines and may re-measure.

## Open Questions

- [ ] None blocking. Two items for `sdd-tasks` to confirm: (a) the A1/A2a/A2b split above;
      (b) whether any slice must split again once its diff is measured rather than forecast.
