# Design: Verify Final Copy Before Publication

Issue #512. Stacked on #511 `canonical-politician-display-names`.

## Technical Approach

One deep module, `congress_videos/modules/final_copy_verification.py`, exposes a single
caller function and hides prompt assembly, defensive verdict parsing, the bounded
correction round, and evidence containment. It never raises (module convention:
`mentioned_people_resolution.py:78-110`, `chapter_speaker_resolution.py`). It takes an
injectable `completion_fn` defaulting to `utils.llm_cache.cached_json_completion`
(same convention, `mentioned_people_resolution.py:139-144`), `model=LLM_DEFAULT`, and
passes **no** `temperature` / `max_tokens` / `max_completion_tokens` (#375 regression
test `tests/utils/test_no_removed_llm_kwargs.py`).

Two DAG tasks consume it, each inserted immediately before its publish task:
`youtube_upload_dag.py` between `t6` (`prepare_upload_config`) and `t7`
(`trigger_youtube_upload`), and `reap_shorts_uploader_dag.py` between `t2`
(`generate_metadata`) and `t3` (`trigger_youtube_upload`).

**Verify the last mutable representation, never an upstream copy.** For long-form that
is `upload_config["videos"][0]["title"|"description"]` — `prepare_orador_upload_config`
(`modules/youtube/youtube_upload.py:66-87`) reads the sidecars from disk and `.strip()`s
them, so the pre-strip XCom values are not the published bytes. For shorts it is the
`shorts_metadata` entries, which only carry their final form after the channel footer
and session line are appended (`reap_shorts_uploader_dag.py:376-377`).

## Architecture Decisions

### D1 — Verdict schema and defensive parsing

**Choice**: one JSON object; parsed into a frozen `CopyVerdict` dataclass with an
explicit `ok: bool`. Anything not exactly conforming is **inconclusive**, never `pass`.

```json
{
  "verdict": "correctable",
  "findings": [
    {"field": "title", "category": "person_name", "severity": "high",
     "detail": "El título escribe «Sanchez»; la evidencia da «Sánchez».",
     "suggestion": "Sánchez"},
    {"field": "thumbnail_text", "category": "party_name", "severity": "medium",
     "detail": "La miniatura atribuye «Sumar»; la evidencia da «PSOE».",
     "suggestion": ""}
  ],
  "corrected": {
    "title": "Sánchez responde sobre la financiación autonómica",
    "description": "Intervención de Sánchez en el Pleno..."
  },
  "rationale": "Un error ortográfico en un apellido y una atribución de grupo no respaldada."
}
```

Parse rules (all failures ⇒ `ok=False`, treated as inconclusive):

| Condition | Outcome |
|---|---|
| `data` is `None`, `error` set, or not a dict | inconclusive |
| `verdict` not in `{pass, correctable, reject}` | inconclusive |
| `findings` present and not a list, or an element not a dict | inconclusive |
| `corrected` present and not a dict, or holds a key outside `{title, description}` | inconclusive |
| `verdict == "correctable"` with no usable `corrected` value | inconclusive |
| finding `field` outside `{title, description, thumbnail_text}` | that finding dropped, verdict kept |
| finding `category` outside the six | kept verbatim, classified `other`; never gates |

Gating reads `verdict` and finding `field` only, never `category` or `severity` — so an
unknown category can never widen or narrow the blocking decision.

**Alternatives considered**: per-field verdicts (three sub-objects); a numeric score.
**Rationale**: one overall verdict plus field-tagged findings is the minimum surface that
supports the locked asymmetry (title blocks, everything else records), and a single
`ok` flag makes the fail-safe direction unambiguous at every call site.

### D2 — Bounded correction contract

**Choice**: straight-line code with **no loop construct at all** and at most two
`completion_fn` invocations.

```
round 0: verify(original)
  pass          → publish original, persist verdict
  reject        → persist; long-form TITLE only: raise ValueError; else publish original
  correctable   → containment check on corrected value
                    fails  → publish original, append finding `unsupported_correction`
                    passes → round 1 (recheck)
  inconclusive  → publish original, persist nothing, log + accumulator
round 1: verify(corrected)          ← the mandatory recheck; terminal, never corrects again
  pass          → publish corrected, persist original + corrected
  anything else → publish ORIGINAL, persist round-0 verdict + `recheck_failed` finding
```

`MAX_CORRECTION_ROUNDS = 1` is a module constant, and a unit test counts
`completion_fn` calls and asserts `<= 2` on every branch. Runaway is structurally
impossible because there is no loop to bound.

The blocking decision is taken **only on round 0's verdict for the value actually
published**. If we fall back to the original, the original was not `reject` in round 0,
so it publishes. This avoids a recheck of a *corrected* string turning into a block on
an original string the verifier never rejected.

**Evidence containment** (`_unsupported_tokens`): the corrected value is discarded
unless every capitalised or accent-bearing token in it, after NFC normalisation,
casefolding and punctuation stripping, appears in the allowed set — the tokens of the
original value plus every string in the evidence bundle. Lowercase tokens are
unconstrained, so spelling and grammar fixes pass while a new surname, party or
institution cannot be introduced. Length bounds apply too: title ≤ 100 chars (YouTube
limit already enforced by `truncate_text(..., max_length=100)` at
`reap_shorts_uploader_dag.py:300`), description ≤ 5000.

**Alternatives considered**: retry-until-clean; trusting `corrected` verbatim.
**Rationale**: an LLM asked to fix an invented name will happily invent another. Token
containment is a cheap, deterministic, testable proof that the correction is a
restatement of the evidence and not a new claim.

### D3 — Idempotency and content versioning

**Choice**: `copy_content_version` = sha256 of the canonical JSON
`{"title", "description", "thumbnail_text", "evidence"}` with `sort_keys=True,
ensure_ascii=False` — the same payload the user prompt is rendered from. Because
`cached_json_completion` keys on exact prompt text (`utils/llm_cache.py:33-61`), the
cache key and the content version move together: identical content ⇒ identical prompt ⇒
cache hit ⇒ identical verdict ⇒ identical write.

Writes are guarded UPDATEs, not inserts:

```sql
UPDATE {speaker_turn_videos} SET copy_verification_verdict = %s, ..., copy_verified_at = NOW(),
       copy_content_version = %s
 WHERE output_path = %s AND copy_content_version IS DISTINCT FROM %s
```

`rowcount == 0` means "already recorded for exactly this content" and is a success, not
an error. An Airflow retry (`retries=1`, unchanged per locked decision 6) therefore
re-runs the task, serves the verdict from the LLM cache, computes the same version and
writes zero rows.

**Stale-copy guard**: immediately before the write, the version is recomputed from the
values about to be published; a mismatch skips the write entirely and emits an
accumulator finding. A correction can never be recorded against copy that changed after
verification.

Keying: long-form writes by `output_path`, matching `mark_turns_uploaded_by_output_path`
(`modules/database.py:1127-1171`) — grouped short turns share one `output_path` across
several `speaker_turn_videos` rows (#129) and all of them describe the same published
video. Shorts write by `video_shorts.id` (the `short_id` already carried through
`_generate_metadata`).

### D4 — Migration 050 shape

`congress_videos/sql/migrations/050_final_copy_verification_audit.sql`, one file,
multiple `ALTER TABLE`, following the 048 precedent
(`048_manual_uploads_do_not_consume_scheduled_quota.sql`). Timestamp types follow each
table's own convention (`speaker_turn_videos` uses `TIMESTAMPTZ`, `video_shorts` uses
`TIMESTAMP`). `copy_thumbnail_text` exists only on `speaker_turn_videos`: the shorts
path has no thumbnail generation step.

```sql
-- Migration 050: audit record of pre-publication copy verification (issue #512).
-- All columns nullable: rows published before this change, and rows whose
-- verification was inconclusive, legitimately carry NULL.

ALTER TABLE speaker_turn_videos
    ADD COLUMN IF NOT EXISTS copy_verification_verdict   TEXT,
    ADD COLUMN IF NOT EXISTS copy_verification_findings  JSONB,
    ADD COLUMN IF NOT EXISTS copy_original_title         TEXT,
    ADD COLUMN IF NOT EXISTS copy_original_description   TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_title        TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_description  TEXT,
    ADD COLUMN IF NOT EXISTS copy_thumbnail_text         TEXT,
    ADD COLUMN IF NOT EXISTS copy_content_version        TEXT,
    ADD COLUMN IF NOT EXISTS copy_verified_at            TIMESTAMPTZ;

ALTER TABLE video_shorts
    ADD COLUMN IF NOT EXISTS copy_verification_verdict   TEXT,
    ADD COLUMN IF NOT EXISTS copy_verification_findings  JSONB,
    ADD COLUMN IF NOT EXISTS copy_original_title         TEXT,
    ADD COLUMN IF NOT EXISTS copy_original_description   TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_title        TEXT,
    ADD COLUMN IF NOT EXISTS copy_corrected_description  TEXT,
    ADD COLUMN IF NOT EXISTS copy_content_version        TEXT,
    ADD COLUMN IF NOT EXISTS copy_verified_at            TIMESTAMP;

-- DOWN (manual only; the migrations DAG executes the whole file in ONE
-- transaction, so an uncommented DOWN block silently reverts the migration):
-- ALTER TABLE video_shorts
--     DROP COLUMN IF EXISTS copy_verification_verdict,
--     DROP COLUMN IF EXISTS copy_verification_findings,
--     DROP COLUMN IF EXISTS copy_original_title,
--     DROP COLUMN IF EXISTS copy_original_description,
--     DROP COLUMN IF EXISTS copy_corrected_title,
--     DROP COLUMN IF EXISTS copy_corrected_description,
--     DROP COLUMN IF EXISTS copy_content_version,
--     DROP COLUMN IF EXISTS copy_verified_at;
-- ALTER TABLE speaker_turn_videos
--     DROP COLUMN IF EXISTS copy_verification_verdict,
--     DROP COLUMN IF EXISTS copy_verification_findings,
--     DROP COLUMN IF EXISTS copy_original_title,
--     DROP COLUMN IF EXISTS copy_original_description,
--     DROP COLUMN IF EXISTS copy_corrected_title,
--     DROP COLUMN IF EXISTS copy_corrected_description,
--     DROP COLUMN IF EXISTS copy_thumbnail_text,
--     DROP COLUMN IF EXISTS copy_content_version,
--     DROP COLUMN IF EXISTS copy_verified_at;
```

`congress_videos/sql/production_schema.sql` is updated in lockstep (the drift test in
`tests/congress_videos/sql/test_production_schema.py` enforces it), plus a
`test_migration_050.py` mirroring `test_migration_043.py`, including an assertion that
the `DOWN` block is commented.

### D5 — Evidence assembly

The evidence bundle is a plain dict, rendered into the user template as a labelled
block. Canonical vs raw is explicit and both are present:

| Key | Source | Canonical or raw |
|---|---|---|
| `speaker.slug` | `db.get_turn_speaker_slug(turn_id)["resolved_participant_slug"]` (`database.py:1893`) | — |
| `speaker.display_name` | `lookup_participant_by_slug(slug)["display_name"]` (`participants_db.py:174`) | **raw** — ground-truth identity |
| `speaker.short_name` | `canonical_display_name(slug)` (#511); omitted when `None` | **canonical** |
| `speaker.party`, `speaker.parliamentary_group` | same roster row, free text verbatim | raw |
| `speaker.resolution_confidence`, `speaker.resolution_method` | `get_turn_speaker_slug` | — |
| `chapter.title`, `.description`, `.topics`, `.speakers`, `.key_speakers`, `.scoring_reasoning` | `db.get_chapter_metadata(chapter_id)` (`database.py:1866`) | raw |
| `chapter.session_number`, `.session_date` | same call, LEFT JOIN to `youtube_source_videos` | raw |
| `mentioned[]` | `mentioned_participant_slugs` → per slug `{slug, display_name (raw), short_name (canonical), party}` | both |

`mentioned_participant_slugs` is tri-valued: `NULL` renders `"mencionados": "no analizado"`,
`{}` renders `"mencionados": []` (analysed, nobody mentioned), a populated array renders
the entries. The prompt must not read "no analizado" as "nadie mencionado".

Under verification, long-form:

| Field | Source |
|---|---|
| `title` | `upload_config["videos"][0]["title"]` (originates at `youtube_upload_dag.py:969`, sidecar-round-tripped) |
| `description` | `upload_config["videos"][0]["description"]` (originates at `:984` via `_extract_metadata_description`) |
| `thumbnail_text` | `db.get_chosen_thumbnail(chapter_id)["art_direction_brief"]["text"]` (`database.py:2385`, JSONB written by `persist_results`, `modules/thumbnail_generation.py:879-903`) |

`art_direction_brief` may be `NULL` or a legacy string (both handled today at
`video_analytics_actions_dag.py:298`); either case omits `thumbnail_text` from the
payload entirely, and no thumbnail finding is then possible. `thumbnail_result` XCom
carries only `chapter_id/success/output_path/title`
(`generic_thumbnail_generator_dag.py:344-349`), so the DB read is mandatory.

Shorts: `title` and `description` come from the `shorts_metadata` entries; evidence
reuses the `ch` row and `turn_speaker_slug` that `_generate_metadata` already reads
(`reap_shorts_uploader_dag.py:255, 276`), adding only the roster lookup. No
`thumbnail_text` key.

On acceptance of a correction, long-form re-pushes the patched `upload_config` **and**
rewrites the sidecars via `_write_orador_sidecars` (`youtube_upload.py:16`) so disk and
published copy stay identical; shorts rewrite the `shorts_metadata` entry in place
before `t3` zips it.

### D6 — The two prompt constants

Added to `congress_videos/config/ai_prompts.py`, matching the file's existing shape
(system prompt as a parenthesised string concatenation ending in the JSON schema; user
template with `{}` placeholders and doubled braces in the literal JSON).

`FINAL_COPY_VERIFICATION_SYSTEM_PROMPT` (Spanish, neutral/professional register):

```
Eres un verificador editorial independiente de textos que van a publicarse en YouTube
sobre sesiones del Congreso de los Diputados de España. Recibes hasta tres textos
(título, descripción y texto de miniatura) y un bloque de EVIDENCIA verificada
procedente de la base de datos. Tu única función es comprobar los textos contra esa
evidencia. No eres el redactor.

Reglas:
- Responde ÚNICAMENTE con JSON válido y nada más.
- La EVIDENCIA es la única fuente de verdad. No inventes identidades, cargos,
  afiliaciones, cifras ni afirmaciones que no estén en la evidencia.
- Si un texto es correcto según la evidencia, no lo cambies.
- Nombres de personas: comprueba grafía, tildes y apellidos contra la evidencia.
  Trata como error el nombre de una persona distinta a la que indica la evidencia.
- Partidos y grupos: la evidencia guarda el partido como texto libre. Variantes,
  abreviaturas, siglas y federaciones territoriales del MISMO partido NO son errores
  (por ejemplo «PSOE», «PSC-PSOE» y «PSE-EE (PSOE)» son el mismo partido). Señala un
  error de partido SOLO cuando el texto atribuye una fuerza política que CONTRADICE la
  evidencia. Ante la duda, no señales nada.
- Ortografía, gramática y uso del español: señala errores reales, no preferencias de
  estilo.
- Afirmaciones no respaldadas: señala lo que el texto da por hecho y la evidencia no
  sostiene.
- El texto de miniatura se verifica pero NUNCA se corrige: no lo incluyas en
  "corrected".
- "corrected" solo puede contener "title" y "description", y solo cuando verdict es
  "correctable". Cada valor corregido debe poder derivarse de la evidencia y del texto
  original: no introduzcas ningún nombre propio, partido ni dato ausente de ambos.
- verdict: "pass" si no hay hallazgos que exijan cambios; "correctable" si los hay y
  puedes corregirlos dentro de la evidencia; "reject" si el texto contiene un error de
  identidad o una afirmación no respaldada que no puedes corregir con la evidencia
  disponible.

Esquema JSON: {"verdict": "pass"|"correctable"|"reject", "findings": [{"field":
"title"|"description"|"thumbnail_text", "category": "person_name"|"party_name"|
"spelling"|"grammar"|"language"|"unsupported_claim", "severity": "low"|"medium"|"high",
"detail": "<una frase>", "suggestion": "<texto o cadena vacía>"}], "corrected":
{"title": "<texto>", "description": "<texto>"}, "rationale": "<una frase>"}
```

`FINAL_COPY_VERIFICATION_USER_TEMPLATE`:

```
TEXTOS A VERIFICAR:
{copy_block}

EVIDENCIA VERIFICADA (base de datos):
{evidence_block}

Devuelve ÚNICAMENTE JSON válido con el esquema indicado.
```

The recheck reuses both constants with the corrected texts in `copy_block`, which is
also why the recheck gets its own cache key.

### D7 — Observability

Long-form: one `logging.info` per verification carrying `turn_id`, `output_path`,
`verdict`, finding count and the field/category pairs; XCom key `copy_verification`
with `{verdict, findings, corrected_applied, persisted, content_version}`. A new
`_copy_verification_problems(payload)` helper — shaped exactly like the existing
`_turn_marking_problems` — is appended to the `problems` list inside
`_check_upload_failures` (`youtube_upload_dag.py:1141-1189`), so a `reject` on
description or thumbnail text, a discarded unsupported correction, or a persistence
failure surfaces as one more accumulated finding without short-circuiting the others.
A missing `copy_verification` XCom is itself a finding, never a short-circuit raise.

Shorts: task log plus the `shorts_copy_verification` XCom only. The shorts DAG has no
accumulator and this change does not add one (locked decision 3).

Only the long-form **turn title** raises, reusing the existing `ValueError` at
`youtube_upload_dag.py:971-976`; no new exception type, no change to `retries=1`.

### D8 — Slice boundary

| # | Slice | Contents | Est. changed lines |
|---|---|---|---|
| 1 | Migration + schema | `050_*.sql`, `production_schema.sql`, `test_migration_050.py`, drift-test update. No consumers. | ~140 |
| 2 | Pure module + prompts | `final_copy_verification.py`, the two prompt constants, verdict parsing, containment, bounded round, unit tests. Not wired. | ~380 |
| 3 | Long-form seam | new task between `t6` and `t7`, `db.record_copy_verification_turn`, config/sidecar patching, accumulator helper, DAG tests. | ~350 |
| 4 | Shorts seam | new task between `t2` and `t3`, `db.record_copy_verification_short`, DAG tests. | ~260 |

Stacked to main; each slice is independently test-green (`uv run pytest`,
`ruff check`, `ruff format --check`) and independently revertible. Slice 2 is the
tightest against the 400-line budget; if the containment helper plus its table-driven
tests push it over, split the containment helper and its tests into slice 2a.

## Data Flow

```
long-form:  t6 prepare_upload_config ──► upload_config (XCom)
                                              │
                    ┌─────────────────────────┴──────────────────────────┐
                    │  t6b verify_final_copy (NEW)                       │
   evidence  ──────►│   assemble → verify → [1 correction + recheck]     │
   (DB reads)       │   → containment → patch config + sidecars          │
                    │   → guarded UPDATE speaker_turn_videos             │
                    └─────────────────────────┬──────────────────────────┘
                                              ▼
                                   t7 trigger_youtube_upload ──► ... ──► t9 _check_upload_failures

shorts:     t2 generate_metadata ──► shorts_metadata ──► t2b verify_final_copy (NEW) ──► t3 trigger_youtube_upload
                                                              └► guarded UPDATE video_shorts
```

## File Changes

| File | Action | Description |
|---|---|---|
| `congress_videos/modules/final_copy_verification.py` | Create | Verifier module: evidence assembly, prompt render, defensive parse, bounded correction, containment, content version |
| `congress_videos/config/ai_prompts.py` | Modify | `FINAL_COPY_VERIFICATION_SYSTEM_PROMPT`, `FINAL_COPY_VERIFICATION_USER_TEMPLATE` |
| `congress_videos/sql/migrations/050_final_copy_verification_audit.sql` | Create | Audit columns on both tables, commented DOWN |
| `congress_videos/sql/production_schema.sql` | Modify | Lockstep snapshot |
| `congress_videos/modules/database.py` | Modify | `record_copy_verification_turn(output_path, ...)`, `record_copy_verification_short(short_id, ...)` — guarded UPDATEs returning rowcount |
| `congress_videos/youtube_upload_dag.py` | Modify | New task between `t6`/`t7`; `_copy_verification_problems` in `_check_upload_failures` |
| `congress_videos/reap_shorts_uploader_dag.py` | Modify | New task between `t2`/`t3` |
| `tests/congress_videos/modules/test_final_copy_verification.py` | Create | Module unit tests |
| `tests/congress_videos/sql/test_migration_050.py` | Create | Migration shape + commented-DOWN assertions |

## Interfaces / Contracts

```python
@dataclass(frozen=True)
class CopyFinding:
    field: str            # title | description | thumbnail_text
    category: str         # person_name | party_name | spelling | grammar | language | unsupported_claim | other
    severity: str         # low | medium | high
    detail: str = ""
    suggestion: str = ""

@dataclass(frozen=True)
class CopyVerdict:
    ok: bool = False                       # False ⇒ inconclusive; NEVER treat as pass
    verdict: str = ""                      # pass | correctable | reject ("" when not ok)
    findings: tuple[CopyFinding, ...] = ()
    title: str = ""                        # value to publish (original or accepted correction)
    description: str = ""
    correction_applied: bool = False
    rationale: str = ""
    content_version: str = ""              # sha256; "" when not ok
    rounds: int = 0                        # 0, 1 or 2 completion calls made

def verify_final_copy(
    *,
    title: str,
    description: str,
    thumbnail_text: str | None = None,
    evidence: dict,
    completion_fn: Callable | None = None,
) -> CopyVerdict:
    """Verify publication copy against trusted evidence. NEVER raises.

    Returns CopyVerdict(ok=False) on any failure, malformed response or
    unsupported correction; callers then publish the values they passed in.
    """
```

Caller contract: publish `verdict.title` / `verdict.description` when `ok` is true, the
values you passed in otherwise. Persist only when `ok` is true. Raise only when
`verdict.verdict == "reject"` and a `title` finding exists and the seam is the long-form
turn.

## Testing Strategy

| Layer | What to test | Approach |
|---|---|---|
| Unit (module) | consistent copy ⇒ `pass`; name correction; party variant is NOT a finding (`PSC-PSOE` vs `PSOE`); genuine party contradiction IS; spelling/grammar; unsupported claim; thumbnail flagged and absent from `corrected`; malformed JSON, unknown verdict token, `corrected` with a stray key, non-list findings ⇒ `ok=False`; unsupported correction discarded; `completion_fn` called ≤2 times on every branch; `content_version` stable across identical inputs and different across changed evidence | fake `completion_fn` returning canned dicts, table-driven |
| Unit (DB) | guarded UPDATE binds the right predicate; second call with the same `content_version` affects 0 rows; grouped `output_path` updates all sibling rows | `mock_psycopg2_connection` fixture, as in `test_thumbnail_generation.py` |
| Unit (SQL) | migration 050 columns, types, `IF NOT EXISTS`, DOWN block commented; `production_schema.sql` drift | regex assertions mirroring `test_migration_043.py` |
| Integration (DAG) | long-form: title `reject` raises the existing `ValueError`; description `reject` publishes and accumulates; inconclusive publishes unchanged and writes nothing; correction patches both config and sidecars; stale-copy mismatch skips the write. Shorts: correction rewrites `shorts_metadata`; `reject` publishes the fallback description | `mocker`-patched DB/verifier, existing DAG-test patterns |
| E2E | DagBag import stays clean | `bash scripts/test-airflow-e2e.sh` (`dags list-import-errors` empty) |

TDD per the loaded skill: one seam, one failing test, one minimal implementation. The
confirmed seams are `verify_final_copy`, the two `record_copy_verification_*` methods,
and the two DAG task callables — never module internals.

## Threat Matrix

N/A — no routing, shell, subprocess, VCS/PR automation, executable-file classification,
or process-integration boundary. The change adds one LLM call and DB column writes at
two existing in-process DAG task seams; the only subprocess in the touched region
(`ffmpeg` at `reap_shorts_uploader_dag.py:310`) is untouched.

## Migration / Rollout

Migration 050 lands in slice 1, ahead of every consumer, and must be applied in both
schemas before slices 3 and 4 reach production. All columns are nullable, so pre-existing
rows and inconclusive verifications legitimately carry `NULL`. Removing the verifier task
restores exactly today's behaviour at both seams; the audit columns are inert without it.

## Open Questions

- [ ] None blocking. The `copy_thumbnail_text` column on `speaker_turn_videos` is a
      refinement beyond the proposal's column list, added so a flagged thumbnail text is
      auditable rather than only described inside `copy_verification_findings`.
