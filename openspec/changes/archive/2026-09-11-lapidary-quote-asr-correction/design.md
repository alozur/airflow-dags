# Design: Correct ASR errors in the lapidary thumbnail quote

## Technical Approach

Approach 2 from the proposal. `extract_lapidary_quote` keeps candidate extraction, ranking and index parsing byte-identical.
After `candidates[idx]` is selected, a pure gate `_risky_token_indices(quote)` runs. An empty set returns the quote as today,
with one LLM call in total. A non-empty set triggers one JSON correction call through the **same injected `completion_fn`**.
The response is validated, checked against the confidence threshold and then checked by a structural guard. The result is the
corrected text or `None`, and `None` falls through to the existing `art_direct` text in `_finalize_brief`. Separately,
`thumbnail_prompt.py` gains a shared `_DIACRITICS_LINE` placed in all three templates. The public signature does not change and
nothing is raised (fail-soft).

## Architecture Decisions

| Decision | Choice | Rejected | Rationale |
|---|---|---|---|
| Call seam | Reuse `completion_fn` (`generate_chat_completion` signature) + `utils.ai_helpers.parse_json_response` | New `correction_fn` param; direct `generate_json_completion` import | One DI seam; `generate_json_completion` is exactly chat+parse; existing `mocker.patch("utils.ai_helpers.generate_chat_completion")` test keeps working |
| Tier | `LLM_CHEAP` | `LLM_DEFAULT` | Proposal constraint; only the risky minority pays; weak recall degrades to low confidence → `None` (safe), never to a wrong name. One constant to bump later |
| Figures | Digit tokens open the gate but are **immutable** | Let model fix numbers | The model cannot know the true figure; the call acts as a verifier (low confidence → drop) |
| Caching | None | `cached_json_completion` | Quotes are unique per turn; consistent with the ranking call |
| Context | Quote + ±800-char window of `srt_fragment` around it | Quote only | Surrounding speech lets the model resolve the name; bounded prompt size |

### Risky-entity gate (pure)

Tokens are `quote.split()`. `bare(t)` strips edge punctuation with `re.sub(r"^\W+|\W+$", "", t)`. `cap(i)` is
`bare[i][:1].isupper() and bare[i].casefold() not in _RISKY_EXEMPT_WORDS`. The exempt set is `gobierno congreso senado estado españa
europa constitución presidente presidenta ministro ministra señoría señorías`. A token index is flagged when any of these holds:

- **R1**: `bare[i]` contains a digit.
- **R2**: `i >= 1` and `cap(i)`, which is a mid-clause proper-noun shape. Acronyms such as `PP` are flagged on purpose.
- **R3**: `i == 0` and `cap(0)` and index 1 is R2-flagged. This catches a clause that opens with a name run ("Aan Curdi").

| Quote | Flagged |
|---|---|
| `Aan Curdi tenía 3 años` (real candidate: the `, una camisa roja` clause is split off and stop-word filtered) | {0,1,3} |
| `Aan Curdi tenía años` / `nos costó 300 millones` / `lo dijo Pedro Sánchez ayer` | {0,1} / {2} / {2,3} |
| **Negatives**: `esto es una prueba seria`, `Esto es una vergüenza absoluta`, `Ustedes engañan al Gobierno` (exempt word), `¡Basta ya de mentiras!` | ∅ |

Accepted gaps: a lone capitalized first token followed by lowercase text is not flagged (for example `Curdi murió en la playa`),
because it cannot be told apart from sentence case without a lexicon. Fully lowercase misheard names are also not flagged.

### Correction contract

```python
LAPIDARY_CORRECTION_SYSTEM_PROMPT = (
    "Eres un corrector de transcripciones automáticas (ASR) de debates parlamentarios españoles. "
    "Recibirás una frase literal, las palabras sospechosas y un fragmento de contexto. "
    "Corrige SOLO la ortografía de las palabras sospechosas (nombres propios mal oídos) y las tildes que falten. "
    "No añadas, quites, reordenes ni sustituyas otras palabras; no cambies ninguna cifra; no reformules. "
    "Si no estás seguro de la forma correcta, devuelve la frase sin cambios y una confianza baja. "
    'Responde SOLO con JSON: {"corrected": "<frase>", "confidence": <número entre 0 y 1>}. '
    "confidence es tu certeza de que la frase devuelta no contiene errores de transcripción."
)
LAPIDARY_CORRECTION_USER_TEMPLATE = "Frase: {quote}\nPalabras sospechosas: {flagged}\nContexto: {context}"
_LAPIDARY_CORRECTION_MIN_CONFIDENCE = 0.8   # in thumbnail_generation.py
_LAPIDARY_CONTEXT_RADIUS = 800
_MIN_TOKEN_SIMILARITY = 0.5
```

A response is valid only when all of the following hold. Anything else returns `None` and logs a `logger.warning`:

- the content is non-empty;
- the parsed data is a `dict`;
- `corrected` is a `str` that is non-blank after `strip()`;
- `confidence` is an `int` or `float`, but not a `bool`, and lies in `[0, 1]`;
- `confidence` is `>= 0.8`.

A correction that returns the text unchanged with high confidence is accepted, so a correctly heard name still ships.

### Structural guard `_passes_correction_guard(original, corrected, flagged, max_chars)`

The whole text is rejected unless `len(corrected) <= max_chars` and both texts have the same `split()` token count. Each token pair
`(o, c)` is then checked by position, so order is preserved. `strip_accents` is NFD with the `Mn` combining marks dropped. The pair
is accepted when one of these holds:

1. `o == c`.
2. It is a **diacritic-only change on any token**: `strip_accents(o) == strip_accents(c)` (case-sensitive), and `c` has at least as
   many combining marks as `o`. Marks may be added or swapped, never removed.
3. It is a **replacement on a flagged token**, and all of these hold:
   - neither token contains a digit;
   - the edge punctuation is identical;
   - `bare(c)` matches `[^\W\d_]+(?:[-'][^\W\d_]+)*`;
   - the initial capitalization is the same;
   - `SequenceMatcher(None, fold(bare(o)), fold(bare(c))).ratio() >= 0.5`, where `fold` is `casefold` plus `strip_accents`.

   `Aan→Aylan` scores 0.75 and `Curdi→Kurdi` scores 0.8. `Curdi→Sánchez` scores 0.17 and is rejected.

Each helper stays under C901 max-complexity 10. There is no C901 per-file ignore for this module.

## Data Flow

    ranking (unchanged) ─→ quote ─→ _risky_token_indices ─→ ∅ ─────────────────→ quote
                                            └─ flagged ─→ _request_quote_correction (completion_fn, LLM_CHEAP)
                                                           └─→ schema+confidence ─→ guard ─→ corrected | None
    _finalize_brief: None ─→ keep art_direct text (existing)  │  str ─→ brief["text"] ─→ build_pikzels_prompt (+ _DIACRITICS_LINE)

## Diacritics line (`thumbnail_prompt.py`)

```python
_DIACRITICS_LINE = (
    "TEXT ACCENTS: render the quoted text exactly as written, keeping every Spanish accent and tilde "
    "(Á É Í Ó Ú Ü Ñ); never drop, replace, or transliterate them.\n"
)
```

The line goes at the end of each TEXT block: `...80% opacity).\n{diacritics_line}\n{logo_line}{safe_zone_line}NO logos...` in
A, B and C, and `build_pikzels_prompt` passes `diacritics_line=_DIACRITICS_LINE`. The line contains no `http`. `str.upper()`
already preserves accents, so `tenía` becomes `TENÍA`.

## File Changes (estimate: changed lines)

| File | Action | Description | ≈ |
|---|---|---|---|
| `congress_videos/modules/thumbnail_generation.py` | Modify | Imports (`unicodedata`, `difflib`, `parse_json_response`, 2 prompts), constants, `_bare`, `_strip_accents`, `_risky_token_indices`, `_is_allowed_token_change`, `_passes_correction_guard`, `_request_quote_correction`, wiring + docstring | 85 |
| `congress_videos/config/ai_prompts.py` | Modify | `LAPIDARY_CORRECTION_SYSTEM_PROMPT` / `_USER_TEMPLATE` | 16 |
| `congress_videos/modules/thumbnail_prompt.py` | Modify | `_DIACRITICS_LINE`, 3 template lines, 1 format kwarg | 10 |
| `tests/congress_videos/modules/test_thumbnail_generation.py` | Modify | Gate, correction, guard, prompt-constant tests (parametrized) | 140 |
| `tests/congress_videos/modules/test_thumbnail_prompt.py` | Modify | `TestDiacriticsLine` | 22 |
| **Code + tests** | | | **≈273** |

## Testing Strategy (strict TDD, `uv run pytest`)

| Spec area | Test | Approach |
|---|---|---|
| Gate flags risky entities | parametrized positives → exact index sets above | pure call |
| Gate ignores sentence case | 4 negatives → `frozenset()` | pure call |
| Happy path unchanged | lowercase quote → verbatim and **exactly 1** `completion_fn` call; the 8 existing tests plus the sentinel tests are untouched (their fixtures are lowercase and digit-free) | counting fake |
| Aylan corrected | fragment `Aan Curdi tenía 3 años, una camisa roja`, fake dispatching on `system_prompt`, correction returns `{"corrected":"Aylan Kurdi tenía 3 años","confidence":0.95}` → that string; 2 calls; the user prompt contains the quote | fake |
| Aylan fallback | confidence 0.4 → `None` | fake |
| Malformed output | `"banana"`, `""`, `{"content": None, "error": "x"}`, missing key, `confidence: true` → `None` | parametrized |
| Guard violations | extra word, reorder, changed non-flagged word (`meses`), changed digit (`4`), implausible name (`Pedro Sánchez tenía 3 años`), over `max_chars` → `None` | parametrized |
| Diacritic-only allowed | `Aan Curdi tenia 3 años` → `Aylan Kurdi tenía 3 años` accepted; removing an accent is rejected | fake |
| Prompt constants | importable; the template renders `{quote}`, `{flagged}` and `{context}`; the system prompt names `corrected` and `confidence` | string asserts |
| Diacritics line | present in A/B/C; `TENÍA` survives uppercasing; no `http` | `build_pikzels_prompt` |

## Threat Matrix

N/A: this change touches no routing, shell, subprocess, VCS/PR automation, executable-file classification or process-integration
boundary. The only external call is the existing never-raise LLM helper.

## Migration / Rollout

No migration and no state change. Rollback is a single revert.

**Budget:** code and tests come to about 273 lines. The openspec folder (proposal 83, explore 72, this design, and the spec and
tasks) adds about 330 more. That puts the PR at roughly 600 lines, which does not fit in 400 lines if the whole openspec folder
ships in the same PR. Precedent #555 (`chore/549-archive`) carried SDD docs in a separate archive PR. The orchestrator or
`sdd-tasks` must pick one: that split, trimmed docs, or `size:exception`.

## Open Questions

- [ ] The delivery split for the openspec docs, which decides whether the 400-line budget holds (see above).
