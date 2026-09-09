# Title scoring rubric — v2

Derived from the 60-item human labelling pass over `corpus/v1/label_sheet.jsonl`,
not from taste or from general copywriting advice. Every rule below is traceable
to labelled items, and the ones the human overturned are marked as such.

Issue #510 names six axes: factual attribution, grammatical quality,
specificity, relevance, length, and non-repetition. They are all here, but
**specificity does not mean what it looks like it means on this channel** — see
axis 3.

## How the labelling pass constrains this rubric

| Measure | Value |
|---|---|
| Items labelled | 60 / 60 |
| Verdict agreement, model vs human | 53 / 60 = **88%** |
| Score within 1.0 | 53 / 60 = 88% |
| Items the model scored **too low** | 10 |
| Items the model scored **too high** | **0** |

The one-sided spread is the finding. The model and the human agree on every
mechanical and factual defect; disagreement lives entirely in two flags:

| Flag | Share of model's errors | Share of model's correct calls |
|---|---|---|
| `sin-actor` | 60% | 28% |
| `cargo-generico` | 30% | 6% |
| `nombre-roto`, `prosa-colada`, `duplicado`, `cola-muerta`, `enfoque`, `formula-senoria` | **0%** | 12–22% |

So the mechanical defects below are settled — human and model never disagreed
on one. Axis 3 is the axis the labelling pass rewrote, and axis 1 is the one the
first judge run rewrote.

**v2 changelog.** A judge scoring against v1 reproduced the human labels only
67% of the time, against an 88% bar. Two causes, both mine: v1 made a mangled
name a hard failure the labels do not support, and v1 asked for H3 without the
harness ever showing the judge that a title was a repeat. Both are fixed below.

## Hard failures — cap the score at 2.5

These are defects, not preferences. No strength elsewhere redeems them, and a
title carrying one is never publishable.

A rule only belongs here when the human's own scores cluster at the floor. H1's
six items all scored 1.0-1.5, so the cap is real. v1 also listed a mangled name
here; the labels put those items between 2.5 and 6.5, so it was demoted to a
penalty on axis 1.

**H1 · Leaked prose.** The chapter summary reaches the title, usually opening
with "En este segmento…" and cut mid-sentence. 6 items.
*Root cause, not a prompt matter:* `video_chapters.title` is prose — 433 of 439
rows are a literal prefix of `description` and 396 begin "En este segmento".
Since `debate_summary = title + "\n" + description`, the generator is handed the
same sentence twice.

**H3 · Byte-identical to another published title.** 8 items. The second
occurrence is the offender, not the first. The harness must supply this as
evidence: a judge shown one title in isolation cannot see a duplicate, and
will score the repeat exactly like the original.

**H4 · Wrong subject.** The title takes a secondary thread of the chapter and
drops the primary one. 6 items. `chapter-102` is titled on immigration while the
chapter is mostly about violencia machista; `chapter-121` picks Israel over
Spain's NATO membership and drops Belarra entirely.
*Not the same as fabrication:* in both cases the topic really was in the
chapter's `topics`. Check emphasis against the primary topic, not mere presence.

## Scored axes

Start at 5. Apply every rule that fires. Clamp to 0–10.

### 1 · Factual attribution — ±2.5

**v1 got this wrong and the judge caught it.** v1 made a mangled name a hard
failure capping the score at 2.5. The human labels refute that outright: the
five titles carrying a broken name scored **2.5, 3.5, 4.5, 5.0 and 6.5** —
spread across the whole range, so it is a penalty, not a gate. Compare H1,
where all six items scored 1.0–1.5 and the cap is correct.

Check names mechanically against `congress_participants` (`name_check.py`),
never by eye. Ministers are absent from that table, so a real cabinet surname
returning no row is a coverage gap in the registry, not a defect.

- **−1.5** a name close to a registry entry but misspelt or truncated:
  "Calv Gómez" ← Calvo Gómez, Pilar · "Eser Muñoz" ← Muñoz de la Iglesia, Ester ·
  "Jordán" ← Jordà i Roura.
- **−2.5** a person-shaped label that names nobody at all: "Interviniente 1",
  or "la Señora de Sumar", which treats a party as a person.
- **−2.5** attributing a statement to the wrong side. `short-745`
  ("Sánchez: ¿Por qué seguimos aguantando un gobierno corrupto?") reads as
  Sánchez attacking his own government; the opposition said it.

### 2 · Grammatical quality — ±1.5
- **−1.0** missing article where Spanish requires one
  (`chapter-33` "Protección consumidores", `chapter-171` "reforma Constitución").
- **−0.5** opens with a gerund (`chapter-47` "Protegiendo…").
- **−0.5** two nouns joined with no relation between them
  (`short-856` "Racismo y privatización").
- **+0.5** a real quote, correctly attributed, doing the work of the headline
  (`chapter-328`, `short-1251`).

### 3 · Notoriety, not specificity — ±2.0

**This axis was rewritten by the labelling pass.** The model assumed
"specificity = name the person" and was wrong on 10 of 10 disagreements.

The human's rule, in their own words: *"no le conoce la gente pero un ministro
da notoriedad"* · *"Vázquez Blanco no es una persona muy relevante"* (they
rewrote that title to **delete** the name) · *"es un político irrelevante"* ·
*"el cargo no está mal del todo"*.

- **+1.5** names a figure the general audience recognises — Sánchez, Feijóo,
  Abascal, Belarra, Álvarez de Toledo, Zapatero.
- **+1.0** uses the *office* for a speaker the audience would not recognise
  ("el Ministro de Hacienda", "la ministra de Inclusión"). This is a **gain**,
  not the penalty the model applied.
- **−1.0** names an unknown backbencher and nothing else. The name is dead
  weight where the office would have pulled.
- **−1.5** parliamentary courtesy form ("Señor Quero", "la señora Belarra").
  It is chamber register, not audience register, and it identifies nobody —
  three different Quero exist in the register.
- **−1.0** neither person nor office nor concrete event: pure abstraction
  (`short-1232`, `short-1076`).

A party or institution as the actor is **not** penalised when the fact behind it
is concrete — `short-1119` ("¡Vox denuncia el despilfarro en RTVE!") was scored
9 by the human.

### 4 · Relevance and concreteness — ±2.0
- **+1.0** a specific, checkable fact: a named case, a figure, a place
  (Plus Ultra, RTVE, el canal de Navarra, la flotilla a Gaza).
- **+1.0** **the most striking element comes first.** The human's note on
  `chapter-278`: *"Estaría bien que empezara con lo más llamativo. Como: el PP
  rechaza la ley del derecho a morir"* — the original buried "pese al rechazo
  del PP" at the end, behind the procedure.
- **−1.0** dead tail: a clause that is always true and therefore informs nobody
  ("El Congreso debate", "en el debate", "La polémica en el Congreso").
- **−1.0** opens by announcing the format instead of the fact
  ("Debate sobre…", "Debate:").
- **−0.5** an opaque phrase the viewer cannot decode ("Joyas de sangre").
- **−0.5** a verb that oversells what follows ("revela" on a routine answer).

### 5 · Length — ±1.0
Measured on published titles, not asserted: the top decile by views sits at a
median of 65 characters, and titles over 70 run at 0.72× the median views of
shorter ones within the same kind.
- **+0.5** at or under 65 characters.
- **−0.5** over 78 characters.
- **−1.0** at or over 90, which is the validator's own ceiling — `short-527`
  shipped at 94.

### 6 · Non-repetition — covered by H3
Beyond exact duplicates, **−0.5** for near-duplicate phrasing across sibling
titles of one chapter (three of the four "Señor Quero denuncia…" titles).

## Bands

| Score | Verdict | Meaning |
|---|---|---|
| ≥ 8 | `good` | Publishable as is |
| 5 – 7.5 | `weak` | Publishable, leaves value on the table |
| ≤ 4.5 | `bad` | Should not have shipped |

Calibrated against the labelled set: 1 `good`, 22 `weak`, 37 `bad`, median 4.5.

## What this rubric cannot fix

H1, H3, H4 and the name defects on axis 1 cover **31 of the 60 titles** and are
caused by malformed input or missing deduplication, not by prompt wording. No
prompt rewrite reaches them. Only axis 3's notoriety gate is a prompt change.

Grade a candidate prompt on axes 2–6 and on H1's absence; treat the name
defects on axis 1, plus H3 and H4, as separate engineering work.

## Using this as a judge — score pairs, not titles

Measured, not assumed. Both numbers come from the same judge (`gpt-5.5`)
reading this rubric over the same 60 labelled titles:

| Method | Agreement with the human |
|---|---|
| Absolute score, 0-10, one title at a time | **65%** |
| Pairwise, "which of these two is better?" | **88%** |

Absolute scoring fails, and it fails structurally rather than by a fixable
margin. The judge's scores correlate with the human's at Spearman 0.60 — it
ranks titles differently, not merely lower — and adding a constant offset to
close the 0.68-point gap moves agreement not at all (65% at +0.0, +0.25 and
+0.5; 57% at +1.0). There is no calibration constant to find.

Pairwise clears the bar on the first attempt: 88% of the time it picks the
title the human scored higher, it is order-independent on 95% of pairs, and
85% of pairs are both consistent under swap and correct. Run it with
`pairwise.py`; `judge.py` remains for diagnosis.

That is also the question the loop actually asks. Gating a prompt change never
needs a title's absolute worth — it needs to know whether the candidate beat
the baseline on the same input, which is a comparison. Scoring each side
separately and subtracting throws away accuracy to answer a question nobody
asked.

**The judge must not be the model that writes the titles.** Generation runs on
`LLM_DEFAULT` (`gpt-5.6-luna`, see `utils/llm_config.py`); a judge on that model
scores its own output and prefers it. Re-measure whenever either model changes.

## What the labels can and cannot carry

55 of the 60 rows are `confirmado` — the human reviewed the model's score and
let it stand — and 5 are corrections carrying written reasons. That is a real
endorsement and not silence, but it is thinner evidence than "60 independent
labels", and every weight in this rubric rests on it. Treat the axis weights as
provisional, and prefer adding labelled pairs over tuning the numbers.
