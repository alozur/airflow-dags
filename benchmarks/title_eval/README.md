# Title evaluation corpus and workflow

Groundwork for issue #510: an auditable benchmark for YouTube title generation,
and the loop that turns it into prompt changes worth promoting.

The premise is that a prompt rewrite that "reads better" proves nothing. To
promote a change we need a frozen set of real inputs, an explicit rubric, and a
judge whose agreement with a human has been measured. This directory holds the
first two and the data needed to build the third.

## What is here

| Path | Purpose |
|------|---------|
| `corpus/v1/observed.jsonl` | Every title this channel has published, joined to its live YouTube outcome |
| `corpus/v1/replay.jsonl` | Items whose generator inputs survive in the database, so a candidate prompt can be re-run on them |
| `corpus/v1/label_sheet.jsonl` | A stratified slice awaiting human verdicts — the starting point of the loop |
| `corpus/v1/manifest.json` | Provenance, counts and content hashes for this corpus version |
| `extract/` | The SQL and the fetch script that reproduce the raw extracts |
| `build_corpus.py` | Assembles the corpus from those raw extracts |
| `analyze_observed.py` | Failure-mode reconnaissance over the observed half |
| `feature_lift.py` | Surface title features against view counts, within one kind |
| `make_label_sheet.py` | Draws the stratified labelling slice |

## Which generator produces what

| Content kind | Generator | Live today |
|---|---|---|
| Long-form chapter | `thumbnail_generation.generate_title` | No — chapter selection has been unreachable from the scheduled DAG since issue #171 made upload selection turn-only |
| Speaker turn | `thumbnail_generation.generate_title` | Yes |
| Short | `reap_shorts_uploader_dag._generate_metadata` | Yes |

Since issue #512 a separate verifier, `final_copy_verification.verify_final_copy`,
runs after generation and immediately before the upload call, and can rewrite or
reject a title. Any end-to-end evaluation has to account for it: the string that
reaches YouTube is not always the string the generator returned.

## Replay gaps — read this before designing an A/B run

Issue #549 closed these gaps going forward: migration
`051_persist_title_generation_input` records the generator's input payload
beside its output, for both live generators. Every title published from that
migration onward is a reproducible evaluation case, and the replay set grows on
its own.

It does not recover the past, and it could not. Of the 957 items in this
corpus, **52 are replayable, and every one of them is on the legacy chapter
path**. For everything published before #549 the inputs are gone:

- **Turns.** `generate_title` takes a `best` argument — the chosen Pikzels
  thumbnail option, whose `style` and `prompt` it interpolates into the prompt
  text. That option was stored per *chapter*, never per turn, and successive
  turns of one chapter overwrote the row.
- **Shorts.** `_generate_metadata` prompts on a Whisper transcript produced on
  the fly from the clip. The transcript was never persisted and the clip is
  reaped, so the prompt input is gone.
- **Sibling titles.** `fetch_recent_thumbnail_history` is a rolling `LIMIT 5`
  window. The as-of set was never recorded, so a replay of a pre-#549 title
  must pass an explicit frozen list rather than re-query.

This is why the corpus keeps two halves. The historical half carries no
replayable inputs, but it is what the rubric and the judge are derived from —
and that work does not need replay, only published titles and a human verdict.

## The loop

The human is not in the iteration; the human is the source of ground truth,
once, at the start.

1. **Label.** Fill `verdict` and `why` in `corpus/v1/label_sheet.jsonl`. The
   `why` matters more than the verdict — the rubric is derived from it.
2. **Derive the rubric.** Turn the recurring reasons into explicit criteria.
   The issue names the axes to cover: factual attribution, grammatical quality,
   specificity, relevance, length, non-repetition.
3. **Calibrate a judge.** Score the labelled slice with an LLM judge and
   measure its agreement with the human verdicts. Below roughly 85% agreement
   the judge is not usable and the rubric needs sharpening, not the judge.
4. **Iterate automatically.** Run a candidate prompt against the frozen replay
   set, score with the calibrated judge, and compare to the baseline.
5. **Promote on measured improvement only,** never on a better-looking sample.
6. **Freeze regressions.** Any case where the judge and the human disagree, or
   a new failure mode appears, becomes a permanent fixture.

## Reproducing the extracts

The queries read the `production` schema; the fetch script runs **inside the
production container** so `YOUTUBE_API_KEY` never leaves the NAS. Only video
ids — already public — are sent outward, and only public metadata comes back.

```bash
# 1. Published video ids by kind
ssh nas '/usr/local/bin/docker exec -i postgres_shared psql -U admin -d congress_videos' \
  < extract/01_published_ids.sql > raw/video_ids.csv

# 2. Generator inputs and context
ssh nas '/usr/local/bin/docker exec -i postgres_shared psql -U admin -d congress_videos' \
  < extract/02_chapter_context.sql > raw/chapter_context.json
ssh nas '/usr/local/bin/docker exec -i postgres_shared psql -U admin -d congress_videos' \
  < extract/03_turn_shorts_context.sql > raw/turns_shorts_raw.txt

# 3. Published titles and outcomes, fetched from inside the container
ssh nas 'tee /tmp/fetch.py > /dev/null && /usr/local/bin/docker cp /tmp/fetch.py airflow-scheduler-prod:/tmp/fetch.py' \
  < extract/fetch_published_titles.py
cut -d, -f2 raw/video_ids.csv \
  | ssh nas '/usr/local/bin/docker exec -i airflow-scheduler-prod python /tmp/fetch.py' \
  > raw/published_titles.jsonl

# 4. Assemble
uv run python build_corpus.py --raw-dir raw --out-dir corpus/v1
```

## Reading the numbers honestly

`feature_lift.py` reports correlations over titles the channel happened to
publish. Topic, thumbnail, publication date and the recommendation surface are
all uncontrolled, and the stored analytics snapshots carry **no impression
counts**, so views cannot distinguish a title nobody clicked from a video
nobody was shown. Treat every ratio as a hypothesis for the labelling pass to
confirm or kill — never as a promotion criterion.

Two consequences worth stating plainly:

- **Chapter view counts sit at a median of 6.** Any split of that population
  moves on a handful of views and carries no signal, whatever the ratio says.
- **Shorts and long-form are not comparable.** They are distributed through
  different surfaces. Every split is computed within one kind for that reason.
