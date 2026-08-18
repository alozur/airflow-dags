# Design: Speaker-Turn Detection Inside video_chapters

**Change:** `speaker-turns-detection`
**Source:** GitHub issue #86 (epic #16, split from #17)
**Status:** Design — resolves the open decisions the spec deferred

## Decision

Deliver a pure, Airflow-free module `congress_videos/modules/speaker_turns.py` fronted by a thin
on-demand DAG `speaker_turns_dag.py`. The module fuses two signals — acoustic boundaries from an
injectable `diarize_fn` (default: isolated Docker pyannote subprocess) and Spanish president-announcement
text from SRT blocks — into a list of resolved `Turn` records, then upserts them idempotently into a new
`speaker_turns` table. Everything except the Docker call and the DB upsert is a pure function, unit-tested
with a stubbed `diarize_fn` and SRT-block fixtures. The pipeline is additive: no existing DAG, table, or
module public interface changes.

## Module Shape (`modules/speaker_turns.py`)

```python
@dataclass(frozen=True)
class Turn:
    start_seconds: float
    end_seconds: float
    speaker_label: str          # acoustic cluster label, e.g. "SPEAKER_01"
    resolved_name: str | None   # canonical participant display_name, nullable
    confidence: float           # 0.0–1.0
    source: str                 # "text_named" | "text_confirmed" | "acoustic"

# Injected boundary producer. Wire format == benchmark postprocessed-speaker-changes.json.
DiarizeFn = Callable[[str, float], list[dict]]
#   input:  (wav_path, chapter_offset_seconds)
#   output: [{start_seconds, from_speaker, to_speaker, confirmed_block_duration_seconds}, ...]
#           start_seconds already rebased to chapter-relative time (offset applied by caller of diarize).

def detect_turns(
    chapter: dict,
    srt_blocks: list[dict],          # [{start_secs, end_secs, text}] pre-filtered to the chapter window
    diarize_fn: DiarizeFn,
    name_resolver: Callable[[str], dict | None] = lookup_participant_fuzzy,
) -> list[Turn]: ...
```

`detect_turns` is the orchestrator: it calls `diarize_fn`, converts changes into candidate segments,
runs the four-step postprocessing pipeline, and returns `Turn[]`. It touches no Airflow context, no DB,
and no Docker — all side-effecting collaborators are injected. `confirmed_block_duration_seconds` is
carried through but NEVER used as an acceptance threshold (Fase 0 dead end).

## President-Announcement Extractor (pure)

`extract_announcement(srt_blocks, t, window=30.0) -> tuple[str | None, bool]` scans blocks whose
`[start_secs, end_secs]` intersect `[t − window, t + window]` and returns `(raw_name_or_None, phrase_found)`.

Patterns (case-insensitive, accent-tolerant, compiled once):

| Pattern | Captures |
|---|---|
| `tiene la palabra (?:el señor\|la señora)\s+(?P<name>[\wÁÉÍÓÚÑáéíóúñ.\- ]+?)(?:[.,]\|$)` | `name` → fuzzy lookup |
| `tiene la palabra su señoría` | phrase-only (confirm, no name) |
| `gracias,?\s+señoría` | phrase-only (handover marker, confirm) |

Name pull: from the block(s) in the **~15–30 s window before** the change `t` (announcements precede the
new speaker), preferring the closest preceding block. The captured name string is passed to `name_resolver`
(default `lookup_participant_fuzzy`, threshold 0.90). When no phrase matches in the window, the extractor
returns `(None, False)` and the caller falls back to `name_resolver(speaker_label)`-style fuzzy on any
available context (nullable result). Pure and testable on SRT-block fixtures.

## Source Routing and Confidence

| Condition | `source` | `confidence` |
|---|---|---|
| phrase found AND name resolves to a participant | `text_named` | `0.95` |
| phrase found BUT name not resolvable | `text_confirmed` | `0.80` |
| no phrase in window (survives postprocessing) | `acoustic` | `0.50` |

`text_named` / `text_confirmed` require `phrase_found=True`. An `acoustic` change is kept only if it is NOT
rejected by ping-pong/same-speaker collapse; its `resolved_name` may be `None`. This makes the text layer
the confirm/deny gate the audit prescribed — acoustic-only detections are deliberately low-confidence.

## Postprocessing Pipeline (ordered, pure)

1. **Gap merge** — adjacent same `speaker_label` with gap `< GAP_MERGE_SECONDS (1.0)` → one segment.
2. **Ping-pong collapse** — `A → B → A` collapsed into one `A` when `B_duration < PINGPONG_B_MAX_SECONDS (5.0)`
   AND `A` resumes within `PINGPONG_RETURN_SECONDS (10.0)`. Targets the audit's **26.9 % redundant duplicates**.
3. **Text gate** — each surviving change evaluated via `extract_announcement`; assigns `source`/`confidence`
   and drops changes that resolve to the same speaker on both sides with no phrase (same-speaker pitch shift,
   the audit's noise class).
4. **Same-name merge** — adjacent turns with identical non-null `resolved_name` → one turn.

All thresholds are module constants (tunable), grounded in Fase 0 (`same_speaker_gap_seconds: 1.0` in the
benchmark rules; B/return windows chosen to catch the 3–8 s ping-pong pairs observed in the audit).

## Diarization Docker Subprocess

Default `diarize_fn` wraps a subprocess call; tests inject a stub and never reach it:

```
docker run --rm --network none --memory 4g --cpuset-cpus 0-1 \
  -v {wav_dir}:/in:ro -v {out_dir}:/out \
  pyannote/speaker-diarization-community-1 \
  --input /in/{wav_name} --output /out/changes.json
```

The module builds the arg list, runs it via `subprocess.run` with an adaptive timeout (audio is 4.6×
realtime → `timeout ≈ base + factor·duration`), then reads and JSON-parses `/out/changes.json` into the
wire format above. The worker needs `docker` access (NAS ops: `/usr/local/bin/docker`, no sudo). Source WAV
mounted read-only; only the output dir is writable. pyannote/torch never enter the Airflow image.

## Migration

Highest on disk NOW is `019_create_video_thumbnails.sql`; `dev` may carry `020`. Next number is **`021`**
(tasks phase MUST re-verify highest+1 against `dev` before creating the file).

```sql
-- 021_create_speaker_turns.sql
CREATE TABLE IF NOT EXISTS speaker_turns (
    turn_id       SERIAL PRIMARY KEY,
    chapter_id    INTEGER NOT NULL REFERENCES video_chapters(chapter_id) ON DELETE CASCADE,
    start_seconds NUMERIC NOT NULL,
    end_seconds   NUMERIC NOT NULL,
    speaker_label TEXT    NOT NULL,
    resolved_name TEXT,
    confidence    NUMERIC NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    source        TEXT    NOT NULL CHECK (source IN ('acoustic','text_confirmed','text_named')),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (chapter_id, start_seconds)
);
CREATE INDEX IF NOT EXISTS idx_speaker_turns_chapter ON speaker_turns(chapter_id);
CREATE INDEX IF NOT EXISTS idx_speaker_turns_name    ON speaker_turns(resolved_name);
```

Upsert: `INSERT ... ON CONFLICT (chapter_id, start_seconds) DO UPDATE SET ..., updated_at = NOW()`.

## DAG Shape (`speaker_turns_dag.py`)

`schedule=None`, conf-driven, mirrors `generic_thumbnail_generator_dag`. Task graph:

```
select_chapters (LIMIT from conf, uploadable_chapters view)
    → process_chapters  (per chapter, graceful skip)
        locate audio (_find_source_video → extract_audio_wav, bounded -ss/-t)
          → detect_turns(chapter, window srt_blocks, diarize_fn, fuzzy)
            → upsert speaker_turns (ON CONFLICT DO UPDATE)
```

Conf keys: `limit` (default e.g. 10), optional `chapter_ids`. `xcom_task` carries the selected chapter
list. Per-chapter failures are caught: missing source video → log + skip (DAG succeeds); missing SRT →
acoustic-only run. Never self-triggers; never chained into `youtube_upload_dag`.

## Data Flow

```
video_chapters row ──▶ extract_audio_wav (transient WAV slice, chapter window)
                          │
                          ▼
                     diarize_fn (Docker pyannote │ stub) ──▶ acoustic changes[]
                          │
   find_srt_for_chapter ──┤ (window blocks [t-30s, t+30s])
   _parse_srt_blocks      ▼
                     gap-merge → ping-pong collapse → text-gate/name → same-name merge
                          │
                          ▼
                     Turn[] ──▶ upsert speaker_turns (idempotent)
```

## Reuse (unchanged)

`extract_audio_wav`, `_find_source_video` (vad_helpers); `find_srt_for_chapter`, `_parse_srt_blocks`
(srt_helpers, window-filtered exactly as `_prepare_thumbnail_config`); `lookup_participant_fuzzy`
(participants_db); `xcom_task` (airflow_helpers). No signature or behavior change.

## Testing Strategy (strict TDD)

| Layer | What | Approach |
|---|---|---|
| Unit — postprocess | gap merge (<1.0 s) vs non-merge (≥1.0 s); ping-pong `A(60)→B(4)→A(90)` collapse; same-name merge across two labels | stub `diarize_fn` returning fixed change dicts; assert `Turn[]` |
| Unit — text gate | phrase confirms+names (`text_named`); phrase-only (`text_confirmed`); no phrase same-speaker noise rejected; fuzzy fallback (`acoustic`) | SRT-block fixtures + fake `name_resolver` |
| Unit — extractor | each regex pattern; name pulled from 15–30 s pre-change window; no-phrase → `(None, False)` | pure fixtures |
| Unit — degradation | missing-video skip returns `[]`; missing-SRT → acoustic-only lower confidence | inject empty srt_blocks / None video path |
| Unit — persistence | upsert idempotency: second run same `chapter_id` → row count unchanged, values updated | fake DB cursor asserting `ON CONFLICT` SQL |
| Smoke | `speaker_turns_dag` imports with no errors | DAG-load test |

No real Docker, pyannote, OpenAI, network, or live DB in tests — `diarize_fn`, `name_resolver`, and the DB
cursor are all injected/faked. RED tests are written before each production slice.

## Threat Matrix

The default `diarize_fn` builds a `docker run` subprocess argument list.

| Row | Status | Safe behavior / RED test |
|---|---|---|
| Shell/subprocess injection | Applicable | Arg-list `subprocess.run` (no `shell=True`); paths are module-built temp paths, not user text. Test asserts arg vector shape, `--network none`, `--memory 4g`. |
| Untrusted input to command | Applicable | WAV path derived from `_find_source_video` + generated temp name; chapter conf ints only. Test: non-int `chapter_ids` rejected before shell. |
| Resource exhaustion | Applicable | `--memory 4g`, `--cpuset-cpus`, adaptive subprocess timeout. Test asserts caps present in arg vector. |
| Network egress | Applicable | `--network none`. Test asserts flag present. |
| VCS/PR automation, executable-file classification, routing | N/A | No such boundary in this change. |

## Migration / Rollout

Additive. Deploy `021_create_speaker_turns.sql`, the module, the DAG, and tests together. Rollback: drop
`speaker_turns_dag.py`, `modules/speaker_turns.py`, and `DROP TABLE speaker_turns`. No existing DAG or table
is touched.

## Open Questions

- [ ] Ping-pong `B_MAX`/`RETURN` windows (5.0 s / 10.0 s) are first-cut defaults; confirm against a second
      labelled chapter before locking (tunable constants, not blocking).
- [ ] Exact `--cpuset-cpus` range depends on the target worker's core count (env-configurable).
