# Design: SRT artifacts for shorts + mentioned-people and topic analysis

## Technical Approach

Three additive slices, each mirroring an existing precedent rather than inventing a pattern:

1. **PR1** extends the issue-#340 sidecar pattern (`write_chapter_srt_sidecar`) to Reap shorts with
   `write_short_srt_sidecar`, hooked best-effort into `ReapJobSensor.poke` right after the MP4 download.
2. **PR2** adds migration `045` (`video_chapters.mentioned_participant_slugs TEXT[]`, plus a re-documenting
   `COMMENT ON COLUMN` for `topics`) and the pure `resolve_mentioned_people` module, a direct structural
   clone of `congress_videos/modules/chapter_speaker_resolution.py`.
3. **PR3** adds the pure `extract_topics` module and wires both analyses into the upload path behind one
   `_analyze_chapter_content` seam with independent try/except and a single only-successful-columns UPDATE.

Nothing in the pipeline may fail because of this change: every new code path is best-effort and degrades to
a logged no-op.

## Two codebase findings that override the proposal's stated plan

These were verified read-only against the worktree and are load-bearing. They do not reopen a confirmed
product decision; they correct two *implementation* assumptions the proposal inherited from exploration.

### F1 — the upload DAG is turn-only, so "hook next to `_resolve_chapter_speaker`" would be dead code

`_run_get_uploadable_item` (`youtube_upload_dag.py:397-415`) reads **only** `db.get_uploadable_turns(limit=1)`;
the chapter queue was retired by issue #171. `_resolve_chapter_speaker` is called at line 294 inside the
`else:` (chapter) branch of `_prepare_thumbnail_config`, which never executes in production. Hooking the two
analyses there would ship code that never runs.

**Correction:** hook them on the shared path of `_prepare_thumbnail_config`, after `blocks` is parsed
(line 341) and outside the `is_turn` branch. Turn rows carry `chapter_id`, `video_id` and `session_date`
(`uploadable_turns`, migration 044, lines 63-85) but NOT the chapter's `start_time`/`end_time`.
`_analyze_chapter_content` therefore obtains the chapter bounds via `db.get_chapter_srt_context(chapter_id)`
(introduced in PR1); when that call fails or returns `None`, both analyses are skipped with a WARNING and
nothing is written.

This is not a theoretical gap. The live uploader row is `SELECT * FROM uploadable_turns`
(`database.py:979`), passed unmodified into `_prepare_thumbnail_config` (`youtube_upload_dag.py:750-753`).
`select_unprepared_turns` (`database.py:1259`) *does* select `vc.start_time`/`vc.end_time`, but that is the
nightly prepare DAG's row, not the uploader's — reading the wrong one is exactly how this design first went
wrong. Had the analyses read `chapter["start_time"]` directly, `chapter_window_blocks(blocks, None, None)`
would fail safe to `[]` (`srt_helpers.py:302-315`), both analyses would receive empty text, nothing would
ever be persisted, and a unit test built on a fabricated row carrying `start_time` would stay green.

A second consequence: the already-computed `fragment` (line 351) is windowed to the **turn's** group span on
the live path. Chapter-level metadata must not be derived from a single turn's text. The design computes a
separate chapter-scoped text from the *same* already-parsed `blocks` — one extra indexed read, no extra
file I/O.

### F2 — `pretrim_start_secs` has two different meanings, but the ffmpeg cut settles the window

In `reap_clip_preparer_dag.py`, when `pretrim_used_srt=True` the offsets come from `select_pretrim_window`
over the **full-video merged SRT** (absolute source seconds, lines 187-208); when it is `False` they are
`0.0 .. target_secs` (chapter-relative). Both are then handed to `_ffmpeg_extract_window(source_path=clip_path, ...)`
where `clip_path` is the already-cut **chapter** MP4 (line 218-224) — so ffmpeg interprets them as
chapter-relative in *both* cases.

**Consequence:** the clip's true content is always `[chapter_start + pretrim_start, chapter_start + pretrim_end]`
in absolute source coordinates, which is exactly the proposal's formula. The formula stands; the discriminator
`pretrim_used_srt` is deliberately **not** used. The absolute/relative inconsistency is a pre-existing defect
of the pre-trim path (it makes the SRT-selected window land on the wrong content), is **out of scope** here,
and is recorded below as a follow-up. A validity guard covers the resulting garbage window.

## Architecture Decisions

| # | Decision | Choice | Alternatives rejected | Rationale |
|---|---|---|---|---|
| D1 | Short SRT source file | Chapter sidecar `subtitles.srt` first (`find_srt_for_chapter(..., canonical_dir=get_video_chapter_dir(...))`), legacy merged SRT as fallback | Merged SRT only (as `write_chapter_srt_sidecar` does) | Both carry **absolute** source timestamps, so the window math is identical; the sidecar is far smaller and survives merged-SRT cleanup. Self-read is impossible: the output is `shorts/{clip_id}.srt`, a different file. |
| D2 | Short SRT timestamps | **Re-timed to clip origin** via `_window_srt_blocks` | `chapter_window_blocks` (absolute), as the proposal mentioned | A sidecar next to a standalone MP4 must start at `00:00:00,000` or every player renders it offset by the chapter start. Absolute timestamps are correct for the *chapter* sidecar (a context payload), wrong for a *clip* sidecar (a playable subtitle track). |
| D3 | Window formula | `[chapter_start + pretrim_start, chapter_start + pretrim_end]`; **either** offset `NULL`/non-numeric → full chapter span; window empty/inverted/disjoint from the chapter span → full chapter span + WARNING | Branch on `pretrim_used_srt` | Per F2 the ffmpeg cut is always chapter-relative. The guard is what makes the pre-existing defect visible instead of silently producing an empty SRT. |
| D4 | Pre-trim offsets source | `ti.xcom_pull(key="claimed_clip")` inside `poke` | New `video_shorts` read | `claim_pending_clip` uses `RETURNING *`, and `_claim_clip_from_queue` already pushes the whole row to XCom (`reap_processor_dag.py:161`). Zero new query, no new serialization risk. Missing XCom degrades to `(None, None)` → full-chapter fallback. |
| D5 | No DB column for short SRTs | Path is re-derived by `get_chapter_short_srt_path` | Track path/status on `video_shorts` | Confirmed product decision 1; matches #340, keeps PR1 migration-free. |
| D6 | Analysis hook placement | Shared path of `_prepare_thumbnail_config`, after `blocks` parsing, via one `_analyze_chapter_content(...)` helper | Inside the (dead) chapter branch; `_run_prepare_thumbnail_config` | See F1. The helper keeps an already-long function from growing an LLM+DB concern inline. `_run_prepare_thumbnail_config` has no `blocks`/SRT text. |
| D7 | Analysis input text | Chapter-scoped text from `chapter_window_blocks(blocks, ctx["start_time"], ctx["end_time"])` where `ctx = db.get_chapter_srt_context(chapter_id)`; one extra indexed read per upload, no extra file I/O. `ctx is None` → skip both analyses with a WARNING | Reuse the turn-scoped `fragment`; read `chapter["start_time"]` off the row | Chapter-level columns must not be derived from one turn's words. The uploader's `uploadable_turns` row has no `start_time`/`end_time` (F1), so reading them off the row yields `[]` and silently persists nothing. Reuses the same parsed `blocks`: no extra file read. |
| D8 | Idempotency / repeat runs | Always re-run and always re-write on success; the LLM call is served by `cached_json_completion` (same text → same key → same normalized output → same UPDATE) | `IS NULL` gate; a `*_analyzed_at` marker column | `topics` is already non-NULL for most chapters (written by `ai_chapter_analyzer`), so an `IS NULL` gate would make `extract_topics` permanently inert. A marker column costs a second migration. The write is genuinely idempotent, at most once per day per chapter (the upload gate publishes one item per run). |
| D9 | Persist gate | Write a column **only** when that analysis returned `ok=True`. `ok=True` + empty people → write `{}` (a real finding). `ok=True` + empty topics → **skip the write**, log INFO | Always write; never write empty | An empty mentioned-people result is meaningful and there is no prior value to lose. An empty topics result would clobber a pre-existing non-empty value written at chapter-identification time, and is far more likely a degenerate window than a genuine "no topics". |
| D10 | Persistence statement | One `update_chapter_content_analysis(chapter_id, *, mentioned_slugs=None, topics=None)` building the `SET` list from non-`None` kwargs; no-op when both are `None` | Two separate UPDATE helpers | Independence is preserved (a failed analysis contributes no column), with one statement, one round trip and ~15 fewer lines against a tight PR3 budget. |
| D11 | Roster gate | Slug must exist in the supplied roster **and** confidence ≥ `0.80`; otherwise dropped and the raw name INFO-logged | Store the raw name; store a placeholder | Confirmed product decision 5. Mirrors `CHAPTER_SPEAKER_MIN_CONFIDENCE`, so both resolvers reject at the same bar. |
| D12 | `clip_id` validation | Re-validated against `^[A-Za-z0-9_-]+$` **inside** `write_short_srt_sidecar` | Rely on the caller's existing check (`reap_processor_dag.py:109`) | The pure module owns the path construction, so it must be safe when called from anywhere. Defence in depth against traversal from the external Reap API. |

## Data Flow

### PR1 — sensor → sidecar

```
ReapJobSensor.poke
  ├─ reap_client.get_project_clips()                    (unchanged)
  ├─ db.get_source_video_id_for_chapter(chapter_id)     (unchanged)
  ├─ db.get_chapter_srt_context(chapter_id)   ── NEW ──▶ {video_id, start_time, end_time, session_date}
  │     └─ try/except → None on any failure (MP4 path unaffected)
  ├─ ti.xcom_pull("claimed_clip") ──▶ pretrim_start_secs, pretrim_end_secs
  └─ for clip in clips:
        ├─ download_clip(...)  ──▶ shorts/{clip_id}.mp4      (unchanged, runs first)
        ├─ db.insert_video_short_clip(...)                   (unchanged)
        └─ try: write_short_srt_sidecar(...) ──▶ shorts/{clip_id}.srt   ── NEW, best-effort ──
             except Exception: WARNING, continue to the next clip
             (the module already never raises; the wrapper is belt-and-braces
              so no future edit to it can ever fail the sensor)

write_short_srt_sidecar
  clip_id charset gate ─▶ existing non-empty .srt? ─yes─▶ INFO "reusing" + return Path
        │no
  find_srt_for_chapter(canonical_dir=chapter dir) ─▶ _parse_srt_blocks
        │
  window = [chap_start + pretrim_start, chap_start + pretrim_end]  (fallback: chapter span)
        │
  _window_srt_blocks(blocks, w_start, w_end)   (re-timed to 0)
        │
  0 blocks ─▶ WARNING + None (no file written)   |   else tmp write + os.replace ─▶ Path
```

### PR3 — upload prep → two analyses → one persist

```
_prepare_thumbnail_config(turn_row, db)
  └─ blocks = _parse_srt_blocks(srt_path)              (already exists, line 341)
       ├─ fragment  = turn-window text  ─▶ config["srt_fragment"]      (unchanged)
       └─ _analyze_chapter_content(chapter_id, blocks, db)   ── NEW
             ├─ try: ctx = db.get_chapter_srt_context(chapter_id)
             │        └─ except / ctx is None ──▶ WARNING, skip BOTH analyses, write nothing
             │           (uploadable_turns has no start_time/end_time — F1)
             ├─ chapter_text = " ".join(b["text"] for b in
             │        chapter_window_blocks(blocks, ctx["start_time"], ctx["end_time"]))
             │        └─ empty ──▶ WARNING, skip both analyses, write nothing
             ├─ try: resolve_mentioned_people(chapter_text, get_participants_roster())
             │        └─ except ──▶ mentioned = None   (topics unaffected)
             ├─ try: extract_topics(chapter_text)
             │        └─ except ──▶ topics = None      (people unaffected)
             └─ try: db.update_chapter_content_analysis(
                        chapter_id,
                        mentioned_slugs=<list if ok else None>,
                        topics=<list if ok and non-empty else None>)
                      └─ except ──▶ WARNING, upload continues
```

## Interfaces / Contracts

### `congress_videos/config/paths.py` (PR1)

```python
def get_chapter_short_srt_path(
    source_video_id: str,
    chapter_id: int,
    clip_id: str,
    channel_slug: str | None = None,
) -> Path:
    """Return the canonical SRT sidecar path for a Reap short (no mkdir).

    Sibling of ``get_chapter_short_file_path``: ``{shorts_dir}/{clip_id}.srt``.
    """
```

Justified: keeps the `.srt` naming inside the single module that owns the canonical layout, so tests and
future readers resolve the path without string surgery on the `.mp4` path.

### `congress_videos/srt_helpers.py` (PR1)

```python
_SAFE_CLIP_ID_RE = re.compile(r"^[A-Za-z0-9_-]+$")

def write_short_srt_sidecar(
    video_id: str,
    chapter_id: int,
    clip_id: str,
    chapter_start_time,            # SRT str or numeric seconds
    chapter_end_time,              # SRT str or numeric seconds
    pretrim_start_secs: float | None = None,
    pretrim_end_secs: float | None = None,
    session_date: str | None = None,
    channel_slug: str | None = None,
) -> Path | None:
```

Contract:

| Situation | Behaviour | Log |
|---|---|---|
| `{clip_id}.srt` exists and is non-empty | return that `Path`, no rewrite | `INFO ... reusing existing sidecar %s` (with path) |
| `clip_id` fails the charset gate | return `None` | `WARNING ... unsafe clip_id %r` |
| `chapter_start_time`/`end_time` unparseable | return `None` | `WARNING ... unparseable chapter bounds` |
| No source SRT found | return `None` | `WARNING ... no source SRT for video_id=%r chapter_id=%r clip_id=%r` |
| Source SRT exists but cannot be read (`OSError`) | `_parse_srt_blocks` yields `[]` → falls into the zero-blocks branch, return `None`, no file written | `WARNING` from `_parse_srt_blocks` (`Failed to read SRT for block parsing`) plus the zero-blocks `WARNING` |
| Offsets `NULL`/non-numeric | window falls back to the full chapter span, continue | `INFO ... no pre-trim offsets — using full chapter span` |
| Derived window inverted or disjoint from the chapter span | fall back to the full chapter span, continue | `WARNING ... derived window %.1f..%.1f outside chapter span — falling back` |
| Window yields 0 blocks | return `None`, **no file created or truncated** | `WARNING ... window yields no blocks — no file written` |
| `OSError` on write | unlink tmp, return `None` | `WARNING ... failed to write sidecar` + `exc_info=True` |

Never raises. Writes via `tmp_path.write_text` + `os.replace`, exactly as `write_chapter_srt_sidecar`.

### `congress_videos/modules/database.py`

```python
# PR1 — consumed by BOTH the PR1 sidecar hook and the PR3 analysis hook
def get_chapter_srt_context(self, chapter_id: int) -> dict | None:
    """{video_id, start_time, end_time, session_date} for one chapter, or None.

    LEFT JOIN youtube_source_videos for session_date, mirroring get_chapter_metadata.
    start_time/end_time are the chapter's own SRT-format bounds. PR3 depends on this
    helper because the uploader's uploadable_turns row does NOT expose them (F1).
    """

# PR3
def update_chapter_content_analysis(
    self,
    chapter_id: int,
    *,
    mentioned_slugs: list[str] | None = None,
    topics: list[str] | None = None,
) -> bool:
    """Write only the columns whose analysis succeeded. Returns False (no statement
    issued) when both kwargs are None. Values are always bound parameters; only the
    SET column names are assembled, from a fixed literal allow-list.
    """
```

### `congress_videos/modules/mentioned_people_resolution.py` (PR2)

```python
MENTIONED_PEOPLE_MIN_CONFIDENCE: float = 0.80
MAX_MENTIONED_PEOPLE: int = 12
MENTIONED_PEOPLE_MAX_CHARS: int = 20_000

@dataclass(frozen=True)
class MentionedPerson:
    mention: str
    participant_slug: str
    display_name: str
    confidence: float
    evidence: str = ""

@dataclass(frozen=True)
class MentionedPeopleResult:
    ok: bool = False                             # True only for a parsed, well-formed response
    people: tuple[MentionedPerson, ...] = ()
    dropped_mentions: tuple[str, ...] = ()

    @property
    def slugs(self) -> tuple[str, ...]: ...      # deduplicated, first-seen order

def resolve_mentioned_people(
    srt_text: str,
    participants: list[dict],
    completion_fn: Callable | None = None,
) -> MentionedPeopleResult:
```

Never raises. Empty `srt_text` or empty roster → `MentionedPeopleResult()` (`ok=False`, no LLM call).
`completion_fn` defaults to a lazily imported `utils.llm_cache.cached_json_completion`, model `LLM_CHEAP`.

Algorithm: truncate text to `MENTIONED_PEOPLE_MAX_CHARS` → build the `slug | display_name | party` roster
block (same shape as `chapter_speaker_resolution`) → one call → on `error`/missing `data` return `ok=False`
→ set `ok=True` → per entry: drop when `participant_slug` is falsy or absent from `roster_by_slug`
(INFO-log the raw name), drop when `float(confidence)` fails or is `< 0.80` → dedup by slug preserving
first-seen order → cap at `MAX_MENTIONED_PEOPLE`. `display_name` always comes from the roster row, never
from the model.

Prompt (`MENTIONED_PEOPLE_SYSTEM_PROMPT` / `MENTIONED_PEOPLE_USER_TEMPLATE` in `config/ai_prompts.py`):

```json
{"mentions": [{"name": "<verbatim from transcript>",
               "participant_slug": "<slug copied exactly from the list, or null>",
               "confidence": 0.0,
               "evidence": "<one sentence>"}]}
```

The system prompt MUST carry the speaker/mentioned distinction verbatim: *"The person who is SPEAKING is not
automatically a mentioned person. Include only people REFERRED TO in the transcript content."* — this is
success criterion #432 ("prompts and storage preserve the distinction") and is asserted directly by a test.

### `congress_videos/modules/topic_extraction.py` (PR3)

```python
MAX_TOPICS: int = 8
MAX_TOPIC_CHARS: int = 60
TOPICS_MAX_CHARS: int = 20_000

@dataclass(frozen=True)
class TopicsResult:
    ok: bool = False
    topics: tuple[str, ...] = ()

def extract_topics(srt_text: str, completion_fn: Callable | None = None) -> TopicsResult:
```

Normalisation, applied in order: `strip()` → `lower()` → collapse internal whitespace (`" ".join(s.split())`)
→ drop empty → drop longer than `MAX_TOPIC_CHARS` → dedup on the normalised string preserving first-seen
order → truncate to `MAX_TOPICS`.

`MAX_TOPICS = 8` rationale: `reap_shorts_uploader_dag.py:163` flattens topics into a single
comma-separated prompt line; 8 normalised phrases of ≤60 chars keep that line bounded (~200 chars typical)
while covering realistic chapter breadth. Beyond 8 the tail is prompt noise with no discriminative value.
`MAX_TOPIC_CHARS = 60` rejects a model that returns a sentence instead of a topic label.

Prompt schema: `{"topics": ["<short topic label>", ...]}`, instructed to return concise Spanish noun phrases,
not sentences.

## Data Model

### `congress_videos/sql/migrations/045_add_chapter_mentioned_people.sql` (PR2)

```sql
-- Migration 045: Record who is MENTIONED in a chapter (issue #432)
-- Created: 2026-09-03
-- Depends on: 020_add_resolved_participant_slug.sql (the speaker column this one is NOT)
--
-- resolved_participant_slug answers "who is SPEAKING in this chapter" (single-valued).
-- mentioned_participant_slugs answers "who is TALKED ABOUT" (many-valued). They are
-- different concepts and must never be conflated.
--
-- Bare TEXT[] with no FK: Postgres cannot express a per-element FK on an array, so
-- integrity is write-time only, enforced by the roster gate in
-- congress_videos/modules/mentioned_people_resolution.py. This matches the existing
-- speakers[] / key_speakers[] columns on the same table.
--
-- NULL = never analysed. '{}' = analysed, nobody mentioned. The distinction is load-bearing.
-- No index: the column is read back per-row by chapter_id, never filtered or sorted on.
-- Runner runs `SET search_path TO {schema}, public`, so names are UNQUALIFIED.

-- UP

ALTER TABLE video_chapters
    ADD COLUMN IF NOT EXISTS mentioned_participant_slugs TEXT[];

COMMENT ON COLUMN video_chapters.mentioned_participant_slugs IS
    'congress_participants.slug values for people MENTIONED in the chapter transcript '
    '(issue #432). Distinct from resolved_participant_slug, which is the chapter SPEAKER. '
    'Roster-gated at write time; unresolved or ambiguous mentions are dropped, never invented. '
    'NULL = never analysed; empty array = analysed, nobody mentioned.';

COMMENT ON COLUMN video_chapters.topics IS
    'Normalized topic labels (lowercase, trimmed, deduplicated, first-seen order, max 8). '
    'Source of truth moved in issue #432 from the chapter-identification by-product '
    '(utils/ai_chapter_analyzer.py) to the dedicated extract_topics call made at upload time '
    '(congress_videos/modules/topic_extraction.py). A successful extraction that yields zero '
    'topics does NOT overwrite a pre-existing value.';

-- DOWN
-- Manual psql only -- the runner executes the WHOLE file in ONE transaction, so a live
-- DOWN block would revert its own UP and still be recorded as applied.
--
-- ALTER TABLE video_chapters
--     DROP COLUMN IF EXISTS mentioned_participant_slugs;
```

### Mirrors and drift test (PR2)

| File | Change |
|---|---|
| `congress_videos/sql/production_schema.sql` | `mentioned_participant_slugs` becomes the **last** column of the `production.video_chapters` block, so two edits are required: (1) append a trailing comma to line 117, making it `upload_verified_at TIMESTAMPTZ,`; (2) after it add `-- Added by migration 045 (mentioned people, issue #432)` and `mentioned_participant_slugs TEXT[]` with **no** trailing comma, immediately before the closing `);`. Writing `TEXT[],` as the final column is invalid SQL. The added comment must contain no literal `);` — the drift test's block extractor terminates on the first one. |
| `congress_videos/sql/youtube_chapters_schema.sql` | Same column added to the `development.video_chapters` block, keeping the dev bootstrap in lockstep. Apply the same last-column comma discipline: whichever line currently precedes the closing `);` gains a trailing comma, and the new column ends without one. |
| `tests/congress_videos/sql/test_production_schema.py` | Append `"mentioned_participant_slugs"` to `TABLE_COLUMNS["video_chapters"]` (line 97-128) and bump the two `133 columns` comments (lines 78, 484) to `134`. No new test class needed — `test_column_present_in_block` is parametrized over `TABLE_COLUMNS` and picks the entry up automatically. |

No `FK_QUALIFICATIONS` entry: the column carries no FK by design.

## File Changes

| File | Action | PR | Description |
|---|---|---|---|
| `congress_videos/config/paths.py` | Modify | 1 | `get_chapter_short_srt_path` |
| `congress_videos/srt_helpers.py` | Modify | 1 | `write_short_srt_sidecar`, `_SAFE_CLIP_ID_RE` |
| `congress_videos/modules/database.py` | Modify | 1 | `get_chapter_srt_context` |
| `congress_videos/reap_processor_dag.py` | Modify | 1 | sidecar hook in `ReapJobSensor.poke` |
| `tests/congress_videos/test_srt_helpers.py` | Modify | 1 | `TestWriteShortSrtSidecar` |
| `tests/congress_videos/config/test_paths.py` | Modify | 1 | short SRT path assertions |
| `docs/PIPELINE.md`, `docs/ARCHITECTURE.md` | Modify | 1 | shorts artifact paths |
| `congress_videos/sql/migrations/045_add_chapter_mentioned_people.sql` | Create | 2 | additive DDL + column comments |
| `congress_videos/sql/production_schema.sql` | Modify | 2 | mirror |
| `congress_videos/sql/youtube_chapters_schema.sql` | Modify | 2 | dev mirror |
| `tests/congress_videos/sql/test_production_schema.py` | Modify | 2 | drift test |
| `congress_videos/modules/mentioned_people_resolution.py` | Create | 2 | pure resolver |
| `congress_videos/config/ai_prompts.py` | Modify | 2, 3 | two prompt pairs |
| `tests/congress_videos/modules/test_mentioned_people_resolution.py` | Create | 2 | |
| `congress_videos/modules/topic_extraction.py` | Create | 3 | pure extractor |
| `congress_videos/youtube_upload_dag.py` | Modify | 3 | `_analyze_chapter_content` + call site |
| `congress_videos/modules/database.py` | Modify | 3 | `update_chapter_content_analysis` |
| `tests/congress_videos/modules/test_topic_extraction.py` | Create | 3 | |
| `tests/congress_videos/test_youtube_upload_dag.py` | Modify | 3 | hook + isolation tests |
| `docs/ARCHITECTURE.md`, `docs/PIPELINE.md` | Modify | 3 | metadata semantics |

## Testing Strategy (strict TDD — RED first on every unit)

| PR | RED test written first | Fixtures / technique |
|---|---|---|
| 1 | `test_writes_srt_next_to_clip_mp4` — asserts `shorts/{clip_id}.srt` exists beside `{clip_id}.mp4` | `tmp_path` + `mocker.patch("congress_videos.config.paths.PROJECT_DATA_DIR", str(tmp_path))`; source SRT built by the existing `srt_file` fixture style |
| 1 | `test_timestamps_are_retimed_to_clip_origin` — first block starts at `00:00:00,000` | asserts on the written text, catching a `chapter_window_blocks` regression (D2) |
| 1 | `test_existing_non_empty_srt_is_reused_not_rewritten` | write a sentinel file, assert bytes unchanged + `caplog` INFO contains the path |
| 1 | `test_null_pretrim_offsets_fall_back_to_full_chapter_span` (parametrized: both `None`, start only `None`, end only `None`, non-numeric) | matches the D3 "either offset" rule |
| 1 | `test_unreadable_source_srt_returns_none_and_writes_no_file` | create the source SRT then `mocker.patch("builtins.open", side_effect=OSError)`; assert `None`, `not path.exists()`, and the WARNING |
| 1 | `test_window_outside_chapter_span_falls_back_with_warning` | offsets that place the window past the chapter end (F2 guard) |
| 1 | `test_zero_blocks_writes_no_file_and_warns` | assert `not path.exists()` **and** returns `None` |
| 1 | `test_missing_source_srt_returns_none_and_warns` | no SRT on disk |
| 1 | `test_unsafe_clip_id_refuses` — parametrized `"../../etc/passwd"`, `"a/b"`, `""` | asserts `None` and that nothing was written outside the shorts dir |
| 1 | `test_oserror_on_write_returns_none_and_leaves_no_tmp` | `mocker.patch` `os.replace` to raise `OSError` |
| 1 | `test_sensor_still_downloads_mp4_when_sidecar_raises` | patch `write_short_srt_sidecar` to raise; assert `poke(...)` **returns `True`** AND `insert_video_short_clip` was called. Both assertions are required: the insert runs *before* the sidecar, so asserting the insert alone cannot detect a raise — only the return value proves the exception was swallowed |
| 2 | `test_returns_empty_and_ok_false_on_empty_text_or_empty_roster` | no `completion_fn` call asserted via a spy |
| 2 | `test_zero_one_and_multiple_people_resolved` (parametrized) | injected `completion_fn` returning canned dicts |
| 2 | `test_slug_absent_from_roster_is_dropped_and_logged` | `caplog` INFO contains the raw name |
| 2 | `test_low_confidence_and_non_numeric_confidence_dropped` (parametrized) | |
| 2 | `test_duplicate_slugs_deduplicated_first_seen_order` | |
| 2 | `test_capped_at_max_mentioned_people` | |
| 2 | `test_malformed_response_returns_ok_false` (parametrized: `{"error": ...}`, `{"data": None}`, missing key, non-list) | ensures no clobbering write downstream |
| 2 | `test_never_raises_on_completion_fn_exception` | `completion_fn` raising |
| 2 | `test_prompt_states_speaker_is_not_a_mention` | asserts on the prompt constant text |
| 2 | drift: existing parametrized `test_column_present_in_block` turns RED before the mirror edit | |
| 3 | `test_topics_normalized_lowercase_trimmed_whitespace_collapsed` | injected `completion_fn` |
| 3 | `test_topics_deduplicated_preserving_first_seen_order` | |
| 3 | `test_no_topics_returns_ok_true_empty` and `test_capped_at_max_topics` | |
| 3 | `test_overlong_topic_dropped` | > 60 chars |
| 3 | `test_malformed_output_returns_ok_false` (parametrized) | |
| 3 | `test_one_analysis_failing_persists_the_other` | patch `resolve_mentioned_people` to raise, assert the UPDATE still carries `topics` and no `mentioned_participant_slugs` |
| 3 | `test_empty_topics_does_not_overwrite` | asserts `topics` absent from the UPDATE kwargs (D9) |
| 3 | `test_analysis_uses_chapter_window_not_turn_window` | turn row built from the real `uploadable_turns` column list (**no** `start_time`/`end_time` keys); `db.get_chapter_srt_context` mocked to return the chapter span; group span a strict subset of it; assert the analyses receive the chapter-scoped text (F1/D7) |
| 3 | `test_missing_chapter_context_skips_both_analyses` | `db.get_chapter_srt_context` returning `None` and raising (parametrized); assert `update_chapter_content_analysis` was never called and the upload continues |
| 3 | `test_db_failure_does_not_fail_the_upload` | `update_chapter_content_analysis` raising |

All LLM interaction is through the injectable `completion_fn`; **no test may hit the network or Postgres**.
Integration coverage stays at the DAG-callable level with a mocked `db`, matching
`tests/congress_videos/test_youtube_upload_dag.py`. E2E is the existing
`airflow dags list-import-errors` gate — PR1 and PR3 both touch DAG modules, so the Docker e2e smoke test
applies (`congress_videos/**`).

## Review Workload Forecast

| PR | Est. changed lines | Budget | If it overruns — cut in this order |
|---|---|---|---|
| PR1 | ~335 | 400 | 1) move `docs/ARCHITECTURE.md` NAS-layout edit to the release PR; 2) parametrize the three unsafe-`clip_id` cases into one test |
| PR2 | ~352 | 400 | 1) move the `youtube_chapters_schema.sql` dev mirror to PR3; 2) parametrize the drop-reason tests (unknown slug / low confidence / non-numeric) into one table; 3) trim the module docstring rationale, which this design already holds |
| PR3 | ~400 | 400 | **At budget — apply cut 1 up front.** 1) move the `docs/PIPELINE.md` turn/upload-flow edit to the release PR (`dev` → `main`), which lands ~20 lines below budget; 2) parametrize the malformed-output cases; 3) `update_chapter_content_analysis` is already the merged single-helper form (saves ~15 vs. two helpers) |

`Decision needed before apply: No` · `Chained PRs recommended: Yes` · `400-line budget risk: Medium`

PR3 grew from ~365 to ~400 because the F1 correction adds the `get_chapter_srt_context` read, the
skip-on-missing-context branch, and two tests. Under no circumstances shrink it by deleting tests, docs or
comments — take cut 1.

Strategy is `auto-chain`: PR1 targets `dev`; PR2 targets PR1's branch; PR3 targets PR2's branch. Each child
diff must show only its own work unit — retarget or rebase if GitHub shows an earlier slice.

## Threat Matrix

`N/A` for the triggering boundaries — this design adds **no** routing, shell command, subprocess, VCS/PR
automation, executable-file classification, or process integration. `reap_clip_preparer_dag.py` runs
`ffmpeg`/`ffprobe` subprocesses but is read-only context here and is not modified.

One untrusted-input control remains a design requirement and propagates to tasks and RED tests unchanged:

| Boundary | Untrusted input | Control | RED test |
|---|---|---|---|
| `shorts/{clip_id}.srt` path construction | `clip_id` from the external Reap API | `^[A-Za-z0-9_-]+$` full-match gate inside `write_short_srt_sidecar` (D12), independent of the caller's existing gate | `test_unsafe_clip_id_refuses` |
| `find_srt_for_chapter` probe | `video_id` from a DB row | Existing charset gate in `find_srt_for_chapter` (`srt_helpers.py:76-78`) — unchanged, relied upon | covered by existing tests |
| `update_chapter_content_analysis` SQL | LLM-derived slugs and topics | Values always bound as parameters; only column **names** are assembled, from a fixed literal allow-list | `test_update_uses_bound_parameters` |

## Docs to Update

| File | Section | Change |
|---|---|---|
| `docs/ARCHITECTURE.md` | NAS layout, lines 73-78 | Add `{channel_slug}/{source_video_id}/video_chapters/{chapter_id}/subtitles.srt` and `.../shorts/{clip_id}.mp4 + {clip_id}.srt`, noting the short SRT is a chapter/pre-trim-window approximation (PR1) |
| `docs/ARCHITECTURE.md` | `video_chapters` column list, lines 189-197 | Add `mentioned_participant_slugs[]` and re-document `topics[]` per the new source of truth (PR3) |
| `docs/PIPELINE.md` | Shorts pipeline, lines 88-94 (`reap_processor`) | State that the sensor writes `{clip_id}.srt` beside the MP4, best-effort, never failing the run (PR1) |
| `docs/PIPELINE.md` | Turn/upload flow, around lines 75-81 | State that the upload prep derives mentioned people and topics from the chapter SRT window, independently persisted (PR3) |

`CONTEXT.md` does not exist at the repo root in this worktree despite the `CLAUDE.md` reference — no edit is
possible or attempted. `docs/DAGS.md` needs no change: it describes schedules and task graphs, and no task
boundary changes.

## Migration / Rollout

Migration `045` is additive and idempotent (`ADD COLUMN IF NOT EXISTS`); every existing row defaults to
`NULL`, so no backfill runs and no read exists until PR3 merges. Apply it via the `run_migrations` DAG on
`dev` first, confirm with `\d video_chapters` on the live schema (`information_schema` is blind to the
`airflow` role — use `pg_attribute`), then pre-apply on `production` **before** merging to `main` to close
the code-without-migration window.

Rollback: PR1 revert leaves inert `.srt` files on disk (harmless). PR2 revert requires the manual
`ALTER TABLE video_chapters DROP COLUMN IF EXISTS mentioned_participant_slugs` from the commented DOWN block
plus reverting the mirrors. PR3 revert leaves both columns populated but unread.

## Open Questions

- [ ] None blocking. One follow-up to file after PR1: `pretrim_start_secs`/`pretrim_end_secs` carry absolute
      source-video seconds when `pretrim_used_srt = TRUE` but chapter-relative seconds when it is `FALSE`,
      while ffmpeg always applies them chapter-relative (F2). The SRT-selected pre-trim window therefore
      lands on the wrong content. Out of scope here; the D3 validity guard makes it visible instead of
      silently emitting an empty sidecar.

## Consistency With the Spec

- Short-sidecar timestamps MUST be re-timed to clip origin — matches **D2**.
- A failed or malformed topic extraction leaves `topics` untouched, and a successful extraction that yields
  zero topics also leaves it untouched — matches **D9**.
- Either pre-trim offset being `NULL`/non-numeric falls back to the full chapter span — matches **D3** and
  the `write_short_srt_sidecar` contract table.

`next_recommended: sdd-tasks`
