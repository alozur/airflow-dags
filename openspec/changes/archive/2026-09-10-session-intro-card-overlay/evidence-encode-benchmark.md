# Evidence: real re-encode cost on the production NAS

Collected 2026-09-10 by the orchestrator, directly against production, to resolve the
blocker-level risk the exploration raised (ffmpeg 3600s timeout cap vs. unbounded video duration).

## 1. Real distribution of long-form video durations

Query against `production.speaker_turn_videos` joined to `production.speaker_turns`, grouped by
`output_path` (so grouped/sibling turns count as ONE video, which is what actually gets re-encoded):

```sql
WITH vid AS (
  SELECT v.output_path, max(t.end_seconds) - min(t.start_seconds) AS span
  FROM production.speaker_turn_videos v
  JOIN production.speaker_turns t ON t.turn_id = v.turn_id
  GROUP BY v.output_path
)
SELECT count(*) AS videos,
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY span) / 60.0 AS p50_min,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY span) / 60.0 AS p95_min,
       max(span) / 60.0 AS max_min
FROM vid;
```

| videos | p50 | p95 | max |
| --- | --- | --- | --- |
| 51 | 5.6 min | 18.8 min | **38.3 min** |

## 2. Real source-media properties

`ffprobe` on the most recently materialized turn video, inside the `airflow-scheduler-prod` container:

- video: `h264`, **1280x720**, `30/1` fps
- audio: `aac`
- ~12.6 MB for 60s

Note: the media is **720p, not 1080p**. Encode cost assumptions based on 1080p would be too pessimistic.

## 3. Measured encode throughput on the NAS

Ran the exact settings `apply_overlays` uses (`libx264 -preset veryfast -crf 20 -c:a copy`) on a real
turn video inside the production container, under the NAS's normal background load:

```
ffmpeg -v error -y -t 60 -i <real turn video> -c:v libx264 -preset veryfast -crf 20 -c:a copy /tmp/bench.mp4
=> encoded 60s of video in 17 seconds
```

**Throughput: ~3.5x realtime.**

## 4. What this means for the timeout risk

`compute_ffmpeg_timeout` = `min(120 + 8.0 * duration, 3600)`.

| Video length | Timeout budget | Expected encode @3.5x | Headroom |
| --- | --- | --- | --- |
| p50, 5.6 min | 2808 s | ~96 s | ~29x |
| p95, 18.8 min | 3600 s (capped) | ~322 s | ~11x |
| max, 38.3 min | 3600 s (capped) | ~656 s (~11 min) | **~5.5x** |

Even a pathological 4x slowdown from I/O contention (0.88x realtime) leaves the worst observed video at
~43 min against a 60 min budget — tight but still inside.

### Conclusion for design

The risk is **real but bounded and much smaller than the exploration feared**. It does not justify the
complexity of segment-and-concat.

- Option (a) — full re-encode under the existing duration-derived timeout — **is viable today** and is
  the recommended baseline.
- Option (b) — segment-and-concat — buys a large constant-factor win but introduces keyframe-alignment
  and A/V-sync risk at the concat boundary. Not warranted by these numbers.
- Option (c) — a hard max-source-duration guard that fails loudly — is **cheap insurance worth keeping**,
  because the 3600s cap means headroom shrinks as video length grows, and nothing in the schema bounds
  that length. A guard turns a silent mid-flight ffmpeg kill into an explicit, diagnosable failure.

Recommended combination: **(a) + (c)**. Numbers above are the evidence; re-measure if the source media
ever moves to 1080p, which would roughly halve the headroom.

## 5. Runtime availability inside the production container (risk #3 from exploration — CLOSED)

The exploration flagged that `FONT_BOLD`/`FONT_REGULAR` and the Pillow runtime were unverified inside
the production Airflow container, because `generic_video_editor` is on-demand-only and may never have
executed there. Checked directly in `airflow-scheduler-prod`:

```
FONT_BOLD    /opt/airflow/data/congress_videos/assets/fonts/LiberationSans-Bold.ttf     exists=True
FONT_REGULAR /opt/airflow/data/congress_videos/assets/fonts/LiberationSans-Regular.ttf  exists=True
Pillow 12.3.0
```

Both fonts resolve and Pillow is importable. **Risk closed.** `_validate_overlay`'s existing font-file
existence check is therefore an adequate guard rather than a theoretical one.

Caveat for implementation: Pillow is **12.x**, so the legacy `ImageDraw.textsize()` API (removed in
Pillow 10) is unavailable. New renderer code must use `textbbox()` / `textlength()`. Verify the 5
existing renderers' idiom and match it.
