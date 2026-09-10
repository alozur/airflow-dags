# Evidence: what a thumbnail regeneration actually costs in production

Collected 2026-09-10 by the orchestrator, directly against production, to resolve the latency-versus-
publish-wrong fork the exploration escalated.

## 1. How often is a thumbnail-text finding raised? — UNKNOWN, and honestly so

```sql
SELECT count(*) AS verified,
       count(*) FILTER (WHERE copy_verification_findings::text LIKE '%thumbnail_text%') AS with_thumb_finding,
       copy_verification_verdict
FROM production.speaker_turn_videos
WHERE copy_verified_at IS NOT NULL
GROUP BY copy_verification_verdict;
```

| verified | with_thumb_finding | verdict |
| --- | --- | --- |
| 1 | 0 | correctable |

**One row.** The #512 verifier only reached production on 2026-09-09, so there is no empirical base rate yet.

This must not be papered over: the expected cost of Approach 1 is `P(finding) x latency`, and `P(finding)`
is currently unmeasurable. The design therefore has to be safe at **any** frequency, not tuned to an
assumed-rare event.

## 2. How long does a regeneration take? — measured, 75 real runs

```sql
SELECT count(*),
       percentile_cont(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (end_date - start_date))) AS p50_s,
       percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (end_date - start_date))) AS p95_s,
       max(EXTRACT(EPOCH FROM (end_date - start_date))) AS max_s
FROM dag_run
WHERE dag_id = 'generic_thumbnail_generator' AND state = 'success' AND end_date IS NOT NULL;
```

| runs | avg | p50 | p95 | max |
| --- | --- | --- | --- | --- |
| 75 | 331 s | **214 s (3.6 min)** | **888 s (14.8 min)** | **3989 s (66 min)** |

## 3. What these numbers actually decide

- **Median added latency is ~3.6 minutes**, and only on the runs where a finding is raised. That is a cheap
  price for not publishing a video with wrong thumbnail text.
- **The tail is the problem, not the median.** p95 is ~15 minutes, and the worst observed run took **66
  minutes** — which **exceeds** the `_THUMBNAIL_MAX_POLLS = 180 x 10s = 1800s` (30 min) bound that
  `video_analytics_actions_dag.py` uses. A synchronous pre-publication wait will therefore genuinely hit its
  bound sometimes; that is not a theoretical edge case, it is in the observed data.

### The consequence for the design

The bounded poll's timeout branch is **not** an error path to be tidied up later — it is a load-bearing,
regularly-exercised path. On timeout the task MUST publish with the existing thumbnail and record that the
regeneration did not land.

That is what preserves #512's non-blocking asymmetry **by construction** rather than by intention: there is
no code path in which a thumbnail-text finding can prevent or indefinitely delay a publication.

A poll bound around **900-1200s** covers the measured p95 (888s) without inheriting the 30-minute worst case
of the analytics-actions DAG, whose post-publication context tolerates far more waiting than the
pre-publication upload path does.

## 4. Cost per regeneration

Fast path: 1 Pikzels image + 1 OpenAI title call. The score-below-threshold retry path (already common)
costs 2 Pikzels images + 1 OpenAI title call. No existing quota or throttle guards this, so the bounded
attempt counter is the **only** spend ceiling — which makes it a cost control, not merely a loop guard.
