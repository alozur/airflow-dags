# Ground-truth audit — speaker-change candidates (run.SC6R8Dgl)

Source: `postprocessed-speaker-changes.json` (32 candidates) from
`pyannote/speaker-diarization-community-1` on `calibration-1310s.wav`
(1310 s ≈ fFpw1dIRcAI). Labelled by ear 2026-08-17. Clips: `clips/` (gitignored).

## Results

| Metric | Value |
|--------|-------|
| Total detections | 32 |
| Noise (false positives) | 6 → idx 2, 3, 8, 16, 23, 31 |
| Redundant duplicates | 7 (pairs 4/5, 9/10, 12/13, 18/19, 21/22, 27/28, 29/30) |
| **Unique real changes** | **19** |
| Crude precision (real/total) | 81.2% |
| **Useful-detection rate (unique/total)** | **59.4%** |
| Redundancy (dup/real) | 26.9% |

## Key finding: `confirmed_block_duration_seconds` is NOT a usable threshold

Score distributions overlap completely:
- noise: 3.0, 6.4, 7.6, 8.1, 8.2, 8.4
- unique real: 1.8, 2.1, 2.3×4, 2.7, 3.1, 3.2, 11.5, 15.7, 16.0, 17.1, 34.4, 40.3, 73.4, 86.1, 92.0, 108.7

Removing all 6 noise requires T≥10.8, which also drops 9/19 real changes
(keeps 53%). No threshold cleanly separates real from noise.

## Why, and design implications for #86

1. **Noise = same-speaker pitch/intonation shifts** (idx 2,3 confirmed by ear).
   The diarization splits one speaker into two clusters; the "new" cluster then
   talks long → high block duration. Block duration measures persistence, not
   speaker-difference confidence — so it cannot discriminate.
2. **Duplicates (27%) are a separate error**, not fixable by score (some dup
   scores reach 133 s). They need temporal merging / A→B→A ping-pong handling in
   postprocessing.
3. **Confirms the #86 design**: acoustic diarization alone yields only ~59%
   useful detections with no reliable threshold. The text layer (president
   announcements) must **confirm/deny** each change, not just name it. The
   text×audio cross-check is the cleanup mechanism.

## Recommended next steps (feed #86 design)

- Postprocessing: merge adjacent changes and collapse A→B→A returns within a
  short window (kills the 27% redundancy).
- Replace/augment the block-duration score with a genuine cluster-separation
  signal (embedding distance) OR gate every acoustic change on a text signal.
- Do NOT calibrate a block-duration threshold — this audit shows it is a dead end.
