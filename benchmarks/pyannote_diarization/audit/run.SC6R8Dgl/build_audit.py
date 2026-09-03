#!/usr/bin/env python3
"""Build a ground-truth audit kit for the pyannote speaker-change candidates.

Reads postprocessed-speaker-changes.json, cuts a short mp3 clip centred on each
candidate change point, and emits a labelling sheet (CSV + Markdown) for a human
to mark each change as real / noise. Precision and a threshold suggestion are
computed later from the filled-in labels.
"""

from __future__ import annotations

import csv
import json
import subprocess
from pathlib import Path

HERE = Path(__file__).parent
WAV = HERE / "calibration-1310s.wav"
CHANGES = HERE / "postprocessed-speaker-changes.json"
CLIPS = HERE / "clips"
PAD_SECONDS = 4.0  # audio before and after the change point
WAV_DURATION = 1309.9935

CLIPS.mkdir(exist_ok=True)


def fmt_mmss(seconds: float) -> str:
    m, s = divmod(seconds, 60)
    return f"{int(m):02d}m{s:05.2f}s"


def main() -> None:
    data = json.loads(CHANGES.read_text())
    changes = data["speaker_changes"]
    rows = []
    for i, ch in enumerate(changes, start=1):
        start = float(ch["start_seconds"])
        clip_start = max(0.0, start - PAD_SECONDS)
        clip_len = min(WAV_DURATION, start + PAD_SECONDS) - clip_start
        clip_name = f"{i:02d}_{fmt_mmss(start)}.mp3"
        clip_path = CLIPS / clip_name
        subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-loglevel",
                "error",
                "-ss",
                f"{clip_start:.3f}",
                "-i",
                str(WAV),
                "-t",
                f"{clip_len:.3f}",
                "-ac",
                "1",
                "-ar",
                "16000",
                "-q:a",
                "4",
                str(clip_path),
            ],
            check=True,
        )
        rows.append(
            {
                "idx": i,
                "timestamp": ch["timestamp"],
                "start_seconds": f"{start:.3f}",
                "transition": f"{ch['from_speaker']}->{ch['to_speaker']}",
                "score_block_seconds": ch["confirmed_block_duration_seconds"],
                "clip": f"clips/{clip_name}",
                "label": "",  # fill: real | ruido
                "notes": "",
            }
        )

    fields = [
        "idx",
        "timestamp",
        "start_seconds",
        "transition",
        "score_block_seconds",
        "clip",
        "label",
        "notes",
    ]
    with (HERE / "audit-sheet.csv").open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        "# Auditoría de oído — 32 candidatos de cambio de orador (run.SC6R8Dgl)",
        "",
        "Escuchá cada clip (el cambio está en el segundo ~4). Marcá en la columna",
        "**label**: `real` si de verdad cambia el orador, `ruido` si no (mismo",
        "orador, solape, silencio, aplauso, corte falso).",
        "",
        "| # | ts | transición | score (s) | clip | label | notas |",
        "|---|----|-----------|-----------|------|-------|-------|",
    ]
    for r in rows:
        lines.append(
            f"| {r['idx']} | {r['timestamp']} | {r['transition']} | {r['score_block_seconds']} | {r['clip']} |  |  |"
        )
    (HERE / "audit-sheet.md").write_text("\n".join(lines) + "\n")
    print(f"OK: {len(rows)} clips + audit-sheet.csv + audit-sheet.md")


if __name__ == "__main__":
    main()
