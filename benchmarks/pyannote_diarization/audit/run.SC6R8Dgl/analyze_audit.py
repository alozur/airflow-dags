#!/usr/bin/env python3
"""Compute precision + a threshold suggestion from the filled-in audit sheet.

Reads audit-sheet.csv (label column filled with real|ruido) and reports:
- Overall precision of the detector at the current postprocessing settings.
- How precision/retention move if we require score_block_seconds >= T, so a
  threshold can be picked that drops low-confidence false changes.

Note: this measures PRECISION only. Recall needs separately labelling the
changes the detector MISSED, which this sheet does not capture.
"""
from __future__ import annotations

import csv
from pathlib import Path

HERE = Path(__file__).parent
SHEET = HERE / "audit-sheet.csv"


def main() -> None:
    rows = list(csv.DictReader(SHEET.open()))
    labelled = [r for r in rows if r["label"].strip().lower() in {"real", "ruido"}]
    missing = len(rows) - len(labelled)
    if missing:
        print(f"⚠ {missing}/{len(rows)} candidatos sin marcar — completá la columna label.")
    if not labelled:
        return

    reals = [r for r in labelled if r["label"].strip().lower() == "real"]
    precision = len(reals) / len(labelled)
    print(f"Candidatos marcados : {len(labelled)}/{len(rows)}")
    print(f"Reales / ruido      : {len(reals)} / {len(labelled) - len(reals)}")
    print(f"Precisión global    : {precision:.1%}\n")

    scored = sorted(labelled, key=lambda r: float(r["score_block_seconds"]))
    thresholds = sorted({round(float(r["score_block_seconds"]), 1) for r in scored})
    print("Umbral (score >= T) | quedan | precisión | reales conservados")
    print("--------------------|--------|-----------|-------------------")
    total_reals = len(reals)
    for t in thresholds:
        kept = [r for r in labelled if float(r["score_block_seconds"]) >= t]
        kept_reals = [r for r in kept if r["label"].strip().lower() == "real"]
        prec = len(kept_reals) / len(kept) if kept else 0.0
        retain = len(kept_reals) / total_reals if total_reals else 0.0
        print(f"{t:>19.1f} | {len(kept):>6} | {prec:>8.1%} | {retain:>16.1%}")


if __name__ == "__main__":
    main()
