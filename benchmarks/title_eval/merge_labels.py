"""Merge the human labelling pass back into the corpus.

The labelling artifact stores one document per item under ``labels/<item_id>``.
This reads those documents (saved locally by the Artifact ``read_db`` action)
and writes them into ``label_sheet.jsonl``, keeping the model's own score and
verdict alongside the human's.

Both are kept on purpose. The human's answer is the ground truth; the model's
is what a judge has to reproduce, and the rate at which the two agree is the
bar any later LLM judge must clear. Collapsing them would throw that number
away.

A ``confirmado`` row means the human reviewed it and let the model's verdict
stand, which is an endorsement, not silence — its rationale is the model's,
recorded with ``why_source: "model-confirmed"`` so the rubric pass can weigh
it differently from a reason the human typed.

Usage::

    uv run python benchmarks/title_eval/merge_labels.py --labels-dir <dir>
"""

from __future__ import annotations

import argparse
import json
import pathlib
import statistics

SHEET = pathlib.Path(__file__).parent / "corpus" / "v1" / "label_sheet.jsonl"


def load_labels(labels_dir: pathlib.Path) -> dict[str, dict]:
    labels: dict[str, dict] = {}
    for path in sorted(labels_dir.rglob("*.json")):
        doc = json.loads(path.read_text(encoding="utf-8"))
        item_id = doc.get("item_id")
        if item_id:
            labels[item_id] = doc
    return labels


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--labels-dir", type=pathlib.Path, required=True)
    args = parser.parse_args()

    labels = load_labels(args.labels_dir)
    rows = [json.loads(line) for line in SHEET.read_text(encoding="utf-8").splitlines() if line.strip()]

    missing = [r["item_id"] for r in rows if r["item_id"] not in labels]
    if missing:
        print(f"warning: {len(missing)} rows have no label yet: {missing[:5]}")

    agreed = 0
    deltas: list[float] = []
    for row in rows:
        doc = labels.get(row["item_id"])
        if not doc:
            continue
        human_why = (doc.get("why") or "").strip()
        row["label"] = {
            "verdict": doc.get("verdict"),
            "score": doc.get("score"),
            "why": human_why or None,
            "why_source": "human" if human_why else "model-confirmed",
            "better_title": (doc.get("better_title") or "").strip() or None,
            "status": doc.get("status"),
            "model_verdict": doc.get("model_verdict"),
            "model_score": doc.get("model_score"),
        }
        if doc.get("verdict") == doc.get("model_verdict"):
            agreed += 1
        deltas.append(float(doc.get("score", 0)) - float(doc.get("model_score", 0)))

    SHEET.write_text(
        "".join(json.dumps(r, ensure_ascii=False, sort_keys=True) + "\n" for r in rows),
        encoding="utf-8",
    )

    labelled = len(deltas)
    print(f"merged {labelled}/{len(rows)} labels into {SHEET.name}")
    if labelled:
        within = sum(1 for d in deltas if abs(d) <= 1.0)
        print(f"verdict agreement:  {agreed}/{labelled} = {100 * agreed / labelled:.0f}%")
        print(f"score within 1.0:   {within}/{labelled} = {100 * within / labelled:.0f}%")
        print(f"score delta:        mean {statistics.mean(deltas):+.2f}  median {statistics.median(deltas):+.2f}")
        # A one-sided delta is a calibration bias, not noise — worth naming.
        low = sum(1 for d in deltas if d > 0)
        high = sum(1 for d in deltas if d < 0)
        print(f"model scored LOW on {low}, HIGH on {high} — a one-sided spread means a systematic bias")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
